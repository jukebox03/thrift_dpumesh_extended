# Hang 분석 — 2026-04-29

## 재현 (1차)

- `./test-dpumesh.sh deploy` (clean)
- `./test-dpumesh.sh throughput 40000 30` × 4회 sequential (PAIR 1 = Run 1+2, PAIR 2 = Run 3+4)
- PAIR 1: 둘 다 1.2 M OK / 0 fail / Wall 30.50 s, P99 0.81–2.31 ms — 정상
- PAIR 2 Run 1: banner 출력도 못 보고 멈춤 → user-perceived hang

## 측정 데이터 (hang 상태에서 캡처)

### DPU
- 마지막 heartbeat: `11:26:50  recv: 38880/s, sent: 0/s, pods: 2, cq_depth: 0, deferred: 0`
- 그 이후 31분간 **로그 라인 0개** (file size 24006 B 고정, last mod 11:26)
- 외부 5-thread fresh test 송신 → **DPU log 크기 변화 없음** = 메시지 1개도 안 도달
- DPU process 정상 (gdb 부착 OK), main thread는 `epoll_pwait → doca_pe_progress → run_dpu_worker:438` 정상 polling

### Gateway
- TCP 상태:
  ```
  ESTABLISHED  : 1600   (PAIR 2 Run 1의 dial이 kernel-level handshake만 성공)
  CLOSE_WAIT   : 12236  (workers stuck → close 호출 못함, 누적)
  LISTEN       : 32
  ```
- FD: 747 / 1,048,576 (한도 여유) — FD 부족 아님
- ephemeral port range 정상 (gateway는 listener라 무관)
- 워커 thread state 변화 (gdb userspace stack):
  - **Snapshot 1 (hang 직후)**: 32× threads 동일 stack
    ```
    pthread_cond_timedwait
    dpumesh_wait_response
    process_request
    conn_drain
    worker_fn
    ```
    → 32 워커가 모두 응답 대기 중
  - **Snapshot 2 (~30분 후)**: 30× `hrtimer_nanosleep` (kernel wchan)
    → `dpumesh_enqueue` 안의 `get_next_dma_desc()` backoff loop. dma_ring 1024 슬롯 다 차서 무한 nanosleep.

### Service
- thread 단 2개: 1× main(`dpumesh_dequeue cond_timedwait` for accept), 1× PE thread spinning
- gdb stack의 main:
  ```
  pthread_cond_timedwait → dpumesh_dequeue
    → TDpumeshServerTransport::acceptImpl
    → TServerFramework::serve → TThreadedServer::serve → main
  ```
- **service에 RPC 0건 도달** (worker thread spawn 자체가 0회)

## 끊긴 위치

```
[client TCP] → [gateway TCP accept] ✓
            → [gateway worker process_request] ✓
            → [gateway dpumesh_enqueue(dma_ring valid=1)] ✓ (host write)
            → [DPA forward thread polling]    ❌ ← 여기서 끊김
            → [DPU comch consumer recv]       (도달 안 함)
            → [DPU process_forward_entry]
            → [DPU dpu_enqueue_reverse_dma]
            → [DPA reverse]
            → [service]
```

## 원인 가설

### Hint (사용자): DPA thread는 idle 시 sleep / 비활성화됨 (BF3 특성)
PAIR 1 Run 2 wind-down (~11:26:50, recv 38 K → 0) 시점에 DPA가 idle 진입 후 비활성화. PAIR 2 Run 1이 dma_ring에 valid=1 써도 DPA가 깨어날 trigger 없음 → 영구 wedge.

### 우리 코드 (`dpa_kernel.c::run_dma_manager`) 형태
```c
__dpa_global__ void run_dma_manager(uint64_t arg) {
    while (1) {
        handle_msgs(thread_arg);
        drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);
    }   // ← reschedule() / request_notification() 둘 다 없음
}
```
주석에서 "edge-triggered notification race를 피하려 pure polling 채택"이라 명시. git log 보면 이전 버전에는 `doca_dpa_dev_comch_consumer_completion_request_notification` + `doca_dpa_dev_thread_reschedule` 호출이 있었으나 race를 우려해 제거됨.

### 정확한 BF3 sleep 메커니즘
- NVIDIA DOCA DPA RTOS는 cooperative scheduler + watchdog 모델. 단 user 관찰("이전엔 다음날 request도 됐다") 고려 시 단순 watchdog kill 가설은 부정확.
- Race condition 또는 idle policy가 timing-dependent하게 발동하는 형태로 추정 (user 관찰: "타이밍이 너무 불규칙").
- 정확한 문서: <https://docs.nvidia.com/doca/sdk/DPA-Subsystem/index.html>, <https://docs.nvidia.com/doca/sdk/DOCA-DPA/index.html>

## 검증 — keepalive trigger

### 변경
`lib/cpp/src/thrift/transport/doca/dpu_worker.c::run_dpu_worker` 1초 heartbeat 블록에 추가:
```c
if (objs->dpa_thread_running && objs->dpa_comch) {
    struct comch_msg trigger;
    memset(&trigger, 0, sizeof(trigger));
    trigger.type = COMCH_MSG_TYPE_TRIGGER;
    (void)dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                   &trigger, sizeof(trigger));
}
```
즉 init 단계에서만 한 번 보내던 `COMCH_MSG_TYPE_TRIGGER`를 DPU가 1Hz로 계속 보내서 DPA consumer completion에 일정 trigger 유지.

### 결과 — 4-run 모두 통과
| | Wall RPS | P99 | Max | Failed |
|---|---|---|---|---|
| PAIR 1 Run 1 | 39 343.9 | 0.90 ms | 37.98 ms | **0** |
| PAIR 1 Run 2 | 39 343.3 | 1.13 ms | 16.02 ms | **0** |
| PAIR 2 Run 1 | 39 343.9 | 0.77 ms | 29.50 ms | **0** |
| PAIR 2 Run 2 | 39 343.9 | 0.74 ms | 12.75 ms | **0** |

총 4.8 M 요청 / 0 실패 / hang 없음.

DPU heartbeat에서 `sent: 1/s`가 keepalive trigger의 흔적. recv가 0/s 떨어진 직후도 sent=1로 active 유지됨.

### 결론
"DPA가 idle 시 sleep" 가설 **확정**. 외부에서 trigger 1Hz만 보내도 DPA가 active 유지되고 hang 완전 회피. 즉 wedge 원인은 wake-up 메커니즘 부재.

---

## 정공법으로 코드 수정 — 상세 설계

지금의 1Hz keepalive는 검증 목적의 임시 조치. 정공법은 **DPA가 표준 perpetual 패턴(`request_notification + reschedule`)으로 동작**하도록 하고, host/DPU가 새 work을 submit할 때마다 명시적으로 wake 신호를 보내는 것.

### 1. `dpa_kernel.c::run_dma_manager` 표준 패턴으로 재작성

```c
__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;
    doca_dpa_dev_comch_consumer_completion_t consumer_comp =
        thread_arg->dpa_consumer_comp;

    while (1) {
        /* (1) Drain 모든 work — comch 메시지 / forward ring / reverse ring /
         *     producer completion. dma_ring(valid 비트 polling)도 여기서. */
        handle_msgs(thread_arg);
        drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);

        /* (2) Arm notification BEFORE last drain — race window 닫기 */
        doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);

        /* (3) Re-drain after arm — arm 직전에 도착한 이벤트 잡기 */
        if (handle_msgs_nonempty(thread_arg) ||
            drain_all_rings_found_work(thread_arg) ||
            producer_completions_pending(thread_arg)) {
            /* arm 후 도착한 work 발견 → reschedule 안 하고 다시 루프 */
            continue;
        }

        /* (4) Yield: notification 도착 시까지 unschedule */
        doca_dpa_dev_thread_reschedule();
    }
}
```

핵심: race window 닫는 표준 패턴은 "arm → drain check" 순서. arm 후 새 이벤트가 들어오면 notification이 fire되어 reschedule이 즉시 깨어남. arm 시점 이전에 들어온 work은 (3) re-drain에서 잡힘.

### 2. dma_ring valid 비트 polling은 별도 신호로 wake

dma_ring valid는 plain memory write라 hardware completion event가 없음. 따라서:
- **host(`dpumesh_enqueue`)가 ring에 valid=1 쓴 후**, 같은 connection을 통해 DPU에게 "trigger" 메시지를 보내고
- **DPU가 받자마자 DPA에 `COMCH_MSG_TYPE_TRIGGER` 전달** → DPA consumer completion fire → DPA 깨어남.

다만 host→DPU 경로는 comch_server (control plane)이고, host→DPA 직접은 없음. 따라서:
- 옵션 (a) host worker가 enqueue 후 DPU로 trigger 보냄 (overhead 큼: 매 enqueue마다 추가 메시지)
- 옵션 (b) host worker가 enqueue 후 어떤 atomic flag만 set, DPU가 PE에서 그 flag 보고 DPA에 trigger
- 옵션 (c) host가 batched trigger — 일정 주기 또는 N개마다 한 번
- 옵션 (d) 현재 keepalive 유지 (1Hz면 거의 무비용, 응답 latency 최대 1초 지연 가능 — 부하 시엔 trigger 자연 발생하므로 영향 없음)

### 3. 실제 권장 구현 순서

1. **`dpa_kernel.c`를 표준 패턴으로 재작성** (위 (1)) — 가장 중요. watchdog 위험 제거 + race window 닫기.
2. **현재 1Hz keepalive 유지** — 옵션 (d)와 동일. idle 상태에서 응답 첫 요청에 최대 1초 지연 가능하나, 부하 시엔 enqueue 자체가 자주 일어나 trigger도 자주 발생.
3. (선택) 옵션 (b) 추가 — host enqueue 시점에 DPU에게 명시 trigger. idle 첫 요청 latency를 마이크로초로 줄임. 다만 keepalive로 충분하면 스킵.

### 4. 검증 기준

수정 후:
- 4-run × 40 K RPS = 4.8 M 요청 / 0 fail
- 30분 idle 후 새 요청도 즉시 처리 (현재 keepalive로 30분 idle 이미 검증 가능)
- back-to-back 무한 반복도 fail 없음

### 5. 위험 / 주의

- `request_notification` API 정확한 시그니처는 DOCA 버전에 따라 다를 수 있음. 빌드 검증 필요.
- DPA reverse ring(DPU→host)도 plain memory polling이라 별도 wake 메커니즘 필요할 수 있음. 다만 reverse는 DPU가 직접 valid=1 쓰므로 DPU PE에서 trigger를 함께 보내면 됨.
- race window 검증은 high-load 부하 + idle 전환 반복 시나리오에서.

---

## 현재 상태

- `dpu_worker.c`의 1Hz keepalive 추가본이 적용된 상태로 hang 재현 안 됨.
- 정공법 코드 작업은 별도 commit으로 진행 권장.
- 임시 keepalive는 정공법 적용 후 제거 예정.
