# dpumesh Throughput 측정 분석 (Go client, 재측정)

## 핵심 결과 요약

| | 이전 (Python client) | **현재 (Go client)** | 변화 |
|---|---:|---:|---:|
| Sustainable ceiling | 21k RPS | **61k RPS** | 2.9× ↑ |
| Sustainable Gbps   | 1.29 Gbps | **3.81 Gbps** | 3.0× ↑ |
| Raw service time @ low load | ~10 ms | **70–110 µs** | 100× ↓ |
| Raw service time @ ceiling | ~10 ms (flat) | 0.07 ms (P50) / 0.56 ms (P99) | — |
| Knee 위치 | 21k → 22k (4.9× e2e jump) | **61k → 62k (11.6× e2e jump)** | — |

**결론**: 이전 측정의 1.29 Gbps / 21k 천장은 **전부 Python client 한계였음**. dpumesh+gateway+service 스택의 실제 sustainable throughput은 약 **3× 높다** (8 KB 메시지 기준).

## 테스트 환경

- 부하 발생기: `tput_client` (Go, stdlib only) — `test-dpumesh.sh throughput <RPS>` 진입점
  - 연결 수 auto-size: `rps/25` (10 ms 가정 시 4× 헤드룸), max 8192
  - 모든 conn에 `TCP_NODELAY`, dial 동시성 cap=32
  - 분리된 dial / run phase, wall-clock 기준 throughput 보고
  - coordinated-omission 보정 (wrk2-style), nominal-duration 분모 버그 없음
- 메시지: 8192 B frame (pad=8124)
- 경로: `tput_client (rapids4)` ─TCP─► `dpumesh-gateway pod (10.244.0.101)` ─Thrift─► `UniqueIdService pod` ─dpumesh DMA─► downstream pod
- Network: cni0 (10 Gbps) — 같은 노드 pod-to-pod

## 측정 결과

| Target RPS | Wall RPS | Gbps | Raw P50 | Raw P99 | E2E P50 | E2E P99 | E2E Avg |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10k | 9.5k | 0.62 | 0.23 | 0.90 | 0.32 | 1.69 | 0.42 |
| 20k | 19.0k | 1.25 | 0.08 | 0.61 | 0.13 | 0.75 | 0.22 |
| 30k | 28.6k | 1.87 | 0.08 | 0.70 | 0.10 | 0.80 | 0.17 |
| 40k | 38.1k | 2.50 | 0.07 | 0.60 | 0.09 | 0.63 | 0.14 |
| 50k | 47.6k | 3.12 | 0.07 | 0.59 | 0.09 | 0.62 | 0.13 |
| 60k | 57.1k | 3.74 | 0.07 | 0.68 | 0.09 | 0.78 | 0.13 |
| **61k** | **58.1k** | **3.81** | **0.07** | **0.56** | **0.09** | **0.60** | **0.13** |
| 62k | 59.0k | 3.87 | 0.07 | 30.59 | 0.09 | 34.49 | 1.51 |
| 63k | 60.0k | 3.93 | 6.56 | 82.86 | 7.82 | 125.40 | 19.04 |
| 65k | 60.7k | 3.98 | 26.52 | 145.90 | 80.25 | 481.29 | 111.87 |
| 70k | 42.6k ⚠️ | 2.79 | 51.41 | 267.33 | 3260.91 | 6631.58 | 3218.61 |

⚠️ 70k에서 **wall-clock RPS가 오히려 떨어짐** (60k → 43k) — server 포화 후 queue overflow로 인한 collapse.

(단위: latency = ms)

## 주요 관찰

### 1. Hockey stick이 매우 sharp
- 60k → 61k → **62k** 경계에서 P99가 0.78 → 0.60 → **34.49 ms** (57× jump)
- E2E avg는 0.13 → 0.13 → **1.51 ms** (11.6× jump)
- 즉 ceiling 직전까지 거의 평탄, 이후 **단 1k RPS 차이로 saturation 진입**. 시스템이 ceiling 부근에서 매우 "급격하게" 무너짐 (gradual degradation 없음).

### 2. 65k에서 wall-clock RPS는 60.7k로 멈춤
- offered가 60k → 65k로 늘어도 **실제로 처리되는 RPS는 ~60k에 capped**.
- 추가 5k는 전부 큐에서 대기 → P99 481 ms로 누적
- 즉 **server-side 진짜 throughput 천장 ≈ 60–61k RPS / 3.8–4.0 Gbps**

### 3. 70k에서 시스템 collapse
- offered 70k → wall RPS **42.6k** 로 감소 (60k도 못 받음)
- corrected avg latency 3.2 초, max 7.7 초
- "throughput-decreasing-with-load" 패턴 — admission control 부재의 신호.
- 자세한 메커니즘 체인 분석은 §70k Collapse 메커니즘 분석 참조.

### 4. Raw service time이 sub-ms 영역
- 50k RPS에서 raw P50 = 0.07 ms = **70 µs** — 이전 Python 측정에서 본 "10 ms 평탄선"이 무엇이었는지 확정:
  - **GIL + 256 thread context switch + sleep 정밀도 + bandwidth calc 버그의 합성**
- 진짜 서버 처리 비용은 서브-ms 단위. 즉 dpumesh DMA path는 대부분의 시간을 microseconds로 통과.

### 5. 클라이언트가 천장이 아니다 (확실)
- 61k * 8192 * 8 / 1e9 ≈ 4.0 Gbps — cni0 10 Gbps의 38%
- conns = 2440 (auto), max 8192까지 여유
- per-conn RPS = 25 (auto-size 헤드룸 기준), 0.07 ms 서비스타임 기준 conn당 14k RPS 가능 → **per-conn 활용률 0.2%**
- dial이 1 초대 (2400+ socket 동시 establish)지만 schedule은 base+500ms 후 시작이라 측정엔 영향 없음
- 즉 이 ceiling은 **server-side 한계**가 맞음.

## 70k Collapse 메커니즘 분석

`offered RPS↑` 인데 `wall RPS↓` 가 발생하는 건 단순 saturation이 아니라 여러 feedback loop가 임계점에서 동시에 트립되는 패턴. Little's law로 in-flight 추정:

| Offered | Wall RPS | E2E avg | In-flight (≈ wall × avg) |
|---:|---:|---:|---:|
| 60k | 57.1k | 0.13 ms | ~7 |
| 61k | 58.1k | 0.13 ms | ~8 |
| 62k | 59.0k | 1.51 ms | ~89 |
| 63k | 60.0k | 19.0 ms | ~1,140 |
| 65k | 60.7k | 112 ms | ~6,800 |
| **70k** | **42.6k** | **3,219 ms** | **~137,000** |

61k → 62k 사이에서 in-flight가 8 → 89로 11×, 65k → 70k 사이에서 6.8k → 137k로 20× — 임계점 한참 넘어선 영역.

### 가설 1 — PE thread starvation (가장 유력)

`dpumesh_doca.c:133` 의 `pe_progress_fn` 은 **busy-spin 일반 pthread** (코어 핀도, RT priority도 없음). 이 스레드가 `TX_ACK 수신 → fc_tx_last_consumer_tail 업데이트 → 호스트 enqueue 재개` 의 책임을 짐.

- 70k 시 gateway는 conn당 1 thread = **2800 worker threads**, 모두 active
- PE thread CPU share = 36 코어 / 2800 thread ≈ **1.3 %**
- comch progress 빈도 ↓ → fc window 복구 ↓ → 워커들이 `nanosleep(10µs→1ms)` 루프 (line 867-893) 에 더 오래 머묾
- 워커들이 더 오래 머물수록 PE 스레드는 더 starved → **양성 피드백**

### 가설 2 — MAX_PENDING (4096) 해시 collision

`dpumesh_doca.c:52, 174` `idx = req_id % MAX_PENDING`:
- 65k: in-flight ≈ 6,800 → 평균 collision 1.7×
- 70k: in-flight ≈ 137,000 → 평균 collision **33×**
- collision 시 `state != 0` 슬롯 덮어쓰기 → 응답 미스라우팅, 슬롯 누수
- 누수 슬롯은 30s timeout까지 잠금 → slot pool 고갈 가속화

### 가설 3 — `ring_lock` mutex convoy

`dpumesh_doca.c:852` 의 단일 mutex 가 모든 enqueue 직렬화. 2800 워커 동시 경쟁 시:
- lock holder가 critical section 내 preempt → 모든 대기자 block
- holder 깨어나서 unlock → futex wakeup → fairness 없음 → cache-hot thread가 또 잡음
- **lock 처리량 50%+ 감소** (Linux mutex의 알려진 convoy state)

### 가설 4 — DPU comp queue 항상 HIGH watermark
`object.h:81-82` `COMP_QUEUE_BP_HIGH = 3072` (75%). 70k에서 항상 75% 위 → DPA가 새 DMA 거부 → 호스트 워커들이 fc_wait → 가설 1 가속.

### 가설 5 — TCP zero-window backpressure
Gateway의 `pthread-per-conn` + `recv()` 직렬 처리. 일부 conn의 커널 recv buffer (~87 KB / 8 KB = 11개) 차면 zero-window 광고 → 클라이언트 200 ms+ stall. 누적되면 throughput hit.

### 가설 6 — `printf` per-request stdio lock
`gateway.c:162, 218` 매 요청마다 `printf` → stdio 내부 mutex. 60k+ 호출 / sec 가 다른 병목과 합쳐져 cliff 형성에 기여.

### Composite 시나리오 (가장 유력)

```
offered 70k
  → in-flight 폭증 (Little's law)
  → 워커 2800 동시 active
  → PE 스레드 CPU share 떨어짐                  [가설 1]
  → fc_tx_last_consumer_tail 업데이트 stall
  → 워커들 nanosleep retry 루프에 갇힘
  → ring_lock 더 오래 잡힘 / convoy             [가설 3]
  → MAX_PENDING collision (in-flight > 4096)    [가설 2]
  → 슬롯 누수 + 응답 미스라우팅
  → 더 많은 워커가 timeout/retry 경로
  → PE 스레드 CPU 더 부족
  → 무한 루프
```

이건 **admission control 부재** + **single-threaded PE driver** 조합으로 발생하는 전형적 패턴.

### 검증 방법

| 방법 | 가설 | 비용 |
|---|---|---|
| `top -H -p $(pgrep gateway)` 실시간 관찰 (PE thread CPU%) | 1 | 30 초 |
| Gateway 로그에서 `"OP_RESPONSE for req_id=X but no waiter"` 카운트 | 2 | 0 (기존 로그) |
| Gateway 로그에서 `"DPU buffer full after N retries"` 카운트 | 4 | 0 |
| `MAX_PENDING 4096 → 65536` rebuild 후 70k 재측정 | 2 | 20 분 |
| PE thread `pthread_setaffinity_np` + `SCHED_FIFO` 후 70k 재측정 | 1 | 30 분 |

가장 cheap한 첫 신호는 `top -H` — PE thread CPU가 거의 0%면 가설 1 확정.

## 병목 가설 — 3.8 Gbps는 어디서 막히는가?

여전히 미분리 영역:

1. **dpumesh transport 자체** (host→DPU→host DMA path)
   - DMA_RING_SIZE=1024, DPU_BUFFER_SIZE=8MB, num_slots=1024 (host TX pool)
   - Slot turnover at 60k RPS = 60000 slots/s 회전 → slot lifetime 17 ms 평균. 1024 slots / 17ms = 60k. **숫자가 맞아떨어진다는 게 의심스러움.**
2. **Gateway pod**
   - `pthread-per-connection` 모델, 2400+ threads → context switch 압박 가능성
   - 매 요청마다 `printf` (gateway.c:162, 218) — stdout I/O 직렬화
   - `dpumesh_tx_alloc` linear bitmap scan O(num_slots=1024) under global lock
3. **UniqueIdService 애플리케이션 로직**
   - C++ Thrift handler, 8 KB carrier map 파싱+직렬화
4. **Go 클라이언트 GC** (영향 작을 가능성 높지만 확정 못함)
   - per-request body alloc, 60k RPS × 8 KB = 500 MB/s allocation
   - max latency 14–30 ms 가끔 튀는 건 GC pause 가능성
5. **TCP 경로 (cni0 + veth)**
   - 10 Gbps이지만 small packet 다발이라 PPS limit 가능성
   - 60k pps in + 60k pps out × 2 (req/resp) = 240k pps — 일반적으론 문제 없는 수준

순위로 1, 2번이 가장 의심스러움. 1번 확인은 코드 instrumentation, 2번은 gateway 측 측정 필요.

## 추가 검증 실험 — 다음 단계

원래 계획(다음 우선순위 정리):

1. **Buffer/lock 계측 (최우선)** — 4개 atomic counter:
   - `tx_alloc_retry_total` (slot pool 고갈 빈도)
   - `fc_wait_retry_total` (DPU RX buffer flow-control 대기 빈도)
   - `ring_wait_retry_total` (DMA ring 고갈)
   - `ring_lock_wait_ns_total` (단일 mutex contention)

   첫 측정 후 어느 layer가 진짜 gate인지 1회 결정 가능.

2. **No-op Thrift method** — 애플리케이션 로직 기여분 분리. UniqueIdService.ComposeUniqueId 대신 즉시 echo/return하는 method 추가.

3. **Gateway 배제** — dpumesh client API를 직접 호출하는 binary로 TCP+gateway 기여분 제거.

4. **Message size sweep** — 128B / 1KB / 8KB / 64KB. RPS-bound vs bandwidth-bound 분리:
   - 작은 msg에서 RPS가 더 높이 가면 → per-op 비용이 한계 (slot/lock)
   - RPS 동일하고 bandwidth만 늘면 → DMA/network 한계

5. **Buffer 크기 sweep** — `DMA_RING_SIZE 1024→4096`, `num_slots 1024→4096`, `DPU_BUFFER_SIZE 8MB→32MB`. 1번 instrumentation에서 gate가 확인된 buffer만 키우면 효과 판별 가능.

6. **70k collapse 분석** — 왜 offered↑일 때 actual↓ 되는지. `pthread`-per-conn에 admission control 추가하거나 `epoll` 기반 gateway로 재작성 검토.

## 생성된 그래프

- `throughput_bps.png` — Throughput–latency 곡선 (Go client, hockey stick + collapse 점)
- `latency_vs_rps.png` — Service time vs end-to-end percentile sweep, collapse zone 표시
- `throughput_vs_latency.png` — Dual-axis (Gbps + P99 latency)
- `queueing_gap.png` — End-to-end latency = service time + schedule wait 분해

모든 그래프는 wall-clock 기준 throughput 사용 (tput_client가 nominal-duration 분모 버그 없이 직접 산출).

## Appendix — 이전 측정과의 차이가 어디서 왔는가

이전 보고서가 보고한 숫자가 왜 그렇게 낮았는지 추적:

| 원인 | 영향 |
|---|---|
| Python GIL + 256 threads | thread당 ~100µs context switch overhead × N → 전체 latency floor 형성 |
| `time.sleep` ≥ 50µs 정밀도 | 47.6 µs interval 스케줄링 미세 어긋남 → backlog |
| 256 thread × sync send/recv = Little's Law 25.6k RPS 천장 | 21k 측정 상한이 정확히 이 영역 |
| `actual_rps = ok_count / nominal_dur` (wall-clock 무시) | 30k에서 1.97 Gbps로 부풀려진 보고 |
| Python 인터프리터 자체 비용 (`struct.pack`, `socket.recv`, list append 등 GIL hold) | raw service time을 ms 단위로 inflate |

이 합성 효과가 "10 ms 평탄선"이라는 잘못된 신호로 보였고, 그것을 dpumesh의 한계로 오해했음. 새 client는 이 다섯 가지를 전부 제거한 결과 진짜 server-side 천장(61k / 3.8 Gbps)이 드러남.
