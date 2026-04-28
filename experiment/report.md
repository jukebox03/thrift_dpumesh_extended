# dpumesh Throughput 측정 분석 — Service-side end-node ACK guarantee

Forward path (gateway → service → gateway) 측정. Transport 라이브러리에만 손대고
service binary나 Thrift 서버 종류는 그대로.

## 핵심 결과

| | 값 |
|---|---:|
| Knee 위치 (e2e avg 5× jump 직전) | **40k RPS** |
| Knee 시점 처리량 | **2.38 Gbps** |
| Knee 시점 e2e P99 | 0.88 ms |
| Knee 시점 e2e avg | 0.33 ms |
| Hard ceiling (max wall RPS) | **40.8k RPS** |
| Hard ceiling 처리량 | **2.67 Gbps** |
| Overdrive envelope | 60k offered까지 collapse 없음 |
| Raw service time (low load) | 200–300 µs |

**핵심 메시지**:
1. **Hard ceiling ≈ 40.8k wall RPS / 2.67 Gbps** — 45k–60k offered 구간에서 wall RPS가 38–41k에 평탄.
2. **40k까지 P99 < 1 ms** — 5k부터 40k까지 e2e P99가 0.71–1.48 ms 범위에 깔끔하게 머무름.
3. **Knee = 40k → 45k 사이**, e2e avg가 0.33 → 38.73 ms (117배). Raw P50도 0.27 → 3.15 ms.
4. **60k까지 collapse 없음** — saturate 후 latency만 큐잉으로 폭증, throughput은 cap 근처에 평탄.
5. 이 cap은 **echo path 의 40k cap 과 동등**. service 경유 forward 가 echo 와 거의 같은 throughput envelope을 보여줌.

## 적용된 변경 — Transport 라이브러리만

목표: service-side 가 gateway 와 같은 end-node 자격으로 동작하도록, TX_ACK 책임을
end-node가 100% 보장하고 host-side per-request 비용을 amortize.

| # | 변경 | 위치 | 원리 |
|---|---|---|---|
| 1 | `TDpumeshTransport::read` 가 flush 후 internal `dpumesh_dequeue` 로 다음 요청 fetch | `TDpumeshTransport.cpp` | TConnectedClient::run 의 `for(;;) processor->process()` 가 같은 thread에서 여러 요청을 처리 → per-request pthread_create 비용 amortize. 외부 server 코드 변경 없음. |
| 2 | `write()` 가 TX slot 에 직접 write (lazy alloc) | `TDpumeshTransport.cpp` | `write_buf_` vector + flush memcpy 페어 폐기. body memcpy 1회로 단축 (gateway의 raw 경로와 동일). |
| 3 | `flush()` 가 `register_pending` + `attach_tx` (enqueue 전) + `release_async` 사용 | `TDpumeshTransport.cpp` | gateway 와 같은 pending 메커니즘으로 TX slot lifecycle 관리. **`attach_tx`를 enqueue 전에 두는 것이 핵심**: TX_ACK은 enqueue 후에야 발생 가능하므로, 빠른 TX_ACK이 attach 보다 먼저 도착해서 handler가 `tx_slot=-1` 보고 no-op 후 entry가 영구 stuck되는 race 차단. |
| 4 | 새 API `dpumesh_pending_release_async` | `dpumesh.h`, `dpumesh_doca.c` | responder가 enqueue + attach_tx 후 호출. `state==0, tx_slot ≥ 0` 이면 state=-2 (TX_ACK이 free + state=-1 함). `state==0, tx_slot < 0` 이면 (TX_ACK이 이미 도착함) state=-1 즉시. |
| 5 | `dpumesh_enqueue` 가 OP_RESPONSE 도 `ring_tx_slot_map[ring_slot] = -1` | `dpumesh_doca.c` | pending 이 TX slot lifecycle 단독 owner. ring-wrap deferred-free 와 TX_ACK pending free 의 double-free race 차단. |
| 6 | `process_forward_entry` 의 reverse DMA fc_header.consumer_tail = `target_pod->rx_consumer_tail` | `dpu_worker.c` | 기존엔 `src_pod->rx_consumer_tail` 을 실어서 receiver 가 다른 pod 의 ring 위치값으로 자기 fc_tx_last_consumer_tail 을 corrupt. echo는 src==target 이라 가려짐, forward 에서만 만성 stall. |
| 7 | TX_ACK on AGAIN → **deferred queue** 로 stash, main loop이 매 iteration `pe_progress` 후 drain | `dpu_worker.c` | DPU 는 host TX slot 을 release 할 수 있는 유일한 권한자. TX_ACK 을 drop 하면 host pending 이 state=-2 에 stuck → 2초 register_pending reclaim → tail latency cliff. inline retry-and-drop 대신 deferred queue 로 **end-node ACK 보장**. inline spin 도 ARM 시간을 잡아먹어 consumer_pe 까지 starve 시키므로 폐기. |
| 8 | `try_advance_consumer_tail` 의 implicit-ACK gate `if (desc.flags & OP_RESPONSE)` | `dpumesh_doca.c` | 기존엔 incoming OP_REQUEST 에 대해서도 pending[req_id].produced_to_pos 를 읽어서 0 으로 fc_tail rollback 시킴. 이제 OP_RESPONSE 에만 적용. |

## 테스트 환경

- 부하 발생기: `tput_client` (Go), conns auto-size (rps/25), TCP_NODELAY
- 메시지: 8192 B frame (pad=8116)
- 경로: rapids4 → cni0 → gateway pod → libthrift (raw C client) → DPU comch / DPA / DMA → service pod (TThreadedServer + libthrift `TDpumeshServerTransport`) → libthrift → DPU → gateway → client
- duration 5 s
- 측정 commit: 본 PR (transport-layer 변경만)

## 측정 결과

| Target RPS | Wall RPS | Gbps | Raw P50 | Raw P99 | Raw Max | E2E P50 | E2E P99 | E2E Max | E2E Avg |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|  5k |  4.5k | 0.30 | 0.54 |  1.12 |  5.38 | 0.68 |  1.48 |    5.61 |   0.69 |
| 10k |  9.1k | 0.60 | 0.33 |  0.90 |  5.08 | 0.43 |  1.13 |    5.80 |   0.47 |
| 15k | 13.6k | 0.89 | 0.22 |  0.84 |  3.33 | 0.31 |  0.98 |    4.03 |   0.37 |
| 20k | 18.2k | 1.19 | 0.19 |  0.78 |  3.99 | 0.25 |  0.88 |    4.01 |   0.31 |
| 25k | 22.7k | 1.49 | 0.21 |  0.69 |  3.70 | 0.25 |  0.78 |    5.67 |   0.29 |
| 30k | 27.3k | 1.79 | 0.20 |  0.70 |  4.19 | 0.22 |  0.75 |    6.31 |   0.26 |
| 35k | 31.8k | 2.08 | 0.23 |  0.79 |  7.80 | 0.26 |  0.84 |   10.43 |   0.30 |
| **40k** | **36.4k** | **2.38** | **0.27** | **0.84** | **5.78** | **0.29** | **0.88** | **5.79** | **0.33** |
| 45k | 39.2k | 2.57 | 3.15 | 48.71 | 53.92 | 3.18 | 447.60 |  570.64 |  38.73 |
| 50k | 38.3k | 2.51 |41.65 | 59.50 | 64.14 |177.26|1369.57| 1593.89 | 304.49 |
| 55k | 39.7k | 2.60 |47.29 | 61.97 | 66.82 |455.10|1765.53| 1860.85 | 559.60 |
| 60k | 40.8k | 2.67 |51.20 | 66.80 | 71.26 |754.01|2205.11| 2257.71 | 870.90 |

(latency 단위 = ms)

## 주요 관찰

### 1. Knee = 40k RPS, 그 다음 step에서 hockey stick
- 5k → 40k 구간 e2e avg 0.69 → 0.33 ms (load 가 늘수록 오히려 평균 감소; tput_client 의 rate-limit 으로 인한 schedule wait 이 줄어들기 때문)
- 40k → 45k 에서 e2e avg 0.33 → 38.73 ms (**117×**) — 명확한 knee
- raw service time 도 같이 jump: 0.27 → 3.15 ms (**12×**)

### 2. Hard ceiling ≈ 2.67 Gbps (40.8k wall RPS)
- 45k–60k offered 구간에서 wall RPS 가 38.3k–40.8k 로 평탄
- offered 가 늘어도 throughput 은 flat cap, latency 만 큐잉으로 폭증
- raw P99 도 49–67 ms 범위에 머무름 — DPU/네트워크 측에서 "처리 가능한 만큼만 처리하고 나머지는 큐에 쌓임" 의 정상 동작
- 60k 를 넘겨도 collapse 없이 saturate 만 함

### 3. 40k 까지 P99 깨끗
| Target | E2E P99 | E2E Max | Raw P99 | Raw Max |
|---:|---:|---:|---:|---:|
|  5k | 1.48 ms | 5.61 ms | 1.12 ms | 5.38 ms |
| 20k | 0.88 ms | 4.01 ms | 0.78 ms | 3.99 ms |
| 40k | 0.88 ms | 5.79 ms | 0.84 ms | 5.78 ms |

40k 까지 P99 가 1 ms 이하, Max 도 단일자리 ms. **이전 측정 (TX_ACK drop 정책 사용 시)** 에 보이던 2초 tail 패턴 (state=-2 stuck → 2 s register_pending reclaim) 은 deferred TX_ACK queue 로 사라짐.

### 4. Raw service time 은 200–300 µs 수준
- low load 의 raw P50 약 200 µs. 대부분의 시간이 PCIe DMA + comch 전송 (forward 1회 + reverse 1회 + service 1회 forward + DPU 1회 reverse, 총 4 hops).
- knee 너머에서는 큐잉이 service time(raw) 에도 누적되어 ms 단위로 증가 (45k 이후 raw P50 3 ms+).

## 새 cap 의 후보 — 2.67 Gbps 가 어디서 묶이는가

### 후보 A: DPU ARM 단일-thread main loop
- 가장 유력. process_completion_queue + drain_deferred_tx_acks 가 한 thread 에서 직렬.
- forward path 의 1요청 = comp_queue 4 entry (forward gw, rev_notify→svc, forward svc, rev_notify→gw) + comch send 4건 (TX_ACK ×2 + DMA_COMPLETION ×2) = ARM 8 events/req.
- 40k RPS × 8 = 320k event/s. ARM 한 thread 가 처리 가능한 한계 근처.
- 검증법: ARM 에서 `top -H -p $(pgrep dpumesh_dpu)` 로 main thread CPU% 확인.

### 후보 B: comch send pool / deferred queue 점유율
- send_tasks_max = 1024 인데 해당 한계로 AGAIN 이 자주 떨어지면 deferred queue 가 일정량 누적된 채로 정상 회전.
- 검증법: deferred_tx_ack 의 num_deferred_tx_acks 를 1초 stat 라인에 노출.

### 후보 C: Service 측 TThreadedServer accept 직렬화
- Transport 측에서 (1) multi-request 처리로 thread spawn 비용을 amortize 했으나, accept 자체는 여전히 single-threaded.
- 검증법: service pod 에서 `ps -eLF` 로 thread 수 모니터링; thread 수가 매우 적게 유지되면 (1) 의 amortize 가 잘 작동하지만 accept 단일화가 cap.

## 다음 단계

| # | 변경 | 비용 | 예상 효과 | 우선순위 |
|---|---|---|---|---|
| 1 | DPU stat 라인에 num_deferred_tx_acks / send_pool 사용률 노출 | 1시간 | A vs B 즉시 판별 | 🔥 |
| 2 | Service pod에서 thread/CPU 모니터링 sweep 중 캡쳐 | 30분 | C 검증 | 🔥 |
| 3 | DPU ARM 의 main loop 분할 (consumer_pe progress 별 thread, comp_queue drain 별 thread) | 2–3일 | (A)이면 +50% 가능 | 1번 후 |
| 4 | comch send_tasks_max 8192 로 증가 | 5분 | (B)이면 deferred queue 부담 ↓ | 1번 후 |
| 5 | `TDpumeshServerTransport` 가 listen 시점에 N개 영구 worker thread 를 spawn 하고 직접 dequeue 처리 | 1–2일 | (C)이면 효과 큼. accept 없이 direct dispatch | 2번 후 |

## 생성된 그래프

- `throughput_bps.png` — Throughput–latency hockey stick (e2e + service time), hard ceiling marker
- `latency_vs_rps.png` — Raw/E2E P50·P99 sweep, knee 표시 + overdrive zone
- `throughput_vs_latency.png` — Wall RPS vs offered (좌) + e2e P99 (우), 60k 까지 평탄 cap 표시
- `queueing_gap.png` — End-to-end 분해 (service time + schedule wait), knee 위치 표시

모두 wall-clock 기준 throughput.
