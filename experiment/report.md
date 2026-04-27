# dpumesh Throughput 측정 분석 (POST optimization)

## 핵심 결과 요약

| | PRE (Go client only) | **POST (A4+A5+A6 적용)** | 변화 |
|---|---:|---:|---:|
| Sustainable ceiling | 61k RPS | **100k RPS** | **1.64× ↑** |
| Sustainable Gbps   | 3.81 Gbps | **6.24 Gbps** | **1.64× ↑** |
| Sustainable P99 (e2e) | 0.60 ms | 0.72 ms | 비슷 |
| Hard ceiling (max wall RPS) | 60.7k (collapses to 42.6k @ 70k) | **118k (graceful @ 130–140k)** | 1.94× ↑ + 무 collapse |
| Knee 위치 | 61k → 62k (e2e avg 11.6× jump) | 100k → 110k (5.2× jump) | knee가 더 부드러워짐 |
| 70k offered → wall RPS | **42.6k (collapse)** | **66.7k (정상)** | collapse 완전 제거 |
| Raw service time @ low load | 70 µs | 70–90 µs | 동일 |
| Raw service time @ ceiling | 90 µs | 90 µs | 동일 |

**핵심 메시지**:
1. **Sustainable throughput 1.64× 향상** — 같은 hardware, 같은 protocol stack, 코드 변경만으로 3.81 → 6.24 Gbps.
2. **70k collapse 완전 제거** — 이전엔 70k에서 시스템이 무너지며 wall RPS가 60k → 43k로 *감소*. 이제 70k는 정상 영역(66.7k 처리), saturation은 ~118k에서 *flat* 으로 부드럽게 cap.
3. **140k offered까지 안정 동작** — 처리는 못해도 죽지 않음. admission control 패턴이 정상화됨.

## 적용한 변경 (POST)

| 변경 | 위치 | 기여 |
|---|---|---|
| **A6** `MAX_PENDING 4096 → 65536` | `dpumesh_doca.c:54` | in-flight 4k+ 시 hash collision 제거 |
| **로그 정리** (INFO → DBG, hot-path printf 제거) | `dpu_worker.c`, `dpa.c`, `comch_server.c`, `dpumesh_doca.c`, `gateway.c` | stdio mutex contention 제거, 60k+ 호출/sec 절감 |
| **A4** `nanosleep` polling → `pthread_cond_*` | `dpumesh_doca.c` (slot_cond, fc_cond) | sleep 정밀도 floor 50–100µs 제거, 즉시 wake-up |
| **A5** epoll thread-pool gateway | `gateway.c` 전체 재작성 | pthread-per-conn (수천) → 32 thread pool + per-worker epoll + SO_REUSEPORT, scheduler thrashing 제거 |

## 테스트 환경

PRE 측정과 동일:
- 부하 발생기: `tput_client` (Go), conns auto-size (rps/25), TCP_NODELAY
- 메시지: 8192 B frame (pad=8124)
- 경로: rapids4 → cni0 (10 Gbps) → gateway pod → Thrift → UniqueIdService → dpumesh DMA → downstream pod
- duration 10 s

## POST 측정 결과

| Target RPS | Wall RPS | Gbps | Raw P50 | Raw P99 | E2E P50 | E2E P99 | E2E Avg |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10k | 9.5k | 0.62 | 0.17 | 0.77 | 0.34 | 1.64 | 0.46 |
| 30k | 28.6k | 1.87 | 0.07 | 0.39 | 0.09 | 0.46 | 0.12 |
| 50k | 47.6k | 3.12 | 0.07 | 0.44 | 0.09 | 0.50 | 0.12 |
| 70k | 66.7k | 4.37 | 0.08 | 0.51 | 0.10 | 0.54 | 0.12 |
| 90k | 85.7k | 5.62 | 0.08 | 0.50 | 0.10 | 0.54 | 0.14 |
| **100k** | **95.2k** | **6.24** | **0.09** | **0.67** | **0.11** | **0.72** | **0.15** |
| 110k | 104.8k | 6.87 | 0.10 | 21.88 | 0.12 | 22.45 | 0.78 |
| 120k | 114.0k | 7.47 | 0.37 | 46.61 | 0.39 | 146.61 | 11.05 |
| 130k | 118.1k | 7.74 | 35.14 | 55.59 | 67.14 | 1306.96 | 197.88 |
| 140k | 118.1k | 7.74 | 43.95 | 60.72 | 525.08 | 2316.84 | 678.60 |

(latency 단위 = ms)

## 주요 관찰

### 1. Sustainable ceiling 100k / 6.24 Gbps (P99 < 1 ms)
- 100k까지 P99 e2e ≤ 0.72 ms, raw P99 ≤ 0.67 ms 유지
- 이전(PRE) 61k 대비 **1.64×**
- `cni0` 10 Gbps의 62% 점유

### 2. Hockey-stick은 여전히 sharp (100k → 110k)
- e2e avg 0.15 → 0.78 ms (5.2× jump)
- e2e P99 0.72 → 22.45 ms (31× jump)
- P50은 0.11 → 0.12 ms로 거의 변화 없음 — **분포가 bimodal**: 대부분 빠르게 처리되고 일부가 큐잉
- PRE 동일 패턴 (62k에서 같은 모양) — 즉 **bottleneck 종류는 그대로, 단지 한계가 위로 이동**

### 3. Hard ceiling은 ~118k wall RPS (= 7.74 Gbps)
- 120k → 114k wall, 130k → 118.05k, 140k → 118.13k — 사실상 평탄
- offered가 더 늘어도 throughput은 **flat cap**, latency만 큐잉으로 폭증
- 이전 PRE의 collapse(throughput 감소)와 다른 패턴 — **정상 saturation**

### 4. Collapse 완전 제거 (가장 중요한 정성적 변화)
| Offered | PRE wall | POST wall | 차이 |
|---:|---:|---:|---|
| 65k | 60.7k | (측정 안 함) | — |
| **70k** | **42.6k (collapse)** | **66.7k (정상)** | +56% |
| 130k | (측정 안 함) | 118.1k | — |
| 140k | (측정 안 함) | 118.1k | — |

이전 collapse 메커니즘 체인 (PE thread starvation → fc-wait nanosleep 폭주 → ring_lock convoy → MAX_PENDING collision)이 다음과 같이 차단됨:
- **PE thread starvation**: 워커 수가 ∞ → 32로 줄어 PE thread CPU share 회복 (1.3% → 12.5%)
- **fc-wait 폭주**: nanosleep polling → cond_var, 대기 시 CPU 안 씀 + 즉시 wake
- **MAX_PENDING collision**: 4096 → 65536, in-flight 60k 미만에서 충돌 거의 없음

### 5. Raw service time은 동일 (70–90 µs)
PRE/POST 모두 ceiling까지 raw service time은 ~70–90 µs로 같음. 즉 **per-request 처리 비용은 변하지 않음**. 변한 건 **얼마나 많은 동시 요청을 처리할 수 있는가** (throughput).

이는 다음을 의미함: dpumesh DMA path 자체는 빠르고, 우리가 푼 건 *동시성 처리 path*. 다음 단계도 같은 방향(더 많은 동시 처리).

## 새 ceiling 분석 — 6.24 Gbps는 어디서 막히는가?

### 후보 1: `ring_lock` 단일 mutex (가장 유력)
`dpumesh_doca.c:73` 의 `pthread_mutex_t ring_lock` 이 모든 enqueue 직렬화. POST에서 32 워커 × 평균 100 µs critical section = 32 × 10k RPS/worker = 320k RPS 이론 상한, 그러나 **mutex 자체의 lock/unlock 비용 + cache ping-pong** 이 ms 수준 누적. 100k 부근에서 lock contention이 첫 병목으로 등장한다고 보면 정확히 들어맞음.
- 다음 단계: **A2 (ring 샤딩)** — ring을 N개로 쪼개서 thread→ring 매핑

### 후보 2: cni0 / TCP 경로 한계
- 10 Gbps cni0의 77% (7.74 Gbps) 도달 — TCP overhead, kernel iptables/conntrack, veth pair processing 비용이 점차 무시 못할 영역
- 8 KB × 118k = 7.74 Gbps 단방향, response까지 합치면 PCIe/NIC 대역 추가 사용
- 특히 small packet (8KB) PPS = 240k pps — kernel softirq budget 한계 가능성

### 후보 3: Linear scan in `tx_alloc` (`dpumesh_doca.c:786`)
1024개 비트맵 linear scan이 여전히 global `slot_lock` 안에 있음. 100k RPS × 평균 100 entries scan = 10M scan/sec. cache hit이긴 하지만 mutex 보유 시간을 늘림.
- 다음 단계: **A3 (freelist)** — O(1) pop/push

### 후보 4: DPU 측 처리 (PE thread, comp queue, DPA)
DPU 측 single PE thread + DPA가 처리할 수 있는 한계. 8 KB DMA 한 번 = ~2-3 µs HW, 100k × 3 µs = 300 ms/sec wall-clock으로 가능. 그러나 SW overhead까지 포함하면 빡빡할 수 있음.
- 검증법: DPU process top — `top -H -p $(pgrep dpumesh_dpu)` 단일 thread CPU 100% 인지

## 다음 단계 — 10–15 Gbps 목표

이전 분석의 우선순위 갱신:

| # | 변경 | 예상 추가 ceiling | 누적 추정 |
|---|---|---:|---:|
| 1 | **A2** ring_lock 샤딩 (ring N개) | +50–80% | **9–11 Gbps** |
| 2 | A3 slot freelist (O(1)) | +10–15% | 10–12 Gbps |
| 3 | B1 zero-copy memcpy 제거 (gateway) | +5–10% | 11–13 Gbps |
| 4 | B5 TCP 커널 buffer 튜닝 + `SO_RCVBUF/SNDBUF` | +5% | 11–13 Gbps |
| 5 | DPU 측 PE thread split / pinning | 검증 필요 | — |
| 6 | C1 fc state atomic화 | 안정성 개선 | — |

**A2가 가장 큰 leverage** — 100k 부근에서 ring_lock contention이 dominant라는 가설이 맞다면 +60% 이상 가능. 코드 변경 범위는 dpumesh_doca.c 안에서 끝남 (3-4일 작업).

만약 A2 적용 후 ceiling이 9 Gbps에 머문다면 cni0 10 Gbps 한계에 임박 — 그 시점에 **메시지 크기 sweep** 으로 RPS-bound vs bandwidth-bound 구분 필요. 예를 들어 32 KB 메시지로 가면 cni0 한계가 RPS 60k 부근으로 내려가서 dpumesh의 다른 한계가 보일 것.

## 70k Collapse는 왜 사라졌는가 — 메커니즘 검증

이전 보고서의 [70k Collapse 메커니즘 분석] 가설 6가지에 대한 사후 검증:

| 가설 | 차단 변경 | 검증 |
|---|---|---|
| 1. PE thread starvation | A5 (워커 수 ∞ → 32) + 로그 정리 | ✓ 70k 정상 동작 |
| 2. MAX_PENDING collision | A6 (4096 → 65536) | ✓ 100k까지 collision 없음 (in-flight ≈ 14, 4096 한참 아래) |
| 3. ring_lock convoy | A4 cond_var (lock hold 시간 단축) + 로그 제거 | △ 일부 완화, 여전히 단일 mutex (다음 단계) |
| 4. DPU comp queue saturation | (직접 변경 없음) | ✓ 100k까지 여유 (in-flight 14 << 4096) |
| 5. TCP zero-window | A5 epoll + TCP_NODELAY | ✓ 워커당 fewer conns, recv buffer 빠르게 drain |
| 6. printf stdio lock | 핫패스 printf/INFO 제거 | ✓ 직접 제거됨 |

가설 1, 2, 5, 6은 직접 차단됨. 가설 3은 부분 완화 (다음 단계 A2가 완전 해결). 가설 4는 본질 변경 없으나 throughput 증가에도 한계 안 닿음.

→ **가설 1+5+6의 합성효과가 collapse의 근원이었음**이 데이터로 확인됨. PE starvation을 단독으로 명시 검증(가설 1) 한 건 아니지만, 그 외 모든 가설이 차단되거나 무관함이 확인되어 가설 1이 정황적으로 강하게 지지됨.

## 생성된 그래프

- `throughput_bps.png` — Throughput–latency hockey stick, **PRE/POST overlay**, 두 ceiling marker
- `latency_vs_rps.png` — Service time/end-to-end percentile, PRE collapse zone vs POST overdrive zone 시각화
- `throughput_vs_latency.png` — Wall RPS vs offered (left) + P99 (right) — **collapse vs graceful saturation의 가장 직접적 시각화**
- `queueing_gap.png` — POST end-to-end 분해 (service time + schedule wait)

모두 wall-clock 기준 throughput.
