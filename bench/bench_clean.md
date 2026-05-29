# DPUmesh vs TCP-sidecar Transport Benchmark

DPUmesh DMA transport 와 TCP service-mesh (Envoy sidecar) 의 raw 성능 비교 실험.
gateway / Thrift / application 로직을 모두 제거하고 transport 비용만 분리해
측정. 본 문서는 실험 설계와 결과, 그리고 cap 의 정체에 대한 attribution 을
정리.

---

## 1. 실험 대상

| | A안 (DPUmesh) | B안 (TCP via Envoy) |
|---|---|---|
| client → server hop | 1 (DPU) | 2 (sidecar1, sidecar2) |
| transport 위치 | DPU ARM + DPA EU (host CPU 외부) | host CPU 내 (app 과 core 공유) |
| L7 처리 | 없음 (transport-only) | 없음 (`tcp_proxy` filter 만) |
| 목적 | DMA-기반 mesh bypass | Istio/Envoy 모델 1:1 비교 |

핵심 가설: dpumesh 의 architectural advantage 는 transport 가 host CPU 외부
(DPU/DPA) 에서 일어난다는 점. 동일한 host CPU 예산 (각 1 core × 2 pod) 을
주면, TCP 측은 app 과 sidecar 가 core 를 나눠 쓰고 dpumesh 측은 app 이 1 core
를 통째로 쓸 수 있음.

---

## 2. Topology

```
═══════════════════════════════════════════════════════════════════════════════════
                    HOST NODE — Namespace: test-bench
                    governor=performance @ 2.5GHz fixed (cores 0-7)
═══════════════════════════════════════════════════════════════════════════════════

  ╔═════════════ A안 (dpumesh) ═════════════╗
  ║ core 0 ─ bench-dpumesh (workers≈rps/100) ║
  ║ core 1 ─ echo-dpumesh (ECHO_THREADS=64)  ║
  ║         comch + DMA (mlx5/PCI)           ║
  ╚═══════════════════════════════════════════╝
  ╔═════════════ B안 (tcp via Envoy) ═══════════════╗
  ║ core 2 ─ bench-tcp (bench + sidecar1, share)   ║
  ║ core 3 ─ echo-tcp  (sidecar2 + echo,  share)   ║
  ╚══════════════════════════════════════════════════╝

═══════════════════════════════════════════════════════════════════════════════════
              DPU (BlueField-3) — host CPU 외부      │ /dev/infiniband (PCI)
═══════════════════════════════════════════════════════════════════════════════════

  ARM (8 cores, pinning X)            DPA (FlexIO EU × 1)
  ┌──────────────────────────────┐    ┌────────────────────────────────┐
  │ dpumesh_dpu process          │    │ run_dma_manager (single EU)    │
  │  ▸ control PE (comch_server) │    │  ▸ drain_all_rings:            │
  │  ▸ consumer PE (DPA → ARM)   │◀──▶│      forward DMA (host→DPU)    │
  │  ▸ dpu_worker (routing/ACK)  │    │      reverse DMA (DPU→host)    │
  └──────────────────────────────┘    └────────────────────────────────┘
```

---

## 3. Resource Layout

CPU pinning 은 `taskset -apc <core> <pid>` 로 hard-pin. CFS quota 미사용
(core 정착이 핵심). `pin_pods()` 가 deploy 끝 / `pin` subcommand 에서 수행.

두 가지 profile:

### Profile = fair (TCP vs DPUmesh 비교용, default)
| core | pod | container(s) |
|------|-----|--------------|
| 0 | bench-dpumesh | bench_dpumesh (1 proc, 다중 worker thread) |
| 1 | echo-dpumesh | echo_dpumesh (ECHO_THREADS=64) |
| 2 | bench-tcp | bench_tcp + sidecar1 |
| 3 | echo-tcp | echo_tcp + sidecar2 |
| DPU | dpumesh_dpu | ARM 8 cores 자유 (pinning X) |
| DPA | run_dma_manager | EU × 1 |

### Profile = hw (HW 한계 측정용)
| core | pod | container(s) |
|------|-----|--------------|
| 0, 4 | bench-dpumesh | 2 cores (host lock 경합 완화) |
| 1, 5 | echo-dpumesh | 2 cores (ECHO_THREADS=64 분산) |
| 2, 3 | bench-tcp, echo-tcp | untouched (자원 비대칭 → 비교 의의 없음) |

DVFS: `cpupower -c 0-7 frequency-set -g performance -d 2.5GHz -u 2.5GHz`
(latency tail noise 제거).

---

## 4. K8s 객체 (test-bench namespace)

```
Deployments              Services             ConfigMaps
bench-dpumesh (1c)       bench-dpumesh:9092   sidecar1-config (upstream=echo-tcp:9091)
echo-dpumesh  (1c)       bench-tcp:9092       sidecar2-config (upstream=127.0.0.1:9092)
bench-tcp     (2c)       echo-tcp:9091
echo-tcp      (2c)

hostPath volumes (privileged):
  /dev/infiniband              ← bench-dpumesh, echo-dpumesh
  $BUILD_DOCA/lib (libthrift)  ← bench-dpumesh, echo-dpumesh
```

---

## 5. 실행 인터페이스

```bash
./test-bench.sh deploy                                 # build + image + DPU restart + pods Ready + fair pin
./test-bench.sh dpumesh    <RPS> <DUR> <SIZE> [<CONNS>]  # fair, dpumesh
./test-bench.sh tcp        <RPS> <DUR> <SIZE> [<CONNS>]  # fair, tcp
./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE> [<CONNS>]  # hw, dpumesh-only
./test-bench.sh pin        # = pin-fair
./test-bench.sh pin-hw
./test-bench.sh cleanup
```

dpumesh / tcp / dpumesh-hw 명령은 실행 직전 자동으로 해당 profile 로 재핀.

Ctrl protocol (line 기반):
```
RUN <rps> <dur_sec> <msg_size> [<conns>]
→ OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>
```

> bench daemon 의 raw 출력은 microseconds. 본 문서 표는 ms 통일.

---

## 6. 측정 방법론

### 6.1 wrk2 식 scheduled-time latency (CO 보정)

bench worker 가 closed-loop (send → wait_response → next) 라 그대로 두면 cap
region 에서 **coordinated omission** 발생 — 시스템이 느려지면 worker 도
같이 느려져서 큐잉이 latency 분포에 안 잡힘. cap 너머에서도 p50 ≈ p99
같은 가짜 plateau 가 보여 진짜 saturation 이 가려짐.

Fix: `t0 = scheduled_time` (보내기로 예약된 tick), `latency = now() - t0`.
worker 가 sleep_until 에서 늦게 깨면 그만큼 큐잉 wait 가 latency 에 그대로
잡혀서 elbow 가 깨끗이 보임. `bench_dpumesh.c` / `bench_tcp.go` 양쪽 적용.

### 6.2 MAX_WORKERS = 4096, 128 KB stack

closed-loop concurrency cap 이 낮으면 (이전 512) Little's law 로 in-flight
가 묶여 measure-able RPS 가 가짜 plateau. 4096 × 128 KB = 512 MB. CO 보정과
결합 시 latency tail / throughput 한계 모두 시스템 속성 직접 반영.

### 6.3 wall-time 측정

`bench_dpumesh.c` 는 watchdog thread + 즉시 join 패턴. main 이 고정 시간 sleep
하면 wall 이 부풀려져 RPS underreport.

### 6.4 측정 도구

| 도구 | 용도 |
|---|---|
| `test-bench.sh dpumesh <rps> <dur> <size> [<conns>]` | dpumesh RPS sweep |
| `run_bench.sh --method 0 ...` (test_dma/bench) | M2 baseline 측정 |
| `perf record -F 999 -g --call-graph dwarf` | flame graph |
| `perf trace -s -p <pid>` | syscall summary |
| `/opt/mellanox/doca/tools/dpa-statistics collect` | DPA EU 통계 |
| `top -H -p $(pgrep dpumesh_dpu)` | ARM thread CPU |
| DPA stat counters (in code) | polls/dma_copy ratio 실측 |

---

## 7. Transport Ceiling Baselines

`experiment/bench.md` 의 micro-bench (`/home/jukebox/test_dma/bench/`) — k8s ·
Thrift · TCP · gateway 모두 제거. host 가 dma_ring 에 desc 직접 post, comch
client + DPA RPC 만 사용. `host_worker.c` 의 단순 루프 (`while(true) {
get_next_dma_desc; desc->valid=1; }`) 로 baseline 으로 valid.

### 7.1 Method 0 / Method 2 측정 (8 KB / 60 s, 2026-05 재측정)

이전 측정 (M0=302K, M2=264K) 은 DOCA `comch_server.c` 의 per-msg
`DOCA_LOG_INFO` 가 stdio I/O 로 stale overhead 주입. test_dma 의 `run_bench.sh`
에 `-l 40` 추가 + stat 로그 WARN 으로 promote 해서 silence 한 결과:

| Mode | 구성 | dma_copy ops/sec | Throughput (1 dir) | vs M0 |
|---|---|---:|---:|---:|
| Method 0 (Only DMA) | `dma_copy` atomic (HW max) | **320,014** | **20.97 Gbps** | 100 % |
| Method 2 (DMA + completion) | `dma_copy` + DPU CPU 가 host 로 `server_send_msg` | **310,472** | **20.34 Gbps** | **97 %** |

Method 0 → Method 2 손실 3 % 만. "DPU CPU → host comch forward" 진짜 비용은
매우 작음. dpumesh 가 이 chain 을 그대로 사용하므로 **Method 2 = dpumesh 의
transport ceiling 의 direct baseline**.

### 7.2 micro-bench vs dpumesh chain 차이 (caveat)

| | micro-bench (Method 2) | dpumesh |
|---|---|---|
| 데이터 흐름 | host → DPU → host (단방향, 1 endpoint) | host A → DPU → host B → DPU → host A (RTT, 2 endpoints) |
| DPU ARM | comch 수신 + `server_send_msg` 한 번 | comp_queue enq + **pod_idx 라우팅 + TX_ACK + reverse desc 작성 + tx_ring enq** |
| descriptor 라우팅 | 없음 | `dst_pod_id` 별 분기 |
| reverse direction | 없음 | 매 hop reverse DMA enq |

→ 같은 "1 dma_copy" 라도 dpumesh 는 DPU ARM 부수 작업 多. 1 RTT = forward
DMA × 2 + reverse DMA × 2 = **4 dma_copy ops**.

---

## 8. TCP/Envoy vs DPUmesh — Direct Comparison

### 8.1 단일 부하점 측정 (rps=40000, dur=10s, size=8192B)

| | dpumesh | tcp/Envoy |
|---|---|---|
| Target RPS | 40,000 | 40,000 |
| **Achieved RPS** | **39,732.7** (99.3 %) | **33,097.0** (82.7 %) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 latency | **4.53 ms** | **661.50 ms** |
| p99 latency | **8.35 ms** | **3,885.30 ms** |
| p999 latency | 8.62 ms | 4,588.43 ms |
| Throughput (RTT) | 620.82 MB/s | 517.14 MB/s |

40 K target 에서 dpumesh 는 healthy region (52 K sustainable 의 76 %), TCP/Envoy
는 deep overload (achieved 33.1 K, p99 3,885 ms). TCP/Envoy saturation 은
40 K 한참 아래.

---

## 9. DPUmesh Saturation Sweep (8 KB, 10 s, conns=auto)

### 9.1 Baseline sweep (in-place forwarding **전**)

| Target RPS | Achieved RPS | p50 (ms) | p99 (ms) | p999 (ms) | Throughput (RTT) | OK / Fail |
|---:|---:|---:|---:|---:|---:|---|
| 5,000 | 4,968.6 | 1.10 | 1.93 | 2.01 | 77.63 MB/s | 50,000 / 0 |
| 10,000 | 9,939.2 | 1.60 | 2.82 | 2.94 | 155.30 MB/s | 100,000 / 0 |
| 15,000 | 14,906.5 | 2.08 | 3.72 | 3.88 | 232.91 MB/s | 150,000 / 0 |
| 20,000 | 19,875.0 | 2.57 | 4.64 | 6.48 | 310.55 MB/s | 200,000 / 0 |
| 25,000 | 24,838.1 | 3.06 | 5.55 | 5.80 | 388.10 MB/s | 250,000 / 0 |
| 30,000 | 29,807.8 | 3.54 | 6.51 | 6.74 | 465.75 MB/s | 300,000 / 0 |
| 35,000 | 34,767.7 | 4.05 | 7.45 | 7.73 | 543.24 MB/s | 350,000 / 0 |
| 40,000 | 39,732.7 | 4.53 | 8.35 | 8.62 | 620.82 MB/s | 400,000 / 0 |
| 45,000 | 44,691.3 | 5.02 | 9.29 | 9.63 | 698.30 MB/s | 450,000 / 0 |
| 50,000 | 49,664.7 | 5.41 | 11.89 | 12.17 | 776.01 MB/s | 500,000 / 0 |
| **52,000** | **51,644.8** | **5.53** | **12.16** | **12.33** | **806.95 MB/s** | 520,000 / 0 |
| 55,000 | 53,553.4 | 74.14 | 189.59 | 194.61 | 836.77 MB/s | 550,000 / 0 |
| 57,000 | 53,404.0 | 319.63 | 604.52 | 611.89 | 834.44 MB/s | 570,000 / 0 |
| 60,000 | 53,698.8 | 587.51 | 1,154.25 | 1,166.28 | 839.04 MB/s | 600,000 / 0 |
| 65,000 | 53,841.1 | 1,043.73 | 1,961.63 | 1,983.88 | 841.27 MB/s | 650,000 / 0 |

### 9.2 핵심 관찰

- **Sustainable saturation ≈ 52 K RPS** (p50 5.5 ms, p99 12 ms). 50 K 까지 거의
  perfectly linear (50 K → 49.7 K, 99.3 %).
- **Elbow = 52 K → 55 K 사이**. 55 K target 에서 p99 가 12 → 190 ms **16 배**
  점프 (전형적 M/M/1 saturation).
- **Overload throughput ceiling ≈ 53.5 K RPS** — 60/65 K target 으로 더 밀어도
  achieved 53.7 K 에 고정. latency 만 발산 (60 K: 1,154 ms, 65 K: 1,962 ms).
- **0 failure** — `WAIT_TIMEOUT_MS = 5,000 ms` 보다 worst-case latency 작아 미발동.
  4-layer flow control (tx_alloc 무한 대기, enqueue 백오프, reverse-path admission
  gate) 이 backpressure 만으로 흡수.
- **Throughput plateau 836–841 MB/s** — DMA chain 53.5 K × 4 dma_copy = 214 K
  ops/s 에서 hard cap.

### 9.3 baseline 의 ceiling 위치

```
   100% ┃ ┌── HW max (Method 0, dma_copy atomic) ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)        ── 310,472 ops/s = 20.34 Gbps
    67% ┃ └── dpumesh @ overload ceiling          ── 214,212 ops/s = 13.71 Gbps
        ┃     (53,553 RPS × 4 dma_copy)
```

| | dma_copy ops/sec | vs M0 | vs M2 |
|---|---:|---:|---:|
| Only DMA (HW max) | 320,014 | 100 % | — |
| DMA + completion | 310,472 | 97 % | 100 % |
| **dpumesh @ overload (53.55 K RPS)** | **214,212** | **67 %** | **69 %** |
| dpumesh @ sustainable (51.64 K RPS) | 206,580 | 65 % | 67 % |

**의미 있는 비교 = 69 % vs Method 2** (동일 chain 구조).

---

## 10. Cap 의 정체 — DPA EU per-dma_copy work

§7 baseline 대비 dpumesh 의 31 % gap 원인 규명. 여러 후보를 직접 측정으로
검증 또는 반증.

### 10.1 DPA EU stat — ARM 99.9 % 인데 EU 99.5 % idle

```
sudo /opt/mellanox/doca/tools/dpa-statistics collect -d mlx5_0 -t 10000
```

50 K RPS 부하, 10 s 윈도우:

| 항목 | 값 | 해석 |
|---|---:|---|
| Wall time | 10,000 ms | 측정창 |
| **EU active time** | **51.5 ms** (ticks @ 1 ns) | EU 가 실제 실행한 시간 |
| **EU active %** | **0.51 %** | 99.49 % idle |
| Cycles | 92.7 G | 51.5 ms × ~1.8 GHz 일치 |
| Instructions | 7.22 G | IPC = 0.078 (memory/DMA stall — 정상) |
| Executions | 7,314 (= 731/s) | DPA thread 깬 횟수 |
| Cycles/execution | 12.7 M | wake 당 평균 7 µs 실행 |
| dma_copy/execution | ~273 | wake 당 batch size |

DPA 는 burst 처리 — wake → 273 dma_copy 일괄 → re-schedule.

### 10.2 ARM perf 분석 (50 K RPS 부하)

`perf report --no-children` self-time:

| 카테고리 | dpumesh | Method 0 | Method 2 |
|---|---:|---:|---:|
| **epoll_pwait syscall 군집** (kernel + libc + vdso) | **64 %** | 65 % | 62 % |
| DOCA infra (CQ poll, comch internals) | 7 % | 13 % | 19 % |
| Atomics (CAS, swap, mutex) | 4 % | 2 % | 3 % |
| **App 코드** (run_dpu_worker, process_*, drain_*) | **5.5 %** | 3.4 % | 2.8 % |

per-op time 환산:

| | per-op time | Δ vs M2 |
|---|---:|---:|
| Method 0 | 3.13 µs | — |
| Method 2 | 3.22 µs | baseline |
| **dpumesh** | **4.67 µs** | **+1.45 µs (+45 %)** |

세 측정 모두 syscall 1순위. 하지만 dpumesh 만 throughput 낮음 — ARM 폴링
자체로는 설명 불가.

### 10.3 ARM CPU 분포 — useful work 분해

`bench/dpumesh_dpu_flame_per_batch.svg` 기준:

| ARM 함수 | self-time % | per-event 환산 |
|---|---:|---:|
| `doca_pe_progress` (epoll_pwait 포함) | 76.5 % | 3.48 µs (polling syscall) |
| `process_completion_queue` | 7.95 % | 0.36 µs |
| `process_rev_notify_entry` | 5.18 % | 0.24 µs |
| `find_pod_by_id` | 2.80 % | 0.13 µs |
| `server_send_msg_to_conn` | 2.20 % | 0.10 µs |
| comp_queue ops (peek/empty/full/dq/eq) | ~3.5 % | 0.16 µs |
| `process_forward_entry` | 0.91 % | 0.04 µs |
| `drain_deferred_tx_acks` | 0.54 % | 0.02 µs |
| **ARM useful work 합계** | **~23 %** | **~1.05 µs/event** |

ARM 과 DPA EU 는 **다른 processor 가 병렬 작동**. cap = max(DPA per-op, ARM
per-event):
- DPA per-dma_copy: **4.55 µs**
- ARM per-event: **1.05 µs**
- → **DPA 가 cap, ARM 은 ~3.5 µs 헤드룸**

ARM 측 최적화는 throughput 에 영향 없음.

### 10.4 DPA polling counter — 결정적 측정 (Experiment X)

가설 갈음:
- 가설 A: dpumesh poll/copy ratio = 2~4 → polling 이 갭의 ~75 %
- 가설 B: dpumesh ratio ≈ 1.0 → polling 무관, 갭은 DPA code work

DPA kernel + `dpa_thread_arg` 에 counter 3 개 추가:
```c
volatile uint64_t stat_inner_iters;   /* drain_all_rings inner do-while */
volatile uint64_t stat_polls;         /* PCIe desc->valid reads */
volatile uint64_t stat_dma_copies;    /* dma_copy chunks issued */
```

DPU ARM 이 1 초마다 `doca_dpa_d2h_memcpy` 로 읽어 ratio 계산.

**측정 결과 (50K RPS, 15s)**:
```
DPA stat: iters=50858 polls=203428 copies=200052  poll/copy=1.017
DPA stat: iters=50799 polls=203196 copies=200033  poll/copy=1.016
DPA stat: iters=50816 polls=203268 copies=200000  poll/copy=1.016
DPA stat: iters=50910 polls=203636 copies=200012  poll/copy=1.018
DPA stat: iters=50876 polls=203504 copies=199999  poll/copy=1.018
```

분해:
- `drain_all_rings` inner iter ~50,800/s
- iter 당 4 polls (forward × 2 pods + reverse × 2 pods) → 203 K polls/s
- iter 당 평균 ~4 dma_copies → 200 K dma_copies/s
- **poll/copy ratio = 1.017** ≈ M2 baseline 의 1.000

→ **가설 B 확정**. 4 rings 폴링이 4 dma_copies 발사로 perfectly amortize.
polling 은 dpumesh-vs-M2 갭의 원인 아님.

### 10.5 cap 정체 (정정된 모델)

| | M2 baseline | dpumesh | Δ |
|---|---:|---:|---:|
| dma_copy/s | **325K** (또는 321K, §11.2 참조) | **220K** | -105K |
| Per-dma_copy 시간 | **3.08 µs** | **4.55 µs** | **+1.47 µs** |
| PCIe polls / dma_copy | ~1.0 | **1.017 (측정)** | 0 |

+1.47 µs/dma_copy 갭은 **DPA EU 의 추가 코드 작업**. M2 의 `poll_desc_ring_dma_copy`
에 없는 dpumesh `process_one_desc` 의 작업:

| 항목 | M2 | dpumesh | 추정 EU 비용 |
|---|---|---|---:|
| validation (mmap=0, size=0, range) | 없음 | 3 checks + range arithmetic | ~0.1-0.2 µs |
| chunking loop (`while offset < total`) | 없음 (단일 dma_copy) | loop + ALIGN_UP_128 + chunk type 선택 | ~0.2-0.3 µs |
| desc 필드 clear 후 writeback × 3 | 1× writeback | 3× writeback | ~0.3-0.5 µs |
| `comch_dma_comp_msg` 필드 수 | 3 (type/pos/length) | 6 (+ req_id/src_pod_id/dst_pod_id/flags) | ~0.1 µs |
| `ensure_producer_slot` 호출 | 매 500 ops lazy drain | 매 chunk + counter check | ~0.2 µs |
| reverse rings 분기 / `dpa_sent_count` 증가 | 없음 | 매 reverse desc | ~0.1 µs |
| **합계 추정** | — | — | **~1.0-1.4 µs** |

추정 합계 ≈ 실측 갭 1.47 µs.

---

## 11. Feature 단계별 측정 — 정량 attribution

DPA kernel work 가설을 두 방향에서 검증: (A) dpumesh 에서 feature 제거,
(B) M2 에 feature 추가.

### 11.1 DPUmesh feature strip (8 KB, 10 s, conns=auto)

3 개 compile-time toggle:
```c
#define STRIP_VALIDATION   /* mmap=0, size=0, range, padded>buf 모두 #ifdef out */
#define STRIP_DESC_CLEAR   /* 3× writeback → 1× writeback (valid=0 만) */
#define STRIP_CHUNK_LOOP   /* size ≤ 8KB single dma_copy fast path */
```

| Stage | 50K ach / p99 | 55K ach / p99 | **65K ach / p99** | DMA tput @ 65K |
|---|---:|---:|---:|---:|
| Baseline (per-batch only) | 49,629 / 9.89 ms | 54,080 / 91.06 ms | **54,291 / 1,894 ms** | 847 MB/s |
| Exp A (+VALIDATION) | 49,721 / 10.04 ms | — | 55,207 / 1,704 ms | 862 MB/s |
| Exp A+B (+DESC_CLEAR) | — | — | 56,134 / 1,545 ms | 877 MB/s |
| **Exp A+B+C (+CHUNK_LOOP)** | **49,659 / 9.36 ms** | **54,617 / 12.27 ms** | **59,195 / 952 ms** | **925 MB/s** |

단계별 기여:
```
Baseline overload ceiling:                 54,291 RPS
   ↓ (+1.7 %)
Exp A — STRIP_VALIDATION:                  55,207 RPS   (+916 RPS)
   ↓ (+1.7 %)
Exp A+B — +STRIP_DESC_CLEAR:               56,134 RPS   (+927 RPS)
   ↓ (+5.5 %)
Exp A+B+C — +STRIP_CHUNK_LOOP:             59,195 RPS   (+3,061 RPS)
─────────────────────────────────────────
Total +9.0 % (54,291 → 59,195 RPS)
```

**Chunking bypass 가 단일 항목 최대 기여**. 이유: chunking loop 안에
`ensure_producer_slot` + `is_consumer_empty` wait + `producer_slots_inflight++`
가 들어있어 1 chunk dma_copy 도 모든 체크 발동. fast path 는 1번씩만.

**Elbow 이동**: baseline 55 K → A+B+C 적용 후 55 K 가 여전히 sustainable
(p99 12.27 ms, baseline 의 52 K 와 동일 profile). p99 가 1/8 (91 → 12 ms).

안정성 (3 회 back-to-back 50K, slot leak 검증):
```
=== run 1 === Achieved RPS: 49342.6  p99: 9381.5 us  OK/Fail: 250000/0
=== run 2 === Achieved RPS: 49353.8  p99: 9347.9 us  OK/Fail: 250000/0
=== run 3 === Achieved RPS: 49344.8  p99: 9329.5 us  OK/Fail: 250000/0
```

ceiling 위치 갱신:
```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
    74% ┃ ├── dpumesh @ Exp A+B+C            ── 236,780 ops/s = 15.13 Gbps
        ┃ │     (59,195 RPS × 4 dma_copy)
    68% ┃ └── dpumesh @ baseline (in-place)   ── 218,992 ops/s = 14.02 Gbps
```

per-dma_copy 시간:
- Baseline: 4.57 µs / dma_copy
- Exp A+B+C: 4.22 µs / dma_copy (-0.35 µs)
- M2: 3.08 µs / dma_copy

### 11.2 M2 에 feature 추가 (Inverse, 8 KB, Method 0, H2D, 10 s)

4 개 toggle:
```c
/* #define ADD_VALIDATION */    /* 4 checks: mmap=0, size=0, range, padded>buf */
/* #define ADD_CHUNK_LOOP */    /* while(offset < total) wrapper */
/* #define ADD_PRODUCER_SLOT */ /* lazy 500-drain → per-call slot check + counter */
/* #define ADD_DESC_CLEAR */    /* 4-field clear + 3× writeback after dma_copy */
```

| Stage | recv/s | Bandwidth | Δ vs prev |
|---|---:|---:|---:|
| **E0 baseline** | **321,803** | 21.08 Gbps | — |
| E1 +ADD_VALIDATION | 324,707 | 21.27 Gbps | +0.9 % (noise) |
| E2 +ADD_CHUNK_LOOP | 319,495 | 20.93 Gbps | **-1.6 %** |
| E3 +ADD_PRODUCER_SLOT | 303,173 | 19.86 Gbps | **-5.1 %** |
| E4 +ADD_DESC_CLEAR | **17,639** | 1.15 Gbps | **-94.2 %** ❗ |

E4 collapse isolation 측정:

| Variant | recv/s | 비고 |
|---|---:|---|
| E4-alone (DESC_CLEAR only) | 17,510 | 다른 toggle 무관 |
| E4-min (`valid=0` 1줄 + writeback 1번만) | 17,225 | writeback 자체가 catastrophic |

**Architectural finding — desc clear 의 100× 비대칭**:

M2 의 ring 은 **lossy** — host 가 valid 안 체크하고 blind write:
```c
struct dma_desc *get_next_dma_desc(struct dma_ring *ring) {
    struct dma_desc *desc = ring->descs + ring->head;
    ring->head = (ring->head + 1) % ring->size;
    return desc;   /* no valid check */
}
```

dpumesh 는 **lossless** — host 가 `valid==1` 이면 NULL 반환, DPA 가 valid=0
clear 할 때까지 대기.

DPA 의 `__dpa_thread_window_writeback()` → host memory 의 같은 cache line 에
PCIe 쓰기. M2 host 는 동시에 같은 cache line write (`desc->addr/size/valid=1`).
→ **양쪽 동시 쓰기 = PCIe cache coherency cost 폭증**.

dpumesh 는 host 가 valid=0 기다림 → cache line 충돌 없음 → 같은 writeback
cheap.

### 11.3 추가 feature (E5-E10, polling amortization sanity check 포함)

| Variant | recv/s | Δ vs E0 | per-iter time | 비고 |
|---|---:|---:|---:|---|
| **E0 baseline** | **321,803** | **0** | 3.11 µs | 1 poll : 1 op |
| E5 +ADD_LARGER_COMP_MSG | 294,207 | -8.6 % | 3.40 µs (+0.29 µs) | 3 → 7 fields (12B → 24B immediate) |
| E6 +ADD_HANDLE_MSGS | 315,513 | -2.0 % | 3.17 µs (+0.06 µs) | `consumer_get_completion` 1× per iter |
| E7 +ADD_EXTRA_CONSUMER_WAIT | 322,857 | +0.3 % (noise) | 3.10 µs | 사실상 free |
| E8 +ADD_REVERSE_DMA | 381,826 | +18.6 % | 5.24 µs / 2 op | **1 poll : 2 op** ← amortize |
| E9 +ADD_4_DMA_COPIES | 417,119 | +29.6 % | 9.62 µs / 4 op | **1 poll : 4 op** ← 더 amortize |
| E10 +ADD_4_DMA_COPIES +EXTRA_RING_POLLS=3 | 338,322 | +5.1 % | 11.83 µs / 4 op | **4 poll : 4 op** ← dpumesh ratio |

E8/E9 의 throughput 상승은 dpumesh ceiling 이 아니라 polling amortization:
```
per_iter_time = α + N × β  (N = dma_copies/iter, 1 poll/iter 고정)
3.11 = α + 1 × β  (E0)
5.24 = α + 2 × β  (E8)
9.62 = α + 4 × β  (E9)
⇒ β ≈ 2.13 µs/dma_copy (marginal cost)
  α ≈ 0.98 µs (per-iter fixed overhead)
```

- M2 per-iter overhead = **0.98 µs**
- dma_copy 1 개 marginal cost = **2.13 µs**

E10 (4 poll : 4 op = dpumesh 패턴) **338,322 recv/s** — E0 와 +5 % 차이.
→ dpumesh 의 1:1 poll:op 패턴에서는 amortization 이득 거의 없음. **비교
baseline 으로 E0 사용이 정직**.

### 11.4 추가 sanity checks

#### E11 — PCIe direction balance (2H+2D vs 3H+1D)

| Variant | recv/s | per-op time |
|---|---:|---:|
| E10 (3H+1D + 4 polls) | 338,322 | 2.96 µs |
| **E11 (2H+2D + 4 polls)** | **337,262** | **2.97 µs** |

Δ = -0.3 % (noise). **PCIe direction balance 영향 없음** — PCIe full-duplex 라
forward + reverse 가 같은 BW lane 공유 안 함. "PCIe BW contention" 가설 부정.

#### Inner pe_progress 제거 (per-entry → per-batch)

`process_completion_queue` 의 entry-당 `pe_progress` × 2 호출 → batch 끝 1회.

| Target | Baseline ach / p99 | Per-batch ach / p99 | Δ ach | Δ p99 |
|---:|---:|---:|---:|---:|
| 50K | 49,655 / 9.94 ms | 49,629 / 9.89 ms | -0.05 % | -0.5 % |
| 55K | 54,085 / 101.39 ms | 54,080 / 91.06 ms | -0.01 % | **-10.2 %** |
| 65K | 54,749 / 1,768 ms | 54,291 / 1,894 ms | -0.8 % | +7.1 % |

Sustainable RPS **변화 없음**. Flame 비교:

| 항목 | inplace | per_batch |
|---|---:|---:|
| `doca_pe_progress` subtree | 67.5 % | **76.5 %** ↑ |
| `__GI_epoll_pwait` | 46.0 % | **54.5 %** ↑ |
| `process_completion_queue` | 19.8 % | **7.95 %** ↓ |

`process_completion_queue` self-time 12pp 감소분 = `epoll_pwait` 로 이동.
**ARM polling 횟수를 줄여도 polling 시간 비중은 안 줄어듦** → ARM 이 cap 아님.

#### epoll_pwait per-call 측정 (perf trace, 50K RPS, 15s)

```
syscall          calls      total(ms)   avg(ms)    min       max
epoll_pwait     3,122,730   6,703.520   0.002      0.001     0.109
```

- 호출 횟수: 208 K calls/s
- 호출당 평균: **2 µs** (syscall enter/exit 고정 비용)
- 총 `epoll_pwait` 시간 / wall = 44.7 % (flame 의 ~45-55 % subtree 와 일치)

avg 2 µs 는 block 깊게 자는 게 아니라 syscall 고정 비용. M2 baseline 도 같은
41 % epoll 비용 무는데 310K events/s 도달 → **epoll overhead 자체는 dpumesh-vs-M2
갭의 원인 아님**.

#### Load generator 검증 (system vs load-gen)

| Target × workers | achieved | p99 |
|---|---:|---:|
| 65K × 650 (default) | 54,291 | 1,894 ms |
| **65K × 1000** | **54,199** | **1,892 ms** |
| **100K × 1500** | **54,227** | **6,814 ms** |
| 65K × 4096 | **hang** — TX slot pool (2048) 충돌 | n/a |

650 → 1500 워커 변화에도 ach 가 54.2 K 에 fix. **system ceiling 확정**.
4096 워커는 `DMA_RING_SIZE = 2048` 동시성 한계로 hang (load-gen overpush).

#### M2 synthetic multi-ring polling (EXTRA_RING_POLLS)

매 dma_copy 마다 N 개 추가 `desc->valid` PCIe read 삽입 (별도 cache line):

| EXTRA_RING_POLLS | 총 rings 폴링 | recv/s | Bandwidth | Δ vs N=0 |
|---:|---:|---:|---:|---:|
| 0 (baseline) | 1 | **325,361** | 21.32 Gbps | — |
| 1 | 2 | 236,660 | 15.50 Gbps | **-27 %** |
| 3 | 4 | 175,421 | 11.49 Gbps | **-46 %** |
| 7 | 8 | 114,543 | 7.50 Gbps | **-65 %** |

monotonic. per-extra-poll cost ≈ 0.8-1.1 µs / dma_copy. dpumesh 220K 가
M2-N=3 (synthetic 4 rings) 의 175K 보다 **높은** 이유 — synthetic 은 매 dma_copy
마다 N reads 무조건 발사, dpumesh `drain_all_rings` 은 한 inner iter 에서
4 rings 폴링 + 0~4 dma_copy → saturation 시 amortize (§10.4 의 ratio 1.017
와 일치).

---

## 12. In-place Forwarding 변경 (2026-05-08)

§10.5 의 우선순위 4 항목 중 가장 큰 ratio (10-15 % 예측) 의 **`process_forward_entry`
8 KB staging memcpy 제거**.

### 12.1 변경 사항

| 파일 | 변경 |
|---|---|
| `object.h` | `pod_state.local_mmap_dpa_handle` 필드 추가 (uint32_t — SDK 일치) |
| `dpa.c` | `setup_pod_dma` 에서 DPA mmap handle 캐싱 |
| `dpu_worker.c` | `dpu_enqueue_reverse_dma` signature (memcpy / write_pos 제거); `process_forward_entry` 성공 path TX_ACK 제거 (에러 path 만); `process_rev_notify_entry` 에 reverse 완료 후 TX_ACK 추가; `send_or_defer_tx_ack` 헬퍼 |
| `device/dpa_kernel.c` | reverse handler 가 `desc->mmap` / `desc->addr` 를 source 로 사용. `desc->mmap == 0` 이면 legacy `dpu_mmap` fallback |

**Slot lifecycle 변화**: src `dma_buffer` slot 점유 시간이 forward-only → **full
RTT**. host TX slot 도 reverse 완료 후 TX_ACK 받아야 release. sustainable
52 K @ 5.5 ms RTT = ~290 in-flight slots, `DMA_RING_SIZE = 2048` 안에 fit.

**Trade-off**: dst pod 별 staging buffer 가 사라지면서 한 src 가 여러 dst 로
보낼 때 **HOL blocking across destinations from same source**. echo bench
(src==dst) 영향 없음 — multi-dst 워크로드에서만 manifest (사용자 합의).

### 12.2 RPS sweep 비교 (8 KB, 10 s, conns=auto)

| Target | Baseline ach | New ach | Baseline p99 | New p99 | Δ p99 |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 4,968.6 | 4,968.8 | 1.93 ms | 1.92 ms | -0.5 % |
| 10,000 | 9,939.2 | 9,935.9 | 2.82 ms | 2.78 ms | -1.4 % |
| 30,000 | 29,807.8 | 29,800.9 | 6.51 ms | 6.38 ms | -2.0 % |
| 50,000 | 49,664.7 | 49,655.3 | 11.89 ms | 9.94 ms | -16.4 % |
| 52,000 | 51,644.8 | 51,625.3 | 12.16 ms | 12.12 ms | -0.3 % |
| **53,000** | — | **52,641.9** | — | **12.23 ms** | 새 sustainable |
| **54,000** | — | **53,636.3** | — | **12.38 ms** | 새 sustainable |
| 55,000 | 53,553.4 | 54,085.4 | 189.59 ms | 101.39 ms | **-46.5 %** |
| 60,000 | 53,698.8 | 54,198.6 | 1,154.25 ms | 998.98 ms | -13.5 % |
| 65,000 | 53,841.1 | 54,748.5 | 1,961.63 ms | 1,767.98 ms | -9.9 % |
| 70,000 | — | 54,369.7 | — | 2,817.77 ms | overload |

**결과**:
- Sustainable 52 K → **54 K** (+~4 %, p99 < 13 ms)
- Overload ceiling 53,553 → **54,748** (+~2 %)
- Overload p99 약 절반 (55 K: 190 → 101 ms)
- 0 failure, 11 회 sequential test slot leak 없음

새 ceiling:
```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
    67% ┃ │   dpumesh @ baseline overload    ── 214,212 ops/s = 13.71 Gbps
    68% ┃ └── dpumesh @ new overload         ── 218,992 ops/s = 14.02 Gbps
        ┃         (54,748 RPS × 4 dma_copy, +2.2 %)
```

| | dma_copy ops/sec | vs Method 0 | vs Method 2 |
|---|---:|---:|---:|
| dpumesh baseline @ overload (53,553 RPS) | 214,212 | 67 % | 69 % |
| **dpumesh new @ overload (54,748 RPS)** | **218,992** | **68 %** | **71 %** |
| dpumesh new @ sustainable (53,636 RPS) | 214,544 | 67 % | 69 % |

---

## 13. Yield/Wake Architecture Cost — M2 에 wake 메커니즘 이식

§10.5 의 "DPA scheduling overhead" 후보 (~0.4-0.6 µs/op 추정) 를 직접 측정.

### 13.1 1단계 — M2 + naive yield (wake source 없음)

M2 DPA kernel 에 `thread_reschedule()` 추가 (273 dma_copy 마다).

**측정: 273 recv/s** — 정확히 1 burst 처리 후 영원히 hang.

→ `doca_dpa_dev_thread_reschedule()` 는 **진짜 yield**. 외부 wake source 없으면
DPA 가 깨어나지 못함. cooperative 아니라 **block 형 suspend**.

### 13.2 2단계 — M2 + yield + wake (DPU 1 kHz keepalive 이식)

dpumesh 의 wake 메커니즘 모방:
- M2 DPU worker 에 `dmesh_doca_dpa_msgq_send` 로 매 1 ms trigger msg 발사
- M2 DPA kernel 의 yield 직전 consumer comp queue 드레인 + ack
- yield counter 를 file-scope global 로 (function-local static 은 thread re-entry
  시 reset 됨 — 아래 발견 참조)

| Variant | recv/s | per-op time | Δ vs E0 |
|---|---:|---:|---:|
| E0 (M2 busy-spin) | 321,803 | 3.11 µs | — |
| **M2 + yield + 1 kHz wake (burst=273)** | **272,928** | **3.66 µs** | **+0.55 µs = -15.2 %** |
| M2 + yield + 1 kHz wake (burst=1000) | 249,916 | 4.00 µs | +0.89 µs = -22.3 % |

### 13.3 발견 — DPA thread re-entry 시 function-local static reset

처음 시도: `static int yield_counter`. 결과 999 recv/s. 분석: DPA thread 가
`thread_reschedule()` 후 **함수 entry 부터 restart**. function-local static 재초기화
→ 매 wake 마다 1 dma_copy 후 다시 yield. 해결: file-scope global + function
entry 에서 reset.

### 13.4 Yield 비용의 정체

burst 273 (=849 µs 처리) 와 wake interval 1 ms (=1000 µs) mismatch 가 비용
주된 원인. burst 1000 (=3110 µs) 으로 늘리면 wake interval 보다 길어져 다른
mismatch → -22 % 더 악화.

dpumesh 의 731 exec/s × 273 ops/exec = 200K 도 같은 패턴. 비용 줄이려면:
- Wake rate 를 burst 처리 시간에 매칭 (250 Hz wake + 1000 op burst)
- 또는 wake-on-event (host trigger desc post 마다)

dpumesh 는 실제로 host trigger 도 함께 사용 (`dpumesh_enqueue` 가 DPU trigger).
1 kHz keepalive 는 idle fallback. multi-source wake 가 dpumesh ~200K 도달의 이유.

---

## 14. DPUmesh Busy-Spin — Architectural Necessity 검증

### 14.1 §11.1 strips **전** busy-spin (yield 제거)

| Target | per-batch ach / p99 | **DPA busy-spin** ach / p99 |
|---:|---:|---:|
| 50K | 49,629 / 9.89 ms | 49,605 / 9.54 ms |
| 55K | 54,080 / 91.06 ms | **53,562 / 224.4 ms** |
| 65K | 54,291 / 1,894 ms | 53,398 / 2,064 ms |

55K p99 91 → 224 ms (**2.5 배 악화**). dpumesh 4 rings × busy-spin = PCIe poll
bandwidth 가 실제 DMA traffic 과 경쟁. yield 가 **PCIe polling rate throttle**
역할.

### 14.2 §11.1 strips **후** busy-spin 재시도

가설: strips 가 DPA EU per-dma_copy work 줄였으니 PCIe BW 헤드룸 생겨 결과 다를 수 있음.

| Target | yield 유지 (§11.1 baseline) | **busy-spin (재시도)** |
|---|---:|---:|
| 50K | 49,659 / 9.36 ms | **49,614 / 8.67 ms** |
| 55K | 54,617 / 12.27 ms | **54,628 / 9.61 ms** |
| 65K | 59,195 / 952 ms | **59,784 / 819 ms** |

표면적으로는 모든 target p99 개선 (50K -7 %, 55K **-22 %**, 65K -14 %).

### 14.3 Back-to-back stability check — catastrophic failure

```
=== run 1 (55K) ===  Achieved 0.0 RPS, OK/Fail 0/1650
=== run 2 (55K) ===  (empty — daemon 응답 없음)
=== run 3 (55K) ===  (empty)
```

bench-dpumesh pod log:
```
[WRN][ring.c:96][get_next_dma_desc] DMA ring busy at head=160 (size=2048)
... 무한 반복
```

**Slot leak — host ring `head=160` slot 이 `valid==1` 채로 stuck**.

### 14.4 Code trace — recovery 로직 0

```c
struct dma_desc *get_next_dma_desc(struct dma_ring *ring) {
    struct dma_desc *desc = ring->descs + ring->head;
    if (desc->valid) {
        DOCA_LOG_WARN("DMA ring busy at head=%u (size=%u)", ring->head, ring->size);
        return NULL;           /* ← head 진행 안 함 */
    }
    uint32_t next_head = (ring->head + 1) % ring->size;
    ring->head = next_head;
    return desc;
}
```

```c
/* bench_dpumesh.c worker */
if (dpumesh_enqueue(g_ctx, &desc) < 0) {
    atomic_fetch_add(w->fail, 1);
    continue;             /* 같은 stuck slot 무한 재시도 */
}
```

Recovery 로직 진짜 0:
- ❌ Stuck slot timeout
- ❌ Force-clear (host 가 valid=0 강제)
- ❌ head++ skip (lossy 진입)
- ❌ Ring reset
- ❌ Backoff + retry escalate

설계가 "DPA reliable" 가정.

### 14.5 Leak mechanism (logical trace)

DPA 의 `process_one_desc` 는 모든 종료 path (success / abort / consumer_empty
timeout) 에서 `desc->valid = 0` + writeback. logic 만으론 leak 불가.

가능한 race:
1. **DPA window cache stale read** — host 의 `valid=1` write 가 coherency 통해
   DPA window cache line invalidate 해야 하는데, busy-spin 의 PCIe + coherency
   부담으로 invalidation 누락/지연. DPA 가 stale `valid=0` 봄 → desc_idx
   advance 안 함 → 그 slot 영원히 skip.
2. **DPA → host writeback propagation 누락** — 5+ 초 갭이면 propagation 시간
   충분 → less likely.

→ (1) 이 plausible 한 유일 path. 정상 coherency 라면 발생 안 해야 하지만
busy-spin stress 에서 corner case 발생.

### 14.6 종합 — yield 는 architectural necessity

| | §14.1 (strips 전) | §14.2-14.3 (strips 후) |
|---|---|---|
| Single-run | 55K p99 91 → **224 ms 악화** | 55K p99 12 → **9.6 ms 개선** |
| Back-to-back 안정성 | 미측정 | **fail (slot leak)** |
| 결론 | busy-spin 안 좋음 (PCIe poll storm) | busy-spin 안 좋음 (slot leak) |

dpumesh 가 **multi-pod / multi-ring / lossless** 인 한 yield 불가피:
- multi-ring busy-spin = PCIe + coherency stress
- coherency race = `valid` bit stuck 가능성
- lossless ring 모델 = stuck slot 가 hang 으로 표면화 (M2 lossy 면 그냥 덮어씀)
- **yield 의 ~0.55 µs/op 비용 (§13) 은 이 architectural 보호의 가격**

§11.1 strips 로 +9 % 회복했지만 busy-spin 으로 더 짜내면 stability 잃음.
**(yield + strips) 가 production 최적점**.

---

## 15. Flow Control Audit

dpumesh flow control 은 7 layer + 3 retry queue:

| # | Layer | 위치 | 메커니즘 | 크기 | 필수성 |
|---|---|---|---|---:|---|
| 1 | DPA producer slot | DPA EU | `ensure_producer_slot` spin + completion drain | 1024 | 필수 |
| 2 | DPA consumer recv | DPA EU | `is_consumer_empty` spin (~0x200K loops) | 8192 | 필수 |
| 3 | DPU comp_queue | DPU ARM | ring + BP_HIGH (3072) / BP_LOW (2048) hysteresis | 4096 | 필수 |
| 4 | DPU send pool mirror | DPU ARM | `send_tasks_in_flight` atomic | 8192 | DOCA AGAIN 회피용 |
| 5 | DPU recv pool mirror | DPU ARM | `recv_tasks_in_flight` atomic | 8192 | DOCA AGAIN 회피용 |
| 6 | Host TX slot | Host | `get_next_dma_desc` NULL on `desc->valid==1` | 2048 | 필수 |
| 7 | DPA reverse admission | DPA EU | `dpa_sent_count - dpa_cached_freed < rq_depth` | 2048 | reverse 필수 |

Retry queues:
- `deferred_tx_acks` (16384) — comch send pool full 시 TX_ACK 저장
- `deferred_recv` (1024) — comp_queue ≥ BP_HIGH 시 recv 보관
- `consumer_retry` (256) — gated submit 실패 시 stash

### 15.1 단순화 후보 (우선순위 낮음, 성능 영향 미미)

- (a) `send_tasks_in_flight` mirror 제거 (DOCA capability check 지원 확인 후):
  ~0.1 µs/dma_copy 절감
- (b) 3 retry queue → 단일 priority queue: 코드 단순화 목적
- (c) `is_consumer_empty` spin 에 backoff: 미세 latency 개선

이중 (a) 만 throughput 에 보일 만한 변경. 나머지는 코드 정리 목적.

### 15.2 결론

Flow control 자체는 **correctness 측면에서 잘 설계됨**. throughput cap 의 주된
원인 아님 (§10-11 의 DPA kernel work 가 더 큼).

---

## 16. HW 한계 측정 해석 가이드

목적: dpumesh 가 §7 의 Method 2 ceiling (77.6 K RPS = 310,472 dma_copy ops/s
÷ 4) 에 host-side 자원을 충분히 줬을 때 얼마나 근접하는지.

`./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE>` — 자동으로 `pin_pods hw` 전환.

### 16.1 결과 해석 매트릭스

| `dpumesh-hw` 결과 | 해석 | 다음 실험 |
|---|---|---|
| RPS ≈ 54 K (현재와 동일) | chain 자체가 cap | DPU `top -H -p $(pgrep dpumesh_dpu)` 로 ARM core 100% 확인 |
| 60-65 K | chain 가까이 도달. host-side lock/scheduling 이 fair-mode cap | bench/echo 코어 더 늘려서 한계 측정 |
| > 77 K | Method 2 baseline 잘못됐거나 측정 노이즈 | sweep 재실행 |
| RPS 떨어짐 (< 54 K) | multi-core thread thrashing. cache locality 손실 | core 개수 줄이거나 NUMA 구분 |

### 16.2 보조 측정

- **CPU 사용률**: `mpstat -P 0-7 1`
- **DPU ARM CPU**: DPU 에 ssh + `top -H -p $(pgrep dpumesh_dpu)`
- **DPA EU stat**: `dpa-statistics show` 의 Cycles, producer drain stall,
  consumer_empty wait

### 16.3 추가 변수

- **msg_size 작게 (1 KB)** — per-msg overhead 격리. 8 KB 는 DMA 비중 큼,
  1 KB 는 ops 자체 cap. ops/sec 비교에 더 깨끗.
- **bench 측 thread 수 (workers)** — 기본값 (rps/100) 이 cap region 에서
  thrashing 유발 가능.

---

## 17. Single-pod / Idle Ring 효과 (분석적 추정)

dpumesh 4 rings 중 일부가 idle (예: bench self-loop 으로 echo pod ring 2개 idle):
- §10.4 측정: 4 rings 다 active = ratio 1.017 (perfect amortize)
- Single-pod self-loop: 2 active + 2 idle = **ratio 2.0** (idle ring 폴링 낭비)

§11.4 의 single-op per-extra-poll cost ~0.8 µs. multi-op amortized cost ~0.19 µs
(E9→E10 의 per-extra). 추정:
- 정상 ratio 1.017 → idle 시 ratio 2.0 = 추가 1 poll/op = **+0.19 µs/op**
- dpumesh 4.55 → 4.74 µs/op → **~211K dma_copy/s** (현재 220K 의 -4 %)

ring 개수 자체는 작은 영향 (다 active 시), idle ring 이 폴링 낭비 만들 때만
중요. 현재 echo bench 는 4 rings 모두 active.

---

## 18. 최종 Attribution — E0 (321,803 dma_copy/s) baseline 기준

dpumesh-vs-E0 갭 = **1.44 µs/op = 32 %** (101.8K dma_copy/s 차이).

| 항목 | 비용 (µs/op) | dma_copy/s loss | 출처 |
|---|---:|---:|---|
| Larger comp_msg (7 vs 3 fields) | 0.29 | ~27K | §11.3 E5 (M2 add, -8.6%) |
| Per-call `ensure_producer_slot` | 0.17 | ~17K | §11.2 E3 (M2 add, -5.1%) |
| `handle_msgs` per iter | 0.06 | ~6K | §11.3 E6 (M2 add, -2.0%) |
| Chunking loop wrapper | 0.05 | ~5K | §11.2 E2 (M2 add, -1.6%) |
| Desc clear (in lossless context) | 0.05 | ~5K | §11.1 STRIP_DESC_CLEAR (dpumesh remove) |
| Validation | ~0 | 0 | §11.2 E1 (noise) |
| Extra consumer empty wait | ~0 | 0 | §11.3 E7 (noise) |
| **측정 합계 (E1-E7 + §11.1)** | **0.62** | **~60K (59 %)** | direct |
| **Yield/wake architecture** | **0.55** | — | **§13.2 (M2 + yield+wake, -15.2%)** |
| Cache footprint + amortization 불완전성 | ~0.27 | — | 잔여 |
| **합계 = 실측 갭** | **1.44** | **~102K** | **E0 → dpumesh, 82 % attributed** |

### 18.1 부정된 가설

- ❌ **PCIe BW contention** (E11: 2H+2D vs 3H+1D = -0.3 % noise)
- ❌ **ARM polling 이 cap** (per-batch pe_progress 변경에도 RPS flat;
  poll/copy ratio = 1.017 측정)
- ❌ **DPA busy-spin 이 도움** (§14.1 latency 2.5× 악화, §14.2-14.3 slot leak)

### 18.2 우선 최적화 후보 (single-core)

| 변경 | 잠재 이득 |
|---|---:|
| comp_msg 필드 줄이기 (7 → 3) | +27K dma_copy/s |
| `ensure_producer_slot` → lazy drain (M2 식 매 500) | +17K |
| `handle_msgs` 빈도 감소 (매 iter → 매 N iter) | +6K |
| **합계** | **+50K dma_copy/s = dpumesh 220K → 270K = +23 % RPS** |

남은 0.27 µs unknown 은 DPA scheduling 패턴, cache footprint, amortization
불완전성 — single-feature 추가/제거로 잡히지 않는 integration-level cost.
architectural 변경 필요.

---

## 19. 측정 환경 종합

### 19.1 Hardware

| 항목 | 사양 |
|---|---|
| Host CPU | DVFS lock @ 2.5 GHz, governor=performance, cores 0-7 |
| Host RAM | (TBD) |
| DPU | NVIDIA BlueField-3 |
| DPU ARM | 8 cores (no pinning — dpumesh offload 강점 유지) |
| DPU DPA | FlexIO EU × 1 (single thread for dpumesh) |
| PCIe | Gen4 |
| Network | 테스트 환경 내부 통신 (NIC 외부 트래픽 없음) |

### 19.2 Software

| 항목 | 버전 / 설정 |
|---|---|
| DOCA SDK | 3.1.0105 |
| FlexIO library | 25.07.2812 |
| Kernel | Linux 5.15.0-176-generic |
| Kubernetes | test-bench namespace, single node |
| DPU 시작 옵션 | `dpumesh_dpu $DPU_PCI -l 40` (WARN+ filter) |
| Test harness | `test-bench.sh` (deploy / dpumesh / tcp / pin / cleanup / logs / status) |

### 19.3 dpumesh 현재 적용된 변경 (working tree)

| 파일 | 변경 |
|---|---|
| `comch_server.c` | `pods_*` mutex → `__atomic_*` lock-free (hot read path) |
| `dpu_worker.c` | `process_completion_queue` 의 inner per-entry `pe_progress` 제거 (per-batch 만) |
| `dpu_worker.c` | DPA stat counter 출력 (INFO, `-l 40` 에선 안 보임) |
| `dpa_common.h` | `stat_inner_iters/polls/dma_copies` 필드 추가 |
| `device/dpa_kernel.c` | `STRIP_VALIDATION` enabled |
| `device/dpa_kernel.c` | `STRIP_DESC_CLEAR` enabled (3× writeback → valid=0 만) |
| `device/dpa_kernel.c` | `STRIP_CHUNK_LOOP` enabled (size ≤ 8KB single-chunk fast path) |
| `device/dpa_kernel.c` | DPA stat counter 증가 (drain_all_rings 안) |

### 19.4 M2 baseline (test_dma/bench) 현재 상태

| 파일 | 변경 |
|---|---|
| `device/dpa_kernel.c` | `DOCA_DPA_DEV_LOG_*` 모두 no-op |
| `device/dpa_kernel.c` | `EXTRA_RING_POLLS=0` (E0 baseline 복원) |
| `device/dpa_kernel.c` | `ADD_VALIDATION`/`CHUNK_LOOP`/`PRODUCER_SLOT`/`DESC_CLEAR_*`/`LARGER_COMP_MSG`/`HANDLE_MSGS`/`EXTRA_CONSUMER_WAIT`/`REVERSE_DMA`/`4_DMA_COPIES`/`YIELD`/`YIELD_WITH_WAKE` 매크로 모두 OFF |
| `dpa.c` | `DOCA_LOG_*` 모두 no-op |
| `dpu_worker.c` | `DOCA_LOG_INFO/ERR/DBG` no-op (WARN 만 유지 — `recv:` stat 출력용) |

### 19.5 Flame graph 파일

| 파일 | 캡처 시점 |
|---|---|
| `bench/m2_dpu_flame.svg` | M2 baseline (Method 2 chain) — log 강도 높던 시절 |
| `bench/dpumesh_dpu_flame.svg` | dpumesh 초기 (in-place forwarding 전) |
| `bench/dpumesh_dpu_flame_inplace.svg` | §12 in-place forwarding 적용 후 |
| `bench/dpumesh_dpu_flame_optimized.svg` | comch_server lock-free 적용 후 |
| `bench/dpumesh_dpu_flame_per_batch.svg` | §11.4 per-batch pe_progress 적용 후 |

### 19.6 소스 파일 인덱스

| 경로 | 내용 |
|---|---|
| `bench/bench_dpumesh.c` | A안 client daemon (dpumesh init 1회, ctrl TCP 9092) |
| `bench/echo_dpumesh.c` | A안 server daemon (32 worker thread) |
| `bench/bench_tcp.go` | B안 client daemon (TCP, ctrl TCP 9092) |
| `bench/echo_tcp.go` | B안 server (Go, listen 9092) |
| `bench/Dockerfile.*` | 4 image (dpumesh: libthrift+DOCA, tcp: slim) |
| `test-bench.sh` | 배포 + 실행 스크립트 (deploy / dpumesh / tcp / pin / cleanup / logs / status) |

---

## 20. 설계 결정 (rationale)

- **bench/echo daemon 구조**: 매 실험마다 init 안 함. dpumesh 등록은 deploy 시
  한 번. 매 실험은 ctrl TCP 명령만으로 트리거. ring 등록/해제 경로를 실험 hot
  path 에서 제거.
- **echo 32 thread**: 단일 thread 는 ~25K RPS 에서 막힘 (서버 측 큐잉 3 ms+ p50).
  32 thread 로 dequeue 병목 해소.
- **wall-time 측정**: bench_dpumesh.c 는 watchdog thread + 즉시 join 패턴. main
  이 고정 시간 sleep 하면 wall 부풀려져 RPS underreport.
- **wrk2 식 scheduled-time latency (CO 보정)**: §6.1 참조. cap 너머 가짜
  plateau 제거.
- **MAX_WORKERS = 4096, 128 KB stack**: closed-loop concurrency cap 낮으면
  (이전 512) Little's law 로 in-flight 묶임. 4096 × 128 KB = 512 MB.
- **Envoy 최소 설정**: tcp_proxy filter 1개, cluster 1개. admin/HTTP/tracing/
  stats sink/access log/runtime 모두 제거. DPUmesh 가 transport-only 인 것에 대응.
- **2 sidecar (1개 아님)**: Istio/Envoy 모델은 client-side + server-side 두 hop.
  socat 단일 splice 는 너무 가벼움 (kernel fast-path) → 비교 부정확.
- **pinning 은 taskset (CFS quota 아님)**: CFS quota 는 시간만 제한, core 안
  정함. 같은 core 공유시키려면 pinning 필수. CFS 는 redundant + 잘못된 throttling
  가능성.
- **DPU/DPA 는 pinning 안 함**: "DPU/DPA 가 host CPU 와 독립" 이 dpumesh
  핵심 advantage. 임의로 묶는 건 비교 의의 깎음.
