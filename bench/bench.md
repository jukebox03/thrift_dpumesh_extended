# DPUmesh vs TCP-sidecar Transport Benchmark

DPUmesh DMA transport와 TCP service-mesh (Envoy sidecar) 의 raw 성능 비교.
gateway / Thrift / application 로직을 모두 제거하고 transport 비용만 분리해
측정. 본 문서는 실험 설계, 결과, cap의 정체 attribution, 그리고 두 차례의
정리(in-place forwarding → strips → dead code 제거 + lazy drain)에 따른
누적 개선을 기록.

---

## 1. 실험 대상

| | A안 (DPUmesh) | B안 (TCP via Envoy) |
|---|---|---|
| client → server hop | 1 (DPU) | 2 (sidecar1, sidecar2) |
| transport 위치 | DPU ARM + DPA EU (host CPU 외부) | host CPU 내 (app과 core 공유) |
| L7 처리 | 없음 (transport-only) | 없음 (`tcp_proxy` filter만) |
| 목적 | DMA-기반 mesh bypass | Istio/Envoy 모델 1:1 비교 |

핵심 가설: dpumesh의 architectural advantage는 transport가 host CPU 외부
(DPU/DPA)에서 일어난다는 점. 동일한 host CPU 예산(각 1 core × 2 pod)을
주면, TCP는 app과 sidecar가 core를 나눠 쓰고 dpumesh는 app이 1 core를
통째로 쓸 수 있음.

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

CPU pinning은 `taskset -apc <core> <pid>`로 hard-pin. CFS quota 미사용
(core 정착이 핵심). `pin_pods()`가 deploy 끝 / `pin` subcommand에서 수행.

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
./test-bench.sh pin / pin-hw
./test-bench.sh cleanup
```

dpumesh / tcp / dpumesh-hw 명령은 실행 직전 자동으로 해당 profile로 재핀.

Ctrl protocol (line 기반):
```
RUN <rps> <dur_sec> <msg_size> [<conns>]
→ OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>
```

> bench daemon raw 출력 단위는 microseconds. 본 문서 표는 ms 통일.

---

## 6. 측정 방법론

### 6.1 wrk2식 scheduled-time latency (CO 보정)

bench worker가 closed-loop (send → wait_response → next)라 그대로 두면 cap
region에서 **coordinated omission** 발생 — 시스템이 느려지면 worker도 같이
느려져서 큐잉이 latency 분포에 안 잡힘. cap 너머에서도 p50 ≈ p99 같은 가짜
plateau가 보여 진짜 saturation이 가려짐.

Fix: `t0 = scheduled_time` (보내기로 예약된 tick), `latency = now() - t0`.
worker가 sleep_until에서 늦게 깨면 그만큼 큐잉 wait가 latency에 그대로
잡혀서 elbow가 깨끗이 보임. `bench_dpumesh.c` / `bench_tcp.go` 양쪽 적용.

### 6.2 MAX_WORKERS = 4096, 128 KB stack

closed-loop concurrency cap이 낮으면(이전 512) Little's law로 in-flight가
묶여 가짜 plateau. 4096 × 128 KB = 512 MB. CO 보정과 결합 시 latency tail /
throughput 한계 모두 시스템 속성 직접 반영.

### 6.3 wall-time 측정

`bench_dpumesh.c`는 watchdog thread + 즉시 join 패턴. main이 고정 시간
sleep하면 wall이 부풀려져 RPS underreport.

### 6.4 측정 도구

| 도구 | 용도 |
|---|---|
| `test-bench.sh dpumesh <rps> <dur> <size> [<conns>]` | dpumesh RPS sweep |
| `run_bench.sh --method 0 ...` (test_dma/bench) | M2 baseline 측정 |
| `perf record -F 999 -g --call-graph dwarf` | flame graph |
| `perf trace -s -p <pid>` | syscall summary |
| `/opt/mellanox/doca/tools/dpa-statistics collect` | DPA EU 통계 |
| `top -H -p $(pgrep dpumesh_dpu)` | ARM thread CPU |
| DPA stat counters (제거됨, §10.4 결정적 측정용으로 일회성 사용 후 폐기) | polls/dma_copy ratio 실측 |

---

## 7. Transport Ceiling Baselines

`experiment/bench.md`의 micro-bench (`/home/jukebox/test_dma/bench/`) — k8s ·
Thrift · TCP · gateway 모두 제거. host가 dma_ring에 desc 직접 post, comch
client + DPA RPC만 사용. `host_worker.c`의 단순 루프(`while(true) {
get_next_dma_desc; desc->valid=1; }`)로 baseline으로 valid.

### 7.1 Method 0 / Method 2 측정 (8 KB / 60 s, 2026-05 재측정)

이전 측정 (M0=302K, M2=264K)은 DOCA `comch_server.c`의 per-msg `DOCA_LOG_INFO`가
stdio I/O로 stale overhead 주입. `run_bench.sh`에 `-l 40` 추가 + stat 로그를
WARN으로 promote해서 silence한 결과:

| Mode | 구성 | dma_copy ops/sec | Throughput (1 dir) | vs M0 |
|---|---|---:|---:|---:|
| Method 0 (Only DMA) | `dma_copy` atomic (HW max) | **320,014** | **20.97 Gbps** | 100% |
| Method 2 (DMA + completion) | `dma_copy` + DPU CPU가 host로 `server_send_msg` | **310,472** | **20.34 Gbps** | **97%** |

Method 0 → Method 2 손실 3%만. "DPU CPU → host comch forward" 진짜 비용은
매우 작음. dpumesh가 이 chain을 그대로 사용하므로 **Method 2 = dpumesh의
transport ceiling의 direct baseline**.

### 7.2 micro-bench vs dpumesh chain 차이 (caveat)

| | micro-bench (Method 2) | dpumesh |
|---|---|---|
| 데이터 흐름 | host → DPU → host (단방향, 1 endpoint) | host A → DPU → host B → DPU → host A (RTT, 2 endpoints) |
| DPU ARM | comch 수신 + `server_send_msg` 한 번 | comp_queue enq + **pod_idx 라우팅 + TX_ACK + reverse desc 작성 + tx_ring enq** |
| descriptor 라우팅 | 없음 | `dst_pod_id` 별 분기 |
| reverse direction | 없음 | 매 hop reverse DMA enq |

→ 같은 "1 dma_copy"라도 dpumesh는 DPU ARM 부수 작업 多. 1 RTT = forward DMA × 2 +
reverse DMA × 2 = **4 dma_copy ops**.

---

## 8. TCP/Envoy vs DPUmesh — Direct Comparison

### 8.1 단일 부하점 측정 (rps=40000, dur=10s, size=8192B)

| | dpumesh | tcp/Envoy |
|---|---|---|
| Target RPS | 40,000 | 40,000 |
| **Achieved RPS** | **39,732.7** (99.3%) | **33,097.0** (82.7%) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 latency | **4.53 ms** | **661.50 ms** |
| p99 latency | **8.35 ms** | **3,885.30 ms** |
| p999 latency | 8.62 ms | 4,588.43 ms |
| Throughput (RTT) | 620.82 MB/s | 517.14 MB/s |

40 K target에서 dpumesh는 healthy region (52 K sustainable의 76%), TCP/Envoy는
deep overload (achieved 33.1 K, p99 3,885 ms). TCP/Envoy saturation은 40 K 한참 아래.

---

## 9. DPUmesh Saturation Sweep — Baseline (in-place forwarding 전)

### 9.1 Sweep (8 KB, 10 s, conns=auto)

| Target RPS | Achieved | p50 (ms) | p99 (ms) | p999 (ms) | Throughput | OK / Fail |
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

### 9.2 핵심 관찰 (baseline)

- **Sustainable saturation ≈ 52 K RPS** (p50 5.5 ms, p99 12 ms). 50 K까지 거의
  perfectly linear (50 K → 49.7 K, 99.3%).
- **Elbow = 52 K → 55 K 사이**. 55 K target에서 p99가 12 → 190 ms **16배** 점프
  (전형적 M/M/1 saturation).
- **Overload throughput ceiling ≈ 53.5 K RPS** — 60/65 K target으로 더 밀어도
  achieved 53.7 K에 고정. latency만 발산.
- **0 failure** — `WAIT_TIMEOUT_MS = 5,000 ms` 보다 worst-case latency가 작아
  미발동. 4-layer flow control (tx_alloc 무한 대기, enqueue 백오프, reverse-path
  admission gate)이 backpressure만으로 흡수.
- **Throughput plateau 836–841 MB/s** — DMA chain 53.5 K × 4 dma_copy = 214 K
  ops/s에서 hard cap.

### 9.3 Baseline ceiling 위치

```
   100% ┃ ┌── HW max (Method 0, dma_copy atomic) ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)        ── 310,472 ops/s = 20.34 Gbps
    67% ┃ └── dpumesh @ overload ceiling          ── 214,212 ops/s = 13.71 Gbps
        ┃     (53,553 RPS × 4 dma_copy)
```

| | dma_copy ops/sec | vs M0 | vs M2 |
|---|---:|---:|---:|
| Only DMA (HW max) | 320,014 | 100% | — |
| DMA + completion | 310,472 | 97% | 100% |
| **dpumesh @ overload (53.55 K RPS)** | **214,212** | **67%** | **69%** |
| dpumesh @ sustainable (51.64 K RPS) | 206,580 | 65% | 67% |

**의미 있는 비교 = 69% vs Method 2** (동일 chain 구조).

---

## 10. Cap의 정체 — DPA EU per-dma_copy work

§7 baseline 대비 dpumesh의 31% gap 원인 규명. 여러 후보를 직접 측정으로
검증 또는 반증.

### 10.1 DPA EU stat — ARM 99.9% 인데 EU 99.5% idle

```
sudo /opt/mellanox/doca/tools/dpa-statistics collect -d mlx5_0 -t 10000
```

50 K RPS 부하, 10 s 윈도우:

| 항목 | 값 | 해석 |
|---|---:|---|
| Wall time | 10,000 ms | 측정창 |
| **EU active time** | **51.5 ms** (ticks @ 1 ns) | EU가 실제 실행한 시간 |
| **EU active %** | **0.51%** | 99.49% idle |
| Cycles | 92.7 G | 51.5 ms × ~1.8 GHz 일치 |
| Instructions | 7.22 G | IPC = 0.078 (memory/DMA stall — 정상) |
| Executions | 7,314 (= 731/s) | DPA thread 깬 횟수 |
| Cycles/execution | 12.7 M | wake 당 평균 7 µs 실행 |
| dma_copy/execution | ~273 | wake 당 batch size |

DPA는 burst 처리 — wake → 273 dma_copy 일괄 → re-schedule.

### 10.2 ARM perf 분석 (50 K RPS 부하)

`perf report --no-children` self-time:

| 카테고리 | dpumesh | Method 0 | Method 2 |
|---|---:|---:|---:|
| **epoll_pwait syscall 군집** (kernel + libc + vdso) | **64%** | 65% | 62% |
| DOCA infra (CQ poll, comch internals) | 7% | 13% | 19% |
| Atomics (CAS, swap, mutex) | 4% | 2% | 3% |
| **App 코드** (run_dpu_worker, process_*, drain_*) | **5.5%** | 3.4% | 2.8% |

per-op time 환산:

| | per-op time | Δ vs M2 |
|---|---:|---:|
| Method 0 | 3.13 µs | — |
| Method 2 | 3.22 µs | baseline |
| **dpumesh** | **4.67 µs** | **+1.45 µs (+45%)** |

세 측정 모두 syscall 1순위. 하지만 dpumesh만 throughput 낮음 — ARM 폴링
자체로는 설명 불가.

### 10.3 ARM CPU 분포 — useful work 분해

`bench/dpumesh_dpu_flame_per_batch.svg` 기준:

| ARM 함수 | self-time % | per-event 환산 |
|---|---:|---:|
| `doca_pe_progress` (epoll_pwait 포함) | 76.5% | 3.48 µs (polling syscall) |
| `process_completion_queue` | 7.95% | 0.36 µs |
| `process_rev_notify_entry` | 5.18% | 0.24 µs |
| `find_pod_by_id` | 2.80% | 0.13 µs |
| `server_send_msg_to_conn` | 2.20% | 0.10 µs |
| comp_queue ops (peek/empty/full/dq/eq) | ~3.5% | 0.16 µs |
| `process_forward_entry` | 0.91% | 0.04 µs |
| `drain_deferred_tx_acks` | 0.54% | 0.02 µs |
| **ARM useful work 합계** | **~23%** | **~1.05 µs/event** |

ARM과 DPA EU는 **다른 processor가 병렬 작동**. cap = max(DPA per-op, ARM per-event):
- DPA per-dma_copy: **4.55 µs**
- ARM per-event: **1.05 µs**
- → **DPA가 cap, ARM은 ~3.5 µs 헤드룸**

ARM 측 최적화는 throughput에 영향 없음.

### 10.4 DPA polling counter — 결정적 측정 (Experiment X)

가설 갈음:
- 가설 A: dpumesh poll/copy ratio = 2~4 → polling이 갭의 ~75%
- 가설 B: dpumesh ratio ≈ 1.0 → polling 무관, 갭은 DPA code work

DPA kernel + `dpa_thread_arg`에 counter 3개 추가:
```c
volatile uint64_t stat_inner_iters;   /* drain_all_rings inner do-while */
volatile uint64_t stat_polls;         /* PCIe desc->valid reads */
volatile uint64_t stat_dma_copies;    /* dma_copy chunks issued */
```

DPU ARM이 1초마다 `doca_dpa_d2h_memcpy`로 읽어 ratio 계산.

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
- **poll/copy ratio = 1.017** ≈ M2 baseline의 1.000

→ **가설 B 확정**. 4 rings 폴링이 4 dma_copies 발사로 perfectly amortize.
polling은 dpumesh-vs-M2 갭의 원인 아님.

> 카운터는 결정적 측정 임무 종료 후 제거 (코드 영구화 X). 필요 시 git
> history에서 복원.

### 10.5 cap 정체 (정정된 모델)

| | M2 baseline | dpumesh | Δ |
|---|---:|---:|---:|
| dma_copy/s | **325K** (또는 321K, §11.2 참조) | **220K** | -105K |
| Per-dma_copy 시간 | **3.08 µs** | **4.55 µs** | **+1.47 µs** |
| PCIe polls / dma_copy | ~1.0 | **1.017 (측정)** | 0 |

+1.47 µs/dma_copy 갭은 **DPA EU의 추가 코드 작업**. M2의 `poll_desc_ring_dma_copy`에
없는 dpumesh `process_one_desc`의 작업:

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

DPA kernel work 가설을 두 방향에서 검증: (A) dpumesh에서 feature 제거,
(B) M2에 feature 추가. 마지막으로 (C) §11.5에서 dead code 제거 + lazy drain +
throttle을 추가 적용한 후속 sweep.

### 11.1 DPUmesh feature strip (8 KB, 10 s, conns=auto)

3개 compile-time toggle (당시 macro, 이후 §19.3에서 영구 코드로 흡수):
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
   ↓ (+1.7%)
Exp A — STRIP_VALIDATION:                  55,207 RPS   (+916 RPS)
   ↓ (+1.7%)
Exp A+B — +STRIP_DESC_CLEAR:               56,134 RPS   (+927 RPS)
   ↓ (+5.5%)
Exp A+B+C — +STRIP_CHUNK_LOOP:             59,195 RPS   (+3,061 RPS)
─────────────────────────────────────────
Total +9.0% (54,291 → 59,195 RPS)
```

**Chunking bypass가 단일 항목 최대 기여**. 이유: chunking loop 안에
`ensure_producer_slot` + `is_consumer_empty` wait + `producer_slots_inflight++`가
들어있어 1 chunk dma_copy도 모든 체크 발동. fast path는 1번씩만.

**Elbow 이동**: baseline 55 K → A+B+C 적용 후 55 K가 여전히 sustainable
(p99 12.27 ms, baseline의 52 K와 동일 profile). p99가 1/8 (91 → 12 ms).

안정성 (3회 back-to-back 50K, slot leak 검증):
```
=== run 1 === Achieved RPS: 49342.6  p99: 9381.5 us  OK/Fail: 250000/0
=== run 2 === Achieved RPS: 49353.8  p99: 9347.9 us  OK/Fail: 250000/0
=== run 3 === Achieved RPS: 49344.8  p99: 9329.5 us  OK/Fail: 250000/0
```

per-dma_copy 시간:
- Baseline: 4.57 µs / dma_copy
- Exp A+B+C: 4.22 µs / dma_copy (-0.35 µs)
- M2: 3.08 µs / dma_copy

### 11.2 M2에 feature 추가 (Inverse, 8 KB, Method 0, H2D, 10 s)

4개 toggle:
```c
/* #define ADD_VALIDATION */    /* 4 checks: mmap=0, size=0, range, padded>buf */
/* #define ADD_CHUNK_LOOP */    /* while(offset < total) wrapper */
/* #define ADD_PRODUCER_SLOT */ /* lazy 500-drain → per-call slot check + counter */
/* #define ADD_DESC_CLEAR */    /* 4-field clear + 3× writeback after dma_copy */
```

| Stage | recv/s | Bandwidth | Δ vs prev |
|---|---:|---:|---:|
| **E0 baseline** | **321,803** | 21.08 Gbps | — |
| E1 +ADD_VALIDATION | 324,707 | 21.27 Gbps | +0.9% (noise) |
| E2 +ADD_CHUNK_LOOP | 319,495 | 20.93 Gbps | **-1.6%** |
| E3 +ADD_PRODUCER_SLOT | 303,173 | 19.86 Gbps | **-5.1%** |
| E4 +ADD_DESC_CLEAR | **17,639** | 1.15 Gbps | **-94.2%** ❗ |

E4 collapse isolation 측정:

| Variant | recv/s | 비고 |
|---|---:|---|
| E4-alone (DESC_CLEAR only) | 17,510 | 다른 toggle 무관 |
| E4-min (`valid=0` 1줄 + writeback 1번만) | 17,225 | writeback 자체가 catastrophic |

**Architectural finding — desc clear의 100× 비대칭**:

M2의 ring은 **lossy** — host가 valid 안 체크하고 blind write:
```c
struct dma_desc *get_next_dma_desc(struct dma_ring *ring) {
    struct dma_desc *desc = ring->descs + ring->head;
    ring->head = (ring->head + 1) % ring->size;
    return desc;   /* no valid check */
}
```

dpumesh는 **lossless** — host가 `valid==1`이면 NULL 반환, DPA가 valid=0
clear할 때까지 대기.

DPA의 `__dpa_thread_window_writeback()` → host memory의 같은 cache line에
PCIe 쓰기. M2 host는 동시에 같은 cache line write (`desc->addr/size/valid=1`).
→ **양쪽 동시 쓰기 = PCIe cache coherency cost 폭증**.

dpumesh는 host가 valid=0 기다림 → cache line 충돌 없음 → 같은 writeback cheap.

### 11.3 추가 feature (E5-E10, polling amortization sanity check 포함)

| Variant | recv/s | Δ vs E0 | per-iter time | 비고 |
|---|---:|---:|---:|---|
| **E0 baseline** | **321,803** | **0** | 3.11 µs | 1 poll : 1 op |
| E5 +ADD_LARGER_COMP_MSG | 294,207 | -8.6% | 3.40 µs (+0.29 µs) | 3 → 7 fields (12B → 24B immediate) |
| E6 +ADD_HANDLE_MSGS | 315,513 | -2.0% | 3.17 µs (+0.06 µs) | `consumer_get_completion` 1× per iter |
| E7 +ADD_EXTRA_CONSUMER_WAIT | 322,857 | +0.3% (noise) | 3.10 µs | 사실상 free |
| E8 +ADD_REVERSE_DMA | 381,826 | +18.6% | 5.24 µs / 2 op | **1 poll : 2 op** ← amortize |
| E9 +ADD_4_DMA_COPIES | 417,119 | +29.6% | 9.62 µs / 4 op | **1 poll : 4 op** ← 더 amortize |
| E10 +ADD_4_DMA_COPIES +EXTRA_RING_POLLS=3 | 338,322 | +5.1% | 11.83 µs / 4 op | **4 poll : 4 op** ← dpumesh ratio |

E8/E9의 throughput 상승은 dpumesh ceiling이 아니라 polling amortization:
```
per_iter_time = α + N × β  (N = dma_copies/iter, 1 poll/iter 고정)
3.11 = α + 1 × β  (E0)
5.24 = α + 2 × β  (E8)
9.62 = α + 4 × β  (E9)
⇒ β ≈ 2.13 µs/dma_copy (marginal cost)
  α ≈ 0.98 µs (per-iter fixed overhead)
```

- M2 per-iter overhead = **0.98 µs**
- dma_copy 1개 marginal cost = **2.13 µs**

E10 (4 poll : 4 op = dpumesh 패턴) **338,322 recv/s** — E0와 +5% 차이.
→ dpumesh의 1:1 poll:op 패턴에서는 amortization 이득 거의 없음. **비교 baseline으로
E0 사용이 정직**.

### 11.4 추가 sanity checks

#### E11 — PCIe direction balance (2H+2D vs 3H+1D)

| Variant | recv/s | per-op time |
|---|---:|---:|
| E10 (3H+1D + 4 polls) | 338,322 | 2.96 µs |
| **E11 (2H+2D + 4 polls)** | **337,262** | **2.97 µs** |

Δ = -0.3% (noise). **PCIe direction balance 영향 없음** — PCIe full-duplex라
forward + reverse가 같은 BW lane 공유 안 함. "PCIe BW contention" 가설 부정.

#### Inner pe_progress 제거 (per-entry → per-batch)

`process_completion_queue`의 entry-당 `pe_progress` × 2 호출 → batch 끝 1회.

| Target | Baseline ach / p99 | Per-batch ach / p99 | Δ ach | Δ p99 |
|---:|---:|---:|---:|---:|
| 50K | 49,655 / 9.94 ms | 49,629 / 9.89 ms | -0.05% | -0.5% |
| 55K | 54,085 / 101.39 ms | 54,080 / 91.06 ms | -0.01% | **-10.2%** |
| 65K | 54,749 / 1,768 ms | 54,291 / 1,894 ms | -0.8% | +7.1% |

Sustainable RPS **변화 없음**. Flame 비교:

| 항목 | inplace | per_batch |
|---|---:|---:|
| `doca_pe_progress` subtree | 67.5% | **76.5%** ↑ |
| `__GI_epoll_pwait` | 46.0% | **54.5%** ↑ |
| `process_completion_queue` | 19.8% | **7.95%** ↓ |

`process_completion_queue` self-time 12pp 감소분 = `epoll_pwait`로 이동.
**ARM polling 횟수를 줄여도 polling 시간 비중은 안 줄어듦** → ARM이 cap 아님.

#### epoll_pwait per-call 측정 (perf trace, 50K RPS, 15s)

```
syscall          calls      total(ms)   avg(ms)    min       max
epoll_pwait     3,122,730   6,703.520   0.002      0.001     0.109
```

- 호출 횟수: 208 K calls/s
- 호출당 평균: **2 µs** (syscall enter/exit 고정 비용)
- 총 `epoll_pwait` 시간 / wall = 44.7% (flame의 ~45-55% subtree와 일치)

avg 2 µs는 block 깊게 자는 게 아니라 syscall 고정 비용. M2 baseline도 같은
41% epoll 비용 무는데 310K events/s 도달 → **epoll overhead 자체는 dpumesh-vs-M2
갭의 원인 아님**.

#### Load generator 검증 (system vs load-gen)

| Target × workers | achieved | p99 |
|---|---:|---:|
| 65K × 650 (default) | 54,291 | 1,894 ms |
| **65K × 1000** | **54,199** | **1,892 ms** |
| **100K × 1500** | **54,227** | **6,814 ms** |
| 65K × 4096 | **hang** — TX slot pool (2048) 충돌 | n/a |

650 → 1500 워커 변화에도 ach가 54.2 K에 fix. **system ceiling 확정**.
4096 워커는 `DMA_RING_SIZE = 2048` 동시성 한계로 hang.

#### M2 synthetic multi-ring polling (EXTRA_RING_POLLS)

매 dma_copy 마다 N개 추가 `desc->valid` PCIe read 삽입 (별도 cache line):

| EXTRA_RING_POLLS | 총 rings 폴링 | recv/s | Bandwidth | Δ vs N=0 |
|---:|---:|---:|---:|---:|
| 0 (baseline) | 1 | **325,361** | 21.32 Gbps | — |
| 1 | 2 | 236,660 | 15.50 Gbps | **-27%** |
| 3 | 4 | 175,421 | 11.49 Gbps | **-46%** |
| 7 | 8 | 114,543 | 7.50 Gbps | **-65%** |

monotonic. per-extra-poll cost ≈ 0.8-1.1 µs / dma_copy. dpumesh 220K가 M2-N=3
(synthetic 4 rings)의 175K보다 **높은** 이유 — synthetic은 매 dma_copy마다
N reads 무조건 발사, dpumesh `drain_all_rings`은 한 inner iter에서 4 rings 폴링
+ 0~4 dma_copy → saturation 시 amortize (§10.4의 ratio 1.017과 일치).

### 11.5 Post-cleanup sweep — dead code 제거 + lazy drain + throttle

§11.1의 strip 매크로를 영구 코드로 흡수한 직후 추가로 적용한 정리.

**적용 사항**:
- **DMA_REQ dead code chain 제거** (총 ~150줄): `dma.c` (init_dma_resources +
  send_dma_request_to_dpa), `dma.h`, `dpa_kernel.c`의 `handle_dpu_msg` DMA_REQ
  case (80줄), `dpa_common.h`의 `struct comch_dma_req_msg` + enum + union field,
  `dpa.c`의 `COMCH_MSG_TYPE_DMA_CHUNK` consumer-side case. **caller 0**이라
  실행 경로 변경 없는 순수 제거.
- **Chunking fallback 제거**: host측 `check_slot_size()`가 `DPUMESH_SLOT_SIZE_DEFAULT
  = 8192`로 강제하므로 `desc->size > 8KB`는 실제로 도달 불가. `process_one_desc` /
  `process_one_rev_desc`의 chunking else 분기 (~120줄) 제거, size > 8KB 시
  명시적 drop guard 추가. forward path는 fast-path만 남아 함수 절반 길이로 단축.
- **`handle_msgs` 호출 주기 감소**: drain_all_rings의 매 inner iter (~50K/s)에서
  매 32 iter (~1.5K/s)로. TRIGGER ~1 kHz / ADD_RING (deploy 1회)만 처리하면
  되므로 대부분 empty poll. outer wake당 1회는 그대로 유지.
- **`ensure_producer_slot` lazy drain**: 내부 drain spin 제거하고 inflight ≥ CAP
  시 -1 return (caller abort). `drain_producer_completions`은 drain_all_rings 안
  매 8 iter (~6K/s)에서 일괄. PRODUCER_SLOT_CAPACITY=1024 ≫ 일반 wake당 ~280
  inflight 이라 abort 사실상 불발.

**Sweep 결과 (8 KB, 10 s)**:

| Target | Achieved | p50 (ms) | p99 (ms) | p999 (ms) | Throughput | OK / Fail | 비고 |
|---:|---:|---:|---:|---:|---:|---|---|
| 30K run 1 | 29,812.7 | 3.19 | **5.78** | 5.93 | 465.82 MB/s | 300,000 / 0 | sustainable |
| 30K run 2 | 29,800.9 | 3.21 | 5.82 | 8.26 | 465.64 MB/s | 300,000 / 0 | back-to-back clean |
| 50K | 49,653.5 | 4.96 | **9.06** | 9.33 | 775.84 MB/s | 500,000 / 0 | sustainable |
| 55K | 54,629.5 | 5.33 | **9.80** | 12.23 | 853.59 MB/s | 550,000 / 0 | sustainable — 이전 elbow 통과 |
| 60K | 59,564.2 | 5.60 | **12.58** | 14.45 | 930.69 MB/s | 600,000 / 0 | sustainable, elbow edge |
| 62K run 1 | 61,536.4 | 5.92 | 12.89 | 13.12 | 961.51 MB/s | 620,000 / 0 | borderline sustainable |
| 62K run 2 | 61,548.9 | 6.24 | 15.15 | 18.08 | 961.70 MB/s | 620,000 / 0 | back-to-back, slot leak 없음 |
| 65K | 62,068.3 | **203.35** | **404.21** | 410.28 | 969.82 MB/s | 650,000 / 0 | overload (p99 폭증) |

**핵심 관찰**:
- **Sustainable elbow 55K → 60K** (§11.1 strips만 적용 시 55K @ p99 12 ms ≈
  이번 60K @ p99 12.6 ms와 동일 profile).
- **Overload throughput ceiling 59,195 → 62,068 RPS** (+4.9%, 65K target에서
  achieved 비교).
- Throughput 970 MB/s @ 65K target — §11.1 925 MB/s 대비 +4.9%.
- 65K → 62K가 hard cap. DMA chain 62K × 4 = **248K dma_copy/s ≈ M2 ceiling 310K의 80%**.
- 8 sweep 전 구간 **0 failure**, 62K back-to-back 둘 다 clean → slot leak 없음.

**누적 ceiling 변화**:

```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
    78% ┃ ├── dpumesh @ §11.5 cleanup        ── 248,272 ops/s = 16.28 Gbps
        ┃ │     (62,068 RPS × 4 dma_copy)
    74% ┃ ├── dpumesh @ §11.1 strips         ── 236,780 ops/s = 15.13 Gbps
    68% ┃ └── dpumesh @ in-place baseline    ── 218,992 ops/s = 14.02 Gbps
```

### 11.6 `comch_dma_comp_msg` packing — 28B → 16B (WQE BB 1개)

§11.3 E5 (M2 add LARGER_COMP_MSG, 12B → 24B = -8.6%)의 역방향. dpumesh의
DPA→DPU 완료 메시지는 모든 7 필드가 라우팅/Thrift semantic에 실제로 쓰이므로
필드 자체는 줄일 수 없음. 대신 **wire format을 더 좁게 packing**.

**Quantization 가설 검증**:

| Quantum 후보 | 12B → 24B 예상 cost | §11.3 E5 측정 | 정합 |
|---|---:|---:|---|
| 64B | 0% | -8.6% | ❌ 모순 |
| 32B | 0% | -8.6% | ❌ 모순 |
| 16B | -50% bytes (1→2 quanta) | -8.6% | ✅ |
| Linear | 비례 | -8.6% | △ |

→ §11.3 E5는 quantum ≥ 32B 가설을 **정량적으로 배제**. 28B → 16B 압축은
32B HW quantum → 16B HW quantum 1단계 강하 = 비슷한 -8% 기대.

**변경 사항** (`dpa_common.h`):

| 필드 | Before | After | 정당화 |
|---|---:|---:|---|
| `type` | `enum` 4B | `uint8_t` 1B | DMA_COMPLETED=2, REV_DMA_COMPLETED=7 — 2값만 사용 |
| `pos` | 4B | 4B | 변경 없음 (offset 4, 자연 정렬) |
| `length` | 4B | 4B | 변경 없음 |
| `req_id` | 4B | 4B | 변경 없음 |
| `src_pod_id` | `int32_t` 4B | `int8_t` 1B | MAX_PODS=32 + -1 sentinel, int8 충분 |
| `dst_pod_id` | `int32_t` 4B | `int8_t` 1B | 동일 |
| `flags` | 1B | 1B | 이미 최소 |
| 트레일 padding | 3B | 0B | 필드 재배치로 16B 정렬 |
| **sizeof** | **28B** | **16B** | -43% bytes |

`__attribute__((packed))` 불필요 — 4B 필드들이 모두 4B-aligned offset에
랜딩하도록 재배치 (`type/flags/src/dst @ 0..3, pos @ 4, length @ 8, req_id @ 12`).
DPA 측 unaligned access penalty 없음. `_Static_assert(sizeof == 16)`로 회귀
방어.

**Read site 수정** (`dpa.c:71-76`): 기존 `*(enum comch_msg_type *)raw`는 4B
read였는데 이제 type가 1B → `raw[0]`로 단축. 다른 caller는 모두
`sizeof(struct comch_dma_comp_msg)` 또는 struct pointer cast 사용이라 자동 적응.

**Sweep 결과 (8 KB, 10 s, packing 적용 후)**:

| Target | Achieved | p50 (ms) | p99 (ms) | p999 (ms) | Throughput | OK / Fail | 비고 |
|---:|---:|---:|---:|---:|---:|---|---|
| 30K | 29,801.4 | 3.04 | **5.53** | 6.21 | 465.65 MB/s | 300,000 / 0 | sustainable |
| 50K | 49,653.8 | 4.70 | **8.59** | 9.23 | 775.84 MB/s | 500,000 / 0 | sustainable |
| 60K | 59,585.6 | 5.44 | **10.04** | 12.64 | 931.03 MB/s | 600,000 / 0 | sustainable |
| 62K | 61,545.3 | 5.51 | **12.59** | 12.79 | 961.65 MB/s | 620,000 / 0 | sustainable |
| **65K run 1** | **64,549.4** | **5.65** | **12.89** | 13.08 | **1008.58 MB/s** | 650,000 / 0 | **sustainable — 1 GB/s 돌파** |
| 65K run 2 | 64,524.0 | 5.70 | 12.90 | 13.22 | 1008.19 MB/s | 650,000 / 0 | back-to-back, slot leak 없음 |
| 66K | 65,529.0 | 12.08 | **24.51** | 26.37 | 1023.89 MB/s | 660,000 / 0 | elbow 초과 |
| 67K | 65,859.6 | 55.45 | **105.72** | 109.01 | 1029.06 MB/s | 670,000 / 0 | overload |

**핵심 관찰**:
- **Sustainable elbow 60K → 65K** (+8.3%). 이전엔 65K가 deep overload
  (p99 404 ms), 이제 65K가 깨끗한 sustainable region.
- **Overload throughput ceiling 62K → 65.5K** (+5.6%).
- **Throughput 1 GB/s 돌파** @ 65K (이전 §11.5의 970 MB/s).
- 50K-60K p99 일괄 5-20% 개선 (60K p99 -20.2%가 가장 큼).
- 8 sweep 전 구간 **0 failure**, 65K back-to-back 둘 다 clean.

**§11.3 E5 가설 정량 검증**:
- E5 인버스 예측: ~+8% throughput
- 실측: overload throughput +5.6%, p99 -20% @ 60K, elbow +8.3% RPS
- → **§11.3 E5의 quantum hypothesis 정합** (16B HW quantum 확정).

**누적 ceiling 갱신**:

```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
    82% ┃ ├── dpumesh @ §11.6 packing        ── 263,436 ops/s = 17.27 Gbps
        ┃ │     (65,859 RPS × 4 dma_copy @ 67K target)
    78% ┃ ├── dpumesh @ §11.5 cleanup        ── 248,272 ops/s = 16.28 Gbps
    74% ┃ ├── dpumesh @ §11.1 strips         ── 236,780 ops/s = 15.13 Gbps
    68% ┃ └── dpumesh @ in-place baseline    ── 218,992 ops/s = 14.02 Gbps
```

vs Method 2: 78% → **85%**. vs Method 0: 78% → **82%**.

### 11.7 Producer slot SDK 위임 + `OPTIMIZE_REPORTS`

§11.5의 lazy-drain은 `producer_slots_inflight` 카운터 + `PRODUCER_SLOT_CAPACITY=1024`
hard cap + abort 패턴으로 구현됐는데, 이 패턴이 SDK의 `DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS`
(완료 batch 통보)와 충돌. OPTIMIZE_REPORTS를 켜면 completion이 deferred되어
`drain_producer_completions`가 0 ack → 카운터 미감소 → CAP 도달 → abort 연쇄
→ host TX stuck (실측 30K target에서 OK 243 / Fail 900). M2 baseline (§7.1)은
inflight 카운터 자체를 안 두고 SDK 내부 backpressure에 위임하며 모든 dma_copy에
OPTIMIZE_REPORTS 켜고 잘 동작.

**변경 사항**:
- `producer_slots_inflight` 카운터 제거 (dpa_thread_arg에서 `_pad1`으로 교체, alignment 보존)
- `ensure_producer_slot` 함수 + 호출 4건 제거 (CAP/abort 패턴 폐기)
- `drain_producer_completions`은 ack만 유지 (count 추적/슬롯 감소 제거)
- 두 `dma_copy` 호출처에 `DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH |
  DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS`

producer 큐 backpressure 안전: per-wake burst ~280 dma_copy (§10.1) ≪ SDK 큐
용량 1024, drain `DRAIN_COMPLETIONS_EVERY=8` inner iter 주기로 ack 유지.

**Sweep (8 KB, 10 s)**:

| Target | Achieved | p50 (ms) | p99 (ms) | p999 (ms) | Throughput | OK / Fail | 비고 |
|---:|---:|---:|---:|---:|---:|---|---|
| 30K | 29,783.2 | 2.94 | **5.38** | 6.70 | 465.36 MB/s | 300,000 / 0 | sustainable |
| 50K | 49,662.8 | 4.58 | **8.35** | 8.61 | 775.98 MB/s | 500,000 / 0 | sustainable |
| 60K | 59,554.3 | 5.35 | **9.84** | 12.49 | 930.54 MB/s | 600,000 / 0 | sustainable |
| 65K | 64,523.0 | 5.59 | **12.84** | 13.08 | 1008.17 MB/s | 650,000 / 0 | sustainable |
| **67K** | **66,533.4** | **5.67** | **13.00** | 13.20 | **1039.58 MB/s** | 670,000 / 0 | **새 sustainable** |
| **68K** | **67,497.0** | **6.45** | **13.72** | 15.21 | **1054.64 MB/s** | 680,000 / 0 | sustainable, elbow edge |
| 70K | 68,126.8 | 119.49 | **248.78** | 253.66 | 1064.48 MB/s | 700,000 / 0 | overload |

**Back-to-back 안정성** (state drift / slot leak 검증, 5,420,000 RTT × 9 run):

| Target | Run 1 / p99 | Run 2 / p99 | Run 3 / p99 |
|---:|---:|---:|---:|
| 60K | 59,553.5 / 9.61 ms | 59,553.9 / 9.82 ms | — |
| 65K | 64,523.0 / 12.85 ms | 64,513.8 / 12.83 ms | — |
| 67K | 66,533.4 / 13.00 ms | 66,508.9 / 13.01 ms | 66,505.5 / 13.04 ms |
| 68K | 67,497.0 / 13.72 ms | 67,488.3 / 13.63 ms | — |

run-to-run achieved 편차 < 0.05%, p99 편차 < 1%, **0 failure × 9 회**. M2-style
패턴이 §14의 lossless ring 가정과 함께 long-run drift 없이 동작.

**핵심 관찰**:
- **Sustainable elbow 65K → 68K** (+4.6% vs §11.6, +13.3% vs §11.5 60K).
- **Overload throughput ceiling 65,859 → 68,127 RPS** (+3.4% vs §11.6).
- Throughput 1,029 → 1,065 MB/s (+3.5%).
- 67K p99 §11.6 105 ms (overload) → 13.0 ms (sustainable, back-to-back stable).
- M2-style 단독 (counter 제거만): +1.5%. +OPTIMIZE_REPORTS 시너지로 +3.4%
  ⇒ 두 변경의 결합 효과. 단독 OPTIMIZE_REPORTS는 §11.5 CAP 패턴과 incompatible.

**누적 ceiling 갱신**:

```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
    85% ┃ ├── dpumesh @ §11.7 SDK delegation ── 272,508 ops/s = 17.85 Gbps
        ┃ │     (68,127 RPS × 4 dma_copy @ 70K target)
    82% ┃ ├── dpumesh @ §11.6 packing        ── 263,436 ops/s = 17.27 Gbps
    78% ┃ ├── dpumesh @ §11.5 cleanup        ── 248,272 ops/s = 16.28 Gbps
    74% ┃ ├── dpumesh @ §11.1 strips         ── 236,780 ops/s = 15.13 Gbps
    68% ┃ └── dpumesh @ in-place baseline    ── 218,992 ops/s = 14.02 Gbps
```

vs Method 2: 82% → **88%**. vs Method 0: 82% → **85%**.

---

## 12. In-place Forwarding 변경 (2026-05-08)

§10.5의 우선순위 4항목 중 가장 큰 ratio (10-15% 예측)의 **`process_forward_entry`
8 KB staging memcpy 제거**.

### 12.1 변경 사항

| 파일 | 변경 |
|---|---|
| `object.h` | `pod_state.local_mmap_dpa_handle` 필드 추가 (uint32_t — SDK 일치) |
| `dpa.c` | `setup_pod_dma`에서 DPA mmap handle 캐싱 |
| `dpu_worker.c` | `dpu_enqueue_reverse_dma` signature (memcpy / write_pos 제거); `process_forward_entry` 성공 path TX_ACK 제거 (에러 path만); `process_rev_notify_entry`에 reverse 완료 후 TX_ACK 추가; `send_or_defer_tx_ack` 헬퍼 |
| `device/dpa_kernel.c` | reverse handler가 `desc->mmap` / `desc->addr`를 source로 사용. `desc->mmap == 0`이면 legacy `dpu_mmap` fallback |

**Slot lifecycle 변화**: src `dma_buffer` slot 점유 시간이 forward-only → **full
RTT**. host TX slot도 reverse 완료 후 TX_ACK 받아야 release. sustainable
52 K @ 5.5 ms RTT = ~290 in-flight slots, `DMA_RING_SIZE = 2048` 안에 fit.

**Trade-off**: dst pod별 staging buffer가 사라지면서 한 src가 여러 dst로
보낼 때 **HOL blocking across destinations from same source**. echo bench
(src==dst) 영향 없음 — multi-dst 워크로드에서만 manifest (사용자 합의).

### 12.2 RPS sweep 비교 (8 KB, 10 s, conns=auto)

| Target | Baseline ach | New ach | Baseline p99 | New p99 | Δ p99 |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 4,968.6 | 4,968.8 | 1.93 ms | 1.92 ms | -0.5% |
| 10,000 | 9,939.2 | 9,935.9 | 2.82 ms | 2.78 ms | -1.4% |
| 30,000 | 29,807.8 | 29,800.9 | 6.51 ms | 6.38 ms | -2.0% |
| 50,000 | 49,664.7 | 49,655.3 | 11.89 ms | 9.94 ms | -16.4% |
| 52,000 | 51,644.8 | 51,625.3 | 12.16 ms | 12.12 ms | -0.3% |
| **53,000** | — | **52,641.9** | — | **12.23 ms** | 새 sustainable |
| **54,000** | — | **53,636.3** | — | **12.38 ms** | 새 sustainable |
| 55,000 | 53,553.4 | 54,085.4 | 189.59 ms | 101.39 ms | **-46.5%** |
| 60,000 | 53,698.8 | 54,198.6 | 1,154.25 ms | 998.98 ms | -13.5% |
| 65,000 | 53,841.1 | 54,748.5 | 1,961.63 ms | 1,767.98 ms | -9.9% |
| 70,000 | — | 54,369.7 | — | 2,817.77 ms | overload |

**결과**:
- Sustainable 52 K → **54 K** (+~4%, p99 < 13 ms)
- Overload ceiling 53,553 → **54,748** (+~2%)
- Overload p99 약 절반 (55 K: 190 → 101 ms)
- 0 failure, 11회 sequential test slot leak 없음

| | dma_copy ops/sec | vs Method 0 | vs Method 2 |
|---|---:|---:|---:|
| dpumesh baseline @ overload (53,553 RPS) | 214,212 | 67% | 69% |
| **dpumesh new @ overload (54,748 RPS)** | **218,992** | **68%** | **71%** |
| dpumesh new @ sustainable (53,636 RPS) | 214,544 | 67% | 69% |

---

## 13. Yield/Wake Architecture Cost — M2에 wake 메커니즘 이식

§10.5의 "DPA scheduling overhead" 후보 (~0.4-0.6 µs/op 추정)를 직접 측정.

### 13.1 1단계 — M2 + naive yield (wake source 없음)

M2 DPA kernel에 `thread_reschedule()` 추가 (273 dma_copy 마다).

**측정: 273 recv/s** — 정확히 1 burst 처리 후 영원히 hang.

→ `doca_dpa_dev_thread_reschedule()`는 **진짜 yield**. 외부 wake source 없으면
DPA가 깨어나지 못함. cooperative 아니라 **block형 suspend**.

### 13.2 2단계 — M2 + yield + wake (DPU 1 kHz keepalive 이식)

dpumesh의 wake 메커니즘 모방:
- M2 DPU worker에 `dmesh_doca_dpa_msgq_send`로 매 1 ms trigger msg 발사
- M2 DPA kernel의 yield 직전 consumer comp queue 드레인 + ack
- yield counter를 file-scope global로 (function-local static은 thread re-entry
  시 reset됨 — 아래 발견 참조)

| Variant | recv/s | per-op time | Δ vs E0 |
|---|---:|---:|---:|
| E0 (M2 busy-spin) | 321,803 | 3.11 µs | — |
| **M2 + yield + 1 kHz wake (burst=273)** | **272,928** | **3.66 µs** | **+0.55 µs = -15.2%** |
| M2 + yield + 1 kHz wake (burst=1000) | 249,916 | 4.00 µs | +0.89 µs = -22.3% |

### 13.3 발견 — DPA thread re-entry 시 function-local static reset

처음 시도: `static int yield_counter`. 결과 999 recv/s. 분석: DPA thread가
`thread_reschedule()` 후 **함수 entry부터 restart**. function-local static 재초기화
→ 매 wake마다 1 dma_copy 후 다시 yield. 해결: file-scope global + function
entry에서 reset.

### 13.4 Yield 비용의 정체

burst 273 (=849 µs 처리)와 wake interval 1 ms (=1000 µs) mismatch가 비용
주된 원인. burst 1000 (=3110 µs)으로 늘리면 wake interval보다 길어져 다른
mismatch → -22% 더 악화.

dpumesh의 731 exec/s × 273 ops/exec = 200K도 같은 패턴. 비용 줄이려면:
- Wake rate를 burst 처리 시간에 매칭 (250 Hz wake + 1000 op burst)
- 또는 wake-on-event (host trigger desc post 마다)

dpumesh는 실제로 host trigger도 함께 사용 (`dpumesh_enqueue`가 DPU trigger).
1 kHz keepalive는 idle fallback. multi-source wake가 dpumesh ~200K 도달의 이유.

---

## 14. DPUmesh Busy-Spin — Architectural Necessity 검증

### 14.1 §11.1 strips **전** busy-spin (yield 제거)

| Target | per-batch ach / p99 | **DPA busy-spin** ach / p99 |
|---:|---:|---:|
| 50K | 49,629 / 9.89 ms | 49,605 / 9.54 ms |
| 55K | 54,080 / 91.06 ms | **53,562 / 224.4 ms** |
| 65K | 54,291 / 1,894 ms | 53,398 / 2,064 ms |

55K p99 91 → 224 ms (**2.5배 악화**). dpumesh 4 rings × busy-spin = PCIe poll
bandwidth가 실제 DMA traffic과 경쟁. yield가 **PCIe polling rate throttle** 역할.

### 14.2 §11.1 strips **후** busy-spin 재시도

가설: strips가 DPA EU per-dma_copy work 줄였으니 PCIe BW 헤드룸 생겨 결과 다를 수 있음.

| Target | yield 유지 (§11.1 baseline) | **busy-spin (재시도)** |
|---|---:|---:|
| 50K | 49,659 / 9.36 ms | **49,614 / 8.67 ms** |
| 55K | 54,617 / 12.27 ms | **54,628 / 9.61 ms** |
| 65K | 59,195 / 952 ms | **59,784 / 819 ms** |

표면적으로는 모든 target p99 개선 (50K -7%, 55K **-22%**, 65K -14%).

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

**Slot leak — host ring `head=160` slot이 `valid==1` 채로 stuck**.

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

Recovery 로직 0:
- ❌ Stuck slot timeout / Force-clear / head++ skip / Ring reset / Backoff escalate

설계가 "DPA reliable" 가정.

### 14.5 Leak mechanism (logical trace)

DPA의 `process_one_desc`는 모든 종료 path (success / abort / consumer_empty
timeout)에서 `desc->valid = 0` + writeback. logic만으론 leak 불가.

가능한 race:
1. **DPA window cache stale read** — host의 `valid=1` write가 coherency 통해
   DPA window cache line invalidate해야 하는데, busy-spin의 PCIe + coherency
   부담으로 invalidation 누락/지연. DPA가 stale `valid=0` 봄 → desc_idx
   advance 안 함 → 그 slot 영원히 skip.
2. **DPA → host writeback propagation 누락** — 5+ 초 갭이면 propagation 시간
   충분 → less likely.

→ (1)이 plausible한 유일 path. 정상 coherency라면 발생 안 해야 하지만
busy-spin stress에서 corner case 발생.

### 14.6 종합 — yield는 architectural necessity

| | §14.1 (strips 전) | §14.2-14.3 (strips 후) |
|---|---|---|
| Single-run | 55K p99 91 → **224 ms 악화** | 55K p99 12 → **9.6 ms 개선** |
| Back-to-back 안정성 | 미측정 | **fail (slot leak)** |
| 결론 | busy-spin 안 좋음 (PCIe poll storm) | busy-spin 안 좋음 (slot leak) |

dpumesh가 **multi-pod / multi-ring / lossless**인 한 yield 불가피:
- multi-ring busy-spin = PCIe + coherency stress
- coherency race = `valid` bit stuck 가능성
- lossless ring 모델 = stuck slot이 hang으로 표면화 (M2 lossy면 그냥 덮어씀)
- **yield의 ~0.55 µs/op 비용 (§13)은 이 architectural 보호의 가격**

§11.1 strips로 +9% 회복했지만 busy-spin으로 더 짜내면 stability 잃음.
**(yield + strips + §11.5 cleanup + §11.6 packing + §11.7 SDK delegation)이 production 최적점**.

---

## 15. Flow Control Audit

dpumesh flow control은 7 layer + 3 retry queue:

| # | Layer | 위치 | 메커니즘 | 크기 | 필수성 |
|---|---|---|---|---:|---|
| 1 | DPA producer slot | DPA EU | SDK 내부 관리 (§11.7 위임, drain ack로 큐 순환) | 1024 | 필수 |
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

### 15.1 3 retry queue 단순화 가능성 — **통합 비추천**

| Queue | Entry | Trigger | Drain | Lock |
|---|---|---|---|---|
| `deferred_tx_acks` | `{conn*, req_id, dst_pod_id}` 20B struct | `server_send_tx_ack_to()` → `DOCA_ERROR_AGAIN` (send pool exhausted) | main loop, **`pe_progress` 직후** (released slots 활용) | lock-free (single PE-callback producer) |
| `deferred_recv` | `struct doca_task *` 포인터 | comp_queue ≥ BP_HIGH 또는 `doca_task_submit` 실패 | main loop, `comp_queue < BP_LOW` 시 | lock-free |
| `consumer_retry` | `struct doca_task *` 포인터 | gated `pool_try_acquire` 성공 후 `task_submit` 실패 (rare) | `objects_drain_consumer_retry()` | **mutex** (PE callback ↔ main loop 경합) |

통합 비추천 이유:
1. **Entry shape 비대칭**: tx_acks 20B struct vs 나머지 8B 포인터 → union으로
   묶으면 12B × 2000 entries ≈ 24KB 메모리 낭비.
2. **Lock 비대칭**: consumer_retry만 mutex 필요. 통합 시 무락 큐들이 락
   코스트 떠안게 됨 — `pe_progress` 직후 hot path에 mutex 진입 → 즉시 regression.
3. **Drain timing 다름**: tx_acks는 send-pool 자유화 직후. recv는 comp_queue
   depth 기준. consumer_retry는 다음 메인 루프. 단일 drain 호출에 분기 필요.

분리가 정당. 그대로 둠.

### 15.2 추가 단순화 후보 (성능 영향 미미)

- (a) `send_tasks_in_flight` mirror 제거 (DOCA capability check 지원 확인 후):
  ~0.1 µs/dma_copy 절감
- (b) `is_consumer_empty` spin에 backoff: 미세 latency 개선

### 15.3 결론

Flow control 자체는 **correctness 측면에서 잘 설계됨**. throughput cap의 주된
원인 아님 (§10-11의 DPA kernel work가 더 큼).

---

## 16. HW 한계 측정 해석 가이드

목적: dpumesh가 §7의 Method 2 ceiling (77.6 K RPS = 310,472 dma_copy ops/s
÷ 4)에 host-side 자원을 충분히 줬을 때 얼마나 근접하는지.

`./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE>` — 자동으로 `pin_pods hw` 전환.

### 16.1 결과 해석 매트릭스

| `dpumesh-hw` 결과 | 해석 | 다음 실험 |
|---|---|---|
| RPS ≈ 60-62 K (§11.5와 동일) | chain 자체가 cap | DPU `top -H -p $(pgrep dpumesh_dpu)`로 ARM core 100% 확인 |
| 65-75 K | chain 가까이 도달. host-side lock/scheduling이 fair-mode cap | bench/echo 코어 더 늘려서 한계 측정 |
| > 77 K | Method 2 baseline 잘못됐거나 측정 노이즈 | sweep 재실행 |
| RPS 떨어짐 (< 60 K) | multi-core thread thrashing. cache locality 손실 | core 개수 줄이거나 NUMA 구분 |

### 16.2 보조 측정

- **CPU 사용률**: `mpstat -P 0-7 1`
- **DPU ARM CPU**: DPU에 ssh + `top -H -p $(pgrep dpumesh_dpu)`
- **DPA EU stat**: `dpa-statistics show`의 Cycles, producer drain stall,
  consumer_empty wait

### 16.3 추가 변수

- **msg_size 작게 (1 KB)** — per-msg overhead 격리. 8 KB는 DMA 비중 큼, 1 KB는
  ops 자체 cap. ops/sec 비교에 더 깨끗.
- **bench 측 thread 수 (workers)** — 기본값 (rps/100)이 cap region에서
  thrashing 유발 가능.

---

## 17. Single-pod / Idle Ring 효과 (분석적 추정)

dpumesh 4 rings 중 일부가 idle (예: bench self-loop으로 echo pod ring 2개 idle):
- §10.4 측정: 4 rings 다 active = ratio 1.017 (perfect amortize)
- Single-pod self-loop: 2 active + 2 idle = **ratio 2.0** (idle ring 폴링 낭비)

§11.4의 single-op per-extra-poll cost ~0.8 µs. multi-op amortized cost ~0.19 µs
(E9→E10의 per-extra). 추정:
- 정상 ratio 1.017 → idle 시 ratio 2.0 = 추가 1 poll/op = **+0.19 µs/op**
- dpumesh 4.55 → 4.74 µs/op → **~211K dma_copy/s** (현재 248K의 -15%)

ring 개수 자체는 작은 영향 (다 active 시), idle ring이 폴링 낭비 만들 때만
중요. 현재 echo bench는 4 rings 모두 active.

---

## 18. 최종 Attribution

### 18.1 누적 ceiling 변화

| Phase | Sustainable elbow (8KB, p99 ≤ ~14 ms) | Overload throughput | DMA ops/s |
|---|---:|---:|---:|
| §9.2 baseline (pre in-place) | 52K (p99 12 ms) | 53,841 @ 65K | 215,364 |
| §12 in-place forwarding | 54K (p99 12.4 ms) | 54,748 @ 65K | 218,992 |
| §11.1 STRIP_VALIDATION/DESC_CLEAR/CHUNK_LOOP | 55K (p99 12.3 ms) | 59,195 @ 65K | 236,780 |
| §11.5 cleanup (dead code + lazy drain + throttle) | 60K (p99 12.6 ms) | 62,068 @ 65K | 248,272 |
| §11.6 comp_msg packing (28B → 16B) | 65K (p99 12.9 ms) | 65,859 @ 67K | 263,436 |
| **§11.7 SDK delegation + OPTIMIZE_REPORTS** | **68K (p99 13.7 ms)** | **68,127 @ 70K** | **272,508** |

vs M2 ceiling (310,472 ops/s = 100%):
- baseline 69% → in-place 71% → strips 76% → §11.5 cleanup 80% → §11.6 packing 85% → **§11.7 SDK 88%** (vs M2)
- vs M0 (320,014 ops/s): 67% → 68% → 74% → 78% → 82% → **85%**

Throughput milestone: §11.6에서 1 GB/s 돌파 (65K @ 1,008 MB/s), §11.7에서 1,065 MB/s @ 70K target.

### 18.2 E0 (321,803 dma_copy/s) baseline 기준 attribution

§11.5/§11.6/§11.7 cleanup 이전 dpumesh-vs-E0 갭 = **1.44 µs/op = 32%**.

| 항목 | 비용 (µs/op) | dma_copy/s loss | 출처 | 회복 상태 |
|---|---:|---:|---|---|
| Larger comp_msg (7 fields, 28B) | 0.29 | ~27K | §11.3 E5 | **§11.6 packing 16B로 부분 회복** |
| Per-call `ensure_producer_slot` | 0.17 | ~17K | §11.2 E3 (M2 add, -5.1%) | **§11.5 lazy drain + §11.7 SDK 위임으로 회복** |
| `handle_msgs` per iter | 0.06 | ~6K | §11.3 E6 (M2 add, -2.0%) | **§11.5 throttle로 회복** |
| Chunking loop wrapper | 0.05 | ~5K | §11.2 E2 (M2 add, -1.6%) | §11.1 CHUNK_LOOP fast path로 회복 |
| Desc clear (in lossless context) | 0.05 | ~5K | §11.1 STRIP_DESC_CLEAR (dpumesh remove) | 회복 |
| Validation | ~0 | 0 | §11.2 E1 (noise) | §11.1 STRIP_VALIDATION로 회복 |
| Extra consumer empty wait | ~0 | 0 | §11.3 E7 (noise) | — |
| **측정 합계 (E1-E7 + §11.1)** | **0.62** | **~60K (59%)** | direct | |
| **Yield/wake architecture** | **0.55** | — | **§13.2 (M2 + yield+wake, -15.2%)** | architectural (보존) |
| Cache footprint + amortization 불완전성 | ~0.27 | — | 잔여 | — |
| **합계 = 실측 갭** | **1.44** | **~102K** | **E0 → dpumesh, 82% attributed** | |

§11.5 cleanup 적용 → **+11.5K dma_copy/s 실측 회복** (248K vs 236K, E3+E6 23K
잠재의 50%).
§11.6 packing 적용 → **추가 +15.2K dma_copy/s 실측 회복** (263K vs 248K,
E5 27K 잠재의 56%).
§11.7 SDK 위임 + OPTIMIZE_REPORTS 적용 → **추가 +9.1K dma_copy/s 실측 회복**
(272K vs 263K, completion 통보 batch + per-call check 제거).
누적 회복 **+35.7K/106K** = E1-E7 측정 가능 갭의 **60%**.

### 18.3 부정된 가설

- ❌ **PCIe BW contention** (E11: 2H+2D vs 3H+1D = -0.3% noise)
- ❌ **ARM polling이 cap** (per-batch pe_progress 변경에도 RPS flat;
  poll/copy ratio = 1.017 측정)
- ❌ **DPA busy-spin이 도움** (§14.1 latency 2.5× 악화, §14.2-14.3 slot leak)
- ❌ **comp_msg quantum ≥ 32B** (§11.6 quantization 표 — 16B HW WQE BB 확정)

### 18.4 남은 최적화 후보 (single-core)

| 변경 | 잠재 이득 | 비고 |
|---|---:|---|
| Doorbell coalescing (drain_all_rings inner iter당 1× FLUSH) | 측정 후 결정 | OPTIMIZE_REPORTS만 켜는 1줄 변경은 §11.5 CAP 패턴과 충돌 — §11.7로 해소됨. 다음 단계는 N-1 op에만 OPTIMIZE_REPORTS 켜고 마지막에 FLUSH 발사하는 batch boundary 추적 필요 |
| Multi-EU DPA thread | ~+50-100% (이론) | §10.1 EU 0.51% active = idle 큼. 단일 EU yield/wake cycle이 cap. 2 EU 분할 (forward/reverse 또는 pod별) 시 throughput 합산. 구조 변경 큼 |
| `dma_desc` cache line split (host-write vs DPA-write 영역 격리) | ~+5-10% (가설) | §11.2 E4의 100× 비대칭은 lossy ring 한정이지만 dpumesh도 같은 cache line 공유. valid 비트만 별도 cache line으로 격리 시 PCIe coherency cost 절감 |
| Wake mechanism 재조정 (1 kHz keepalive 빈도/제거) | 측정 후 결정 | §13.4에서 거론만. 현재 host trigger + 1 kHz keepalive 병용. keepalive 낮추거나 제거하면 wake mismatch 변화 |
| comp_msg 추가 packing (16B → 12B) | 측정 필요 | §11.3 E5에서 12B가 M2 baseline. `pos`/`length`를 uint16_t로 줄이면 가능 (≤8KB 한도 내). 단 16B HW WQE BB quantum 아래로 내려가도 추가 quantum 절약 없을 수 있음 |
| §15.2 (a) `send_tasks_in_flight` mirror 제거 | +0.1 µs/dma_copy | DOCA capability check 필요 |

남은 0.27 µs unknown은 DPA scheduling 패턴, cache footprint, amortization
불완전성 — single-feature 추가/제거로 잡히지 않는 integration-level cost.
architectural 변경 필요.

---

## 19. 측정 환경 종합

### 19.1 Hardware

| 항목 | 사양 |
|---|---|
| Host CPU | DVFS lock @ 2.5 GHz, governor=performance, cores 0-7 |
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

### 19.3 dpumesh 정리 후 적용된 변경 (merge 대상)

§10.4 / §11.1 / §11.4 / §11.5 / §11.6 / §11.7의 측정으로 채택된 항목만 영구
코드로 남기고 실험용 toggle / instrumentation은 모두 제거.

| 파일 | 변경 | 근거 |
|---|---|---|
| `comch_server.c` + `object.h` | `pods_*` mutex → `__atomic_*` publication (lock-free hot read path) + 동기화 모델 코멘트 | hot read path 락 제거 |
| `object.h` | `comp_queue_*` helpers `always_inline` 강제 | -O2 self frame 남는 문제 |
| `dpu_worker.c` | `process_completion_queue` per-entry `pe_progress` × 2 → per-batch 1회 | §11.4 (RPS flat, 55K p99 -10.2%) |
| `device/dpa_kernel.c` | descriptor 검증 (mmap=0 / size=0 / range / buf-overflow) 제거 | §11.1 Exp A (+1.7%) |
| `device/dpa_kernel.c` | desc 종료 시 4-field clear + 3× writeback → `valid=0` + 1× writeback | §11.1 Exp B (+1.7%) |
| `device/dpa_kernel.c` | size ≤ 8KB 단일 dma_copy fast path + >8KB 명시적 drop guard | §11.1 Exp C (+5.5%) |
| `device/dpa_kernel.c` | chunking fallback else 분기 ~120줄 제거 (host `check_slot_size`로 도달 불가) | §11.5 dead code |
| `device/dpa_kernel.c` | `run_dma_manager` yield 유지 (코멘트로 측정 근거 보존) | §13/§14 |
| `device/dpa_kernel.c` | `handle_msgs` throttle (매 32 inner iter) + `drain_producer_completions` throttle (매 8 inner iter) | §11.5 throttle (§11.3 E6 inverse) |
| `device/dpa_kernel.c` + `dpa_common.h` | `producer_slots_inflight` 카운터 + `ensure_producer_slot` CAP/abort 패턴 제거 → SDK가 producer-side backpressure 관리, drain은 ack만 유지 (`producer_slots_inflight` 필드는 `_pad1` 으로 교체해 struct 정렬 보존) | §11.7 SDK 위임 (M2 baseline 패턴 일치, OPTIMIZE_REPORTS 호환 위한 선행 조건) |
| `device/dpa_kernel.c` | 두 `dma_copy` 호출에 `DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS` 추가 (FLUSH와 OR) | §11.7 SDK delegation (+3.4% throughput) |
| `dpa_common.h` | `comch_dma_comp_msg` 28B → 16B packing (type → uint8_t, src/dst_pod_id → int8_t, 필드 재배치로 자연 정렬 유지) + `_Static_assert(sizeof == 16)` | §11.6 packing (§11.3 E5 inverse, +5.6% throughput) |
| `dpa.c` | DPA→DPU recv handler의 type read `*(enum *)raw` (4B) → `raw[0]` (1B) — packed struct 정합 | §11.6 packing 동반 |
| `dma.c` / `dma.h` 완전 삭제, `dpa_kernel.c` 의 DMA_REQ case (80줄), `dpa_common.h`의 `comch_dma_req_msg` + enum + union field, `dpa.c`의 DMA_CHUNK case | dead chain (caller 0) ~150줄 | §11.5 dead code |
| `comch_*`, `dpa.c`, `dpu_worker.c`, `ring.c` | hot path `DOCA_LOG_DBG` 제거 | `-l 40` 정책 일관성 |

§10.4의 결정적 측정 (poll/copy=1.017)에 쓰였던 `stat_inner_iters /
stat_polls / stat_dma_copies` 카운터 및 `dpu_worker.c`의 `doca_dpa_d2h_memcpy`
로깅 블록은 임무 완료로 제거.

### 19.4 M2 baseline (test_dma/bench) 현재 상태

| 파일 | 변경 |
|---|---|
| `device/dpa_kernel.c` | `DOCA_DPA_DEV_LOG_*` 모두 no-op |
| `device/dpa_kernel.c` | `EXTRA_RING_POLLS=0` (E0 baseline 복원) |
| `device/dpa_kernel.c` | `ADD_VALIDATION`/`CHUNK_LOOP`/`PRODUCER_SLOT`/`DESC_CLEAR_*`/`LARGER_COMP_MSG`/`HANDLE_MSGS`/`EXTRA_CONSUMER_WAIT`/`REVERSE_DMA`/`4_DMA_COPIES`/`YIELD`/`YIELD_WITH_WAKE` 매크로 모두 OFF |
| `dpa.c` | `DOCA_LOG_*` 모두 no-op |
| `dpu_worker.c` | `DOCA_LOG_INFO/ERR/DBG` no-op (WARN만 유지 — `recv:` stat 출력용) |

### 19.5 Flame graph 파일

| 파일 | 캡처 시점 |
|---|---|
| `bench/m2_dpu_flame.svg` | M2 baseline (Method 2 chain) |
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
  path에서 제거.
- **echo 32 thread**: 단일 thread는 ~25K RPS에서 막힘 (서버 측 큐잉 3 ms+ p50).
  32 thread로 dequeue 병목 해소.
- **wall-time 측정**: bench_dpumesh.c는 watchdog thread + 즉시 join 패턴. main이
  고정 시간 sleep하면 wall이 부풀려져 RPS underreport.
- **wrk2식 scheduled-time latency (CO 보정)**: §6.1 참조. cap 너머 가짜 plateau 제거.
- **MAX_WORKERS = 4096, 128 KB stack**: closed-loop concurrency cap 낮으면
  (이전 512) Little's law로 in-flight 묶임. 4096 × 128 KB = 512 MB.
- **Envoy 최소 설정**: tcp_proxy filter 1개, cluster 1개. admin/HTTP/tracing/
  stats sink/access log/runtime 모두 제거. DPUmesh가 transport-only인 것에 대응.
- **2 sidecar (1개 아님)**: Istio/Envoy 모델은 client-side + server-side 두 hop.
  socat 단일 splice는 너무 가벼움 (kernel fast-path) → 비교 부정확.
- **pinning은 taskset (CFS quota 아님)**: CFS quota는 시간만 제한, core 안 정함.
  같은 core 공유시키려면 pinning 필수. CFS는 redundant + 잘못된 throttling 가능성.
- **DPU/DPA는 pinning 안 함**: "DPU/DPA가 host CPU와 독립"이 dpumesh 핵심 advantage.
  임의로 묶는 건 비교 의의 깎음.
