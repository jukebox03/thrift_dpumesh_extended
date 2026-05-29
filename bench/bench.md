# DPUmesh vs TCP-sidecar Benchmark

DPUmesh transport와 TCP service-mesh (Envoy sidecar) 의 raw 성능을 비교하기 위한
실험 환경. gateway / Thrift / 다른 application 로직 없이 transport 비용만 분리.

---

## 1. 비교 대상 (architectural goal)

| | A안 (DPUmesh) | B안 (TCP via Envoy sidecar) |
|---|---|---|
| client → server hop | 1 (DPU) | 2 (sidecar1, sidecar2) |
| transport 위치 | DPU ARM + DPA EU (host CPU와 독립) | host CPU 안 (app과 같은 core 공유) |
| L7 처리 | 안 함 (transport-only) | 안 함 (Envoy `tcp_proxy` filter만) |
| 용도 | DMA-기반 mesh 우회 | Istio/Envoy 모델의 1:1 비교 |

핵심 가설: **DPUmesh의 가장 큰 architectural advantage는 transport 작업이 host CPU
외부 (DPU/DPA) 에서 일어난다는 것.** 그래서 같은 host CPU 예산을 양쪽에 동등하게
주면 (각 1 core × 2 pod = 2 cores), B안은 sidecar가 app과 core를 나눠 쓰는 반면
A안은 app이 1 core를 통째로 쓸 수 있음.

---

## 2. Topology

```
═══════════════════════════════════════════════════════════════════════════════════
                    HOST NODE — Namespace: test-bench
                    governor=performance @ 2.5GHz fixed (cores 0-7)
═══════════════════════════════════════════════════════════════════════════════════

  ╔═════════════ A안 (dpumesh) ═════════════╗
  ║                                          ║
  ║ core 0 ─ Pod: bench-dpumesh              ║   사용자
  ║         container: bench_dpumesh         ║◀── ./test-bench.sh dpumesh ...
  ║         workers ≈ rps/100                 ║   nc <pod_ip>:9092 RUN ...
  ║         pod_id=10, dst=11                 ║
  ║         libthrift.so + /dev/infiniband    ║
  ║                  │                        ║
  ║                  │ comch + DMA (mlx5/PCI) ║
  ║                  ▼                        ║
  ║           ┌─ DPU + DPA ─┐                 ║
  ║           └───┬─────────┘                 ║
  ║               │                           ║
  ║ core 1 ─ Pod: echo-dpumesh                ║
  ║         container: echo_dpumesh           ║
  ║         ECHO_THREADS=32 worker threads    ║
  ║         pod_id=11, Service 없음          ║
  ╚═══════════════════════════════════════════╝

  ╔═════════════ B안 (tcp via Envoy) ═══════════════╗
  ║                                                  ║
  ║ core 2 ─ Pod: bench-tcp (2 containers, share core)║◀── ./test-bench.sh tcp ...
  ║         ┌ bench_tcp                              ║    nc <pod_ip>:9092 RUN ...
  ║         │   TARGET=127.0.0.1:9091                ║
  ║         │       │ localhost TCP                  ║
  ║         │       ▼                                ║
  ║         └ sidecar1 (Envoy distroless v1.30)      ║
  ║             tcp_proxy ONLY (no L7/admin/tracing) ║
  ║             upstream=echo-tcp:9091 (k8s svc)     ║
  ║                  │                                ║
  ║                  ▼                                ║
  ║ core 3 ─ Pod: echo-tcp (2 containers, share core) ║
  ║         ┌ sidecar2 (Envoy)                       ║
  ║         │   listener:9091, upstream:127.0.0.1:9092║
  ║         │       │ localhost TCP                  ║
  ║         │       ▼                                ║
  ║         └ echo_tcp (Go, listen 9092)             ║
  ╚══════════════════════════════════════════════════╝

═══════════════════════════════════════════════════════════════════════════════════
              DPU (BlueField, host CPU 외부)        │ /dev/infiniband (PCI)
═══════════════════════════════════════════════════════════════════════════════════

  ARM (8 cores, no pin)              DPA (FlexIO EU × 1)
  ┌──────────────────────────────┐   ┌────────────────────────────────┐
  │ dpumesh_dpu process          │   │ run_dma_manager (single EU)    │
  │  ▸ control PE (comch_server) │   │  ▸ drain_all_rings:            │
  │  ▸ consumer PE (DPA → ARM)   │◀─▶│      forward DMA  (host→DPU)   │
  │  ▸ dpu_worker:                │   │      reverse DMA  (DPU→host)   │
  │      pod_id 10/11 routing    │   │      lazy credit refresh        │
  │      TX_ACK 송신              │   │  ▸ chunked dma_copy             │
  │      reverse desc enqueue    │   │      (8KB/call, 128B aligned)   │
  └──────────────────────────────┘   └────────────────────────────────┘
```

---

## 3. Resource Layout

CPU pinning은 `taskset -apc <core> <pid>` 로 hard-pin (CFS quota 미사용).
`pin_pods()` 가 deploy 끝 + `pin` subcommand 에서 수행.

두 가지 pin profile 이 있음 — `fair` (TCP vs DPUmesh 1:1 비교) / `hw` (HW
한계 측정). `./test-bench.sh dpumesh` / `tcp` 는 자동으로 fair, `dpumesh-hw`
는 자동으로 hw 로 전환.

**Profile = fair** (default, TCP vs DPUmesh 비교용)

| core | pod | container(s) | 비고 |
|------|-----|--------------|------|
| 0 | bench-dpumesh | bench_dpumesh | 1 process, 다중 worker thread |
| 1 | echo-dpumesh | echo_dpumesh | ECHO_THREADS=64 |
| 2 | bench-tcp | bench_tcp + sidecar1 | 두 container 같은 core 공유 |
| 3 | echo-tcp | echo_tcp + sidecar2 | 두 container 같은 core 공유 |
| (4-7) | — | — | DVFS lock 범위에만 포함 |
| DPU | dpumesh_dpu | ARM 8 cores 자유 | **pin 안 함 (offload 강점)** |
| DPA | run_dma_manager | EU × 1 | 코드 상수 |

**Profile = hw** (HW 한계 측정용, dpumesh 측만 multi-core)

| core | pod | container(s) | 비고 |
|------|-----|--------------|------|
| 0, 4 | bench-dpumesh | bench_dpumesh | 2 cores — host lock 경합 완화 |
| 1, 5 | echo-dpumesh | echo_dpumesh | 2 cores — ECHO_THREADS=64 분산 |
| 2 | bench-tcp | (untouched) | 측정 의미 없음 — 자원 비대칭 |
| 3 | echo-tcp | (untouched) | 측정 의미 없음 |
| 6, 7 | — | — | 사용 안 함 (DVFS lock만) |

`hw` 모드 결과는 TCP 와 직접 비교 불가 (자원량 다름). 오직 chain ceiling
(Method 2 clean = 77.6 K RPS = 310,472 dma_copy ops/s ÷ 4) 에 dpumesh 가 host-side
자원을 충분히 줬을 때 얼마나 근접하는지 측정용. 이게 여전히 ~54 K 에서
막히면 진짜 cap 은 DPU ARM single-thread 의 per-op 비용 (§6.2.4 참조).

DVFS: `cpupower -c 0-7 frequency-set -g performance -d 2.5GHz -u 2.5GHz`
→ latency tail noise 제거.

---

## 4. K8s 객체 (test-bench namespace)

```
Deployments              Services             ConfigMaps
────────────             ──────────           ────────────
bench-dpumesh (1c)       bench-dpumesh:9092   sidecar1-config
echo-dpumesh  (1c)       bench-tcp:9092         (Envoy yaml,
bench-tcp     (2c)       echo-tcp:9091          upstream=echo-tcp:9091)
echo-tcp      (2c)       (echo-dpumesh 없음)  sidecar2-config
                                                (Envoy yaml,
                                                upstream=127.0.0.1:9092)

hostPath volumes (privileged)
─────────────
/dev/infiniband               ← bench-dpumesh, echo-dpumesh
$BUILD_DOCA/lib (libthrift)   ← bench-dpumesh, echo-dpumesh
```

---

## 5. Run

```bash
# 한 번 배포 (build + image + DPU restart + pods Ready + fair 핀)
./test-bench.sh deploy

# === Fair 비교 (TCP vs DPUmesh, 양쪽 1 core) ===
./test-bench.sh dpumesh    <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh tcp        <RPS> <DUR> <SIZE> [<CONNS>]

# === HW 한계 측정 (dpumesh 측만 multi-core, chain ceiling 추적) ===
./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE> [<CONNS>]

# pod 재시작 후 재핀 (필요 시 모드 명시)
./test-bench.sh pin        # = pin-fair
./test-bench.sh pin-hw

# 정리
./test-bench.sh cleanup
```

`dpumesh` / `tcp` / `dpumesh-hw` 명령은 실행 직전 자동으로 해당 profile 로
재핀하므로 모드 토글을 신경쓸 필요 없음.

ctrl protocol (line 기반):
```
RUN <rps> <dur_sec> <msg_size> [<conns>]
→ OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>
```

> bench daemon 의 raw 출력은 **microseconds**, 본 문서의 표는 **ms** 로
> 통일 (보기 편하게). test-bench.sh 가 출력하는 us 값을 ÷1000 하면 ms.

---

## 6. 실험 결과

### 6.1 첫 측정 (rps=40000, dur=10s, size=8192B, conns=auto)

> Latency 측정은 wrk2 식 **scheduled-time** 기준 (coordinated omission 보정).
> `t0 = 보내기로 예약된 tick`, `now() - t0`. saturation 너머에서 큐잉
> 지연이 latency 에 그대로 잡힘. 자세한 건 §8 참고.

| | dpumesh | tcp/Envoy |
|---|---|---|
| Target RPS | 40,000 | 40,000 |
| **Achieved RPS** | **39,732.7** (99.3%) | **33,097.0** (82.7%) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 latency | **4.53 ms** | **661.50 ms** |
| p99 latency | **8.35 ms** | **3,885.30 ms** |
| p999 latency | 8.62 ms | 4,588.43 ms |
| Throughput (RTT) | 620.82 MB/s | 517.14 MB/s |

**관찰**: 40 K target 에서 dpumesh 는 healthy region (sustainable saturation
52 K 의 76 %), 반면 TCP/Envoy 는 이미 **deep overload** — achieved 가
33.1 K 에 그치고 큐잉 지연이 p99 3,885 ms 까지 폭증. 즉 TCP/Envoy 의 saturation
은 40 K 보다 한참 아래에 위치. 양쪽 sweep 으로 정확한 elbow 찾는 건 §6.2.2
방식 권장.

### 6.2 Transport ceiling 비교 (only DMA / DMA + completion / dpumesh)

`experiment/bench.md` 의 micro-bench (`/home/jukebox/test_dma/bench/`) — k8s ·
Thrift · TCP · gateway 모두 제거하고, host process 가 dma_ring 에 desc 직접
post 하는 minimal loop. comch_client + DPA RPC 만 사용 (확인: `host_worker.c`
의 `while(true) { get_next_dma_desc; desc->valid=1; }` 루프, Thrift/k8s 헤더
0개). 따라서 baseline 으로 valid.

**Caveat — micro-bench 와 dpumesh 의 chain 구성 차이**:

| | micro-bench (Method 2) | dpumesh |
|---|---|---|
| 데이터 흐름 | host → DPU → host (단방향, 1 endpoint) | host A → DPU → host B → DPU → host A (RTT, 2 endpoints) |
| DPU ARM | comch 수신 + `server_send_msg` 한 번 | comp_queue enqueue + **pod_idx 라우팅 + TX_ACK send + reverse DMA desc 작성 + tx_ring enqueue** |
| descriptor 라우팅 | 없음 | `dst_pod_id` 별 분기 |
| reverse direction | 없음 | 매 hop 마다 reverse DMA enqueue |

→ 같은 "1 dma_copy" 라도 dpumesh 측은 micro-bench 보다 DPU ARM 의 부수 작업이
많음. perf 실측 (§6.2.4) 결과 갭은 약 31 % 이며, 이 안에는 DPU ARM 의 routing /
TX_ACK / reverse-enqueue 작업 비용이 들어있음. **chain HW 자체는 한계가 아니고,
DPU ARM single thread 가 cap 결정자**.

#### 6.2.1 Micro-bench ceiling (clean re-measurement, 8 KB / 60 s)

> **2026-05 재측정**: 이전 표 (Method 0 = 302K, Method 2 = 264K) 는 DOCA library
> 의 `comch_server.c` 안 `DOCA_LOG_INFO("Server task sent successfully")` 가
> per-msg fire 해서 stdio I/O 가 chain 측정에 stale overhead 주입. test_dma 의
> `run_bench.sh` 에 `-l 40` 추가 + stat 로그를 WARN 레벨로 promote 해서 silence
> 하고 재측정. 결과 — Method 2 의 진짜 비용은 거의 없었음 (97 % vs M0).

| Mode | 구성 | dma_copy ops/sec | Throughput (1 dir) |
|---|---|---:|---:|
| **Only DMA** (Method 0) | `dma_copy` atomic (DMA + comch immediate, HW max) | **320,014** | **20.97 Gbps** |
| **DMA + completion** (Method 2) | `dma_copy` + DPU CPU 가 host 로 `server_send_msg` | **310,472** | **20.34 Gbps** (97 % of HW) |

Method 0 → Method 2 의 손실은 **3 %** (이전 13 % 로 보고 됐던 건 stdio artifact).
즉 "DPU CPU → host comch forward" 의 진짜 비용은 매우 작음. **dpumesh 가 이
chain 을 그대로 사용**하므로 Method 2 가 dpumesh 의 transport ceiling 의 직접
baseline.

#### 6.2.2 dpumesh saturation sweep (8 KB, 10 s, conns=auto)

> 모든 측정은 wrk2 식 **scheduled-time** 기반 (CO 보정). `MAX_WORKERS=4096` +
> 128 KB 스택으로 worker pool 이 cap 안 걸림. 따라서 latency tail 이 진짜
> 시스템 saturation 을 반영함.

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

**관찰** (CO 보정 후 본 진짜 saturation 모양):

- **Sustainable saturation ≈ 52 K RPS** (p50 5.5 ms, p99 12 ms, p999 12 ms).
  여기까지는 latency 가 큐잉 없이 service time 만 잡힘. 50 K 까지 거의
  perfectly linear (50 K → 49.7 K, 99.3 %).
- **Elbow = 52 K → 55 K 사이**. 55 K target 에서 p99 가 12 ms → 190 ms 로
  **16 배** 점프. 시스템이 sustainable rate 를 살짝 넘기는 순간 큐가 폭발
  적으로 자라는 전형적 M/M/1 saturation 패턴.
- **Overload throughput ceiling ≈ 53.5 K RPS** (60 K, 65 K target 으로 더
  밀어도 achieved 가 53.7 K 에 고정). 즉 시스템은 53.5 K 까지 무한히 큐를
  쌓아가며 처리는 하는데 latency 는 60 K 에서 1,154 ms, 65 K 에서 1,962 ms 로
  발산 — 이는 closed-loop self-throttling 이 아니라 진짜 시스템 한계의 신호.
  (이전 측정의 9.5 ms plateau 는 MAX_WORKERS=512 cap 의 가짜 한계였음.)
- **0 failure throughout** — `WAIT_TIMEOUT_MS = 5,000 ms` 보다 worst-case
  latency (~2,000 ms @ 65 K) 가 작아서 timeout 미발동. dpumesh 의 4-layer
  flow control (`tx_alloc` 무한 대기, `enqueue` 백오프, reverse-path admission
  gate) 이 cap 너머 큐잉 상황에서도 데이터 손실 없이 backpressure 만으로 흡수.
- **Throughput 은 836–841 MB/s 에서 평탄** — DMA chain 자체는 53.5 K ×
  4 dma_copy = 214 K ops/s 에서 hard cap. cap 너머에서 latency 만 늘어날
  뿐 throughput 은 이미 한계.

이 데이터는 closed-loop 가 아닌 진짜 open-loop semantics 에서 측정됐기
때문에 (1) sustainable rate (52 K), (2) overload ceiling (53.5 K), (3) 큐잉
지연이 latency 에 어떻게 반영되는지 — 셋이 모두 분리되어 보임. **운영
관점의 SLO 는 52 K @ p99 < 12 ms**, 그 이상은 latency 가 100 ms 단위로
들어감.

#### 6.2.3 dpumesh 의 ceiling 대비 위치

dpumesh request 1 RTT = forward DMA × 2 + reverse DMA × 2 = **4 dma_copy ops**.

```
   100% ┃ ┌── HW max (Method 0, dma_copy atomic) ─── 320,014 ops/s = 20.97 Gbps
        ┃ │
    97% ┃ ├── DMA + completion (Method 2)         ── 310,472 ops/s = 20.34 Gbps
        ┃ │            ↑ dpumesh 가 쓰는 chain 과 같은 구조
        ┃ │
    67% ┃ └── dpumesh @ overload ceiling           ── 214,212 ops/s = 13.71 Gbps
        ┃     (53,553 RPS × 4 dma_copy)
```

| | dma_copy ops/sec | vs Only DMA (Method 0) | vs DMA+completion (Method 2) |
|---|---:|---:|---:|
| Only DMA (HW max) | 320,014 | 100 % | — |
| DMA + completion | 310,472 | 97 % | 100 % |
| **dpumesh @ overload ceiling (53.55 K RPS)** | **214,212** | **67 %** | **69 %** |
| dpumesh @ sustainable (51.64 K RPS, p99 12 ms) | 206,580 | 65 % | 67 % |

**의미 있는 비교는 69 % (vs Method 2)** — Method 2 는 dpumesh 와 동일한
"DMA + DPU CPU → host server_send" chain 을 쓰는 구조라 같은 ground 위 비교.
Method 0 은 DPU CPU forward 비용이 빠진 absolute HW max 인데 둘 차이가 3 % 밖에
안 나서 사실상 같은 ceiling 을 의미함.

**Throughput 관점**:
- dpumesh DMA 사용량 @ overload ceiling: 53.55 K × 32 KB = **13.71 Gbps**
- HW max: 20.97 Gbps → 약 **7.3 Gbps (35 %) 추가 활용 여지** (단 software 측 갭
  제거 조건)

**갭 31 % 의 의미**:
- chain HW capacity 는 cap 이 아님 — Method 2 가 같은 chain 을 single-core 에서
  310 K ops/s 까지 뽑음
- **DPU ARM single thread 의 per-op 시간** (4.67 µs vs M2 의 3.22 µs) 이 cap.
  추가 작업 1.45 µs/op 이 31 % throughput 손실을 amplify
- 이 1.45 µs 를 줄여서 M2 baseline 에 맞추면 **single core 그대로 77.6 K RPS** 도달.
  multi-core/multi-EU 는 baseline 도 같이 늘어나서 *상대적* 갭은 그대로 — 우리
  목표는 single-core 에서 per-op 비용 줄이기 (§6.2.4)

#### 6.2.4 DPU ARM perf 분석 — 왜 dpumesh 가 ceiling 의 69 % 만 찍나

`top -H -p $(pgrep dpumesh_dpu)` 로 확인 — DPU 의 dpumesh_dpu 프로세스는 thread
3 개 중 **1 개만 99.9 % CPU**, 나머지 0 %. 즉 **single ARM thread 가 진짜 cap**.

`perf record -F 999 -g --call-graph dwarf` 30 초 캡처 + `perf report --no-children`
self-time 분포 (dpumesh @ 50 K RPS 부하):

| 카테고리 | dpumesh | Method 0 | Method 2 |
|---|---:|---:|---:|
| **epoll_pwait syscall 군집** (kernel + libc + vdso) | **64 %** | 65 % | 62 % |
| DOCA infra (CQ poll, comch internals) | 7 % | 13 % | 19 % |
| Atomics (CAS, swap, mutex) | 4 % | 2 % | 3 % |
| **App 코드** (run_dpu_worker, process_*, drain_*) | **5.5 %** | 3.4 % | 2.8 % |

세 측정 모두 **syscall storm 이 1순위**. 하지만 throughput 은 dpumesh 만
54 K RPS (= 214 K ops/s) 로 뒤쳐짐. per-op time 으로 환산 시:

| | per-op time | Δ vs M2 |
|---|---:|---:|
| Method 0 | 3.13 µs | — |
| Method 2 | 3.22 µs | baseline |
| **dpumesh** | **4.67 µs** | **+1.45 µs (+45 %)** |

이 +1.45 µs/op 의 정체는 **DPU ARM 의 추가 작업** (Method 2 비교):
- `comp_queue.enqueue` — DMA_COMPLETED 마다 4096 슬롯 ring write
- `process_forward_entry` — pod_idx 라우팅 + 8 KB staging memcpy + reverse
  dma_desc 작성 + tx_ring slot acquire + `server_send_tx_ack_to`
- `process_rev_notify_entry` — `server_send_msg(DMA_COMPLETION)` 발행
- `drain_deferred_tx_acks` — comch send-pool full 시 retry 큐 처리

perf 의 app % 비교는 5.5 % vs 2.8 % = +2.7 % 차이만 보이지만, saturation 상태
에서는 small CPU delta 가 throughput 에 큰 손실로 amplify 됨 (chain 이 backlog
받기 시작하면 latency 발산하고 다음 dma_copy 못 issue).

**핵심 결론**:
1. **DPU ARM single thread 가 cap** — top -H 에서 100 % 점유 thread 1 개 확정
2. **그 thread 의 64 % 는 epoll_pwait syscall** (DOCA `doca_pe_progress` 의
   기본 polling 구현이 syscall-heavy)
3. **나머지 36 % 안에서 dpumesh 가 M2 보다 +2.7 % 더 app code 사용** —
   process_forward_entry / process_rev_notify_entry / TX_ACK / reverse_enqueue
4. **그 +2.7 % CPU 가 throughput 31 % 손실로 amplify** — saturation 영역 특성
5. **fix 방향**: **single-core 에서** dpumesh 의 per-op 비용을 M2 baseline (3.22 µs)
   까지 줄이는 것이 목표. multi-core 는 M2 baseline 도 같이 늘어나니 갭이 안 줄어듦.

**Single-core 최적화 후보 (per-op 비용 분해 기준)**:

dpumesh per-op 시간 4.67 µs 중 M2 와 다른 부분 (≈ +1.45 µs):

| 추가 작업 | 추정 비용 | 줄이는 방법 | 잠재 이득 | 난이도 |
|---|---:|---|---:|---|
| **`comp_queue` enq/deq 간접화** | ~0.2 µs | recv callback 에서 곧바로 process — comp_queue 거치지 않음. 단, latency-sensitive operation 이라 careful | ~5 % | 중간 |
| **`process_forward_entry` 의 8 KB staging memcpy** | ~0.4 µs | DPU 가 reverse-DMA src 로 host RX buf 를 직접 사용 (in-place forward). 별도 DPU TX buffer 안 거침 | ~10-15 % | 큼 (architecture change) |
| **per-op comch send (TX_ACK / DMA_COMPLETION) 의 producer task lifecycle** | ~0.3 µs | TX_ACK 를 DMA_COMPLETION 에 piggyback (single send per RPC). 또는 N 개 batch | ~5-8 % | 중간 |
| **2 회 `doca_pe_progress` syscall per main loop iter** | ~0.3 µs (amortized) | (a) idle backoff 로 work 없을 때 syscall 빈도 감소, (b) `pe` + `consumer_pe` 를 1 epoll set 으로 통합 (DOCA 지원 확인 필요) | ~10-15 % | 작음 (a) / 큼 (b) |
| **`process_completion_queue` batch=128 의 함수 호출 dispatch** | ~0.1 µs | batch ↑ (예: 256/iter) 또는 inline dispatch | ~2-3 % | 작음 |

**우선순위 (single-core 환경 기준)**:
1. **Idle backoff** — 가장 cheap, syscall overhead 감소, ceiling 까지 ~10 % 추가 가능
   → **§6.5 의 분석으로 단연 1순위 확정**. ARM 의 polling 이 cap 의 진짜 정체.
   useful_CPU 33 % → 50 % 가능 → **+50 % RPS (NIC 한계까지 ~80 K RPS)**.
2. **TX_ACK piggyback** — comch send 의 chain 비용 감소
3. **comp_queue 우회** — 간접화 줄여 cache 친화적
4. ~~**In-place forwarding** — staging memcpy 제거 (architecture-level 변경 필요)~~
   → **§6.4 에서 적용 완료**. 실측 sustainable 52 K → 54 K (+ ~4 %), overload
   ceiling 53.5 K → 54.5 K (+ ~2 %). 예측 (10-15 %) 대비 작은 이유는 §6.5 참조
   (free 된 CPU 가 polling 으로 흡수됨).

위 4 개 모두 적용 시 단일 core 에서 dpumesh 가 M2 baseline 의 90 %+ 도달 가능
(이론적). 즉 dpumesh = ~280 K ops/s = **70 K RPS** 까지 single core 에서 가능.

CO-corrected 측정에서 본 saturation 모양 — sustainable 52 K 에서는 p50/p99
가 5.5 / 12 ms 로 안정 (DMA chain 처리 속도와 거의 일치), 55 K 부터는
p99 가 190 ms 로 점프 (16 배). 즉 elbow 너머에서는 DMA chain 이 backlog 를
받아 큐잉이 폭증함. **chain 처리 속도 (= DPU ARM single thread 의 per-op time)
가 cap 결정자**.

### 6.3 HW 한계 측정 (chain ceiling 추적)

목적: dpumesh 가 §6.2.3 의 Method 2 ceiling (77.6 K RPS = 310,472 dma_copy
ops/s ÷ 4) 에 host-side 자원을 충분히 줬을 때 얼마나 근접하는지. 만약
여전히 54 K 에 막히면 cap 은 chain 내부 (DPU ARM single-thread per-op time —
§6.2.4 분석 참조), 풀리면 cap 은 host 측 lock/scheduling 이었음.

**셋업**: `./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE>` — 자동으로
`pin_pods hw` 로 전환:

| pod | fair | hw |
|---|---|---|
| bench-dpumesh | core 0 | **0, 4** |
| echo-dpumesh | core 1 | **1, 5** |
| ECHO_THREADS | 64 | 64 (변경 없음) |

#### 6.3.1 측정 후 해석 가이드

| `dpumesh-hw` 결과 | 해석 | 다음 실험 |
|---|---|---|
| RPS ≈ 54 K (현재와 동일) | chain 자체가 cap. host 자원 더 줘봐야 무의미. | DPU `top -H -p $(pgrep dpumesh_dpu)` 로 ARM core 100% 인지 확인 |
| 60-65 K | chain 가까이 도달. host-side lock/scheduling 이 fair-mode 의 일부 cap 였음. | bench/echo 코어 더 늘려서 한계 측정 |
| > 77 K | Method 2 baseline 잘못됐거나 측정 노이즈. | sweep 재실행 |
| RPS 떨어짐 (< 54 K) | multi-core thread thrashing. cache locality 손실. | core 개수 줄이거나 NUMA 구분 |

#### 6.3.2 보조 측정 도구

- **CPU 사용률**: `mpstat -P 0-7 1` — 어느 코어가 100 % 인지. fair-mode 에서
  core 0 (bench) / 1 (echo) 가 100 % 면 그쪽이 cap. hw-mode 에서 core
  0,1,4,5 모두 < 100 % 면 host 측 풀림.
- **DPU ARM CPU**: DPU 에 ssh + `top -H -p $(pgrep dpumesh_dpu)`. ARM 단일
  thread 가 100 % 면 ARM 측 cap 확정.
- **DPA EU stat**: `dpa-statistics show` 의 `Cycles`, `producer drain stall`,
  `consumer_empty wait` — chain 내부 saturation 신호.

#### 6.3.3 추가 변수

- **msg_size 작게 (1 KB)** — per-msg overhead 격리. 8 KB 는 DMA 비중 큼,
  1 KB 는 ops 자체로 cap 됨. ops/sec 비교에 더 깨끗.
- **bench 측 thread 수 (workers)** — `dpumesh-hw <RPS> <DUR> <SIZE> <CONNS>`
  로 worker 수 직접 지정. 기본값 (rps/100) 이 cap region 에서 thread thrashing
  유발할 수 있음.

### 6.4 In-place forwarding 적용 후 실측 (2026-05-08)

§6.2.4 우선순위 4 항목 중 가장 큰 ratio (10-15 % 예측) 의 **`process_forward_entry`
8 KB staging memcpy 제거** 를 commit. dst pod 의 `tx_buffer` 에 memcpy 하던
단계를 제거하고, reverse DMA 가 src pod 의 `dma_buffer` (forward 가 쓴 그
자리) 를 직접 source 로 사용. dma_desc 의 `mmap` / `addr` 필드 (이미 정의되어
있던 4B + 8B) 에 src pod 의 mmap handle + full VA 를 박아서 DPA reverse
handler 가 per-descriptor 로 dispatch.

#### 6.4.1 코드 변경

| 파일 | 변경 |
|---|---|
| `lib/cpp/src/thrift/transport/doca/object.h` | `pod_state.local_mmap_dpa_handle` 필드 추가 (uint32_t — SDK 일치) |
| `lib/cpp/src/thrift/transport/doca/dpa.c` | `setup_pod_dma` 에서 DPA mmap handle 캐싱 |
| `lib/cpp/src/thrift/transport/doca/dpu_worker.c` | `dpu_enqueue_reverse_dma` signature 변경 (memcpy / write_pos 제거); `process_forward_entry` 성공 path 에서 TX_ACK 제거 (에러 path 만 유지); `process_rev_notify_entry` 에 reverse 완료 후 TX_ACK 추가; `send_or_defer_tx_ack` 헬퍼 |
| `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c` | reverse handler 가 `desc->mmap` / `desc->addr` 를 source 로 사용. `desc->mmap == 0` 이면 ring 의 legacy `dpu_mmap` fallback (partial deploy 대비) |

**보존**: `tx_buffer` / `tx_mmap` / `tx_buf_size` / `tx_producer_head`
allocation 자체는 살림 (legacy fallback 가능 + rollback 안전). 별도 cleanup
PR 권장.

**Slot lifecycle 변화**: src 의 `dma_buffer` slot 점유 시간이
forward-only → **full RTT** 로 늘어남. host TX slot 도 reverse 완료 후 TX_ACK
받아야 release. sustainable 52 K @ 5.5 ms RTT = ~290 in-flight slots,
`DMA_RING_SIZE = 2048` 안에 충분히 fit.

**받아들인 trade-off**: dst pod 별 staging buffer 가 사라지면서 한 src 가
여러 dst 로 보낼 때 **HOL blocking across destinations from same source**
발생. echo bench (src==dst) 에는 영향 없음 — multi-dst 워크로드에서만
manifest. 사용자 합의 사항 (verified prior).

#### 6.4.2 RPS sweep 비교 (8 KB, 10 s, conns=auto)

§6.2.2 와 동일 측정 조건 (wrk2 식 scheduled-time, MAX_WORKERS=4096). 좌측 =
baseline (separate buffer + memcpy), 우측 = new (in-place forward):

| Target | Baseline ach | New ach | Baseline p99 | New p99 | Δ p99 |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 4,968.6 | 4,968.8 | 1.93 ms | 1.92 ms | -0.5 % |
| 10,000 | 9,939.2 | 9,935.9 | 2.82 ms | 2.78 ms | -1.4 % |
| 30,000 | 29,807.8 | 29,800.9 | 6.51 ms | 6.38 ms | -2.0 % |
| 50,000 | 49,664.7 | 49,655.3 | 11.89 ms | 9.94 ms | -16.4 % |
| 52,000 | 51,644.8 | 51,625.3 | 12.16 ms | 12.12 ms | -0.3 % |
| **53,000** | — (안 측정) | **52,641.9** | — | **12.23 ms** | **새 sustainable** |
| **54,000** | — (안 측정) | **53,636.3** | — | **12.38 ms** | **새 sustainable** |
| 55,000 | 53,553.4 | 54,085.4 | 189.59 ms | 101.39 ms | **-46.5 %** |
| 60,000 | 53,698.8 | 54,198.6 | 1,154.25 ms | 998.98 ms | -13.5 % |
| 65,000 | 53,841.1 | 54,748.5 | 1,961.63 ms | 1,767.98 ms | -9.9 % |
| 70,000 | — | 54,369.7 | — | 2,817.77 ms | overload |

**결과**:

- **Sustainable region 확장**: 52 K → **54 K** (+ ~4 %, p99 < 13 ms 기준).
  53 K, 54 K target 에서 baseline 의 52 K 와 동일한 latency profile (p50 5.5 ms,
  p99 12 ms) 가 깨끗하게 나옴.
- **Overload ceiling**: 53,553 → **54,748** (+ ~2 %).
- **Overload p99 약 절반** (55 K target 에서 190 → 101 ms). cap 너머 큐잉이
  덜 발산 — backpressure 가 더 부드럽게 진입.
- **0 failure 전체**. Back-to-back 11 회 sequential test, slot leak 없음 (per
  feedback memory rule: redeploy 없이 연속 통과 확인).

#### 6.4.3 chain ceiling 대비 새 위치

```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
        ┃ │
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
        ┃ │
    67% ┃ │   dpumesh @ baseline overload    ── 214,212 ops/s = 13.71 Gbps
        ┃ │     (53,553 RPS × 4 dma_copy)
    68% ┃ └── dpumesh @ new overload         ── 218,992 ops/s = 14.02 Gbps
        ┃         (54,748 RPS × 4 dma_copy, +2.2 %)
```

| | dma_copy ops/sec | vs Method 0 | vs Method 2 |
|---|---:|---:|---:|
| dpumesh baseline @ overload (53,553 RPS) | 214,212 | 67 % | 69 % |
| **dpumesh new @ overload (54,748 RPS)** | **218,992** | **68 %** | **71 %** |
| dpumesh new @ sustainable (53,636 RPS, p99 12.4 ms) | 214,544 | 67 % | 69 % |

Method 2 (DMA + completion) baseline 대비 69 % → **71 %**. ceiling 까지 남은
gap 의 약 7 % 를 단일 변경으로 닫음.

#### 6.4.4 예측 (10-15 %) vs 실측 (~4 %) 의 gap 분석

§6.2.4 의 예측은 memcpy 제거가 per-op time 4.67 → ~4.27 µs (-9 %) 로 줄어 saturation
영역에서 amplify 되어 RPS +10-15 % 로 이어진다는 가정. 실측은 더 작음. 가설:

1. **memcpy 의 실제 self-time 이 예상보다 작았을 가능성**. perf flame 의 5.5 %
   app 코드 중 memcpy 차지 비중이 추정치보다 작음. 새 빌드로 `dpumesh_dpu_flame.svg`
   재캡처해서 process_forward_entry 의 self-time 변화 확인 필요.
2. **TX_ACK + DMA_COMPLETION clustering**. 새 설계는 두 comch send 가 같은
   `process_rev_notify_entry` iter 에서 fire — 평균 발송 수는 동일하지만 burst
   빈도가 늘어 deferred queue hit 가 잦아짐. send pool 압력 burst 화.
3. **저부하 p999 regression** (5 K 에서 2.0 → 5-6 ms): (2) 의 deferred queue 가
   단일 request 의 TX_ACK 를 다음 main-loop iter 로 미는 경우 추가 ~1-3 ms 가산.
   p50/p99 영향 없음, p999 만 끌어올림. 이는 ceiling 보다 latency 분포에 영향.

가설 (2) 가 맞으면 §6.2.4 의 #2 후보 (**TX_ACK piggyback**) 가 자연스러운
다음 단계 — DMA_COMPLETION 메시지에 TX_ACK 정보를 fold 해서 single send per
RPC 로 만들면 burst 자체가 사라짐.

> **2026-05-08 추가**: §6.4 의 "+4% 만 늘었다" 가설들은 §6.5 의 더 큰 진단으로
> 대체됨. 진짜 원인은 ARM 의 free 된 CPU 가 polling 으로 흡수되어 throughput 으로
> 전환 안 됐기 때문. §6.5 참조.

### 6.5 진짜 cap 의 정체 — DPA stat + cross-baseline 폴링 분석 (2026-05-08)

§6.4 의 "예측 10-15 % vs 실측 ~4 %" gap 의 원인을 추적하다가 §6.2.4 의 "**single
ARM thread 가 cap**" 결론 자체가 부정확했음을 확인. ARM 이 100 % busy 인 건
사실이지만 **그 이유가 compute 가 아니라 polling**. DPA EU stat + M2 baseline 과
폴링 비중 직접 비교로 정정.

#### 6.5.1 DPA EU stat — ARM 99.9 % 인데 EU 99.5 % idle

```
sudo /opt/mellanox/doca/tools/dpa-statistics collect -d mlx5_0 -t 10000
```

50 K RPS 부하 중 10 s 윈도우:

| 항목 | 값 | 해석 |
|---|---:|---|
| Wall time | 10,000 ms | 측정창 |
| **EU active time** | **51.5 ms** (ticks @ 1 ns) | **EU 가 실제 실행한 시간** |
| **EU active %** | **0.51 %** | **99.49 % idle** |
| Cycles | 92.7 G | 51.5 ms × ~1.8 GHz EU clock 일치 |
| Instructions | 7.22 G | IPC = 0.078 (memory/DMA stall heavy — 정상) |
| Executions | 7,314 (= 731 / s) | DPA thread 가 깬 횟수 |
| Cycles/execution | 12.7 M | wake 당 평균 7 µs 실행 |
| dma_copy/execution | ~273 (200K dma_copy/s ÷ 731) | wake 당 batch size |

DPA 는 burst 처리 — wake → 273 dma_copy 일괄 → re-schedule. **99.5 % idle**.

ARM (`top -H -p $(pgrep dpumesh_dpu)`): thread 0 = 99.9 % busy. 수치는 §6.2.4 와
동일. 하지만 이 99.9 % 의 분포가 § 6.2.4 의 해석과 다름.

#### 6.5.2 폴링 비중 cross-baseline — M2 가 dpumesh 보다 더 폴링함

`bench/m2_dpu_flame.svg` (Method 2 baseline) vs `bench/dpumesh_dpu_flame_inplace.svg`
(현재 dpumesh) 의 subtree % 직접 비교:

| | M2 (baseline ceiling) | dpumesh new | M2 가 더 ↑? |
|---|---:|---:|---|
| `doca_pe_progress` subtree | **82.08 %** | 67.52 % | ✓ |
| `__GI_epoll_pwait` | 54.84 % | 45.99 % | ✓ |
| `el0t_64_sync` (kernel syscall path) | 44.48 % | 37.77 % | ✓ |
| **추정 폴링 비중** | **~77 %** | ~67 % | ✓ |
| **추정 useful work 비중** | ~23 % | ~33 % | dpumesh 가 ↑ |

M2 가 **더 많이 폴링** 하는데도 **더 높은 throughput** 도달 (310 K events/s vs
dpumesh 219 K dma_copy/s). 즉 "폴링 = cap" 의 단순 모델로는 설명 불가.

#### 6.5.3 정정된 모델 — `throughput = useful_CPU / per_event_work`

ARM CPU 100 % = polling + useful_work. throughput cap = `useful_work_time /
per_event_cost`. 양쪽 다 측정 데이터로 검증:

| | useful CPU | per-event 비용 | 모델 계산 | 측정 |
|---|---:|---:|---:|---:|
| M2 | 23 % = 230 ms/s | 0.74 µs/event | 230 ms ÷ 0.74 µs = 311 K events/s | **310 K ✓** |
| dpumesh | 33 % = 330 ms/s | 6.0 µs/RPC | 330 ms ÷ 6.0 µs = 55 K RPC/s | **55 K ✓** |

dpumesh 가 useful CPU 는 더 많은데 throughput 이 적은 건 **per-event 비용이
8× 무겁기 때문** (M2: counter++ + 1 comch send vs dpumesh: comp_queue + routing
+ reverse desc + 2× comch send).

#### 6.5.4 진짜 cap — 어디서 막히고 있나

| 시스템 | 진짜 cap | 증거 |
|---|---|---|
| M2 | **NIC DMA bandwidth** | 310 K / 320 K (HW max) = **97 %** saturated |
| dpumesh | **ARM 의 useful CPU 양** | 219 K / 320 K = 68 %. NIC 32 % 헤드룸 남았는데 ARM useful 33 % 만 |

**§6.2.3 의 "dpumesh 가 M2 ceiling 의 71 %" 비교는 misleading** — M2 는 ARM-bound
가 아니라 NIC-bound. dpumesh 의 ARM 폴링을 줄이면 NIC 한계까지 도달 가능.

§6.2.4 의 "single ARM thread 가 cap" 도 절반만 정확:
- ✅ ARM 이 cap 인 것은 사실 (99.9 % busy)
- ❌ "compute 가 부족해서" 가 아니라 "**polling 이 useful work 시간을 잠식해서**"

#### 6.5.5 Idle backoff 의 효과 모델

폴링을 줄여 useful work time 을 늘렸을 때 dpumesh 의 RPS:

| useful CPU | per-RPC 비용 (현재 6.0 µs) | RPC/s | dma_copy/s | NIC 활용 |
|---:|---:|---:|---:|---:|
| 33 % (현재) | 6.0 µs | 55 K | 220 K | 68 % |
| 40 % | 6.0 µs | 67 K (+21 %) | 268 K | 84 % |
| 50 % | 6.0 µs | 83 K (+50 %) | 332 K | 100 % (NIC cap) |

useful CPU 50 % 까지 끌어올리면 **NIC 한계 (320 K dma_copy/s) 에 수렴 → ~80 K
RPS**. §6.2.4 의 "70 K RPS 까지 가능" 추정과 일치.

M2 에 같은 idle backoff 를 적용해도 효과 없음 — 이미 NIC saturated.

#### 6.5.6 함의

1. **§6.2.4 우선순위 재정렬**: idle backoff 가 단연 1순위. 다른 후보들은 per-event
   비용 (6.0 µs) 을 4-5 µs 로 줄이는 효과지만 (+10-30 % RPS), idle backoff 는
   useful_CPU 자체를 늘리므로 **+50 % RPS** 가능.
2. **§6.4 의 "+4 %" gap 의 진짜 원인**: memcpy 제거로 free 된 ~8 pp CPU 가
   throughput 으로 전환되지 않은 건 ARM 이 polling 으로 흡수했기 때문. cap 이
   "useful CPU 양" 인 한, per-event 비용을 줄여도 saturation 모양은 안 바뀜
   (ARM 이 free 된 시간을 polling 으로 채워버림). idle backoff 와 같이 적용해야
   memcpy 의 이득이 throughput 으로 보임.
3. **NIC saturation 까지 도달 시 다음 cap**: 320 K dma_copy/s 도달 후에는 dpumesh
   가 NIC-bound 가 됨. 그 이후로는 RPC 당 dma_copy 횟수 (현재 4) 를 줄이는 게
   유일한 길 — 예: forward + reverse 의 chunk 통합, 또는 작은 메시지 batching.
4. **다음 측정 (idle backoff 적용 후 검증)**:
   - DPA EU active % 가 늘어나는지 (현재 0.51 % → ↑)
   - ARM polling % 가 줄어드는지 (flame 의 epoll 비중 ↓)
   - RPS 가 useful_CPU 모델 예측과 일치하는지

> **2026-05-27 추가**: §6.5 의 "ARM polling 이 useful_CPU 잠식" 가설은 §6.6 의
> 다섯 번 연속 실험으로 **반증** 됨. 진짜 cap 은 ARM polling 이 아니라
> **DPA kernel 의 per-dma_copy 작업량** (validation, chunking, multiple
> writebacks 등). §6.6 참조.

---

### 6.6 cap 의 정체 재진단 — 다섯 번 연속 실험 (2026-05-27)

§6.5 의 "ARM useful_CPU 양이 cap, polling 이 잠식" 모델을 다섯 가지 cheap
실험으로 검증. 결론: **§6.5 모델 틀렸음**. cap 은 ARM 의 polling 도, useful CPU
양도 아닌 — **DPA kernel 의 per-dma_copy 추가 작업** 임. §6.2.4 의 "DPA ARM
single thread 작업이 cap" 으로 회귀하되, "ARM" 이 아니라 "DPA EU" 가 cap 임을
정정.

#### 6.6.1 실험 1 — Inner `pe_progress` 제거 (per-entry → per-batch)

`process_completion_queue` 가 entry 1개 처리할 때마다 `pe_progress(pe)` +
`pe_progress(consumer_pe)` 2회 호출 → batch=128 이면 256 회/main-iter 의 추가
폴링. 이걸 batch 끝에 1회만 호출로 변경 (`dpu_worker.c:343-344` 의 2줄 제거).

**예측** (§6.5 모델): polling 시간 ↓ → useful_CPU ↑ → RPS ↑

**측정** (§6.4.2 baseline = in-place forward 적용 직후):

| Target | Baseline ach / p99 | Per-batch ach / p99 | Δ ach | Δ p99 |
|---:|---:|---:|---:|---:|
| 50K | 49,655 / 9.94 ms | 49,629 / 9.89 ms | -0.05 % | -0.5 % |
| 55K | 54,085 / 101.39 ms | 54,080 / 91.06 ms | -0.01 % | **-10.2 %** |
| 65K | 54,749 / 1,768 ms | 54,291 / 1,894 ms | -0.8 % | +7.1 % |

**결과**: sustainable RPS **변화 없음** (49,629 vs 49,655). p99 만 약간 흔들림
(noise 수준).

**Flame 비교** (`bench/dpumesh_dpu_flame_per_batch.svg`):

| 항목 | inplace | per_batch (지금) |
|---|---:|---:|
| `doca_pe_progress` subtree | 67.5 % | **76.5 %** ↑ |
| `__GI_epoll_pwait` | 46.0 % | **54.5 %** ↑ |
| `process_completion_queue` | 19.8 % | **7.95 %** ↓ |

`process_completion_queue` 의 self-time 12 pp 감소분이 그대로 `epoll_pwait`
self-time 으로 이동. **polling 횟수를 줄여도 polling 시간 비중은 안 줄어들고
오히려 늘어남**. ARM 이 일을 안 하는 게 아니라, events 도착을 더 깊게 block.

#### 6.6.2 실험 2 — `epoll_pwait` 호출당 시간 측정

```
sudo perf trace -p $(pgrep dpumesh_dpu) -s -- sleep 15
```

50K RPS 부하 중 (per-batch 변경 적용 후):

```
syscall          calls      total(ms)   avg(ms)    min       max
epoll_pwait     3,122,730   6,703.520   0.002      0.001     0.109
```

- 호출 횟수: 3.12 M / 15 s = **208 K calls/s**
- 호출당 평균 시간: **2 µs** (min 1 µs, max 109 µs, stddev 0.02 %)
- 총 `epoll_pwait` 시간 / wall = 44.7 % ← flame 의 ~45-55 % subtree 와 일치

**중요**: avg 2 µs 는 **block 깊게 자는 게 아니라 syscall enter/exit 고정 비용**
(가설 A 부정, 가설 B 확정). 200K events/s 도착률에서 events 가 빠르게 흘러
들어와서 epoll 이 사실상 polling 처럼 동작.

**그러나** — M2 baseline 도 동일한 메인 루프 구조 (sleep 없음, `pe_progress` ×
2). 같은 41 % epoll 비용을 무는데 M2 는 310K events/s 도달, dpumesh 는 220K
에서 막힘. **즉 epoll overhead 자체가 dpumesh-vs-M2 갭의 원인은 아님**.

#### 6.6.3 실험 3 — DPA `thread_reschedule` 제거 (busy-spin)

가설: dpumesh DPA 가 yield 하는 동안 1.37 ms wake gap 이 host desc 처리를 지연.
busy-spin 으로 바꾸면 M2 처럼 즉시 처리할 것.

변경: `dpa_kernel.c` 의 `if (chunks == 0) doca_dpa_dev_thread_reschedule()` 제거.

| Target | per-batch ach / p99 | **DPA busy-spin** ach / p99 |
|---:|---:|---:|
| 50K | 49,629 / 9.89 ms | 49,605 / 9.54 ms |
| 55K | 54,080 / 91.06 ms | **53,562 / 224.4 ms** |
| 65K | 54,291 / 1,894 ms | 53,398 / 2,064 ms |

**결과**: 55K p99 가 91 → 224 ms 로 **2.5 배 악화**. 가설 **반증**.

해석: dpumesh 의 4 rings × busy-spin = PCIe poll bandwidth 가 실제 DMA 트래픽과
경쟁. yield 가 단순 절전이 아니라 **PCIe polling rate throttle** 역할.

→ revert.

#### 6.6.4 실험 4 — Load generator 검증

dpumesh 의 54K ceiling 이 system 인지 load-gen 인지 갈음. 기본 워커수 (rps/100):

| Target × workers | achieved | p99 |
|---|---:|---:|
| 65K × 650 (default) | 54,291 | 1,894 ms |
| **65K × 1000** | **54,199** | **1,892 ms** |
| **100K × 1500** | **54,227** | **6,814 ms** |
| 65K × 4096 | **hang** — TX slot pool (2048) 충돌 | n/a |

650 → 1000 / 1500 워커 변화에도 ach 가 54.2K 에 fix. **system ceiling 확정**.
4096 워커는 dpumesh 의 dpumesh_tx_alloc + DMA_RING_SIZE=2048 동시성 한계에
부딪혀 hang (load-gen 너무 무리하게 push 한 케이스).

M2 baseline 도 healthy:
- M2 는 open-loop (host posts as fast as possible, DPA consume rate 가 cap)
- 측정값 310K = NIC 97 % saturation. clearly maxed.

**양쪽 load gen 모두 건전 — 54K 갭은 load-gen 차이가 아니라 시스템 구조 차이**.

#### 6.6.5 실험 5 — M2 에 synthetic multi-ring polling 추가

M2 baseline 의 `poll_desc_ring_dma_copy` 에 매 dma_copy 마다 N 개 추가
`desc->valid` PCIe read 삽입. 같은 ring 의 다른 슬롯 (offset × 128, 별도 cache
line) 을 읽어서 PCIe 부하만 발생시키고 결과는 버림.

DOCA `DOCA_LOG_*` / `DOCA_DPA_DEV_LOG_*` 전부 nullify (DPU 메모리 압력 + log
flooding 회피).

| EXTRA_RING_POLLS | 총 rings 폴링 | recv/s | Bandwidth | Δ vs N=0 |
|---:|---:|---:|---:|---:|
| 0 (baseline) | 1 | **325,361** | 21.32 Gbps | — |
| 1 | 2 | 236,660 | 15.50 Gbps | **-27 %** |
| 3 | 4 | 175,421 | 11.49 Gbps | **-46 %** |
| 7 | 8 | 114,543 | 7.50 Gbps | **-65 %** |

monotonic 하게 떨어짐. **per-extra-poll cost ≈ 0.8-1.1 µs / dma_copy** (PCIe
read latency 와 일치).

**Note**: N=0 의 325K 는 §6.2.1 의 310K 보다 약간 ↑. DOCA log strip 효과.

**User 의 sharp 질문**: dpumesh 가 실제 4 rings 폴링 하는데 측정 220K 가
M2-N=3 (synthetic 4 rings) 의 175K 보다 **높음**. 모순.

분석: 내 synthetic 은 매 dma_copy 마다 N 개 추가 PCIe read **무조건** 발사 →
per dma_copy 4 reads. dpumesh 의 실제 `drain_all_rings` 은 한 inner iter 에서
4 rings 폴링 → 0~4 dma_copy 발사 → saturation 시 amortize 됨. 즉 실제 ratio 가
1 이 될 가능성 큼.

#### 6.6.6 실험 X — DPA polling counter 직접 측정 (결정적)

**가설 갈음**:
- 가설 A: dpumesh ratio = 2-4 → polling 이 갭의 ~75 %
- 가설 B: dpumesh ratio ≈ 1.0 → polling 무관, 갭은 DPA code 작업

DPA kernel + `dpa_thread_arg` 에 counter 3개 추가:

```c
volatile uint64_t stat_inner_iters;   /* drain_all_rings inner do-while */
volatile uint64_t stat_polls;         /* PCIe desc->valid reads (fwd+rev) */
volatile uint64_t stat_dma_copies;    /* dma_copy chunks issued */
```

DPU ARM 이 매 1초 `doca_dpa_d2h_memcpy` 로 읽어서 ratio 계산 + stat 출력.

**측정 결과** (50K RPS, 15s):

```
DPA stat: iters=50858 polls=203428 copies=200052  poll/copy=1.017
DPA stat: iters=50799 polls=203196 copies=200033  poll/copy=1.016
DPA stat: iters=50816 polls=203268 copies=200000  poll/copy=1.016
DPA stat: iters=50910 polls=203636 copies=200012  poll/copy=1.018
DPA stat: iters=50876 polls=203504 copies=199999  poll/copy=1.018
                                                  ↑
                                          15초 내내 1.016 ~ 1.027
```

분해:
- `drain_all_rings` inner iter ~50,800/s
- iter 당 4 polls (forward × 2 pods + reverse × 2 pods) → 203K polls/s
- iter 당 평균 ~4 dma_copies 처리 → 200K dma_copies/s
- **poll/copy ratio = 1.017** ≈ M2 baseline 의 1.000

**가설 B 확정**. dpumesh 의 PCIe polling rate 는 M2 baseline 과 동일. 4 rings
폴링이 4 dma_copies 발사로 perfectly amortize. **polling 은 dpumesh-vs-M2 갭의
원인 아님**.

#### 6.6.7 정정된 cap 모델 — DPA EU 의 per-dma_copy 작업

| | M2 baseline | dpumesh | Δ |
|---|---:|---:|---:|
| 1 / dma_copy | **325K** | **220K** | -105K |
| Per-dma_copy 시간 | **3.08 µs** | **4.55 µs** | **+1.47 µs** |
| PCIe polls / dma_copy | ~1.0 | **1.017 (측정)** | 0 |

→ +1.47 µs/dma_copy 갭은 **PCIe read 양 차이가 아닌, DPA EU 의 추가 코드 작업**.
M2 의 `poll_desc_ring_dma_copy` 에 없는 dpumesh `process_one_desc` 의 작업:

| 항목 | M2 | dpumesh | 추정 EU 비용 |
|---|---|---|---:|
| validation (mmap=0, size=0, range) | 없음 | 3 checks + range arithmetic | ~0.1-0.2 µs |
| chunking loop (`while offset < total`) | 없음 (단일 dma_copy) | loop + ALIGN_UP_128 + chunk type 선택 | ~0.2-0.3 µs |
| desc 필드 clear 후 writeback × 3 | 1× writeback | 3× writeback | ~0.3-0.5 µs |
| `comch_dma_comp_msg` 필드 수 | 3 (type/pos/length) | 6 (+ req_id/src_pod_id/dst_pod_id/flags) | ~0.1 µs |
| `ensure_producer_slot` 호출 | 매 500 ops lazy drain | 매 chunk + counter check | ~0.2 µs |
| reverse rings 분기 / `dpa_sent_count` 증가 | 없음 | 매 reverse desc | ~0.1 µs |
| **합계 추정** | — | — | **~1.0-1.4 µs** |

추정 합계 1.0-1.4 µs ≈ 실측 갭 1.47 µs. 갭이 거의 다 DPA kernel 코드 무게로
설명됨.

#### 6.6.8 §6.5 모델이 틀렸던 이유

§6.5 는 ARM flame 의 `epoll_pwait` 비중 (45-55 %) 을 보고 "polling 이 useful CPU
잠식 → cap" 으로 결론. 실제로는:

1. ARM 의 epoll 시간이 길지만 그건 syscall enter/exit 의 고정 비용 (실험 2).
   block 깊게 자는 게 아니라 events 처리 사이 빠른 invocation.
2. M2 도 같은 epoll 비용 무름 → epoll 자체는 dpumesh-만의 cap 아님.
3. ARM useful_CPU 가 늘어도 RPS 안 늘어남 (실험 1 — polling 256 → 2 호출
   변경에도 RPS flat). 즉 ARM 이 cap 이 아님.
4. 진짜 cap 은 **DPA EU 가 dma_copy 1개 발사하는 데 걸리는 시간** (4.55 µs).
   ARM 은 DPA 의 burst (273 dma_copy / wake) 를 받아서 처리하는 쪽인데, DPA 의
   wake 빈도가 cap 결정자.

#### 6.6.9 함의 — 새 우선순위

1. **DPA kernel 의 per-dma_copy 작업 줄이기** (가장 큰 단일 변수, ~1.47 µs
   회복 가능):
   - chunking bypass when `total ≤ 8 KB` (single-chunk fast path) — ~0.2-0.3 µs
   - desc field clear 를 한 번에 (3× writeback → 1× writeback) — ~0.3-0.5 µs
   - validation 제거 (echo bench 에선 desc corrupt 가능성 0) — ~0.1-0.2 µs
   - 합쳐 적용 시 dpumesh dma_copy time 4.55 → 3.55 µs 추정 → **282K dma_copy/s
     = 70K RPS** (현재 54K → +30 %)
2. **이전 우선순위 (idle backoff, TX_ACK piggyback) 재평가**:
   - idle backoff 는 ARM 측 epoll 호출률을 줄이지만 RPS 에 영향 없음 (실험 1
     결과). cap 이 DPA 인 한 ARM 어떻게 해도 무의미. **deprioritize**.
   - TX_ACK piggyback 은 ARM 의 send/s 절반. 하지만 cap 이 ARM 아니라 DPA 라면
     영향 미미. **deprioritize**.
3. **다음 측정**: §6.7 (DPA feature 단계적 제거) 으로 정량 검증.

---

### 6.7 DPA feature 단계적 제거 — 가설 검증 (2026-05-27)

§6.6 의 "cap = DPA kernel 의 per-dma_copy 작업" 가설을 dpumesh 에서 직접 검증.
`process_one_desc` / `process_one_rev_desc` 에 3개 compile-time toggle 추가:

```c
#define STRIP_VALIDATION   /* mmap=0, size=0, range, padded>buf 모두 #ifdef out */
#define STRIP_DESC_CLEAR   /* 3× writeback → 1× writeback (valid=0 만) */
#define STRIP_CHUNK_LOOP   /* size ≤ 8KB single dma_copy fast path */
```

각 stage 별 deploy + sweep. 모든 측정은 8 KB, conns=auto, wrk2 scheduled-time.

#### 6.7.1 측정 결과

| Stage | 50K ach / p99 | 55K ach / p99 | **65K ach / p99** | DMA tput@65K |
|---|---:|---:|---:|---:|
| Baseline (per-batch only) | 49,629 / 9.89 ms | 54,080 / 91.06 ms | **54,291 / 1,894 ms** | 847 MB/s |
| Exp A (+VALIDATION) | 49,721 / 10.04 ms | — | 55,207 / 1,704 ms | 862 MB/s |
| Exp A+B (+DESC_CLEAR) | — | — | 56,134 / 1,545 ms | 877 MB/s |
| **Exp A+B+C (+CHUNK_LOOP)** | **49,659 / 9.36 ms** | **54,617 / 12.27 ms** | **59,195 / 952 ms** | **925 MB/s** |

#### 6.7.2 단계별 기여 분해

```
Baseline overload ceiling: 54,291 RPS
   ↓ (+1.7 %)
Exp A — STRIP_VALIDATION:   55,207 RPS   (Δ +916 RPS)
   ↓ (+1.7 %)
Exp A+B — +STRIP_DESC_CLEAR: 56,134 RPS  (Δ +927 RPS)
   ↓ (+5.5 %)
Exp A+B+C — +STRIP_CHUNK_LOOP: 59,195 RPS (Δ +3,061 RPS)
─────────────────────────────────────────
Total +9.0 % (54,291 → 59,195 RPS)
```

**Chunking loop 제거가 단일 항목으로 가장 큰 기여** (+5.5 %, 다른 두 개를 합친
것보다 큼). 이건 chunking 코드 자체보다 **`ensure_producer_slot` + `is_consumer_empty`
wait + `producer_slots_inflight` increment 가 chunking loop 안에 있어서 1개
dma_copy 임에도 chunk 마다 동작**해서 그런 것으로 추정. fast path 는 이 모든
체크가 1번씩만.

#### 6.7.3 가장 큰 효과는 — Elbow 이동

- **Baseline**: 55K target 에서 이미 elbow (p99 91 ms 폭증)
- **Exp A+B+C**: 55K target 에서 여전히 **sustainable 영역** (p99 12.27 ms,
  baseline 의 52K 와 같은 latency profile)

**Sustainable saturation 이 52-54K → 55K+ 로 이동**. p99 가 1/8 로 떨어짐
(91 → 12 ms).

Overload 영역도 개선:
- 65K target p99: **1,894 → 952 ms (-50 %)** — overload 큐가 절반으로
- Throughput @ overload: 847 → 925 MB/s (+9.2 %)

#### 6.7.4 chain ceiling 대비 새 위치

```
   100% ┃ ┌── HW max (Method 0)              ── 320,014 ops/s = 20.97 Gbps
        ┃ │
    97% ┃ ├── DMA + completion (Method 2)    ── 310,472 ops/s = 20.34 Gbps
        ┃ │
    74% ┃ ├── dpumesh @ Exp A+B+C            ── 236,780 ops/s = 15.13 Gbps
        ┃ │     (59,195 RPS × 4 dma_copy)
    68% ┃ └── dpumesh @ baseline (in-place)   ── 218,992 ops/s = 14.02 Gbps
        ┃         (54,748 RPS × 4 dma_copy)
```

| | dma_copy ops/sec | vs Method 0 | vs Method 2 |
|---|---:|---:|---:|
| dpumesh baseline (in-place + per-batch) | 218,992 | 68 % | 71 % |
| **dpumesh Exp A+B+C** | **236,780** | **74 %** | **76 %** |

per-dma_copy 시간:
- Baseline: 1 / 54,748 × 4 dma_copy = 4.57 µs / dma_copy
- Exp A+B+C: 1 / 59,195 × 4 = 4.22 µs / dma_copy
- M2: 1 / 81,260 (= 325K / 4) × 4 = 3.08 µs / dma_copy

**0.35 µs 회복** (4.57 → 4.22). 남은 갭 1.14 µs.

#### 6.7.5 안정성 확인

3회 back-to-back 50K test (memory rule: slot leak 검증):

```
=== run 1 === Achieved RPS: 49342.6  p99: 9381.5 us  OK/Fail: 250000/0
=== run 2 === Achieved RPS: 49353.8  p99: 9347.9 us  OK/Fail: 250000/0
=== run 3 === Achieved RPS: 49344.8  p99: 9329.5 us  OK/Fail: 250000/0
```

완전히 안정. validation 제거로 인한 corner case 노출 없음 — echo bench
조건에서는 desc 가 항상 valid 함이 확인됨.

#### 6.7.6 §6.6.9 예측 vs 실측

| 항목 | 예측 회복 | 실측 회복 | Δ |
|---|---:|---:|---:|
| Validation | 0.1-0.2 µs | ~0.08 µs | underestimate? |
| Desc clear | 0.3-0.5 µs | ~0.08 µs | 큰 overestimate |
| Chunk loop bypass | 0.2-0.3 µs | ~0.19 µs | well-matched |
| **합계** | 0.6-1.0 µs | **0.35 µs** | overestimate |

총 회복량은 예측의 ~40 %. 가설 방향은 맞음 (DPA kernel work 가 cap) 하지만
개별 항목 비용은 예상보다 작음. 남은 1.14 µs 갭의 구성:

1. **`ensure_producer_slot` 자체 비용** (chunking loop 제거해도 1번은 호출됨,
   `producer_slots_inflight < cap` 비교 + atomic) — 추정 ~0.2 µs
2. **`is_consumer_empty` 호출** (1 dma_copy 당 한 번) — 추정 ~0.2 µs
3. **`comp_msg` 6 필드 vs M2 의 3 필드** — 추정 ~0.05 µs
4. **`padded_total` 계산 + `pos` wrap-around 분기** — 추정 ~0.05 µs
5. **`handle_msgs` 매 drain_all_rings iter** — 추정 ~0.1 µs
6. **reverse direction 의 DMA 트래픽이 forward 와 PCIe BW 공유** — 추정 ~0.5 µs
   (가장 클 것으로 추정 — M2 는 forward only)

(6) 이 맞다면 **dpumesh 가 forward-only 가 아닌 한 M2 ceiling 에 도달 불가**.
reverse direction 자체가 architectural cost. RPS 당 dma_copy 수를 줄이는 게
(forward + reverse 통합?) 다음 큰 변수.

#### 6.7.7 결론

- §6.6 의 "DPA kernel work 가 cap" 가설 **방향성 검증됨** (+9.0 % RPS 회복)
- 단일 항목으로는 **chunking bypass 가 가장 큼** (절반 이상 기여)
- p99 latency profile 이 깨끗하게 개선 (55K target 이 elbow → sustainable)
- 남은 1.14 µs 갭은 chunking/validation/writeback 외의 작업 (producer_slot
  check, consumer_empty check, reverse direction PCIe 공유) 으로 분산

---

### 6.8 Flow control 정리 — 현재 레이어 audit (2026-05-27)

dpumesh 의 flow control 은 다음 7 개 layer + 3 개 retry queue 로 구성:

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

#### 6.8.1 관찰

1. **Layer 4, 5 의 atomic counter mirror** 는 DOCA 가 task pool 사용량을
   exposes 안 해서 우회로 만든 것. 매 send/recv 마다 `atomic_fetch_add` + 비교.
   per-dma_copy ~0.1 µs 추정 (small but non-zero).

2. **Layer 1 의 `ensure_producer_slot`** 이 §6.7 chunking bypass 의 큰 이득의
   원인. 매 chunk 호출 → fast path 에선 1번 호출 → ~0.2 µs 절감.

3. **Layer 3 의 BP_HIGH/BP_LOW hysteresis** 는 잘 설계됨. oscillation 없이
   backpressure. saturation 측정 중 deferred_recv 0 으로 관찰됨 (BP 미발동).

4. **3 개 retry queue 분리** 는 ownership 명확 (TX_ACK / recv / consumer) 하지만
   각자 drain 코드 중복. 만약 통합 retry priority queue 였다면 -50 줄.

5. **Layer 2 (consumer empty) 와 Layer 3 (comp_queue full)** 는 사실상 같은
   "DPU side 가 처리 못 따라감" 신호를 두 곳에서 잡음. Layer 2 는 즉시 DPA spin,
   Layer 3 는 recv 보류. 만약 Layer 3 가 잘 동작하면 Layer 2 의 spin 은 거의
   안 함 (보류된 recv 가 consumer pool 채우지 않으므로). 현재 보면 saturation
   에서도 양쪽 다 정상 — 둘 중 어느 한 쪽이 redundant 인지는 부하 패턴에 따름.

6. **Layer 7 의 host_credit 갱신은 lazy refresh** (margin 64) — 잘 설계됨.
   M2 는 reverse 없으니 비교 불가.

#### 6.8.2 단순화 가능 후보 (우선순위 낮음 — 성능 영향 미미)

- (a) `send_tasks_in_flight` mirror 제거: DOCA API 가 capability check 메서드를
  지원하는지 확인 후, 지원하면 atomic 제거 가능. ~0.1 µs/dma_copy 절감.
- (b) 3 retry queue → 단일 priority queue: 코드 단순화 목적. 성능 변화 미미.
- (c) `is_consumer_empty` spin 에 backoff 추가: cap 영역에서 burst spin 시간
  단축. 미세 latency 개선 가능.

이중 (a) 가 유일하게 throughput 에 보일 만한 변경. 다른 것들은 코드 정리
목적.

#### 6.8.3 결론

flow control 자체는 **correctness 측면에서 잘 설계됨**. 단순화 여지는 있지만
throughput cap 의 주된 원인 아님 (§6.7 의 DPA kernel work 가 더 큼). 코드
가독성 / 유지보수 관점에서 (b) 정도가 합리적 cleanup 후보.

---

### 6.9 Inverse 실험 — M2 baseline 에 dpumesh feature 추가 (2026-05-27)

§6.7 은 dpumesh 에서 feature 를 빼면서 측정. **§6.9 는 반대로 M2 baseline 에
feature 를 하나씩 더하면서 측정** — 더 깔끔한 attribution. M2 의 알려진 fast
point (325K) 에서 시작.

#### 6.9.1 실험 설계

`/home/jukebox/test_dma/bench/device/dpa_kernel.c` 의 `poll_desc_ring_dma_copy`
에 4개 compile-time toggle 삽입:

```c
/* #define ADD_VALIDATION */    /* 4 checks: mmap=0, size=0, range, padded>buf */
/* #define ADD_CHUNK_LOOP */    /* dma_copy 를 while(offset < total) 로 감쌈 */
/* #define ADD_PRODUCER_SLOT */ /* lazy 500-drain → per-call slot check + counter */
/* #define ADD_DESC_CLEAR */    /* 4-field clear + 3× writeback after dma_copy */
```

각 toggle 누적 enable, build + run.

#### 6.9.2 측정 결과 (8 KB, Method 0, H2D, 10 s)

| Stage | recv/s | Bandwidth | Δ vs prev |
|---|---:|---:|---:|
| **E0 baseline** | **321,803** | 21.08 Gbps | — |
| E1 +ADD_VALIDATION | 324,707 | 21.27 Gbps | +0.9 % (noise) |
| E2 +ADD_CHUNK_LOOP | 319,495 | 20.93 Gbps | **-1.6 %** |
| E3 +ADD_PRODUCER_SLOT | 303,173 | 19.86 Gbps | **-5.1 %** |
| E4 +ADD_DESC_CLEAR | **17,639** | 1.15 Gbps | **-94.2 %** ❗ |

E4 의 collapse 가 예상 못 한 결과. isolation 측정 추가:

| Variant | recv/s | 비고 |
|---|---:|---|
| E4-alone (DESC_CLEAR only, 다른 toggle OFF) | 17,510 | E4 누적과 동일 — 다른 feature 와 상호작용 무관 |
| E4-min (`valid=0` 1줄 + writeback 1번만) | 17,225 | 3 writeback 도, 4 field clear 도 무관 — **writeback 자체가 catastrophic** |

→ ADD_DESC_CLEAR 의 비용은 writeback 횟수가 아니라 **첫 writeback 의 비용 자체**.

#### 6.9.3 왜 writeback 이 M2 에서 catastrophic 인가

**M2 의 ring 은 lossy** — host `get_next_dma_desc` 가 valid 안 체크하고 blind
write (M2 가 공유하는 ring.c):

```c
struct dma_desc *get_next_dma_desc(struct dma_ring *ring) {
    struct dma_desc *desc = ring->descs + ring->head;
    ring->head = (ring->head + 1) % ring->size;
    return desc;   /* no valid check */
}
```

vs **dpumesh 의 lossless 모델** — host 가 `desc->valid == 1` 이면 NULL 반환:

```c
if (desc->valid) return NULL;   /* host waits for DPA to clear */
```

DPA 가 `__dpa_thread_window_writeback()` 발사하면 host memory 의 같은 cache
line 에 PCIe 쓰기. M2 의 host 는 그 cache line 에 동시 write (`desc->addr =
...; desc->size = ...; desc->valid = 1`). **양쪽이 같은 cache line 을 동시에
쓰면 PCIe cache coherency cost 폭증**.

dpumesh 는 host 가 valid=0 기다리고 그 동안 안 씀 → **cache line 충돌 없음** →
같은 writeback 이 cheap.

#### 6.9.4 §6.7 (remove from dpumesh) 와 정량 비교

같은 feature 에 대해 § 6.7 (제거시 회복) vs §6.9 (추가시 손실):

| Feature | §6.7 dpumesh 제거 회복 | §6.9 M2 에 추가 손실 | 비대칭? |
|---|---:|---:|---|
| VALIDATION | +1.7 % | 0 % (noise) | matched: 작음 |
| CHUNK_LOOP | +5.5 % | **-1.6 %** | dpumesh 가 더 큼 — chunking 안의 per-chunk side effects 가 dpumesh 에선 추가로 있음 |
| PRODUCER_SLOT | (§6.7 미측정) | -5.1 % | meaningful cost |
| DESC_CLEAR | +1.7 % | **-94 %** | **극단적 비대칭 — architecture 차이** |

비대칭의 의미:
- **VALIDATION** — 거의 free 양쪽 다. 측정 일치.
- **CHUNK_LOOP** — dpumesh 에서 +5.5 % 인데 M2 에선 -1.6 %. 이유: dpumesh chunking
  loop 안에 `ensure_producer_slot` + `is_consumer_empty` wait + `producer_slots_inflight++`
  까지 함께 들어있음. 그 모든 per-chunk 작업이 N=1 chunk 일 때도 발동되어 누적.
  M2 의 ADD_CHUNK_LOOP 은 loop wrapper 만 (without side effects) 이라 cost 작음.
- **PRODUCER_SLOT** — M2 에서 -5.1 % 측정. dpumesh 에선 chunk 안에 hidden 되어
  있어 따로 측정 안 됨. CHUNK_LOOP 의 5.5 % 대부분이 이것일 가능성.
- **DESC_CLEAR** — 비대칭이 가장 큼. dpumesh 에서는 backpressure 신호로 host 가
  존중하므로 cheap. M2 에서는 cache line 충돌로 catastrophic. **같은 코드라도
  context (lossy vs lossless) 에 따라 cost 가 100배 차이**.

#### 6.9.5 Baseline = E0 (M2 의 canonical 측정)

**비교 baseline 은 항상 E0 (321,803 dma_copy/s, 3.11 µs/op)**. M2 의 실제 측정값.
E8/E9/E10 은 polling amortization 효과를 보여주는 sanity check 이지 alternative
ceiling 이 아님 (§6.9.7 참고).

**dpumesh-vs-baseline 갭**:

```
M2 baseline (E0):  321,803 dma_copy/s   (3.11 µs/op)   ← 기준
dpumesh 실측:      220,000 dma_copy/s   (4.55 µs/op)
─────────────────────────────────────────────────────
갭:                101,803 dma_copy/s   (1.44 µs/op)   = 32 %
```

#### 6.9.6 함의

1. **§6.7 (strip) + §6.9 (add) 를 결합** 해야 정확한 attribution. 단일 방향만
   으로는 context 효과 못 잡음.
2. **DESC_CLEAR 의 100× 비용 차이는 architectural** — 같은 코드도 lossy vs
   lossless context 에서 다르게 동작. dpumesh 의 lossless backpressure 는
   "공짜로 얻은" 게 아니라 host 의 valid wait + DPA 의 writeback 이 한 쌍으로
   설계되어야 함. M2 의 lossy 모델 위에 dpumesh 의 desc clear 만 가져오면
   catastrophic.
3. **§6.9 의 attribution** 은 baseline = E0 기준. dpumesh-vs-E0 갭의 비율이
   진짜 기준.

#### 6.9.7 추가 측정 — comp_msg / handle_msgs / consumer_wait, 그리고 amortization sanity check (E8-E10)

§6.9 측정을 (E5-E7 = 추가 feature) + (E8-E10 = polling amortization 검증) 으로
확장:

| Variant | recv/s | Δ vs E0 | per-iter time | 비고 |
|---|---:|---:|---:|---|
| **E0 baseline** | **321,803** | **0 (기준)** | 3.11 µs | 1 poll : 1 op |
| E5 +ADD_LARGER_COMP_MSG | 294,207 | -8.6 % | 3.40 µs (+0.29 µs) | 3 → 7 fields (12B → 24B immediate) |
| E6 +ADD_HANDLE_MSGS | 315,513 | -2.0 % | 3.17 µs (+0.06 µs) | `consumer_get_completion` 1× per iter |
| E7 +ADD_EXTRA_CONSUMER_WAIT | 322,857 | +0.3 % (noise) | 3.10 µs | 사실상 free |
| E8 +ADD_REVERSE_DMA | 381,826 | +18.6 % | 5.24 µs / 2 op | **1 poll : 2 op** ← amortize |
| E9 +ADD_4_DMA_COPIES | 417,119 | +29.6 % | 9.62 µs / 4 op | **1 poll : 4 op** ← 더 amortize |
| E10 +ADD_4_DMA_COPIES +EXTRA_RING_POLLS=3 | 338,322 | +5.1 % | 11.83 µs / 4 op | **4 poll : 4 op** ← polling fair |

#### 6.9.8 E8/E9 의 "성능 향상" 은 anomaly 가 아니라 polling amortization

E8/E9 가 baseline 보다 빨라진 건 cap 분석의 정답이 아니라 **단순 산수**:
**polling 1번의 비용을 N 개 dma_copy 가 나눠 가지면 per-op time 이 떨어짐**.

선형 회귀로 per-iter time 분해:
```
per_iter_time = α + N × β  (N = dma_copies/iter, 1 poll/iter 고정)
3.11 = α + 1 × β  (E0)
5.24 = α + 2 × β  (E8)
9.62 = α + 4 × β  (E9)
⇒ β ≈ 2.13 µs/dma_copy (marginal cost of adding one)
  α ≈ 0.98 µs (per-iter fixed overhead — desc poll + consumer_empty wait 등)
```

즉:
- M2 의 per-iter overhead = **0.98 µs** (1 poll + 1 consumer_empty wait + drain check)
- dma_copy 1개의 marginal cost = **2.13 µs**
- E0 의 3.11 µs = 0.98 (iter overhead) + 1 × 2.13 (1 dma_copy)
- E9 의 2.40 µs/op = (0.98 + 4 × 2.13) / 4 = 9.62/4 (iter overhead 가 4× amortize)

E8/E9 의 throughput 상승은 dpumesh 가 도달 가능한 ceiling 이 아니라 **polling
amortization 의 측정**. dpumesh 의 실제 패턴 (4 poll : 4 op, §6.6.6 ratio
1.017) 은 amortization 이 없으므로 E0 와 같은 baseline 위치에 있어야 함.

#### 6.9.9 E10 — dpumesh 와 같은 polling pattern 으로 매칭한 sanity check

**E10 = E9 + EXTRA_RING_POLLS=3**: 4 polls + 4 dma_copies = dpumesh 의 1:1 ratio
매칭. **측정 338,322 recv/s**.

E10 의 의미: "**dpumesh 가 multi-op-per-iter 의 amortization 효과만 얻고 추가
overhead 가 없다면**" 이론값이 338K. dpumesh 실측은 220K — amortization 효과는
얻고 있지만 (∵ E0 의 321K 와 E10 의 338K 가 5 % 차이밖에 안 남), **dpumesh-specific
추가 작업** 으로 인한 손실이 있음.

**E0 와 E10 의 차이는 작음 (321K vs 338K = +5 %)** — dpumesh 가 1 poll : 1 op
패턴인 한, amortization 으로 얻을 게 거의 없음. **즉 비교 baseline 으로 E0 를
쓰는 것이 정직**.

#### 6.9.10 dpumesh-vs-E0 갭의 attribution

**갭 = 1.44 µs/op (=32 %)**. §6.9 inverse + §6.7 strip 측정으로 설명되는 부분:

| 원인 | per-op 비용 | dma_copy/s loss (vs E0) |
|---|---:|---:|
| larger comp_msg (7 vs 3 fields, E5) | +0.29 µs | ~27K |
| `ensure_producer_slot` per call (E3) | +0.17 µs | ~17K |
| `handle_msgs` (E6) | +0.06 µs | ~6K |
| desc clear (lossless context, §6.7) | +0.05 µs | ~5K |
| chunking loop wrapper (E2) | +0.05 µs | ~5K |
| validation (E1) | ~0 µs | 0 |
| extra consumer wait (E7) | ~0 µs | 0 |
| **측정 합계** | **0.62 µs** | **~60K (59 %)** |
| Unknown | ~0.82 µs | ~42K (41 %) |
| **합계** | **1.44 µs** | **~102K** |

**측정 가능한 항목으로 갭의 59 % 설명**. 나머지 41 % unknown 의 후보:

1. **DPA scheduling overhead** — dpumesh DPA 가 yield + wake (731 exec/s, 273
   dma_copy/wake) 패턴. wake transition, EU context 재로드, completion queue
   재구성 등. 추정 ~0.3-0.5 µs/dma_copy.
2. **PCIe BW contention** — dpumesh forward + reverse 가 같은 PCIe lane 의
   양방향 traffic. 추정 ~0.1-0.3 µs/dma_copy stall.
3. **dpumesh 의 wider hot-path state** — 4 rings × ring_info (~64B each) + 4
   desc_idx[] + 4 pos[] + dpa_sent_count[] + dpa_cached_freed[] 등 cache
   footprint 증가. 추정 ~0.05-0.1 µs.
4. **amortization 불완전성** — §6.6.6 의 1.017 ratio 는 평균. 가끔 ring 1-2개만
   active 일 때 ratio 가 더 올라감. 평균 위로 가는 outlier 가 throughput
   끌어내림. 추정 ~0.05-0.1 µs amortized.

#### 6.9.11 결론

**dpumesh-vs-M2 baseline (E0) 갭의 진짜 크기**: 102K dma_copy/s (1.44 µs/op,
**32 %**).

**§6.9 측정으로 attribute 가능한 부분: 59 %**. 우선 최적화 후보 (single-core):

| 변경 | 잠재 이득 |
|---|---:|
| comp_msg 필드 줄이기 (7 → 3, req_id 외 필요한지 검토) | +27K dma_copy/s |
| `ensure_producer_slot` → lazy drain (M2 식 매 500) | +17K |
| `handle_msgs` 빈도 감소 (매 iter → 매 N iter) | +6K |
| 합계 | **~+50K dma_copy/s = dpumesh 220K → 270K = +23 % RPS** |

**남은 41 % unknown (0.82 µs/op)** 은 single-feature 추가/제거로 잡히지 않는
integration-level cost. DPA scheduling 패턴, PCIe BW contention, cache footprint,
amortization 불완전성. architectural 변경 필요.

**E8/E9/E10 의 역할**: dpumesh 가 cap 인 이유가 아니라 polling amortization 이
어떻게 동작하는지의 sanity check. dpumesh 의 1:1 poll:op 패턴에서는 (E10 이
보여줬듯) E0 와 큰 차이 없는 이론값 (~338K) 에 있어야 함. 거기서 220K 로 떨어진
102K 손실이 진짜 attribution 대상.

#### 6.9.12 추가 확인 — E11 (PCIe direction balance), ARM CPU 분포, single-pod 분석

##### E11: 2 H2D + 2 D2H balanced (dpumesh 의 실제 PCIe 방향 비율 매칭)

E10 (3H+1D) 는 dpumesh (2H+2D) 와 방향 비율이 skew 됨. E11 = E10 의 ADD_4_DMA_COPIES
분기를 `rev_dir = (extra >= 1)` 로 바꿔 2H+2D 매칭.

| Variant | recv/s | per-op time | 비고 |
|---|---:|---:|---|
| E10 (3H+1D + 4 polls) | 338,322 | 2.96 µs | skewed forward |
| **E11 (2H+2D + 4 polls)** | **337,262** | **2.97 µs** | balanced (dpumesh ratio) |

**Δ = -0.3 % (noise)**. PCIe direction balance 는 throughput 에 영향 없음. **PCIe
가 full-duplex 라 forward + reverse 가 같은 BW lane 을 공유하지 않음** — §6.9.10
의 "PCIe BW contention (~0.1-0.3 µs)" 가설은 부정. unknown 0.82 µs 중 이 부분
제거 → DPA scheduling overhead 와 cache footprint 쪽이 더 큰 후보.

##### ARM CPU 분포 — 기존 flame 데이터로 attribution

`bench/dpumesh_dpu_flame_per_batch.svg` 의 ARM CPU subtree:

| ARM 함수 | self-time % | per-event 환산 |
|---|---:|---:|
| `doca_pe_progress` (epoll_pwait 포함) | 76.5 % | 3.48 µs (polling syscall) |
| `process_completion_queue` | 7.95 % | 0.36 µs |
| `process_rev_notify_entry` | 5.18 % | 0.24 µs |
| `find_pod_by_id` (top-level inline) | 2.80 % | 0.13 µs |
| `server_send_msg_to_conn` | 2.20 % | 0.10 µs |
| comp_queue ops (peek/empty/full/dq/eq) | ~3.5 % | 0.16 µs |
| `process_forward_entry` | 0.91 % | 0.04 µs |
| `drain_deferred_tx_acks` | 0.54 % | 0.02 µs |
| **ARM useful work 합계** | **~23 %** | **~1.05 µs/event** |

**중요**: ARM 과 DPA EU 는 **다른 processor 가 병렬 작동**. dpumesh 의 cap =
max(DPA per-op, ARM per-event). 측정:
- DPA per-dma_copy: **4.55 µs**
- ARM per-event: **1.05 µs**
- → **DPA 가 cap, ARM 은 ~3.5 µs 헤드룸**

ARM 측 최적화 (comp_queue 우회, find_pod 캐싱, TX_ACK piggyback) 는 DPA 가 cap
인 한 throughput 에 영향 없음. **DPA kernel work 줄이는 게 유일한 방향**.

##### Single-pod / idle ring 효과 — 분석적 추정

dpumesh 의 4 rings 중 일부가 idle 상태일 때 (예: bench self-loop 으로 echo pod
의 ring 2개가 idle):
- §6.6.6 측정: 4 rings 다 active = ratio 1.017 (perfect amortize)
- Single-pod self-loop: 2 active + 2 idle = **ratio 2.0** (idle ring 폴링 낭비)

§6.9.2 의 single-op per-extra-poll cost ~0.8 µs. multi-op amortized cost ~0.19 µs
(E9→E10 의 per-extra). dpumesh 의 multi-op amortized case 에서 추가 1 poll 당
~0.19 µs.

추정:
- 정상 ratio 1.017 → idle 시 ratio 2.0 = 추가 1 poll/op = **+0.19 µs/op**
- dpumesh 4.55 → 4.74 µs/op → **~211K dma_copy/s** (현재 220K 의 -4 %)

→ ring 개수 자체는 작은 영향 (다 active 시), idle ring 이 폴링 낭비 만들 때만
중요. 현재 echo bench 는 4 rings 모두 active 이므로 비교 의의 작음. **실험 setup
없이 분석 충분**.

##### DPA busy-spin retry — §6.6.3 결과 그대로 (재실행 안 함)

이전 측정: `thread_reschedule` 제거 시 55K p99 91 → 224 ms (2.5× 악화). 원인은
4 rings × continuous polling 의 PCIe poll rate 폭증. yield 는 단순 idle 절전이
아니라 **PCIe poll rate throttle** 역할. 같은 가설이라 재실험 unnecessary.

#### 6.9.13 최종 정정된 attribution (E11 결과 반영)

§6.9.10 의 unknown 41 % 후보들 중 **PCIe BW contention 항목은 E11 로 부정**.
unknown 의 진짜 구성 후보:

| Unknown 후보 | 이전 추정 | E11 후 정정 |
|---|---:|---:|
| DPA scheduling overhead (yield/wake) | 0.3-0.5 µs | **0.4-0.6 µs** ↑ (PCIe 빠진 만큼 흡수) |
| PCIe BW contention | 0.1-0.3 µs | **~0 µs** ↓ (E11 로 부정) |
| Wider hot-path state (cache) | 0.05-0.1 µs | 0.1-0.2 µs ↑ |
| Amortization 불완전성 | 0.05-0.1 µs | 0.1-0.2 µs ↑ |
| **합계** | ~0.82 µs | ~0.82 µs (총량 동일, 분포 정정) |

DPA scheduling overhead 가 unknown 의 가장 큰 단일 후보 (0.4-0.6 µs, 약 50 %).
나머지는 cache footprint + amortization 불완전성으로 분산.

**최종 결론** (E0 baseline = 321K 기준):
- 갭 = **1.44 µs/op = 32 %** (101.8K dma_copy/s)
- 측정 attribution: **0.62 µs (59 %)**
- Unknown: **0.82 µs (41 %)** — 거의 모두 DPA-internal cost (scheduling + cache + amortization)
- **PCIe direction / BW 는 cap 아님** (E11 확정)

#### 6.9.14 Yield/wake architecture 비용 — M2 에 wake 메커니즘 이식 (2026-05-27)

§6.9.12 의 "DPA scheduling overhead" 후보 (~0.4-0.6 µs/op 추정) 를 직접 측정.

##### 1단계 — M2 + naive yield (wake source 없음)

M2 DPA kernel 에 `thread_reschedule()` 추가 (273 dma_copy 마다). M2 는 dpumesh 처럼
host→DPA trigger 메커니즘이 없음.

**측정: 273 recv/s** — 정확히 1 burst 처리 후 **영원히 hang**.

→ `doca_dpa_dev_thread_reschedule()` 는 **진짜 yield**. 외부 wake source 가 없으면
DPA 가 깨어나지 못함. cooperative 가 아니라 **block 형 suspend**.

##### 2단계 — M2 + yield + wake (DPU 1kHz keepalive 이식)

dpumesh 의 wake 메커니즘 모방:
- M2 DPU worker 에 `dmesh_doca_dpa_msgq_send` 로 매 1ms trigger msg 발사
- M2 DPA kernel 의 yield 직전 consumer comp queue 드레인 + ack (msg 누적 방지)
- yield counter 를 file-scope global 로 (function-local static 은 reset 됨 — 아래 발견 참조)

**측정** (burst=273, 1kHz wake):

| Variant | recv/s | per-op time | Δ vs E0 |
|---|---:|---:|---:|
| E0 (M2 busy-spin) | 321,803 | 3.11 µs | — |
| **M2 + yield + 1kHz wake (burst=273)** | **272,928** | **3.66 µs** | **+0.55 µs = -15.2 %** |
| M2 + yield + 1kHz wake (burst=1000) | 249,916 | 4.00 µs | +0.89 µs = -22.3 % |

##### 발견 — DPA thread re-entry 시 function-local static 이 reset 됨

처음 시도: `static int yield_counter` 사용. 결과 999 recv/s (= 거의 1 wake/ms 마다
1 dma_copy 만 처리). 분석: DPA thread 가 `thread_reschedule()` 후 **함수 entry
부터 restart** 됨. function-local static 이 재초기화되어 매 wake 마다 1 dma_copy
후 다시 yield.

해결: file-scope global 사용 + **function entry 에서 reset**. 둘 다 필요한 이유는
RESUME / RESTART semantics 양쪽 안전하게 처리하기 위해. dpumesh 도 `dpa_sent_count[]`
등 file-scope global 사용 (function-local static 안 씀).

##### 의미

dpumesh 의 unknown 0.82 µs/op 중 **~0.55 µs 가 yield/wake architecture cost** 임이
직접 측정으로 확인:

| 갭의 분포 (E0 baseline 기준) | 비용 | 출처 |
|---|---:|---|
| §6.9 inverse 측정 (E1-E7) | 0.62 µs | direct |
| **Yield/wake (M2 yield+wake 실측)** | **~0.55 µs** | direct |
| Cache footprint + amortization 불완전성 | ~0.27 µs | 잔여 |
| **합계** | **1.44 µs** | ✓ |

→ **dpumesh 의 갭 1.44 µs 중 ~82 % 가 직접 측정으로 attribute** (0.62 + 0.55 =
1.17 µs of 1.44 µs).

##### Yield 비용의 정체 — 단순 syscall 아닌 wake interval mismatch

burst 273 (=849µs 처리) 와 wake interval 1 ms (=1000µs) 의 mismatch 가 비용의
주된 원인. burst 1000 (=3110µs) 으로 늘리면 wake interval 보다 길어져 다른 식의
mismatch (다음 keepalive 까지 wait) 발생 → -22 % 로 더 악화.

dpumesh 의 731 exec/s × 273 ops/exec = 200K 도 같은 패턴 — wake 메커니즘의
internal timing 이 cap 결정. 이 비용을 줄이려면:
- Wake rate 를 burst 처리 시간에 매칭 (예: 250 Hz wake + 1000 op burst)
- 또는 wake-on-event (host trigger 가 desc post 마다 발사) 로 변경

dpumesh 는 (실제로) host trigger 도 함께 사용 — `dpumesh_enqueue` 가 DPU 로
trigger 보냄. 1 kHz keepalive 는 idle fallback. multi-source wake 가 dpumesh 가
~200K 까지 도달하는 이유.

##### 결론 정정

§6.9.10 의 "Unknown ~0.97 µs" 는 yield/wake direct 측정으로 보정:

| 원인 | per-op 비용 (정정 후) |
|---|---:|
| 측정된 features (E1-E7 + §6.7) | 0.62 µs |
| **Yield/wake architecture (직접 측정)** | **~0.55 µs** |
| Cache + amortization (잔여) | ~0.27 µs |
| **합계 = 실측 갭** | **1.44 µs (32 %)** |

**dpumesh 갭의 ~82 %** 가 단일 변수 측정으로 attribute. 나머지 ~18 % (~0.27 µs)
는 cache footprint + amortization 불완전성 등 작은 effects.

---

### 6.10 dpumesh busy-spin 재시도 (with §6.7 strips) + slot leak 발견 (2026-05-27)

§6.6.3 의 busy-spin 시도는 strips (`STRIP_VALIDATION`/`DESC_CLEAR`/`CHUNK_LOOP`) **전에** 했음.
지금 strips 적용된 상태에서 다시 시도하면 어떻게 되나? PCIe BW 부담이 줄었으니 결과가
다를 수 있다는 가설.

#### 6.10.1 실험 설계

dpumesh `dpa_kernel.c` 의 `run_dma_manager`:

```c
/* 변경 — yield 라인 주석 처리 */
while (1) {
    handle_msgs(thread_arg);
    (void)drain_all_rings(thread_arg);
    drain_producer_completions(thread_arg);
    /* if (chunks == 0)
        doca_dpa_dev_thread_reschedule(); */
}
```

§6.7 의 strips 모두 활성 상태에서 deploy + sweep.

#### 6.10.2 첫 sweep — 표면적으론 개선

| Target | yield 유지 (§6.7 baseline) | **busy-spin (지금)** |
|---|---:|---:|
| 50K | 49,659 / 9.36 ms | **49,614 / 8.67 ms** |
| 55K | 54,617 / 12.27 ms | **54,628 / 9.61 ms** |
| 65K | 59,195 / 952 ms | **59,784 / 819 ms** |

**모든 target 에서 p99 개선** (50K -7 %, 55K **-22 %**, 65K -14 %). ach 도 65K 에서
+1 %. §6.6.3 와 정반대 — strips 가 DPA EU per-dma_copy work 를 줄여 PCIe BW 헤드룸
만든 효과로 추정.

#### 6.10.3 그러나 back-to-back stability check 에서 catastrophic failure

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

**Slot leak — host 의 ring `head=160` slot 이 `valid==1` 인 채로 stuck**.

#### 6.10.4 코드 추적 — 왜 hang 이 되었나

`ring.c` 의 `get_next_dma_desc` 가 `desc->valid==1` 일 때:

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

→ `head` 가 안 움직임. 다음 호출도 같은 slot 폴링.

`bench_dpumesh.c` 의 worker:
```c
if (dpumesh_enqueue(g_ctx, &desc) < 0) {
    /* tx_slot/req_id cleanup */
    atomic_fetch_add(w->fail, 1);
    continue;             /* 다음 iter — 또 같은 stuck slot 부딪힘 */
}
```

→ Worker 가 fail 카운트만 증가, 같은 slot 계속 재시도 → **무한 fail loop**.

**Recovery 로직 진짜 0**:
- ❌ Stuck slot timeout
- ❌ Force-clear (host 가 valid=0 강제)
- ❌ head++ skip (lossy 진입)
- ❌ Ring reset
- ❌ Backoff + retry 후 escalate

설계자가 "DPA reliable" 가정으로 만든 것 — DPA 가 valid clear 못 하면 발생할 corner
case 가정 안 함. busy-spin 이 그 가정 깨는 케이스.

#### 6.10.5 정확한 leak mechanism — 코드 logical trace

`desc[N].valid` 의 모든 write site:

| Writer | Write | 발생 위치 |
|---|---|---|
| Host bench worker | `desc.valid = 1` (post) | `bench_dpumesh.c:131` |
| DPA `process_one_desc` | `desc->valid = 0` + writeback | 모든 종료 path (success / abort / consumer_empty timeout) |

코드 path 상 DPA 가 valid=1 인 desc 를 보면 **무조건 valid=0 으로 끝남**. logic 만으론
leak 불가능. 그러나 실측에서 leak 관찰됨.

가능한 race condition:
1. **DPA window cache stale read** — host 의 `valid=1` write 가 coherency network 통해
   DPA window 의 그 cache line 을 invalidate 해야 하는데 busy-spin 의 PCIe + coherency
   부담으로 invalidation 누락/지연. DPA 가 `window_read_inv()` + read 했는데 여전히
   stale `valid=0` 봄 → `if (!desc->valid) return 0;` → desc_idx 안 advance → **그
   slot 영원히 skip**.
2. **DPA → host writeback propagation 누락** — DPA 의 `valid=0` writeback 이 host CPU
   cache 까지 invalidate 못 함. Host 가 stale `valid=1` 봄. 그러나 5+ 초 갭이면
   propagation 시간 충분 — less likely.

**(1) 이 코드 trace 상 plausible 한 유일한 path**. 정상 coherency 라면 발생 안 해야
하지만 busy-spin 의 stress 상황에서 corner case 로 발생.

#### 6.10.6 §6.6.3 와 §6.10 의 종합

| | §6.6.3 (strips 전) | §6.10 (strips 적용 후) |
|---|---|---|
| Single-run 측정 | 55K p99 91→**224 ms** 악화 | 55K p99 12→**9.6 ms** 개선 |
| Back-to-back 안정성 | 미측정 | **fail (slot leak)** |
| 결론 | busy-spin 안 좋음 (PCIe poll storm) | busy-spin 안 좋음 (slot leak) |

**두 실험 모두 dpumesh 의 busy-spin 부적합 확인**. mechanism 다르게 보일 수 있지만
근본은 같음 — **multi-ring busy-spin 의 coherency stress 가 어딘가에서 race 만들어냄**.
yield 가 그 race 의 implicit barrier 역할.

§6.6.3 는 race 가 latency tail 로 manifest (writeback propagation 지연 → host 가
가끔 stuck slot 발견 → bench worker timeout). §6.10 는 strips 로 latency 자체는
줄였지만 race 자체는 그대로 → 누적된 leak 가 back-to-back 에서 catastrophic 발현.

#### 6.10.7 결론 — yield 는 architectural necessity

dpumesh 가 **multi-pod / multi-ring / lossless** 인 한 yield 불가피:
- multi-ring busy-spin = PCIe + coherency stress
- coherency race = `valid` bit 의 stuck 가능성
- lossless ring 모델 = stuck slot 가 hang 으로 표면화 (M2 lossy 면 그냥 덮어씀)
- **yield 의 ~0.55 µs/op 비용 (§6.9.14) 은 이 architectural 보호의 가격**

§6.7 의 strips 로 dpumesh 가 +9 % 회복했지만 busy-spin 으로 더 짜내려 하면 stability
잃음. **현재가 (yield + strips) production 최적점**.

---

### 6.11 측정 환경 종합 기록 (2026-05-27 기준)

#### 6.11.1 Hardware

| 항목 | 사양 |
|---|---|
| Host CPU | (DVFS lock @ 2.5 GHz, governor=performance, cores 0-7) |
| Host RAM | (TBD) |
| DPU | NVIDIA BlueField-3 |
| DPU ARM | 8 cores (no pinning — dpumesh 의 offload 강점 유지) |
| DPU DPA | FlexIO EU × 1 (single thread for dpumesh) |
| PCIe | Gen4, dpumesh ↔ host 간 |
| Network | (테스트 환경 내부 통신만, NIC 외부 트래픽 없음) |

#### 6.11.2 Software

| 항목 | 버전 / 설정 |
|---|---|
| DOCA SDK | 3.1.0105 |
| FlexIO library | 25.07.2812 |
| Kernel | Linux 5.15.0-176-generic |
| Kubernetes | (test-bench namespace, single node) |
| DPU 시작 옵션 | `dpumesh_dpu $DPU_PCI -l 40` (WARN+ filter) |
| Test harness | `test-bench.sh` (deploy / dpumesh / tcp / pin) |

#### 6.11.3 측정 도구

| 도구 | 용도 |
|---|---|
| `test-bench.sh dpumesh <rps> <dur> <size> [<conns>]` | dpumesh RPS sweep |
| `run_bench.sh --method 0 ...` (test_dma/bench) | M2 baseline 측정 |
| `perf record -F 999 -g --call-graph dwarf` | flame graph 캡처 |
| `perf trace -s -p <pid>` | syscall summary (epoll_pwait latency 등) |
| `/opt/mellanox/doca/tools/dpa-statistics collect` | DPA EU 통계 (active %, executions, dma_copy) |
| `top -H -p $(pgrep dpumesh_dpu)` | ARM thread CPU 사용률 |
| DPA stat counters (`stat_inner_iters/polls/dma_copies`) | dpumesh DPA 측 polls/dma_copy ratio 실측 |

#### 6.11.4 dpumesh 현재 적용된 변경 (working tree)

| 파일 | 변경 |
|---|---|
| `comch_server.c` | `pods_*` mutex → `__atomic_*` lock-free (hot read path) |
| `dpu_worker.c` | `process_completion_queue` 의 inner per-entry `pe_progress` 제거 (per-batch 만) |
| `dpu_worker.c` | DPA stat counter 출력 (INFO 레벨, `-l 40` 에선 안 보임) |
| `dpa_common.h` | `stat_inner_iters/polls/dma_copies` 필드 추가 |
| `device/dpa_kernel.c` | `STRIP_VALIDATION` enabled |
| `device/dpa_kernel.c` | `STRIP_DESC_CLEAR` enabled (3× writeback → valid=0 만) |
| `device/dpa_kernel.c` | `STRIP_CHUNK_LOOP` enabled (size ≤ 8KB single-chunk fast path) |
| `device/dpa_kernel.c` | DPA stat counter 증가 (drain_all_rings 안) |

#### 6.11.5 M2 baseline (test_dma/bench) 현재 상태

| 파일 | 변경 |
|---|---|
| `device/dpa_kernel.c` | `DOCA_DPA_DEV_LOG_*` 모두 no-op (DPU 메모리 압력 회피) |
| `device/dpa_kernel.c` | `EXTRA_RING_POLLS=0` (E0 baseline 상태로 복원) |
| `device/dpa_kernel.c` | `ADD_VALIDATION`/`CHUNK_LOOP`/`PRODUCER_SLOT`/`DESC_CLEAR_*`/`LARGER_COMP_MSG`/`HANDLE_MSGS`/`EXTRA_CONSUMER_WAIT`/`REVERSE_DMA`/`4_DMA_COPIES`/`YIELD`/`YIELD_WITH_WAKE` 매크로 모두 OFF (실험용 toggle, 현재 비활성) |
| `dpa.c` | `DOCA_LOG_*` 모두 no-op |
| `dpu_worker.c` | `DOCA_LOG_INFO/ERR/DBG` no-op (WARN 만 유지 — `recv:` stat 출력용) |

#### 6.11.6 최종 attribution 표 (E0 baseline = 321,803 dma_copy/s 기준)

| 항목 | 비용 | 측정 출처 |
|---|---:|---|
| Larger comp_msg (7 vs 3 fields) | +0.29 µs/op | §6.9 E5 (M2 add, -8.6%) |
| Per-call producer slot check | +0.17 µs/op | §6.9 E3 (M2 add, -5.1%) |
| handle_msgs per iter | +0.06 µs/op | §6.9 E6 (M2 add, -2.0%) |
| Chunking loop wrapper | +0.05 µs/op | §6.9 E2 (M2 add, -1.6%) |
| Desc clear (in lossless context) | +0.05 µs/op | §6.7 STRIP_DESC_CLEAR (dpumesh remove) |
| Validation | ~0 µs/op | §6.9 E1 (noise) |
| Extra consumer empty wait | ~0 µs/op | §6.9 E7 (noise) |
| **측정 합계** | **0.62 µs/op (59 %)** | direct |
| Yield/wake architecture cost | **+0.55 µs/op** | §6.9.14 (M2 + yield+wake, -15.2%) |
| Cache footprint + amortization 불완전성 | ~0.27 µs/op (잔여) | 추정 |
| **합계 = 실측 갭** | **1.44 µs/op (32 %)** | E0 → dpumesh |

#### 6.11.7 모든 실험 데이터 인덱스

| 실험 | 위치 |
|---|---|
| §6.4 In-place forwarding 적용 RPS sweep | bench.md §6.4.2 |
| §6.6.1 per-batch pe_progress | bench.md §6.6.1 |
| §6.6.2 perf trace epoll_pwait | bench.md §6.6.2 |
| §6.6.3 DPA busy-spin (strips 전) | bench.md §6.6.3 |
| §6.6.4 Load gen 검증 | bench.md §6.6.4 |
| §6.6.5 M2 synthetic multi-ring (EXTRA_RING_POLLS) | bench.md §6.6.5 |
| §6.6.6 DPA polls/dma_copy ratio counter (Experiment X) | bench.md §6.6.6 |
| §6.7 dpumesh feature 단계적 제거 A/B/C | bench.md §6.7 |
| §6.8 Flow control audit | bench.md §6.8 |
| §6.9.1-6 Inverse M2 + DESC_CLEAR catastrophic | bench.md §6.9 |
| §6.9.7-11 E5-E10 (comp_msg, handle_msgs, reverse, 4_dma_copies) | bench.md §6.9.7-11 |
| §6.9.12 E11 PCIe direction balance | bench.md §6.9.12 |
| §6.9.13 보정된 attribution (PCIe BW 가설 부정) | bench.md §6.9.13 |
| §6.9.14 yield+wake architecture cost | bench.md §6.9.14 |
| §6.10 dpumesh busy-spin retry + slot leak | bench.md §6.10 |

#### 6.11.8 Flame graph 파일

| 파일 | 캡처 시점 |
|---|---|
| `bench/m2_dpu_flame.svg` | M2 baseline (Method 2 chain) — log 강도 높던 시절 |
| `bench/dpumesh_dpu_flame.svg` | dpumesh 초기 (in-place forwarding 전) |
| `bench/dpumesh_dpu_flame_inplace.svg` | §6.4 in-place forwarding 적용 후 |
| `bench/dpumesh_dpu_flame_optimized.svg` | comch_server lock-free 적용 후 |
| `bench/dpumesh_dpu_flame_per_batch.svg` | §6.6.1 per-batch pe_progress 적용 후 |

---

| 경로 | 내용 |
|------|------|
| `bench/bench_dpumesh.c` | A안 client daemon (dpumesh init 1회, ctrl TCP 9092) |
| `bench/echo_dpumesh.c` | A안 server daemon (32 worker thread) |
| `bench/bench_tcp.go` | B안 client daemon (TCP, ctrl TCP 9092) |
| `bench/echo_tcp.go` | B안 server (Go, listen 9092) |
| `bench/Dockerfile.*` | 4개 image (dpumesh쪽: libthrift+DOCA, tcp쪽: slim) |
| `test-bench.sh` | 배포 + 실행 스크립트 (deploy / dpumesh / tcp / pin / cleanup / logs / status) |

---

## 8. 설계 결정 (rationale)

- **bench/echo daemon 구조**: 매 실험마다 init하지 않음. dpumesh 등록은 deploy 시
  한 번. 매 실험은 ctrl TCP 명령만으로 트리거. 결과적으로 ring 등록/해제 경로를
  실험 hot path에서 제거.
- **echo는 32 thread**: 단일 thread는 ~25K RPS에서 막힘 (서버 측 큐잉으로 인한
  3ms+ p50 발생). 32 thread로 늘려서 dequeue 병목 해소.
- **wall-time 측정**: bench_dpumesh.c는 watchdog thread + 즉시 join 패턴. main이
  고정 시간 sleep하면 wall이 부풀려져 RPS underreport (이전 버전에서 25.7K로
  보고된 원인이었음).
- **wrk2 식 scheduled-time latency (CO 보정)**: bench worker 가 closed-loop
  (send → wait_response → next) 라 그대로 두면 cap region 에서 **coordinated
  omission** 이 발생 — 시스템이 느려져도 worker 가 같이 느려지므로 큐잉이
  latency 분포에 안 잡힘. 결과적으로 cap 너머에서도 p50 ≈ p99 ≈ 9.5 ms 같은
  가짜 안정 plateau 가 보이고 진짜 saturation 이 가려짐 (이전 버전의 버그).
  fix: `t0 = scheduled_time` (보내기로 예약된 tick), latency = `now() - t0`.
  worker 가 sleep_until 에서 늦게 깨면 그만큼 큐잉 wait 가 latency 에 그대로
  잡혀서 elbow 가 깨끗이 보임. 동일하게 `bench_tcp.go` 도 적용.
- **MAX_WORKERS = 4096, 128 KB stack**: closed-loop concurrency cap 이 낮으면
  (이전 512) Little's law 로 in-flight 가 묶여서 measure-able RPS 가 가짜
  plateau 를 만듦. 4096 + 128 KB stack 으로 4096 × 128 KB = 512 MB 만 차지.
  CO 보정 + 큰 cap 결합하면 latency tail 과 throughput 한계가 모두 시스템
  속성을 직접 반영함 — bench tool 의 self-throttling 이 아니라.
- **Envoy 최소 설정**: tcp_proxy filter 1개, cluster 1개. admin/HTTP/tracing/
  stats sink/access log/runtime 모두 제거. DPUmesh가 transport-only로 도는 것에
  대응.
- **2 sidecar (1개 아님)**: Istio/Envoy 모델은 client-side + server-side 두 hop.
  socat 단일 splice는 너무 가벼워서 (kernel fast-path) 비교가 부정확.
- **pinning은 taskset (CFS quota 아님)**: CFS quota는 시간만 제한, core를 안 정함.
  같은 core를 공유시키려면 pinning 필수. CFS는 redundant + 잘못된 throttling
  가능성이 있어 모두 제거.
- **DPU/DPA는 pinning 안 함**: "DPU/DPA가 host CPU와 독립"이 dpumesh 의 핵심
  advantage. 임의로 묶는 건 비교 의의를 깎음.
