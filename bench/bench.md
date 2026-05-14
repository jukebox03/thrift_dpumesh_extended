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

| | dma_copy ops/sec | vs Method 0 | vs Method 1 |
|---|---:|---:|---:|
| dpumesh baseline @ overload (53,553 RPS) | 214,212 | 67 % | 69 % |
| **dpumesh remove memcpy @ overload (54,748 RPS)** | **218,992** | **68 %** | **71 %** |
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

---

## 7. 파일

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
