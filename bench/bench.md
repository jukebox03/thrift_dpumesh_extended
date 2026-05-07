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

| core | pod | container(s) | 비고 |
|------|-----|--------------|------|
| 0 | bench-dpumesh | bench_dpumesh | 1 process, 다중 worker thread |
| 1 | echo-dpumesh | echo_dpumesh | ECHO_THREADS=32 |
| 2 | bench-tcp | bench_tcp + sidecar1 | 두 container 같은 core 공유 |
| 3 | echo-tcp | echo_tcp + sidecar2 | 두 container 같은 core 공유 |
| (4-7) | — | — | DVFS lock 범위에만 포함 |
| DPU | dpumesh_dpu | ARM 8 cores 자유 | **pin 안 함 (offload 강점)** |
| DPA | run_dma_manager | EU × 1 | 코드 상수 |

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
# 한 번 배포 (build + image + DPU restart + pods Ready + pinning)
./test-bench.sh deploy

# 부하 실행 (pod 재시작 없음 — daemon이 ctrl TCP 9092 리슨)
./test-bench.sh dpumesh <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh tcp     <RPS> <DUR> <SIZE> [<CONNS>]

# pod이 재시작되면 pinning 다시
./test-bench.sh pin

# 정리
./test-bench.sh cleanup
```

ctrl protocol (line 기반):
```
RUN <rps> <dur_sec> <msg_size> [<conns>]
→ OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>
```

---

## 6. 실험 결과

### 6.1 첫 측정 (rps=40000, dur=10s, size=8192B, conns=auto)

| | dpumesh | tcp/Envoy |
|---|---|---|
| Target RPS | 40,000 | 40,000 |
| **Achieved RPS** | **39,725.6** (99.3%) | **32,269.8** (80.7%) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 latency | 3,541.0 us | 10,195.5 us |
| p99 latency | 6,774.9 us | 25,149.8 us |
| p999 latency | 7,094.5 us | 35,089.3 us |
| Throughput (RTT) | 620.71 MB/s | 504.22 MB/s |

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
많음. 즉 "Method 2 의 81.7%" 라는 숫자는 *chain 활용도* 로서는 정확하지만, 그
18 % 갭 안에는 (a) host user-space 비용 + (b) DPU ARM routing/TX_ACK/reverse-
enqueue 비용이 모두 섞여 있음. **갭 분해 (a vs b) 는 별도 측정 필요**
(예: DPU ARM 의 thread CPU% 모니터링, comp_queue depth 추세).

#### 6.2.1 Micro-bench ceiling (experiment/bench.md, 8 KB)

| Mode | 구성 | dma_copy ops/sec | Throughput (1 dir) |
|---|---|---:|---:|
| **Only DMA** (Method 0) | `dma_copy` atomic (DMA + comch immediate, HW max) | 302,617 | **19.83 Gbps** |
| **DMA + completion** (Method 2) | `dma_copy` + DPU CPU 가 host 로 `server_send_msg` | 264,869 | **17.35 Gbps** (87 % of HW) |

Method 0 → Method 2 의 13 % 손실은 "DPU CPU → host comch forward" 추가 비용.
**dpumesh 도 이 chain 을 그대로 사용** (forward DMA + reverse DMA 모두 DPU ARM
이 수신측 host 로 DMESH_MSG_DMA_COMPLETION 송신 포함). 따라서 Method 2 가
dpumesh 의 transport ceiling 의 직접 baseline.

#### 6.2.2 dpumesh saturation sweep (8 KB, 10 s, conns=auto)

| Target RPS | Achieved RPS | p50 (us) | p99 (us) | p999 (us) | Throughput (RTT) | OK / Fail |
|---:|---:|---:|---:|---:|---:|---|
| 40,000 | 39,725.6 | 3,541.0 | 6,774.9 | 7,094.5 | 620.71 MB/s | 400,000 / 0 |
| 45,000 | 44,672.8 | 4,007.5 | 7,506.5 | 7,800.3 | 698.01 MB/s | 450,000 / 0 |
| 50,000 | 49,635.3 | 4,287.5 | 9,780.4 | 10,239.9 | 775.55 MB/s | 500,000 / 0 |
| **55,000** | **54,067.8** | **9,363.8** | **9,482.1** | **9,666.1** | **844.81 MB/s** | 550,000 / 0 |
| 60,000 | 53,703.2 | 9,479.2 | 9,667.1 | 13,973.4 | 839.11 MB/s | 600,000 / 0 |
| 55,000 *(재)* | 53,555.0 | 9,587.1 | 9,616.4 | 14,496.4 | 836.80 MB/s | 550,000 / 0 |
| 55,000 *(재)* | 54,091.5 | 9,376.6 | 9,637.3 | 14,088.5 | 845.18 MB/s | 550,000 / 0 |

**관찰**:
- saturation = **≈ 54 K RPS** (55 K target 3 회 측정 모두 53.5–54.1 K, p50 ≈
  9.4 ms — 매우 reproducible).
- 50 K target 까지는 거의 linear (50 K → 49.6 K, 99 %). 55 K 가 elbow.
- 60 K target 으로 더 밀어도 RPS 가 53.7 K 로 떨어짐 (collapse).
  `experiment/report.md` 의 45 K → 50 K collapse 패턴과 동일한 saturation
  behavior.
- saturation 시 p50 ≈ p99 ≈ p999 (9.36–9.66 ms 좁은 범위) — 모든 요청이
  비슷한 큐잉 지연을 받는 안정 큐잉 상태. 60 K 는 큐가 더 깊어져 p999 가
  14 ms 까지 확장.
- 0 failure throughout — backpressure 없이도 데이터 손실은 없음.

#### 6.2.3 dpumesh 의 ceiling 대비 위치

dpumesh request 1 RTT = forward DMA × 2 + reverse DMA × 2 = **4 dma_copy ops**.

```
   100% ┃ ┌── HW max (Method 0, dma_copy atomic) ─── 302,617 ops/s = 19.83 Gbps
        ┃ │
   87.5%┃ ├── DMA + completion (Method 2)         ── 264,869 ops/s = 17.35 Gbps
        ┃ │            ↑ dpumesh 가 쓰는 chain 과 같은 구조
        ┃ │
   71.5%┃ └── dpumesh @ saturation                 ── 216,366 ops/s = 13.85 Gbps
        ┃     (54,091 RPS × 4 dma_copy)
```

| | dma_copy ops/sec | vs Only DMA (Method 0) | vs DMA+completion (Method 2) |
|---|---:|---:|---:|
| Only DMA (HW max) | 302,617 | 100 % | — |
| DMA + completion | 264,869 | 87.5 % | 100 % |
| **dpumesh @ saturation (54.09 K RPS)** | **216,366** | **71.5 %** | **81.7 %** |

**의미 있는 비교는 81.7 % (vs Method 2)** — Method 2 는 dpumesh 와 동일한
"DMA + DPU CPU → host server_send" chain 을 쓰는 구조라 같은 ground 위 비교.
Method 0 은 DPU CPU forward 비용이 빠진 absolute HW max 라 dpumesh 가 거기까지
도달 못 하는 건 당연.

**Throughput 관점**:
- dpumesh DMA 사용량: 54 K × 32 KB (4 dma_copy × 8 KB) = **13.85 Gbps**
- HW max: 19.83 Gbps → 약 **6 Gbps (28 %) 추가 활용 여지** (단 software 측 갭
  제거 조건)

**갭 18.3 % 의 의미**:
- chain capacity 는 cap 이 아님 (Method 2 baseline 의 81.7 % 활용 중)
- **chain 외 작업이 cap 결정자** — software 레벨에서만 줄일 수 있음
- 이 18 % 를 모두 제거하면 이론적으로 **Method 2 ceiling = 66 K RPS** 도달.
  그 이상은 HW 한계 (multi-EU 또는 multi-pod 분산이 필요해짐)

**갭의 정체 (DMA / non-DMA 분해)**:

이 18 % 의 갭은 "host 측인지 DPU ARM 측인지" 보다 "DMA HW 작업인지 그 외
software 작업인지" 로 분해하는 게 actionable. DMA chain 자체는 HW-bound 라
거의 못 줄이고, **non-DMA software 비용이 진짜 optimization surface**.

| 영역 | 한계의 성격 | 줄일 여지 |
|---|---|---|
| DMA chain (dma_copy + comch + PCIe writes) | HW-bound (19.83 Gbps cap) | 거의 없음 (이미 81.7 % 활용) |
| **non-DMA overhead** | software | 줄이거나 hiding 가능 — 진짜 헤드룸 |

non-DMA overhead 후보 (큰 순서 추정):
1. **Echo 의 8 KB memcpy** — `tx_buf ← rx_buf` 가 1 core 에서 ~422 MB/s 의
   CPU memcpy + cache thrashing 유발. 32 thread 가 동시 memcpy 하면 L1/L2
   계속 깨짐.
2. **Mutex / lock 경합** — bench 의 1 req 당 `slot_lock` / `p->lock` (×2) /
   `ring_lock` / `rx_slot_lock` = 5–6 회 lock acquire. 540 worker × 54 K
   /s × 6 = ~1.9 M mutex op/s 가 1 core 에 집중.
3. **Context switching** — 1 core 위 32 thread (echo) / 540 worker (bench)
   는 over-subscription. CFS 가 매 32 µs 마다 thread swap 하면서 cache
   invalidate.
4. **Per-request data structure work** — sw_descriptor 작성, comch
   immediate parse, pending table lookup, slot bitmap 갱신.

Saturation 에서 p50 = 9.5 ms 의 의미: 540 worker × 18.5 µs/req (= 1/54K) ≈
10 ms 큐잉. 즉 **bench 측 worker pool 이 cap 에서 모두 큐잉 대기 중** — DMA
chain 이 더 빨리 처리할 수 있는데도 software-side 가 못 따라가는 명확한 신호.

### 6.3 chain 활용도 18 % 갭 좁히기 — 검증 항목

1. **echo 를 multi-core** — `get_pod_cores` 에서 `echo-dpumesh` 를 `"1,5"` 로
   변경 + `ECHO_THREADS=64`. echo 가 cap 이면 RPS 거의 2 배 증가해야 함.
2. **bench 도 multi-core** — echo 가 풀린 다음 cap 후보. `"0,4"` 등.
3. **CPU 사용률 모니터링** — `mpstat -P 0,1,2,3 1` 로 100 % 도달 여부 확인.
   54 K 에서 echo core (1) 이 100 % 면 echo cap 확정.
4. **DPA EU stat** — `dpa-statistic show` 의 `producer drain stall`,
   `consumer_empty wait` 카운터. chain 내부 saturation 신호 확인.
5. **msg_size 작게 (1 KB)** — per-msg overhead 격리. 8 KB 는 DMA 비중 큼,
   1 KB 는 ops 자체로 cap 됨.

→ 이 실험들로 cap 위치 확정 후 multi-core 핀으로 chain ceiling (66 K) 에
근접 가능한지 확인. 18 % 가 chain 내부면 그게 진짜 ceiling, 외부면 user-space
재설계로 더 늘릴 수 있음.

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
