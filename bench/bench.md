# DPUmesh Transport Benchmark — 결과 보고서

DPUmesh DMA transport vs TCP service-mesh(Envoy sidecar)의 raw transport 성능 비교, 그리고 dpumesh
transport의 최적화 결과와 ceiling 측정. gateway/Thrift/application 로직을 모두 제거하고 transport
비용만 분리 측정한다.

> **이 문서를 읽는 법.** 각 수치에 근거 등급을 표시한다. **[측정]** = 데이터로 직접 측정(CSV·sweep
> 로그). **[산술]** = 측정값에 산술(예: dma_copy/s = RPS×4). **[귀인]** = 측정값을 소거법/모델로 해석한
> 것 — *직접 증거가 아니며 틀릴 수 있다*(§5.2 참고). **[미측정]** = 직접 증거 없음. raw data는
> 신뢰할 수 있으나, "병목의 원인"류 귀인은 여러 번 번복되었으므로(§5.2) 등급을 보고 받아들일 것.

---

## 0. 요약

### 측정된 사실 (raw data·코드로 확증)

1. **dpumesh transport는 TCP/Envoy 대비 동일 부하에서 latency 2~3 자릿수 우위** [측정]. 40K target에서
   dpumesh p99 8.35 ms / 0 fail, TCP/Envoy p99 3,885 ms (§2).
2. **단일 EU chain은 누적 최적화로 sustainable 52K→74K RPS, overload 53.5K→77.4K RPS** [측정], 전
   구간 0 fail (§3). 77,434 RPS = 309,736 dma_copy/s [산술] = M2-N=1 baseline(320,105)의 96.8%,
   pure single-EU engine(556K)의 55.7% (§4).
3. **chain throughput은 *활성 EU 수*에 스케일**한다: single-EU 76K → 2-EU 104K RPS(+37%) [측정].
   2-pod echo는 ring→EU=`pod%N`이라 N≥2에서 활성 EU가 항상 2개 → N=2≈N=4≈104K (코드 확증, §4).
4. **engine은 chain보다 훨씬 빠르다** [측정]: pure single-EU 556K dma_copy/s(size 무관 → op-rate
   bound), 2-EU 1.07M(1.93×), 4-EU peak 1.8M, **N=8에서 절대 감소**(1.47M). M0(drain만) N=8 1.53M,
   M2(drain+forward) N≥4 ~1.0M plateau (§4). chain 2-EU 416K dma_copy/s [산술] = M2-N=2(625K)의 67%.
5. **chain 천장은 in-flight 깊이가 아니다** [측정]: depth 2048→4096에서 throughput 평탄(~103K),
   latency만 깊이에 비례, 더 밀면 achieved 하락 — 교과서적 M/M/1 고정-서비스율 큐(§4.4의 가장 강한
   positive 증거).
6. **DPU ARM 제어평면을 2 코어로 분할해도 throughput 무변화** [측정]: sends-only −0.9%, rebalanced
   +2%→0%(노이즈). 분할은 0-fail로 correctness만 입증(§4.3).

### 병목의 원인 — [귀인], 직접 확증되지 않음

- **chain이 416K(2-EU)/309K(1-EU)에서 멈추는 *원인*은 직접 측정되지 않았다.** §5.1의 M/M/1 거동은
  측정되었으나, 그 고정-서비스율을 "2개 활성 EU의 결합된 per-RTT 작업"으로 *국소화*한 것은 소거법에
  의한 귀인이다(§5.2). 결정적 실험(독립 multi-ARM 샤딩 / >2 pod로 활성 EU 증가)은 **아직 안 했다**.
- **pure N=8 regression의 원인 "공유 DMA-engine op-rate 천장 ~1.6M"** 역시 소거법뿐이다(spin/
  oversubscription/window/bandwidth/drain은 직접 배제됨, 그러나 그 공유 자원이 DMA-engine이라는
  positive 증거는 없음 — occupancy는 polling으로 무용)(§5.2).

### 남은 lever (§6)

활성 EU 수 ↑(>2 pod 필요) · host→host direct DMA(RTT당 dma_copy 4→2, USER L7 결정 선행) ·
per-op EU 비용↓(§3에서 M2 96.8%까지 소진). single-EU 최적화는 사실상 종착.

---

## 1. 실험 설계

### 1.1 대상

| | A안 (DPUmesh) | B안 (TCP via Envoy) |
|---|---|---|
| client→server hop | 1 (DPU) | 2 (sidecar1, sidecar2) |
| transport 위치 | DPU ARM + DPA EU (host CPU 외부) | host CPU 내 (app과 core 공유) |
| L7 처리 | 없음 (transport-only) | 없음 (`tcp_proxy` filter만) |

가설: dpumesh의 architectural advantage는 transport가 host CPU 외부(DPU/DPA)에서 일어난다는 점.
동일 host CPU 예산(각 1 core × 2 pod)에서 TCP는 app+sidecar가 core를 나눠 쓰고 dpumesh는 app이 1
core를 통째로 쓴다.

### 1.2 Topology & Resource Layout

```
HOST NODE (test-bench ns, governor=performance @ 2.5GHz, cores 0-7)
  A안: core0 bench-dpumesh / core1 echo-dpumesh(ECHO_THREADS=64) + comch+DMA(mlx5/PCI)
  B안: core2 bench-tcp(bench+sidecar1) / core3 echo-tcp(sidecar2+echo)
DPU (BlueField-3, host CPU 외부, /dev/infiniband PCI)
  ARM(8core): dpumesh_dpu — control PE(comch_server) / consumer PE(DPA→ARM) / dpu_worker(routing/ACK)
  DPA(FlexIO EU×N): run_dma_manager — drain_all_rings: forward DMA(host→DPU) + reverse DMA(DPU→host)
```

CPU pinning은 `taskset -apc` hard-pin (CFS quota 미사용). DVFS lock `cpupower ... -g performance
-d 2.5GHz -u 2.5GHz` (latency tail noise 제거).

| profile | layout |
|---|---|
| **fair** (default, TCP 비교) | core0 bench-dpumesh / core1 echo-dpumesh / core2 bench-tcp+sidecar1 / core3 echo-tcp+sidecar2 / DPU ARM 8core 자유 / DPA EU×N |
| **hw** (host-side 한계 측정) | core0,4 bench-dpumesh(2core) / core1,5 echo-dpumesh(2core) / core2,3 tcp(untouched) |

K8s 객체(test-bench ns): bench-dpumesh(1c)/echo-dpumesh(1c)/bench-tcp(2c)/echo-tcp(2c), services
bench-dpumesh:9092 / bench-tcp:9092 / echo-tcp:9091, sidecar1-config(upstream=echo-tcp:9091) /
sidecar2-config(upstream=127.0.0.1:9092). hostPath: /dev/infiniband, $BUILD_DOCA/lib(libthrift).

### 1.3 실행 인터페이스

```bash
./test-bench.sh deploy                                    # build + image + DPU restart + pods Ready + fair pin
./test-bench.sh dpumesh    <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh tcp        <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE> [<CONNS>]   # hw profile 자동 전환
DPUMESH_DPA_THREADS=N ./test-bench.sh deploy              # N개 DPA EU (chain)
```
Ctrl protocol(line): `RUN <rps> <dur_sec> <msg_size> [<conns>]` →
`OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>`. (daemon raw µs, 표는 ms 통일.)

### 1.4 측정 방법론

- **wrk2식 scheduled-time latency (CO 보정)**: `t0=scheduled_time`, `latency=now()-t0`로 큐잉 wait를
  latency에 포착 → cap region에서 elbow 선명(coordinated omission plateau 제거). 양 client에 적용.
- **MAX_WORKERS=4096, 128 KB stack**: concurrency cap이 낮으면 Little's law로 in-flight 묶여 가짜 plateau.
- **wall-time 측정**: watchdog thread + 즉시 join (고정 sleep은 wall 부풀려 RPS underreport).
- **도구**: `test-bench.sh`(chain sweep) / `run_bench.sh`(M0/M2) / `run_pure_dma.sh`(pure engine) /
  `perf record/trace` / `dpa-statistics collect` / `top -H -p $(pgrep dpumesh_dpu)`.

### 1.5 측정 환경

| | |
|---|---|
| Host CPU | DVFS lock @ 2.5 GHz, governor=performance, cores 0-7 |
| DPU | NVIDIA BlueField-3, ARM 8core, DPA FlexIO EU partition 0-63 |
| PCIe | Gen4 |
| DOCA SDK | 3.1.0105 · FlexIO 25.07.2812 · Kernel 5.15.0-176-generic |
| DPU 시작 | `dpumesh_dpu $DPU_PCI -l 40` (WARN+ filter) |

### 1.6 측정 구성 3종 & dma_copy 정의

`dma_copy` = `doca_dpa_dev_comch_producer_dma_copy` (per-op completion immediate 동반). **DPU recv/s
== dma_copy/s** (동일 관측점). 세 구성은 *같은* copy primitive를 쓰고 주변 작업만 다르다(코드 확증):

| 구성 | 설명 | host descriptor gate | per-op completion | 스크립트 |
|---|---|---|---|---|
| **pure_dma** | EU가 고정 회전창에 dma_copy back-to-back | **없음** | 있음 | `run_pure_dma.sh` |
| **M0** | host desc ring 경유, DPU는 카운트만 | 있음 | 있음 | `run_bench.sh --method 0` |
| **M2** | M0 + DPU ARM이 매 완료를 host로 `server_send_msg` forward | 있음 | 있음 | `run_bench.sh --method 2` |
| **chain** | 실제 mesh, 1 RTT = forward×2 + reverse×2 = **4 dma_copy** | 있음 | 있음 | `test-bench.sh dpumesh` |

pure_dma가 M0와 다른 것은 **per-op `desc->valid` PCIe 악수의 제거 한 가지뿐**(나머지 데이터 경로·
완료 통보·DPU recv drain 동일) — 그래서 556K vs 320K vs 416K는 같은 primitive 위의 *주변 작업 증가*
비교로 공정하다(코드 확증). **1 RTT = 4 dma_copy**: `dpa_kernel.c`에 dma_copy 호출지점은 forward
(`process_fwd_ring`)·reverse(`process_rev_ring`) 둘뿐이고, echo RTT는 host→host 전달이 2회(요청·
응답)이므로 2×(1 fwd + 1 rev) = 4 (코드 확증). 모든 cross-comparison(416K=104K×4, 309,736=77,434×4)이
이 비율에 의존한다.

---

## 2. 결과 1 — TCP/Envoy 대비 (rps=40000, dur=10s, size=8192B) [측정]

| | dpumesh | tcp/Envoy |
|---|---|---|
| Achieved RPS | **39,732.7** (99.3%) | **33,097.0** (82.7%) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 | **4.53 ms** | 661.50 ms |
| p99 | **8.35 ms** | 3,885.30 ms |
| p999 | 8.62 ms | 4,588.43 ms |
| Throughput(RTT) | 620.82 MB/s | 517.14 MB/s |

40K target에서 dpumesh는 healthy(sustainable 52K의 76%), TCP/Envoy는 deep overload(achieved 33.1K,
p99 3,885 ms). **caveat**: 단일 부하점 비교다 — TCP는 이미 saturation 너머, dpumesh는 elbow 아래.
서로 다른 부하 regime을 비교하므로 "architectural advantage"는 latency 우위라는 사실로만 읽고,
배율로 일반화하지 말 것.

---

## 3. 결과 2 — 단일 EU chain 최적화 (8KB, 0 fail 전 구간)

### 3.1 Baseline sweep (in-place forwarding 전) [측정]

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 5,000 | 4,968.6 | 1.10 | 1.93 | 2.01 | 77.63 MB/s | 50,000/0 |
| 10,000 | 9,939.2 | 1.60 | 2.82 | 2.94 | 155.30 | 100,000/0 |
| 15,000 | 14,906.5 | 2.08 | 3.72 | 3.88 | 232.91 | 150,000/0 |
| 20,000 | 19,875.0 | 2.57 | 4.64 | 6.48 | 310.55 | 200,000/0 |
| 25,000 | 24,838.1 | 3.06 | 5.55 | 5.80 | 388.10 | 250,000/0 |
| 30,000 | 29,807.8 | 3.54 | 6.51 | 6.74 | 465.75 | 300,000/0 |
| 35,000 | 34,767.7 | 4.05 | 7.45 | 7.73 | 543.24 | 350,000/0 |
| 40,000 | 39,732.7 | 4.53 | 8.35 | 8.62 | 620.82 | 400,000/0 |
| 45,000 | 44,691.3 | 5.02 | 9.29 | 9.63 | 698.30 | 450,000/0 |
| 50,000 | 49,664.7 | 5.41 | 11.89 | 12.17 | 776.01 | 500,000/0 |
| **52,000** | **51,644.8** | **5.53** | **12.16** | 12.33 | 806.95 | 520,000/0 |
| 55,000 | 53,553.4 | 74.14 | 189.59 | 194.61 | 836.77 | 550,000/0 |
| 57,000 | 53,404.0 | 319.63 | 604.52 | 611.89 | 834.44 | 570,000/0 |
| 60,000 | 53,698.8 | 587.51 | 1,154.25 | 1,166.28 | 839.04 | 600,000/0 |
| 65,000 | 53,841.1 | 1,043.73 | 1,961.63 | 1,983.88 | 841.27 | 650,000/0 |

sustainable ≈ 52K, overload ceiling ≈ 53.5K (60/65K 밀어도 53.7K 고정, latency만 발산). 0 fail 전
구간(4-layer backpressure 흡수).

### 3.2 누적 최적화 (각 단계 = 측정 endpoint) [측정]

| Phase | 기법 | Sustainable elbow(p99≤~14ms) | Overload(RPS) | DMA ops/s | vs M2-N1 |
|---|---|---:|---:|---:|---:|
| baseline | (§3.1) | 52K | 53,841@65K | 215,364 | 67% |
| in-place forward | staging memcpy 제거, source mmap 직접 read | 54K | 54,748@65K | 218,992 | 68% |
| feature strip | validation/desc-clear/chunk-loop compile-out | 55K | 59,195@65K | 236,780 | 74% |
| cleanup | dead chain 제거, lazy drain, throttle | 60K | 62,068@65K | 248,272 | 78% |
| comp_msg 28B→16B | wire format packing (WQE BB 1개) | 65K | 65,859@67K | 263,436 | 82% |
| SDK 위임 + OPTIMIZE_REPORTS | producer slot counter 제거, 완료 batch | 68K | 68,127@70K | 272,508 | 85% |
| **fence/batch drain** | read_inv hoist + RING_BATCH_CAP=32 + wb batch | **74K** | **77,434@78K** | **309,736** | **96.8%** |

각 기법은 `dpa_kernel.c`에 영구 반영(§부록 B). throughput milestone: comp_msg packing에서 1 GB/s
돌파(65K@1,008 MB/s), fence/batch에서 1,186 MB/s(overload).

**fence/batch 최종 sweep** [측정]:

| Target | 30K | 50K | 65K | 73K | 74K | 75K |
|---|---:|---:|---:|---:|---:|---:|
| p99(ms) | 5.19 | 7.97 | 9.83 | 13.50 | **13.57** | 15.83(overload) |

Back-to-back(74K×4 + 73K×2 + 30K recovery): 편차 ach<0.05% / p99<1%, 0 fail.

### 3.3 정직성 주석 (per-step 귀인의 한계)

- **누적 결과(52K→74K sustainable, +42%; overload 215K→310K dma_copy/s, +44%)는 robust** —
  서로 다른 측정·0-fail back-to-back으로 확증 [측정].
- 그러나 **개별 기법의 기여 배분은 run-to-run 노이즈(~±1.3%)에 묻히는 경우가 있다.** feature-strip
  단계 안의 +1.7% 같은 sub-step delta는 §4/§5의 노이즈 바닥과 같은 크기다. 큰 단계(fence/batch
  drain +8.5%)는 노이즈 위지만, +2% 미만 기여를 "확정 기여"로 읽지 말 것.
- **`comch_dma_comp_msg` 28B→16B**: 7필드는 라우팅/Thrift semantic에 모두 쓰여 줄일 수 없어 wire만
  packing(type→u8, src/dst→i8, 재배치로 4B-aligned). `_Static_assert(sizeof==16)`. 16B = HW WQE BB
  1개.
- **`dma_desc` 32B pack 시도 → 실패(영구 기록)**: `__dpa_thread_window_writeback()`가 cache-line
  단위 RMW라 32B 2개가 한 line 공유 시 stale 이웃을 clobber → slot stuck. **64B-per-desc는 padding이
  아니라 writeback 격리를 위한 load-bearing 제약** [측정 신호 + 코드 확증; 정확한 clobber 이벤트는
  failure signature에서 추론].
- **busy-spin(yield 제거) 시도 → 단일 run은 p99 개선되나 back-to-back 붕괴**(slot leak, 0 RPS).
  관측 사실: yield 제거 시 multi-ring busy-spin이 slot stuck을 유발. *메커니즘*("DPA window cache가
  host valid=1을 stale 읽음")은 [귀인]이며 직접 관측되지 않음(missing recovery logic, fence/ordering
  bug 등 다른 설명도 배제 안 됨). 결론: **yield는 multi-ring lossless chain에서 보존**(§부록 B).

---

## 4. 결과 3 — 측정된 천장 계층 [측정]

모든 수치 dma_copy/s. chain은 RPS×4 정규화 [산술].

### 4.1 천장 계층 한 장

| 계층 | 천장 (dma_copy/s) | 근거 |
|---|---:|---|
| pure single-EU 발행 | **556K** | [측정] size 무관(128B≈8KB) → op-rate bound |
| pure 2-EU | **1.07M** (1.93×) | [측정] near-linear |
| pure DMA-engine 공유 천장 | **~1.6–1.8M** | [측정] N=4 peak ≈1.7-1.8M; N=8 contention으로 1.5-1.6M로 *하락* |
| M0 (데이터평면+단일 drain) N=8 | **1.53M** | [측정] drain은 ≥1.5M까지 스케일 |
| M2 (단일 ARM one-way forward) N≥4 | **~1.0M** | [측정] plateau |
| **chain (2 활성 EU)** | **416K** (104K RPS×4) | [측정]+[산술] M2-N=2(625K)의 67% |

### 4.2 pure_dma engine [측정]

**single-EU, size 무관** (`pure_dma_results_20260529_190327.csv`):

| size | dma_copy/s | payload BW |
|---:|---:|---:|
| 8 KB | **555,383** | 36.39 Gbps |
| 128 B | **556,149** | 0.56 Gbps |

128B와 8KB가 동일 → 대역폭 아니라 **copy 1번당 고정비용(WQE 발행 + 완료 통보)**이 천장 = op-rate bound.
8KB의 4.55 GB/s는 PCIe Gen4(~32 GB/s) 한참 미달.

**multi-EU 스케일링** (`..._194717.csv` N=1, `..._194834.csv` N=2/4/8):

| EU 수 | dma_copy/s | scaling | payload BW |
|---:|---:|---:|---:|
| 1 | 554,789 | 1.00× | 36.35 Gbps |
| 2 | 1,070,930 | 1.93× | 70.18 Gbps |
| 4 | **1,804,363** | **3.25×** | 118.25 Gbps |
| 8 | **1,466,216** | **2.64× (절대 감소)** | 96.08 Gbps |

N=4 peak 후 **N=8 절대 감소** = 단순 포화(plateau)가 아니라 능동적 간섭. **caveat**: 1.8M은 per-op
DPU completion이 섞인 **측정 하한**(§4.6) — 진짜 multi-EU engine 천장은 더 높을 수 있다.

### 4.3 N=8 regression — 직접 배제한 후보 (2026-06-02, run_pure_dma.sh) [측정]

throughput만 사용(occupancy 무용). backoff=256, 8KB.

| 실험 | N=4 | N=8 | 판정 | CSV |
|---|---:|---:|---|---|
| ① baseline | 1,710,039 | 1,517,996 | regression 재현 | `..._115553.csv` |
| ② affinity (thread i→EU i) | 1,766,900 | 1,522,404 | 변화 없음 → oversubscription 아님 | `..._120037.csv` |
| ③ positive control (8 thread → EU0) | — | **544,451** | ≈single-EU → affinity API 작동 확증 | `..._120409.csv` |
| ④ fixed 128KB window | 1,734,649 | 1,581,702 | N=4 안 떨어짐 → window-shrink 아님 | `..._120638.csv` |
| ⑤ op-rate vs BW (128B vs 8KB) | — | 128B=1,558,214 / 8KB=1,594,613 | 64× payload 차에도 op-rate 동일 → BW 아님 | `..._120953.csv` |

**spin-contention 배제** (`..._200702.csv`): N=8 backoff 0 vs 256 = 1,557,332 vs 1,526,402 (오히려
약간 악화). **단일 drain 배제**: M0가 같은 단일 consumer_pe로 N=8 1.53M 도달(§4.5).

> **caveat (affinity 실험 ②③)**: 이 실험들이 쓴 affinity 커널/`dpa.c` 변형은 현재 복원된 tree에
> 없다 — 재현 불가. 현재 `pure_dma`/`bench` 코드에는 affinity API가 없다(코드 확증). chain만 affinity ON.

### 4.4 M0 / M2 multi-EU & chain N-스케일링 [측정]

**M0/M2 N=1/2/4/8** (`bench_results_20260602_020604.csv` N=1,2 + `..._021256.csv` N=4,8):

| N | M0 (drain만) | scaling | M2 (+forward) | scaling | M2/M0 |
|---:|---:|---:|---:|---:|---:|
| 1 | 324,630 | 1.00× | 321,754 | 1.00× | 0.99 |
| 2 | 628,622 | 1.94× | 625,708 | 1.94× | 0.99 |
| 4 | 1,187,147 | 3.66× | 1,006,224 | 3.13× | 0.85 |
| 8 | **1,534,325** | **4.73×** | **1,024,805** | **3.19×(plateau)** | 0.67 |

(N=1 baseline 별도 확인: M0 325,407 / M2 320,105, `bench_results_20260530_014304.csv`.)
N=2는 M2≈M0(near-linear) → 단일 ARM forward는 N=2를 안 가둠. N≥4에서 M2 ~1.0M plateau (M0는 계속 증가).

**chain N-스케일링** (fair, 2-pod echo, 8KB, OK/Fail 전부 0):

| target | N=1 | N=2 | N=4 | N=4 (hw, 2-core host) |
|---:|---:|---:|---:|---:|
| 78,000 | 75,922 | — | — | 77,468 |
| 85,000 | 74,704 | — | 84,352 | 84,367 |
| 95,000 | — | 94,117 | 94,249 | 94,194 |
| 105,000 | — | **104,100** | **104,185** | 104,220 |
| 115,000(과부하) | — | 102,806 | 105,695 | 104,526 |

- ring→EU=`pod%N`이라 N≥2에서 활성 EU 항상 2개 → **N=2≈N=4≈104K** (코드 확증, `dpa.c:1084`).
- fair(1-core host) ≈ hw(2-core host) ≈ 104K → **병목은 host posting 아님** [측정].
- N=1 ~76K = §3.2 fence/batch(77,434) 재현(리팩터 회귀 0).
- **multi-EU 실이득**: 2 활성 EU가 76K→104K(+37%, 1.37×). pure 2-EU 1.93×보다 낮음 = chain 추가 작업.

**buffer/depth 반증** [측정]:

| 변경 | 결과 | 판정 |
|---|---|---|
| `DPU_COMP_QUEUE_SIZE` 4096→16384 + `RING_BATCH_CAP` 32→128 | N=2@105K → 100,064 (변화 0) | depth cap 아님 |
| `CC_DPA_MAX_MSG_NUM` 1024→4096 | 배포 실패 (DPA HW recv-task 한계 초과) | HW상 불가 |

### 4.5 in-flight 깊이 — depth-bound 아님 (M/M/1) [측정, positive]

깊이 결정 3종을 함께 2배(`DPU_BUFFER_SIZE` 16→32MB, `DMA_RING_SIZE`/`DPUMESH_NUM_SLOTS`/`rq_depth`
2048→4096). 2-EU, 8KB.

| target | 2048 원본 | 4096 | 2048 restored | p50 2048→4096→restored |
|---:|---:|---:|---:|---|
| 95,000 | 94,115.1 | — | 94,151.6 | 12.14 → — → 12.09ms |
| 104,000 | 103,181.0 | 103,001.5 | 103,203.6 | 14.50 → **47.86** → 14.19ms |
| 110,000 | 104,893.6 | — | 104,891.9 | 233.52 → — → 243.94ms |

4096 추가 sweep: 120K=100,947 / 140K=97,901 / 160K=95,196 / 180K=92,237 / 200K=89,852 (achieved가
부하 밀수록 *하락*).

**판정**: 4096에서 throughput 평탄(~103K)인데 latency만 깊이에 비례(104K p50 14.5→47.9ms ≈
queue_depth/μ), 더 밀면 achieved 하락 — **교과서적 M/M/1 포화**(줄만 길어지고 처리율 μ 불변).
**버퍼는 병목이 아니다.** 16MB/2048 복구 후 정확 재현(104K p50 47.9→14.2ms, 110K 천장 104,892 ≈
원본 104,894). 이것이 chain 천장이 "고정 서비스율"이라는 **유일한 positive 증거**다.

### 4.6 pure/M0/M2/chain 같은-EU-수 비교 [측정+산술]

| | single-EU | 2-EU | N=8 |
|---|---:|---:|---:|
| pure_dma | 556K | 1.07M | 1.5–1.6M (DMA-engine 천장 부근) |
| M0 (drain만) | 325K | 629K | 1.53M |
| M2 (+forward) | 322K | 626K | 1.02M (plateau) |
| **chain** | ~309K | **416K** | — (2-pod=2EU cap) |

같은 HW·device·단일 drain에서 pure 2-EU=1.07M, M0 N=8=1.53M → HW/drain은 chain 416K의 cap이 아님.
M2가 단일 ARM으로 N=2 1.94×·N≥4 1.0M까지 스케일 → 단일 ARM "존재" 자체도 cap 아님. chain 2-EU
416K = M2-N=2(625K)의 **67%** (여유 1.5×). 나머지 33% gap의 *원인*은 §5.2 참조(직접 측정 안 됨).

### 4.7 DPU ARM 멀티코어 분할 — throughput 무효 [측정]

`DPUMESH_SPLIT_SEND` 토글: 0=단일 루프(byte 동치), 1=sends-only split(A=route/consumer_pe,
B=send/cc_server, lock-free SPSC), 2=rebalanced(A=drain만, B=route+reverse+send). 2-pod echo, 8KB,
fair-pin, EU affinity ON. ARM split: A=core2/B=core3.

| 구성 | overload 천장(RPS) | dma_copy/s | sustainable elbow | Δ vs off |
|---|---:|---:|---|---:|
| 1-EU off | 75,921 (@78K) | 303,684 | 74K (p99 13.7ms) | — |
| 1-EU sends-only | 76,079 (@82K) | 304,316 | ~72K (74K→38.7ms) | +0.2%(noise), elbow −2~3K |
| 2-EU off | 104,894 (@110K) | 419,576 | 104K (p99 23.1ms) | — |
| 2-EU sends-only | 103,912 (@110K) | 415,648 | 104K (p99 23.3ms) | **−0.9%(noise)** |
| 2-EU rebalanced | 107,012 (@110K) | 428,048 | 104K (p99 22.1ms) | +2.0%@110K → **+0.0%@120K** |

rebalanced 이득은 부하 밀수록 0으로 소멸(110K +2.0% → 120K +0.0%, 103,563 vs 103,562) = 일정한
천장 이동이 아니라 노이즈. off 자체도 ±1.3% 흔들림. **결론**: send 경로를 100% 떼어내도(sends-only)
2-EU 104K 무변화 → binding per-RTT 작업은 send가 아니다. 분할 30 포인트 전부 0-fail → lock-free
2-스레드 설계는 건전(진단 토글로 보존, 기본 0).

> **caveat (raw sweep)**: 분할 실험의 전 포인트 per-target sweep(95K~130K, p50/p99/MB)은 별도
> harness 로그로 보관; 위 표가 1차 기록(천장·elbow·Δ). chain CSV 미보관 정책은 §부록 A와 동일.

---

## 5. 분석

### 5.1 측정으로 확실한 것

1. **chain의 cap은 HW도 EU 수도 host도 in-flight 깊이도 아니다** [측정]. pure 2-EU 1.07M·M0 N=8
   1.53M(HW/drain 여유), fair≈hw(host 무관), depth 2배 평탄(§4.5)이 각각 직접 배제.
2. **chain은 활성 EU 수에 스케일**한다(76K→104K, +37%) [측정]. 2-pod은 `pod%N`으로 2 EU 고정.
3. **chain 천장은 고정-서비스율 큐처럼 거동**한다 [측정, positive] — §4.5의 M/M/1 (throughput 평탄
   + latency ∝ depth + 과부하 시 achieved 하락). 이것이 가장 단단한 다리다.
4. **send 경로는 chain의 binding 작업이 아니다** [측정] — sends-only split이 100% 떼어내도 무변화.
5. **per-op 비용에서 EU > ARM** [측정] — flame/perf: EU ~4.55µs/op vs ARM ~1.05µs/event; M2에
   feature를 더하는 inverse 실험(E1-E7)이 per-feature 비용으로 갭을 재현.

### 5.2 귀인(소거법)은 신뢰하지 말 것 — 이 보고서의 핵심 교훈

**raw data는 신뢰할 수 있으나, "병목의 *원인*"류 귀인은 거의 전부 소거법이고, 핵심 귀인은 실험이
추가될 때마다 세 번 번복되었다:**

- **§(구버전) cap = 단일 DPA EU per-op work** → **단일 ARM per-RTT 제어평면(~2.4µs/op)** → **ARM은
  cap 아님; 2개 활성 EU의 결합된 per-RTT 고정 서비스율**. 매 단계가 직전 소거 결론을 반박했다.

소거법("A·B·C가 아니므로 D")은 **진짜 원인이 열거한 후보 집합 안에 있다고 가정**한다. 모든 후보를
열거할 수 없으면 틀린다 — 위 세 번의 번복이 그 증거다. 따라서 아래 귀인들은 *사실이 아니라 가설*로
취급한다:

| 귀인 | 등급 | positive 증거 | 직접 배제된 것 | 안 따져본 대안(예시) |
|---|---|---|---|---|
| **N=8 regression = 공유 DMA-engine op-rate 천장 ~1.6M** | [귀인] 소거뿐 | **없음** (DPA stall-cycle 미계측, occupancy는 polling으로 saturate되어 무용) | spin, oversubscription, window, BW, drain | 공유 comch 완료-통보 경로 / PCIe doorbell·credit 직렬화 / DPU recv-task pool(CC_DPA_MAX_MSG_NUM HW 한계가 힌트) / EU WQE-issue arbitration / EU→물리 DMA 채널 매핑 |
| **chain 416K = 2개 활성 EU의 결합 per-RTT 고정 서비스율** | [귀인] (M/M/1 거동만 [측정]) | M/M/1 거동은 측정됨(§4.5). 그러나 "2 EU 결합 rate"로의 *국소화*는 소거 | ARM compute(split), depth, send | 결정적 실험(독립 multi-ARM 샤딩 / >2 pod로 활성 EU↑) **미실시**; 33% gap을 "reverse+admission+lossless 결합"으로 본 것은 미측정 잔차 |
| **76.5% `doca_pe_progress`는 연산 아니라 대기** | [귀인] 해석 | 없음(epoll event-vs-timeout 비율·idle cycle 미계측) | — | — |
| **slot leak = DPA가 stale valid=1 읽음**("유일 plausible path") | [귀인] | 없음 | logical leak | missing recovery logic, fence/ordering bug, host advance bug, PCIe write-ordering race |

> **"유일하게 가능한 설명"이라고 쓰고 싶을 때가 가장 위험한 순간이다.** 그것은 보통 "내가 떠올린 것
> 중 유일"이라는 뜻이지 "객관적으로 유일"이 아니다.

### 5.3 보고서에서 정정한 오류 (코드 확증)

- **per-RTT 단일-ARM 작업 목록 정정**: admission accounting(`dpa_sent_count`/`dpa_cached_freed`)은
  **ARM이 아니라 DPA EU의 file-scope global**이다(`dpa_kernel.c:41-42, 308, 358, 430-440`). 단일-ARM
  per-RTT 작업은 ① comp_queue 4-entry drain ② `find_pod_by_id` 라우팅 ③ `dpu_enqueue_reverse_dma`
  ×2 ④ host-bound lossless send ×4(hop당 DMA_COMPLETION + TX_ACK) ⑤ full-RTT slot lifecycle —
  admission은 DPA per-op 예산에 속한다.
- **per-op 비용 분해는 *과거*(cleanup 전) 작업**이다: validation/chunking-loop/multi-writeback는 이미
  strip됨(`dpa_kernel.c` 확증). 현재 EU per-desc 작업으로 읽으면 틀린다.
- **multi-EU 1.8M은 측정 하한** — per-op DPU completion 포함. §6의 "~450K RPS 이론 상한"은 이
  하한 위에 세운 값이므로 soft하다.
- **96.8% vs 55.7%**: 309,736 = M2-N=1(320K)의 96.8%지만, 그 M2 baseline 자체가 host-feed-bound였다
  (pure single-EU 556K가 입증) → engine 기준으로는 55.7%. "96.8%"를 engine 한계 도달로 읽지 말 것.

### 5.4 부정된 가설 (직접 측정으로) [측정]

- ❌ PCIe BW contention (§4.3 ⑤ + 별도 direction-balance −0.3%)
- ❌ ARM polling이 cap (per-batch RPS flat; poll/copy ratio 1.017≈1.0)
- ❌ DPA busy-spin이 도움 (latency 2.5× 악화 + slot leak, §3.3)
- ❌ comp_msg quantum ≥ 32B (16B HW WQE BB 확정)
- ❌ desc 32B pack로 PCIe read 절감 (line-granular writeback이 이웃 clobber)
- ❌ Method 0/2 = HW max (host descriptor feed-bound였음, pure 556K가 입증)
- ❌ in-flight 깊이가 chain cap (§4.5 M/M/1)
- ❌ 단일 ARM compute가 chain cap (§4.7 split 무효)
- ❌ host posting/core 수가 chain cap (§4.4 fair≈hw)

---

## 6. Lever & 정직한 한계

**Lever (병목을 직접 깎을 후보):**

| 변경 | 잠재 이득 | 등급 | 비고 |
|---|---:|---|---|
| **활성 EU 수 ↑ (>2 pod)** | chain은 활성 EU에 스케일(N=1→2 +37%) | [측정 근거 있음] | 2-pod이 `pod%N`으로 2개 고정 → 4-pod harness 확장 필요. **이것이 "chain 천장 = 고정 서비스율" 가설의 결정적 직접 테스트이기도 하다** — 미실시 |
| **host→host direct DMA** (RTT당 4→2 dma_copy) | ~+100% | [추정] | reverse staging+중재+admission 제거. **USER L7 결정 선행**(DPU가 dst_pod_id로만 라우팅, body 미독) |
| per-op EU 비용 ↓ | 소진 | [측정] | §3에서 M2 96.8%까지 |
| send 배치 | 효과 작음 | [측정] | send는 병목 아님(§4.7) |

**정직한 한계 — 아직 안 한 결정적 실험:**

1. **chain을 독립 2쌍(4-pod)으로 2 ARM/>2 EU에 샤딩.** throughput이 오르면 "고정 서비스율" 가설
   확정, 안 오르면 반증. 교차 의존을 피해 락 없이 측정. **이 한 실험이 §5.2 첫 두 귀인을 동시에
   판가름한다.** (functional split은 같은 루프를 공유하므로 이 테스트의 대체가 아니다.)
2. **N=8 공유 자원의 positive 국소화** — DPA-side stall-cycle 계측 등. 소거가 아니라 직접 증거로.

---

## 7. comch 메시지층 리팩터 후 재검증 (2026-06-03) [측정]

comch 메시지층 리팩터 직후 **회귀 검증 + N-스케일링/split 재측정**. 측정 환경 §1.5 동일
(BlueField-3, 8KB, dur=10s, fair-pin, EU affinity ON, `-l 40`). knob은 deploy 시 환경변수로 주입
(`DPUMESH_DPA_THREADS`/`DPUMESH_SPLIT_SEND`). **전 실험 0 fail.** chain은 CSV 미보관 정책(§부록 A)이라
아래 표가 1차 기록 — raw 그대로, 누락 없이.

### 7.0 이 실험의 코드 변경 (merge 대상, §부록 B에도 추가)

- **enum 정리·용어 통일**: 두 계열을 `DMESH_MSG_*`(Host↔ARM control) / `DPA_MSG_*`(ARM↔DPA datapath,
  구 `comch_msg_type`)로 통일, **0=INVALID 예약**, 연속 번호, 죽은 값(`EXPORT_DPA_COMP`/`RX_DATA`) +
  Family B 갭{0,1,5} 제거, FWD/REV 공통 동사.
- **host-bound completion `dmesh_dma_completion_msg` 28B→16B**(control path; DPA-side
  `comch_dma_comp_msg`는 기존 16B). host control 디스패치 4B enum→**1B read**(LE 최하위 바이트).
- **vestigial consumer-id 핸드셰이크 제거**(`CONSUMER_ID`/`POD_CONSUMER_ID` + `remote_consumer_id`):
  검증 — host가 comch producer 미생성·`remote_consumer_id` read 사이트 0·CONSUMER_ID에 block 안 함.
  consumer **객체**는 유지.
- **안전수정 5종**: export_desc 스택버퍼 경계 가드, 역방향 RX `length≤slot_size` 가드 + `pos+len`
  uint32 overflow 수정, `_Static_assert(DPA_DMA_COPY_MAX≤DPUMESH_SLOT_SIZE)`, cross-ABI `_Static_assert`
  (dpa_ring_info=72/comch_add_ring_msg=80/comch_msg=84), TX_ACK `owner_req_id` 가드(wrap 후 오해제 방지).

호스트 빌드 클린 통과 + 전 static_assert 통과; DPU/DPA측은 본 deploy의 `build_dpu`로 컴파일 검증됨.

### 7.1 단일 EU (N=1, split off) — §3.2 재현 [측정]

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 30,000 | 29,782.8 | 3.38 | 5.24 | 6.19 | 465.36 MB/s | 300,000/0 |
| 50,000 | 49,653.0 | 4.85 | 7.99 | 8.99 | 775.83 | 500,000/0 |
| 65,000 | 64,529.3 | 5.81 | 9.93 | 12.90 | 1,008.27 | 650,000/0 |
| 73,000 | 72,460.1 | 6.30 | 13.59 | 16.01 | 1,132.19 | 730,000/0 |
| **74,000** | **73,446.8** | **6.40** | **13.66** | 13.92 | 1,147.61 | 740,000/0 |
| 75,000 | 73,710.8 | 66.09 | 118.97 | 121.15 | 1,151.73 | 750,000/0 (overload) |
| 78,000 | 75,200.8 | 177.69 | 309.98 | 314.94 | 1,175.01 | 780,000/0 (overload) |

**back-to-back/recovery**(redeploy 없이): 74K→73,433.9(p99 13.74ms) · 74K→73,455.7(p99 13.73ms,
편차 ach<0.03%/p99<0.1%) · 30K 회복→29,807.2(p99 5.29ms), 전부 0 fail → **slot leak 없음**.

**판정**: sustainable elbow ~74K @ p99 13.66ms(75K부터 latency 발산=overload). §3.2 fence/batch
baseline(74K, p99 13.57ms; overload 77,434@78K)을 **회귀 없이 재현**. host-bound completion 16B 압축은
처리율 중립(host control send는 chain 병목이 아니므로 — §4.7과 정합).

### 7.2 DPA multithread (N=2 / N=4, split off) — §4.4 재현 [측정]

**N=2:**

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 95,000 | 94,166.5 | 12.19 | 16.84 | 17.40 | 1,471.35 | 950,000/0 |
| 104,000 | 103,203.2 | 14.70 | 20.78 | 25.24 | 1,612.55 | 1,040,000/0 |
| **105,000** | **104,202.0** | 15.02 | **20.58** | 21.61 | 1,628.16 | 1,050,000/0 |
| 110,000 | 103,969.9 | 287.92 | 542.17 | 549.71 | 1,624.53 | 1,100,000/0 (overload) |
| 115,000 | 102,794.4 | 590.93 | 1,158.70 | 1,171.41 | 1,606.16 | 1,150,000/0 (overload) |

**N=4:**

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 104,000 | 103,102.1 | 14.63 | 21.29 | 23.69 | 1,610.97 | 1,040,000/0 |
| **105,000** | **104,168.2** | 15.15 | **22.16** | 24.56 | 1,627.63 | 1,050,000/0 |
| 110,000 | 106,993.6 | 134.93 | 269.56 | 275.93 | 1,671.78 | 1,100,000/0 (overload) |
| 115,000 | 106,624.5 | 409.10 | 772.99 | 783.68 | 1,666.01 | 1,150,000/0 (overload) |

**판정**: N=2 sustainable ~105K(104,202) = N=1(73,447) 대비 **+41.8%** [산술]. **N=2(104,202)≈N=4(104,168)**
— 2-pod은 `pod%N`으로 활성 EU 2개 고정(§4.4 코드 확증 재현). §4.4 기록(N=2 105K→104,100, N=4→104,185)과
일치.

### 7.3 DPU multithread (`DPUMESH_SPLIT_SEND`, 2-EU) — §4.7 재현 [측정]

2-EU(N=2) 위에서 ARM 기능분할. off baseline = §7.2 N=2(105K→104,202, p99 20.58ms).

**sends-only (split=1):**

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 104,000 | 103,001.4 | 14.54 | 21.51 | 23.11 | 1,609.40 | 1,040,000/0 |
| **105,000** | **104,205.9** | 15.14 | 25.44 | 28.27 | 1,628.22 | 1,050,000/0 |
| 110,000 | 106,990.4 | 148.73 | 259.76 | 264.86 | 1,671.72 | 1,100,000/0 (overload) |
| 115,000 | 105,697.4 | 429.98 | 825.91 | 834.47 | 1,651.52 | 1,150,000/0 (overload) |

**rebalanced (split=2):**

| Target | Achieved | p50(ms) | p99(ms) | p999(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 104,000 | 102,999.3 | 26.70 | 43.01 | 44.93 | 1,609.36 | 1,040,000/0 |
| 105,000 | 103,139.1 | 103.01 | 170.97 | 173.46 | 1,611.55 | 1,050,000/0 (overload) |
| 110,000 | 101,989.9 | 372.12 | 723.44 | 732.44 | 1,593.59 | 1,100,000/0 (overload) |
| 115,000 | 101,005.1 | 671.58 | 1,300.85 | 1,314.70 | 1,578.20 | 1,150,000/0 (overload) |
| 120,000 | 100,049.4 | 977.84 | 1,903.29 | 1,922.25 | 1,563.27 | 1,200,000/0 (overload) |

**판정**: 처리율 천장 — off 104,202 · sends-only 104,206 · rebalanced 103,139, 셋 다 **±1% 노이즈 내**.
**split(1/2)는 처리율 천장을 올리지 못한다**(§4.7 결론 재확인). raw 관찰: 이 run에서 rebalanced는 elbow
latency가 악화(105K p99 170.97ms vs off 20.58ms·sends-only 25.44ms)되고 105K가 이미 overload —
*원인은 미측정*(단일 run, run-to-run 노이즈 ±1.3% 및 rebalanced 경로 추가 가능성 둘 다 배제 못 함).

### 7.4 요약 (raw 기준)

| 구성 | sustainable(≈p99 elbow) | overload achieved | vs N=1 | 0-fail |
|---|---:|---:|---:|---|
| N=1 off | ~74K (13.66ms) | 75,201@78K | — | ✅ |
| N=2 off | ~105K (20.58ms) | 104,202 | +41.8% | ✅ |
| N=4 off | ~105K (22.16ms) | 106,994 | +41.8% | ✅ |
| N=2 sends-only | ~105K (25.44ms) | 104,206 | +41.8% | ✅ |
| N=2 rebalanced | ~104K(elbow↓) | 103,139 | +40.4% | ✅ |

리팩터 후에도 **§3.2(74K)·§4.4(N=2≈N=4≈104K)·§4.7(split 처리율 중립)이 모두 재현**되고 전 구간
0-fail·slot leak 없음 → **comch 메시지층 리팩터는 비회귀**(measured). 천장의 *원인* 귀인은 §5.2 그대로
(미측정).

### 7.5 추가 compaction: TX_ACK 8B + 양쪽 MT 동시 — 검증 [측정]

`dmesh_tx_ack_msg` 12→8B: type 4→1B + dead `dst_pod_id`(int32; DPU 송신이 set하나 host는 `req_id`만
읽어 미사용 — 코드 확증) 제거. completion(REV_DONE)과 대칭(둘 다 per-RTT DPU→Host 컨트롤 send).
양쪽 MT 동시(**DPA EU=2 + `SPLIT_SEND`=1 sends-only**) 구성에서 검증:

| Target | Achieved | p50(ms) | p99(ms) | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---|
| 30,000 | 29,801.1 | 3.96 | 6.16 | 465.64 MB/s | 300,000/0 |
| 104,000 | 103,112.5 | 14.59 | 21.94 | 1,611.13 | 1,040,000/0 |
| **105,000** | **104,201.7** | 14.82 | 22.11 | 1,628.15 | 1,050,000/0 |
| 110,000 | 105,926.7 | 182.52 | 311.92 | 1,655.11 | 1,100,000/0 (overload) |

**판정**: sustainable ~105K(104,202, p99 22.11ms) = 압축 전 N=2 off(104,202)·sends-only(104,206)와
동일 → TX_ACK 8B는 DPU 컴파일·동작 확인, **0-fail·비회귀**. 처리율 중립(예상대로 — host-bound send는
chain 병목이 아님 §4.7). 가치는 dead-필드 제거·wire 축소.

---

## 부록

### A. 원자료 CSV

| 구성 | CSV (경로 `/home/jukebox/test_dma/`) |
|---|---|
| pure single/size | `pure_dma_results_20260529_190327.csv` |
| pure N=1 / N=2,4,8 | `..._194717.csv` / `..._194834.csv` |
| pure backoff (N=4,8) | `..._200702.csv` |
| pure ①baseline / ②affinity / ③control / ④window / ⑤op-rate (2026-06-02) | `..._115553` / `_120037` / `_120409` / `_120638` / `_120953.csv` |
| M0/M2 N=1,2 / N=4,8 | `bench_results_20260602_020604.csv` / `..._021256.csv` |
| M0/M2 N=1 baseline | `bench_results_20260530_014304.csv` |
| chain N-sweep / split / depth | `test-bench.sh dpumesh` 로그(별도 harness, CSV 미보관) |

### B. dpumesh에 영구 반영된 변경 (merge 대상)

| 파일 | 변경 | 근거 |
|---|---|---|
| `comch_server.c`+`object.h` | `pods_*` mutex → `__atomic` publication (lock-free hot read) | §3.2 cleanup |
| `dpu_worker.c` | `process_completion_queue` per-entry pe_progress×2 → per-batch 1회 | §3.2 |
| `device/dpa_kernel.c` | validation 제거 / desc-clear 1×wb / size≤8KB single dma_copy fast path + >8KB drop | §3.2 strip |
| `device/dpa_kernel.c` | dead DMA_REQ chain ~150줄 제거, chunking fallback 제거 | §3.2 cleanup |
| `device/dpa_kernel.c` | `run_dma_manager` yield 보존(주석에 측정 근거) | §3.3 |
| `device/dpa_kernel.c` | `handle_msgs` 매 32 iter + `drain_producer_completions` 매 8 iter throttle | §3.2 |
| `device/dpa_kernel.c`+`dpa_common.h` | producer slot counter+CAP/abort 제거(SDK 위임) | §3.2 |
| `device/dpa_kernel.c` | 두 dma_copy에 `FLUSH\|OPTIMIZE_REPORTS` | §3.2 |
| `dpa_common.h` | `comch_dma_comp_msg` 28B→16B + `_Static_assert(==16)` | §3.2 |
| `dpa.c` | recv handler type read 4B→1B | §3.2 동반 |
| `device/dpa_kernel.c` | `__dpa_thread_window_read_inv()` iter당 1회 hoist | §3.2 fence |
| `device/dpa_kernel.c` | `process_fwd_ring`/`process_rev_ring` + `RING_BATCH_CAP=32` batch drain | §3.2 (+8.5%) |
| `device/dpa_kernel.c` | per-desc writeback → iter당 1회(found>0) | §3.2 fence |
| `dpa_common.h` | `dma_desc` 64B 유지 + cache-line 독점 가드 주석 | §3.3 32B-pack 실패 |
| `dpa.c`+`dpa_kernel.c` | multi-EU: `pod%N` 할당, per-EU 2D file-scope admission global, `DPUMESH_DPA_THREADS` knob | §4.4 |
| `dpu_worker.c`+`object.h`+`comch_*` | `DPUMESH_SPLIT_SEND` 0/1/2(SPSC, 3 correctness fix), 기본 0 | §4.7 (진단 토글) |
| `comch_*`/`dpa.c`/`dpu_worker.c`/`ring.c` | hot path `DOCA_LOG_DBG` 제거 | `-l 40` 일관성 |
| `comch_common.h`+`dpa_common.h` | enum 통일 `DMESH_MSG_*`/`DPA_MSG_*`, 0=INVALID, 연속 번호, 죽은 값(EXPORT_DPA_COMP/RX_DATA)+갭 제거 | §7.0 |
| `comch_common.h`+`dpumesh_doca.c`+`comch_client.c` | host-bound `dmesh_dma_completion_msg` 28B→16B + host 디스패치 1B read + `_Static_assert(==16)` | §7.1 (처리율 중립) |
| `comch_server.c`+`comch_client.c`+`dpumesh_doca.c`+`object.h` | vestigial CONSUMER_ID/POD_CONSUMER_ID + `remote_consumer_id` 제거 (consumer 객체 유지) | §7.0 (검증 후) |
| `comch_common.c` | export_desc_len 스택버퍼 경계 가드 | §7.0 안전 |
| `dpumesh_doca.c` | 역방향 RX `length≤slot_size` 가드 + `pos+len` uint32 overflow 수정 + TX_ACK `owner_req_id` 가드 | §7.0 안전 |
| `device/dpa_kernel.c`+`dpa_common.h` | `_Static_assert(DPA_DMA_COPY_MAX≤DPUMESH_SLOT_SIZE)` + cross-ABI `_Static_assert`(dpa_ring_info=72/comch_add_ring_msg=80/comch_msg=84) | §7.0 안전 |
| `comch_common.h`+`comch_server.c` | `dmesh_tx_ack_msg` 12→8B (type 1B + dead `dst_pod_id` 제거) + `_Static_assert(==8)` | §7.5 (처리율 중립) |

### C. Flame graph

| 파일 | 시점 |
|---|---|
| `bench/m2_dpu_flame.svg` | M2 baseline |
| `bench/dpumesh_dpu_flame.svg` | dpumesh 초기 |
| `bench/dpumesh_dpu_flame_inplace.svg` | in-place 후 |
| `bench/dpumesh_dpu_flame_optimized.svg` | comch_server lock-free 후 |
| `bench/dpumesh_dpu_flame_per_batch.svg` | per-batch pe_progress 후 |
| `bench/dpumesh_dpu_flame_current.svg` | 현재 |

### D. 소스 인덱스

| 경로 | 내용 |
|---|---|
| `bench/bench_dpumesh.c` / `echo_dpumesh.c` | A안 client/server daemon (echo 32 worker) |
| `bench/bench_tcp.go` / `echo_tcp.go` | B안 client/server (Go) |
| `bench/Dockerfile.*` | 4 image |
| `test-bench.sh` | A/B 배포+실행 |
| `/home/jukebox/test_dma/bench/` + `run_bench.sh` | M0/M2 micro-bench |
| `/home/jukebox/test_dma/pure_dma/` + `run_pure_dma.sh` | pure DMA-engine 측정 |

### E. 설계 결정 (rationale)

- **bench/echo daemon**: 매 실험 init 안 함(deploy 1회 등록), ctrl TCP만으로 트리거 → ring 등록/해제를
  hot path에서 제거.
- **echo 32 thread**: 단일 thread는 ~25K RPS에서 dequeue 병목.
- **Envoy 최소 설정**: tcp_proxy filter 1 + cluster 1, 나머지 제거.
- **2 sidecar**: Istio 모델(client-side + server-side 두 hop). socat 단일 splice는 kernel fast-path라 비교 부정확.
- **pinning은 taskset**: CFS는 시간만 제한·core 안 정함.
- **DPU/DPA pinning 안 함**: "DPU/DPA가 host CPU와 독립"이 dpumesh 핵심 advantage.
- **measurement caveat**: bench `host_worker.c`는 free-running(M2가 valid 안 지움 → 첫 sweep 후 영구
  valid=1, DPA free-run). latent UB지 crash 아님(HOST 로그가 생존 입증), DPU 숫자 무해.
