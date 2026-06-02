# DPUmesh Transport Benchmark — 결론 & 데이터

DPUmesh DMA transport vs TCP service-mesh(Envoy sidecar)의 raw transport 성능 비교,
그리고 dpumesh transport의 ceiling attribution + 단계별 최적화 기록. gateway/Thrift/
application 로직을 모두 제거하고 transport 비용만 분리 측정.

---

## 0. 결론 요약 (TL;DR)

1. **dpumesh transport는 단계적 최적화로 transport baseline(M2, §2.0) ceiling의 96.8%에 도달.**
   8 KB 기준 sustainable 52K→74K RPS, overload dma_copy/s 215K→**309,736**(= M2 320,105의 96.8%).
2. **cap은 single DPA EU의 per-dma_copy work**다. ARM/epoll/PCIe-BW/polling-ratio는 모두 cap이
   아님이 직접 측정으로 입증·반증됨. EU active%는 0.51%지만 single-EU의 yield/wake + per-op
   work가 throughput을 결정.
3. **그러나 micro-bench baseline(M0=325K, M2=320K) 자체가 host descriptor feed에
   묶인 값이었다.** host 게이트를 제거한 pure-DMA 측정에서 single EU가 **~556K dma_copy/s**
   (size-independent, op-rate bound) 달성 → baseline은 engine 한계가 아니었음. dpumesh
   309,736은 진짜 single-EU engine ceiling의 **55.7%**.
4. **op-rate는 EU 수에 비례**한다: 1→2 EU near-linear(1.93×), 4 EU peak **1.8M/s**(3.25×).
   8 EU에서 **감소(2.64×, 1.47M)** — 이 regression의 원인은 EU spin-contention이 **아님**(backoff
   무효로 확증). DPU-side drain 또는 EU oversubscription/DMA-HW contention으로 좁혀짐(진단 중).
5. dpumesh는 1 RTT = 4 dma_copy. 측정된 1.8M op-rate만으로도 ≈**450K RPS** 이론 상한(현재 77K의
   ~5.8배). **병목은 engine이 아니라 work를 먹이고 구조화하는 것**(host posting + 4-dma_copy chain
   + single-EU). 추가 이득은 §7의 architectural lever(host→host direct, multi-EU)에서만 나옴.
6. **yield는 architectural necessity**다. 제거하면 single-run latency는 좋아져도 multi-ring
   busy-spin이 slot leak(lossless ring stuck)을 유발 → back-to-back 붕괴.

---

## 1. 실험 설계

### 1.1 대상

| | A안 (DPUmesh) | B안 (TCP via Envoy) |
|---|---|---|
| client→server hop | 1 (DPU) | 2 (sidecar1, sidecar2) |
| transport 위치 | DPU ARM + DPA EU (host CPU 외부) | host CPU 내 (app과 core 공유) |
| L7 처리 | 없음 (transport-only) | 없음 (`tcp_proxy` filter만) |

가설: dpumesh의 architectural advantage는 transport가 host CPU 외부(DPU/DPA)에서 일어난다는
점. 동일 host CPU 예산(각 1 core × 2 pod)에서 TCP는 app+sidecar가 core를 나눠 쓰고 dpumesh는
app이 1 core를 통째로 씀.

### 1.2 Topology & Resource Layout

```
HOST NODE (test-bench ns, governor=performance @ 2.5GHz, cores 0-7)
  A안: core0 bench-dpumesh / core1 echo-dpumesh(ECHO_THREADS=64) + comch+DMA(mlx5/PCI)
  B안: core2 bench-tcp(bench+sidecar1) / core3 echo-tcp(sidecar2+echo)
DPU (BlueField-3, host CPU 외부, /dev/infiniband PCI)
  ARM(8core, pinning X): dpumesh_dpu — control PE(comch_server) / consumer PE(DPA→ARM) / dpu_worker(routing/ACK)
  DPA(FlexIO EU×1): run_dma_manager — drain_all_rings: forward DMA(host→DPU) + reverse DMA(DPU→host)
```

CPU pinning은 `taskset -apc`로 hard-pin (CFS quota 미사용). DVFS lock
`cpupower -c 0-7 frequency-set -g performance -d 2.5GHz -u 2.5GHz` (latency tail noise 제거).

| profile | layout |
|---|---|
| **fair** (default, TCP 비교) | core0 bench-dpumesh / core1 echo-dpumesh / core2 bench-tcp+sidecar1 / core3 echo-tcp+sidecar2 / DPU ARM 8core 자유 / DPA EU×1 |
| **hw** (HW 한계 측정) | core0,4 bench-dpumesh(2core) / core1,5 echo-dpumesh(2core) / core2,3 tcp(untouched) |

K8s 객체(test-bench ns): bench-dpumesh(1c)/echo-dpumesh(1c)/bench-tcp(2c)/echo-tcp(2c),
services bench-dpumesh:9092/bench-tcp:9092/echo-tcp:9091, sidecar1-config(upstream=echo-tcp:9091)/
sidecar2-config(upstream=127.0.0.1:9092). hostPath: /dev/infiniband, $BUILD_DOCA/lib(libthrift)
→ bench/echo-dpumesh.

### 1.3 실행 인터페이스

```bash
./test-bench.sh deploy                            # build + image + DPU restart + pods Ready + fair pin
./test-bench.sh dpumesh    <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh tcp        <RPS> <DUR> <SIZE> [<CONNS>]
./test-bench.sh dpumesh-hw <RPS> <DUR> <SIZE> [<CONNS>]   # hw profile 자동 전환
./test-bench.sh pin / pin-hw / cleanup
```
Ctrl protocol(line): `RUN <rps> <dur_sec> <msg_size> [<conns>]` → `OK <rps_ach> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>`. (daemon raw 단위 µs, 표는 ms 통일.)

### 1.4 측정 방법론

- **wrk2식 scheduled-time latency (CO 보정)**: closed-loop worker는 cap region에서 coordinated
  omission 발생(가짜 p50≈p99 plateau). `t0=scheduled_time`(예약 tick), `latency=now()-t0`로
  큐잉 wait를 latency에 포착 → elbow 선명. `bench_dpumesh.c`/`bench_tcp.go` 양쪽 적용.
- **MAX_WORKERS=4096, 128 KB stack** (512 MB): concurrency cap이 낮으면(이전 512) Little's law로
  in-flight 묶여 가짜 plateau.
- **wall-time 측정**: watchdog thread + 즉시 join (고정 sleep은 wall 부풀려 RPS underreport).
- **도구**: `test-bench.sh dpumesh`(sweep) / `run_bench.sh --method 0`(M2 baseline, test_dma/bench) /
  `perf record -F 999 -g --call-graph dwarf`(flame) / `perf trace -s`(syscall) /
  `dpa-statistics collect`(EU stat) / `top -H -p $(pgrep dpumesh_dpu)`(ARM thread CPU).

### 1.5 측정 환경

| | |
|---|---|
| Host CPU | DVFS lock @ 2.5 GHz, governor=performance, cores 0-7 |
| DPU | NVIDIA BlueField-3, ARM 8core(pinning X), DPA FlexIO EU×1(dpumesh), partition EUs 0-63 |
| PCIe | Gen4 |
| DOCA SDK | 3.1.0105 · FlexIO 25.07.2812 · Kernel 5.15.0-176-generic |
| DPU 시작 | `dpumesh_dpu $DPU_PCI -l 40` (WARN+ filter) |

---

## 2. Transport Baseline — dpumesh와 동일 구조 micro-bench (≠ engine 한계, → §6)

`test_dma/bench/` micro-bench — k8s·Thrift·TCP·gateway 모두 제거. host가 dma_ring에 desc 직접
post, comch client + DPA RPC만. `host_worker.c`의 단순 루프(`while(true){ get_next_dma_desc;
desc->valid=1; }`).

### 2.0 용어 — baseline 2종 / Method 번호 (먼저 고정)

Method 번호 = `run_bench.sh --method N` = `g_bench_method`. (코드 일부 주석이 dma_copy를 "Method 2"로
부르지만 무시하고 아래로 통일.)

| Method | g_bench_method | DPA 커널 | DPU ARM | 측정 지점 | 의미 |
|---|---|---|---|---|---|
| **M0** "Only DMA" | 0 | dma_copy + per-op imm | recv 카운트만 (forward 없음) | DPU recv/s | DPA→DPU dma_copy+completion rate |
| **M1** | 1 | post_memcpy (stop-and-wait) | recv 카운트만 | DPU recv/s | 헤드라인 비교 미사용 |
| **M2** "DMA + completion" | 2 | dma_copy + per-op imm (커널은 M0와 동일) | 매 recv를 host로 `server_send_msg` forward | DPU recv/s¹ | **dpumesh와 동일 구조의 transport baseline** |

¹ M0·M2 모두 DPU recv/s(= dma_copy/s)로 측정 — 동일 관측점. M0→M2 delta = DPU ARM forward 비용(§2.1).

**baseline 2종 — 서로 다른 질문에 답한다 (둘 다 정당):**
- **Transport baseline (M2)** = dpumesh의 per-op 구조(single-EU · host descriptor-gated · dma_copy +
  per-op completion imm · `is_consumer_empty` polling)를 그대로 가진 micro-bench. 질문: *"dpumesh가
  자기 구조에서 낼 수 있는 최대 dma_copy/s는?"* → §0/§2/§4의 ceiling·vs M2·vs M0는 전부 이 기준.
  **engine 한계가 아니다.** (M2 lossy vs dpumesh lossless 차이는 §5.6 E4가 보이듯 dpumesh 문맥에선
  writeback이 싸서 dma_copy/s ceiling으로는 공정. RTT 4× 차이는 dma_copy/s = RPS×4로 정규화.)
- **Engine op-rate ceiling (§6, pure_dma)** = host descriptor 핸드셰이크를 제거하고 single EU가
  dma_copy를 back-to-back 발사한 raw op-rate(**556K**). 질문: *"이 HW single-EU의 절대 한계는?"*
  → dpumesh 309,736은 이 기준 **55.7%**.

### 2.1 Method 0 / Method 2 (8 KB, H2D, 60 s, 2026-05-30)

`-l 40` + per-msg 로그 제거, **둘 다 DPU-side recv/s(= dma_copy/s)** 측정:

| Method | 구성 | dma_copy/s | Throughput(1 dir) | vs M0 |
|---|---|---:|---:|---:|
| Method 0 (Only DMA) | `dma_copy` + per-op imm | **325,407** | 21.32 Gbps | 100% |
| Method 2 (DMA + completion) | M0 + DPU ARM이 매 completion을 host로 `server_send_msg` forward | **320,105** | 20.97 Gbps | 98.4% |

M0→M2 = **−1.6%** = DPU ARM per-completion forward가 sustainable dma_copy/s에 주는 비용. M2 =
dpumesh와 동일 per-op 구조(single-EU · host descriptor-gated · dma_copy + per-op completion +
polling)의 transport baseline(§2.0); engine op-rate은 §6(pure_dma 556K).

### 2.2 micro-bench vs dpumesh chain 차이 (caveat)

| | micro-bench (M2) | dpumesh |
|---|---|---|
| 흐름 | host→DPU→host (단방향, 1 endpoint) | host A→DPU→host B→DPU→host A (RTT, 2 endpoints) |
| DPU ARM | comch 수신 + `server_send_msg` 1회 | comp_queue enq + pod_idx 라우팅 + TX_ACK + reverse desc 작성 + tx_ring enq |
| reverse | 없음 | 매 hop reverse DMA enq |

→ **1 RTT = forward DMA×2 + reverse DMA×2 = 4 dma_copy ops.** dpumesh RPS ≈ dma_copy/s ÷ 4.

---

## 3. TCP/Envoy vs DPUmesh (rps=40000, dur=10s, size=8192B)

| | dpumesh | tcp/Envoy |
|---|---|---|
| Achieved RPS | **39,732.7** (99.3%) | **33,097.0** (82.7%) |
| OK / Fail | 400,000 / 0 | 400,000 / 0 |
| p50 | **4.53 ms** | 661.50 ms |
| p99 | **8.35 ms** | 3,885.30 ms |
| p999 | 8.62 ms | 4,588.43 ms |
| Throughput(RTT) | 620.82 MB/s | 517.14 MB/s |

40K target에서 dpumesh는 healthy(52K sustainable의 76%), TCP/Envoy는 deep overload(achieved
33.1K, p99 3,885 ms). TCP saturation은 40K 한참 아래.

---

## 4. dpumesh 최적화 — 누적 ceiling

8 KB·10 s·conns=auto. 각 단계 = 데이터 + 핵심. 누적 요약(§4.9)이 spine.

### 4.1 Baseline sweep (in-place forwarding 전)

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

- sustainable ≈ **52K**(p50 5.5/p99 12 ms), 50K까지 거의 linear. elbow 52K→55K(p99 12→190ms,16×).
- overload throughput ceiling ≈ **53.5K** (60/65K 밀어도 53.7K 고정, latency만 발산).
- **0 failure** 전 구간(`WAIT_TIMEOUT_MS=5000` 미발동, 4-layer backpressure 흡수). plateau 836-841 MB/s.

ceiling 위치:

| | dma_copy/s | vs M0 | vs M2 |
|---|---:|---:|---:|
| Only DMA (M0) | 325,407 | 100% | — |
| DMA + completion (M2) | 320,105 | 98.4% | 100% |
| dpumesh @ overload (53.55K) | 214,212 | 66% | 67% |
| dpumesh @ sustainable (51.64K) | 206,580 | 63% | 65% |

M0·M2는 host descriptor-gated dma_copy/s ceiling(§2.0); single-EU engine op-rate은 556K(§6).

### 4.2 In-place forwarding (2026-05-08)

`process_forward_entry`의 8 KB staging memcpy 제거. desc가 source mmap handle+VA 운반 → DPA
reverse handler가 원 sender 버퍼에서 직접 읽음. **slot 점유 forward-only → full RTT** (host TX slot도
reverse 완료 TX_ACK 후 release; 52K@5.5ms = ~290 in-flight ⊂ DMA_RING_SIZE=2048). trade-off:
same-source multi-dst HOL blocking (echo src==dst 무영향).

| Target | Base ach | New ach | Base p99 | New p99 | Δp99 |
|---:|---:|---:|---:|---:|---:|
| 5,000 | 4,968.6 | 4,968.8 | 1.93 | 1.92 | -0.5% |
| 10,000 | 9,939.2 | 9,935.9 | 2.82 | 2.78 | -1.4% |
| 30,000 | 29,807.8 | 29,800.9 | 6.51 | 6.38 | -2.0% |
| 50,000 | 49,664.7 | 49,655.3 | 11.89 | 9.94 | -16.4% |
| 52,000 | 51,644.8 | 51,625.3 | 12.16 | 12.12 | -0.3% |
| **53,000** | — | **52,641.9** | — | **12.23** | new sustainable |
| **54,000** | — | **53,636.3** | — | **12.38** | new sustainable |
| 55,000 | 53,553.4 | 54,085.4 | 189.59 | 101.39 | -46.5% |
| 60,000 | 53,698.8 | 54,198.6 | 1,154.25 | 998.98 | -13.5% |
| 65,000 | 53,841.1 | 54,748.5 | 1,961.63 | 1,767.98 | -9.9% |
| 70,000 | — | 54,369.7 | — | 2,817.77 | overload |

sustainable 52K→**54K**, overload 53,553→**54,748**, overload p99 ~절반. 0 fail, 11회 sequential
slot leak 없음. → @overload 54,748 = 218,992 ops/s (67% M0 / 68% M2); @sustainable 53,636 = 214,544.

### 4.3 Feature strip (compile-time toggle → 영구 흡수)

3 toggle: `STRIP_VALIDATION`(mmap=0/size=0/range/padded>buf 제거), `STRIP_DESC_CLEAR`(3×wb→1×wb
valid=0만), `STRIP_CHUNK_LOOP`(size≤8KB single dma_copy fast path).

| Stage | 50K ach/p99 | 55K ach/p99 | **65K ach/p99** | DMA tput@65K |
|---|---:|---:|---:|---:|
| Baseline(per-batch) | 49,629/9.89 | 54,080/91.06 | **54,291/1,894** | 847 MB/s |
| +VALIDATION | 49,721/10.04 | — | 55,207/1,704 | 862 |
| +DESC_CLEAR | — | — | 56,134/1,545 | 877 |
| **+CHUNK_LOOP** | **49,659/9.36** | **54,617/12.27** | **59,195/952** | **925** |

단계 기여: 54,291 → 55,207(+916,+1.7%) → 56,134(+927,+1.7%) → 59,195(+3,061,+5.5%) = **+9.0%**.
chunking bypass가 최대 기여(loop 안 `ensure_producer_slot`+`is_consumer_empty` wait+inflight++가
1 chunk도 발동; fast path는 1회). elbow 55K가 sustainable로(p99 91→12 ms). per-dma_copy:
Baseline 4.57µs → A+B+C 4.22µs(-0.35) → (M2 3.08µs). 안정성 3× 50K: 49342.6/9381.5µs,
49353.8/9347.9, 49344.8/9329.5 (모두 250000/0).

### 4.4 Post-cleanup (dead code 제거 + lazy drain + throttle)

DMA_REQ dead chain ~150줄 제거(caller 0), chunking fallback else 제거(host `check_slot_size`로
>8KB 도달 불가 → drop guard), `handle_msgs` 매 iter→매 32 iter, `ensure_producer_slot` lazy
drain(inflight≥CAP시 -1 abort, drain 매 8 iter, CAP=1024 ≫ wake당 ~280 inflight).

| Target | Achieved | p50 | p99 | p999 | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 30K run1 | 29,812.7 | 3.19 | **5.78** | 5.93 | 465.82 | 300,000/0 |
| 30K run2 | 29,800.9 | 3.21 | 5.82 | 8.26 | 465.64 | 300,000/0 |
| 50K | 49,653.5 | 4.96 | **9.06** | 9.33 | 775.84 | 500,000/0 |
| 55K | 54,629.5 | 5.33 | **9.80** | 12.23 | 853.59 | 550,000/0 |
| 60K | 59,564.2 | 5.60 | **12.58** | 14.45 | 930.69 | 600,000/0 |
| 62K run1 | 61,536.4 | 5.92 | 12.89 | 13.12 | 961.51 | 620,000/0 |
| 62K run2 | 61,548.9 | 6.24 | 15.15 | 18.08 | 961.70 | 620,000/0 |
| 65K | 62,068.3 | **203.35** | **404.21** | 410.28 | 969.82 | 650,000/0 |

sustainable 55K→**60K**, overload 59,195→**62,068**(+4.9%). 62K×4=**248,272 ops/s ≈ M2 78%**.
0 fail 전 구간, 62K back-to-back clean.

### 4.5 `comch_dma_comp_msg` packing 28B → 16B (WQE BB 1개)

DPA→DPU 완료 메시지 7필드는 라우팅/Thrift semantic에 다 쓰여 줄일 수 없어 wire format을 packing.

| 필드 | Before→After | | 필드 | Before→After |
|---|---|---|---|---|
| type | enum 4B → uint8 1B | | src_pod_id | int32 4B → int8 1B |
| pos | 4B (유지) | | dst_pod_id | int32 4B → int8 1B |
| length | 4B (유지) | | flags | 1B (유지) |
| req_id | 4B (유지) | | 트레일 pad | 3B → 0B |

sizeof 28B→16B(-43%). 필드 재배치로 4B-aligned offset 유지(`type/flags/src/dst@0..3, pos@4,
length@8, req_id@12`) → packed 불필요, DPA unaligned penalty 없음, `_Static_assert(sizeof==16)`.
read site(`dpa.c:71-76`) `*(enum*)raw`(4B) → `raw[0]`(1B).

Quantization 가설(§5.6 E5 인버스): 64B/32B quantum이면 12→24B cost 0%여야 하나 E5 측정 -8.6%
→ 32B 이상 quantum **배제**, 16B HW quantum 확정. 28B→16B = 32B→16B quantum 1단강하 ≈ -8% 기대.

| Target | Achieved | p50 | p99 | p999 | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 30K | 29,801.4 | 3.04 | **5.53** | 6.21 | 465.65 | 300,000/0 |
| 50K | 49,653.8 | 4.70 | **8.59** | 9.23 | 775.84 | 500,000/0 |
| 60K | 59,585.6 | 5.44 | **10.04** | 12.64 | 931.03 | 600,000/0 |
| 62K | 61,545.3 | 5.51 | **12.59** | 12.79 | 961.65 | 620,000/0 |
| **65K run1** | **64,549.4** | **5.65** | **12.89** | 13.08 | **1008.58** | 650,000/0 |
| 65K run2 | 64,524.0 | 5.70 | 12.90 | 13.22 | 1008.19 | 650,000/0 |
| 66K | 65,529.0 | 12.08 | **24.51** | 26.37 | 1023.89 | 660,000/0 |
| 67K | 65,859.6 | 55.45 | **105.72** | 109.01 | 1029.06 | 670,000/0 |

sustainable 60K→**65K**(+8.3%), overload 62K→**65.5K**(+5.6%), **1 GB/s 돌파**@65K, 50-60K p99
5-20% 개선(60K -20.2%). 65,859×4=**263,436 ops/s(82% M2)**. E5 인버스 검증 정합(예측+8%, 실측 +5.6%
tput/-20% p99/elbow +8.3%).

### 4.6 Producer slot SDK 위임 + `OPTIMIZE_REPORTS`

§4.4 lazy-drain의 `producer_slots_inflight` 카운터+CAP=1024+abort 패턴이 SDK의
`DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS`(완료 batch 통보)와 충돌(완료 deferred → ack 0 →
카운터 미감소 → CAP → abort 연쇄 → host TX stuck, 실측 30K OK 243/Fail 900). 카운터 제거(_pad1로
교체), `ensure_producer_slot`+호출 4건 제거, drain은 ack만 유지, 두 dma_copy에 `FLUSH|OPTIMIZE_REPORTS`.
M2 baseline은 카운터 없이 SDK backpressure에 위임. 안전: wake당 burst ~280 ≪ SDK 큐 1024,
`DRAIN_COMPLETIONS_EVERY=8`.

| Target | Achieved | p50 | p99 | p999 | Throughput | OK/Fail |
|---:|---:|---:|---:|---:|---:|---|
| 30K | 29,783.2 | 2.94 | **5.38** | 6.70 | 465.36 | 300,000/0 |
| 50K | 49,662.8 | 4.58 | **8.35** | 8.61 | 775.98 | 500,000/0 |
| 60K | 59,554.3 | 5.35 | **9.84** | 12.49 | 930.54 | 600,000/0 |
| 65K | 64,523.0 | 5.59 | **12.84** | 13.08 | 1008.17 | 650,000/0 |
| **67K** | **66,533.4** | **5.67** | **13.00** | 13.20 | **1039.58** | 670,000/0 |
| **68K** | **67,497.0** | **6.45** | **13.72** | 15.21 | **1054.64** | 680,000/0 |
| 70K | 68,126.8 | 119.49 | **248.78** | 253.66 | 1064.48 | 700,000/0 |

Back-to-back(5,420,000 RTT × 9 run, 편차 ach<0.05%/p99<1%, 0 fail×9):

| Target | Run1/p99 | Run2/p99 | Run3/p99 |
|---:|---:|---:|---:|
| 60K | 59,553.5/9.61 | 59,553.9/9.82 | — |
| 65K | 64,523.0/12.85 | 64,513.8/12.83 | — |
| 67K | 66,533.4/13.00 | 66,508.9/13.01 | 66,505.5/13.04 |
| 68K | 67,497.0/13.72 | 67,488.3/13.63 | — |

sustainable 65K→**68K**, overload 65,859→**68,127**(+3.4%), tput 1,029→1,065 MB/s. 67K p99 §4.5
105ms(overload)→13.0ms(sustainable). 68,127×4=**272,508 ops/s(85% M2)**. 단독(카운터 제거만)
+1.5%, +OPTIMIZE_REPORTS 시너지로 +3.4% (단독 OPTIMIZE_REPORTS는 CAP 패턴과 incompatible).

### 4.7 DPA poll/writeback fence 일괄화 + ring batch drain (→ M2 96.8%)

DPA EU가 cap이므로 EU의 per-desc 비용을 3단계로 절감(host/DPA 자료구조 변경 없음):
**(a) read_inv hoist** — window-wide read fence를 inner iter당 4회→1회. **(b) ring batch drain** —
`process_one_desc`/`process_one_rev_desc`→`process_fwd_ring`/`process_rev_ring`, ring당
`RING_BATCH_CAP=32` 연속 desc를 1호출에 처리(메타데이터 1회 로드, 중복 consumer-wait 통합).
**(c) writeback batch** — per-desc writeback 제거, inner iter당 1회(found>0). 각 desc가 64B
독점이라 일괄 flush 안전.

| Target | §4.6 | (a)read_inv | (b)batch drain | (c)wb batch |
|---:|---:|---:|---:|---:|
| 30K p99 | 5.38 | 5.33 | 5.28 | **5.19** |
| 50K p99 | 8.35 | 8.31 | 8.04 | **7.97** |
| 65K p99 | 12.84 | 12.78 | **9.98** | 9.83 |
| 73K p99 | overload | overload | 13.50 | **13.50** |
| **74K p99** | overload | overload | 23.22 | **13.57** |
| 75K p99 | overload | overload | overload | 15.83 |
| Sustainable elbow | 68K | 69K | 73K | **74K** |
| Overload ceiling(RPS) | 68,127 | 68,819 | 74,702 | **77,434** |

단계 overload: 68,127→(a)68,819(+1.0%)→(b)74,702(**+8.5%**)→(c)77,434(**+3.7%**). (b)가 최대(65K
p99 12.78→9.98,-22%). Back-to-back(74K×4 + 73K×2 + 30K recovery): 74K 73,422-73,453/p99
13.56·13.60·13.59·13.585; 73K 72,447·72,455/13.50·13.49; 30K 29,803/5.19. 편차 ach<0.05%/p99<1%,
0 fail.

**Milestone**: 77,434×4 = **309,736 dma_copy/s = M2 320,105의 96.8%** (1,186 MB/s overload).
transport baseline M2(§2.0)에 근접 — single-EU single-ring-chain의 자연 종착점(engine op-rate 556K
대비 55.7%, §6).

### 4.8 desc 32B pack 시도 — 실패

§4.7(b)의 read 절감을 더 밀어 `dma_desc` 64B→32B(2 desc가 한 cache line 공유) 시도 → **30K에서
1992 OK / 900 Fail / 132 achieved 붕괴**. 원인: `__dpa_thread_window_writeback()`는 cache-line
단위 RMW. 32B 2개가 한 line 공유 시 DPA가 desc[0].valid=0 flush할 때 stale desc[1](valid=1)을
host로 되써서 clobber → slot stuck → timeout (§5.6 E4 storm과 동근). **64B-per-desc는 padding이
아니라 writeback 격리를 위한 load-bearing 제약**.

### 4.9 누적 ceiling 변화

| Phase | Sustainable elbow(8KB,p99≤~14ms) | Overload throughput | DMA ops/s | vs M2 | vs M0 |
|---|---:|---:|---:|---:|---:|
| §4.1 baseline | 52K(12ms) | 53,841@65K | 215,364 | 67% | 66% |
| §4.2 in-place | 54K(12.4) | 54,748@65K | 218,992 | 68% | 67% |
| §4.3 strips | 55K(12.3) | 59,195@65K | 236,780 | 74% | 73% |
| §4.4 cleanup | 60K(12.6) | 62,068@65K | 248,272 | 78% | 76% |
| §4.5 packing | 65K(12.9) | 65,859@67K | 263,436 | 82% | 81% |
| §4.6 SDK 위임 | 68K(13.7) | 68,127@70K | 272,508 | 85% | 84% |
| **§4.7 fence/batch** | **74K(13.6)** | **77,434@78K** | **309,736** | **96.8%** | **95.2%** |

throughput milestone: §4.5에서 1 GB/s 돌파(65K@1,008 MB/s), §4.7에서 1,186 MB/s(overload). (위
"vs M2/vs M0"는 transport baseline(§2.0) 기준; engine op-rate 556K 대비는 55.7% — §6.)

---

## 5. Cap Attribution — 왜 dpumesh < M2였나

§4 baseline 대비 dpumesh 31% gap의 원인을 직접 측정으로 검증/반증. **결론: cap = single DPA EU의
per-dma_copy work** (ARM/epoll/PCIe-BW/polling은 cap 아님).

### 5.1 DPA EU stat — ARM 99.9%인데 EU 99.5% idle (50K, 10s)

`dpa-statistics collect -d mlx5_0 -t 10000`:

| 항목 | 값 | 해석 |
|---|---:|---|
| Wall time | 10,000 ms | 측정창 |
| EU active time | **51.5 ms** | 실제 실행 |
| EU active % | **0.51%** | 99.49% idle |
| Cycles | 92.7 G | 51.5ms × ~1.8GHz 일치 |
| Instructions | 7.22 G | IPC 0.078 (memory/DMA stall, 정상) |
| Executions | 7,314 (731/s) | DPA thread wake 횟수 |
| Cycles/execution | 12.7 M | wake당 ~7µs |
| dma_copy/execution | ~273 | wake당 batch |

DPA는 burst 처리 — wake → 273 dma_copy 일괄 → re-schedule.

### 5.2 ARM perf self-time (`perf report --no-children`, 50K)

| 카테고리 | dpumesh | M0 | M2 |
|---|---:|---:|---:|
| epoll_pwait 군집(kernel+libc+vdso) | **64%** | 65% | 62% |
| DOCA infra(CQ poll, comch) | 7% | 13% | 19% |
| Atomics(CAS/swap/mutex) | 4% | 2% | 3% |
| App 코드(run_dpu_worker/process_*/drain_*) | **5.5%** | 3.4% | 2.8% |

per-op time: M0 3.13µs, M2 3.22µs(baseline), **dpumesh 4.67µs (+1.45µs, +45%)**. 세 측정 모두
syscall 1순위지만 dpumesh만 throughput 낮음 → ARM 폴링 자체로는 설명 불가.

### 5.3 ARM CPU 분포 (flame `dpumesh_dpu_flame_per_batch.svg`)

| ARM 함수 | self% | per-event |
|---|---:|---:|
| `doca_pe_progress`(epoll 포함) | 76.5% | 3.48µs |
| `process_completion_queue` | 7.95% | 0.36µs |
| `process_rev_notify_entry` | 5.18% | 0.24µs |
| `find_pod_by_id` | 2.80% | 0.13µs |
| `server_send_msg_to_conn` | 2.20% | 0.10µs |
| comp_queue ops(peek/empty/full/dq/eq) | ~3.5% | 0.16µs |
| `process_forward_entry` | 0.91% | 0.04µs |
| `drain_deferred_tx_acks` | 0.54% | 0.02µs |
| **ARM useful work 합** | **~23%** | **~1.05µs/event** |

ARM·DPA EU는 병렬 processor. cap = max(DPA per-op 4.55µs, ARM per-event 1.05µs) → **DPA가 cap,
ARM ~3.5µs 헤드룸**. ARM 최적화는 throughput 무영향.

### 5.4 DPA polling counter — 결정적 측정 (50K, 15s)

DPA kernel에 `stat_inner_iters/stat_polls/stat_dma_copies` 카운터 추가, ARM이 `doca_dpa_d2h_memcpy`로
1초마다 읽음:
```
iters=50858 polls=203428 copies=200052  poll/copy=1.017
iters=50799 polls=203196 copies=200033  poll/copy=1.016
iters=50816 polls=203268 copies=200000  poll/copy=1.016
iters=50910 polls=203636 copies=200012  poll/copy=1.018
iters=50876 polls=203504 copies=199999  poll/copy=1.018
```
inner iter ~50,800/s, iter당 4 polls(forward×2 + reverse×2 pods)→203K polls/s, iter당 ~4
dma_copies→200K/s. **poll/copy=1.017 ≈ M2 1.000** → 4 rings 폴링이 4 dma_copies로 perfectly
amortize. **polling은 갭 원인 아님(가설 B 확정).** (카운터는 측정 후 제거.)

### 5.5 정정된 cap 모델

| | M2 baseline | dpumesh | Δ |
|---|---:|---:|---:|
| dma_copy/s | 325K (또는 321K) | 220K | -105K |
| Per-dma_copy 시간 | 3.08µs | 4.55µs | +1.47µs |
| PCIe polls/dma_copy | ~1.0 | 1.017(측정) | 0 |

+1.47µs/dma_copy = DPA EU 추가 코드 작업. M2 `poll_desc_ring_dma_copy`에 없는 dpumesh
`process_one_desc` 작업 추정:

| 항목 | 추정 EU 비용 |
|---|---:|
| validation(mmap=0/size=0/range) | ~0.1-0.2µs |
| chunking loop(while offset<total, ALIGN_UP_128, chunk type) | ~0.2-0.3µs |
| desc 필드 clear + writeback ×3 | ~0.3-0.5µs |
| comp_msg 필드 3→6 | ~0.1µs |
| `ensure_producer_slot` per chunk + counter | ~0.2µs |
| reverse rings 분기 / `dpa_sent_count`++ | ~0.1µs |
| **합** | **~1.0-1.4µs** (실측 갭 1.47µs와 정합) |

### 5.6 M2에 feature 추가 (Inverse, 8 KB, Method 0, H2D, 10s)

| Stage | recv/s | Bandwidth | Δ vs prev |
|---|---:|---:|---:|
| **E0 baseline** | **321,803** | 21.08 Gbps | — |
| E1 +ADD_VALIDATION | 324,707 | 21.27 | +0.9%(noise) |
| E2 +ADD_CHUNK_LOOP | 319,495 | 20.93 | **-1.6%** |
| E3 +ADD_PRODUCER_SLOT | 303,173 | 19.86 | **-5.1%** |
| E4 +ADD_DESC_CLEAR | **17,639** | 1.15 | **-94.2%** ❗ |

E4 collapse 격리: E4-alone(DESC_CLEAR only) 17,510, E4-min(valid=0 1줄+wb 1번) 17,225 →
**writeback 자체가 catastrophic.** 구조적 발견 — **desc clear의 100× 비대칭**: M2 ring은
**lossy**(host가 valid 안 보고 blind write), dpumesh는 **lossless**(host가 valid==1이면 NULL,
DPA가 valid=0 clear까지 대기). DPA writeback이 host의 같은 cache line에 PCIe write하는데, M2
host는 동시에 같은 line write(addr/size/valid=1) → **양쪽 동시 쓰기 = PCIe cache coherency 폭증.**
dpumesh는 host가 valid=0 대기 → 충돌 없음 → 같은 writeback cheap.

추가 feature (E5-E10, polling amortization sanity):

| Variant | recv/s | Δ vs E0 | per-iter | 비고 |
|---|---:|---:|---:|---|
| **E0** | **321,803** | 0 | 3.11µs | 1 poll:1 op |
| E5 +LARGER_COMP_MSG | 294,207 | -8.6% | 3.40µs(+0.29) | 3→7 필드(12B→24B imm) |
| E6 +HANDLE_MSGS | 315,513 | -2.0% | 3.17µs(+0.06) | `consumer_get_completion` 1×/iter |
| E7 +EXTRA_CONSUMER_WAIT | 322,857 | +0.3%(noise) | 3.10µs | 사실상 free |
| E8 +REVERSE_DMA | 381,826 | +18.6% | 5.24µs/2op | 1 poll:2 op ← amortize |
| E9 +4_DMA_COPIES | 417,119 | +29.6% | 9.62µs/4op | 1 poll:4 op ← 더 amortize |
| E10 +4_DMA_COPIES+EXTRA_RING_POLLS=3 | 338,322 | +5.1% | 11.83µs/4op | 4 poll:4 op ← dpumesh 패턴 |

E8/E9 상승은 ceiling 아니라 polling amortization. `per_iter = α + N×β`: 3.11=α+β(E0),
5.24=α+2β(E8), 9.62=α+4β(E9) ⇒ **β≈2.13µs/dma_copy(marginal), α≈0.98µs(per-iter fixed)**. E10(4
poll:4 op = dpumesh) 338,322 = E0와 +5% → dpumesh 1:1 패턴에선 amortization 이득 거의 없음,
**E0를 정직한 baseline으로 사용.**

### 5.7 추가 sanity checks

**E11 — PCIe direction balance**: E10(3H+1D+4polls) 338,322/2.96µs vs E11(2H+2D+4polls)
337,262/2.97µs, Δ-0.3%(noise) → PCIe full-duplex라 forward+reverse BW lane 공유 안 함, **"PCIe BW
contention" 부정.**

**Inner pe_progress per-entry→per-batch**:

| Target | Baseline ach/p99 | Per-batch ach/p99 | Δach | Δp99 |
|---:|---:|---:|---:|---:|
| 50K | 49,655/9.94 | 49,629/9.89 | -0.05% | -0.5% |
| 55K | 54,085/101.39 | 54,080/91.06 | -0.01% | **-10.2%** |
| 65K | 54,749/1,768 | 54,291/1,894 | -0.8% | +7.1% |

sustainable RPS 무변화. flame: `doca_pe_progress` 67.5%→76.5%, `__GI_epoll_pwait` 46.0%→54.5%,
`process_completion_queue` 19.8%→7.95%. **ARM polling 횟수 줄여도 polling 시간 비중 안 줄어듦 →
ARM cap 아님.**

**epoll_pwait per-call (perf trace, 50K, 15s)**: `epoll_pwait 3,122,730 calls / 6,703.520 ms total
/ 0.002 avg / 0.001 min / 0.109 max`. 208K calls/s, **호출당 2µs(syscall 고정비용)**, 총/wall
44.7%. M2도 같은 41% epoll 비용에 310K 도달 → **epoll overhead 자체는 갭 원인 아님.**

**Load generator 검증**: 65K×650 54,291/1,894, 65K×1000 54,199/1,892, 100K×1500 54,227/6,814,
65K×4096 **hang**(TX slot pool 2048 충돌). 650→1500 워커에도 ach 54.2K fix → **system ceiling 확정.**

**M2 synthetic multi-ring polling (EXTRA_RING_POLLS, 매 dma_copy마다 N개 추가 desc->valid read)**:

| EXTRA_RING_POLLS | 총 rings | recv/s | Bandwidth | Δ vs N=0 |
|---:|---:|---:|---:|---:|
| 0 | 1 | **325,361** | 21.32 Gbps | — |
| 1 | 2 | 236,660 | 15.50 | -27% |
| 3 | 4 | 175,421 | 11.49 | -46% |
| 7 | 8 | 114,543 | 7.50 | -65% |

monotonic, per-extra-poll ~0.8-1.1µs/dma_copy. dpumesh 220K가 M2-N=3(175K)보다 높은 이유 —
synthetic은 매 dma_copy마다 N reads 무조건, dpumesh `drain_all_rings`은 한 iter에서 4 rings 폴링 +
0~4 dma_copy → saturation 시 amortize (§5.4 ratio 1.017 일치).

### 5.8 E0(321,803) 기준 attribution

cleanup 이전 dpumesh-vs-E0 갭 = **1.44µs/op = 32%**.

| 항목 | 비용(µs/op) | dma_copy/s loss | 출처 | 회복 |
|---|---:|---:|---|---|
| Larger comp_msg(7필드,28B) | 0.29 | ~27K | E5 | §4.5 packing 16B 부분회복 |
| Per-call `ensure_producer_slot` | 0.17 | ~17K | E3 | §4.4 lazy + §4.6 SDK 위임 |
| `handle_msgs` per iter | 0.06 | ~6K | E6 | §4.4 throttle |
| Chunking loop wrapper | 0.05 | ~5K | E2 | §4.3 CHUNK_LOOP |
| Desc clear(lossless context) | 0.05 | ~5K | §4.3 STRIP_DESC_CLEAR | 회복 |
| Validation | ~0 | 0 | E1(noise) | §4.3 STRIP_VALIDATION |
| Extra consumer empty wait | ~0 | 0 | E7(noise) | — |
| **측정 합(E1-E7+§4.3)** | **0.62** | **~60K(59%)** | direct | |
| **Yield/wake architecture** | **0.55** | — | §5.9 | architectural(보존) |
| Cache footprint + amortization 불완전 | ~0.27 | — | 잔여 | — |
| **합 = 실측 갭** | **1.44** | **~102K** | 82% attributed | |

실측 회복: §4.4 +11.5K(248 vs 236), §4.5 +15.2K(263 vs 248), §4.6 +9.1K(272 vs 263), §4.7
+37.2K(309.7 vs 272.5). 누적 **+72.9K/106K = 69%**.

### 5.9 Yield/Wake architecture cost

M2 DPA kernel에 wake 메커니즘 이식해 직접 측정.

- **1단계 — naive yield(wake source 없음)**: `thread_reschedule()` 추가(273 dma_copy마다) → **273
  recv/s**(1 burst 후 영원히 hang). `doca_dpa_dev_thread_reschedule()`는 진짜 yield(block형 suspend),
  외부 wake 없으면 못 깸.
- **2단계 — yield + 1 kHz wake**(DPU keepalive 이식):

| Variant | recv/s | per-op | Δ vs E0 |
|---|---:|---:|---:|
| E0(busy-spin) | 321,803 | 3.11µs | — |
| **+yield+1kHz wake(burst 273)** | **272,928** | **3.66µs** | **+0.55µs=-15.2%** |
| +yield+1kHz wake(burst 1000) | 249,916 | 4.00µs | +0.89µs=-22.3% |

비용 정체: burst 273(=849µs) vs wake interval 1ms(=1000µs) mismatch. burst 1000(=3110µs)은 더
악화. dpumesh는 host trigger도 함께 사용(`dpumesh_enqueue`가 DPU trigger), 1 kHz는 idle fallback.
**발견**: function-local `static yield_counter`는 thread re-entry 시 reset(처음 999 recv/s) → file-scope
global + entry reset 필요.

### 5.10 Busy-spin은 architectural necessity (yield 제거 불가)

**strips 전**:

| Target | per-batch ach/p99 | busy-spin ach/p99 |
|---:|---:|---:|
| 50K | 49,629/9.89 | 49,605/9.54 |
| 55K | 54,080/91.06 | **53,562/224.4** |
| 65K | 54,291/1,894 | 53,398/2,064 |

55K p99 91→224ms(2.5× 악화). 4 rings busy-spin = PCIe poll BW가 DMA traffic과 경쟁, yield가 PCIe
polling rate throttle 역할.

**strips 후** (단일 run은 개선):

| Target | yield 유지 | busy-spin |
|---|---:|---:|
| 50K | 49,659/9.36 | 49,614/8.67 |
| 55K | 54,617/12.27 | 54,628/9.61 |
| 65K | 59,195/952 | 59,784/819 |

표면상 모든 p99 개선(50K -7%, 55K -22%, 65K -14%). **그러나 back-to-back 붕괴**:
```
run1(55K) Achieved 0.0 RPS, OK/Fail 0/1650
run2/3 (empty — daemon 무응답)
[WRN][ring.c:96][get_next_dma_desc] DMA ring busy at head=160 (size=2048) ... 무한 반복
```
**Slot leak — host ring head=160 slot이 valid==1 stuck.** recovery 로직 0(timeout/force-clear/
head++ skip/reset/backoff escalate 전무, "DPA reliable" 가정). leak 메커니즘(logical): DPA
`process_one_desc`는 모든 종료 path에서 valid=0+wb라 logic만으론 leak 불가 → busy-spin의 PCIe+
coherency 부담으로 **DPA window cache가 host의 valid=1 write를 stale 읽음 → desc_idx 안 advance →
slot 영원히 skip**(유일 plausible path).

**결론**: dpumesh가 multi-pod/multi-ring/lossless인 한 yield 불가피(multi-ring busy-spin = PCIe+
coherency stress = valid bit stuck; lossless ring이 stuck을 hang으로 표면화, M2 lossy면 덮어씀).
yield의 ~0.55µs/op는 이 보호의 가격. **(yield + 모든 §4 최적화)이 production 최적점.**

### 5.11 Flow control audit

7 layer + 3 retry queue:

| # | Layer | 위치 | 크기 | 필수성 |
|---|---|---|---:|---|
| 1 | DPA producer slot | DPA EU(SDK 관리, §4.6 위임) | 1024 | 필수 |
| 2 | DPA consumer recv | DPA EU `is_consumer_empty` spin | 8192 | 필수 |
| 3 | DPU comp_queue | DPU ARM(BP_HIGH 3072/BP_LOW 2048 hysteresis) | 4096 | 필수 |
| 4 | DPU send pool mirror | `send_tasks_in_flight` atomic | 8192 | DOCA AGAIN 회피 |
| 5 | DPU recv pool mirror | `recv_tasks_in_flight` atomic | 8192 | DOCA AGAIN 회피 |
| 6 | Host TX slot | `get_next_dma_desc` NULL on valid==1 | 2048 | 필수 |
| 7 | DPA reverse admission | `dpa_sent_count - dpa_cached_freed < rq_depth` | 2048 | reverse 필수 |

Retry queue: `deferred_tx_acks`(16384), `deferred_recv`(1024), `consumer_retry`(256). 3큐 통합
**비추천**: entry shape 비대칭(tx_acks 20B struct vs 8B 포인터 → union 24KB 낭비), lock 비대칭
(consumer_retry만 mutex → 통합 시 무락 큐가 락 코스트 + hot path mutex regression), drain timing
상이(tx_acks=send-pool 자유화 직후, recv=comp_queue depth, consumer_retry=다음 메인루프). 추가
단순화 후보(영향 미미): (a) `send_tasks_in_flight` mirror 제거 ~0.1µs/op, (b) `is_consumer_empty`
backoff. **flow control은 correctness 잘 설계됨, throughput cap 주원인 아님(§5의 DPA work가 큼).**

### 5.12 Single-pod / idle ring 효과 (분석적 추정)

4 rings 다 active = ratio 1.017(perfect amortize). single-pod self-loop(2 active+2 idle) = ratio
2.0 → 추가 1 poll/op = +0.19µs/op → 4.55→4.74µs/op → ~211K dma_copy/s(현재 248K의 -15%). ring
개수 자체는 작은 영향, idle ring이 폴링 낭비 만들 때만 중요. 현재 echo bench는 4 rings 모두 active.

### 5.13 부정된 가설

- ❌ PCIe BW contention (§5.7 E11 -0.3% noise)
- ❌ ARM polling이 cap (§5.7 per-batch RPS flat; §5.4 ratio 1.017)
- ❌ DPA busy-spin이 도움 (§5.10 latency 2.5× 악화 / slot leak)
- ❌ comp_msg quantum ≥ 32B (§4.5 16B HW WQE BB 확정)
- ❌ desc 32B pack로 PCIe read 절감 (§4.8 line-granular writeback이 이웃 clobber)
- ❌ (§6) Method 0/2 = HW max — **host descriptor feed에 묶인 값이었음**

---

## 6. Pure DMA-engine ceiling — baseline가 host-bound였음 (2026-05-29)

### 6.1 동기 & 측정 코드

§2의 Method 0(320K)/Method 2(310K)가 DMA engine 한계인지, host descriptor feed에 묶인 값인지
미검증. Method 0/2 커널은 매 dma_copy를 host descriptor에 게이트한다
(`poll_desc_ring_dma_copy`의 `while(!desc->valid) read_inv;`) → host single-thread posting rate가
측정값에 섞임. host를 루프에서 제거해 "single EU가 dma_copy를 초당 몇 번 발사하나"를 직접 측정.

코드: `test_dma/pure_dma/` + `run_pure_dma.sh` (기존 `test_dma/bench/`·`run_bench.sh` 미수정, 동일
`$SRC_DIR`에 install 후 복원). baseline(`bench/`) 대비 **변경 5파일 / 동일 4파일**:

| 파일 | 변경 |
|---|---|
| `device/dpa_kernel.c` | **재작성**. descriptor ring 안 봄 — 고정 주소창(`dpu_pos`/`host_pos`가 1MB 버퍼 회전)에 `producer_dma_copy` back-to-back. size=`PURE_MSG_SIZE`, 게이트 `is_consumer_empty`만, producer 완료 256 op마다 drain+ack. multi-EU용 `is_consumer_empty` spin backoff(`PURE_BACKOFF_SPINS`). |
| `dpa_common.h` | `dpa_thread_arg`에 `uint64_t host_addr` 추가(descriptor 없어 host base 직접 전달). |
| `dpa.c` | `.host_addr=remote_addr`; `g_pure_thread_idx/n`로 1MB 버퍼를 EU별 128B-aligned slice 분할. |
| `host_worker.c` | posting loop → idle(comch PE만 유지, post 안 함). ring/buffer/mmap export 셋업 동일. |
| `dpu_worker.c` | 공유 DPA 1개 위에 N개 thread + N개 독립 comch 채널(`PURE_NUM_THREADS`). 모든 DPU consumer를 같은 `consumer_pe`에 연결 → 한 pe_progress가 전부 drain, `recv_msg_cnt`가 EU 합산. thread launch 후 **kick(`send_dma_request_to_dpa`) per channel** 필수. |
| `comch_consumer.c` | teardown recv-error 로그 mute. |
| `object.h`/`dpa.h`/`comch_client.c` | byte-identical. |

**구조 차이(한 줄)**: 데이터 경로(host→DPU buffer, dma_copy + 완료 imm)는 동일. baseline은 매 copy를
`desc->valid` PCIe 악수로 host에 동기화(= host posting rate에 묶임), pure_dma는 그 악수 제거하고
EU가 back-to-back 발사. 이 한 가지가 320K→556K의 전부.

**NO_DATA 버그(fix됨)**: `doca_dpa_thread_run`은 thread를 arm만 하고, handler는 consumer에 메시지
도착 시 첫 실행(§5.9 "DPA는 external wake 필요"). pure 커널은 무한 no-yield loop라 kick 1회면 영원히
돎. kick(`send_dma_request_to_dpa`) 누락이 원인 → 채널별 kick 복원.

### 6.2 Single-EU 측정 (8 KB / 128 B, H2D, dma_copy, 10s)

recv/s = dma_copy/s:

| size | dma_copy/s | payload BW | status |
|---:|---:|---:|---|
| 8 KB(8192 B) | **555,383** | 36.39 Gbps | OK |
| 128 B | **556,149** | 0.56 Gbps | OK |

8 KB run DPU `recv:` raw 샘플(15s steady): `556045, 556164, 556161, 556160, 556156, 556154,
556149, 556151, 556150, 556147, 556146, 556146, 556149, 556150, 556150`; teardown 1회성 drop
`20693`(host가 DPA보다 먼저 종료 → 사라진 host 버퍼 read 실패; `comch_consumer.c:273` IO_FAILED 256건
= in-flight recv pool 1회 drain, 측정 무영향. fix: teardown 순서 `stop_dpu; stop_host` + 로그 mute).

**해석**:
- **op-rate bound (size-independent)**: 128B와 8KB가 동일 ~556K/s. 8KB의 36.4 Gbps=4.55 GB/s로
  PCIe Gen4(~32 GB/s) 한참 미달 → 대역폭 아니라 **copy 1번당 고정비용(WQE 발행 + 완료 통보)**이 천장.
- **baseline은 host-feed bound**: pure 556K = **M0(325K)의 1.71×, M2(320K)의 1.74×**. host
  descriptor 게이트(per-op `desc->valid` PCIe 악수) 제거 시 천장 1.7×+ → §2 baseline(M0/M2)은 engine
  한계가 아니라 host descriptor 핸드셰이크에 묶인 값.
- **transport baseline vs engine**: dpumesh 309,736은 transport baseline M2(320,105) 기준 **96.8%**,
  진짜 single-EU engine op-rate(556K) 기준 **309,736/556,150 = 55.7%** — single-EU에도 ~44% 헤드룸.
- single-EU 이론 상한: dpumesh 1 RTT=4 dma_copy → op-rate÷4 ≈ **139K RPS**. 현재 77K는 그 56%.
- caveat: 556K는 dma_copy에 필수 per-op 완료 imm 포함(= dpumesh primitive, 직접 비교 가능). 단
  comch 완료통보·DPU recv drain 경로도 per-op 포함.

### 6.3 Multi-EU 스케일링 — op-rate는 EU 수에 비례 (peak ≈ 4 EU)

N개 DPA thread(EU)가 각자 독립 dma_copy 발사. 모든 EU 합산 = DPU recv/s.

| EU 수 | dma_copy/s | scaling | efficiency | payload BW |
|---:|---:|---:|---:|---:|
| 1 | 554,789 | 1.00× | 100% | 36.35 Gbps |
| 2 | 1,070,930 | 1.93× | 96% | 70.18 Gbps |
| 4 | **1,804,363** | **3.25×** | 81% | 118.25 Gbps |
| 8 | 1,466,216 | 2.64× | (regression) | 96.08 Gbps |

N=4 DPU `recv:` raw: `1802837, 1803922, 1802533 /s`. (N=1 = 554,789 → §6.2 555,383 재현, 리팩터 검증.)

**해석**: op-rate가 EU 수에 비례(2 EU near-linear, 4 EU peak 1.8M) → §6.2의 556K는 **per-EU 발사
한계**이지 device 한계 아님(공유 comch/recv가 556K 천장이었다면 EU 늘려도 안 올랐을 것). **N=8이
N=4보다 낮음(regression)** — 포화면 plateau여야 하므로 단순 포화 아님(= 능동적 contention).

### 6.4 N=8 regression 원인 — spin-contention 가설 **기각** (8 KB, 10s)

`is_consumer_empty` busy-spin에 backoff(`PURE_BACKOFF_SPINS`) 추가해 폴링 contention 가설 검증
(DPU 코어 1개 유지, dma_copy 유지):

| threads | backoff | dma_copy/s | payload BW |
|---:|---:|---:|---:|
| 4 | 0 | 1,741,769 | 114.14 Gbps |
| 4 | 256 | 1,739,988 | 114.03 |
| 8 | 0 | 1,557,332 | 102.06 |
| 8 | 256 | 1,526,402 | 100.03 |

- N=4: backoff 무영향(1.742M≈1.740M, 거의 안 막힘 — 예상대로).
- N=8: backoff 효과 없음 — 오히려 약간 나쁨(1.557M→1.526M). regression 그대로.

→ **N=8 감소는 EU spin-contention이 아니다(가설 기각).** backoff는 EU-side 폴링만 건드리는데 효과
없음 → 한계는 EU가 손댈 수 없는 곳(DPU-side 또는 EU/DMA-HW).

### 6.5 미해결 & 다음 단계

- **regression 재해석(진단 중)**: 가장 유력 — 단일 DPU drain 스레드가 N개 consumer 컨텍스트를 하나의
  `doca_pe_progress`로 처리, N↑ 시 per-cycle 폴링 오버헤드↑로 실효 drain rate↓ → producer throttle↓
  (DPU-side, EU backoff로 못 고침). 2차 후보: EU oversubscription(8 thread가 8 distinct EU에 안
  올라감), 공유 DMA-engine/PCIe contention.
- **다음 진단(값쌈, 함수·DPU 1코어 유지)**: N=8 sustained 중 `top -H`(drain thread ~100%? → DPU-bound)
  + `dpa-statistics`(EU active% / 활성 EU 수: 낮음→DPU 기다리며 stall, 높은데 throughput 낮음→DMA
  contention, <8→oversubscription).
- **engine 진짜 multi-EU 천장**: per-op DPU completion을 없애고 DPA-side 카운터 + d2h readout으로
  세야 정확(§5.4 instrumentation 패턴). 현재 1.8M은 측정 하한.

---

## 7. 종합 결론 & 남은 lever

dpumesh chain은 single-EU single-ring-chain의 최적화를 사실상 소진(M2 96.8%, §4.7). 그러나 §6에서
그 baseline 자체가 host-bound임이 드러났고, engine은 single-EU 556K·multi-EU ≥1.8M의 헤드룸 보유.
추가 이득은 **chain 구조 변경**에서만 나옴:

| 변경 | 잠재 이득 | 비고 |
|---|---:|---|
| **Direct host→host DMA** (chain 4→2 dma_copy/RTT) | ~+100% | staging hop 제거, host A→host B 직접. DPA에 양쪽 host mmap 노출(이미 forward는 host read, reverse는 host write 하므로 신규 노출 작음). dma_copy 절반 → M2 무관 |
| **Multi-EU DPA thread** | ~+50-100%(실측 3.25× peak) | §5.1 EU 0.51% idle. single-EU yield/wake가 cap. §6.3에서 op-rate가 EU에 비례 확인. producer/consumer/comp_queue 분할 + N=8 regression(§6.4-6.5) 해결 필요 |
| Wake mechanism 재조정(host-trigger wake) | 측정 후 결정 | §5.9 + 1 kHz keepalive가 wake clock. event-driven으로 idle 비효율 제거 가능하나 burst amortization 잃을 위험 |
| §5.11(a) `send_tasks_in_flight` mirror 제거 | +0.1µs/op | DOCA capability check 필요, 효과 작음 |

dpumesh 1 RTT=4 dma_copy이므로 measured 1.8M op-rate만으로도 ≈450K RPS 이론 상한(현재 77K의 ~5.8×).
**병목은 engine이 아니라 work를 먹이고 구조화하는 것.**

### HW 한계 측정 해석 가이드 (`dpumesh-hw`)

| 결과 | 해석 | 다음 |
|---|---|---|
| RPS ≈ 60-62K (§4.4와 동일) | chain 자체가 cap | DPU `top -H`로 ARM 100% 확인 |
| 65-75K | chain 가까이, host-side lock/scheduling이 fair-mode cap | bench/echo core 더 늘려 측정 |
| > 77K | M2 baseline 오류 또는 노이즈 | sweep 재실행 |
| < 60K | multi-core thrashing, cache locality 손실 | core 줄이거나 NUMA 구분 |

보조: `mpstat -P 0-7 1`(CPU), DPU `top -H -p $(pgrep dpumesh_dpu)`(ARM), `dpa-statistics show`(EU).
추가 변수: msg_size 1KB(per-msg overhead 격리, ops cap 깨끗), bench thread 수(기본 rps/100, cap region
thrashing 가능).

---

## 8. 부록

### 8.1 dpumesh에 적용된 변경 (merge 대상, §4·§5 채택분만 영구화)

| 파일 | 변경 | 근거 |
|---|---|---|
| `comch_server.c`+`object.h` | `pods_*` mutex → `__atomic_*` publication(lock-free hot read) | hot read path 락 제거 |
| `object.h` | `comp_queue_*` helpers `always_inline` | -O2 self frame 제거 |
| `dpu_worker.c` | `process_completion_queue` per-entry `pe_progress`×2 → per-batch 1회 | §5.7 (RPS flat, 55K p99 -10.2%) |
| `device/dpa_kernel.c` | descriptor 검증(mmap=0/size=0/range/overflow) 제거 | §4.3 +VALIDATION |
| `device/dpa_kernel.c` | 4-field clear + 3×wb → valid=0 + 1×wb | §4.3 +DESC_CLEAR |
| `device/dpa_kernel.c` | size≤8KB single dma_copy fast path + >8KB drop guard | §4.3 +CHUNK_LOOP |
| `device/dpa_kernel.c` | chunking fallback else ~120줄 제거 | §4.4 dead code |
| `device/dpa_kernel.c` | `run_dma_manager` yield 유지(측정 근거 코멘트) | §5.9/§5.10 |
| `device/dpa_kernel.c` | `handle_msgs` 매 32 iter + `drain_producer_completions` 매 8 iter throttle | §4.4 |
| `device/dpa_kernel.c`+`dpa_common.h` | `producer_slots_inflight`+`ensure_producer_slot` CAP/abort 제거(→ SDK 위임, _pad1 교체) | §4.6 |
| `device/dpa_kernel.c` | 두 dma_copy에 `OPTIMIZE_REPORTS`(FLUSH OR) | §4.6 (+3.4%) |
| `dpa_common.h` | `comch_dma_comp_msg` 28B→16B(type→u8, src/dst→i8, 재배치 자연정렬) + `_Static_assert(==16)` | §4.5 (+5.6%) |
| `dpa.c` | recv handler type read `*(enum*)raw`(4B)→`raw[0]`(1B) | §4.5 동반 |
| `device/dpa_kernel.c` | `__dpa_thread_window_read_inv()` 매 desc(iter당 4회)→iter당 1회 hoist | §4.7(a) |
| `device/dpa_kernel.c` | `process_one_desc`/`process_one_rev_desc`→`process_fwd_ring`/`process_rev_ring` + `RING_BATCH_CAP=32` batch drain | §4.7(b) +8.5% |
| `device/dpa_kernel.c` | per-desc writeback 제거 → iter당 1회(found>0) | §4.7(c) |
| `device/dpa_kernel.c`+`dpa.c` | dead `hello_world` 커널 + `launch_dpa_kernel` 제거(호출 0) | §4.7 동반 |
| `dpa_common.h` | `dma_desc` 64B 유지 + cache-line 독점 가드 주석 | §4.8 실패 기록 |
| `dma.c`/`dma.h` 삭제, `dpa_kernel.c` DMA_REQ case(80줄), `dpa_common.h` `comch_dma_req_msg`+enum+union, `dpa.c` DMA_CHUNK case | dead chain(caller 0) ~150줄 | §4.4 |
| `comch_*`/`dpa.c`/`dpu_worker.c`/`ring.c` | hot path `DOCA_LOG_DBG` 제거 | `-l 40` 일관성 |

§5.4 결정적 측정용 카운터(`stat_inner_iters/stat_polls/stat_dma_copies`)와 `doca_dpa_d2h_memcpy`
로깅은 임무 완료로 제거.

### 8.2 M2 baseline (test_dma/bench) 현재 상태

| 파일 | 변경 |
|---|---|
| `device/dpa_kernel.c` | `DOCA_DPA_DEV_LOG_*` no-op; `EXTRA_RING_POLLS=0`(E0 복원); `ADD_*` 매크로 전부 OFF |
| `dpa.c` | `DOCA_LOG_*` no-op |
| `dpu_worker.c` | `DOCA_LOG_INFO/ERR/DBG` no-op (WARN만 — `recv:` 출력용) |

### 8.3 Flame graph

| 파일 | 시점 |
|---|---|
| `bench/m2_dpu_flame.svg` | M2 baseline |
| `bench/dpumesh_dpu_flame.svg` | dpumesh 초기(in-place 전) |
| `bench/dpumesh_dpu_flame_inplace.svg` | §4.2 in-place 후 |
| `bench/dpumesh_dpu_flame_optimized.svg` | comch_server lock-free 후 |
| `bench/dpumesh_dpu_flame_per_batch.svg` | §5.7 per-batch pe_progress 후 |

### 8.4 소스 인덱스

| 경로 | 내용 |
|---|---|
| `bench/bench_dpumesh.c` | A안 client daemon (init 1회, ctrl TCP 9092) |
| `bench/echo_dpumesh.c` | A안 server daemon (32 worker thread) |
| `bench/bench_tcp.go` | B안 client daemon |
| `bench/echo_tcp.go` | B안 server (Go, listen 9092) |
| `bench/Dockerfile.*` | 4 image (dpumesh: libthrift+DOCA, tcp: slim) |
| `test-bench.sh` | A/B 배포+실행 (deploy/dpumesh/tcp/pin/cleanup/logs/status) |
| `test_dma/bench/` + `run_bench.sh` | M2 micro-bench (Method 0/2) |
| `test_dma/pure_dma/` + `run_pure_dma.sh` | §6 pure DMA-engine 측정(single/multi-EU, `--threads`/`--backoff`) |

### 8.5 설계 결정 (rationale)

- **bench/echo daemon**: 매 실험 init 안 함(dpumesh 등록은 deploy 1회), 매 실험은 ctrl TCP만으로
  트리거 → ring 등록/해제를 hot path에서 제거.
- **echo 32 thread**: 단일 thread는 ~25K RPS에서 막힘(서버 큐잉 3ms+ p50), 32 thread로 dequeue 병목 해소.
- **wall-time 측정 / wrk2 CO 보정 / MAX_WORKERS 4096**: §1.4.
- **Envoy 최소 설정**: tcp_proxy filter 1 + cluster 1, admin/HTTP/tracing/stats/access log/runtime 제거.
- **2 sidecar(1개 아님)**: Istio 모델 = client-side + server-side 두 hop. socat 단일 splice는 너무
  가벼움(kernel fast-path) → 비교 부정확.
- **pinning은 taskset(CFS quota 아님)**: CFS는 시간만 제한·core 안 정함, 같은 core 공유엔 pinning 필수.
- **DPU/DPA pinning 안 함**: "DPU/DPA가 host CPU와 독립"이 dpumesh 핵심 advantage, 임의 묶기는 비교 의의 깎음.

---

## 9. Multi-EU DPA threads — 스케일링 천장 계층과 진짜 병목 (구 §9+§10 통합)

구 §9(dpumesh chain multi-EU) + 구 §10(M2 multi-EU) + 2026-06-02 직접 실험을 **하나의 결론**으로 통합·재작성. 모든 원자료(§6/구§9/구§10 표 + 오늘 5개 run)를 누락 없이 포함. 표기: **[측정]** 데이터로 직접 / **[추론]** 측정값에 산술·모델 / **[미측정]** 아직 직접 증거 없음.

---

### 결론 (하나)

**Multi-EU throughput은 `데이터평면 → 공유 DMA-engine → 단일-ARM 제어평면`의 계층적 천장에 막힌다. BF3 DPA의 DMA-engine 자체는 ~1.6M dma_copy/s까지 낸다(pure_dma N=8의 "regression"은 그 engine의 op-rate contention이지 EU 배치·drain·대역폭이 아님 — 5개 실험으로 직접 증명). 그러나 dpumesh chain은 그 1/4도 안 되는 416K dma_copy/s(=104K RPS×4, 1.37×)에서 멈춘다. 멈추는 원인은 EU 수도, EU 배치도, DMA-HW도, host 코어도 아니라 단일 ARM의 per-RTT 제어평면 작업량이다.**

따라서 다음 lever는 **EU를 더 늘리는 것이 아니라**, ① per-RTT ARM 작업을 줄이거나(host→host direct DMA: 4→2 dma_copy/RTT) ② 그 작업을 병렬화(multi-ARM)하는 것이다. 단 "단일 ARM이 정확히 포화인가"의 **직접 측정(multi-ARM 시 throughput 상승)** 한 칸은 아직 비어 있다(§6).

---

### 0. 측정 환경

- HW: BlueField-3, ARM 8-core(제어평면은 단일 스레드 사용), DPA FlexIO EU partition 0–63, PCIe Gen4, DOCA 3.1.0105.
- dma_copy = `doca_dpa_dev_comch_producer_dma_copy`(완료 immediate 동반). **DPU recv/s == dma_copy/s.**
- 구성 3종:
  - **pure_dma** — host descriptor gate 없이 EU가 고정 회전창에 dma_copy back-to-back. `run_pure_dma.sh`.
  - **bench M0/M2** — host descriptor ring 경유. M0=DPU가 카운트만, M2=M0+매 완료를 host로 forward. `run_bench.sh --method/--threads`.
  - **chain(dpumesh)** — 실제 mesh, 1 RTT = forward×2 + reverse×2 = **4 dma_copy**. `test-bench.sh dpumesh`.
- 노브: `DPUMESH_DPA_THREADS=N`(chain), `--threads N`(pure/bench). EU affinity 기본: chain=ON, **pure/bench=없음**.

---

### 1. 천장 계층 — 한 장의 그림

모든 수치는 dma_copy/s. (chain은 RPS×4로 정규화 — 정규화 후이므로 "RTT당 4번"은 이미 나눠 없앤 것.)

| 계층 | 천장 (dma_copy/s) | 근거 | 비고 |
|---|---:|---|---|
| pure single-EU 발행 | **556K** | §6.2 [측정] | size 무관(128B≈8KB) → op-rate bound |
| pure 2-EU | **1.07M** (1.93×) | §6.3 [측정] | near-linear |
| **DPA DMA-engine 공유 천장** | **~1.6–1.8M** | §6.3 + 오늘 [측정] | pure N=4 peak ≈1.7M; N=8은 contention으로 1.5–1.6M로 *하락* |
| M0(데이터평면+단일 drain) N=8 | **1.53M** | §10.2 [측정] | drain은 ≥1.5M까지 스케일 → drain은 cap 아님 |
| 단일 ARM one-way forward(M2) | **~1.0M** | §10.2 [측정] | N≥4 plateau |
| **dpumesh chain (2 활성 EU)** | **416K** (104K RPS×4) | §9.4 [측정]+[추론] | ← **실제 병목.** DMA-engine 천장의 ~26% |

읽는 법(위→아래): HW(DMA-engine)는 ~1.6M까지 가능 → drain도 1.5M까지 OK → 단일 ARM이 가벼운 forward만 하면 1.0M → **chain의 무거운 per-RTT 제어평면이 416K로 떨어뜨린다.** 천장이 내려갈수록 "왜 여기서 막히나"의 답은 더 위쪽(HW)이 아니라 **소프트웨어 제어평면**으로 이동한다.

---

### 2. 공유 DMA-engine 천장과 N=8 regression — 직접 실험으로 증명 (2026-06-02)

#### 2.1 pure_dma 스케일링 (배경, §6.2–6.3 [측정])

| EU 수 | dma_copy/s | scaling | payload BW |
|---:|---:|---:|---:|
| 1 | 554,789 | 1.00× | 36.35 Gbps |
| 2 | 1,070,930 | 1.93× | 70.18 |
| 4 | **1,804,363** | **3.25×** | 118.25 |
| 8 | **1,466,216** | **2.64× (절대 감소)** | 96.08 |

N=4 peak 후 **N=8에서 절대 감소** = 단순 포화(plateau)가 아니라 능동적 간섭. 원인은 §6.5에서 "조사 중"으로 남아 있었음(spin-contention만 §6.4에서 기각).

#### 2.2 오늘의 A/B 실험 — 후보를 하나씩 직접 제거 (run_pure_dma.sh, 같은 세션)

backoff=256, 8KB 고정. **모든 실험은 throughput만 사용 → polling-immune**(점유율/CPU% 안 씀).

| 실험 | N=4 | N=8 | Δ(N8/N4) | 판정 |
|---|---:|---:|---:|---|
| ① baseline (affinity 없음) | 1,710,039 | 1,517,996 | **−11%** | regression 재현 |
| ② affinity (thread i→EU i, 8 distinct EU) | 1,766,900 | 1,522,404 | **−14%** | 변화 없음 → **oversubscription 아님** |
| ③ positive control (8 thread 모두 EU 0) | — | **544,451** | — | ≈single-EU(556K) → **affinity API가 실제 작동함을 증명** |
| ④ fixed 128KB window (모든 N 동일 working set) | 1,734,649 | 1,581,702 | **−9%** | N=4 안 떨어짐 → **window-shrink confound 아님** |
| ⑤ op-rate vs BW (N=8, 128B vs 8KB) | — | 128B=1,558,214 / 8KB=1,594,613 | — | 64× payload 차이에도 op-rate 동일 → **bandwidth 아님, op-rate 천장** |

- ②+③ 결합이 핵심: 8개를 한 EU에 몰면(③) 544K로 붕괴 = affinity가 분명히 적용됨 → 8개를 distinct EU로 흩어도(②) N=8이 그대로 1.52M = **EU 배치(oversubscription)는 원인이 아님.**
- ④ N=4와 N=8에 동일 128KB 창을 줘도 N=4는 ~1.73M 유지, N=8은 여전히 regression → **per-EU 창 축소(1MB/N) confound도 원인 아님.**
- ⑤ op-rate가 size-independent → 막는 자원은 **DMA-engine의 per-op(WQE 발행+완료) 처리율**이지 PCIe 대역폭이 아님.

#### 2.3 §6.4/§10.6에서 이미 제거된 후보

- **spin-contention**: §6.4 backoff 0 vs 256 → N=8 1.557M vs 1.526M(오히려 약간 악화) → 기각 [측정].
- **단일 DPU drain**: §10.6/§10.2 — bench-M0가 *같은* 단일 consumer_pe drain으로 N=8 1.53M 도달, 단조 증가 → drain은 ≥1.5M까지 스케일 → 1차 원인 아님 [측정].

#### 2.4 N=8 결론 (증명됨, §6.5 "조사 중"을 대체)

**원인 = 공유 DMA-engine의 per-op 처리 천장 ~1.6M ops/s.** pure_dma의 descriptor-free EU는 각자 bench보다 ~1.7× 빠르게 발행 → N≈4에서 이미 그 공유 천장 부근까지 차오르고, EU 5–8을 더하면 DMA-HW에서 능동적 간섭이 생겨 **aggregate가 절대 감소**한다.
- **직접 증명**: oversubscription(②③)·window(④)·bandwidth(⑤)·spin(§6.4)·drain(§10.6) 전부 직접 제거 + op-rate-bound 확정.
- **[미측정] 잔여**: "그 공유 자원이 DMA-engine이다 / 간섭이다"의 *양성* 직접 증거(예: DPA-side stall-cycle 계측)는 아직 없음. occupancy(top -H/active%)는 **ARM·EU가 busy-spin(polling)이라 항상 ~100%로 saturate되어 무용** — 그래서 이 결론은 occupancy가 아닌 throughput 소거로 세움.

---

### 3. 단일 ARM 제어평면 천장 — M2 multi-EU (구 §10)

M2는 chain과 같은 단일 ARM 제어평면(단일 consumer_pe·cc_server·매 완료 host forward)을 갖되 reverse-DMA·routing·lossless RTT가 없는 **one-way** 구조. "단일 ARM + 가벼운 per-op forward"만 분리한 control.

#### 3.1 측정 (8KB, H2D, 10s) [측정]

| N | M0(forward 없음) | scaling | M2(+forward) | scaling | M2/M0 |
|---:|---:|---:|---:|---:|---:|
| 1 | 324,630 | 1.00× | 321,754 | 1.00× | 0.99 |
| 2 | 628,622 | 1.94× | 625,708 | 1.94× | 0.99 |
| 4 | 1,187,147 | 3.66× | 1,006,224 | 3.13× | 0.85 |
| 8 | **1,534,325** | **4.73×** | **1,024,805** | **3.19×(plateau)** | 0.67 |

- N=1 재현: M2 321,754 ≈ §2.1 baseline 320,105(리팩터 회귀 0).
- **N=2 near-linear 1.94×, M2≈M0** → per-op 단일-ARM forward는 N=2를 안 가둠.
- **N≥4에서 forward가 cap**: M0는 N=8까지 증가(1.53M)하나 M2는 ~1.0M plateau → **단일 ARM의 forward(server_send_msg) 처리 천장 ≈ 1.0M op/s.**

#### 3.2 chain 416K의 3.7× 분해 [측정 2칸 + 추론]

| 단계 | dma_copy/s | 직전 대비 | 무엇이 깎나 |
|---|---:|---:|---|
| raw HW+drain (M0 N=8) | 1,534,325 [측정] | — | DMA engine + 단일 drain |
| + 최소 forward (M2 N=8) | 1,024,805 [측정] | ÷1.5 | 단일 ARM이 매 op host forward |
| chain (104K RPS×4) | **416,000** [추론] | ÷2.4 | chain per-RTT 제어평면 묶음 |

**1.53M → 416K = 3.7× = 1.5×(forward) × 2.4×(chain RTT).** chain은 DMA-HW 능력의 **~27%**만 쓴다. (단 416K는 104K×4 추론, ÷1.5·÷2.4·"3.7×"는 산술 → [추론]. 상단 두 행만 CSV 직접.)

#### 3.3 §9.6e의 정정

M2가 **단일 ARM으로 625K send/s(N=2)·1.0M(N≥4)**를 실제로 침(N=2 host 도착률 625,148/s ≈ DPU 625,352/s [측정, 단 N=2만]) → **"host-bound comch send 경로가 지배 병목"(§9.6e)은 refuted.** chain은 send 천장(1.0M)의 42%(416K)에서 멈춤 → 병목은 send 자체가 아니라 **send를 포함한 per-RTT 묶음 전체**(아래 ①–⑥), op당 ARM ~2.4µs(M2 forward 1.0µs의 2.4×).

per-RTT 단일 ARM 작업(코드 확인 [측정-구조]): ① comp_queue 4 entry drain(2 FORWARD+2 REV_NOTIFY) ② routing `find_pod_by_id` ③ reverse-DMA 중재 `dpu_enqueue_reverse_dma` ×2 ④ admission 회계 `dpa_sent_count/cached_freed` ⑤ **host-bound lossless send ×4**(hop당 DMA_COMPLETION + TX_ACK) ⑥ full-RTT slot lifecycle.

---

### 4. dpumesh chain multi-EU (구 §9)

데이터평면만 N개 EU로 복제, **DPU ARM 제어평면은 단일 유지**(tx_ring 단일 producer → lock 0).

#### 4.1 N-value 스케일링 [측정] (fair, 2-pod echo, 8KB, OK/Fail 전부 0)

| target | N=1 | N=2 | N=4 | N=4 (hw, 2-core host) |
|---:|---:|---:|---:|---:|
| 78,000 | 75,922 | — | — | 77,468 |
| 85,000 | **74,704** | — | 84,352 | 84,367 |
| 95,000 | — | 94,117 | 94,249 | 94,194 |
| 105,000 | — | **104,100** | **104,185** | 104,220 |
| 115,000(과부하) | — | 102,806 | 105,695 | 104,526 |

- **2-pod 벤치는 ring→EU=`pod%N`이라 N≥2에서 활성 EU 항상 2개** → N=2≈N=4≈**104K(1.37×)**. N>2는 활성 EU 못 늘려 같은 천장.
- **fair(1-core host) ≈ hw(2-core host) ≈ 104K** → **host 코어 수 무관 → 병목은 host posting 아님** [측정].
- 각 활성 EU는 solo 76K의 ~68%(≈52K)만 사용 → EU 여유 있음에도 416K에서 멈춤.

#### 4.2 buffer 반증 [측정]

| 변경 | 결과 | 판정 |
|---|---|---|
| `DPU_COMP_QUEUE_SIZE` 4096→16384 + `RING_BATCH_CAP` 32→128 | N=2@105K → 100,064(변화 0) | depth cap 아님 |
| `CC_DPA_MAX_MSG_NUM` 1024→4096 | 배포 실패(DPA HW recv-task 한계 초과) | HW상 불가 + cap 아님 |

→ depth 계열 버퍼 전부 천장 무영향.

#### 4.3 admission 카운터 위치 회귀 [측정]

| N=1 변형 | @85K(천장) |
|---|---:|
| 원본(file-scope 전역) | 76,749 |
| 카운터 in heap struct | 71,561 (**−7% 회귀**) |
| **per-EU 2D 전역(수정본)** | 74,704 (회귀 0) |

교훈: DPA hot-path per-op 카운터는 heap struct가 아니라 file-scope 전역에.

#### 4.4 핵심 [측정+추론]

- single-EU 회복: N=1 ~76K = §4.7(77,434) 재현(리팩터 회귀 0).
- **multi-EU 실이득**: 2 활성 EU가 76K→104K(**+37%, 1.37×**). pure_dma 2-EU 1.93×보다 낮음 = 실체인엔 단일 ARM 라우팅·cc_server·reverse admission이 추가되기 때문.
- **2-EU=104K=416K dma_copy/s > M2 320K(30% 초과)** → DPU를 그대로 두고 EU만 늘려 M2를 넘김 → M2는 ARM 단독 천장이 아니라 single-EU/chain에 묶인 값이었음.

---

### 5. 종합 — 왜 chain은 416K에서 멈추나 (단일 결론 재확인)

세 구성의 같은-EU-수 비교(전부 dma_copy/s 정규화):

| | single-EU | 2-EU | N=8 |
|---|---:|---:|---:|
| pure_dma | 556K | 1.07M | 1.5–1.6M(DMA-engine 천장 부근) |
| bench M0 | 325K | 629K | 1.53M |
| bench M2(+forward) | 322K | 626K | 1.02M(단일 ARM forward 천장) |
| **chain** | ~309K | **416K** | — (2-pod=2EU cap) |

- **HW/drain은 cap 아님**: 같은 HW·device·단일 drain에서 pure 2-EU=1.07M, M0 N=8=1.53M.
- **EU 배치·창·대역폭·spin은 cap 아님**: §2의 5개 실험으로 직접 제거(이건 DMA-engine 천장 내부 얘기, chain은 그 한참 아래).
- **단일 ARM "존재"가 cap도 아님**: M2가 단일 ARM으로 1.94×(N=2)·1.0M까지 스케일.
- **그러므로 chain 416K의 원인 = 단일 ARM이 op당 하는 per-RTT 제어평면 작업의 무게**(§3.3 ①–⑥, op당 ~2.4µs). chain은 DMA-engine 능력(~1.6M)의 ~26%, 단일-ARM forward 천장(1.0M)의 ~42%만 쓴다.

→ **결론(재확인): 병목은 DMA-HW도 EU도 host도 아니라, 단일 ARM의 per-RTT 제어평면 작업량.**

---

### 6. Lever & 정직한 한계

**Lever(병목을 직접 깎음):**
1. **host→host direct DMA** — 1 RTT 4→2 dma_copy, reverse staging+중재+admission 통째 제거 → per-RTT ARM 작업 절반 이하. (단 USER L7 결정 선행.) M2가 보인 "가벼운 제어평면=near-linear scaling"을 chain으로 끌어옴.
2. **multi-ARM(DPU multicore, Phase 3)** — per-RTT 작업을 EU/shard로 병렬화. 고위험(현재 단일 ARM 설계는 lock 0; 멀티코어는 comp_queue/pod state/tx_ring/send pool에 동시성 제어 재도입 필요).
- send 배치(§9.6e 제안)는 효과 작음 — send 자체가 병목 아님(M2가 625K~1.0M send/s 입증).

**[미측정] 단 하나의 빈 칸 — "단일 ARM이 정확히 포화인가":**
§3(M2 등가: 같은 코어가 가벼운 op 1.0M = 무거운 op 416K = 동일 총작업)으로 강하게 시사되나, **chain 104K에서 ARM 코어가 실제 ~100%인지는 직접 측정 안 됨**(occupancy는 polling으로 무용 → §2.4와 동일 한계). 이 칸을 polling 없이 직접 메우는 가장 깔끔한 실험:

> **chain을 DPU ARM 여러 코어로 돌린다(또는 독립 2쌍을 2 ARM에 샤딩). throughput이 오르면 → 단일 ARM이 cap이었다 확정. 안 오르면 → ARM은 cap 아님(단, 샤딩 락이 새 병목이 아닌지 먼저 배제).** 교차 의존(pod10 forward → pod11 reverse ring)을 피하려면 pod-shard가 아니라 독립 2쌍(4-pod)으로 나눠 락 없이 측정(confound 제거).

이 실험 결과는 §3의 추론(3.7× = 1.5×forward × 2.4×chain, 둘 다 단일 ARM)에 영향을 주지 않으며, "단일 ARM 포화"의 마지막 직접 증거만 채운다.

---

### 부록 A — 원자료 CSV

| 구성 | CSV |
|---|---|
| pure single/2/4/8 (§6.2–6.3) | `test_dma/pure_dma_results_20260529_{190327,194717,194834}.csv` |
| pure backoff (§6.4) | `test_dma/pure_dma_results_20260529_200702.csv` |
| **pure baseline N4/8 (오늘 ①)** | `test_dma/pure_dma_results_20260602_115553.csv` (4=1,710,039 / 8=1,517,996) |
| **pure affinity N4/8 (오늘 ②)** | `test_dma/pure_dma_results_20260602_120037.csv` (4=1,766,900 / 8=1,522,404) |
| **pure positive control 8→EU0 (오늘 ③)** | `test_dma/pure_dma_results_20260602_120409.csv` (544,451) |
| **pure fixed-window N4/8 (오늘 ④)** | `test_dma/pure_dma_results_20260602_120638.csv` (4=1,734,649 / 8=1,581,702) |
| **pure op-rate test N8 128B/8KB (오늘 ⑤)** | `test_dma/pure_dma_results_20260602_120953.csv` (128B=1,558,214 / 8KB=1,594,613) |
| M0/M2 N=1,2 / N=4,8 (§10.2) | `test_dma/bench_results_20260602_020604.csv` / `..._021256.csv` |
| chain N-sweep (§9.4) | `test-bench.sh dpumesh` 로그(별도 harness, CSV 미보관) |

### 부록 B — 신뢰도 주석 (audit)

- §6.3(N4=1.804M/N8=1.466M)과 §6.4/오늘 run은 **별도 run**(일변동 −3.5%/+6.2%); N=8<N=4 regression은 모든 run에서 robust, 크기만 가변.
- §10.2의 N=2 HOST 도착률(625K)은 host 로그 값으로 **CSV 미보관**(run_bench.sh는 DPU 로그만 저장); N≥4 host 도착률은 미검증.
- bench `host_worker.c`는 free-running host(M2가 valid 안 지움 → 첫 sweep 후 영구 valid=1, DPA free-run). `get_next_dma_desc`가 valid==1에 NULL 반환하나 §10.2 HOST 로그가 host 생존을 입증 → latent UB지 crash 아님, DPU 숫자 무해.
- pure/bench 둘 다 EU affinity 없음(오늘 실험으로 무관 확인). chain은 affinity ON.
