# DPUmesh 멀티스레딩 — 통합 계획 (2026-06-02 재작성)

이전 판(`multithread.md`+`multithread_plan.md`+`multithread_verified_plan.md` 통합본)의 **소거법 기반
결론들을 정정**하고, 이번 세션의 정밀 재분석을 반영해 재작성한다. 핵심 질문을 **"chain이 같은 2 EU의
M2 대비 왜 낮고, DPU 코어를 더 줬는데 왜 안 늘었나"** 로 다시 맞춘다.

> **표기**: **[측정]** 데이터 직접 / **[산술]** 측정값에 산술(예: dma_copy/s = RPS×4) / **[추론]** 측정+모델
> / **[귀인]** 소거법 해석 — *직접 증거 아님, 가설로 취급* / **[정정]** 이전 판에서 틀렸던 것.
>
> **이번 세션 USER 결정 (범위)**: ① **host→host direct DMA는 보류** — lever로 고려하지 않는다(§8).
> ② **"pod 늘려 활성 EU 늘리기"는 초점 아님** — `pod%N`이라 2-pod=2 EU는 사실이나(§2), 그걸 늘리는
> 게 아니라 *가진 2 EU에서 chain이 M2에 못 미치는 것*이 문제다.

---

## 0. 한 줄 결론 (정정)

**chain 2-EU 416K dma_copy/s는 M2-N=2의 626K보다 33% 낮은데, 그 33%는 *EU가 노는 시간*이다
(416K ≈ 626K × 0.68, EU util 68%). 단일 ARM은 병목이 *아니다* — send 천장 ~1.0M 중 416K(42%)만
써서 ~2배 여유가 있다. EU가 노는 건 ARM 처리량 부족이 아니라 요청마다 EU→ARM→EU로 번갈아 의존하는
*결합 닫힌 루프의 handoff latency*를 기다리기 때문이다. DPU 코어를 더 줘도 안 늘어난 이유: ARM은
애초에 여유가 있었고(코어 추가 = idle 용량만 증가), 게다가 시도한 split 2종은 일을 균형 분할하지
못했다. → 진짜 lever는 코어·EU·버퍼가 아니라 *per-request handoff latency*이고, 무엇을 줄일지는
*per-request 구간 측정*으로 먼저 국소화한다.**

이전 판의 다음 결론은 정정됨:
- ❌ "N=8 regression = 공유 DMA-engine op-rate 천장 (PROVEN)" → **[정정] 소거법뿐, positive 증거 없음**(§3.4).
- ❌ "chain 416K = 단일 ARM per-RTT 작업 무게(op당 2.4µs)" → **[정정] ARM은 ~2배 여유, 병목 아님**(§4).
- ❌ "functional split이 ~2× + ARM=cap 검증" → **[정정] 이미 구현·측정됨, ~0% (이유 §4.4)**.
- ❌ host→host를 Lever 1로 → **[정정] USER 보류, 범위 밖(§8)**.

---

## 1. 핵심 질문 (이번 세션 재정의)

| 비교 | dma_copy/s | 단위 메모 |
|---|---:|---|
| chain 2-EU | **416,000** | = 104K RPS × 4 [측정+산술] |
| M2-N=2 (drain+forward, 같은 2 EU) | **625,708** | [측정] |
| M0-N=2 (drain만) | 628,622 | [측정] |

**핵심 항등식 [추론]**: `chain EU dma_copy rate (416K) ≈ M2 EU rate (626K) × EU util (≈68%)` →
0.68 × 626K = 426K ≈ 416K. **33% gap = EU가 기다리며 노는 시간 그 자체.** chain 내부 비교로도 같은 신호:
chain N=1 = 309K(77K×4) → N=2 = 416K = 2 EU인데 **1.35×뿐**(§4.3).

---

## 2. 두 축 & 현재 구조 (전부 코드 확인)

| 축 | 무엇 | 자원 (per-EU vs 공유) | 근거 |
|---|---|---|---|
| **EU (데이터평면)** | dma_copy 실행 | **per-EU**: `dpa_threads[k]`, `dpa_comches[k]`(1c/1p), `dpu_consumer_id`, `is_consumer_empty` 게이트, admission `dpa_sent_count[e][r]` | object.h:334-349, dpa.c:797-816, dpa_kernel.c:41 |
| **ARM (제어평면)** | 라우팅 + reverse-DMA enqueue + comch send | **공유 1개**: `consumer_pe`, `comp_queue`, ARM worker thread | object.h:339, dpu_worker.c:696-722 |

```
N개 EU (각자 독립 데이터평면)  ──모두──►  단일 consumer_pe ─► 단일 comp_queue ─► 단일 ARM worker
   pod%N로 ring 할당(dpa.c:1084)              (object.h:339, "consumer k → ARM thread k" 주석만 있고 미구현)
```
- 1 cross-pod RTT = **4 dma_copy**(fwd×2 + rev×2, `dpa_kernel.c:231,342`) + per-RTT ARM 작업:
  comp_queue 4-entry drain + `find_pod_by_id`(linear, RTT당 ~4-6회, comch_server.c:672) +
  `dpu_enqueue_reverse_dma`×2(dst tx_ring에 post) + **comch send×4**(hop당 DMA_COMPLETION+TX_ACK).
- **admission 회계(`dpa_sent_count/cached_freed`)는 ARM이 아니라 DPA-EU 작업**(dpa_kernel.c:41, [정정] — 이전 판이 ARM으로 오기).
- `pod%N`이라 2-pod echo는 N≥2에서 활성 EU 항상 2개(사실, 단 §USER 초점 아님).

---

## 3. 측정된 천장 사다리 (raw data, 신뢰)

전부 dma_copy/s. chain은 RPS×4 정규화.

| 계층 | N=1 | N=2 | N=4 | N=8 | 근거 |
|---|---:|---:|---:|---:|---|
| pure_dma (host gate 없음) | 556K | 1.07M | **1.8M** | 1.47M(감소) | [측정] |
| M0 (drain만) | 325K | 629K | 1.19M | **1.53M** | [측정] |
| M2 (drain + forward 1회) | 322K | 626K | 1.00M | **1.02M(plateau)** | [측정] |
| **chain (full RTT)** | **309K** | **416K** | — (2-pod=2EU) | — | [측정+산술] |

이 사다리에서 끌어내는 핵심 [측정/추론]:
- **단일 ARM의 send 천장 ≈ 1.0M/s**: M2가 N≥4에서 1.0M로 plateau(N=4 1.006M, N=8 1.025M). [측정]
- **M2-N=2의 626K는 ARM이 아니라 EU/drain bound**: M0(send 0회)≈M2(send 1회) at N=2(629K≈626K) →
  send가 이 rate에선 공짜 → 병목은 EU feed, ARM 아님. [추론]
- **chain의 ARM은 4 send/RTT × 104K = 416K send/s = 천장 1.0M의 42%** → **ARM ~2배 여유, 포화 아님.** [추론]
- chain EU는 solo-chain 309K 중 208K/EU(67%)만 사용 → **EU도 포화 아님.** [추론]

### 3.4 N=8 regression 원인 — [정정] 소거법뿐, "PROVEN" 아님

`run_pure_dma.sh` A/B(backoff=256, 8KB, throughput만):

| 실험 | N=4 | N=8 | 직접 배제한 것 |
|---|---:|---:|---|
| ① baseline | 1,710,039 | 1,517,996 | (regression 재현) |
| ② affinity(i→EU i) | 1,766,900 | 1,522,404 | oversubscription |
| ③ 8 thread→EU0 | — | 544,451 | (affinity 작동 확증) |
| ④ fixed 128KB window | 1,734,649 | 1,581,702 | window-shrink |
| ⑤ 128B vs 8KB | — | 1,558,214 / 1,594,613 | bandwidth (op-rate bound) |
| §6.4 backoff | — | 1.557M vs 1.526M | spin-contention |
| M0 N=8 | — | 1.53M | 단일 drain |

**[정정]**: spin/oversubscription/window/BW/drain을 직접 배제했으나, "그 공유 자원이 *DMA-engine*이다"
라는 **positive 직접 증거는 없다**(DPA stall-cycle 미계측, occupancy는 polling으로 무용). 이전 판의
"PROVEN"은 소거법을 PROVEN으로 표기한 과장. **N=8은 이 계획의 초점이 아니다**(2 EU만 쓰므로) — 다만
"DMA-engine 천장이라 EU 추가가 무효"라는 *이전 판의 핵심 전제가 미입증*임을 명시한다.

---

## 4. 왜 chain이 M2보다 낮나 + 왜 코어가 안 먹혔나 (정정된 핵심 분석)

### 4.1 ARM은 병목이 아니다
§3에서: chain ARM은 416K send/s로 천장 1.0M의 42%만 쓴다. drain은 1.53M 중 27%. EU는 309K 중 67%.
**어느 구성요소도 자기 천장에 안 닿았다.** 단일 ARM이 cap이라는 이전 판(op당 2.4µs) 결론은
[정정]된다 — ARM은 처음부터 ~2배 여유가 있었다.

### 4.2 그러면 416K cap의 정체 = 결합 닫힌 루프의 handoff latency
각 요청은 슬롯을 RTT 내내 쥐고 `EU(fwd dma_copy) → ARM(route + reverse desc post) → EU(rev dma_copy)
→ ARM(DMA_COMPLETION + TX_ACK) → 해제`를 돈다(dpu_worker.c:144-148, 426-433). EU와 ARM이 요청마다
*번갈아 의존*한다. **두 서버가 각각 67%/42%인데 직렬 의존 + handoff 지연 때문에 합산 throughput이
둘 중 누구의 천장보다도 낮다.** throughput = effective_concurrency / W(per-request RTT latency).
측정: ~1500 in-flight, W≈14.5ms → 104K. 깊이↑ → W↑, T 평탄(M/M/1, depth 2048→4096 실험).

### 4.3 EU는 정확히 뭘 기다리나 (knee에서 코드+숫자로 후보 좁힘) [추론, 측정으로 확정 필요]
104K knee, in-flight ~1500. EU가 dma_copy 전 멈출 수 있는 곳 3개를 숫자로 검정:
- **admission gate** (dpa_kernel.c:307, inflight≥rq_depth=2048): 1500<2048 → **안 걸림**.
- **is_consumer_empty / recv recycle** (dpa.c:171, comp_queue≥BP_HIGH=3072이면 정지): 1500<3072 → **안 걸림**.
- **reverse desc 미도착**: fwd 완료 → consumer_pe progress → comp_queue → `process_completion_queue`
  → reverse desc `valid=1` → EU의 *다음* drain iter 픽업. **이 handoff latency가 남는 유일한 후보.**

N=1→N=2가 1.35×뿐인 것도 이걸로 설명: N=1은 pod10·pod11이 같은 EU(handoff 내부), N=2는 forward(EU0)와
그 reverse(EU1)가 **다른 EU로 갈려** 매 전달이 ARM을 거쳐 cross-EU handoff를 탄다 → EU 대기 증가.

### 4.4 왜 DPU 코어를 더 줘도 안 늘었나 — split 결과(이미 구현·측정됨)
`DPUMESH_SPLIT_SEND` 토글(0=off, 1=sends-only, 2=rebalanced)을 *이미 구현·측정*했고 전부 **무효**:

| 구성 | overload 천장(RPS) | Δ vs off |
|---|---:|---:|
| 1-EU off / sends-only | 75,921 / 76,079 | +0.2% (noise) |
| 2-EU off / sends-only | 104,894 / 103,912 | **−0.9% (noise)** |
| 2-EU rebalanced | 107,012@110K → 103,562@120K | **+2%→0% (부하 밀수록 소멸 = noise)** |

**왜 무효였나 [정정+추론]**: (i) ARM이 애초에 cap이 아니라(§4.1) 코어 추가는 *노는 ARM에 idle 용량만*
더한 것. (ii) split 절단선이 *불균형* — `sends-only`는 send 신택스만 B로(send는 병목 아님),
`rebalanced`는 *전부* B로(=한 코어). 일을 두 코어에 균형 분할한 적이 없다. (iii) 더 근본적으로 코어는
per-request handoff *latency*(§4.2)를 못 줄인다 — latency는 DMA 왕복 + comp_queue hop이지 ARM CPU가 아님.
→ **"코어를 더 줬는데 안 늘었다"는 "ARM이 병목이 아니었다"의 직접 증거다.** (단 split machinery는
0-fail×30으로 correctness는 입증 → 진단 토글로 보존.)

---

## 5. 개선 plan (고정 2 EU, host→host 제외)

레버는 `T = concurrency / W`에서 **W(handoff latency)를 줄여 EU idle을 없애는 것**. EU util 68→90%면
throughput ~140K RPS(+35%). 우선순위:

### Lever 1 (측정 먼저) — per-request 구간 측정으로 EU 대기 국소화 ★
정적 코드로는 결합 루프 latency를 정확히 못 짚는다(§4.3은 강한 후보지 확정 아님). **측정이 1·2를
targeted fix로 바꾼다.** 방법(§6).

### Lever 2 — handoff latency 단축 (측정이 reverse-desc 대기를 확인하면)
`run_dpu_worker` 메인루프가 reverse desc를 더 빨리 post하도록:
- per-iter 오버헤드 축소: 매 iter `clock_gettime`·1kHz keepalive 분기·deferred drains를 hot path에서 경량화.
- fwd 완료 → reverse post 경로 단축: `process_completion_queue`와 `consumer_pe` progress의 인터리빙
  cadence 조정(reverse desc가 EU의 다음 drain iter 안에 들어오도록).
- 목표: EU가 reverse 일감을 더 빨리 받아 idle↓.

### Lever 3 — `find_pod_by_id` O(n)→O(1) (싸고 무해, 어느 가설이든 도움)
`comch_server.c:672` linear scan, RTT당 ~4-6회. pod_id 인덱스 배열로 per-entry 라우팅 latency 제거.

### 하지 말 것 (측정으로 여유 입증됨)
- **ARM 코어 추가**: ARM ~2배 여유(§4.1), split 무효(§4.4).
- **버퍼/depth, rq_depth 증가**: M/M/1, throughput 평탄(depth 2048→4096); inflight 1500<2048.
- **EU 추가**: 2-pod=2 EU, 그리고 EU도 67%로 포화 아님.

---

## 6. 측정 계획 — per-request 구간 분해 (occupancy 대신)

occupancy(top -H / dpa-statistics active%)는 ARM·EU 둘 다 busy-spin이라 항상 ~100%로 무용. **시간이
어디로 가는지 직접** 측정:
- **DPA 측**: `__dpa_thread_cycles()`를 fwd dma_copy(dpa_kernel.c:231)·rev dma_copy(:342) 직전 stamp,
  `{req_id, t_fwd, t_rev}`를 device 버퍼에 적재 → run 후 `doca_dpa_d2h_memcpy`로 회수(§bench.md §5.4 패턴, hot-path PCIe 0).
- **ARM 측**: req_id별 `CLOCK_MONOTONIC` — `process_forward_entry`(dpu_worker.c:207) dequeue,
  reverse 직후, `process_rev_notify_entry`(:335) dequeue, send 직후.
- **구간**: fwd-dma / **ARM-route** / **comp_queue 체류** / **reverse-pickup 대기** / rev-dma / ARM-notify.
- **판정 맵**: reverse-pickup 대기 + comp_queue 체류가 지배 → handoff latency 확정(Lever 2 타깃).
  ARM-route/notify가 지배 → find_pod/send 비용(Lever 3 + send 경량화). 모든 구간 평탄한데 T plateau →
  순수 RTT 직렬 latency(EU↔ARM 왕복 자체).
- **주의**: EU cycle은 cross-EU 비교 불가; DPA-cycle과 host-MONOTONIC 도메인 혼합 금지(같은 pod의 fwd/rev는
  같은 EU라 intra-EU delta는 유효); 1/K 샘플; **계측 빌드가 104K/0-fail 재현하는지 먼저 확인**(자기무효 방지);
  구간 합이 측정 p50(~14.5ms)와 sampling error 내 일치해야 누락 없음.

---

## 7. 시퀀싱

| Phase | 내용 | 선행 | 기대 | 리스크 |
|---|---|---|---|---|
| **0** | **per-request 구간 측정**(§6) — EU가 무엇을 얼마나 기다리는지 확정 | 진단 토글, 코드 소량 | 병목 *positive* 국소화 | low |
| **1** | `find_pod_by_id` O(1)(§Lever 3) — 측정과 무관하게 무해 | — | per-entry latency↓ | low |
| **2** | handoff latency 단축(§Lever 2) — 측정이 reverse-pickup 대기 확인 시 | Phase 0 | EU util↑ → +~35% | med |
| **—** | (보류) host→host, data-shard, multi-EU 확장 | §8 | — | — |

**핵심**: Phase 0 측정 전에는 구조를 안 바꾼다 — 소거법으로 단정하면 이전 판처럼 결론이 번복된다.

---

## 8. 보류 / 범위 밖 (이번 세션 USER 결정)

- **host→host direct DMA**: 보류. (참고: RTT당 dma_copy 4→2로 per-request 작업·EU op을 직접 반감하는
  유일한 *구조적* 레버지만, 이번 세션에선 고려하지 않음. 재개 시 admission을 dst rq_depth로 re-key +
  forward-time dst 가시성 + per-EU admission single-writer 불변식 처리 필요 — 별도 검토.)
- **활성 EU 늘리기(>2 pod / data-shard 7.B)**: 초점 아님. `pod%N`→2-pod=2 EU는 사실이나 이 문제의 핵심이
  아님. data-shard는 dst-routing demux 미구현 + tx_ring multi-producer + per-EU admission cross-write 등
  고리스크이고, *ARM이 cap으로 측정된 뒤에만* 의미 있는데 §4에서 ARM은 cap 아님으로 기울어 후순위.
- **multi-EU 데이터평면 확장**: 2 EU도 67%로 포화 아니므로 우선순위 낮음.

---

## 9. 정직한 한계 / 미해결

- [ ] **§6 구간 측정** — EU idle의 원인(reverse-pickup 대기 가설)을 *positive*로 확정. 최우선.
- [ ] §4.3의 "EU가 reverse-desc handoff를 기다린다"는 knee에서 admission·recv를 숫자로 배제하고 남은
      강한 코드-확실 후보지만, 결합 루프 latency의 정확한 국소화는 측정 전까지 [추론]이다.
- [ ] N=8 "DMA-engine 천장"은 [귀인] 소거뿐(§3.4) — 이 계획엔 비핵심이나 이전 판의 "PROVEN"은 정정됨.
- [ ] (multi-EU 추진 시) comp_queue overflow at N≥2: capacity 4095, BP_HIGH 3072 headroom 1023 vs
      N×1024 recv pool → 동기화 버스트가 committed 완료를 silent drop 가능(dpa.c:122) — 별도 correctness 이슈.

---

## 부록 — 근거 인덱스 (정정 반영)

| 사실 | 위치 | 판정 |
|---|---|---|
| 4 dma_copy/RTT(fwd×2+rev×2) | dpa_kernel.c:231,342 | ✅ |
| per-EU 데이터평면 / 공유 단일 제어평면 | object.h:334-349,339; dpa.c:569,797-816 | ✅ |
| `pod%N` → 2-pod=2 EU | dpa.c:1084 | ✅ |
| 단일 ARM send 천장 ≈1.0M (M2 N≥4 plateau) | bench M2 N=4 1.006M/N=8 1.025M | ✅ [측정] |
| 626K(M2-N=2)는 EU/drain bound (M0≈M2 @N=2) | M0-N2 629K ≈ M2-N2 626K | ✅ [측정] |
| chain ARM 416K send/s = 천장 42% → ARM 여유 ~2× | 산술(4 send/RTT×104K) | ✅ [추론] |
| chain EU 416K = M2 EU 626K × 0.68(util) | 산술 | ✅ [추론] |
| chain N=1 309K → N=2 416K = 1.35× | bench [측정] | ✅ |
| knee in-flight ~1500 < 2048 slot, < BP_HIGH 3072 | 104K×14.5ms; object.h:88 | ✅ [추론] |
| admission DPA-EU 작업(ARM 아님) | dpa_kernel.c:41,308,358 | ✅ [정정] |
| SPLIT_SEND 구현·측정, 2-EU −0.9%/+2%→0% (무효) | dpu_worker.c:526-572; bench [측정] | ✅ [정정 from "계획"] |
| split 무효 = ARM 여유 + 불균형 절단 + latency 못줄임 | §4.4 | ✅ [추론] |
| N=8 = 소거뿐, positive 증거 없음 | §3.4, bench §9 | ✅ [정정 from "PROVEN"] |
| occupancy(top -H/active%) polling으로 무용 | dpu_worker.c busy-spin, pure no-yield | ✅ |
| host→host 보류 (USER 이번 세션) | — | ✅ [범위] |
