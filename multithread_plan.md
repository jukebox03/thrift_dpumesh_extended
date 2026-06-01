# DPUmesh 멀티스레딩 구현 계획 (L7 all-to-all proxy)

목표: DPU 위 L7 proxy + **모든 pod ↔ 모든 pod DMA**. bench가 보인 DPA 병목을 multithread로 푼다.
모든 주장은 코드(`file:line`)·`bench/bench.md`(§)로 검증됨(4단계 분석: 타당성 → race → 두 DPU 한계
→ 구현 디테일). 자매 문서 [multithread.md] = 타당성/리스크 분석, 본 문서 = **구현 계획**.

---

## 0. 요약

- **Q1 가능한가**: 네. 단 **두 개의 독립 축**으로 분리해야 한다 — DPA EU(데이터 평면) + DPU ARM(제어 평면).
  현재 코드가 이미 sharding 친화적(아래 증거). all-to-all도 **race-free로 가능**하다.
- **Q2 멀티코어 확장 방식**: **end-to-end destination sharding** — pod-shard `k`를 `(ARM-thread k, EU k)`가
  통째로 소유. 이게 cross-pod producer race를 **구조적으로 제거**한다(이전에 제안한 N×N matrix **불필요**).
  핵심 enabler: forward 완료를 **per-call `consumer_id`**(`dpa_kernel.c:230,340`)로 목적지 shard에 직접 라우팅.
- **Q3 추가 연산**: multi-EU(코히런시 window·per-ring admission·idle-poll) + multi-ARM(완료 라우팅·per-shard
  큐·**공유 send-pool 경합** ← 최대 병목). §3 표 참조.
- **최우선 hard gate 2개**: ① comch `max_num_producers=1`이 진짜 하드 제약인가(2h 실험) ② cap이 EU냐 ARM이냐
  (`top -H`+`dpa-statistics`). 둘 다 무료/저비용. 코드 한 줄 전에 먼저.

---

## 1. Q1 — 멀티스레딩 가능한가

### 1.1 두 개의 독립 축 (반드시 분리)
| 축 | 무엇 | 한계 | 근거 |
|---|---|---|---|
| **EU (데이터 평면)** | dma_copy 실행 | ~N=4 (3.25×) | bench §6.3 |
| **ARM (제어 평면)** | 라우팅 + comch send | 단일 스레드 → real routing ~N=3, L7은 더 빨리 | §5.3, 앞선 분석 |

이전 "multithread" 제안은 이 둘을 한 덩어리로 봐서 race(B1)를 피할 수 없다고 결론냈다. **분리하면 풀린다.**

### 1.2 코드가 이미 sharding 친화적이라는 증거 (전부 검증)
| 사실 | 의미 | 근거 |
|---|---|---|
| producer `consumer_id`가 **per-call** | DPA가 완료를 **임의 shard로 라우팅** 가능 → dst-sharding 가능 | `dpa_kernel.c:230,340` |
| 다중 `doca_pe` 생성 허용, device 제한 없음 | ARM drain 스레드별 독립 PE 가능 | `comch_server.c:343`,`dpa.c:401` |
| `max_num_consumers/producers=1`은 **msgq별** | N개 독립 채널로 우회(=pure_dma 방식) | `dpa.c:416,423` |
| mmap 핸들 = **device-level** | 어느 EU나 src/dst 버퍼 접근 | `dpa.c:955,961` |
| `pods[]` = setup 후 read-only(atomic `registered`) | M 스레드 동시 read 안전 | `object.h:248-254`,`comch_server.c:650,666` |
| `tx_ring`·`rev_pos[r]`·admission이 **per-ring** | 1소유자면 lock 없이 안전 | `dpa_kernel.c:333,343,306,356` |
| host RX = EU 커서 `rev_pos[r]` + 독립 bitmap alloc | all-to-all에서도 충돌 없음 | `dpa_kernel.c:333`,`dpumesh_doca.c:230` |

→ **결론: 가능. all-to-all 안전.** 단 §4의 게이트가 선행 조건.

---

## 2. Q2 — 멀티코어 확장 방식

### 2.1 핵심 원칙 — end-to-end destination sharding

pod-shard `k`(= pod_id % N)를 **한 쌍 `(ARM-thread k, EU k)`가 통째로 소유**한다.

```
A(shard a)→B(shard b) 한 방향 전달:

host A ──post──► A의 fwd ring ──drain&copy──► [EU a] ──DMA_COMPLETED(consumer_id=b)──►
                                                              │ (per-call 라우팅)
                                                              ▼
                                              [ARM-thread b] ──routing──► B의 tx_ring (sole producer)
                                                              │
                                              [EU b] ──drain & reverse copy──► host B RX
                                                              │
                                              [ARM-thread b] ──► DMA_COMPLETION→B, TX_ACK→A
```

**왜 race-free (all-to-all):**
- 각 `tx_ring`는 **strict SPSC**: 목적지 B를 소유한 ARM-thread b가 유일 producer, EU b가 유일 consumer.
- `rev_pos[b]`·`dpa_sent_count[b]`·`dpa_cached_freed[b]` 전부 **단일 writer**(EU b) → 비원자라도 안전.
- host RX는 EU b의 순차 커서 + host 독립 bitmap → 여러 소스가 B로 와도 충돌 없음.
- "cross-shard"는 **send/routing 2건뿐**(forward 완료의 consumer_id 라우팅, A로 가는 TX_ACK) — **공유 가변 ring 상태가 아님.**

**source-sharding과의 결정적 차이**: source로 쪼개면 forward 완료가 자연스레 src-shard에 도착하지만, 여러
src-shard가 같은 dst의 `tx_ring`에 쓰게 됨 → many-to-one race → **N×N matrix 필요**. **destination-sharding은
forward 완료를 dst-shard로 재라우팅**(per-call `consumer_id`로 공짜)해서 **matrix를 없앤다.** ← 이번 분석의 핵심.

### 2.2 자원 분할표 (the heart of the plan)

**DPA 측 (EU 복제):**
| 자원 | 현재 | 분할 | 근거 / 비고 |
|---|---|---|---|
| `doca_dpa` device | 1 | **공유** | `dpa.c:356` device-level |
| `doca_dpa_thread` | 1 (`dpa.c:363`) | **per-EU ×N** | `doca_dpa_thread_create` N회 |
| `dpa_thread_arg` | 1 (`dpa.c:356`,`h2d:1052`) | **per-EU ×N** | `rings[]`/`rev_rings[]`/`desc_idx[]`/`rev_pos[]` 격리 |
| comch send msgq (DPU→DPA) | 1 (`dpa.c:409`) | **per-EU 채널 ×N** | 1c/1p가 msgq별 → ADD_RING 라우팅용 |
| `consumer_comp` | 1 (`dpa.c:645,672`) | **per-EU ×N** | thread별 attach |
| `producer_comp` | 1 (`dpa.c:686,692`) | **per-EU ×N** | thread별 attach |
| `dpa_sent_count[]`/`dpa_cached_freed[]` | global (`dpa_kernel.c:39,42`) | **per-EU (단일 writer)** | 비원자 → race. per-EU 이동 필수 |
| `dpu_consumer_id` | scalar (`dpa.c:749`) | **per-shard 배열** | dst→consumer_id 라우팅(2.1) |
| mmap 핸들 | per-pod (`dpa.c:955,961`) | **공유 read-only** | device-level, 불변 |
| DPA producer slot pool | 1024 depth 공유 (`dpa.c:686`) | 공유-경합 ⚠ | N=4 ~56% 점유, N=8 overflow 위험 |

**DPU ARM 측 (M 스레드 분할):**
| 자원 | 현재 | 분할 | 근거 / 비고 |
|---|---|---|---|
| `objs->pe` (control send PE) | 1 (`comch_server.c:343`) | **공유-동기화 ← 병목** | device-level, per-thread 불가 |
| send pool + `send_tasks_in_flight` | 8192 atomic (`object.h:281`) | **공유-동기화 ← 최대 병목** | send당 atomic, O(M) 경합 |
| `consumer_pe` (DPA→DPU) | 1 (`dpu_worker.c:443`) | **per-shard ×N** (다중 PE 허용) | `doca_pe_create` 다중 OK |
| `comp_queue` | 1×4096 (`object.h:45`) | **per-shard ×N** | 비원자 head/tail, 단일가정 |
| `deferred_tx_acks[]` | 1×16384 (`object.h:273`) | **per-shard** | plain int counter race |
| `deferred_recv[]`+BP | 1×1024 (`object.h:263`) | **per-shard** | BP threshold 복제 |
| `pods[]` | append-only (`object.h:248`) | **공유 read-only ✓** | atomic `registered`, 변경 불필요 |
| per-pod `tx_ring`+`tx_producer_head` | per-pod (`object.h:166,184`) | **단일 producer(dst-shard)** | plain uint32 → 1 writer 강제 |
| `consumer_retry` | mutex (`object.h:288`) | **그대로** | 이미 보호됨, 저빈도 |
| stat counters | plain (`object.h:222`) | per-thread 합산 | 영향 미미 |

### 2.3 단계적 구현 경로

| 단계 | 내용 | 게이트/조건 | 기대 | 리스크 |
|---|---|---|---|---|
| **0** | **진단 + capability 실험**(무료): `top -H`+`dpa-statistics`로 cap 확정; comch multi-producer 2h 실험(§4) | 코드 0줄 | 병목·제약 확정 | low |
| **1** | **host→host direct DMA** (chain 4→2, bench §7). `ADD_REV_RING`에 src mmap 노출 | 게이트 무관 선행 | EU·ARM event 둘 다 ½ | med (프로토콜 소변경) |
| **2** | **multi-EU (단일 ARM 유지)**: `dpa_thread`×N, `thread_arg`×N, per-EU counters, EU affinity, dest-sharded reverse rings. N=4 | Step0 cap=EU 확인 시 | ~2.5–3× (~200K RPS) | med-high |
| **3** | **multi-ARM (end-to-end dst-shard)**: `comp_queue`/`deferred`×M, forward 완료 per-call `consumer_id` 라우팅, ARM pthread M개 코어 핀 | Step0 cap=ARM 또는 L7 도입 | ARM 병목 해소 | high |
| **4** | **send-pool 병목 완화**: `CC_SEND_TASK_NUM`↑(8K→32K, HW~65K) / TX_ACK+DMA_COMPLETION batch / per-shard send 채널 | Step3 후 측정 | 제어평면 ceiling↑ | high |

Step 2까지가 데이터 평면(당신이 본 N≤4 스케일), Step 3–4가 **L7의 본 게임**(제어 평면 병렬화).

---

## 3. Q3 — 추가로 발생하는 연산

### 3.1 기준선: 1 cross-pod RTT (현재)
**4 dma_copy** (fwd×2 + rev×2) + **4 comch 메시지** (DMA_COMPLETED×2, REV_DMA_COMPLETED×2) + ARM 라우팅
(`find_pod_by_id` 0.13µs, reverse enqueue 0.04µs, `process_rev_notify` 0.24µs, send 0.10µs). EU 4×4.55µs=18.2µs,
ARM ~1µs/RTT (헤드룸 큼, §5.3). 근거: `dpa_kernel.c:230-240,340-350`,`dpu_worker.c:86-124,263-316`, bench §2.2/§5.3.

### 3.2 multi-EU가 추가 (hot path)
| 연산 | 빈도 | 비용 | 완화 |
|---|---|---|---|
| per-EU writeback window / `read_inv` | iter당 | 코히런시 stress(§4.8/§5.10 slot-leak hang) | **per-EU ring 격리**로 자연 해결 |
| per-ring admission counter (per-EU) | reverse op당 | race 제거 목적 | per-EU 배열로 이동(2.2) |
| idle-ring polling | iter당 idle ring 수 | **+0.19µs/idle ring** (§5.12) | dense 배치(8 ring/4 EU=활성2)→0; sparse면 completion-driven/subscription |
| EU affinity 핀닝 | setup 1회 | — | distinct EU 보장(§B3) |
| DPA producer slot 경합 | op당 | 1024 depth를 N EU가 분점 | per-EU 채널 |

### 3.3 multi-ARM이 추가 (hot path)
| 연산 | 빈도 | 비용 | 완화 |
|---|---|---|---|
| forward 완료 → dst-shard 라우팅 | forward op당 | `consumer_id[shard(dst)]` 배열 lookup (싼 편) | per-shard consumer 사전 등록 |
| per-shard `comp_queue` enq/deq | completion당 | **무경합**(분할 덕) | — |
| per-shard `deferred_tx_acks` drain | iter당 | **무경합**(분할) | — |
| **공유 send-pool atomic 경합** | send당(~2/RTT) | **O(M) ← 최대 병목** (`object.h:281`, bench §5.11) | **Step 4**: pool↑/batch/per-shard 채널 |
| (source-shard였다면) N×N matrix 폴링 | iter당 32셀 | +60~100%(§5.12) | **dst-shard로 회피** |

### 3.4 setup-time (hot path 아님 — 무시 가능)
ADD_RING/ADD_REV_RING ×N채널(`dpa_kernel.c:65-105`), mmap export, `dpa_thread`×N 생성+kick(`dpa.c:1088-1112`),
ARM pthread M개 생성+코어 핀. pod 등록당 1회, ~ms 단위.

---

## 4. 최우선 게이트 & 정직한 기대치

### 4.1 Hard gate #1 — comch multi-producer (2h 실험, 최우선)
`max_num_producers=1`(`dpa.c:423`)이 SDK 하드 제약이면 → M EU가 **한 producer를 공유**(spinlock) 하거나
**M개 독립 msgq**(producer 1개씩) 필요. 실험: 같은 msgq에 `doca_comch_msgq_producer_create` 2회 호출 →
실패하면 "N채널 필수" 확정. **이 결과가 §2.2의 comch 채널 복제 방식을 결정**한다.

### 4.2 Hard gate #2 — cap이 EU냐 ARM이냐
N=4 dest-sharded 실측 중 `top -H -p $(pgrep dpumesh_dpu)`(ARM 코어 %) + `dpa-statistics`(활성 EU·active%).
- ARM<100%·EU active%↑·RPS↑ → **EU-bound** → Step 2가 유효.
- ARM 한 코어 100% → **ARM-bound** → Step 3로 직행.
(pure_dma N=8 regression은 "drain 폴링" 한계지 "L7 라우팅" 한계가 아님 → 실제 ARM 천장은 이 실험으로만 확정.)

### 4.3 정직한 기대치
- 이론 상한: 1.8M dma_copy/s ÷ 4 = **~450K RPS** (현재 77K의 5.8×).
- 현실: host→host(Step1) ~2× → multi-EU(Step2) 단일 ARM 천장 ~3× (≈**200–230K RPS**) → multi-ARM(Step3-4)로
  그 위. **L7 routing/policy가 ARM을 재포화**시키므로 5.8× 전부는 비현실적; **chain 단축 + ARM 병렬화**가 RPS를 결정.

---

## 5. 미해결 / 검증 필요
- comch producer multi-instance 가부(§4.1) — **최우선**.
- 동일 `doca_dpa_dev_comch_producer_t`를 M EU가 동시 submit 시 thread-safe? (아니면 M producer 필요)
- ring→EU 동적 매핑(online pod add/remove) 공정성 + `ADD_REV_RING` 런타임 변경 동기화(`dpa_kernel.c:86-94`).
- per-EU writeback이 N배일 때 §5.10 slot-leak hang 재현 여부 → PCIe coherency 측정.
- send-pool `CC_SEND_TASK_NUM` 8K→32K HW 허용치.
