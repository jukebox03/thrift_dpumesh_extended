# DPUmesh 멀티스레딩 — 재검증 + 정정 계획 (verified)

`multithread.md`(타당성/리스크) + `multithread_plan.md`(구현)을 코드·bench에 대해 **한 번 더 빡세게**
교차검증한 결과와 그로부터 정정한 실행 계획. 검증 방식: lead가 모든 구조적 `file:line`을 소스에서
직접 대조 + 7개 적대적 agent가 bench 수치·architecture·comch SDK·race·cap·sequencing을 독립 반증.

**한 줄 결론**: 두 문서의 *사실*은 거의 다 맞다(bench 인용 10/10 수치 정확). 그러나 **세 가지 핵심
판단이 과장/누락**됐고, 그게 시퀀싱을 바꾼다 — ① dst-shard 라우팅의 진짜 enabler(per-dst `consumer_id`)가
**코드에 없음**(현재는 단일 스칼라 상수) ② 그 enabler의 비용이 `max_num_consumers=1` 때문에 **×N이 아니라
×N²일 수 있음**(plan이 안 짚은, 더 결정적인 게이트) ③ host→host는 "blocker 0"이 아니라 "med"이고,
**현재 "L7 proxy"는 실은 body를 안 보는 L3/L4 pod-id 라우터**라 host→host가 진짜 L7 body inspection을
원천 차단함.

---

## 0. 두 문서 모순 — 증거로 판정

| # | multithread.md | multithread_plan.md | 판정 (증거) |
|---|---|---|---|
| C1 | cross-pod은 echo만 안전(§B1) | dst-shard로 all-to-all race-free(§2.1) | **둘 다 부분적으로 맞다.** B1은 *현재 코드* 기준 옳음(dst-routing 미구현 → tx_ring many-to-one race 실재). plan은 *dst-routing을 구축한 뒤* "나열된 변경과 함께" 옳음. 무조건 race-free 아님 |
| C2 | host→host **blocker 0개**(§4) | host→host **med (프로토콜 소변경)**(§2.3 Step1) | **plan이 옳다.** bench/plan.md:15 "진짜 비용은 routing/cursor/credit plumbing", :53 admission을 source→**destination** rq_depth로 re-key 필요 |
| C3 | host→host "이미 설계됨, architecture §5.3" | (Step1 선행) | **multithread.md 틀림.** architecture §5.3 부재(§5=Reverse path), design.md 부재. bench §7 lever 1줄 + bench/plan.md:15 로드맵뿐, **미구현** |
| C4 | cap 미검증 → 진단 게이트(§B4) | cap이 EU냐 ARM이냐(§4.2) | **모순 아님 — 운영점이 다름.** 현재 2-pod 체인은 bench §5.1–5.5에서 **이미 EU-per-op-work bound로 측정**, ARM ~3.5µs 헤드룸("ARM polling이 cap"은 §5.13에서 명시적 *기각*). ARM은 host→host·multi-EU·L7 추가 후 **나중에** 재포화 |

---

## 1. 검증 결과 — 무엇이 맞고 무엇이 과장인가

### 1.1 전부 confirmed (lead가 소스 직접 대조)
단일 DPA thread(`dpa.c:363`)·모든 ring 순회(`dpa_kernel.c:386-459`); DPU transport `pthread_create`
**0개**(host측 `dpumesh_doca.c:497` 1개뿐); affinity 호출 **0개**; mmap **device-level**(`dpa.c:955,961`);
`max_num_consumers=1` **그리고** `max_num_producers=1` **둘 다 per-msgq**(`dpa.c:416,423`); ring head·
comp_queue head/tail **non-atomic**; reverse producer = `get_next_dma_desc(dst_pod->tx_ring)`(`dpu_worker.c:102`);
`dpa_sent_count[]/dpa_cached_freed[]` **file-scope global, ring-index 색인**(`dpa_kernel.c:39,42`);
4 dma_copy/RTT(architecture §8 라이프사이클). **bench 인용 10/10 수치·섹션 정확**(8 confirmed, 2 partial).

### 1.2 과장/부정확 (재검증으로 정정)

| 주장 | 판정 | 정정 |
|---|---|---|
| "forward 완료를 per-call `consumer_id`로 dst-shard에 라우팅 → matrix 불필요" (plan §0/§1.2/§2.1) | 🚨 **현재 코드 기준 refuted** | `dpu_consumer_id`는 thread_arg **단일 스칼라**(`dpa_common.h:42`), `=recv_consumer_id`(`dpa.c:754`), 모든 desc/ring에 **변경 없이** 전달(`dpa_kernel.c:231,341`). per-call *능력*은 SDK에 있으나 **per-dst *값*은 없음**. 이게 dst-shard 계획의 load-bearing enabler인데 **미구현**. 안 만들면 multi-EU split 시 모든 forward 완료가 단일 consumer로 직렬화(병렬화 무효) + tx_ring race 재발 |
| "comch 채널 ×N으로 우회" (plan §2.2 자원표) | ⚠️ **partial — ×N²일 수 있음** | `max_num_consumers=1`(per-msgq)이면 한 EU producer는 자기 msgq의 **유일 consumer 1명**만 도달. dst별 M consumer에 닿으려면 EU당 M msgq → **N_EU × M_ARM 채널**. plan의 ×N 예산은 **`max_num_consumers>1`이 가능할 때만** 성립. 이게 plan §4.1이 안 짚은 **더 결정적인 게이트**(plan은 `max_num_producers`만 테스트) |
| "multi-EU 3.25×(N=4 peak)가 체인에 적용" | ⚠️ **partial — pure_dma 상한** | 3.25×는 `pure_dma`(descriptor 없는 고정주소창, 단일소스, EU별 사설 1MB slice, **공유 가변상태 0**)에서 측정(bench §6.1, L683-685). 실제 lossless multi-ring 체인엔 §5.6-E4가 catastrophic·§5.10이 yield를 강제한 **coherency stress**가 있고, 그건 폴링 EU가 늘수록 악화(N=8 regression의 §6.5 *가장 유력* 원인=단일 DPU drain). **체인에 그대로 전이 안 됨.** 2-pod 벤치는 pod%4로 **최대 2 EU만 점등** → N=4 이득 구조적 도달 불가 |
| "host→host blocker 0개 / 순수 topology" (multithread.md §4·§6) | 🚨 **refuted** | reverse source가 현재 DPU staging buffer(`dpu_worker.c:112-113`), host A 원본 VA는 완료 ABI로 전파 안 됨(`comch_dma_comp_msg`는 16B HW-lock). 필요: desc->mmap을 host A 핸들로 re-source + host VA 전파 + admission을 **dst** rq_depth로 re-key(`dpa_kernel.c:305`). = **med** |
| "B1 race = §5.6-E4·§5.10 실패모드" (multithread.md §B1) | ⚠️ **partial** | E4/§5.10은 **단일-EU vs host 같은 cache line coherency**(single writer), B1은 **두 producer torn-head**. *증상*(slot stuck/hang)은 같으나 *메커니즘*은 다름 |
| "send-pool atomic = 최대 O(M) 병목" (plan §3.3) | ⚠️ extrapolation | bench §5.11은 "flow control은 cap 주원인 **아님**". multi-ARM에서야 병목 — **현재 사실이 아닌 미래 투영** |
| plan §2 "EU knob 없음 = `dpa.c:443-455` 1 하드코딩" | ❌ 잘못된 인용 | :443-455는 msgq-create 에러처리. 실제는 `dpa.c:363` 단일 `doca_dpa_thread_create` |

---

## 2. 핵심 게이트 — 재정의 (코드 한 줄 전, 무료/저비용)

> plan §4의 게이트를 증거에 맞게 재배치. **Gate A가 plan이 놓친 진짜 결정자.**

| Gate | 무엇 | 왜 결정적 | 비용 |
|---|---|---|---|
| **A 🔑** | comch **`max_num_consumers>1`** + `max_num_producers>1`을 **DPA-datapath msgq**에서 실험. 같은 recv msgq에 consumer 2개 등록 + DPA producer가 per-call `consumer_id`로 둘 다 도달되나 | dst-shard 완료 fan-out 비용이 **×N(가능) vs ×N²(불가)** 를 가른다. plan은 producer만 테스트 — consumer가 더 결정적. NVMe 레퍼런스는 1c/1p·`consumer_id=1` 하드코딩이라 **multi-consumer는 미문서화**, 반드시 실측 | ~2h |
| **B** | 현재 cap = EU냐 ARM이냐 | **이미 답 있음**(bench §5: 현재 2-pod 체인 EU-per-op-work bound, ARM 헤드룸). *진짜* 미측정은 **N=8 regression 원인** — 근데 `pure_dma` 하니스가 이 repo에 없음(bench/plan.md:44). multi-EU 추진 시에만 하니스 복원 후 `top -H`+`dpa-statistics` | 하니스 복원 필요 |
| **C** | affinity 핀닝(`doca_dpa_thread_set_affinity`) | N개 EU가 distinct EU에 착지 보장. N=8 regression이 oversubscription이면 **이것만으로 끝**. correctness 무해 | ~0 코드 |

---

## 3. 정정된 시퀀싱

> 누락된 변경을 각 단계에 **명시적으로** 박았다(plan이 빠뜨린 것 = §5 체크리스트).

### Phase 0 — 게이트 (위 §2). **Gate A 결과 없이 multi-ARM 설계 동결.**

### Phase 1 — `host→host direct DMA` (chain 4→2, 최우선·최고가치·correctness-safe)
- **선결 USER 결정(§4)**: "L7 proxy"가 **body 내용 기반 라우팅**을 요구하나? 그렇다면 host→host와 **충돌**(아래).
- 작업(= **med**, "0 blocker" 아님): ① reverse desc->mmap을 host A forward 핸들로 re-source ② host A 원본 VA를 완료 경로(`comp_msg`/`comp_entry`)로 전파 ③ **admission gate를 source→destination rq_depth로 re-key**(`dpa_kernel.c:305`) ④ routing 위치 결정(DPA pod-table vs DPU pre-resolve).
- **신규 correctness 리스크(두 문서 미분석)**: admission re-key 안 하면 fan-in 시 host B RX over/under-admit; **HOL 증폭**(src TX slot을 RTT 내내 점유, 한 느린 dst가 host A의 다른 모든 dst를 back-pressure); `host_rx_buf_size` 128-배수 불변식이 load-bearing.
- 리스크 등급: **새 ring producer 0개**(여전히 단일 worker·단일 EU) → **B1 producer-race류와 격리**. 그래서 이게 가장 안전한 +RPS lever.

### Phase 2 — multi-EU (데이터 평면) — **조건부**
- 진행 조건: Gate B에서 *목표 스케일*이 EU-bound 확인 **AND** 독립 포화되는 dst pod ≥ N (2-pod 벤치로는 증명 불가).
- 필수 변경: `dpa_sent_count[]/dpa_cached_freed[]` **per-EU thread_arg 이동**(global 충돌, plan §B2 인정) + affinity(Gate C) + per-EU `thread_arg`/comch/comp.
- echo(dst==src) 한정 안전(dst-routing 없이). **3.25× 기대 금지** — pure_dma 상한이고 체인 coherency stress로 훨씬 낮을 것.
- **사용자의 "pod별 독립 EU" 매핑 채택 금지**(두 문서 합의): MAX_PODS=8 → pod-per-EU=8=regression 지점+idle-ring 낭비. 한다면 **고정 4-EU 풀 + `pod%4`**.

### Phase 3 — multi-ARM dst-sharding (L7 제어평면의 *진짜* lever, 최고 리스크) — **Gate A 통과 시에만**
race-free는 "**나열된 변경과 함께**" 성립. plan이 빠뜨린 필수 작업:
1. **forward 완료 dst-routing demux 구축**(중심 enabler, 현재 부재): M개 DPU consumer + `consumer_id[shard(dst)]` 배열 + 호출시 lookup. 안 만들면 모든 완료가 단일 `comp_queue`로(`dpa.c:119`, src-keyed) → 병목 그대로 + tx_ring race 재발.
2. **cross-EU coherency fence**: pod A의 dma_buffer를 EU_a write·EU_b read → plan §3.2 "per-EU 격리로 자연해결"은 **거짓**. forward→reverse 핸드오프에 명시적 fence/handshake 필요(§5.10 stale-read와 동일 실패모드).
3. **`deferred_tx_acks`를 producer(ARM_b) shard로 키잉**(destination 아님): ACK 대상=source A ≠ 생산 shard=dst ARM_b. dst로 샤딩하면 cross-shard write race. 또는 atomic append.
4. **pods[] 변경 필요**(plan "변경 불필요"는 틀림): 모든 pod-table 변이를 단일 control PE에 고정하거나 `num_pods`에 `atomic_fetch_add`(`comch_server.c:551-557` plain RMW).
5. **단일 cc_server/`objs->pe` 스레드안전성 해결**: 모든 host-bound send가 하나의 server ctx·하나의 PE 통과(`comch_server.c:349,358`). M 스레드가 한 PE에 `doca_task_submit` 동시 호출이 **안전한가** — 아니면 **correctness blocker**(plan은 "병목"으로만 취급). dst-shard가 이 벽을 **안 없앤다**.
6. `busy_head/busy_probes` static(`ring.c:105-106`) → `_Thread_local`/per-ring (cosmetic, 로그 race).

### Phase 4 — 제어평면 ceiling 완화 — Phase 3가 cap으로 측정된 뒤
`CC_SEND_TASK_NUM` 8K→32K(HW~65K) / TX_ACK·DMA_COMPLETION batch / per-shard send 채널.

---

## 4. 선결 USER 결정 — host→host vs L7 body inspection

**현재 "L7 all-to-all proxy"는 실은 L3/L4 pod-id 라우터다.** DPU는 `dst_pod_id` 메타데이터로만
라우팅하고 **body를 절대 안 읽는다**(`dpu_worker.c:142-165`, `dpa_common.h:71` "DPU는 byte-position
미해석"). host→host direct DMA는 body를 DPU 메모리에서 빼므로:
- L7 = **헤더/메타데이터 라우팅**이면 → host→host와 **호환** (Phase 1 진행).
- L7 = **body 내용 기반 라우팅/정책**(진짜 L7)이면 → host→host가 **원천 차단**. 그땐 body가 DPU를
  지나야 하므로 host→host는 테이블에서 빠지고, multi-EU/multi-ARM이 유일 lever.

→ **이 결정이 Phase 1 가부를 정한다. 코드 전에 확정 필요.**

---

## 5. 두 문서가 빠뜨린 변경 — 체크리스트 (구현 시 누락 금지)

- [ ] **dst-routing demux** (M consumer + `consumer_id[shard]` 배열) — dst-shard의 *전제*, 현재 0
- [ ] **cross-EU forward→reverse coherency fence** (§3.2 "자연해결" 거짓)
- [ ] `deferred_tx_acks` **producer-shard 키잉** (dst 아님)
- [ ] `pods[]` 멀티라이터 안전(단일 PE 고정 or atomic 슬롯 클레임)
- [ ] 단일 `cc_server`/`objs->pe` **다중스레드 submit 안전성** 검증(correctness, not perf)
- [ ] host→host: admission **dst rq_depth re-key** + HOL 증폭 + `host_rx_buf_size` 128-배수 불변식
- [ ] `dpa_sent_count[]/dpa_cached_freed[]` per-EU 이동(plan 인정분)
- [ ] `busy_head/busy_probes` static thread-safety (cosmetic)
- [ ] 단일 `producer_comp`(depth 1024) 공유 시 cross-shard HOL 차단(Gate A 호의적이어도)

---

## 6. 정직한 기대치 (정정)

- plan §4.3 "1.8M ÷ 4 = 450K RPS"의 **분자 1.8M은 pure_dma 이상치**(공유상태 0). 실체인은
  309,736 dma_copy/s(bench §4.7). 체인에 multi-EU 3.25× 그대로 적용 불가 → **450K는 soft ceiling,
  200–230K도 낙관**.
- **현실적 near-term**: host→host(~2×, 최저 리스크, but med 작업) ＞ 그 위에 multi-ARM dst-shard
  (L7 제어평면, Gate A 통과 시) ＞ multi-EU(체인 전이 미입증, 2-pod에선 ~0).
- **L7 목표엔 multi-EU before multi-ARM이 오순서**: L7 routing/policy는 **ARM**을 적재하지 EU가 아님.
- N=8 regression 원인은 **두 문서 모두 미진단**(bench §6.5 후보 3개, 측정 0). 모든 multi-EU RPS 투영이
  "software로 고칠 수 있다"를 가정 — (c) DMA-HW/PCIe 포화면 ~4 EU 하드천장이라 ceiling 붕괴.

---

## 7. 근거 인덱스 (재검증 완료)

| 사실 | 위치 | 판정 |
|---|---|---|
| 단일 DPA thread / 모든 ring 순회 | `dpa.c:363` / `dpa_kernel.c:386-459` | ✅ lead 직접 |
| `max_num_consumers=1` **및** `max_num_producers=1`, per-msgq | `dpa.c:416,423` | ✅ lead 직접 |
| `dpu_consumer_id` 단일 스칼라, dst-routing 미구현 | `dpa_common.h:42`, `dpa.c:754`, `dpa_kernel.c:231,341` | ✅ 5 agents |
| comch 채널 ×N vs ×N² (consumer 게이트) | `dpa.c:416`; SDK `doca_dpa_dev_comch_msgq.h:153-165` | ✅ comch-sdk agent |
| DPU 단일 worker, pthread 0 (host측 1개) | `dpu_worker.c:442-537`, `dpumesh_doca.c:497` | ✅ lead 직접 |
| reverse producer many-to-one(현 코드) | `dpu_worker.c:102`; `ring.c:99,101,118-119` | ✅ |
| 단일 `cc_server`/`objs->pe` 제어평면 벽 | `comch_server.c:349,358`; `dpu_worker.c:444`; `object.h:281` | ✅ caps agent |
| 4 dma_copy/RTT | architecture §8 (T4/T8/T15/T17) | ✅ |
| host→host = bench §7 lever 1줄, **미구현**, "§5.3 설계" 부재 | bench §7; architecture(§5.3 없음); design.md 없음 | ✅ refuted |
| host→host가 L7 body inspection 차단 | `dpu_worker.c:142-165`, `dpa_common.h:71` | ✅ arch agent |
| multi-EU 3.25× = pure_dma(공유상태 0), 체인 미전이 | bench §6.1-6.3 (L683-685) | ✅ caps agent |
| 현재 cap = 단일 EU per-op work, ARM 헤드룸("ARM cap" 기각) | bench §5.1·5.3·5.5·5.13 | ✅ sequencing agent |
| host→host "med", admission dst re-key | bench/plan.md:15,53; `dpa_kernel.c:305` | ✅ sequencing agent |
| N=8 regression 미진단(후보 3) | bench §6.5 | ✅ |
