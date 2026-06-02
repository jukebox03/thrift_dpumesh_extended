# DPUmesh 멀티스레딩 — 통합 계획 (타당성 + 검증 + DPU multi-core 구현)

`multithread.md`(타당성/리스크) + `multithread_plan.md`(구현) + `multithread_verified_plan.md`(재검증·정정)를
하나로 통합하고, **2026-06-02 직접 실험**(N=8 regression 원인 증명)과 **DPU multi-core 구현 계획**을 추가했다.
모든 구조적 주장은 코드(`file:line`)·`bench/bench.md`(§)로 검증됨(lead 직접 대조 + 7 적대적 agent + 본 세션 실험).
표기: **[측정]** 데이터 직접 / **[추론]** 측정에 산술·모델 / **[미측정]** 직접 증거 없음 / **[정정]** 재검증으로 수정.

---

## 0. 한 줄 결론

**Multi-EU는 천장이 낮다(DMA-engine op-rate ~1.6M/s = 실효 ~4 EU 하드천장, 본 세션 증명). dpumesh chain이 416K에서 멈추는 건 EU·배치·DMA-HW·host가 아니라 단일 ARM 제어평면의 per-RTT 작업량이다. 따라서 진짜 lever 두 개는 ① host→host direct DMA(dma_copy 4→2)와 ② ARM 제어평면 병렬화이며, ②는 데이터 샤딩(고리스크)보다 functional pipeline split(lock-free, 권장 시작)으로 먼저 친다.**

### 요약
- **두 독립 축**: DPA EU(데이터평면) / DPU ARM(제어평면). 반드시 분리해서 본다.
- **N=8 regression = 공유 DMA-engine op-rate 천장**(PROVEN, §2). → multi-EU는 ~4 EU에서 막힘, EU 추가는 lever 아님.
- **chain 416K = 단일 ARM per-RTT 작업 무게**(bench §10). DMA-engine 능력의 ~26%.
- **Lever 우선순위**: host→host(~2×, med) ＞ multi-ARM 제어평면 ＞ multi-EU(체인 전이 미입증, 2-pod엔 ~0).
- **multi-ARM 두 길**: **(A) functional pipeline split** — 2코어, lock-free, 저리스크, 권장 시작 / **(B) destination data-shard** — M코어, 고병렬, 고리스크(verified Phase 3).
- **선결 게이트**: Gate A(comch `max_num_consumers>1` → fan-out ×N vs ×N²), USER 결정(host→host vs 진짜 L7 body inspection).

---

## 1. 두 독립 축 & 현재 구조 — 왜 단일 EU/단일 ARM이 load-bearing인가

| 축 | 무엇 | 현재 한계 | 근거 |
|---|---|---|---|
| **EU (데이터평면)** | dma_copy 실행 | ~4 EU(아래 §2에서 하드천장 확정) | bench §6.3 + 본 세션 |
| **ARM (제어평면)** | 라우팅 + comch send | 단일 스레드 → chain 416K(2-EU) | bench §10 |

현재 구조(전부 코드 확인):
```
Host(pod별 독립 process: dma_ring + PE thread)
      │ fwd ring (Host→DPU)
      ▼
[DPA EU×1: drain_all_rings → 모든 pod의 fwd+rev ring 순회]        doca_dpa_thread ×1 (dpa.c:363)
      │ 단일 comch (max_num_consumers=1, producers=1, per-msgq)   dpa.c:416,423
      ▼
[DPU 단일 worker 스레드 → 단일 comp_queue (lock-free, non-atomic head/tail)]   dpu_worker.c:448-460
      │ 라우팅(pod_id) + reverse desc enqueue(dst tx_ring) + DMA_COMPLETION + TX_ACK
      ▼
  목적지 pod의 tx_ring(rev ring) ─► Host RX
```
- DPU transport 코드에 `pthread_create` **0개**(`dpu_worker.c:442-460` 단일 loop). host측만 1개(`dpumesh_doca.c:497`).
- comp_queue 단일·non-atomic(`object.h:25-27,45-47`). host 변경 불필요(EU-agnostic 64B `dma_desc`).
- 1 cross-pod RTT = **4 dma_copy**(fwd×2 + rev×2) + 4 comch 메시지 + ARM 라우팅(architecture §8).

---

## 2. 측정된 천장 계층 — N=8 regression 원인 증명 (2026-06-02, 세 문서의 "미진단" 해소)

세 문서 모두 N=8 regression을 **미진단**으로 남기고(§6.5 후보 3개, Gate B/B4), "DMA-HW 포화면 ~4 EU 하드천장이라 ceiling 붕괴"를 *리스크*로만 적었다. **본 세션이 이를 실험으로 확정했다.**

### 2.1 천장 사다리 (전부 dma_copy/s, chain은 RPS×4 정규화)

| 계층 | 천장 | 근거 |
|---|---:|---|
| pure single-EU 발행 | 556K | §6.2 [측정] |
| pure 2-EU | 1.07M (1.93×) | §6.3 [측정] |
| **공유 DMA-engine** | **~1.6–1.8M** | §6.3 + 본 세션 [측정] |
| M0(데이터평면+단일 drain) N=8 | 1.53M | §10.2 [측정] |
| 단일 ARM one-way forward(M2) | ~1.0M | §10.2 [측정] |
| **dpumesh chain (2 활성 EU)** | **416K** (104K×4) | §9.4 [측정+추론] |

### 2.2 N=8 = DMA-engine op-rate contention — 직접 실험으로 증명

`run_pure_dma.sh` 같은 세션 A/B(backoff=256, 8KB), **throughput만 사용(polling-immune)**:

| 실험 | N=4 | N=8 | 판정 |
|---|---:|---:|---|
| ① baseline(affinity 없음) | 1,710,039 | 1,517,996 | regression 재현(−11%) |
| ② affinity(i→EU i, 8 distinct EU) | 1,766,900 | 1,522,404 | **oversubscription 아님** |
| ③ positive control(8 thread→EU 0) | — | **544,451** | ≈single-EU → **affinity가 실제 작동함을 증명** |
| ④ fixed 128KB window(N 무관 동일) | 1,734,649 | 1,581,702 | **window-shrink confound 아님** |
| ⑤ op-rate vs BW(N=8, 128B/8KB) | — | 1,558,214 / 1,594,613 | **bandwidth 아님, op-rate 천장** |

기존 데이터로도: spin-contention 기각(§6.4 backoff null), 단일 drain 기각(bench-M0 N=8 1.53M, 단조).

**결론**: 원인 = **공유 DMA-engine의 per-op(WQE 발행+완료) 처리 천장 ~1.6M ops/s.** pure의 descriptor-free EU는 bench보다 ~1.7× 빠르게 발행 → N≈4에서 천장 도달, EU 5–8을 더하면 능동 간섭으로 *절대 감소*.

### 2.3 계획에 주는 함의 (중대)

1. **multi-EU 하드천장 = ~4 EU.** DMA-engine ÷ per-EU 발행률. EU를 4 초과로 늘려도 aggregate 안 오름(오히려 감소). → **verified §6 "DMA-HW 포화면 ~4 EU 하드천장"이 *리스크*가 아니라 *확정 사실*.** multi-EU RPS 투영은 이 천장 아래로 제한.
2. **진짜 lever는 dma_copy COUNT를 줄이는 것**(host→host: 4→2/RTT = 같은 engine으로 2× RPS) **+ ARM 병렬화**. EU 추가가 아님.
3. **Gate B(cap=EU냐 ARM이냐)의 `top -H`/`dpa-statistics active%` 방법은 무효** — ARM(`doca_pe_progress` 무한 busy-spin)·pure EU(no-yield spin) 둘 다 **점유율이 항상 ~100%로 saturate**되어 "유용한 일 vs spin"을 구분 못 함. **cap 판별은 throughput-based만**(multi-ARM 시 throughput 상승 여부 = §7의 실험).

---

## 3. 코드가 sharding 친화적이라는 증거 + 정정

| 사실 | 의미 | 근거 | 정정(verified) |
|---|---|---|---|
| producer `consumer_id`가 per-call | DPA가 완료를 임의 shard로 라우팅 *가능* | `dpa_kernel.c:230,340` | per-call **능력**은 있으나 **per-dst 값 미구현** — `dpu_consumer_id`는 단일 스칼라(`dpa_common.h:42`,`dpa.c:754`). **dst-shard의 load-bearing enabler가 코드에 없음** 🚨 |
| 다중 `doca_pe` 생성 허용 | ARM drain 스레드별 독립 PE 가능 | `comch_server.c:343`,`dpa.c:401` | ✅ |
| `max_num_consumers/producers=1`은 msgq별 | N개 독립 채널로 우회 | `dpa.c:416,423` | ⚠️ `max_num_consumers=1`이면 dst fan-out이 **×N이 아니라 ×N²**(EU당 M msgq). **Gate A의 핵심** |
| mmap 핸들 = device-level | 어느 EU나 src/dst 접근 | `dpa.c:955,961` | ✅ |
| `pods[]` = setup 후 read-only(atomic `registered`) | M 스레드 동시 read 안전 | `object.h:248-254` | ⚠️ multi-ARM 시 `num_pods` RMW·rev-ring 갱신은 atomic publish 필요(`comch_server.c:551-557` plain RMW) |
| `tx_ring`·`rev_pos[r]`·admission = per-ring | 1소유자면 lock 없이 안전 | `dpa_kernel.c:333,343,306,356` | ✅ 단 file-scope global → per-EU 이동 필수(B2) |

→ **결론(정정): 가능. 단 dst-shard 방식은 "미구현 enabler 구축 + Gate A 통과"가 선행.** functional split(§7.A)은 이 enabler가 불필요.

---

## 4. 착수 전 게이트 (재정의) & 선결 USER 결정

| Gate | 무엇 | 왜 결정적 | 비용 |
|---|---|---|---|
| **A 🔑** | comch **`max_num_consumers>1`** + `max_num_producers>1`을 DPA-datapath msgq에서 실측(같은 recv msgq에 consumer 2개 등록 + per-call `consumer_id`로 둘 다 도달?) | dst-shard fan-out이 **×N(가능) vs ×N²(불가)**. NVMe 레퍼런스는 1c/1p 하드코딩이라 multi-consumer 미문서화 → **반드시 실측** | ~2h(HW 필요) |
| **B** | cap = EU냐 ARM이냐 | **부분 해결**: 현재 2-pod 체인은 bench §5에서 EU-per-op-work bound(ARM ~3.5µs 헤드룸), §5.13에서 "ARM polling이 cap" 명시 *기각*. **N=8 원인은 §2에서 DMA-engine으로 확정.** 진짜 미측정 = "**multi-ARM 시 chain RPS가 오르나**"(= §7 실험). **`top -H`/active%는 polling으로 무효(§2.3)** | §7로 측정 |
| **C** | affinity 핀닝(`doca_dpa_thread_set_affinity`, 현재 호출 0) | distinct EU 착지 보장. **단 §2에서 affinity는 N=8을 *안 고침*(oversubscription 아님)** → affinity는 correctness용일 뿐, regression 해결책 아님 | ~0 코드 |

**선결 USER 결정 — host→host vs 진짜 L7 body inspection:**
현재 "L7 all-to-all proxy"는 실은 **L3/L4 pod-id 라우터**다. DPU는 `dst_pod_id` 메타데이터로만 라우팅하고 **body를 안 읽는다**(`dpu_worker.c:142-165`, `dpa_common.h:71`).
- L7 = 헤더/메타데이터 라우팅 → host→host와 **호환**(Phase 1 진행).
- L7 = body 내용 기반 라우팅/정책(진짜 L7) → host→host가 body를 DPU에서 빼므로 **원천 차단**. 그땐 multi-EU/multi-ARM이 유일 lever.
→ **이 결정이 Phase 1 가부를 정한다. 코드 전에 확정.**

---

## 5. Lever 1 — host→host direct DMA (chain 4→2, 최우선·최고가치·correctness-safe)

RTT당 dma_copy를 **4→2로 절반**(bench §7, ~+100%). **EU 천장(§2.3)을 안 건드리고** 같은 engine으로 2× RPS 커버.

- **난이도 = med(정정)**, "blocker 0" 아님. 현재 reverse source가 DPU staging buffer(`dpu_worker.c:112-113`), host A 원본 VA는 완료 ABI(16B HW-lock `comch_dma_comp_msg`)로 전파 안 됨.
- 작업: ① reverse `desc->mmap`을 host A forward 핸들로 re-source ② host A 원본 VA를 완료 경로로 전파 ③ **admission gate를 source→destination rq_depth로 re-key**(`dpa_kernel.c:305`) ④ routing 위치 결정(DPA pod-table vs DPU pre-resolve).
- **신규 correctness 리스크(원 문서 미분석)**: admission re-key 안 하면 fan-in 시 host B RX over/under-admit; **HOL 증폭**(src TX slot을 RTT 내내 점유 → 한 느린 dst가 host A의 다른 모든 dst를 backpressure); `host_rx_buf_size` 128-배수 불변식 load-bearing.
- **리스크 등급**: 새 ring producer 0개(여전히 단일 worker·단일 EU) → §6의 B1 race류와 **격리**. 가장 안전한 +RPS lever.

---

## 6. Lever 2 — multi-EU (데이터평면) — 조건부·제한적

- **진행 조건**: Gate B에서 목표 스케일이 EU-bound 확인 **AND** 독립 포화 dst pod ≥ N(2-pod 벤치로는 증명 불가, `pod%N`이라 N≥2에서 활성 EU 2개 고정).
- **천장(§2.3)**: 실효 ~4 EU. **3.25× 기대 금지** — 그건 pure_dma 상한이고 체인은 §5.6-E4/§5.10 coherency stress로 훨씬 낮음.
- 필수 변경(전부 코드 확인):
  - `dpa_sent_count[]/dpa_cached_freed[]`를 file-scope global → **per-EU `dpa_thread_arg`**(B2, `dpa_kernel.c:39,42`; ADD_REV_RING compacted index 정합 `:86-94`).
  - affinity(Gate C), per-EU `thread_arg`/comch 채널/comp 2종.
  - **echo(dst==src) 한정 안전**(dst-routing 없이). 일반 cross-pod은 §6의 B1 race.
- **"pod별 독립 EU" 매핑 채택 금지**(세 문서 합의): MAX_PODS=8 → pod-per-EU=8=regression 지점+idle-ring 낭비. 한다면 **고정 4-EU 풀 + `pod%4`**.

### 6.1 cross-pod 정합성 blocker (B1, multi-EU/multi-ARM 공통)
- A→B에서 forward 완료는 소스 A의 EU에 도착(`dpa.c:117`)하나 reverse desc는 **목적지 B의 tx_ring**에 write(`dpu_worker.c:102`). `get_next_dma_desc`는 head를 **lock/atomic 없이** read-check-increment(`ring.c:99,101,118-119`), 복구 0.
- N producer가 같은 dst tx_ring에 → torn head / slot 이중할당 = §5.6-E4·§5.10 *증상*(slot stuck/hang)과 동일(*메커니즘*은 torn-head로 다름, [정정]).
- **해결**: per-destination SPSC hand-off 큐 OR tx_ring lock(후자는 경합 재도입). dst-shard로도 producer many-to-one은 안 풀림 → forward 완료 dst-routing demux가 전제(§3 정정).

---

## 7. Lever 3 — ARM 제어평면 병렬화 (DPU multi-core) — 구현 계획

chain 416K의 병목은 단일 ARM의 per-RTT 작업(bench §10: op당 ~2.4µs = comp_queue drain + routing + reverse 중재 + admission + **host-bound lossless send×4**(DMA_COMPLETION+TX_ACK) + full-RTT slot). 이를 두 가지로 푼다.

현재 단일 루프(`dpu_worker.c:448-460`):
```c
while (true) {
  doca_pe_progress(consumer_pe);  // ① DPA→DPU 완료 drain → comp_queue
  doca_pe_progress(objs->pe);     // ② comch 서버: 연결/REGISTER/send 완료 회수
  drain_deferred_tx_acks(objs);   // ③ 밀린 TX_ACK 재전송
  process_completion_queue(128);  // ④ 라우팅 + reverse-DMA post + send×4
}
```

### 7.A 접근 A — functional pipeline split (권장 시작: lock-free, 저리스크) ★

**일을 데이터로 쪼개지 않고(샤딩 아님) *스테이지*로 쪼갠다.** 비대칭이라 **각 공유 구조에 소유자가 1명** → lock이 필요 없다. 절단선은 **두 PE 경계**와 정확히 일치(DOCA PE는 한 스레드만 progress 가능).

| 코어 | 소유 | 일 |
|---|---|---|
| **A (ingest/route, "나머지")** | `consumer_pe` | ① drain + ④ `process_completion_queue`(라우팅 + reverse-DMA를 자기 tx_ring에 post). REV_NOTIFY에서 **직접 send 대신 {conn,msg}를 SPSC 핸드오프에 enqueue** |
| **B (egress, "completion 날려주기")** | `objs->pe` | SPSC dequeue → `server_send_msg_to_conn`(DMA_COMPLETION/TX_ACK) + ② send-pool 회수 + ③ deferred_tx_acks |

lock 0인 이유:
| 구조 | 소유자 | 동기화 |
|---|---|---|
| `consumer_pe`, `comp_queue` | A만 | recv-cb→process, SPSC, lock 無 |
| `tx_ring`(reverse), admission | A만 | 단일 writer, lock 無 (DPA가 cross-device read=기존 그대로) |
| `objs->pe`, `cc_server`, send-pool | B만 | 단일 writer, lock 無 |
| **A→B 핸드오프** | A produce/B consume | bounded SPSC ring, atomic head/tail, lock 無 |

**고려사항(구현 체크리스트):**
1. **DOCA PE 단일-스레드 제약**이 절단선을 결정 — A=`consumer_pe`, B=`objs->pe`. recv-cb(A)는 comp_queue만 건드림(B 구조 무접촉).
2. **pod_state 교차접근**: A가 hot-path `find_pod_by_id`(읽기), B의 `objs->pe`가 `pods_register`/연결이벤트로 씀(드묾). §8.1 `__atomic` publication으로 hot read 안전. **확인 필요**: `update_rev_ring_host_rx` 등 A가 읽는 rev-ring 필드도 atomic publish 되는지 — 아니면 그 필드만 publish 추가.
3. **bounded SPSC + 백프레셔 재설계**: B가 밀리면 큐 full → A가 comp_queue drain 중단 → 기존 BP_HIGH→deferred_recv→DPA `is_consumer_empty` stall로 EU까지 rate-match. **유일한 실질 설계 포인트.**
4. **밸런스 = max(A,B)**: send가 per-op의 절반 이상이면 ~2× 근처. 비대칭이라 측정 후 work 재배치 자유(routing을 B로, 또는 B를 submit/완료-회수 2스테이지로).
5. **send-pool 재활용**: `server_send_msg`는 슬롯 없으면 `progress_all_pes` 최대 10000 spin(`comch_server.c:82`). B가 submit과 `objs->pe` progress를 둘 다 → B 루프는 "조금 submit, 자주 progress".
6. **순서**: 같은 req의 DMA_COMPLETION→TX_ACK는 A가 순서대로 enqueue + B FIFO → 보존.
7. **핀닝/캐시**: A·B를 다른 물리 ARM 코어에 taskset 핀, 핸드오프 **배치**로 cache-line bouncing 완화.
8. EU 평면 무변경(직교). N EU는 여전히 A의 consumer_pe로 모임.

**장점 vs 7.B**: dst-routing demux 불필요, cross-EU fence 불필요, `pods[]` 멀티라이터 문제 없음, 단일 `cc_server` 다중-submit 문제 없음(B가 유일 sender), Gate A(×N²) 무관. **= verified Phase 3의 hard blocker 대부분을 우회.**
**한계**: 2-스테이지라 천장 ~2×. 그 이상은 7.B 또는 추가 분할 필요.

**시작 범위**: `process_rev_notify_entry`의 두 send 호출 → SPSC enqueue로 교체 + send 전담 pthread(`objs->pe` 소유권 이관). env `DPUMESH_SPLIT_SEND=1` 토글, 되돌리기 쉬움.
**이게 곧 Gate B 검증**: 2-thread에서 chain RPS 오르면 → 단일 ARM이 cap이었다 확정(§10 [미측정] 칸). 안 오르면 → 핸드오프 비용(배치 강화 후 재측정) 또는 ARM이 cap 아님.

### 7.B 접근 B — destination data-sharding (고병렬·고리스크, verified Phase 3) — Gate A 통과 시에만

pod-shard `k`(=pod_id%N)를 `(ARM-thread k, EU k)`가 통째로 소유. M 코어로 확장 가능하나 **race-free는 "아래 변경과 함께"만 성립**:

1. **forward 완료 dst-routing demux 구축**(중심 enabler, **현재 부재**): M개 DPU consumer + `consumer_id[shard(dst)]` 배열 + 호출시 lookup. 안 만들면 모든 완료가 단일 src-keyed comp_queue로(`dpa.c:119`) → 병목 그대로 + tx_ring race 재발.
2. **cross-EU coherency fence**: pod A의 dma_buffer를 EU_a write·EU_b read → "per-EU 격리로 자연해결"은 **거짓**[정정]. forward→reverse 핸드오프에 명시적 fence(§5.10 stale-read 실패모드).
3. **`deferred_tx_acks`를 producer(ARM_b) shard로 키잉**(destination 아님): ACK 대상=source A ≠ 생산 shard=dst → dst 샤딩 시 cross-shard write race. 또는 atomic append.
4. **`pods[]` 멀티라이터 안전**: 모든 변이를 단일 control PE에 고정 or `num_pods` `atomic_fetch_add`(`comch_server.c:551-557`).
5. **단일 `cc_server`/`objs->pe` 다중-submit 안전성**: M 스레드가 한 PE에 `doca_task_submit` 동시 호출이 안전한가 — 아니면 **correctness blocker**(plan은 "병목"으로만 취급). dst-shard가 이 벽을 안 없앤다.
6. `busy_head/busy_probes` static(`ring.c:105-106`) → `_Thread_local`(cosmetic 로그 race).

**자원 분할표(DPA 측, EU 복제)** — §6의 multi-EU와 공유:
| 자원 | 현재 | 분할 |
|---|---|---|
| `doca_dpa` device / mmap | 1 / per-pod | 공유(device-level) |
| `doca_dpa_thread`·`dpa_thread_arg`·comch 채널·`consumer_comp`·`producer_comp` | 1 | **per-EU ×N** |
| `dpa_sent_count[]`/`dpa_cached_freed[]` | global | **per-EU(단일 writer)** |
| `dpu_consumer_id` | scalar | **per-shard 배열**(미구현 enabler) |
| DPA producer slot pool | 1024 공유 | 공유-경합 ⚠ (N=8 overflow 위험) |

**자원 분할표(DPU ARM 측, M 스레드)**:
| 자원 | 현재 | 분할 |
|---|---|---|
| `consumer_pe` / `comp_queue` / `deferred_tx_acks` / `deferred_recv`+BP | 1 | **per-shard ×N** |
| `objs->pe`(control send PE) | 1 | **공유-동기화 ← 벽**(device-level, per-thread 불가) |
| send pool + `send_tasks_in_flight` | 8192 atomic | **공유-동기화**(현재 병목 아님; multi-ARM에서 부상 — [정정] 미래투영) |
| per-pod `tx_ring`+head | per-pod | **단일 producer(dst-shard)** |
| `pods[]` | append-only | 공유 read + atomic 슬롯 클레임 |

### 7.C 두 접근 비교 — 언제 무엇

| | A. functional split | B. destination data-shard |
|---|---|---|
| 병렬도 | 2 코어(~2× 상한) | M 코어 |
| lock | **0** | dst-routing/fence/pods/cc_server 해결 필요 |
| 미구현 enabler | 없음 | per-dst `consumer_id` demux(중심) |
| Gate A(×N²) | 무관 | **의존** |
| 리스크 | 저 | 고 |
| 권장 | **먼저**(저리스크로 "ARM이 cap" 확정 + 즉시 ~2×) | A로 부족·Gate A 통과·L7 본게임 시 |

---

## 8. Lever 4 — 제어평면 ceiling 완화 (Phase 3/7.B가 cap으로 측정된 뒤)
`CC_SEND_TASK_NUM` 8K→32K(HW~65K) / TX_ACK·DMA_COMPLETION **batch** / per-shard send 채널.
(bench §5.11: flow control은 현재 cap 주원인 아님 → multi-ARM 후에야 유효.)

---

## 9. 통합 시퀀싱

| Phase | 내용 | 선행 게이트 | 기대 | 리스크 |
|---|---|---|---|---|
| **0** | **게이트**: Gate A(comch multi-consumer, HW 2h) + USER 결정(host→host vs L7 body). `top -H` 방식 폐기(§2.3) | 코드 0줄 | 제약·방향 확정 | low |
| **1** | **host→host direct DMA**(4→2) — 최우선. admission dst re-key + HOL/불변식 | USER=메타라우팅 | ~2× | med |
| **2** | **multi-EU(데이터평면)** — 조건부. per-EU counter + affinity, echo 한정, **≤4 EU** | Gate B EU-bound **AND** pod≥N | 체인 전이 미입증(2-pod ~0) | med-high |
| **3A** | **functional pipeline split**(§7.A) — send/route 2코어, lock-free, `DPUMESH_SPLIT_SEND` 토글 | — (저리스크 선행 가능) | ~2× + "ARM=cap" 검증 | med |
| **3B** | **destination data-shard**(§7.B) — M코어, dst-routing demux 구축 | **Gate A 통과** + 3A 부족 시 | ARM 병목 해소 | high |
| **4** | 제어평면 ceiling 완화(§8) | 3 후 cap 측정 | ceiling↑ | high |

- L7 목표엔 **multi-EU before multi-ARM이 오순서** — L7 routing/policy는 EU가 아니라 **ARM**을 적재.
- **3A를 1과 병행 가능**(직교, 저리스크): host→host로 dma_copy 절반 + functional split로 ARM 절반 = 곱셈.

---

## 10. 정직한 기대치 (정정)

- **multi-EU 상한 = DMA-engine ~1.6M ÷ 4 ≈ pure 환산**, 체인은 §5.6-E4/§5.10 stress로 더 낮음. "1.8M÷4=450K RPS"의 분자 1.8M은 **pure_dma 이상치**(공유상태 0); 실체인 309,736(bench §4.7). → **450K는 soft ceiling, 200–230K도 낙관.**
- **near-term 현실**: host→host(~2×, 최저 리스크 but med) ＞ functional split(~2×, 저리스크) ＞ data-shard(Gate A 통과 시) ＞ multi-EU(2-pod엔 ~0).
- **N=8 원인은 이제 해소**(§2: DMA-engine op-rate). 단 이는 **multi-EU를 software로 못 넘는다는 뜻** — DMA-HW가 ~1.6M에서 막으므로 EU 추가는 무효, **count 감소(host→host)와 ARM 병렬화가 유일 lever**.

---

## 11. 미해결 / 검증 필요

- [ ] **Gate A**: comch `max_num_consumers>1` 실측 (×N vs ×N²) — HW 2h, 최우선.
- [ ] **USER 결정**: host→host vs 진짜 L7 body inspection (§4).
- [ ] **dst-routing demux**(per-dst `consumer_id` 배열) — 7.B의 전제, 현재 0.
- [ ] **functional split(§7.A) 사전 확인**: `pod_state`/rev-ring 필드 atomic publish 여부; bounded SPSC 백프레셔 설계.
- [ ] cross-EU forward→reverse coherency fence (7.B-2, "자연해결" 거짓).
- [ ] 단일 `cc_server`/`objs->pe` 다중-submit thread-safety (7.B-5, correctness).
- [ ] host→host: admission dst rq_depth re-key + HOL 증폭 + `host_rx_buf_size` 128-배수 불변식.
- [ ] `dpa_sent_count[]/dpa_cached_freed[]` per-EU 이동 (B2); `pods[]` 멀티라이터; `busy_head` static.
- [ ] send-pool `CC_SEND_TASK_NUM` 8K→32K HW 허용치.

---

## 부록 — 근거 인덱스 (검증 완료)

| 사실 | 위치 | 판정 |
|---|---|---|
| 단일 DPA thread / 모든 ring 순회 | `dpa.c:363` / `dpa_kernel.c:386-459` | ✅ lead 직접 |
| `max_num_consumers=1` **및** `producers=1`, per-msgq | `dpa.c:416,423` | ✅ |
| `dpu_consumer_id` 단일 스칼라, dst-routing 미구현 | `dpa_common.h:42`,`dpa.c:754`,`dpa_kernel.c:231,341` | ✅ 5 agents |
| comch 채널 ×N vs ×N²(consumer 게이트) | `dpa.c:416`; SDK `doca_dpa_dev_comch_msgq.h:153-165` | ✅ |
| DPU 단일 worker, pthread 0(host 1) | `dpu_worker.c:442-460`,`dpumesh_doca.c:497` | ✅ |
| reverse producer many-to-one(B1) | `dpu_worker.c:102`,`ring.c:99,101,118-119` | ✅ |
| file-scope global admission | `dpa_kernel.c:39,42`; ADD_REV_RING `:86-94` | ✅ |
| affinity 미설정(호출 0) | `dpa.c:351-385` | ✅ (본 세션 추가) |
| 4 dma_copy/RTT | architecture §8 | ✅ |
| host→host = bench §7 lever 1줄, **미구현**, "§5.3 설계" 부재 | bench §7 | ✅ refuted |
| host→host가 L7 body inspection 차단 | `dpu_worker.c:142-165`,`dpa_common.h:71` | ✅ |
| 단일 `cc_server`/`objs->pe` 제어평면 벽 | `comch_server.c:349,358`; `object.h:281` | ✅ |
| 현재 cap = 단일 EU per-op work, ARM 헤드룸("ARM cap" 기각) | bench §5.1·5.3·5.5·5.13 | ✅ |
| **N=8 regression = 공유 DMA-engine op-rate 천장**(affinity/window/BW/drain/spin 직접 기각) | **본 세션 2026-06-02**, bench §9(통합) §2 | ✅ **측정 신규** |
| 천장 사다리(556K/1.07M/~1.6M/1.53M/1.0M/416K) | bench §6·§9(통합) | ✅ |
| **occupancy(top -H/active%)는 polling으로 무효** | `dpu_worker.c:448`(busy-spin), pure 커널 no-yield | ✅ **신규** |
| functional split = lock-free(PE 경계 절단) | `dpu_worker.c:448-460`,`comch_server.c:25,68`,§8.1 atomic | ✅ **신규 설계** |
| chain 416K = per-RTT ARM 작업(op당 ~2.4µs) | bench §10 | ✅ |
