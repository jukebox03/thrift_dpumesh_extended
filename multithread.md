# DPA 멀티스레딩 — 구현 가능성 & 주의점

DPUmesh transport에 DPA(FlexIO) multi-EU를 도입하자는 제안("pod/DPU별 독립 DPA thread")의
타당성. 모든 핵심 주장을 실제 코드(`file:line`)와 `bench/bench.md`(§ 번호)로 교차검증 완료
— 구조 6 / race 9 / 자원 6 / 벤치 10 / 대안 4개 주장 **전부 confirmed**.

---

## 결론 (TL;DR)

1. **구현 자체는 가능하나, echo(dst==src) 트래픽에서만 안전하다.** 일반 cross-pod mesh는
   tx_ring producer race(→ §B1)로 막혀, lock 재도입 없이는 정합성이 붕괴한다.
2. **유효 병렬성 상한은 ~4 EU**(파티션 코어 64개와 무관). 4 EU 3.25× peak, **8 EU는
   regression**(bench §6.3).
3. **사용자의 "pod별 독립 EU" 매핑은 데이터가 반증한다.** `MAX_PODS=8`이라 pod당 1 EU =
   정확히 8-EU regression 지점 + idle-ring 폴링 낭비. → 한다면 pod 수와 분리된 **고정 4-EU
   풀 + ring 샤딩(`pod%4`)**.
4. **그 4 EU조차 "DPU 단일 drain이 병목"이라는 미검증 가설 위에 있다.** pure_dma는 단일
   drain으로 N≤4를 깨끗이 3.25× 스케일 → 가설 반증. **착수 전 무료 진단 게이트 필수**(→ §B4).
5. **같은 +100%를 더 싸게 주는 `host→host direct DMA`가 이미 설계됐고 구현만 안 됐다**(→ §4).
   blocker 0개 → **이걸 먼저** 하는 게 맞다(두 lever는 곱셈).

---

## 1. 구현 가능한가 — feasibility 판정

| 항목 | 판정 | 근거 |
|---|---|---|
| 코드상 multi-EU 경로 | ✅ 존재 | 단일 `doca_dpa` 아래 N `doca_dpa_thread` 분리 가능. routing은 DPU ARM에서만(`dpu_worker.c`) → DPA ring 샤딩이 routing 의미를 안 깸 |
| host 변경 | ✅ 불필요 | host는 이미 pod별 독립 poster(`dpumesh_doca.c`); `dma_desc`(64B)에 EU/consumer 필드 없음 → EU-agnostic ABI |
| echo(dst==src) 트래픽 | ✅ 안전 | 자기 tx_ring을 단일 소스만 write |
| 일반 cross-pod mesh | 🚨 high-risk | tx_ring producer race(§B1); shard-by-destination으로도 안 풀림 |
| 실측 이득 | ⚠️ ~4 EU 한정, 2-pod 벤치엔 ~0 가능 | bench §6.3 / §5.1·§5.5 (EU 99.49% idle, cap=per-op work) |

→ **"가능하나 echo 한정 + 진단·대안 선행 조건부."**

---

## 2. 현재 구조 — 왜 "단일 EU"가 load-bearing인가

```
Host(pod별 독립 process: dma_ring + ring_lock + PE thread)
      │ fwd ring (Host→DPU)
      ▼
[DPA EU×1: drain_all_rings → 모든 pod의 fwd+rev ring 순회]   ← doca_dpa_thread ×1
      │ 단일 comch (consumers=1, producers=1)
      ▼
[DPU: 단일 worker 스레드 → 단일 comp_queue (lock-free, non-atomic head/tail)]
      │ 라우팅(pod_id) + reverse desc enqueue + TX_ACK
      ▼
  목적지 pod의 tx_ring (rev ring) ─► Host RX
```

핵심 사실(전부 코드 확인):
- DPA thread 1개만 생성(`dpa.c:363`), 그 한 스레드가 모든 ring을 순회(`dpa_kernel.c:386-459`).
- comch `max_num_consumers=1`/`producers=1`(`dpa.c:416,423`) → 모든 완료가 단일 깔때기.
- DPU는 진짜 단일 스레드 — transport DPU 코드에 `pthread_create` **0개**
  (`dpu_worker.c:442-454` 단일 loop). comp_queue도 단일·non-atomic(`object.h:25-27,45-47`).
- host는 변경 불필요(위 §1).

---

## 3. 주의할 점 — 착수 전 반드시 (blockers)

### B1. 🚨 cross-pod tx_ring producer race (치명적, 가장 중요)
- A→B 요청에서 forward 완료는 **소스 A의 EU**에 도착(`dpa.c:117` `pod_idx = src_pod - pods`)
  하지만 reverse desc는 **목적지 B의 tx_ring**에 write(`dpu_worker.c:102`
  `get_next_dma_desc(dst_pod->tx_ring)`, 소스 버퍼 read는 `:112`).
- `get_next_dma_desc`(`ring.c:99,101,118-119`)는 `head`를 **lock/atomic 없이**
  read-check-increment. 복구 로직 0(rate-limited WARN만).
- N drain 스레드에서 A→B·C→B 동시 → torn head / slot 이중할당 / valid==1 slot 덮어쓰기
  = §5.6-E4(94% 붕괴)·§5.10(영구 slot-leak hang)와 동일 실패 모드.
- **shard-by-destination 무효**: tx_ring의 *consumer(EU)* 만 단일화, *producer*(many-to-one,
  아무 소스→아무 목적지)는 그대로. **echo(dst==src)만 자기완결적으로 안전** — 단, echo pod로
  향하는 cross-pod 요청이 섞이면 그것도 race. 일반 mesh는 **per-destination SPSC hand-off 큐**
  또는 **tx_ring lock** 필수(후자는 없애려던 경합을 hot path에 재도입).

### B2. file-scope 전역 충돌
- `dpa_cached_freed[]`/`dpa_sent_count[]`(`dpa_kernel.c:39,42`)는 로컬 ring index `r`로 인덱싱
  → N thread instance에서 충돌. **per-EU `dpa_thread_arg`로 이동 필수**. `ADD_REV_RING`이
  index를 update-in-place 재사용(`dpa_kernel.c:86-94`)하므로 compacted index 정합 보장 필요.

### B3. affinity 미설정 → EU oversubscription
- `doca_dpa_thread_set_affinity`/`eu_affinity_create` 호출 **0개**(`dpa.c:351-385`), 기본
  'relaxed' 배치 → N 스레드가 N개의 서로 다른 EU에 안 올라갈 수 있음(N=8 regression의 유력 후보).

### B4. 진짜 병목 미검증 — 진단 게이트 (코드 0줄, 무료, 최우선)
- bench §6.5는 3 후보(① DPU drain ② EU oversubscription ③ DMA-HW/PCIe contention)를
  **나열만** 하고 진단 미실행. pure_dma는 단일 drain으로 N=2 1.93×·N=4 3.25× 깨끗이 스케일
  → **"단일 drain이 N≤4 cap"을 반증** → N=8 절벽은 oversubscription/DMA-HW 쪽 가능성↑.
- 진단(N=8 sustained 중):
  ```
  top -H -p $(pgrep dpumesh_dpu)   # drain ~100%? → drain-bound (split 정당)
  dpa-statistics                    # 활성 EU<8? → oversubscription (affinity만으로 해결)
  ```
- **가장 싼 변별 실험**: drain split 없이 **affinity 핀닝만** 추가 후 N=8 재측정. 회복되면
  oversubscription → **DPU drain 대공사 전체가 불필요**.

### B5. 자원 ×N — HW 한계
- EU당 N × (1024 recv task + 1024 producer completion depth + 2 msgq + 2 comp). 현재 전부
  단일 instance(`dpa.c:616,654,686`). `CC_DPA_MAX_MSG_NUM=1024`는 **per-msgq라 글로벌 풀 아님**
  → BF-3 DPA HW 한계 대비 확인 필요.

### B6. 잔존 공유 ceiling
- `cc_server` send pool + `send_tasks_in_flight` atomic + 단일 `objs->pe`는 multi-EU 후에도
  공유 → 고RPS에서 새 병목 가능(per-EU server send context 필요할 수 있음). drain 스레드의
  `pod_state` write(`tx_producer_head` 등)는 소유 스레드로 한정하거나 atomic화.

---

## 4. 더 싸고 효과는 같은 대안 (먼저 할 것)

**`host→host direct DMA`** — RTT당 dma_copy chain을 **4→2로 절반**(bench §7, ~+100%):
- 현재 cross-pod 1 RTT = forward DMA×2 + reverse DMA×2 = 4 copy(`architecture §349-397`).
- **EU 수와 무관**, B1–B5 hard blocker **전부 불필요**(순수 data-path topology 변경).
- `architecture/dpumesh_architecture.md` §5.3 / `bench/plan.md` #1 / memory
  `project_phase_b_root_cause.md`: **설계됐고 구현만 안 된 상태**(Phase B cliff 근본원인).
- multi-EU와 **곱셈**: chain이 절반이면 같은 op-rate가 2배 RPS 커버.

---

## 5. 그래도 multi-EU를 추진한다면 — 단계 경로

모두 §B4 진단이 **"drain-bound" 확인된 이후** 진행.

| 단계 | 내용 | 리스크 |
|---|---|---|
| 0 | **진단 게이트**(§B4): `top -H` + `dpa-statistics`(N=8). drain-bound 아니면 중단 | low |
| 1 | 무동작 리팩터: `doca_dpa` 1개 아래 N thread 분리; 전역(§B2)→per-EU `dpa_thread_arg`; `dpa_comch`→`[N]`. N=1로 동일 동작 확인 | medium |
| 2 | N=1에 **affinity 핀닝** 추가, `dpa-statistics`로 의도 EU 착지 확인 | low |
| 3 | **N=2 + echo 한정**: EU별 {thread, arg, affinity, msgq쌍, comp 2종, consumer_id} + DPU측 {consumer_pe, comp_queue, drain pthread(ARM 코어 핀)}. op-rate ~1.93× 기대 | high |
| 4 | **cross-pod 정합성 해결**(§B1): per-destination hand-off 큐. 非-echo A↔B back-to-back로 slot-leak 0 검증 | high |
| 5 | N=4 일반화(`pod%4`, ~2 pod/EU). back-to-back이 단일 EU를 못 이기면 중단 | high |

---

## 6. 권고 (sequencing)

1. **§B4 진단 먼저**(1시간, 무료) — 병목 정체부터 확정. oversubscription이면 affinity만으로 끝.
2. **`host→host direct DMA`(§4) 먼저 구현** — 같은 +100%, blocker 0, 이미 설계됨.
3. **그 후** halved-chain baseline에서 multi-EU를 *고정 4-EU 풀*로 재평가. **사용자의
   "pod별 EU" 매핑은 8-pod regression + idle-ring 낭비로 반증되므로 그대로 채택 금지.**

---

## 부록 — 근거 인덱스 (검증 완료)

| 사실 | 위치 | 판정 |
|---|---|---|
| EU 파티션 64 | `dpa_resource_config.yaml:4-7` | ✅ |
| 단일 DPA thread / 모든 ring 순회 | `dpa.c:363` / `dpa_kernel.c:386-459` | ✅ |
| comch consumers/producers=1 | `dpa.c:416,423` | ✅ |
| 비-atomic ring head + 복구 0 | `ring.c:99,101,118-119` | ✅ |
| cross-pod reverse enqueue | `dpu_worker.c:102,112`; src 결정 `dpa.c:117` | ✅ |
| file-scope 전역(reverse admission) | `dpa_kernel.c:39,42`; ADD_REV_RING `:86-94` | ✅ |
| affinity 미설정 | `dpa.c:351-385` (호출 없음) | ✅ |
| DPU 단일 worker, pthread_create 0 | `dpu_worker.c:442-454` | ✅ |
| comp_queue 단일·non-atomic | `object.h:25-27,45-47` | ✅ |
| MAX_PODS/MAX_DPA_RINGS=8, CC_DPA_MAX_MSG_NUM=1024(per-msgq) | `dpumesh_common.h:23-24`, `dpa.h:9` | ✅ |
| setup_pod_dma 분기점 | `dpa.c:1044,1062,1078,1104` | ✅ |
| multi-EU 스케일 1.93×/3.25×/regression | bench §6.3 | ✅ |
| 단일 EU 556K op-rate bound / spin 기각 | bench §6.2 / §6.4 | ✅ |
| N=8 3 후보 + 진단 미실행 | bench §6.5 | ✅ |
| idle-ring 폴링 1.017→2.0 / ARM 헤드룸 ~3.5µs | bench §5.12·§5.4 / §5.3 | ✅ |
| EU 99.49% idle, cap=per-op work | bench §5.1·§5.5 | ✅ |
| host→host direct DMA(chain 4→2, ~+100%) | bench §7 / `architecture` §5.3·§349-397 | ✅ |
| 현재 sustainable 74K / 309,736 dma_copy/s | bench §4.7·§4.9 | ✅ |
