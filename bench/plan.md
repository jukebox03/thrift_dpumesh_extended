# DPUmesh — Forward Plan

`bench.md`(측정·근거)의 결론을 받아 **다음에 뭘 할지**만 간결히 정리. 근거가 필요하면 bench.md 해당 §를 참조.

## 1. 현재 위치

- 8KB **77,434 RPS (309,736 dma_copy/s)**, 0 failure, back-to-back clean. single-EU single-ring-chain 최적화는 소진(§4.9).
- 이건 host-bound였던 옛 M2 baseline의 99.8%지만, **진짜 single-EU engine 천장(556K dma_copy/s)의 55.7%**일 뿐(§6.2). EU active 0.51%(§5.1).
- 천장 = **(a) RTT당 4 dma_copy chain + (b) per-op EU work(yield ~0.55µs)**. DMA HW 아님. 추가 throughput은 chain 구조 변경에서만 나옴(§7).

## 2. 로드맵 (우선순위 / 상태)

| # | Lever | 잠재 영향 | 상태 | 비고 |
|---|---|---|---|---|
| 1 | **Direct host→host DMA** (4→2 copy/RTT) | **~+100%** (single-EU 천장 139K→**278K**, §6.2 정정) | ⏸ 추후 | 최대 lever. mmap 핸들은 이미 DPA 도달, 진짜 비용은 routing/cursor/credit plumbing. memory "Phase B root cause"와 동일 |
| 2 | **Multi-EU DPA** + 병렬 DPU drain | +50~100% (4EU 3.25×=1.8M op/s) | ⏸ 추후 | EU 노브 없음(dpa.c:443-455 1 하드코딩). **N=8 regression(§5) 진단 선행 필수** |
| 3 | Slot-leak 방어 | RPS 0 (안정성) | 🔵 일부 지금 | **WARN rate-limit만 지금**. heartbeat/recovery는 Rank 5와 함께 |
| 5 | Wake 재조정 (yield 유지) | ~한 자릿수, 주로 idle/tail | ⏸ 추후 | **throughput lever 아님**. regress 위험(§5.9 burst1000 -22.3%), host-trigger 이미 부분 event-driven. cap 안 건드림 |
| — | (4) 하니스 계측 (payload verify/EU knob) | RPS 0 | ⏸ Rank 1/2 전제조건 | Rank 1·2 미루므로 함께 미룸. fail-cause 분리만 선택적 |

## 3. 지금 적용 (Now) — correctness 안전 세트 + WARN rate-limit — ✅ 적용+검증 완료 (2026-05-29)

저위험·명확, hot/deploy 경로 미변경. **검증 결과(8KB, fair, N1–N5 적용 후):** DPU+host 빌드 0 error. 10K 100,000/0 · 50K p99 7.90ms 500,000/0 · 65K p99 9.89ms 1,008 MB/s 650,000/0 · **74K back-to-back ×2 (redeploy 無): 73,425·73,444 / p99 13.59·13.66 / 각 740,000/0 / 1,147 MB/s** → bench.md §4.7과 정합, **회귀·slot-leak 없음**. "DMA ring busy" WARN 0건(N5 정상시 dormant 확인).

| # | 위치 | 문제 | 수정 | 등급 |
|---|---|---|---|---|
| N1 | `buffer.c:211` | `buffer = NULL;` (로컬 파라미터만 NULL) → caller에 dangling pointer 노출. 바로 밑 `*mmap=NULL`(:214)은 올바름 | `*buffer = NULL;` | correctness |
| N2 | `dpa.c:119-124` | line 92에서 이미 `find_pod_by_id(src_pod_id)`로 `src_pod` 얻었는데, ACQUIRE gate 없이 `pod_id` inline 재스캔 → half-published race + RTT당(~309K/s) 중복 스캔 | inline 스캔 삭제, `entry.pod_idx = (int)(src_pod - objs->pods);` (find_pod_by_id가 `&pods[i]` 반환) | correctness |
| N3 | `dpa_common.h` (comch_dma_comp_msg 부근) | `src/dst_pod_id`가 int8인데 wire 제약 assert 없음. MAX_PODS(=8)가 127 넘으면 라우팅 silent 손상 | `_Static_assert(MAX_PODS <= 127, "pod_id wire format is int8");` | correctness |
| N4 | `dpa.c:50,63` | `recv_cb_count` — 가장 바쁜 ARM 콜백(~309K/s)에서 증가만 하고 never read (dead store) | 제거. **`recv_err_count`(:206-213)는 에러 로그에서 읽히므로 유지** | performance |
| N5 | `ring.c:94-96` | slot-leak 시 "DMA ring busy" WARN가 head 안 advance라 무한 flood → 박스 진단 불가 (유일한 leak 증거라 제거는 금지) | rate-limit (N회당 1회 / stuck-window당 1회), severity 유지 | stability |

## 4. 보류 (Deferred) + 이유

- **Rank 1 / Rank 2** — 사용자 결정으로 추후. 둘 다 large, 구조 변경.
- **Rank 3 heartbeat + 자동 recovery** — 정상 운영엔 현재 문제 없음(leak은 §5.10 yield 제거 실험에서만). 가치는 wake/slot-lifetime을 건드리는 Rank 5/1 착수 시 생김. 자동 force-skip은 root cause 미규명(§5.10 가설)이라 live 요청 드롭 위험 → 비추천.
- **Rank 5 wake** — throughput 효과 미미·regress 위험, measure-first. latency 튜닝 필요해질 때.
- **process_fwd_ring/process_rev_ring 헬퍼 통합** — 버그 아님. EU hot path codegen 위험 + Rank 1이 이 경로 재작성 → Rank 1과 함께.
- **첫 pod h2d_memcpy → ADD_RING 통일** — **버그 아님.** h2d가 `doca_dpa_thread_run`(dpa.c:1232) *전*이라 동시접근 없음 → 안전. divergence는 의도적(init=h2d, live update=ADD_RING). deploy 경로 fragile → 건드리지 않음.
- **§5.11(a) `send_tasks_in_flight` mirror 제거** (~0.1µs/op) — small lever, DOCA capability check 필요. 추후.

## 5. N=8 regression 진단 플랜 (Rank 2 착수 전 선행)

§6.4가 EU spin-contention은 backoff로 기각 — 재시도 금지. 유력 가설: **단일 DPU drain(1 pe_progress + 1 comp_queue + 1 ARM thread)**. (※ pure_dma 하니스는 이 repo에 없음 → 재현은 Rank-4 EU 노브 필요.)

1. **(재시작 전, 비파괴)** N=4 vs N=8에서 DPU ARM drain thread `top -H`. N=8만 ~100%면 → 단일 drain이 cap → drain 병렬화 필수. + `dpa-statistics` active-EU < N이면 oversubscription.
2. **차등 측정**: (a) drain 직렬화 → comp_queue per-EU 샤딩 후 8EU 회복? (b) EU oversubscription → FlexIO EU 할당 확인 (c) DMA-HW/PCIe 포화 → drain 여유+active-EU==N인데도 regress면 per-EU completion latency↑ = 하드 천장(software로 못 고침).
3. **생산 EU 수 결정**: (c)면 ~4 cap, 8 추구 금지. (a)면 per-EU drain split 구축해 8.

## 6. 미해결 결정 / 질문

- Rank 1 routing 위치: DPA-side pod 테이블(full +100%, race 어려움) vs DPU pre-resolve(작은 변경, 부분 이득)?
- Rank 1 slot release: 1-copy 시 src TX slot이 단일 copy 완료 기준으로 풀려야 하고 admission이 **목적지 pod의 rq_depth** 기준(현재 source, dpa_kernel.c:305)이어야 함 — 재설계 필요.
- N=8: software 직렬화(샤딩 가능) vs 하드 DMA/PCIe 천장? → Rank 2가 8 목표할지 4 cap할지 결정. §5 측정 없이는 불가.
- `host_rx_buf_size`(dpa.c:1338/1418)가 128 배수 보장? → reverse pos wrap(dpa_kernel.c:352)이 dead code인지 판정 (Rank 1과 묶임).
- 원래 DPA-death root cause(architecture §9: producer-pool 고갈 vs send-pool 기아; 12s 설은 반증) — Rank 5 wake 전 규명.
