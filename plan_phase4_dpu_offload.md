# Phase 4 — DPU-offload body DMA (2026-05-15)

> v5 측정 infra 위에서 다음 architectural step.
> 현재 src host가 chunk_builder를 통해 직접 DMA descriptor를 발사하는 구조를,
> "DPU가 hdr 받자마자 본인이 직접 body DMA 명령을 발사"하는 구조로 전환.
> batching 효과(31 body/chunk packing)는 그대로 유지.

---

## 0. 왜 바꾸는가

v5 측정 결과 + 현재 코드 trace에서 드러난 한계:

1. **K=1 RTT = 4ms** — split 모드(PE 별도 코어)에서도 동일. CFS rotation 아니라 **builder timeout 계열**(host chunk_builder + echo chunk_builder + hdr 양방향 ~1ms × 4 hop). v5 §3 분석 부분 정정 필요.
2. **split 모드 K=64+에서 망가짐** — orphan expected timeout 다발. 원인: PE와 chunk_builder가 다른 코어에 분리되면 hdr/chunk DMA 도착 시점 race. 현재 코드는 **사실상 shared(1-core) 전용**.
3. **1 core cap 450k RPS의 정체** — libdoca polling 93% (host PE thread). 그 polling 중 일부는 chunk_builder의 dma_ring CASE_DIRECT enqueue용 completion. 이 경로 빼면 polling 부담도 줄어들 가능성.

새 구조의 기대:
- chunk_builder timeout 1ms 사라짐 → K=1 RTT **~2ms 추정** (hdr 양방향만 남음)
- expected ring + parked chunk 로직 단순화 → race 표면 감소 → **split 모드 가능**
- host의 chunk-side libdoca polling 감소 → 1 core cap 상향 여지

---

## 1. 현재 구조 (변경 전)

### 1.1 Body 경로 (요청)

```
src host:
  worker → send_request
    ├─ tx_alloc → chunk_tx slot (N body packing 공간)
    ├─ memcpy(body) into chunk_tx slot at offset
    ├─ hdr_builder.append(mesh_hdr_req)          ★ hdr_builder
    └─ chunk_builder.append (body 슬롯 위치 기록)  ★ chunk_builder

src host (lock 안 또는 timeout 1ms):
  builder_flush_locked
    ├─ hdr_batch DMA submit (host → DPU staging)
    └─ chunk dma_ring에 descriptor 1줄 write + valid=1
        (DPA가 그 ring spin polling하다 발견)

DPA:
  ring descriptor 발견 → doca_dpa_dev_comch_producer_dma_copy(
       src_host_chunk_tx_mmap, src_chunk_addr + offset,
       dst_host_rx_mmap,       dst_rx_addr + cursor,
       chunk_size)

DPU:
  hdr batch 받음 → service resolve → dst pod → hdr_fwd 발사 (DPU → dst host)
  body는 안 봄. 완료 시 dmesh_dma_completion_msg 보냄.

dst host (PE thread):
  OP_HDR_BATCH 도착 → process_hdr_batch → expected[src_id] ring push
  OP_CHUNK 도착 → process_chunk → expected와 매칭 → deliver
                                   매칭 안 되면 parked
```

### 1.2 누가 무엇을 하는가

| 행위자 | 일 | 비용 (450k RPS, 1코어) |
|---|---|---:|
| src host worker | builder append, memcpy, hdr+chunk paired flush | ~1% CPU |
| src host PE thread | libdoca poll completions (hdr ACK, DMA completion) | ~93% CPU |
| **DPA** | chunk ring polling, CASE_DIRECT DMA 발사 | 별 코어, 측정 안 함 |
| DPU ARM | hdr forward, completion msg, TX_ACK | 3% (epoll-blocked) |
| dst host PE thread | hdr/chunk 도착 매칭, deliver | src와 같은 패턴 |

★ **결정적**: host의 chunk-side 책임 = (a) packing (memcpy), (b) ring descriptor write + valid=1, (c) chunk-side completion 받기. 이 중 **(b)와 (c)가 DPU로 이동**할 수 있고 **(a)는 그대로 유지**.

---

## 2. 새 구조 (Phase 4)

### 2.1 Body 경로 (요청, 변경 후)

```
src host:
  worker → send_request
    ├─ tx_alloc → chunk_tx slot                  ★ 유지
    ├─ memcpy(body) into chunk_tx slot at offset  ★ 유지 (packing)
    └─ hdr_builder.append(mesh_hdr_req_v4)
       (hdr 안에 src_chunk_slot, src_offset, body_len 포함)
                                                  ★ chunk_builder 통째 제거

src host:
  hdr_builder flush (cap 또는 timeout) → hdr_batch DMA → DPU

DPU:
  hdr batch 받음 → 각 hdr 처리:
    ├─ service resolve → dst pod_id
    ├─ ★ DPU가 DPA의 dma_ring에 descriptor write + valid=1
    │     (src_mmap = src의 chunk_tx_mmap, src_addr = chunk_slot + offset,
    │      dst_mmap = peer dst의 rx_mmap, dst_addr = peer cursor,
    │      size = body_len, flags = CASE_DIRECT)
    └─ hdr_fwd 발사 (현재처럼)

DPA:
  DPU가 쓴 ring descriptor 발견 → 기존 CASE_DIRECT DMA 그대로

dst host (PE thread):
  OP_HDR_BATCH 도착 → process_hdr_batch
  (hdr 안에 body location 정보가 있으므로 body 도착도 곧 보장됨)
  OP_BODY_COMPLETION (DPA→host 완료 신호) 도착 → deliver
                                                  ★ expected/parked 단순화 또는 제거
```

### 2.2 핵심 변경 한 줄

> "host의 dma_ring에 descriptor 쓰는 행위가 DPU의 dma_ring에 descriptor 쓰는 행위로 이동"

DPA 코드는 거의 그대로 (어떤 ring에서 읽냐만 다름). DPU에는 이미 `dpu_enqueue_reverse_dma`로 비슷한 ring write 메커니즘이 있으므로 재사용.

### 2.3 Batching 유지 메커니즘

- host packing 그대로: 1 chunk_tx slot = N body packed.
- hdr_builder가 hdr들을 batch로 묶음 (현재처럼 ~96 entries / hdr_batch).
- DPU가 hdr batch 한 번 받으면 그 안의 hdr들을 일괄 처리:
  - 같은 src의 hdr들은 같은 chunk_tx slot을 가리킬 가능성 큼 (host packing 보장)
  - 그 경우 N hdr → 1 ring descriptor (single DMA로 packed body들 한 번에)
- 즉 **DMA 횟수는 chunk 단위로 그대로** (256B 31 RPC → 1 DMA).

---

## 3. 구체 변경 사항

### 3.1 mesh.h — wire format

```c
// 새 필드 추가
struct mesh_hdr_req {
    struct mesh_req_id req_id;
    char     dst_service[24];
    int32_t  dst_pod_id_hint;
    uint32_t body_size;
    uint64_t trace_id;
    uint64_t span_id;
    uint8_t  flags;
    // ★ 새 필드 — chunk_tx slot에서 body 위치
    int32_t  src_chunk_slot;        // src의 chunk_tx_buffer slot index
    uint32_t src_chunk_offset;      // 그 slot 안에서 body 시작 offset
    uint8_t  reserved[15];          // 23 → 15
} __attribute__((packed));
```

`mesh_hdr_fwd`는 변경 불필요 (dst host에 도착하는 hdr는 src body 위치 정보 필요 없음 — DMA로 직접 dst의 rx_dma_buffer에 도착하니까).

### 3.2 dpumesh_doca.c — host side

**제거**:
- `chunk_builder_*` 함수 전체 (~300 LOC)
- `builder_flush_locked` 내부의 chunk dma_ring submit 부분
- `process_chunk`, `consume_chunk`, `deliver_chunk_to_rxq`, `expected_ring_*`, `park_*` (~400 LOC)
- `dma_ring` 호스트 측 CASE_DIRECT enqueue 코드 (~100 LOC)

**유지/수정**:
- `send_request`: chunk_tx_alloc → memcpy → hdr_builder append (chunk slot 정보 포함)
- `send_response`: 동일 패턴
- `hdr_builder`: 거의 그대로 (hdr 필드만 확장)
- 응답 deliver path: hdr 도착 시점에 즉시 deliver 가능 (body가 hdr와 같이/먼저 도착하는 보장 필요 — §5.3 결정)

### 3.3 dpu_worker.c — DPU 측

**추가**:
- hdr batch 처리 루프 안에서 hdr별로 DPA dma_ring descriptor write
- `dpu_enqueue_body_dma` (새 함수, `dpu_enqueue_reverse_dma`와 형제):
  - 입력: src_pod (src의 chunk_tx_mmap, slot idx + offset), dst_pod (rx_mmap, cursor), size
  - 출력: peer dst의 forward dma_ring에 descriptor 한 줄 write + valid=1
- 같은 src에서 온 연속 hdr들이 같은 chunk_tx_slot 가리키면 합치는 최적화 (옵션, 2단계)

### 3.4 dpa.c — PEER_TOPOLOGY

ADD_PEER 시점에 추가로 공유해야 할 것:
- src의 **chunk_tx_buffer** (host TX body buffer)의 mmap handle을 DPU에 알려야 함 (DPA가 직접 사용은 안 하고 DPU가 descriptor 쓸 때 참고)
- 정확히는 dpa_peer_info 또는 dpu의 peer 테이블에 `host_tx_mmap_handle` + `host_tx_addr` 추가

### 3.5 dpa_kernel.c

- 거의 그대로. ring descriptor 출처가 DPU측 ring으로 바뀜 (이미 reverse path도 같은 패턴이라 호환).
- CASE_DIRECT 처리 로직 변경 없음.

---

## 4. 단계 (각 단계 빌드/측정 통과 가능)

| # | 단계 | 변경 | 검증 |
|---|---|---|---|
| 1 | **mesh.h hdr 확장** | mesh_hdr_req에 새 필드. 사이즈는 80B 유지 (reserved 활용). | 컴파일 통과 |
| 2 | **host에서 새 필드 채우기** | send_request가 src_chunk_slot/offset 정보 hdr에 기록. 아직 사용 안 함. chunk_builder는 그대로 동작. | 기존 K sweep 결과 유지 |
| 3 | **DPU peer 테이블에 src host chunk_tx mmap 추가** | dpa.c ADD_PEER 변경. dpu_worker.c peer 구조 확장. 아직 사용 안 함. | 기존 K sweep 결과 유지 |
| 4 | **DPU side body DMA submit 구현 (옵션, host와 병행)** | dpu_worker.c에 `dpu_enqueue_body_dma` 추가. 처음엔 dual mode (host도 enqueue, DPU도 enqueue) — race 확인용. | 디버그 시 dual race 발견 가능. 정상 후 단일 path로. |
| 5 | **host의 chunk dma_ring enqueue 제거** | builder_flush_locked에서 chunk submit 부분 제거. 이제 DPU 단일 enqueuer. | K sweep, shared 모드 정상 동작 확인 |
| 6 | **dst host 측 단순화** | expected ring/parked chunk 로직 사용 안 함 (또는 제거). hdr 도착 = body 도착 보장 시점 정리. | K sweep 통과 |
| 7 | **양방향**: send_response도 같은 path | 응답 path도 chunk_builder 빼고 hdr only. DPU가 response body DMA 발사. | echo로 ping-pong 정상 |
| 8 | **chunk_builder 등 사용 안 하는 코드 제거** | 죽은 코드 정리. ~500-800 LOC 감소. | 최종 K sweep |
| 9 | **split 모드 재시도** | PE 별도 코어로 핀. orphan timeout 발생 안 해야 함. | split K=2048 측정. shared와 비교. |

---

## 5. 미정 / 결정 포인트

### 5.1 hdr 사이즈 유지 vs 확장

기존 mesh_hdr_req = 80B (24B service + 8B req_id + 등등 + 23B reserved). 새 필드 8B 정도 필요. reserved 활용으로 80B 유지 가능. 별도 hdr v4 type 도입은 불필요.

### 5.2 hdr_builder timeout 영향

현재 `BUILDER_FLUSH_TIMEOUT_US = 1000` (1ms). hdr_builder 단독이 되면 timeout 영향이 양방향 hdr 모두에. K=1 RTT = (src hdr timeout) + DPU forward + DMA + (echo hdr timeout) + DMA + delivery ≈ **2ms 정도**. shared 모드 K=1 4ms의 절반.

### 5.3 ★ body 도착 신호를 dst host에 어떻게 알릴 것인가 ★

가장 까다로운 부분. 옵션:

(a) **DMA completion 메시지 (DPU → dst host)**
- DPU가 body DMA submit 후, DPA 완료를 알게 되면 dst host에 메시지로 알림.
- 현재 reverse path가 비슷한 패턴 (`dmesh_dma_completion_msg`).
- 장점: hdr 도착과 body 도착 별도 신호 → 현재 receive 모델과 호환.
- 단점: 추가 메시지 라운드. PE polling 부담 약간 유지.

(b) **DPU가 hdr forward를 body DMA 완료 후에만 발사**
- DPU가 body DMA submit → 완료 대기 → hdr_fwd 발사.
- dst host는 hdr 도착 시점에 body가 이미 메모리에 있다고 가정 → expected ring 자체 불필요.
- 장점: 가장 깔끔. receive path 코드 대폭 단순화.
- 단점: DPU가 DMA 완료 대기 (몇 μs 추가 latency). DPU 처리 직렬화.

(c) **DPA가 dst host에 직접 신호**
- 현재 `doca_dpa_dev_comch_producer_dma_copy`의 `comp_msg` 파라미터로 immediate data 전송 가능.
- 장점: DPU 거치지 않음, 가장 빠름.
- 단점: dst host의 comch consumer에서 직접 메시지 받아 처리하는 로직 필요.

**추천: (b) 우선 시도**. 가장 깔끔. DPA가 sync 응답 가능한지 확인 필요. 안 되면 (a)로 fallback.

### 5.4 batching window 어디서

- 현재: hdr_builder + chunk_builder 둘 다 host에서 packing.
- 새 구조: hdr_builder만 host에서 packing. body packing(chunk_tx slot)은 그대로.
- 1 hdr_batch flush = 여러 src body의 packed chunk_tx slot들 가리킴. DPU가 hdr batch 처리 시 같은 chunk_tx_slot 가리키는 hdr끼리 묶어서 1 DMA로 처리 → 자연스럽게 batching.

### 5.5 Hard Rule #2 (legacy `BENCH_SPLIT=0`)

옛 legacy path는 단일 closed-loop 호출. Phase 3 split path와 별개. Phase 4 변경은 split path 안에서 일어남. **Legacy `BENCH_SPLIT=0`은 코드상 별 흐름이라 영향 없음**. 단, legacy path가 chunk_builder 의존 안 한다는 점 코드 trace로 확인 필요 (1단계 검증 항목).

---

## 6. Rollback

각 단계가 git commit 단위. 망가지면 직전 commit으로 revert. 7-9단계는 큰 변경이라 backup branch (`phase4-checkpoint-N`)를 권장.

---

## 7. 검증 plan

- **단계 5 완료 후**: shared K sweep — RPS가 v5 측정(450k)보다 같거나 높아야 함. K=1 RTT가 ~2ms로 떨어졌는지.
- **단계 7 완료 후**: 양방향 정상.
- **단계 9 완료 후**: split K sweep — orphan timeout 0, shared 대비 ~2x throughput 또는 K=1 RTT < 1ms.
- **Flame graph 재캡쳐**: 단계 8 이후, host bench-pod의 libdoca % 변화 확인. 줄어들었으면 chunk-side polling 제거 효과.

---

## 8. 미해결 질문 (사용자 결정 필요)

1. **§5.3 body completion 신호 방식** — (a)/(b)/(c) 어느 것?
2. **단계 4의 dual mode 단계 둘지** — 안전하지만 시간 더 소모. 또는 4단계 건너뛰고 5에서 바로 단일 path로 전환.
3. **chunk_builder packing 위치** — host worker thread에서 인라인 vs 별도 packer thread? 현재는 worker thread에서 인라인. 그대로 유지 추천.

---

*Plan 본문 끝 (Phase 4, 2026-05-15)*

---

# 9. 구현 진행 상황 (2026-05-18 update)

## 9.1 사용자 결정 (위 §8 답)

- **§5.3 body 신호**: option (b) "DPU가 body DMA 완료 후 hdr_fwd 발사" 선택. 그러나 코드 분석 결과 수신측 expected_ring/parked 로직을 **유지**하면 option (b)/(a) 차이가 사라짐 → 실제 구현은 expected_ring 유지하고 body+hdr 병렬 발사 (option a 동작이지만 dst 코드 변경 0). 추후 Stage C3에서 expected_ring 제거 검토.
- **dual mode**: 건너뛰고 C2에서 단일 path로 직접 전환 (Stage C1+C2 함께).
- **packing 위치**: worker thread inline 그대로 유지 (변경 없음).

## 9.2 plan 대비 simplification

| plan §3 항목 | plan 의도 | 실제 구현 | 이유 |
|---|---|---|---|
| §3.1 mesh.h hdr 확장 | mesh_hdr_req에 src_chunk_slot/offset 추가 | **불필요 — 변경 안 함** | 1 hdr_batch ↔ 1 chunk_slot pairing이 builder_flush_locked에서 보장됨. sw_descriptor_t의 metadata로만 전파하면 충분. |
| §3.2 process_chunk/expected_ring 제거 | 단순화 | **유지** | option (b) "DPU가 직렬화" 안 하기로 결정. body+hdr 병렬 발사 + race를 expected_ring/parked가 처리. |
| §3.4 DPU peer 테이블에 src chunk_tx mmap 추가 | DPU가 desc 쓸 때 참고 | **불필요 — 기존 핸들 재사용** | DPU는 이미 pod->remote_mmap (= host body buffer) 보유. DPA forward ring의 host_mmap도 이미 동일. |

→ 변경 LOC가 plan 추산 800 → 실제 ~300 LOC로 축소.

## 9.3 각 단계 상태

| # | 단계 | 상태 | 검증 |
|---|---|---|---|
| A1 | sw_descriptor_t 확장 (src_chunk_buf_slot/_len) | ✅ done | host build OK |
| A2 | dma_desc + comch_dma_comp_msg + dpu_comp_entry_t 전파 | ✅ done | DPU build OK |
| A3 | src host chunk_tx mmap 노출 | ✅ no-op (기존 핸들 사용) | — |
| **A 통합** | shared K=1..2048 sweep | ✅ **PASSED** | zero failures, baseline 유지 (K=1: 249RPS/4ms, K=2048: 448k RPS) |
| B1 | DPU-owned per-pod forward ring + buf_arr + ADD_RING | ✅ done | MAX_DPA_RINGS 8→16 |
| B2 | DPA kernel polling | ✅ no-op (기존 process_one_desc 재사용, ADD_RING handler 호환) | — |
| B3 | dpu_enqueue_body_dma 함수 | ✅ done | unused-attr 가드 |
| **B 통합** | shared K=1..2048 sweep | ✅ **PASSED** | zero failures, baseline 유지 |
| C1 | process_forward_entry에서 DPU body DMA fire (body_dma_done/hdr_rev_dma_done flags) | ✅ done | |
| C2 | host builder_flush_locked의 chunk desc enqueue 제거 | ✅ done | pending_attach_tx_v2는 유지 (TX_ACK가 chunk_slot free) |
| **C1+C2 1차 시도** | shared K-sweep | ❌ **K=1..256 OK, K=1024+ HANG** | 아래 §9.4 |
| C-fix | deferred_body_dma 큐 추가 (FIFO 데드락 해결) | ✅ code done | 검증됨 (아래) |
| **C-fix 검증** | shared K-sweep (2026-05-18) | ❌ **K=1..256 OK, K=512 HANG** | 새 failure mode — 아래 §10 |
| C3 | dst host expected_ring 제거 | ⏸ 재설계 필요 | §10 design 논의 참조 |
| **C 통합** | shared K-sweep | ❌ 미통과 | C3 (또는 등가 redesign) 필요 |
| D | 양방향 (이미 echo 응답 path 포함됨), cleanup, split mode | ⏳ 미진행 | C 완료 전 진행 불가 |

## 9.4 K=1024+ HANG 분석 (Stage C 1차 시도)

증상:
- shared K=1..256: zero failures, baseline RPS/latency 유지
- shared K=1024, 2048: bench daemon 응답 timeout (>40s)
- bench host log: `orphan expected timeout src=10 seq=4495843...` 다발
- bench stat: `pend=1023, p_chk=0, p_hdr=0` → bench가 echo로부터 chunk/hdr를 전혀 받지 못함
- echo stat: `c_dir=13k/sec, p_chk=13k/sec, park=13k/sec` → echo는 정상 처리 + 응답 발사 중

Root cause: **comp_queue FIFO blocking deadlock**

```
1. K=1024+ saturation
2. DPA의 CASE_DIRECT admission gate가 dst rx 크레딧 고갈로 defer
3. DPU의 dpu_fwd_ring 가득 → dpu_enqueue_body_dma → AGAIN
4. process_forward_entry가 return 0 → comp_queue front entry stuck
5. 뒤에 줄 서 있는 CASE_DIRECT comp_entries (DMA_COMPLETION 보내야 할
   = dst의 process_chunk가 fire 되도록 해야 할) 도 FIFO로 함께 블락
6. dst host의 process_chunk 안 fire → rx_free 안 일어남 → 크레딧 반환 X
7. DPA admission 영원히 defer → 2번으로 되돌아감
```

Stage A/B는 host가 직접 chunk_dma_ring에 enqueue → host 측 path여서 DPU comp_queue 블락 없음 → 데드락 없었음.

## 9.5 Fix (코드 작성 완료, 검증 대기)

`deferred_body_dma_t` 큐를 DPU objs에 추가. process_forward_entry의 body DMA fire가 AGAIN 받으면 큐에 push하고 **return 1**로 comp_queue 진행 허용. main loop에서 `drain_deferred_body_dmas` 호출 (drain_deferred_tx_acks와 동일 패턴).

변경 파일:
- `lib/cpp/src/thrift/transport/doca/object.h`: `deferred_body_dma_t`, MAX_DEFERRED_BODY_DMA=8192, objs에 큐 추가
- `lib/cpp/src/thrift/transport/doca/dpu_worker.c`: process_forward_entry의 body DMA AGAIN 경로 변경, `drain_deferred_body_dmas` 함수 추가, main loop에서 호출, init 추가

Host build OK 확인됨. DPU build + deploy + K-sweep 검증 필요.

## 9.6 (해소됨) 이전 deploy blocker

이전 update에서 기록된 `./test-bench.sh deploy`의 TCP image import 실패는 **2026-05-18 재실행 시 발생하지 않음**. transient sudo cache 또는 환경 이슈였던 듯. 이번 deploy는 모든 단계 통과 (DPU register grep timeout warning은 `-l 30` ERROR-only 모드라 spurious).

## 9.7 다음 step — 재설계 필요

C-fix 검증이 K=512에서 fail (§10 상세). 단순 deadlock fix만으론 부족. 재설계 방향은 §10.4 design 논의를 따름. 다음 결정 필요:

1. dst 매칭 자료구조 재설계 (expected_ring/parked → hash-based per-(src,seq))
2. 또는 host→DPU 인터페이스 변경 (hdr/body 독립 flushing, descriptor에 body 주소 array)
3. 또는 둘 다

C 통과 전 D는 진행 안 함.

## 9.8 검증 이력

| 단계 | 시각 | 결과 |
|---|---|---|
| Stage A K-sweep (256B, K=1..2048) | 2026-05-15 | ✅ zero failures, baseline preserved (K=1:249RPS, K=2048:448k RPS) |
| Stage B K-sweep | 2026-05-15 | ✅ zero failures, baseline preserved (K=1:248RPS, K=2048:449k RPS) |
| Stage C 1차 K-sweep | 2026-05-15 | ⚠ K=1..256 OK (K=256:158k RPS), K=1024+ HANG (deadlock) |
| **Stage C-fix K-sweep** | **2026-05-18** | ❌ **K=1..256 OK (K=256:161k RPS), K=512 HANG** (새 mode, §10) |

---

# 10. Stage C-fix 검증 결과 + 재설계 논의 (2026-05-18 session)

## 10.1 K-sweep 결과

| K | RPS | p50 | p99 | fail | 상태 |
|---|---:|---:|---:|---:|---|
| 1 | 249 | 4004 | 4010 | 0 | OK (= Stage A/B baseline) |
| 2 | 499 | 4004 | 4010 | 0 | OK |
| 4 | 998 | 4004 | 4010 | 0 | OK |
| 8 | 1997 | 4004 | 4011 | 0 | OK |
| 16 | 3993 | 4004 | 4012 | 0 | OK |
| 32 | 7991 | 4004 | 4012 | 0 | OK |
| 64 | **29719** | **2008** | 3015 | 0 | OK, latency **2ms로 떨어짐** (batching saturation 효과) |
| 128 | 53873 | 2116 | 3820 | 0 | OK |
| 256 | 161128 | 1724 | 1932 | 0 | OK (vs Stage C 1차 158k 거의 동일) |
| **512** | — | — | — | — | **HANG** (40s timeout) |
| 1024, 2048 | — | — | — | — | 미실행 (K=512 fail로 stop) |

**K=1 RTT**: 4004us — plan §5.2 예측 "hdr_builder 단독 → ~2ms" 미달성. 단, K=64+ saturation 구간에서는 ~2ms로 떨어짐.

## 10.2 K=512 hang 증상

- pods 모두 Running, no restart
- DPU log: ERROR 0건 (`-l 30` 모드)
- **bench-dpumesh stat**:
  ```
  send=53755 rxfree=53243 ... pend=512  ← 1초차: 정상 (53k 처리)
  send=1     ... pend=512                ← 2초차: 거의 정지
  send=0     ... pend=512→511→500 ...    ← 12초+ 동결, pend가 2초 reclaim으로 1/sec drop
  ```
- **echo-dpumesh log**:
  - `orphan expected timeout src=10 seq=...` (수십~수백 entries) — hdr만 도착, body 매칭 안됨
  - `parked chunk timeout src=10 first_seq=... num=31` — chunk만 도착, hdr 매칭 안됨
  - **두 timeout이 동시에 다발** → hdr/body 매칭이 양방향으로 깨짐

## 10.3 Stage C 1차 vs C-fix 비교

| | Stage C 1차 | Stage C-fix |
|---|---|---|
| Fail 시작점 | K=1024 | K=512 (악화) |
| 증상 | bench pend=1023 stuck, echo 정상 처리 중 | bench pend=512 stuck, echo 양방향 timeout |
| Root cause | comp_queue FIFO deadlock | hdr/body reordering at dst |
| 일관성 | echo가 chunk/hdr를 정상 처리 | echo 매칭 자체가 깨짐 |

C-fix가 comp_queue deadlock은 해결했지만, defer 도입으로 **body가 hdr 대비 늦게 도착하는 race가 심화**되어 dst의 1초 sweep timeout window를 깨고 cascade.

## 10.4 Design 논의 (재설계 방향 합의)

### 10.4.1 expected_ring 본질

`expected_ring` (CAP=1024, FIFO)은 hdr_rx_buffer에 이미 있는 hdr 데이터를 parsing해서 **복사**한 큐. arrived_us(timestamp) 외엔 새 정보 없음. hdr_rx_buffer가 wrap cursor라 직접 참조 불가해서 복사 보관.

문제:
- 인공적 capacity 한계 (1024) — overflow 시 `expected_ring_push` line 2050 **silent drop**
- FIFO matching — chunk가 expected head와 first_seq 매칭해야 deliver. 도착 순서 race에 취약
- sweep 1초 timeout — 매칭 지연 1초 넘으면 entry drop (cascade trigger)

### 10.4.2 사용자 제안: dst 책임 매칭 + decoupled flushing

**핵심 idea**:
1. host는 hdr와 body를 **독립적으로** flush (현재 `builder_append_locked`의 1:1 강제 invariant 제거)
2. hdr DMA 시 descriptor에 **body 주소 array** (그 hdr_batch의 N개 hdr에 해당하는 body들의 src_chunk_buf slot/offset/len 모두) 포함
3. DPU가 hdr_batch 받으면 descriptor의 body 주소 array를 읽어 **N개 body DMA를 각각 발사**
4. dst는 도착 순서 무관 hash 매칭 — race 있어도 dst-side에서 handle

근거:
- hdr ≤ 80B, body 1KB+ → hdr packing은 96/batch, body는 8/batch. 1:1 강제는 hdr slot 활용율 80%+ 낭비
- dst가 chunk body 자르는 데 이미 **hdr.body_size를 cursor로 사용** (line 2217-2231) → body size 가변 처리 이미 가능
- 도착 순서 보장은 race + 추가 메커니즘 비용. dst-side 매칭으로 우회

### 10.4.3 사용자 제안 design에 대한 짚을 점

1. **chunk_tx slot 수명 관리** ⚠ 가장 큰 변경
   - 1:1 폐기 시 한 chunk_tx slot의 bodies가 여러 hdr_batch에 걸쳐 참조됨
   - 현재 TX_ACK 1번 = chunk_slot 통째 free (1:1 가정)
   - 변경 후: per-body TX_ACK 또는 host side slot refcount 필요

2. **DPU side burst** (여전히 valid)
   - 1 hdr_batch → 최대 96 body DMA enqueue → dpu_fwd_ring 채워질 수 있음
   - C-fix 같은 deferred 메커니즘 또는 host backpressure 여전히 필요

3. **dst side 양쪽 큐 여전히 필요**
   - hdr 먼저 도착할 수도, body 먼저 도착할 수도 (race 무시 안 함, dst가 handle)
   - 자료구조 자체는 expected_ring/parked 폐기해도 OK, but hash 두 개 (hdr-side, body-side) **둘 다** 필요
   - "한쪽 큐만 있으면 충분"은 race 무시한 것

4. **host bookkeeping ↑**
   - hdr append 시점에 그 RPC의 body가 어느 (slot, offset)에 있는지 per-RPC 기록
   - hdr_batch flush 시 그 array를 descriptor에 채움

5. **descriptor 크기 ↑**
   - (slot, offset, len) × 96 ≈ 1.5KB metadata per hdr_batch
   - hdr_batch payload prefix로 가든 별도 small DMA로 가든 wire 추가 트래픽

### 10.4.4 정리 — 합의된 방향 (high level)

- **dst 자료구조**: expected_ring/parked 폐기, hash-based (src, seq) 매칭 두 개 (hdr-side, body-side)
- **chunk slicing**: hdr.body_size 그대로 사용 (이미 그렇게 동작)
- **host builder**: hdr/body 독립 flush, per-RPC body location 기록, hdr_batch descriptor에 body 주소 array
- **DPU**: hdr_batch 받으면 N body DMA 발사, defer 메커니즘 유지
- **chunk_tx slot 수명**: per-body TX_ACK 또는 slot refcount (TBD)

추정 변경 규모: 400-600 LOC across host (`dpumesh_doca.c`), DPU (`dpu_worker.c`), 그리고 wire format (`mesh.h`의 mesh_hdr_req 또는 batch descriptor).

## 10.5 다음 step

1. **상세 설계**: §10.4.3의 5가지 issue 각각 해결 방안 명세
2. **구현 순서 결정**: 한 번에 다 vs 단계별 (capacity bump + hash 매칭만 먼저 → 추후 host decoupling)
3. **검증 plan**: K=512 통과 후 → K=1024, K=2048 → body size sweep (256B / 1KB / 4KB)

---

*문서 업데이트 (2026-05-18 session 종료) — Stage C-fix 검증 후 K=512 hang 발견 → 재설계 논의 진행*
