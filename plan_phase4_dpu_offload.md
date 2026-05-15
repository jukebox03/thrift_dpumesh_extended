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

*문서 끝 (Phase 4, 2026-05-15) — 의논용 draft, 아직 구현 안 함*
