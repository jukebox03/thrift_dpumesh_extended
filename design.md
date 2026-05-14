# DPUmesh — Control/Data Plane Split 설계

> 작성일: 2026-05-12
> Phase 4a (single-RPC L7 inline path) revert 후, control plane(header)와 data plane(body)을
> 분리한 새 아키텍처. Plan §2.1의 target state로 직진.

---

## 1. 핵심 아이디어

**Header는 DPU로, Body는 src→dst 직접.** L7 처리(resolve, policy, tracing)는 DPU의 작은
header path에서만 수행. Body는 8KB chunk에 여러 RPC를 packing해서 dst로 직접 DMA.

이 분리가 주는 것:
- **slot 크기 고정 자체 제거** — wire_len이 slot_size와 일치할 때의 ALIGN_UP_128 cross-slot
  버그(8128B edge case) 같은 boundary cliff가 구조적으로 사라짐
- **공간 효율** — 256B body 30+개를 한 8KB DMA에 packing
- **DPU CPU off bulk path** — L7은 작은 header path 위에만 있고, bulk 데이터가 ARM을 거치지
  않음 → DPU CPU saturation으로 인한 throughput cliff 회피
- **Service 단 추상화 일치** — service는 header와 message buffer를 분리해서 다룬다는 사용자
  요구사항과 wire format이 1:1 매칭

---

## 2. 사용자 결정 사항 (확정)

| # | 결정 | 비고 |
|---|---|---|
| 1 | **req_id는 src가 생성** | DPU는 (src_pod_id, req_id) 튜플 검증/명명만. extra RTT 없음. |
| 2 | **Speculative body send** | Body chunk가 hdr_fwd보다 먼저 dst에 도착할 수 있음. dst는 양쪽 도착할 때까지 park. |
| 3 | **Body > 8KB는 fail** | 이번 단계 범위 밖. Phase 6 (host→host P2P)에서 재설계. |
| 4 | **별도 header DMA channel** | comch가 아닌 dedicated header ring + DMA. comch 폭주 회피. |

---

## 3. 데이터 흐름

```
                ┌──────────── DPU (Control Plane) ────────────┐
                │  header batch 파싱 (N개)                      │
                │  per-header: mesh_resolve(service) → dst_pod │
                │  per-header: policy / tracing / metric       │
                │  dst별로 outbound hdr batch에 누적             │
                │  full or timeout → flush to dst              │
                └────▲──────────────────────────┬─────────────┘
                     │ hdr_batch                │ hdr_batch
                     │   (src → DPU hdr ring,   │   (DPU → dst hdr ring,
                     │    N headers packed)     │    N headers packed)
                     │                          ▼
       ┌──── Src ────┴────┐         ┌──── Dst ────────────┐
       │ hdr_builder      │         │ pending_inbound[rid] │
       │  - N hdr 누적     │  body   │   .has_hdr / .has_body
       │  - flush trigger │  chunk  │   .size / .src_pod   │
       │                  ├────────►│ chunk parser:        │
       │ chunk_builder    │  (src → │   meta → bodies      │
       │  - dst별 누적     │   dst   │ hdr batch parser:    │
       │  - first_rid +   │   body  │   N headers          │
       │    size list     │   ring) │ → match req_id       │
       │  - flush trigger │         │ → deliver to app     │
       └──────────────────┘         └──────────────────────┘

세 가지 batching 동시 운용:
  (1) Src hdr_batch  ── DPU 행
  (2) Src body_chunk ── Dst 직행
  (3) DPU outbound hdr_batch ── Dst 행 (per-dst 누적)
```

---

## 4. Wire 구조

### 4.1 Single header entry (src→DPU 방향)

```c
struct mesh_hdr_req {           /* 64 B packed (padded for alignment) */
    uint32_t req_id;            /* src 발급 — (src_pod_id, req_id) 튜플 unique */
    int32_t  src_pod_id;
    char     dst_service[24];   /* NUL-terminated within 24B */
    uint32_t body_size;         /* dst가 buffer 예약하는 단위 */
    uint64_t trace_id;          /* 옵션, 0 가능 */
    uint64_t span_id;
    uint8_t  flags;             /* OP_REQUEST | OP_RESPONSE */
    uint8_t  reserved[7];       /* pad to 64 B */
} __attribute__((packed));

_Static_assert(sizeof(struct mesh_hdr_req) == 64, "hdr_req ABI");
```

### 4.2 Single header entry (DPU→dst 방향)

```c
struct mesh_hdr_fwd {           /* 64 B packed (same size as req for ring symmetry) */
    uint32_t req_id;
    int32_t  src_pod_id;
    uint32_t body_size;
    uint64_t trace_id;
    uint64_t span_id;
    uint8_t  flags;
    uint8_t  status;            /* 0=OK, 1=DROP (DPU policy fail) */
    uint8_t  reserved[6];       /* pad to 64 B */
} __attribute__((packed));

_Static_assert(sizeof(struct mesh_hdr_fwd) == 64, "hdr_fwd ABI");
```

### 4.3 Header batch layout (양방향 공통)

```
+---------------+---------------+---------------+ ... +---------------+
| hdr_entry 0   | hdr_entry 1   | hdr_entry 2   |     | hdr_entry N-1 |
| (64 B)        | (64 B)        | (64 B)        |     | (64 B)        |
+---------------+---------------+---------------+ ... +---------------+
↑ MESH_HDR_BATCH_SIZE = 4096 B = 64 entries max
DMA size 가변: (실제 N × 64), N은 1..64
```

- N = 0인 batch는 보내지 않음 (의미 없음)
- Receiver는 `dma_size / 64`로 N 계산 → 첫 entry부터 순서대로 파싱
- ALIGN_UP_128 적용: N=1일 때 padded 128B, N=2일 때 padded 128B, N=3일 때 padded 192B, ...
  → wire_len 항상 128B 배수보다 작은 padded값으로 안전. cross-slot 위험 없음 (slot 4KB).

**상수 (확정)**:
- `MESH_HDR_ENTRY_SIZE = 64`
- `MESH_HDR_BATCH_MAX_ENTRIES = 128`
- `MESH_HDR_BATCH_SIZE = 8192` (= 128 × 64)

> **DMA 비용은 size가 아니라 count로 결정됨** (DPA dma_copy ≤ 8KB는 per-call 비용 고정).
> 4KB로 절반만 채우면 같은 throughput에 DMA call 횟수 2배 → 8KB까지 채워 보내야 효율적.
> 이 원칙은 header/body 양쪽 모두에 적용 (body chunk도 8KB 그대로).

### 4.4 Body chunk (src → dst, dedicated DMA ring "body")

```c
/* Chunk 첫 부분: 메타데이터 */
struct mesh_chunk_meta {        /* 가변 길이 */
    int32_t  src_pod_id;
    uint16_t num_bodies;        /* 1..MESH_CHUNK_MAX_BODIES (제안: 16) */
    uint16_t _pad;
    /* 이어서 num_bodies × { uint32_t req_id; uint32_t size } */
};

/* 메타 끝 = 8 + num_bodies × 8 bytes
 * 이후: 각 body가 메타의 entry 순서대로 concatenate, 패딩 없음.
 *
 * Chunk total = sizeof(meta) + Σ body_size ≤ MESH_CHUNK_SIZE (8 KB).
 */
```

**MESH_CHUNK_SIZE = 8192**, **MESH_CHUNK_MAX_BODIES = 16** 제안:
- 메타 최대 = 8 + 16×8 = 136 B
- Body payload 영역 = 8192 - 136 = 8056 B
- 가장 짧은 body 1B 가정 시 chunk당 16 RPC packing (메타 한계로 cap)
- 1 RPC × 8056 B 까지 single chunk inline 가능 → Phase 4 inline 한계

### 4.5 Slot vs DMA size 관계

- Header ring slot: **MESH_HDR_BATCH_SIZE = 4096 B** (batch 단위로 1 slot)
- Body ring slot: **MESH_CHUNK_SIZE = 8192 B** (chunk 단위로 1 slot)
- 두 ring 모두 실제 DMA size는 가변 (batch 안 N개 또는 chunk 안 가변 body)이지만 slot 자체는
  full 크기로 확보 → ALIGN_UP_128이 slot_size를 넘는 일 자체가 없음 (boundary cliff 회피)

---

## 5. Components

### 5.1 Src 측 state

```c
struct dpumesh_ctx {
    /* 기존 (Phase 1-3) */
    ...

    /* Header TX path (header batching to DPU) */
    void           *hdr_tx_buffer;        /* HDR_TX_NUM_SLOTS × MESH_HDR_BATCH_SIZE */
    uint8_t        *hdr_tx_bitmap;
    struct dma_ring *hdr_tx_ring;

    struct hdr_builder {
        int       slot_idx;               /* hdr_tx_buffer 안의 slot 인덱스 */
        uint8_t  *buf;                    /* slot_idx → buffer 포인터 */
        int       num_headers;            /* 0..MESH_HDR_BATCH_MAX_ENTRIES */
        uint64_t  first_hdr_us;           /* timeout flush trigger */
    } cur_hdr_batch;                       /* 전체 src에 1개 (DPU는 single dst) */
    pthread_mutex_t hdr_batch_lock;

    /* Body chunk packing state (dst별 누적) */
    struct chunk_builder {
        int       slot_idx;
        uint8_t  *buf;
        size_t    body_off;               /* 다음 body 쓸 위치 (메타 영역 뒤) */
        int       num_bodies;
        struct {
            uint32_t req_id;
            uint32_t size;
            int      tx_slot;             /* original body source slot (flush 후 free) */
        } entries[MESH_CHUNK_MAX_BODIES];
        uint64_t  first_body_us;
        int32_t   dst_pod_id;
    } *cur_chunk_by_dst[MAX_PODS];
    pthread_mutex_t chunk_lock;
};
```

- **Header batch는 single (DPU 단일 target)**: src는 하나의 DPU에만 header를 보내므로 dst별 분리
  불필요. lock contention 최소화.
- **Body chunk는 dst별 누적**: 한 chunk는 한 dst로만 가므로 dst 개수만큼 builder 유지.
- Flush trigger:
  - **Header**: full(64 entries) OR 첫 hdr 후 50µs (작은 데이터라 latency 우선)
  - **Body**: full(8KB) OR 첫 body 후 100µs OR explicit flush

### 5.2 Dst 측 state

```c
struct dpumesh_ctx {
    /* 기존 */
    ...

    /* Header RX path */
    void           *hdr_rx_buffer;     /* HDR_NUM_SLOTS × 64B */

    /* Body chunk RX path */
    void           *body_rx_buffer;    /* CHUNK_NUM_SLOTS × 8KB */

    /* Inbound pending: hdr + body 도착 짝짓기 */
    struct pending_inbound {
        volatile int has_hdr;
        volatile int has_body;
        uint32_t size;
        int32_t  src_pod_id;
        uint8_t *body_ptr;             /* body_rx_buffer 안의 위치 */
        uint8_t  flags;
        uint64_t parked_us;            /* speculative 짝매칭 timeout */
    } pending_inbound[MAX_INBOUND];   /* req_id % MAX_INBOUND 인덱싱 */
    pthread_mutex_t inbound_lock[MAX_INBOUND_LOCK_STRIPES];
};
```

**Reconciliation logic** (dst side, header 또는 body 도착 callback에서):
```c
on header_arrival(hdr):
    slot = hdr.req_id % MAX_INBOUND
    lock(slot)
    pi = &table[slot]
    pi->has_hdr = 1
    pi->size = hdr.body_size
    pi->src_pod_id = hdr.src_pod_id
    pi->flags = hdr.flags
    if pi->has_body:
        deliver(pi)
        clear(pi)
    unlock(slot)

on chunk_arrival(chunk):
    meta = parse(chunk)
    body_cursor = chunk + sizeof(meta) + meta.num_bodies * 8
    for entry in meta.entries:
        slot = entry.req_id % MAX_INBOUND
        lock(slot)
        pi = &table[slot]
        pi->has_body = 1
        pi->body_ptr = body_cursor          /* zero-copy ref into body_rx_buffer */
        if pi->has_hdr:
            deliver(pi)
            clear(pi)
        unlock(slot)
        body_cursor += entry.size

timeout sweep (periodic):
    for slot in table:
        if (now - pi->parked_us) > INBOUND_TIMEOUT_US:
            drop(pi)
            clear(pi)
```

### 5.3 DPU 측 (Control plane)

```c
struct dpu_outbound_hdr_batch {
    uint8_t   buf[MESH_HDR_BATCH_SIZE];   /* 4 KB staging */
    int       num_headers;
    int32_t   dst_pod_id;
    uint64_t  first_hdr_us;
};

struct pod_state {
    /* 기존 */
    ...
    struct dpu_outbound_hdr_batch hdr_outbound;  /* dst → batch 누적 */
};
```

**처리 흐름**:
1. Src로부터 hdr_batch 도착 (DMA, N entries)
   - ARM cache invalidate (N × 64B 영역)
   - Loop: 각 entry 파싱 → mesh_resolve(dst_service) → dst_pod
   - 각 entry를 dst_pod의 `hdr_outbound.buf`에 append (mesh_hdr_fwd 형식으로 변환)
   - dst_pod 다른 entry는 다른 outbound batch로
2. Outbound flush trigger:
   - `num_headers == MESH_HDR_BATCH_MAX_ENTRIES` (full)
   - `now - first_hdr_us > 50 µs`
   - 또는 DPU main loop의 periodic tick
3. Outbound DMA: dst pod의 hdr_rx ring으로 (DPU→host reverse DMA path 재사용)

**Body path는 DPU 거치지 않음** — DPU는 등록 시 (또는 첫 hdr 도착 시)
src와 dst 양쪽에 peer mmap handle을 push. 이후 src의 body chunk DMA는 DPU staging을 경유하지
않고 직접 dst host buffer로 들어감 (Plan §6 Phase 6 메커니즘과 동일).

---

## 6. Phase 분할 (구현 순서)

### Phase A: Header batch path (2일)
- `mesh_hdr_req` / `mesh_hdr_fwd` / batch layout struct 정의
- Src 측 `hdr_builder` + dedicated header DMA ring + buffer pool
- Dst 측 header RX buffer + batch parser
- DPU 측 control path:
  - Inbound: hdr_batch 수신 → loop으로 각 entry resolve → dst별 outbound batch에 push
  - Outbound: full/timeout에 dst 측 header rx ring으로 batch DMA
- Bench 검증: header batch flow 단독 (body 없이 hdr만 보냈을 때 sender→DPU→receiver 도착)

### Phase B: Body chunk path (2일)
- Src 측 `chunk_builder` 구현 (dst별 누적, flush trigger)
- Src→Dst direct body DMA ring 셋업 (DPU가 peer mmap broadcast)
- Dst 측 chunk parser + body_rx_buffer
- Bench 검증: body chunk 단독 (sender→receiver 직접 도달)

### Phase C: Reconciliation & end-to-end (1.5일)
- Dst 측 `pending_inbound[]` table + lock striping
- Header/body 양쪽 도착 시 짝매칭 → 앱 deliver
- Speculative ordering 처리 (timeout sweep)
- `dpumesh_send_split(service, body, body_len, &req_id)` 공개 API
- Bench/echo 새 API로 전환 → end-to-end single RPC, low load, back-to-back stability

### Phase D: Throughput sweep & tuning (1일)
- 1k/5k/10k/20k/40k × 다양한 body size
- header/body flush timeout 튜닝 (50µs, 100µs 시작점)
- chunk packing 효율 측정 (256B body 시 batch당 RPC 수)
- DPU log 폭주 방지 검증

**Total: 6.5 일 추정**

---

## 7. Open Questions (구현 단계에서 결정)

1. **DPU → host peer mmap push 메커니즘**: Plan §2.1 언급된 peer topology push 구현 방식.
   기존 control comch 재사용 vs 새 메시지 타입. 현재 DPU가 pod register 받을 때 자동 broadcast로
   하는 게 자연스러움.
2. **MESH_CHUNK_MAX_BODIES**: 16 적정? 늘리면 메타 오버헤드 ↑, 줄이면 small RPC packing 효율 ↓
3. **MESH_HDR_BATCH_MAX_ENTRIES**: 64 (= 4KB / 64B entry)
4. **Flush timeout 초기값**: header 50µs, body 100µs. 측정 후 조정
5. **MAX_INBOUND lock stripes**: lock contention vs 메모리. 64 stripes 제안
6. **응답 path 처리**: echo (또는 일반 server)가 응답 보낼 때도 src→DPU→dst 같은 split. echo는
   request 받자마자 hdr_builder로 응답 header push + chunk_builder로 body push.
7. **OP_REQUEST vs OP_RESPONSE 처리**: 같은 hdr_batch에 섞일 수 있는지 (effort: src가 같은
   pod로 가는 req/resp를 같은 builder에 누적 가능)
8. **DPU의 src→DPU header ring 셋업**: 기존 dma_ring은 host→DPU body용 (Phase 1-3). header용
   은 별도 ring 또는 multiplexed ring. 별도 ring이 깔끔.

---

## 8. 폐기 / 보류된 설계 요소

- **Phase 4a OP_BATCH inline path (이전 시도)**: revert됨. 새 설계가 superset.
- **Phase 4b 8-slot fixed packing (plan §6)**: 새 설계의 가변 packing이 더 유연. plan 갱신 필요.
- **mesh_hdr 64B struct (Phase 3 인프라)**: 새 설계의 hdr_req와 별개. 기존 struct는 deprecated.
  Phase 3 인프라(hdr_tx_buffer/hdr_rx_buffer)는 새 설계의 header ring backing memory로 재사용.

---

*문서 끝*
