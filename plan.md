# DPUmesh Implementation Plan — Header/Body Pipeline Split (v2)

> 마지막 업데이트: 2026-05-14 (multi-dst / multi-src 결함 정정, event-driven park/wake 도입)
>
> 이 문서는 자기-완결적이다. 이전 plan.md(2026-05-13 작성)는 폐기. 정정된 결정사항만 이 문서가 권위.
>
> **핵심 원칙 (절대 깨지 말 것)**:
> 1. Header pipeline과 Body pipeline은 buffer / pool / lifecycle이 완전 독립
> 2. req_id는 `(src_id, seq)` 페어로 globally unique (multi-src 환경 가정)
> 3. dst는 expected를 src_id별 ring buffer로 demux 보관. chunk 도착은 event로 처리, spin 없음

---

## 0. TL;DR

**무엇을 만드는가**: Thrift application 아래에 깔리는 DPU 가속 mesh layer. App 코드는 변경 없음.
Mesh layer가 header와 body를 분리해서 운반:
- **Header path** (control plane): src → DPU → dst. DPU가 resolve / policy / tracing.
- **Body path** (data plane, 최종): src → dst **직접** (DPU 우회). chunk에 여러 RPC body packing.

**v1.0.0 (legacy) 기준점**: 1 RPC = 1 DMA, 40k+ RPS p99 ~8ms, back-to-back 안정. 모든 trafic이
host→DPU→dst (DPU CPU 통과). Mesh feature 0개.

**v2 설계 결정사항 (이번 세션 확정)**:
| 항목 | 값 | 이유 |
|---|---|---|
| `req_id` 인코딩 | 64bit `{u32 src_id; u32 seq;}` | seq wrap 없음 (~40k RPS × 일 단위 OK), multi-src globally unique |
| chunk header | 12B `[src_id 4, first_seq 4, num 4]` | dst가 src별 demux된 queue로 packing 보존하면서도 메타 최소 |
| dst의 expected | per-src ring buffer, cap=1024 | alloc 없음. cap 초과 시 backpressure |
| Parked chunks | rx_dma_buffer slot 점유 + metadata만 | zero-copy. flow control과 함께 슬롯 관리 |
| Timeout (parked / orphan expected) | 1초 통일 | wait_response 5초가 backstop |
| Ordering 처리 | event-driven park/wake (spin 없음) | hdr arrival event가 parked chunk 깨움 |

**Multi-dst 처리의 핵심**: src counter는 1개 (atomic), dst별 chunk_builder가 src→이 dst로 가는 호출
순서대로만 body 누적. dst는 expected[src_id]를 ordered queue로 관리. DPU의 dst별 outbound staging이
strict FIFO니까 dst가 받는 hdr 순서 = src의 dst-필터링된 호출 순서 = chunk의 body 순서. 듬성한 seq가
와도 dst의 queue 안에선 빽빽함.

**현재 코드 상태 (2026-05-13)**:
- Phase A (header batch infra, hdr_builder, OP_HDR_BATCH parser/forwarder) 구현 완료. 단, hdr_builder가
  body의 TX 풀(`dma_buffer`)에서 slot을 할당 → **풀 경쟁**.
- Phase B (body chunk packing, OP_CHUNK 분기) 구현 완료. 단:
  - chunk format이 옛 over-engineered version (`meta(src_pod_id, num_bodies) + entries[N]{req_id, size} + bodies`)
  - body chunk가 **DPU staging을 경유** (host→DPU→dst)
- 결과: 1000-2000 RPS 이하에서만 안정. back-to-back 시 state 누적으로 더 낮은 RPS에서도 cliff.

**해야 할 일 (4 phase)**:
- **Phase 1**: hdr 전용 TX pool (DOCA mmap, DPA handle), forward DMA desc.mmap override, TX_ACK pool_type.
- **Phase 2**: hdr 전용 RX buffer, reverse DMA desc.dst_mmap override.
- **Phase 3**: chunk format 12B로 정리 + per-src expected ring queue + event-driven park/wake + 1s timeout.
- **Phase 4**: peer topology push, host→host direct DMA, per-(src,dst) partition flow control, ARM-mediated TX_ACK.

각 phase 독립 검증. Phase 1만 끝나도 5k+ RPS back-to-back. Phase 4까지 가야 throughput 회복.

---

## 1. 변경 불가 원칙 (Hard Rules)

| # | 원칙 | 검증 방법 |
|---|---|---|
| **1** | 기존 Thrift 서비스 코드 (UniqueIdService.cpp 등) 변경 0줄 | `grep` 으로 외부 service 코드 diff 0 |
| **2** | Bench legacy baseline 회귀 0 | `BENCH_SPLIT=0`로 40k @ 10s @ 8192B → p99 ≤ 9ms, 0 fail (v1.0.0=`bf688d525` 부근) |
| **3** | Back-to-back 안정성 | 같은 sweep 2회 연속 → 동일 결과 (slot leak 없음 — `feedback_no_slot_leak`) |
| **4** | Deploy 명령어 한 줄로 | `./test-bench.sh deploy` 1회. Manual DPU restart 금지 |
| **5** | DPU log 폭주 방지 | 어떤 phase의 어떤 bug 상태에서도 DPU log < 10MB. Rate-limited diagnostic 필수 |
| **6** | Header pool / Body pool 독립 | 한 pool 고갈이 다른 pool 진행을 블록하지 않음 |
| **7** | req_id는 (src_id, seq) — globally unique | multi-src 충돌 0. wire / API 모두 64bit |
| **8** | Spin 없음. Event-driven park/wake | hdr/chunk arrival event 외엔 dst CPU가 polling 안 함 |

---

## 2. Architecture

### 2.1 두 파이프라인 — 완전 분리

```
              ┌────────────────── DPU (Control plane) ──────────────────┐
              │                                                          │
              │   ┌────────────────┐         ┌────────────────────────┐ │
              │   │ hdr_batch 수신  │ ──────► │ per-dst hdr_outbound   │ │
              │   │ (forward DMA)   │ resolve │ FIFO staging (8KB×N)   │ │
              │   │ - cache inv     │ ─────►  │                        │ │
              │   │ - parse entries │         │ 8KB full or 50µs t/o   │ │
              │   │   per dst       │         │ → reverse DMA to dst    │ │
              │   └────────────────┘         └────────────────────────┘ │
              └──────▲─────────────────────────────────┬─────────────────┘
                     │ HDR forward DMA                 │ HDR reverse DMA
                     │ (src→DPU)                       │ (DPU→dst)
                     │ desc.mmap = hdr_tx_dpa          │ desc.dst_mmap = host_hdr_rx_dpa
                     │ desc.pool_type=POOL_HOST_TX_HDR │ 
                     │                                 ▼
   ┌──── Src Pod ────┴────┐                  ┌──── Dst Pod ───────────┐
   │ ┌──hdr_builder──────┐ │                  │ hdr_rx_buffer (8MB)    │
   │ │ accumulate hdr    │ │                  │ ┌── expected[src]──────┐│
   │ │ entries (64B each)│ │                  │ │ ring buffer per src  ││
   │ │ flush as 8KB DMA  │ │                  │ │ enqueue on hdr arrive││
   │ │ from hdr_tx_buf   │ │                  │ │ pop on chunk consume ││
   │ └───────────────────┘ │                  │ └──────────────────────┘│
   │ ┌──chunk_builder[N]─┐ │                  │ ┌── parked_chunks[src]─┐│
   │ │ accumulate bodies │ │                  │ │ {first_seq, num,     ││
   │ │ by dst_pod        │ │                  │ │  body_ptr in rx_buf} ││
   │ │ flush as 8KB DMA  │ │                  │ │ drain on hdr arrive  ││
   │ │ direct→dst        │ │ ━━━━━━━━━━━━━━━► │ └──────────────────────┘│
   │ │ via peer mmap     │ │ Body host→host    │ ┌──chunk parser──────┐ │
   │ │ (Phase 4)         │ │ direct DMA        │ │ read 12B header    │ │
   │ │                   │ │ (DPU bypassed)    │ │ pop num from queue │ │
   │ │                   │ │                   │ │ deliver bodies     │ │
   │ └───────────────────┘ │                   │ └─────────────────────┘ │
   └───────────────────────┘                   └────────────────────────┘
```

### 2.2 컴포넌트별 역할

| 컴포넌트 | 역할 |
|---|---|
| **Thrift application** | RPC 호출만. Mesh 보이지 않음. |
| **TDpumeshClientTransport / ServerTransport** | `dpumesh_send_request(svc, body, len, &req_id)` — 내부에서 hdr entry + body entry 동시 append (단일 critical section). |
| **Mesh layer (host C)** | hdr/body builder, hdr/body pool, peer topology, resolve. |
| **DPU ARM** | hdr batch parse, resolve, policy/tracing/metric, dst별 FIFO outbound batching. Body 안 봄. |
| **DPA** | DMA engine. Forward / reverse / direct(host→host) 모두. desc→mmap/dst_mmap 기반 분기. |

### 2.3 Per-RPC sequence (Phase 4 완성 시)

```
src worker:
   1. lock(chunk_builder[dst_id].lock)
      lock(hdr_builder.lock)        ← 두 lock 동시 hold, deadlock 회피 위해 항상 chunk→hdr 순
      seq = atomic_fetch_add(&ctx->next_seq, 1)
      req_id = {ctx->src_id, seq}
      register_pending(req_id)       state=0
      hdr_builder.append({req_id, dst_id, size, ...})
        if full or first→arm timer: schedule flush
      chunk_builder[dst].append(body, len)
        if full or first→arm timer: schedule flush
      unlock(hdr_builder.lock)
      unlock(chunk_builder[dst].lock)
   2. dpumesh_wait_response(req_id, &resp, 5000ms)
      cond_wait until state=1 or timeout

배경 thread:
   hdr_builder flush:
      desc.flags=OP_HDR_BATCH; desc.pool_type=POOL_HOST_TX_HDR
      desc.mmap=hdr_tx_dpa_handle; desc.req_id=owner_seq
      forward DMA → DPU
      TX_ACK arrives (pool_type=HDR) → free hdr_tx_slot in pending[owner].hdr_tx_slot

   chunk_builder[dst] flush:
      desc.flags=OP_CHUNK|CASE_DIRECT (Phase 4)
      desc.mmap=ring->host_mmap (body buf); desc.dst_mmap=peer[dst].rx_dpa_handle
      desc.dst_addr=peer[dst].rx_partition_base + partition.write_cursor
      partition.write_cursor advance, credit decrement
      DPA host→host DMA, completion → ARM → TX_ACK to src
      TX_ACK arrives (pool_type=BODY) → free body_tx_slot

   DPU ARM forward path:
      OP_HDR_BATCH → parse, for each entry → resolve dst → hdr_outbound[dst] FIFO append
      hdr_outbound[dst] full(8KB) or 50µs timeout → reverse DMA
      desc.dst_mmap = dst_pod.host_hdr_rx_dpa_handle
      DPA reverse DMA → dst's hdr_rx_buffer + cursor
      send DMA_COMPLETION msg to dst (offset, size, src_pod_id)

dst host rx_data_hook:
   on OP_HDR_BATCH completion:
      for each hdr_fwd entry in hdr_rx_buffer[offset..offset+size]:
         e = (entry.req_id, entry.body_size, entry.flags, entry.trace_id, ...)
         expected[entry.src_id].ring_push(e)
      for each src_id touched:
         drain_parked(src_id)

   on OP_CHUNK completion (Phase 4 direct DMA):
      header = (chunk_header *)(rx_dma_buffer + offset)
      src = header->src_id; first = header->first_seq; num = header->num_bodies
      if expected[src].size() >= num
         && expected[src].head().req_id.seq == first:
         consume(rx_dma_buffer+offset, expected[src], num)
         release rx_dma_buffer slot
      else:
         parked_chunks[src].push({first, num, offset, arrived_us})
         /* rx_dma_buffer slot stays occupied */

   drain_parked(src):
      while parked_chunks[src] not empty:
         pc = parked_chunks[src].front()
         if expected[src].size() >= pc.num
            && expected[src].head().req_id.seq == pc.first:
            consume(rx_dma_buffer+pc.offset, expected[src], pc.num)
            release rx_dma_buffer slot at pc.offset
            parked_chunks[src].pop_front()
         else:
            break

   timeout sweep (1초 tick):
      for each src:
         while parked_chunks[src].front().arrived_us < now - 1s:
            log warning; release slot; pop
         while expected[src].head().arrived_us < now - 1s && no parked match:
            log warning; ring_pop
```

---

## 3. Wire formats

### 3.1 req_id

```c
struct mesh_req_id {            /* 8 B */
    uint32_t src_id;            /* src pod의 stable id (DPU register 시 부여) */
    uint32_t seq;               /* src 내 monotonic counter */
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_req_id) == 8, "req_id ABI");

#define REQ_ID_EQ(a,b) ((a).src_id==(b).src_id && (a).seq==(b).seq)
```

### 3.2 Header batch entry (src → DPU): `mesh_hdr_req`

```c
struct mesh_hdr_req {           /* 80 B packed */
    struct mesh_req_id req_id;  /* 8B */
    char     dst_service[24];   /* NUL-terminated within 24B */
    int32_t  dst_pod_id_hint;   /* -1 if unknown (DPU resolves) */
    uint32_t body_size;
    uint64_t trace_id;
    uint64_t span_id;
    uint8_t  flags;             /* OP_REQUEST | OP_RESPONSE */
    uint8_t  reserved[23];
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_hdr_req) == 80, "hdr_req ABI");
```

8KB slot당 최대 102 entries (8192/80=102). `MESH_HDR_BATCH_MAX_ENTRIES = 96` (안전 margin).

### 3.3 Header forward entry (DPU → dst): `mesh_hdr_fwd`

```c
struct mesh_hdr_fwd {           /* 64 B packed */
    struct mesh_req_id req_id;  /* 8B (src의 것 echo) */
    int32_t  src_pod_id;        /* DPU가 mapping table에서 src_id→src_pod_id 확정 */
    uint32_t body_size;
    uint64_t trace_id;
    uint64_t span_id;
    uint8_t  flags;
    uint8_t  status;            /* 0=OK, 1=DROP */
    uint8_t  reserved[26];
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_hdr_fwd) == 64, "hdr_fwd ABI");
```

8KB slot당 128 entries. **DPU의 dst별 outbound staging은 strict FIFO** — src의 hdr_builder push 순서
보존이 dst의 expected queue 순서를 결정.

### 3.4 Body chunk (src → dst, Phase 4 direct)

```c
struct mesh_chunk_header {      /* 12 B */
    uint32_t src_id;            /* src의 stable id */
    uint32_t first_seq;         /* chunk 첫 body의 seq */
    uint32_t num_bodies;        /* chunk 안에 packed된 body 개수 */
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_chunk_header) == 12, "chunk_header ABI");
```

```
+──── slot (8 KB) ───────────────────────────────────────────────+
│ src_id (4B) | first_seq (4B) | num_bodies (4B)  ← 12B header    │
│ body[0]                  ← size from expected[src].pop()        │
│ body[1]                                                          │
│ ...                                                              │
│ body[num-1]                                                      │
+────────────────────────────────────────────────────────────────+
DMA size = 12 + Σ body_size. Padding to 128B by DPA's ALIGN_UP_128.
```

**불변식 (반드시)**:
1. Chunk 내 body는 **src→이 dst로 보낸 호출 순서**대로 packed.
2. dst의 `expected[src_id]` queue head부터 num개의 entry size = chunk 안의 body들 size에 정확히 일치.
3. dst가 head.req_id.seq != header.first_seq면 **sync 깨짐 — DPU forward 손실 또는 chunk 손실 의심**.
   현재 chunk를 park하고 timeout sweep이 정리.

**최대 body 개수**: `MESH_CHUNK_MAX_BODIES = 64`. 8KB - 12B = 8180B / 64 = ~127B/body 평균. 실제로는
가변 크기 body 섞여서 더 적을 수도 있음. cap만 둠.

**Body size 한계**: 단일 body ≤ 8180B. 더 큰 body는 chunking (Phase 5+) — 지금 scope 밖.

### 3.5 TX_ACK (DPU → src)

```c
struct dmesh_tx_ack_msg {
    enum dmesh_msg_type type;          /* = DMESH_MSG_TX_ACK */
    struct mesh_req_id req_id;          /* desc.req_id 그대로 echo (=owner) */
    int32_t dst_pod_id;
    uint8_t pool_type;                  /* POOL_HOST_TX_BODY 또는 POOL_HOST_TX_HDR */
    uint8_t pad[3];
};
```

Host TX_ACK 핸들러:
```c
p = pending[req_id_to_slot(req_id)];
lock(p);
if (state == 0 || state == -2) {
    if (ack.pool_type == POOL_HOST_TX_HDR) {
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
    } else {
        if (p->body_tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->body_tx_slot);
            p->body_tx_slot = -1;
        }
    }
    if (state == -2 && p->body_tx_slot < 0 && p->hdr_tx_slot < 0) {
        state = -1;
        cond_broadcast;
    }
}
unlock(p);
```

레거시 호환: `pool_type == 0` (POOL_NONE)이면 POOL_HOST_TX_BODY로 간주.

### 3.6 Peer topology message (DPU → host, Phase 4)

```c
struct dmesh_peer_topology_msg {
    enum dmesh_msg_type type;       /* = DMESH_MSG_PEER_TOPOLOGY */
    int32_t  pod_id;                /* peer의 pod_id */
    uint32_t src_id_assigned;       /* peer의 stable src_id */
    char     app_name[64];
    void    *rx_addr;
    size_t   rx_buf_size;
    size_t   rx_export_desc_len;
    uint8_t  rx_export_desc[];      /* peer의 rx_dma_mmap export PCI desc */
};
```

받은 host: `doca_mmap_create_from_export` → `doca_mmap_dev_get_dpa_handle` → peer table 저장.

---

## 4. Buffer/Pool architecture

### 4.1 Host TX side — 두 개의 독립 pool

```c
struct dpumesh_ctx {
    /* === Identity === */
    uint32_t              src_id;                /* DPU가 register 시 부여 */
    atomic_uint_fast32_t  next_seq;              /* req_id seq counter */
    int32_t               pod_id;                /* k8s pod id */

    /* === BODY TX pool (기존) === */
    void                 *dma_buffer;            /* 2048 × 8KB = 16MB */
    struct doca_mmap     *local_mmap;
    doca_dpa_dev_mmap_t   dpa_mmap_handle;
    uint8_t              *slot_bitmap;
    pthread_mutex_t       slot_lock;
    pthread_cond_t        slot_cond;
    int                   num_slots;             /* 2048 */
    int                   slot_size;             /* 8192 */
    struct dma_ring      *dma_ring;

    /* === HDR TX pool (Phase 1) === */
    void                 *hdr_tx_buffer;         /* 1024 × 8KB = 8MB */
    struct doca_mmap     *hdr_tx_mmap;
    doca_dpa_dev_mmap_t   hdr_tx_dpa_handle;
    uint8_t              *hdr_tx_bitmap;
    pthread_mutex_t       hdr_slot_lock;
    pthread_cond_t        hdr_slot_cond;
    int                   hdr_num_slots;         /* 1024 */
    int                   hdr_slot_size;         /* 8192 */
    /* Forward ring은 body와 공유 — desc.mmap field로 분기 */

    /* === Builders === */
    struct hdr_builder    hdr_builder;           /* single, DPU 단일 target */
    struct chunk_builder  chunk_builders[MAX_DST_PODS];

    /* === Pending table === */
    dpumesh_pending_t     pending[MAX_PENDING];  /* 65536 entries */
};
```

### 4.2 Host RX side

```c
struct dpumesh_ctx {
    /* ... */

    /* === BODY RX (기존) === */
    void                 *rx_dma_buffer;         /* 16MB, DPA writes */
    struct doca_mmap     *rx_dma_mmap;
    size_t                rx_dma_buf_size;
    /* slot occupancy tracking: parked chunk가 점유하면 free 불가 */
    pthread_mutex_t       rx_slot_lock;
    /* per-partition write_cursor (Phase 4) — 자세히는 §6.4 */

    /* === HDR RX (Phase 2) === */
    void                 *hdr_rx_buffer;         /* 8MB */
    struct doca_mmap     *hdr_rx_mmap;
    size_t                hdr_rx_buf_size;

    /* === Expected ring + parked chunks (Phase 3) === */
    struct expected_ring  expected[MAX_SRC_IDS]; /* indexed by src_id, sparse OK (hash) */
    struct parked_list    parked_chunks[MAX_SRC_IDS];
    pthread_mutex_t       expected_locks[MAX_SRC_IDS]; /* per-src lock */

    /* === Peer table (Phase 4) === */
    struct peer_info      peers[MAX_PEERS];
    pthread_rwlock_t      peer_lock;
};
```

`MAX_SRC_IDS = MAX_PEERS = 16` 통일. expected/parked는 자기 자신 src_id 제외하고 채워짐.

```c
struct expected_entry {
    struct mesh_req_id req_id;
    uint32_t size;
    uint8_t  flags;
    int32_t  src_pod_id;
    uint64_t trace_id;
    uint64_t span_id;
    uint64_t arrived_us;
};

struct expected_ring {
    struct expected_entry buf[EXPECTED_RING_CAP];  /* cap=1024 */
    uint32_t head;       /* dequeue */
    uint32_t tail;       /* enqueue */
    uint32_t count;
};

struct parked_chunk_node {
    uint32_t first_seq;
    uint32_t num_bodies;
    uint32_t rx_slot_idx;     /* rx_dma_buffer 내 slot index — body 영역 점유 */
    uint32_t total_size;      /* DMA payload total — slot 길이 검증용 */
    uint64_t arrived_us;
    struct parked_chunk_node *next;
};

struct parked_list {
    struct parked_chunk_node *head;
    struct parked_chunk_node *tail;
    uint32_t count;
};
```

### 4.3 DPU side — per-pod state

```c
struct pod_state {
    /* === 기존 (body path) === */
    struct doca_mmap *remote_mmap;          /* host's body TX */
    struct doca_mmap *ring_mmap;
    struct doca_mmap *local_mmap;           /* DPU staging */
    doca_dpa_dev_mmap_t local_mmap_dpa_handle;
    struct dma_ring *dma_ring;
    struct dma_ring *tx_ring;
    struct doca_mmap *tx_mmap;
    struct doca_mmap *host_rx_mmap;
    /* ... */

    /* === Identity === */
    uint32_t src_id;             /* DPU가 register 시 부여, 이 pod = src일 때 */
    int32_t  pod_id;

    /* === Phase 1: host의 hdr TX mmap === */
    struct doca_mmap *remote_hdr_mmap;
    void  *remote_hdr_addr;
    size_t remote_hdr_buf_size;
    doca_dpa_dev_mmap_t remote_hdr_dpa_handle;

    /* === Phase 2: host의 hdr RX mmap === */
    struct doca_mmap *host_hdr_rx_mmap;
    void  *host_hdr_rx_addr;
    size_t host_hdr_rx_buf_size;
    doca_dpa_dev_mmap_t host_hdr_rx_dpa_handle;

    /* === Phase A 기존: outbound hdr batch staging (per-dst FIFO) === */
    /* 한 src의 outbound를 dst pod_id로 분류해서 누적 */
    struct hdr_outbound_q hdr_outbound[MAX_DST_PODS];
};

struct hdr_outbound_q {
    uint8_t  staging[8192];      /* 8KB FIFO */
    int      num_entries;
    uint64_t first_us;           /* age-based flush */
    /* strict FIFO 보장: append in src's hdr_builder push order */
};
```

### 4.4 메모리 예산 (per pod)

| 영역 | 크기 | 용도 |
|---|---|---|
| Body TX (`dma_buffer`) | 16 MB | RPC body 8KB chunk pool |
| Body RX (`rx_dma_buffer`) | 16 MB | 받는 chunk 영역 (parked chunk 점유 가능) |
| RX post-copy (`rx_buffer`) | 16 MB | App에 deliver 전 staging |
| Hdr TX (`hdr_tx_buffer`) | 8 MB | 1024 × 8KB |
| Hdr RX (`hdr_rx_buffer`) | 8 MB | 1024 × 8KB |
| Pending table | ~13 MB | 65536 × ~200B |
| Expected rings | ~16 MB | 16 src × 1024 × ~80B + locks |
| Parked nodes | ~256 KB | up to 1024 nodes × 32B |
| Peer info | < 64 KB | MAX_PEERS=16 |
| **Total** | **~95 MB** per pod | (legacy ~61MB + 신규 ~34MB) |

K8s pod의 `hugetlb` 또는 `memory.limit` 조정 필요. **별도 task로 분리** (구현 시 확인).

---

## 5. Slot lifecycle

### 5.1 핵심 원칙

**1 user RPC = 1 pending entry**. 그 안에 `body_tx_slot`, `hdr_tx_slot` 두 필드. Pool당 1 slot 모델
유지, pool은 두 개.

### 5.2 Pending state machine

`dpumesh_pending_t.state`:
- `-1` = unused
- `0` = waiting (registered, response 대기)
- `1` = arrived (response 도착, wait_response가 deliver 대기)
- `-2` = deferred-release (timed out, slot 두 개 다 -1 될 때까지 점유)

Transitions:
```
register_pending: -1 → 0
TX_ACK (state ∈ {0,-2}): 해당 pool slot freed
  if state==-2 && body_tx_slot<0 && hdr_tx_slot<0: state→-1, cond_broadcast
Response arrival (state==0): state→1, cond_broadcast
wait_response success (state==1): 두 slot 다 force-free, state→-1
wait_response timeout (state==0): 두 slot 다 force-free, state→-2 (safety net)
```

### 5.3 pending 구조

```c
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    sw_descriptor_t desc;
    volatile int    state;
    int             body_tx_slot;       /* body pool, -1 if none */
    int             hdr_tx_slot;        /* hdr pool, -1 if none */
    struct mesh_req_id req_id;          /* 64bit */
    uint64_t        registered_us;
} dpumesh_pending_t;
```

### 5.4 req_id → pending index 매핑

```c
static inline uint32_t req_id_to_pending_slot(struct mesh_req_id rid) {
    /* src_id == ctx->src_id 만 들어옴 (자기 RPC). seq로 indexing */
    return rid.seq & (MAX_PENDING - 1);
}
```

다른 src에서 온 incoming RPC는 expected_ring을 거쳐 별도로 처리되므로 pending과 충돌 없음.

### 5.5 Owner = 첫 entry의 req_id

`hdr_builder`의 첫 entry의 user req_id → `pending[owner].hdr_tx_slot = hdr_slot`.
`chunk_builder[dst]`의 첫 body의 user req_id → `pending[owner].body_tx_slot = body_slot`.

같은 user가 양쪽 owner여도 OK — 필드가 다름.

**batch_req_id는 안 씀**. 옛 plan(2026-05-13)의 batch_req_id 안티패턴은 `feedback`에 기록 (§9).

### 5.6 Force-free on timeout

state=-2가 ACK loss로 영구 점유되는 걸 막기 위해:
- `wait_response` 타임아웃 시 두 slot 다 force-free (bitmap에서 직접 clear).
- 추가로, `pending sweep thread`가 30s 주기로 state=-2 + registered_us > now-30s인 entry force-clear.

---

## 6. DMA infrastructure

### 6.1 Forward path (host → DPU)

**기존**: DPA가 `ring->host_mmap` (= body buffer의 DPA handle)에서 desc.addr 위치 읽음.

**Phase 1 신규**: `desc.mmap` 필드를 src override로 활용.

DPA forward kernel patch:
```c
doca_dpa_dev_mmap_t src_mmap = desc->mmap ? desc->mmap : ring->host_mmap;
/* desc.addr는 절대 가상 주소 (host에서 작성 시 (uint64_t)buffer + offset) */

/* Range check: override 안 한 경우만 ring->host_addr 범위 검증 */
if (desc->mmap == 0) {
    if (src_addr < ring->host_addr || src_end > ring->host_end) { /* error */ }
}
/* dma_copy: src_mmap, desc->addr */
```

Host enqueue:
```c
if (desc->src_body_pool_type == POOL_HOST_TX_HDR) {
    dma->mmap = ctx->hdr_tx_dpa_handle;
    dma->addr = (uint64_t)ctx->hdr_tx_buffer + slot * ctx->hdr_slot_size;
} else {
    dma->mmap = 0;
    dma->addr = (uint64_t)ctx->dma_buffer + slot * ctx->slot_size;
}
```

### 6.2 Reverse path (DPU → host)

**Phase 2 신규**: `desc.dst_mmap` 필드 (reserved에서 8B 재할당).

```c
struct dma_desc {
    doca_dpa_dev_mmap_t mmap;      /* 8B - SRC override (forward / reverse) */
    uint64_t addr;                 /* 8B */
    uint32_t size;                 /* 4B */
    uint64_t idx;                  /* 8B */
    int32_t  dst_pod_id;           /* 4B */
    int8_t   flags;                /* 1B */
    uint8_t  pad0[3];              /* 3B */
    int32_t  src_pod_id;           /* 4B */
    doca_dpa_dev_mmap_t dst_mmap;  /* 8B - reverse / direct DST override */
    uint64_t dst_addr;             /* 8B - direct DMA dst absolute addr (Phase 4) */
    uint8_t  src_body_pool_type;   /* 1B */
    uint8_t  reserved[10];
    volatile uint8_t valid;        /* 1B */
};
_Static_assert(sizeof(struct dma_desc) == 72, "dma_desc layout");
/* TODO: 정확한 64B 정렬 위해 필드 정리. 위는 명세, 구현 시 packing 확인. */
```

DPA reverse kernel:
```c
doca_dpa_dev_mmap_t dst_mmap = desc->dst_mmap ? desc->dst_mmap : ring->host_mmap;
uint64_t dst_addr = desc->dst_mmap ? desc->dst_addr : (ring->host_addr + ring_pos);
```

### 6.3 Direct path (host → host, Phase 4)

DPA forward kernel CASE_DIRECT 분기:
```c
if (desc->flags & CASE_DIRECT) {
    /* src = ring->host_mmap, dst = desc->dst_mmap (peer RX) */
    /* No DPU staging */
    src_mmap = ring->host_mmap;   /* local body buf */
    dst_mmap = desc->dst_mmap;     /* peer's RX */
    dst_addr = desc->dst_addr;     /* partition base + cursor */
    dma_copy(dst_mmap, dst_addr, src_mmap, desc->addr, desc->size);
    /* Completion: alert ARM via dedicated comp queue */
    notify_arm_direct_complete(desc->idx, desc->req_id, desc->dst_pod_id);
}
```

ARM 받자마자 `send_or_defer_tx_ack(src_pod, req_id, dst_pod, POOL_HOST_TX_BODY)`.

### 6.4 Cursor partition + flow control (Phase 4)

**구조**: dst의 rx_dma_buffer 16MB를 src별 partition으로 분할.
- partition 수 = MAX_PEERS = 16
- partition 크기 = 16MB / 16 = 1MB per src
- 각 src는 자기 partition 안에서 wrap

**각 partition state** (dst가 보관, peer table 별도로):
```c
struct rx_partition {
    uint32_t base;        /* slot index 시작 */
    uint32_t cap;         /* slot 수 */
    atomic_uint32_t write_cursor;  /* dst가 본 가장 최신 write 위치 (src가 알려줌) */
    uint32_t read_cursor; /* dst가 consume한 위치 — chunk 처리 + parked release 시 advance */
};
```

**src가 보관 (peer_info의 일부)**:
```c
struct peer_partition {
    uint32_t base_offset;  /* dst의 rx_dma_buffer 내 자기 partition 시작 byte offset */
    uint32_t cap_bytes;    /* 자기 partition 총 byte */
    uint32_t local_write_cursor;  /* 자기 partition 안에서 자기가 쓴 byte offset */
    atomic_uint32_t credit_bytes; /* dst가 grant한 free 영역 */
};
```

**Grant scheme**:
1. 초기: src가 grant_bytes = cap_bytes 받음 (전체).
2. src chunk_flush: `if (credit_bytes >= chunk.size) { credit -= chunk.size; write }`. 부족하면 chunk
   flush를 지연 (chunk_builder가 wait).
3. dst가 chunk consume + slot release 시: 일정 누적 (e.g., 64KB) 이상이면 src에 `GRANT(bytes)`
   메시지. src의 credit 증가.
4. Loss 대비: dst가 주기적(50ms)으로 누적 grant 보냄.

**메시지**:
```c
struct dmesh_grant_msg {
    enum dmesh_msg_type type;       /* = DMESH_MSG_GRANT */
    int32_t  src_pod_id;            /* 어느 src의 partition인지 */
    int32_t  dst_pod_id;            /* 보내는 dst */
    uint32_t bytes_released;        /* 증분 */
};
```

이건 DPU를 거치지 않고 dst→src로 직접 (comch 또는 별도 control channel). Phase 4 진입 시 구체 결정.

---

## 7. Dst's expected ring + parked chunks (Phase 3)

### 7.1 Enqueue on hdr arrival

```c
void rx_data_hook_hdr_batch(ctx, hdr_rx_buf, offset, size) {
    int n = size / sizeof(struct mesh_hdr_fwd);
    struct mesh_hdr_fwd *entries = hdr_rx_buf + offset;
    uint16_t touched_src_bitmap = 0;
    for (int i=0; i<n; ++i) {
        uint32_t sid = entries[i].req_id.src_id;
        lock(ctx->expected_locks[sid % LOCK_STRIPES]);
        struct expected_ring *r = &ctx->expected[sid];
        if (r->count >= EXPECTED_RING_CAP) {
            log_warn("expected[%u] full, dropping hdr seq=%u", sid, entries[i].req_id.seq);
            unlock; continue;
        }
        r->buf[r->tail] = (struct expected_entry){
            .req_id = entries[i].req_id,
            .size = entries[i].body_size,
            .flags = entries[i].flags,
            .src_pod_id = entries[i].src_pod_id,
            .trace_id = entries[i].trace_id,
            .span_id = entries[i].span_id,
            .arrived_us = now_us(),
        };
        r->tail = (r->tail + 1) % EXPECTED_RING_CAP;
        r->count++;
        unlock;
        touched_src_bitmap |= (1u << (sid % 16));
    }
    /* drain parked for touched sources */
    for (int sid=0; sid<MAX_SRC_IDS; ++sid) {
        if (touched_src_bitmap & (1u << (sid % 16)))
            drain_parked(ctx, sid);
    }
}
```

### 7.2 Chunk arrival

```c
void rx_data_hook_chunk(ctx, rx_buf, offset, size) {
    struct mesh_chunk_header *h = (void*)(rx_buf + offset);
    uint32_t sid = h->src_id;
    lock(ctx->expected_locks[sid % LOCK_STRIPES]);
    struct expected_ring *r = &ctx->expected[sid];
    if (r->count >= h->num_bodies &&
        r->buf[r->head].req_id.seq == h->first_seq) {
        consume_chunk(ctx, rx_buf + offset, r, h->num_bodies);
        unlock;
        release_rx_slot(ctx, offset / SLOT_SIZE);
    } else {
        parked_push(ctx, sid, h->first_seq, h->num_bodies, offset / SLOT_SIZE, size);
        unlock;
        /* rx slot stays occupied */
    }
}
```

### 7.3 Drain parked

```c
void drain_parked(ctx, sid) {
    lock(ctx->expected_locks[sid % LOCK_STRIPES]);
    struct expected_ring *r = &ctx->expected[sid];
    struct parked_chunk_node *node;
    while ((node = ctx->parked_chunks[sid].head)) {
        if (r->count < node->num_bodies) break;
        if (r->buf[r->head].req_id.seq != node->first_seq) {
            /* sync 깨짐 — head가 더 나중 seq를 가리킴. 이건 hdr loss 의심.
             * Park 그대로 두고 timeout sweep에 맡김. */
            break;
        }
        void *buf = ctx->rx_dma_buffer + node->rx_slot_idx * SLOT_SIZE;
        consume_chunk(ctx, buf, r, node->num_bodies);
        release_rx_slot(ctx, node->rx_slot_idx);
        ctx->parked_chunks[sid].head = node->next;
        if (!node->next) ctx->parked_chunks[sid].tail = NULL;
        ctx->parked_chunks[sid].count--;
        free(node);
    }
    unlock(ctx->expected_locks[sid % LOCK_STRIPES]);
}
```

### 7.4 Consume

```c
void consume_chunk(ctx, chunk_buf, ring, num) {
    uint32_t cursor = sizeof(struct mesh_chunk_header);
    for (uint32_t i=0; i<num; ++i) {
        struct expected_entry e = ring->buf[ring->head];
        ring->head = (ring->head + 1) % EXPECTED_RING_CAP;
        ring->count--;
        void *body_ptr = (uint8_t*)chunk_buf + cursor;
        cursor += e.size;
        process_rx_dma_entry(ctx, e.req_id, body_ptr, e.size, e.src_pod_id, e.flags, e.trace_id, e.span_id);
    }
}
```

### 7.5 Timeout sweep (1초)

별도 thread 또는 PE 주기 callback에서 1초마다:
```c
uint64_t now = now_us();
for each sid in MAX_SRC_IDS:
    lock(expected_locks[sid % LOCK_STRIPES]);
    /* GC parked older than 1s */
    while (parked_chunks[sid].head && now - parked_chunks[sid].head->arrived_us > 1e6) {
        log_warn("parked chunk timeout src=%u first_seq=%u", sid, head->first_seq);
        release_rx_slot(ctx, head->rx_slot_idx);
        parked_chunks[sid].head = head->next;
        free(head);
    }
    /* GC orphan expected entries older than 1s (only at head — strict FIFO) */
    while (expected[sid].count > 0 && now - expected[sid].buf[expected[sid].head].arrived_us > 1e6) {
        log_warn("orphan expected timeout src=%u seq=%u", sid, ...);
        expected[sid].head = (expected[sid].head + 1) % CAP;
        expected[sid].count--;
    }
    unlock;
```

---

## 8. Phase-by-phase implementation

### Phase 1 — TX-side HDR pool independence

**목표**: hdr_builder가 전용 풀(hdr_tx_buffer)에서 slot 할당. Body 풀과 경쟁 0.
**검증 효과**: 1k-5k RPS back-to-back 안정.

**Scope**: TX만. RX-side는 Phase 2로 미룸. 이 Phase 종료 시점에 incoming hdr_forward는 여전히 body의
rx_dma_buffer에 도착 (OP_HDR_BATCH flag로 demux).

**파일별 변경**:

1. `lib/cpp/src/thrift/transport/doca/dpumesh_common.h`
   - `#define POOL_HOST_TX_HDR 8`

2. `lib/cpp/src/thrift/transport/doca/comch_common.h`
   - `enum dmesh_mmap_type { DMA_HOST_TX_HDR_BUFFER = 4 }`
   - `struct dmesh_tx_ack_msg`에 `uint8_t pool_type; uint8_t pad[3];`
   - `struct mesh_req_id` 정의 (8B)

3. `lib/cpp/src/thrift/transport/doca/comch_common.c`
   - `process_mmap_msg`의 mmap_type 분기에 DMA_HOST_TX_HDR_BUFFER: `mmap = &pod->remote_hdr_mmap`
   - 받은 후 DPA handle resolve

4. `lib/cpp/src/thrift/transport/doca/object.h`
   - `struct pod_state`에 `remote_hdr_*` 필드

5. `lib/cpp/src/thrift/transport/dpumesh_doca.c`
   - `struct dpumesh_ctx`에 hdr TX pool 필드
   - `struct dpumesh_pending_t`에 `body_tx_slot`, `hdr_tx_slot` (기존 `tx_slot` rename: `body_tx_slot`)
   - `init_datapath` 끝에 hdr_tx_buffer alloc + mmap + export + DPA handle
   - `cleanup_ctx`에 destroy 추가
   - 신규: `dpumesh_hdr_tx_alloc/free/buf`
   - `hdr_builder_flush_locked`: `dpumesh_hdr_tx_alloc` + `desc.src_body_pool_type = POOL_HOST_TX_HDR`
   - `dpumesh_enqueue`: `src_body_pool_type` 분기로 `dma->mmap`/`dma->addr` 결정
   - `rx_data_hook` TX_ACK 분기: pool_type 보고 hdr_tx_free / tx_free 분기

6. `lib/cpp/src/thrift/transport/doca/dpu_worker.c`
   - `send_or_defer_tx_ack` 시그니처에 `uint8_t pool_type` 추가
   - 호출 사이트:
     - `process_forward_entry`의 OP_HDR_BATCH 분기 → POOL_HOST_TX_HDR
     - 그 외 forward (legacy/OP_CHUNK) → POOL_HOST_TX_BODY
     - `process_rev_notify_entry` → POOL_HOST_TX_BODY
   - `deferred_tx_acks`에 pool_type 필드
   - `server_send_tx_ack_to`에 pool_type

7. `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c`
   - Forward kernel: `desc->mmap ? desc->mmap : ring->host_mmap` 분기, range check 가드

**검증**:
```bash
./test-bench.sh deploy
kubectl set env deployment/bench-dpumesh -n test-bench BENCH_SPLIT=1
sleep 5 && ./test-bench.sh pin

# Sanity
timeout 15 ./test-bench.sh dpumesh 500 3 256
timeout 15 ./test-bench.sh dpumesh 1000 3 256
timeout 15 ./test-bench.sh dpumesh 1000 3 256   # back-to-back

# Cliff probe
for rps in 2000 3000 5000; do
    timeout 15 ./test-bench.sh dpumesh $rps 3 256
done

# Legacy 회귀
kubectl set env deployment/bench-dpumesh -n test-bench BENCH_SPLIT-
sleep 5 && ./test-bench.sh pin
timeout 25 ./test-bench.sh dpumesh 40000 10 8192
```

**Success criteria**:
- Phase 1 split path: 1k/2k/3k/5k 각각 0 fail, back-to-back 동일 결과
- Legacy 회귀 0 (Hard Rule #2)
- DPU log < 100KB

**예상 effort**: 2-3일

---

### Phase 2 — RX-side HDR buffer separation

**목표**: incoming hdr forward가 `hdr_rx_buffer`(별도 mmap)에 직접 도착.

**Scope**:
- host `hdr_rx_buffer` mmap setup + export to DPU
- DPU의 pod_state에 `host_hdr_rx_*` 추가
- `dma_desc`에 `dst_mmap` 필드 신설
- DPA reverse kernel: dst_mmap override
- DPU `dpu_flush_hdr_outbound` reverse desc에 dst_mmap 세팅

**파일별 변경**:

1. `lib/cpp/src/thrift/transport/doca/dpa_common.h`
   - `struct dma_desc`에 `dst_mmap`, `dst_addr` 필드 (reserved 재할당)
   - Static_assert로 size/offset 확정

2. `lib/cpp/src/thrift/transport/doca/comch_common.h`
   - `DMA_HOST_RX_HDR_BUFFER = 5`

3. `comch_common.c`
   - mmap_type 분기 처리

4. `object.h`
   - `pod_state.host_hdr_rx_*`

5. `dpumesh_doca.c`
   - `hdr_rx_buffer` alloc → mmap → export
   - `rx_data_hook` OP_HDR_BATCH 분기에서 데이터 출처: `hdr_rx_buffer + pos`
   - DMA completion msg에 `pool_type` 또는 OP flag로 buffer 분기

6. `dpu_worker.c`
   - `dpu_flush_hdr_outbound`: reverse desc.dst_mmap = dst_pod->host_hdr_rx_dpa_handle

7. `dpa_kernel.c`
   - Reverse kernel: `desc->dst_mmap ? desc->dst_mmap : ring->host_mmap`
   - dst_addr override 처리

**검증**: hdr_forward가 hdr_rx_buffer에 도착, body chunk가 rx_dma_buffer에 도착, demux 확인. 기능적
회귀 0.

**예상 effort**: 2일

---

### Phase 3 — Chunk format + expected ring + event-driven park/wake

**목표**: chunk format 12B로 정리. per-src ring buffer expected queue + parked chunks. event-driven
처리. 1초 timeout sweep. req_id 64bit 전환.

**파일별 변경**:

1. `lib/cpp/src/thrift/transport/doca/mesh.h`
   - 기존 chunk_header 폐기, 새 12B 정의
   - `mesh_req_id` 64bit
   - `mesh_hdr_req` (80B), `mesh_hdr_fwd` (64B) 정정

2. `dpumesh_doca.c`
   - `next_seq` atomic counter, `src_id` 필드
   - `dpumesh_send_request` 통합 API (또는 send_header + send_body가 호출 측에서 같은 lock 안에)
   - chunk_builder가 dst→이 dst 호출 순서대로 body 누적, flush 시 12B header 작성
   - `expected_ring`, `parked_chunks`, `expected_locks` 정의 + init/destroy
   - `rx_data_hook` OP_HDR_BATCH: ring enqueue + drain_parked
   - `rx_data_hook` OP_CHUNK: lookup match → consume 또는 park
   - 1초 timeout sweep thread 또는 PE callback

3. `dpu_worker.c`
   - hdr_outbound가 dst pod_id별로 분리, strict FIFO 보장
   - `mesh_hdr_fwd` 작성 시 src_id 그대로 echo

4. `bench/bench_dpumesh.c`, `bench/echo_dpumesh.c`
   - 새 API 호출 (또는 wrap)
   - req_id 64bit handling

**검증**:
- 1 RPC × 1KB: hdr → body 순서 도착 → deliver
- multi-dst: src→A, src→B, src→A 순서로 보내고 dst_A의 chunk 안에서 정확히 1, 3번 body가 들어 있는지 확인
- chunk가 hdr보다 일찍 도착하는 race: park → drain 동작 확인 (logs)
- 1초 timeout: hdr 손실 simulate 시 parked chunk가 1초 후 정리되는지

**예상 effort**: 4-5일

---

### Phase 4 — Body host→host direct DMA + peer topology + flow control

**목표**: design.md §5.3 완성. body chunk DPU 우회. throughput 회복.

**Scope**:
- DPU peer topology push at register (RX mmap export 포함)
- Host peer table + `peer_partition`
- DPA forward kernel CASE_DIRECT 분기
- ARM-mediated TX_ACK
- Per-(src,dst) partition + credit/grant flow control

**파일별 변경**:

1. `comch_common.h`
   - `DMESH_MSG_PEER_TOPOLOGY`, `DMESH_MSG_GRANT`
   - `struct dmesh_peer_topology_msg`, `struct dmesh_grant_msg`

2. `comch_server.c` (DPU)
   - 새 pod register 시 다른 pod들에게 broadcast + catch-up

3. `dpumesh_doca.c`
   - `rx_data_hook` PEER_TOPOLOGY 분기: `doca_mmap_create_from_export` → DPA handle → peers[]
   - `rx_data_hook` GRANT 분기: credit 증가
   - `chunk_flush_locked`:
     - `peer = &ctx->peers[dst_id]`
     - credit 검증, 부족하면 cond_wait
     - `desc.dst_mmap = peer->rx_dpa_handle`
     - `desc.dst_addr = peer->base + peer->local_write_cursor`
     - `flags |= CASE_DIRECT`
     - cursor advance, credit -= size
   - dst 측: chunk consume + slot release 시 누적 grant, 64KB threshold 또는 50ms 주기에 GRANT 발사

4. `dpa_kernel.c`
   - Forward kernel CASE_DIRECT 분기
   - Completion: ARM에 alert (별도 comp queue 또는 reserved bit)

5. `dpu_worker.c`
   - 새 comp type 처리 (host→host 완료) → 즉시 TX_ACK 발사

**검증**:
- 1 RPC end-to-end (Phase 4): hdr→DPU→dst, body direct, expected match, deliver
- 1k-40k RPS sweep, 256~7000B body
- Back-to-back × 2회
- Legacy 회귀 0
- DPU CPU 사용량 측정 (body가 안 거치니까 큰 폭 감소 기대)

**Success criteria**:
- 40k RPS @ 8192B: 0 fail, p99 ≤ 9ms
- BENCH_SPLIT path가 legacy path와 동등 또는 우위
- DPU log < 100KB
- Hard Rule 1~8 모두 통과

**예상 effort**: 5-7일

---

## 9. Failure modes & lessons

### 9.1 batch_req_id register_pending 안티패턴

옛 plan이 chunk_flush마다 별도 batch_req_id로 register_pending → pending 5× 사용 → 충돌율 증가 →
worker가 2s 블록 → cliff. **해결**: tx_slot을 첫 user의 pending에 직접 attach. pending 사용량 v1.0.0
수준.

### 9.2 Sweep mechanism은 root cause 회피용

옛 시도: deadline_us + sweep으로 0/20 영구화는 풀었지만 cliff 숫자만 옮김. **옳은 fix는 pool 분리**.

### 9.3 같은 pool 경쟁 (현 상태 cliff 원인)

hdr_builder가 body TX pool 사용 → 1 RPC = 2 slot → ceiling 절반. **해결 (Phase 1)**: hdr 풀 분리.

### 9.4 같은 user_req_id가 hdr/chunk 양쪽 owner

해결: `pending`에 `body_tx_slot` + `hdr_tx_slot` 별도.

### 9.5 옛 chunk format의 multi-dst 결함 (이번 세션 발견)

옛 plan의 8B chunk header `[first_req_id, num]`는 single-src single-dst 모델 가정 — multi-dst면
같은 src의 dst_A 시퀀스가 듬성해서 first+i 매칭 불가. **해결**: chunk header 12B `[src_id, first_seq, num]`
+ dst의 expected를 src별 ring queue로 demux. 듬성한 seq도 queue 안에선 빽빽.

### 9.6 옛 chunk format의 multi-src 결함

옛 expected[req_id % N]은 단일 src 가정. 다른 src에서 같은 req_id 보내면 충돌. **해결**: req_id 64bit
(src_id, seq), expected는 src별 demux.

### 9.7 Ordering race — spin은 대답 아님

hdr이 chunk보다 늦게 도착하는 race를 spin으로 해결하려는 시도 X. **해결**: event-driven park/wake.
chunk arrival에서 expected miss면 park, hdr arrival에서 drain. DPU의 dst별 outbound가 strict FIFO이고
src의 hdr+chunk push가 같은 critical section이면 ordering 자체는 보장됨 — race는 단지 도착 타이밍.

### 9.8 ALIGN_UP_128 cross-slot DMA

DPA의 ALIGN_UP_128 (예: 8128B → 8192B)이 다음 slot 침범 가능. **방지**: chunk/hdr 모두 slot_size 안에서
크기 cap. chunk header 12B는 항상 slot 안. body들도 slot_size - 12B 안에 packing.

### 9.9 DPU log 폭주

%s on garbage memory → MB 단위. 항상 safe-copy + NUL-term + rate-limit.

### 9.10 hdr/chunk flush 순서 (Phase 3 새 risk)

hdr_builder와 chunk_builder가 독립 timer로 flush → chunk가 hdr보다 먼저 flush 가능. 해결책 (9.7과
같음): event-driven이므로 dst 측이 park로 처리. src 측은 강제 sequencing 안 함.

### 9.11 Phase 4 partition overrun

src가 wrap 시 dst가 못 비웠으면 in-flight 덮어쓰기. **방지**: credit/grant scheme (§6.4).

---

## 10. Test protocol

### 10.1 매 phase 통과 기준

| Test | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|---|---|---|---|---|
| Deploy EXIT=0 | ✓ | ✓ | ✓ | ✓ |
| Legacy 회귀 0 (40k @ 8192) | ✓ | ✓ | ✓ | ✓ |
| Split path single RPC | ✓ | ✓ | ✓ | ✓ |
| Split path low load (1k×3s×256B) | ✓ | ✓ | ✓ | ✓ |
| Back-to-back (×2회) | ✓ | ✓ | ✓ | ✓ |
| Multi-dst routing 정확성 | — | — | ✓ | ✓ |
| Split path 5k RPS | ✓ (Phase 1) | ✓ | ✓ | ✓ |
| Split path 40k RPS | — | — | — | ✓ (Phase 4) |
| Parked-chunk hdr-loss timeout | — | — | ✓ | ✓ |
| DPU log < 100 KB | ✓ | ✓ | ✓ | ✓ |

### 10.2 회귀 sweep (모든 phase 후)

```bash
unset BENCH_SPLIT
for rps in 5k 10k 20k 30k 40k 50k; do
    timeout 25 ./test-bench.sh dpumesh $rps 10 8192
done

kubectl set env deployment/bench-dpumesh -n test-bench BENCH_SPLIT=1
for rps in 1k 5k 10k 20k 40k; do
    for size in 256 1024 4096 7000; do
        timeout 25 ./test-bench.sh dpumesh $rps 10 $size
    done
done
```

### 10.3 Deploy / hang protocol

(Memory: `feedback_deploy_script`, `feedback_hang_diagnose_first`, `feedback_test_loop_abort`)

```bash
./test-bench.sh deploy   # 항상 이것만

# DPU log 모니터링
ssh DPU 'wc -c /tmp/dpumesh_dpu_bench.log'
ssh DPU 'sudo truncate -s 0 /tmp/dpumesh_dpu_bench.log'

# Hang 시: 로그부터 캡처
ssh DPU 'cat /tmp/dpumesh_dpu_bench.log | head -200'
kubectl logs -n test-bench -l app=bench-dpumesh --tail=50
kubectl logs -n test-bench -l app=echo-dpumesh --tail=50
kubectl get pods -n test-bench
ssh DPU 'ps -C dpumesh_dpu'
```

### 10.4 Timeout 규칙

Memory `feedback_test_timeout`: expected duration + 10s.

---

## 11. Glossary

| Term | Meaning |
|---|---|
| `mesh_req_id` | 64bit `{src_id, seq}`. globally unique RPC id |
| **src_id** | DPU가 register 시 부여한 stable src pod id (32bit) |
| **seq** | src 내 monotonic counter |
| **TX pool** | Host 송신. body=`dma_buffer`, hdr=`hdr_tx_buffer` |
| **RX buffer** | 수신. body=`rx_dma_buffer`, hdr=`hdr_rx_buffer` |
| **expected[src]** | dst가 src별로 demux된 ordered ring buffer. hdr arrival에 enqueue, chunk consume에 pop |
| **parked_chunks[src]** | chunk가 hdr보다 일찍 도착해서 park된 list. drain_parked가 hdr arrival에 깨움 |
| **desc.mmap override** | forward의 src mmap override (Phase 1) |
| **desc.dst_mmap override** | reverse / direct의 dst mmap override (Phase 2, 4) |
| **hdr_builder** | src의 hdr 누적 buffer (single, DPU 단일 target) |
| **chunk_builder[dst]** | src의 dst별 body 누적 buffer. dst→이 dst 호출 순서대로 append |
| **owner_req_id** | builder flush 시 batch의 tx_slot을 어느 user pending에 묶을지 — 첫 entry의 user req_id |
| **TX_ACK** | DPU → src. forward 처리 완료, tx_slot 해제 가능. pool_type 필드로 어느 풀 |
| **CASE_DIRECT** | Phase 4의 host→host direct DMA flag. DPU 우회 |
| **Grant / credit** | Phase 4 flow control. dst가 src에 partition free 영역 알림 |

---

## 12. File index — phase별 touch list

| File | P1 | P2 | P3 | P4 |
|---|---|---|---|---|
| `doca/dpumesh_common.h` | POOL_HOST_TX_HDR | — | — | CASE_DIRECT |
| `doca/comch_common.h` | DMA_HOST_TX_HDR_BUFFER + tx_ack pool_type + req_id | DMA_HOST_RX_HDR_BUFFER | — | PEER_TOPOLOGY, GRANT |
| `doca/comch_common.c` | mmap_type 처리 | mmap_type 처리 | — | peer broadcast |
| `doca/object.h` | remote_hdr_* | host_hdr_rx_* | — | — |
| `doca/dpu_worker.c` | tx_ack pool_type | dpu_flush_hdr_outbound dst_mmap | per-dst FIFO outbound | direct path completion |
| `doca/dpa_common.h` | — | dma_desc.dst_mmap + dst_addr | — | — |
| `doca/device/dpa_kernel.c` | forward src override | reverse dst override | — | CASE_DIRECT 분기 |
| `transport/dpumesh.h` | hdr API | — | mesh_req_id 64bit, send_request 통합 | peer table API |
| `transport/dpumesh_doca.c` | hdr pool infra | hdr_rx mmap | chunk + expected + parked + sweep | peer table + chunk_flush direct + credit |
| `doca/mesh.h` | — | — | mesh_chunk_header 12B, hdr fmt | — |
| `doca/comch_server.c` | — | — | — | peer broadcast on register |
| `bench/bench_dpumesh.c` | — | — | 64bit req_id + send_request | — |
| `bench/echo_dpumesh.c` | — | — | 64bit req_id + send_request | — |
| `test-bench.sh` | — | — | — | (echo-tcp image fix 별도 task) |

---

## 13. 현재 working tree 상태 (2026-05-13)

세션 중 진행되다 멈춘 변경들이 working tree에 남아 있음.

**옵션 A — clean reset 권장**:
```bash
git stash    # 또는 git checkout v1.0.0 후 새 브랜치
```

**옵션 B — 이어가기**:
이미 적용된 변경:
- Phase A (hdr_builder + OP_HDR_BATCH parser) 구현됨 — 그러나 body TX pool 사용 중
- Phase B (chunk_builder + OP_CHUNK, 옛 chunk format) 구현됨 — 32bit req_id, single src 가정
- BENCH_SPLIT env switch
- 이번 세션에서 batch_req_id 제거 + owner_req_id 도입 (incomplete)
- POOL_HOST_TX_HDR / DMA_HOST_TX_HDR_BUFFER / TX_ACK pool_type 필드 추가
- DPU pod_state에 remote_hdr_mmap 필드 추가
- ctx에 hdr_tx_mmap struct doca_mmap * 필드 추가
- **빌드 안 됨**: hdr_tx_mmap이 init/cleanup에 wire 안 됨, dpumesh_hdr_tx_alloc 함수 없음, DPA kernel 미수정

**권장: 옵션 A (clean reset)**. 이 plan을 따라 차근차근 phase 1부터.

옵션 B로 갈 거면 위 incomplete 변경들을 마무리해야 함 + chunk format / req_id가 64bit로 바뀌어야
하니 어차피 큰 surgery 필요. clean reset이 낫다.

### 13.1 별개 task

- `docker.io/bench/echo-tcp:latest` rapids4 노드에 import 안 됨. `test-bench.sh` 의 image import가
  모든 노드 대상이거나 registry 경유로 변경 필요. 5분 task. Phase 진행과 무관.

---

## 14. 다음 step

1. 이 plan을 사용자가 검토 ✓ (이미 결정 완료)
2. 현재 working tree 처리 (옵션 A — `git stash` 또는 v1.0.0 baseline 으로 reset)
3. Phase 1 시작 → 검증 → Phase 2 → Phase 3 → Phase 4
4. 각 phase 후 Hard Rule 1~8 통과 확인
5. Phase 4 완료 후 throughput 측정 + legacy 회귀 0 확인
6. 그 후 L7 hook (metric, tracing, policy), cross-node mTLS 등 가치 추가

---

*문서 끝 (v2, 2026-05-14)*
