#ifndef OBJECT_H_
#define OBJECT_H_

#include <pthread.h>
#include <stdatomic.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

#include "comch_server.h"
#include "comch_common.h"
#include "dpumesh_common.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct doca_dpa;
struct dma_ring;
typedef uint64_t doca_dpa_dev_buf_arr_t;
/* doca_dpa_dev_mmap_t is 32-bit in the SDK (doca_dpa_dev_buf.h:35) — must
 * stay uint32_t to match the dma_desc.mmap field at offset 0 (followed by
 * the 64-bit addr at offset 4). */
typedef uint32_t doca_dpa_dev_mmap_t;

/* Deferred completion queue — DPU only.
 * Consumer callback enqueues; main loop drains.
 * Single-threaded (same DPU worker), so no lock needed.
 * Sized with headroom so in-flight recv tasks across all active EUs cannot
 * overflow it above BP_HIGH even at MAX_DPA_RINGS active EUs. */
#define DPU_COMP_QUEUE_SIZE 16384

#define COMP_ENTRY_FORWARD     0  /* Forward DMA completed (CPU→DPU), needs TX_ACK + reverse route */
#define COMP_ENTRY_REV_NOTIFY  1  /* Reverse DMA completed (DPU→CPU), needs Host notification */

typedef struct {
    uint8_t  entry_type;   /* COMP_ENTRY_FORWARD or COMP_ENTRY_REV_NOTIFY */
    int32_t  src_pod_id;
    int32_t  dst_pod_id;   /* FORWARD: DMESH_POD_BLANK -> resolve dst_service */
    int16_t  src_service;  /* caller service (opaque passthrough) */
    int16_t  dst_service;  /* callee service (routing input when dst_pod_id==BLANK) */
    uint16_t src_port;     /* sender port (opaque passthrough) */
    uint16_t dst_port;     /* dest port (opaque passthrough; PORT_BLANK -> accept queue on host) */
    uint16_t seq;          /* per-conn sequence (opaque passthrough) */
    uint32_t length;
    uint32_t buf_offset;   /* FORWARD: offset in pod's RX DMA buffer; REV_NOTIFY: pos in Host RX buf */
    uint8_t  route_group;  /* FORWARD: route-affinity key (0 = normal LB); REV_NOTIFY: unused */
    int32_t  pod_idx;      /* FORWARD: index into pods[]; REV_NOTIFY: unused (-1) */
} dpu_comp_entry_t;

typedef struct {
    dpu_comp_entry_t entries[DPU_COMP_QUEUE_SIZE];
    uint32_t head;  /* dequeue index */
    uint32_t tail;  /* enqueue index */
} dpu_comp_queue_t;

/* ===== L7-readiness routing hook (plan.md) =====
 * Seam for a FUTURE Envoy-like L7 proxy on the DPU (prev/architecture.md):
 * called once per BLANK-dst forward DATA message, BEFORE the default L4 route,
 * with the message BODY readable in the src pod's staging buffer (the forward
 * DMA has landed — the same completion-after-data ordering the in-place reverse
 * DMA already relies on). Contract (v1):
 *   - body is READ-ONLY and valid only for the duration of the call;
 *   - return a live pod_id (ANY service — gateway-style content routing is
 *     allowed; the client's dst_service is a hint), or DMESH_ROUTE_DROP to
 *     drop the message (caller TX_ACKs the sender), or DMESH_ROUTE_DEFER to
 *     fall through to the default L4 routing (service_table + route-affinity);
 *   - FINs (0-length) and replies (concrete dst) never reach the hook;
 *   - NULL hook (default) = bit-identical L4 behavior, zero body access.
 * Runs on the single ARM routing thread today; a future multi-thread ROUTER
 * shards this per src pod (prev/architecture.md §3–§5). */
#define DMESH_ROUTE_DROP  (-1)   /* == the existing "unroutable" return */
#define DMESH_ROUTE_DEFER (-2)   /* fall through to the default L4 route */
struct objects;
typedef int32_t (*dmesh_route_fn)(struct objects *objs,
                                  const dpu_comp_entry_t *entry,
                                  const uint8_t *body, uint32_t body_len);

/* Force inline so these collapse into the caller. */
#define CQ_INLINE static inline __attribute__((always_inline))

CQ_INLINE int comp_queue_full(const dpu_comp_queue_t *q) {
    return ((q->tail + 1) % DPU_COMP_QUEUE_SIZE) == q->head;
}

CQ_INLINE int comp_queue_empty(const dpu_comp_queue_t *q) {
    return q->head == q->tail;
}

CQ_INLINE int comp_queue_enqueue(dpu_comp_queue_t *q, const dpu_comp_entry_t *e) {
    if (comp_queue_full(q)) return -1;
    q->entries[q->tail] = *e;
    q->tail = (q->tail + 1) % DPU_COMP_QUEUE_SIZE;
    return 0;
}

CQ_INLINE dpu_comp_entry_t *comp_queue_peek(dpu_comp_queue_t *q) {
    if (comp_queue_empty(q)) return NULL;
    return &q->entries[q->head];
}

CQ_INLINE void comp_queue_dequeue(dpu_comp_queue_t *q) {
    if (!comp_queue_empty(q))
        q->head = (q->head + 1) % DPU_COMP_QUEUE_SIZE;
}

CQ_INLINE uint32_t comp_queue_usage(const dpu_comp_queue_t *q) {
    if (q->tail >= q->head)
        return q->tail - q->head;
    return DPU_COMP_QUEUE_SIZE - q->head + q->tail;
}

/* Backpressure threshold: defer recv task resubmission when queue exceeds
 * BP_HIGH to slow DPA inflow; resume resubmission below BP_LOW. Absolute values
 * so enlarging the queue does not move the trip points. */
#define COMP_QUEUE_BP_HIGH  3072
#define COMP_QUEUE_BP_LOW   2048

/* Max deferred recv tasks. When comp_queue ≥ BP_HIGH the DPA recv-cb stashes
 * completed recv tasks here for the main loop to resubmit once it drains below
 * BP_LOW. Sized to hold every in-flight recv task across all EUs. */
#define MAX_DEFERRED_RECV  8192

/* Max deferred TX_ACK sends. The DPU MUST eventually deliver every TX_ACK —
 * dropping one parks the host's pending entry until the reclaim timeout — so we
 * never drop on the AGAIN path; we defer and retry each main-loop iteration. */
#define MAX_DEFERRED_TX_ACK  16384

/* TX_ACK we couldn't send synchronously because the comch send pool was
 * full. Stored verbatim so the main loop can retry without recomputing. */
typedef struct {
    struct doca_comch_connection *conn;
    uint16_t  port;   /* source endpoint port of the acked leg (TX_ACK key with seq) */
    uint16_t  seq;
} deferred_tx_ack_t;

/* ====== DOCA task pool capacity tracking (check-first model) ======
 * DOCA does not expose in-flight task counts, so we mirror them at
 * submit/completion boundaries. Submits are gated on our counter rather
 * than relying on DOCA_ERROR_AGAIN + retry loop (which can block PE threads).
 *
 * TASK_POOL_MARGIN: safety headroom below max to absorb counter races.
 * Increment-then-check means we may briefly exceed max by the number of
 * concurrent submitters; margin covers that.
 *
 * MAX_CONSUMER_RETRY: fallback stash size for the rare case a gated submit
 * still fails (e.g. transient state during ctx restart). Drained from the
 * main PE loop.
 */
#define TASK_POOL_MARGIN 8
#define MAX_CONSUMER_RETRY 256


/* Per-pod state (DPU only) */
struct pod_state {
    struct doca_comch_connection *connection;
    int32_t pod_id;
    int32_t service_id;     /* this pod's service id (DPU service_table[service_id]=pod_id); SVC_NONE if none */
    int registered;         /* 1 = DMESH_MSG_POD_REGISTER received */
    int dma_ready;          /* 1 = both mmaps arrived, DPA ring added */

    /* EU-sharding: K forward descriptor rings spread across K EUs (K=k_rings).
     * The host exports K DMA_RING mmaps in order; ring_mmap_count counts arrivals
     * so the import handler fills ring_mmaps[0..K-1]. The host TX data buffer
     * (remote_mmap) and host RX buffer are shared/partitioned, not K-plural. */
    int k_rings;                                   /* = objs->k_rings (1 = legacy) */
    struct doca_mmap *ring_mmaps[MAX_EU_PER_POD];  /* Host-exported forward rings */
    int ring_mmap_count;                           /* DMA_RING exports received */
    struct doca_mmap *remote_mmap;   /* Host TX buffer mmap (shared by all K rings) */
    void *remote_addr;
    size_t remote_buf_size;

    /* Per-pod DPA buffer arrays (one over each forward ring) */
    struct doca_buf_arr *buf_arrs[MAX_EU_PER_POD];

    /* Per-pod RX DMA buffer (DPU receives CPU→DPU data here, and under
     * in-place forwarding also serves as the source of reverse DMA when this
     * pod is a forward sender). Slot lifetime is the full RTT, so the host's
     * TX-slot accounting must hold the slot until the reverse TX_ACK lands. */
    struct doca_mmap *local_mmap;
    void *dma_buffer;
    /* DPA handle for local_mmap so the reverse desc enqueued by DPU ARM can
     * carry it as desc->mmap, letting DPA read directly from this pod's
     * dma_buffer. */
    doca_dpa_dev_mmap_t local_mmap_dpa_handle;

    /* === Reverse direction (DPU→CPU) === */

    /* DPU→CPU descriptor rings (K, one per EU). Under in-place forwarding the
     * reverse DMA reads from the source pod's dma_buffer via desc->mmap/addr, so
     * there is no separate destination TX data buffer. The single ARM thread is
     * the sole writer of all K rings, so each stays single-producer (lock-free). */
    struct dma_ring *tx_rings[MAX_EU_PER_POD];
    struct doca_mmap *tx_ring_mmaps[MAX_EU_PER_POD];
    struct doca_buf_arr *tx_buf_arrs[MAX_EU_PER_POD];

    /* Host RX buffer mmap (exported from Host, DPA DMAs into this) */
    struct doca_mmap *host_rx_mmap;
    void *host_rx_addr;
    size_t host_rx_buf_size;

    /* Host's RX RQ depth (= num_slots), derived from host_rx_buf_size.
     * Used by DPA admission gate as the cap on in-flight reverse DMAs. */
    uint32_t rq_depth;

    /* Batched TX_ACK accumulator. Response-forward TX_ACKs destined to THIS pod
     * accumulate here; flushed as one dmesh_batch_tx_ack_msg when full or on the
     * idle (drain-empty, proc==0) flush. Single ARM thread owns this — no lock. */
    struct dmesh_tx_ack_entry txack_batch[BATCH_TXACK_MAX];
    int      txack_batch_n;

    /* Batched REV_DONE accumulator (mirror of txack_batch). Reverse-DMA
     * completions destined to THIS pod accumulate here; flushed as one
     * dmesh_batch_rev_done_msg when full or on the idle (drain-empty, proc==0)
     * flush. Single ARM thread owns this — no lock. */
    struct dmesh_rev_done_entry rev_done_batch[BATCH_REVDONE_MAX];
    int      rev_done_batch_n;
};

/* Data-plane publication gate. `dma_ready` is set with RELEASE at the END of
 * setup_pod_dma (after dma_buffer / local_mmap_dpa_handle / tx_ring are all
 * written). When setup writes run on thread B while the hot path reads run on
 * thread A, the `registered` gate is not sufficient for the data fields, which
 * are written later. ACQUIRE-loading dma_ready before dereferencing dma_buffer/
 * tx_ring/local_mmap_dpa_handle establishes the missing happens-before. */
static inline int pod_data_ready(const struct pod_state *pod) {
    return __atomic_load_n(&pod->dma_ready, __ATOMIC_ACQUIRE);
}

/* ===================================================================
 * DPU connection tracking (model B: the DPU owns every connection)
 * ===================================================================
 * A client addresses a SERVICE (dst_pod=BLANK); the DPU picks a backend per
 * message and owns the "upstream" connection to it. Each upstream gets a
 * DPU-assigned id `up_port` from [DMESH_UPORT_BASE, 65535]; host client conns
 * use [1, DMESH_UPORT_BASE), so a host that is BOTH client and backend
 * (loopback) never collides in its own ports[] table.
 *
 * Toward the backend the DPU rewrites the tuple to src=(client_pod, up_port),
 * dst_port=up_port, so the backend sees the DPU id (not the real client) and
 * replies to it. The reply returns to the DPU (which forwards everything), maps
 * up_port -> (client_pod, client_port) and rewrites dst_port back to the client's
 * real port. The client's TX_ACK is likewise translated up_port -> client_port
 * so the client frees the right slot. (DMESH_UPORT_BASE is defined in the shared
 * dpumesh_common.h so the host side sees the same split.) */

struct dpu_upstream {
    int      in_use;
    int32_t  client_pod;
    uint16_t client_port;   /* the downstream client's REAL port */
    int32_t  backend_pod;
};

/* Reuse lookup: (client_pod, client_port, backend_pod) -> up_port, so a
 * downstream reuses one upstream to a backend instead of creating a new one
 * (and a fresh backend accept) per message. Open-addressed, linear probe. */
#define DPU_CONN_HT_SIZE 131072u   /* power of two, >> max concurrent upstreams */
struct dpu_conn_ht_entry {
    int      in_use;
    int32_t  client_pod;
    uint16_t client_port;
    int32_t  backend_pod;
    uint16_t up_port;
};

struct dpu_conntrack {
    struct dpu_upstream      upstream[65536];      /* by up_port (only [BASE,65535) live) */
    struct dpu_conn_ht_entry ht[DPU_CONN_HT_SIZE]; /* reuse lookup */
    uint32_t next_uport;                           /* round-robin cursor */
};

static inline uint32_t dpu_ct_hash(int32_t cp, uint16_t cport, int32_t bpod) {
    uint32_t h = (uint32_t)cp * 2654435761u;
    h ^= (uint32_t)cport * 40503u;
    h ^= (uint32_t)bpod * 2246822519u;
    return h & (DPU_CONN_HT_SIZE - 1u);
}

/* Return an existing up_port for (cp,cport,bpod), or 0 if none. */
static inline uint16_t dpu_upstream_find(struct dpu_conntrack *ct, int32_t cp,
                                         uint16_t cport, int32_t bpod) {
    uint32_t mask = DPU_CONN_HT_SIZE - 1u, i = dpu_ct_hash(cp, cport, bpod);
    for (uint32_t n = 0; n < DPU_CONN_HT_SIZE; n++) {
        struct dpu_conn_ht_entry *e = &ct->ht[(i + n) & mask];
        if (!e->in_use) return 0;
        if (e->client_pod == cp && e->client_port == cport && e->backend_pod == bpod)
            return e->up_port;
    }
    return 0;
}

/* Allocate a new up_port for (cp,cport,bpod) and index it. Returns 0 if the
 * upstream id space [BASE,65535) is exhausted. */
static inline uint16_t dpu_upstream_create(struct dpu_conntrack *ct, int32_t cp,
                                           uint16_t cport, int32_t bpod) {
    uint32_t span = 65536u - DMESH_UPORT_BASE;
    uint16_t uP = 0;
    for (uint32_t k = 0; k < span; k++) {
        uint32_t p = DMESH_UPORT_BASE + ((ct->next_uport - DMESH_UPORT_BASE + k) % span);
        if (!ct->upstream[p].in_use) { uP = (uint16_t)p; break; }
    }
    if (uP == 0) return 0;
    ct->next_uport = (uP + 1u >= 65536u) ? DMESH_UPORT_BASE : (uint32_t)(uP + 1u);
    ct->upstream[uP].in_use      = 1;
    ct->upstream[uP].client_pod  = cp;
    ct->upstream[uP].client_port = cport;
    ct->upstream[uP].backend_pod = bpod;
    uint32_t mask = DPU_CONN_HT_SIZE - 1u, i = dpu_ct_hash(cp, cport, bpod);
    for (uint32_t n = 0; n < DPU_CONN_HT_SIZE; n++) {
        struct dpu_conn_ht_entry *e = &ct->ht[(i + n) & mask];
        if (!e->in_use) {
            e->in_use = 1; e->client_pod = cp; e->client_port = cport;
            e->backend_pod = bpod; e->up_port = uP;
            break;
        }
    }
    return uP;
}

/* Free an upstream (on close/FIN): clear the slot + remove its reuse entry
 * (backward-shift so the linear-probe chain stays intact — no tombstones). */
static inline void dpu_upstream_free(struct dpu_conntrack *ct, uint16_t uP) {
    if (uP < DMESH_UPORT_BASE || !ct->upstream[uP].in_use) return;
    int32_t  cp    = ct->upstream[uP].client_pod;
    uint16_t cport = ct->upstream[uP].client_port;
    int32_t  bpod  = ct->upstream[uP].backend_pod;
    ct->upstream[uP].in_use = 0;

    uint32_t mask = DPU_CONN_HT_SIZE - 1u, i = dpu_ct_hash(cp, cport, bpod);
    uint32_t idx = UINT32_MAX;
    for (uint32_t n = 0; n < DPU_CONN_HT_SIZE; n++) {
        uint32_t p = (i + n) & mask;
        if (!ct->ht[p].in_use) break;
        if (ct->ht[p].client_pod == cp && ct->ht[p].client_port == cport &&
            ct->ht[p].backend_pod == bpod) { idx = p; break; }
    }
    if (idx == UINT32_MAX) return;
    uint32_t hole = idx, p = (idx + 1u) & mask;
    while (ct->ht[p].in_use) {
        uint32_t home = dpu_ct_hash(ct->ht[p].client_pod, ct->ht[p].client_port,
                                    ct->ht[p].backend_pod);
        if (((p - home) & mask) >= ((p - hole) & mask)) {
            ct->ht[hole] = ct->ht[p];
            hole = p;
        }
        p = (p + 1u) & mask;
    }
    ct->ht[hole].in_use = 0;
}

struct objects {
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    union {
        struct doca_comch_server *cc_server;
        struct doca_comch_client *cc_client;
    };
    struct doca_comch_connection *connection;  /* primary (first) connection */

    /* Host-only fields (used by dpumesh_doca.c client side) */
    struct doca_mmap *local_mmap;
    void *dma_buffer;
    /* Set by the client recv callback when a DMESH_MSG_POD_ASSIGNED arrives at
     * init; the register wait loop polls it. -1 = not yet assigned. Single init
     * thread drives doca_pe_progress, so the callback runs synchronously. */
    int32_t assigned_pod_id;

    /* DPA (shared device, N EU threads for multi-EU data plane).
     *
     * num_dpa_threads (= N, clamp [1, MAX_DPA_RINGS]) EU threads share ONE
     * doca_dpa device (`dpa`). Each EU k owns its own dpa_threads[k]
     * (doca_dpa_thread + arg) and its own 1c/1p comch channel dpa_comches[k].
     * A pod's K forward rings map to EUs (pod_id*K + j) % num_dpa_threads (ring j). The DPU side
     * stays single-threaded: all N recv-msgq consumers connect to the one
     * consumer_pe, so one pe_progress drains every channel into the single
     * comp_queue — no lock, tx_ring stays single-producer.
     *
     * Kept as pointer arrays (not inline) so host-side translation units that
     * include object.h never pull in doca_dpa.h. */
    struct doca_dpa *dpa;                                   /* shared DPA device */
    struct dmesh_doca_dpa_thread *dpa_threads[MAX_DPA_RINGS];
    struct dmesh_doca_dpa_comch  *dpa_comches[MAX_DPA_RINGS];
    int num_dpa_threads;                                    /* N */
    int k_rings;                            /* K = rings per pod, spread across K EUs (1 = legacy) */
    int dpu_ready;   /* 0 until DPA + msgq init done. Gates setup_pod_dma so a fast
                      * host whose mmaps arrive DURING init (before the DPA msgq is
                      * up) doesn't setup too early; those pods run in a deferred
                      * pass in run_dpu_worker once this is published. */
    /* Route affinity (large-message SAR + dmesh_pin_route): (dst_service, route_group
     * byte) -> pinned backend pod, so every message sharing a non-zero key routes to ONE
     * backend. Keyed BY SERVICE: group ids are claimed from per-channel rolling counters,
     * so two processes (or one process after 255 claims) reuse the same byte — scoping the
     * pin to the message's dst_service confines a collision to same-service traffic (a
     * balance skew) and makes cross-service redirection impossible. Overwrite-on-reuse —
     * self-healing (a stale pin only risks a suboptimal same-service backend). 128×256×4B
     * = 128 KB. Single ARM thread → no lock. -1 = unset. */
    int32_t  route_group_backend[POD_ID_SPACE][256];
    /* Test-only per-message round-robin LB across a fixed backend list (env DPUMESH_LB_RR,
     * e.g. "11,13,14") so route-affinity can be exercised under real scatter. count 0 =
     * normal service_table routing (production). */
    int32_t  lb_rr_pods[8];
    int      lb_rr_count;
    uint32_t lb_rr_cursor;
    /* L7-readiness routing hook (see typedef above). NULL (production default) =
     * bit-identical L4 routing, body never touched. Installed at init only (env),
     * before any traffic — no synchronization needed. l7_demo_* backs the TEST
     * content-router route_l7_demo (env DPUMESH_L7_DEMO="svc[,svc...]"). */
    dmesh_route_fn route_fn;
    int32_t  l7_demo_svcs[8];
    int      l7_demo_n;
    int dpa_thread_running[MAX_DPA_RINGS];  /* per-EU: 1 = thread k started */
    int dpa_thread_running_any;             /* 1 = at least one EU started (keepalive guard) */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;

	doca_error_t consumer_result;  /* Last result from a consumer callback (comch_consumer.c). */

    /* RX data hook (comch control path → dpumesh_ctx) */
    void (*rx_data_hook)(void *hook_ctx, const uint8_t *data, uint32_t len);
    void *rx_hook_ctx;

    /* Multi-pod table (DPU only).
     *
     * Concurrency model: lock-free with publication ordering on `registered`.
     *
     *   1. Slots are append-only: pods_add_connection writes into
     *      pods[num_pods] then increments num_pods. Slots are NEVER compacted
     *      or recycled, so &pods[i] is a stable pointer for the lifetime of
     *      the process.
     *   2. `registered` is the publication gate. Writers set every other
     *      field of pod_state FIRST, then publish via
     *      __atomic_store_n(&pods[i].registered, 1, __ATOMIC_RELEASE).
     *      Disconnect tears down in the opposite order: store registered=0
     *      with RELEASE first, then NULL-ify connection/mmap/etc.
     *   3. Readers (find_pod_by_id / find_pod_by_connection / hot path)
     *      observe via __atomic_load_n(&pods[i].registered, __ATOMIC_ACQUIRE).
     *      Seeing registered=1 guarantees visibility of the prior field
     *      writes. Seeing registered=0 is treated as "not found".
     *
     * Single writer (control PE callbacks dispatched on the one PE thread).
     */
    struct pod_state pods[MAX_PODS];
    int num_pods;

    /* O(1) pod_id -> slot-index accelerator for find_pod_by_id. Indexed by
     * pod_id (valid range [0, POD_ID_SPACE)); entry = index into pods[], or -1
     * if no live pod holds that id. Published with RELEASE in pods_register,
     * cleared in pods_remove_connection; read with ACQUIRE in find_pod_by_id
     * which still re-validates pods[idx].registered + pod_id, so the map is only
     * an accelerator (the registered gate remains the authority).
     * Must be initialized to all -1 before the worker starts. */
    int pod_id_to_slot[POD_ID_SPACE];

    /* service_id -> pod_id resolution (DPU routing seam — dpu_route mock).
     * Populated from pods_register(service_id). -1 = unknown (dpu_route returns
     * -1 → the forward entry is DROPPED + the sender TX_ACK'd; NO src-pod fallback).
     * Indexed by service_id [0,POD_ID_SPACE).
     * The future L7 proxy replaces this lookup. Init to all -1 at startup. */
    int service_table[POD_ID_SPACE];

    /* DPU-owned connection tracking (model B). Heap-allocated (large) in
     * run_dpu_worker; single-threaded (control PE thread) so no lock. */
    struct dpu_conntrack *conntrack;

    /* Deferred completion queue (DPU only) */
    dpu_comp_queue_t comp_queue;

    /* Backpressure: deferred consumer recv tasks (DPU only).
     * When comp_queue is nearly full, consumer callbacks defer recv task
     * resubmission here. DPA sees consumer_empty and pauses. Main loop
     * resubmits when queue drains below BP_LOW. */
    struct doca_task *deferred_recv[MAX_DEFERRED_RECV];
    int num_deferred_recv;

    /* Deferred TX_ACK sends (DPU only). When a batched TX_ACK send returns
     * AGAIN (comch send pool full), we stash the ACK here and the main
     * loop retries each iteration after pe_progress drains completions.
     * The DPU is the only authority that can free a host's TX slot, so we
     * never drop a TX_ACK — deferring keeps the contract intact. */
    deferred_tx_ack_t deferred_tx_acks[MAX_DEFERRED_TX_ACK];
    int num_deferred_tx_acks;

    /* ====== In-flight counters for DOCA task pools ======
     * Mirror DOCA's internal task pool usage so submits can be gated
     * BEFORE calling doca_task_submit, avoiding DOCA_ERROR_AGAIN entirely.
     * Counters are atomic because completions fire in PE threads while
     * submits may come from other threads (e.g. host thrift workers). */
    atomic_int send_tasks_in_flight;   /* comch send task pool (server or client) */
    int        send_tasks_max;          /* CC_SEND_TASK_NUM */
    atomic_int recv_tasks_in_flight;   /* comch consumer post_recv task pool */
    int        recv_tasks_max;          /* CC_DATA_PATH_TASK_NUM */

    /* Rare-case retry list: consumer recv tasks whose gated submit still
     * failed (e.g. transient ctx state). Drained from main PE loop. */
    struct doca_task *consumer_retry[MAX_CONSUMER_RETRY];
    int num_consumer_retry;
    pthread_mutex_t consumer_retry_lock;

};

/* Ingest hand-off: the single ARM thread's recv-cb pushes each completion to the
 * comp_queue, drained by the main loop. Returns 0 on enqueue, -1 if full
 * (caller drops + logs). */
static inline int ingest_push(struct objects *objs, const dpu_comp_entry_t *e) {
    return comp_queue_enqueue(&objs->comp_queue, e);
}
static inline uint32_t ingest_usage(struct objects *objs) {
    return comp_queue_usage(&objs->comp_queue);
}

/* ====== Task-pool helpers ======
 * Acquire/release a slot in an atomic in-flight counter. Acquire may fail
 * (return 0) if the pool is full; caller must treat that like DOCA_ERROR_AGAIN
 * and defer. These never sleep, never block, never call DOCA. */
static inline int doca_pool_try_acquire(atomic_int *cnt, int max) {
    int new_count = atomic_fetch_add(cnt, 1) + 1;
    if (new_count > max - TASK_POOL_MARGIN) {
        atomic_fetch_sub(cnt, 1);
        return 0;
    }
    return 1;
}
/* Race-free variant for callers known to be single-threaded (e.g. recv
 * completion → resubmit on the PE thread). Checks against the true pool
 * max without the concurrent-submitter margin, so a bootstrap that filled
 * the pool to `max` can round-trip every released slot back in. */
static inline int doca_pool_try_acquire_exact(atomic_int *cnt, int max) {
    int new_count = atomic_fetch_add(cnt, 1) + 1;
    if (new_count > max) {
        atomic_fetch_sub(cnt, 1);
        return 0;
    }
    return 1;
}
static inline void doca_pool_release(atomic_int *cnt) {
    atomic_fetch_sub(cnt, 1);
}

/* Progress both PEs: control-path PE + consumer PE.
 * consumer_pe must be progressed here to resubmit DPA recv tasks during
 * server_send_msg_to_conn retry loops. Without this, DPA exhausts consumer
 * credits and permanently stalls under load.
 * Safe because server_send_msg_to_conn is only called from:
 *   - process_completion_queue (main loop, not inside any callback)
 *   - server_message_recv_callback (pe callback, not consumer_pe callback)
 * So consumer_pe is never re-entered. */
static inline void progress_all_pes(struct objects *objs) {
    doca_pe_progress(objs->pe);
    if (objs->consumer_pe)
        doca_pe_progress(objs->consumer_pe);
}

void
cleanup_objects(struct objects *objs);

/* Prime task-pool counters (call once from control-path init). */
void
objects_init_task_pools(struct objects *objs);

/* Drain consumer_retry list: try gated-submit each stashed task.
 * Safe to call from any thread that progresses the consumer PE.
 * Returns number of tasks successfully submitted. */
int
objects_drain_consumer_retry(struct objects *objs);

#endif // OBJECT_H_
