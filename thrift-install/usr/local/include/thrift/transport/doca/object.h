#ifndef OBJECT_H_
#define OBJECT_H_

#include <pthread.h>
#include <stdatomic.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#include "comch_server.h"
#include "comch_common.h"
#include "dpumesh_common.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct doca_dpa;
struct dma_ring;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;
/* doca_dpa_dev_mmap_t is 32-bit in the SDK (doca_dpa_dev_buf.h:35) — must
 * stay uint32_t to match the dma_desc.mmap field at offset 0 (followed by
 * the 64-bit addr at offset 4). */
typedef uint32_t doca_dpa_dev_mmap_t;

/* Deferred completion queue — DPU only.
 * Consumer callback enqueues; main loop drains.
 * Single-threaded (same DPU worker), so no lock needed.
 *
 * Multi-EU sizing: the single consumer_pe drains ALL active EU channels into
 * this one queue. comp_queue grows under load until it reaches BP_HIGH, at
 * which point the DPA recv-cb defers recv-task resubmission to brake the DPA.
 * After crossing BP_HIGH, in-flight recv tasks can still deliver, so the queue
 * needs headroom of up to MAX_DPA_RINGS(8) × CC_DPA_MAX_MSG_NUM(1024) = 8192
 * above BP_HIGH before they are all deferred. 16384 (BP_HIGH 3072 + 13312
 * headroom) cannot overflow even at 8 active EUs. (Was 4096 — safe only up to
 * ~2-3 active EUs; at ≥4 EUs the queue could overflow and drop completions →
 * lost requests. The BP_HIGH/BP_LOW trip points are kept at the legacy 3072/
 * 2048 so backpressure timing — and ≤2-EU behaviour — is unchanged.) */
#define DPU_COMP_QUEUE_SIZE 16384

#define COMP_ENTRY_FORWARD     0  /* Forward DMA completed (CPU→DPU), needs TX_ACK + reverse route */
#define COMP_ENTRY_REV_NOTIFY  1  /* Reverse DMA completed (DPU→CPU), needs Host notification */

typedef struct {
    uint8_t  entry_type;   /* COMP_ENTRY_FORWARD or COMP_ENTRY_REV_NOTIFY */
    int32_t  src_pod_id;
    int32_t  dst_pod_id;
    uint32_t req_id;
    uint32_t length;
    int8_t   flags;
    uint32_t buf_offset;   /* FORWARD: offset in pod's RX DMA buffer; REV_NOTIFY: pos in Host RX buf */
    int32_t  pod_idx;      /* FORWARD: index into pods[]; REV_NOTIFY: unused (-1) */
} dpu_comp_entry_t;

typedef struct {
    dpu_comp_entry_t entries[DPU_COMP_QUEUE_SIZE];
    uint32_t head;  /* dequeue index */
    uint32_t tail;  /* enqueue index */
} dpu_comp_queue_t;

/* always_inline: -O2 was leaving these as separate call frames in perf;
 * forcing inline collapses them into the caller. */
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
 * BP_HIGH to slow DPA inflow; resume resubmission below BP_LOW. Kept as ABSOLUTE
 * values (not a fraction of DPU_COMP_QUEUE_SIZE) so enlarging the queue for
 * multi-EU overflow headroom does NOT move the trip points — backpressure timing
 * and ≤2-EU behaviour stay byte-identical to the legacy 4096-queue (3072/2048). */
#define COMP_QUEUE_BP_HIGH  3072
#define COMP_QUEUE_BP_LOW   2048

/* Max deferred recv tasks. When comp_queue ≥ BP_HIGH the DPA recv-cb stashes
 * completed recv tasks here for the main loop to resubmit once it drains below
 * BP_LOW. Worst case every in-flight recv task across all EUs can be deferred at
 * once = MAX_DPA_RINGS(8) × CC_DPA_MAX_MSG_NUM(1024) = 8192, so this must be ≥
 * that. (Was 1024 — one EU's worth; at ≥4 EUs the overflow dropped recv tasks →
 * shrinking recv capacity → DPA stall.) */
#define MAX_DEFERRED_RECV  8192

/* Max deferred TX_ACK sends. Each entry is a small POD (~32B). The DPU
 * MUST eventually deliver every TX_ACK — dropping one parks the host's
 * pending entry at state=-2 until the 2-second collision-wait reclaim,
 * which is the source of the long-tail-latency cliff observed under
 * saturation. So we never drop on the AGAIN path; we defer and retry
 * each main-loop iteration. Sized large enough to absorb a few hundred
 * ms of saturation-burst at 40k+ RPS. */
#define MAX_DEFERRED_TX_ACK  16384

/* TX_ACK we couldn't send synchronously because the comch send pool was
 * full. Stored verbatim so the main loop can retry without recomputing. */
typedef struct {
    struct doca_comch_connection *conn;
    uint32_t  req_id;
    int32_t   dst_pod_id;
} deferred_tx_ack_t;

/* ====== DPU ARM functional-split modes (DPUMESH_SPLIT_SEND) ======
 *   0 SPLIT_OFF   — single worker thread (default, byte-identical to legacy).
 *   1 SPLIT_SENDS — sends-only cut: A = consumer_pe drain + route + reverse-DMA
 *                   enqueue; B = comch sends. A→B hand-off = send_spsc (send_req).
 *                   (Measured a throughput no-op: sends aren't the binding work.)
 *   2 SPLIT_REBAL — rebalanced cut: A = consumer_pe drain ONLY (feed work_spsc);
 *                   B = route + reverse-DMA enqueue + inline comch sends.
 *                   A→B hand-off = work_spsc (raw dpu_comp_entry_t). Tests whether
 *                   the route/reverse half is the per-RTT cap vs the ingest funnel. */
#define SPLIT_OFF    0
#define SPLIT_SENDS  1
#define SPLIT_REBAL  2
/*   3 SPLIT_SHARD  — N-way sharded control plane (multi-EU). Thread A drains
 *                   consumer_pe and routes each completion to
 *                   shard_work[effdst % num_dpa_threads]; worker k drains
 *                   shard_work[k], routes + posts reverse DMA to its EU's
 *                   tx_rings (sole writer), and hands egress sends to
 *                   shard_send[k]; the SENDER drains all shard_send[k] into the
 *                   single cc_server. Each shard_work[k]/shard_send[k] is a
 *                   strict 1-producer/1-consumer SPSC. dst is known at ingress
 *                   (host-stamped / mock dpu_route), so no N^2 ROUTER shuffle is
 *                   needed; real L7 (dst from body) would reinstate it. */
#define SPLIT_SHARD  3

/* ====== A→B egress send hand-off (SPLIT_SENDS) ======
 * Thread A (consumer_pe) drains DPA→DPU completions, routes, posts reverse DMA
 * to the destination tx_ring, and on a send pushes a send_req here instead of
 * calling the comch send path. Thread B (objs->pe) drains this ring and submits
 * the actual comch sends (DMA_COMPLETION + TX_ACK) + reaps send completions.
 * A is the sole producer (tail), B the sole consumer (head) — bounded SPSC,
 * atomic head/tail, no lock. Sized ≥ 2× DPU_COMP_QUEUE_SIZE so a worst case of
 * "every comp_queue entry is a REV_NOTIFY → 2 sends" still fits without the
 * SPSC filling before the comp_queue. Backpressure is expressed THROUGH
 * comp_queue retention (process_completion_queue returns 0 on SPSC-full),
 * preserving the existing comp_queue≥BP_HIGH → deferred_recv → DPA
 * is_consumer_empty stall as the single rate-matcher. 8192 ≥ 2×BP_HIGH(3072)
 * so the comp_queue trips backpressure before the SPSC saturates. */
#define SEND_SPSC_SIZE 8192    /* power of two */

#define SEND_REQ_DMA_COMPLETION 0
#define SEND_REQ_TX_ACK         1

typedef struct {
    struct doca_comch_connection *conn;
    uint8_t  kind;        /* SEND_REQ_DMA_COMPLETION | SEND_REQ_TX_ACK */
    int8_t   flags;
    int32_t  src_pod_id;
    int32_t  dst_pod_id;
    uint32_t pos;
    uint32_t length;
    uint32_t req_id;
} send_req_t;

typedef struct {
    send_req_t entries[SEND_SPSC_SIZE];
    _Atomic uint32_t head;   /* consumer index (thread B) */
    _Atomic uint32_t tail;   /* producer index (thread A) */
} send_spsc_t;

/* Free-running indices masked on access; usable capacity = SIZE-1. */
CQ_INLINE uint32_t send_spsc_free(const send_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    return (SEND_SPSC_SIZE - 1u) - ((tail - head) & (SEND_SPSC_SIZE - 1u));
}
/* Producer (A): caller must ensure send_spsc_free() ≥ 1 first. */
CQ_INLINE void send_spsc_push(send_spsc_t *q, const send_req_t *r) {
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    q->entries[tail & (SEND_SPSC_SIZE - 1u)] = *r;
    atomic_store_explicit(&q->tail, tail + 1u, memory_order_release);
}
/* Consumer (B): peek head without removing (NULL if empty). */
CQ_INLINE send_req_t *send_spsc_peek(send_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    if (head == tail) return NULL;
    return &q->entries[head & (SEND_SPSC_SIZE - 1u)];
}
/* Consumer (B): pop head after a successful send. */
CQ_INLINE void send_spsc_pop(send_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    atomic_store_explicit(&q->head, head + 1u, memory_order_release);
}

/* ====== A→B completion hand-off (SPLIT_REBAL) ======
 * Raw dpu_comp_entry_t SPSC: thread A's recv-cb pushes every completion here
 * (instead of comp_queue); thread B drains it and does the full route +
 * reverse-DMA enqueue + inline send. comp_queue stays untouched in this mode so
 * SPLIT_OFF/SPLIT_SENDS are unaffected. Backpressure: A's recv-cb gates recv-task
 * resubmission on work_spsc_usage ≥ BP_HIGH (same threshold as comp_queue), so
 * a slow B → work_spsc fills → DPA is_consumer_empty stall (single rate-matcher,
 * mirrors the comp_queue chain). Same size as comp_queue. */
#define WORK_SPSC_SIZE 8192    /* power of two */

typedef struct {
    dpu_comp_entry_t entries[WORK_SPSC_SIZE];
    _Atomic uint32_t head;   /* consumer index (thread B) */
    _Atomic uint32_t tail;   /* producer index (thread A) */
} work_spsc_t;

CQ_INLINE uint32_t work_spsc_usage(const work_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    return (tail - head) & (WORK_SPSC_SIZE - 1u);
}
/* Producer (A): push one completion; returns -1 if full (caller drops, like
 * comp_queue_enqueue), 0 on success. */
CQ_INLINE int work_spsc_push(work_spsc_t *q, const dpu_comp_entry_t *e) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_acquire);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_relaxed);
    if (((tail - head) & (WORK_SPSC_SIZE - 1u)) >= (WORK_SPSC_SIZE - 1u))
        return -1;   /* full */
    q->entries[tail & (WORK_SPSC_SIZE - 1u)] = *e;
    atomic_store_explicit(&q->tail, tail + 1u, memory_order_release);
    return 0;
}
/* Consumer (B): peek head (NULL if empty) — pointer stays valid until pop. */
CQ_INLINE dpu_comp_entry_t *work_spsc_peek(work_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    if (head == tail) return NULL;
    return &q->entries[head & (WORK_SPSC_SIZE - 1u)];
}
/* Consumer (B): pop head after the entry is fully processed. */
CQ_INLINE void work_spsc_pop(work_spsc_t *q) {
    uint32_t head = atomic_load_explicit(&q->head, memory_order_relaxed);
    atomic_store_explicit(&q->head, head + 1u, memory_order_release);
}

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
    char app_name[64];
    int registered;         /* 1 = DMESH_MSG_POD_REGISTER received */
    int dma_ready;          /* 1 = both mmaps arrived, DPA ring added */

    /* Per-pod mmap (Host에서 export, CPU→DPU forward direction) */
    struct doca_mmap *ring_mmap;
    struct doca_mmap *remote_mmap;   /* Host TX buffer mmap */
    void *remote_addr;
    size_t remote_buf_size;

    /* Per-pod DPA buffer array (forward ring에 매핑) */
    struct doca_buf_arr *buf_arr;

    /* Per-pod RX DMA buffer (DPU receives CPU→DPU data here, AND — under
     * in-place forwarding — also serves as the source of reverse DMA when
     * this pod is a forward sender). Lifetime per slot is full RTT instead
     * of forward-only, so host's TX-slot accounting must hold the slot
     * until reverse-completion TX_ACK lands. */
    struct doca_mmap *local_mmap;
    void *dma_buffer;
    /* DPA handle for local_mmap, cached at setup_pod_dma so the reverse
     * desc enqueued by DPU ARM can carry it as desc->mmap, letting DPA
     * read directly from this pod's dma_buffer without going through the
     * destination pod's tx_buffer. */
    doca_dpa_dev_mmap_t local_mmap_dpa_handle;

    /* === Reverse direction (DPU→CPU) === */

    /* DPU→CPU descriptor ring. (The former per-pod 16MB DPU TX *data* buffer —
     * tx_mmap/tx_buffer/tx_buf_size — was removed: under in-place forwarding the
     * reverse DMA reads from the SOURCE pod's dma_buffer via desc->mmap/addr, so
     * the destination pod's TX data region was never read.) */
    struct dma_ring *tx_ring;
    struct doca_mmap *tx_ring_mmap;
    struct doca_buf_arr *tx_buf_arr;

    /* Host RX buffer mmap (exported from Host, DPA DMAs into this) */
    struct doca_mmap *host_rx_mmap;
    void *host_rx_addr;
    size_t host_rx_buf_size;

    /* Host's RX RQ depth (= num_slots), derived from host_rx_buf_size.
     * Used by DPA admission gate as the cap on in-flight reverse DMAs. */
    uint32_t rq_depth;

    /* Batched TX_ACK accumulator (DPUMESH_BATCH_TXACK). Response-forward
     * TX_ACKs destined to THIS pod accumulate here; flushed as one
     * dmesh_batch_tx_ack_msg when full or on the periodic tail-flush. Single
     * ARM thread (split-OFF) owns this — no lock. */
    uint32_t txack_batch[BATCH_TXACK_MAX];
    int      txack_batch_n;
};

/* Data-plane publication gate. `dma_ready` is set with RELEASE at the END of
 * setup_pod_dma (after dma_buffer / local_mmap_dpa_handle / tx_ring are all
 * written). Under DPUMESH_SPLIT_SEND the setup writes run on thread B while the
 * hot path reads run on thread A, so the `registered` gate (published at REGISTER
 * time, before EXPORT_DESC/setup) is NOT sufficient for the data fields — they
 * are written later. ACQUIRE-loading dma_ready before dereferencing dma_buffer/
 * tx_ring/local_mmap_dpa_handle establishes the missing happens-before. In the
 * single-thread (non-split) build this is a plain "is this pod set up" check. */
static inline int pod_data_ready(const struct pod_state *pod) {
    return __atomic_load_n(&pod->dma_ready, __ATOMIC_ACQUIRE);
}

struct objects {
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    union {
        struct doca_comch_server *cc_server;
        struct doca_comch_client *cc_client;
    };
    struct doca_comch_connection *connection;  /* primary (first) connection — backward compat */

    /* Host-only fields (used by dpumesh_doca.c client side) */
    struct doca_mmap *local_mmap;
    struct doca_mmap *remote_mmap;
    void *dma_buffer;
    void *remote_addr;
    size_t remote_buf_size;
    struct dma_ring *dma_ring;
    struct doca_mmap *ring_mmap;    /* used for DMA ring mmap */

    struct doca_buf_arr *buf_arr;

    /* DPA (shared device, N EU threads for multi-EU data plane).
     *
     * num_dpa_threads (= N, from DPUMESH_DPA_THREADS, default 1, clamp
     * [1, MAX_DPA_RINGS]) EU threads share ONE doca_dpa device (`dpa`).
     * Each EU k owns its own dpa_threads[k] (doca_dpa_thread + arg) and its
     * own 1c/1p comch channel dpa_comches[k]. A pod's rings are assigned to
     * EU (pod_id % num_dpa_threads). The DPU side stays single-threaded:
     * all N recv-msgq consumers connect to the one consumer_pe, so one
     * pe_progress drains every channel into the single comp_queue — no lock,
     * tx_ring stays single-producer. Forward-compatible with future DPU
     * multicore: consumer k just moves to ARM thread k.
     *
     * Kept as pointer arrays (not inline) so object.h needs only the
     * forward declarations above — host-side translation units that include
     * object.h never pull in doca_dpa.h. */
    struct doca_dpa *dpa;                                   /* shared DPA device */
    struct dmesh_doca_dpa_thread *dpa_threads[MAX_DPA_RINGS];
    struct dmesh_doca_dpa_comch  *dpa_comches[MAX_DPA_RINGS];
    int num_dpa_threads;                                    /* N */
    int dpa_affinity;                       /* 1 = pin thread k to EU k; 0 = relaxed (DPUMESH_DPA_AFFINITY) */
    int dpa_thread_running[MAX_DPA_RINGS];  /* per-EU: 1 = thread k started */
    int dpa_thread_running_any;             /* 1 = at least one EU started (keepalive guard) */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;

	doca_error_t consumer_result;		  /* Holds result will be updated in consumer callbacks */

    int recv_msg_cnt;                  /* Counts number of messages received by consumer */
    int sent_msg_cnt;

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
     *      the process. Stale comp_queue entries holding pod_idx remain
     *      dereferenceable.
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
     * Single writer (control PE callbacks) is currently assumed. Multi-writer
     * pods_add_connection would need __atomic_fetch_add on num_pods to claim
     * a slot atomically; not needed now since all callbacks dispatch on the
     * one PE thread that runs doca_pe_progress().
     */
    struct pod_state pods[MAX_PODS];
    int num_pods;

    /* O(1) pod_id -> slot-index accelerator for find_pod_by_id (called ~4-6x
     * per RTT on the hot path). Indexed by pod_id (int8 on the wire, valid
     * range [0, POD_ID_SPACE)); entry = index into pods[], or -1 if no live
     * pod holds that id. Published with RELEASE in pods_register, cleared in
     * pods_remove_connection; read with ACQUIRE in find_pod_by_id which still
     * re-validates pods[idx].registered + pod_id, so the map is only an
     * accelerator (the registered gate remains the authority). Sized by the
     * pod_id space, NOT MAX_PODS, so lookups stay O(1) as the pod count grows.
     * Must be initialized to all -1 before the worker starts. */
    int pod_id_to_slot[POD_ID_SPACE];

    /* Deferred completion queue (DPU only) */
    dpu_comp_queue_t comp_queue;

    /* Backpressure: deferred consumer recv tasks (DPU only).
     * When comp_queue is nearly full, consumer callbacks defer recv task
     * resubmission here. DPA sees consumer_empty and pauses. Main loop
     * resubmits when queue drains below BP_LOW. */
    struct doca_task *deferred_recv[MAX_DEFERRED_RECV];
    int num_deferred_recv;

    /* Deferred TX_ACK sends (DPU only). When server_send_tx_ack_to returns
     * AGAIN (comch send pool full), we stash the ACK here and the main
     * loop retries each iteration after pe_progress drains completions.
     * The DPU is the only authority that can free a host's TX slot via
     * the pending mechanism, so dropping a TX_ACK here would force the
     * host into a 2-second register_pending reclaim — observed as a tail
     * latency cliff at saturation. Deferring keeps the contract intact. */
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

    /* ====== Functional pipeline split (DPUMESH_SPLIT_SEND) — DPU only ======
     * Mode int: SPLIT_OFF(0) / SPLIT_SENDS(1) / SPLIT_REBAL(2). Non-zero spawns
     * thread B (run_send_thread, owns objs->pe + send pool); thread A
     * (run_dpu_worker loop) owns consumer_pe. SPLIT_SENDS: A routes + posts
     * reverse DMA, hands sends to B via send_spsc. SPLIT_REBAL: A only drains
     * consumer_pe and hands raw completions to B via work_spsc; B routes + posts
     * reverse DMA + sends inline. */
    int split_send;
    pthread_t send_thread;            /* thread B (SENDER under all split modes) */
    send_spsc_t send_spsc;            /* A→B egress send hand-off (SPLIT_SENDS) */
    work_spsc_t work_spsc;            /* A→B completion hand-off (SPLIT_REBAL) */

    /* ====== SPLIT_SHARD (mode 3) — N-way sharded control plane ======
     * shard_work[k]: thread A (producer) → worker k (consumer). Routed by
     * effdst % num_dpa_threads so worker k handles every pod whose pod_id%N==k,
     * making it the SOLE writer of those pods' tx_rings (single-writer preserved
     * even all-to-all). shard_send[k]: worker k (producer) → SENDER (consumer).
     * Both are the existing lock-free SPSC primitives, one instance per EU. */
    work_spsc_t shard_work[MAX_DPA_RINGS];
    send_spsc_t shard_send[MAX_DPA_RINGS];
    pthread_t   shard_workers[MAX_DPA_RINGS];
    int         shard_worker_core_base;   /* worker k pinned to base+k (-1=relaxed) */

    /* ====== Drain sharding (DPUMESH_DRAIN_SHARDS, SPLIT_SHARD only) ======
     * Split the single consumer_pe completion-drain into M independent drain
     * threads, each owning a contiguous EU-channel group [g*N/M, (g+1)*N/M) on
     * its OWN doca_pe (one-PE-per-thread, the DOCA-idiomatic multi-thread
     * pattern). Removes the single ARM drain as the shared serial point so
     * disjoint pod-pairs drain on independent threads. REQUIRES group-affine
     * traffic (a completion's effective dst%N is in the same group as the EU it
     * arrived on) so each shard_work[k] keeps a single producer (the one drain
     * owning group g). M=1 (default) = original single drain (consumer_pe_shard[0]
     * == consumer_pe). drain[0] runs on the main thread; drain[1..M-1] spawned. */
    struct doca_pe *consumer_pe_shard[MAX_DPA_RINGS];
    pthread_t       drain_threads[MAX_DPA_RINGS];
    pthread_mutex_t consumer_lock_shard[MAX_DPA_RINGS];
    int             num_drain_shards;     /* M */
    /* Serializes consumer_pe access: thread A's doca_pe_progress(consumer_pe)
     * + keepalive producer sends VS thread B's setup-time DPU→DPA msgq sends
     * (ADD_RING/ADD_REV_RING), which progress consumer_pe internally. Held only
     * on the rare setup path + once per A iteration; uncontended on the hot
     * path. Unused when split_send == 0. */
    pthread_mutex_t consumer_lock;
    int arm_core_a;                  /* DPUMESH_ARM_CORE_A (thread A pin, -1 = none) */
    int arm_core_b;                  /* DPUMESH_ARM_CORE_B (thread B pin, -1 = none) */
};

/* Ingest hand-off selector: thread A's recv-cb pushes completions to work_spsc
 * under SPLIT_REBAL, else to comp_queue. Backpressure reads the matching depth.
 * Returns 0 on enqueue, -1 if full (caller drops + logs). */
/* SPLIT_SHARD: pick the worker for a completion by its effective destination pod
 * (echo / dst==src → src). Worker k owns every pod with pod_id%N==k, so this is
 * also the key that keeps each tx_ring single-writer. */
static inline int shard_worker_of(const struct objects *objs, const dpu_comp_entry_t *e) {
    int32_t eff = (e->dst_pod_id < 0 || e->dst_pod_id == e->src_pod_id)
                      ? e->src_pod_id : e->dst_pod_id;
    int n = objs->num_dpa_threads > 0 ? objs->num_dpa_threads : 1;
    return (int)((uint32_t)eff % (uint32_t)n);
}
/* Drain shard owning EU index k: contiguous groups [g*N/M, (g+1)*N/M). */
static inline int drain_group_of_eu(const struct objects *objs, int k) {
    int m = objs->num_drain_shards > 0 ? objs->num_drain_shards : 1;
    int n = objs->num_dpa_threads > 0 ? objs->num_dpa_threads : 1;
    int g = (k * m) / n;
    return g >= m ? m - 1 : g;
}
static inline int ingest_push(struct objects *objs, const dpu_comp_entry_t *e) {
    if (objs->split_send == SPLIT_SHARD)
        return work_spsc_push(&objs->shard_work[shard_worker_of(objs, e)], e);
    if (objs->split_send == SPLIT_REBAL)
        return work_spsc_push(&objs->work_spsc, e);
    return comp_queue_enqueue(&objs->comp_queue, e);
}
static inline uint32_t ingest_usage(struct objects *objs) {
    if (objs->split_send == SPLIT_SHARD) {
        /* Backpressure on the MOST-backed-up worker queue: if any worker falls
         * behind, throttle the DPA so its shard_work can't overflow (→ drop). */
        uint32_t mx = 0;
        int n = objs->num_dpa_threads > 0 ? objs->num_dpa_threads : 1;
        for (int k = 0; k < n; k++) {
            uint32_t u = work_spsc_usage(&objs->shard_work[k]);
            if (u > mx) mx = u;
        }
        return mx;
    }
    if (objs->split_send == SPLIT_REBAL)
        return work_spsc_usage(&objs->work_spsc);
    return comp_queue_usage(&objs->comp_queue);
}

/* True when egress sends must be handed to a SENDER thread via an SPSC rather
 * than submitted inline (SPLIT_SENDS shares one send_spsc; SPLIT_SHARD uses the
 * per-worker shard_send[k] selected by the worker's thread-local). */
static inline int send_via_spsc(const struct objects *objs) {
    return objs->split_send == SPLIT_SENDS || objs->split_send == SPLIT_SHARD;
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
