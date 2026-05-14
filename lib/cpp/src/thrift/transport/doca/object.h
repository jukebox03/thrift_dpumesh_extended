#ifndef OBJECT_H_
#define OBJECT_H_

#include <pthread.h>
#include <stdatomic.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#include "comch_server.h"
#include "dpumesh_common.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct dma_ring;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;
/* doca_dpa_dev_mmap_t is 32-bit in the SDK (doca_dpa_dev_buf.h:35) — must
 * stay uint32_t to match the dma_desc.mmap field at offset 0 (followed by
 * the 64-bit addr at offset 4). */
typedef uint32_t doca_dpa_dev_mmap_t;

#define MAX_CONSUMERS 16

/* Deferred completion queue — DPU only.
 * Consumer callback enqueues; main loop drains.
 * Single-threaded (same DPU worker), so no lock needed. */
#define DPU_COMP_QUEUE_SIZE 4096

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

static inline int comp_queue_full(const dpu_comp_queue_t *q) {
    return ((q->tail + 1) % DPU_COMP_QUEUE_SIZE) == q->head;
}

static inline int comp_queue_empty(const dpu_comp_queue_t *q) {
    return q->head == q->tail;
}

static inline int comp_queue_enqueue(dpu_comp_queue_t *q, const dpu_comp_entry_t *e) {
    if (comp_queue_full(q)) return -1;
    q->entries[q->tail] = *e;
    q->tail = (q->tail + 1) % DPU_COMP_QUEUE_SIZE;
    return 0;
}

static inline dpu_comp_entry_t *comp_queue_peek(dpu_comp_queue_t *q) {
    if (comp_queue_empty(q)) return NULL;
    return &q->entries[q->head];
}

static inline void comp_queue_dequeue(dpu_comp_queue_t *q) {
    if (!comp_queue_empty(q))
        q->head = (q->head + 1) % DPU_COMP_QUEUE_SIZE;
}

static inline uint32_t comp_queue_usage(const dpu_comp_queue_t *q) {
    if (q->tail >= q->head)
        return q->tail - q->head;
    return DPU_COMP_QUEUE_SIZE - q->head + q->tail;
}

/* Backpressure threshold: defer recv task resubmission when queue
 * exceeds this fraction (3/4) to slow DPA inflow. */
#define COMP_QUEUE_BP_HIGH  (DPU_COMP_QUEUE_SIZE * 3 / 4)
#define COMP_QUEUE_BP_LOW   (DPU_COMP_QUEUE_SIZE / 2)

/* Max deferred recv tasks (one per CC_DPA_MAX_MSG_NUM) */
#define MAX_DEFERRED_RECV  1024

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
    uint8_t   pool_type;   /* Phase 1: which TX pool the host should free */
    uint8_t   _pad[3];
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
    char app_name[64];
    int registered;         /* 1 = DMESH_MSG_REGISTER received */
    int dma_ready;          /* 1 = both mmaps arrived, DPA ring added */
    uint32_t remote_consumer_id; /* Host datapath consumer ID (for DPU->Host payload) */

    /* Per-pod mmap (Host에서 export, CPU→DPU forward direction) */
    struct doca_mmap *ring_mmap;
    struct doca_mmap *remote_mmap;   /* Host TX buffer mmap (body pool) */
    void *remote_addr;
    size_t remote_buf_size;

    /* Phase 1 (v2 plan): independent host TX hdr pool. DPA forward kernel
     * picks src mmap via desc->mmap override; when host enqueues a hdr
     * batch, dma_desc.mmap = remote_hdr_dpa_handle below. */
    struct doca_mmap *remote_hdr_mmap;
    void *remote_hdr_addr;
    size_t remote_hdr_buf_size;
    doca_dpa_dev_mmap_t remote_hdr_dpa_handle;

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

    /* DPU TX buffer (DPU writes data here for DPA to DMA to Host) */
    struct doca_mmap *tx_mmap;
    void *tx_buffer;
    size_t tx_buf_size;

    /* DPU→CPU descriptor ring */
    struct dma_ring *tx_ring;
    struct doca_mmap *tx_ring_mmap;
    struct doca_buf_arr *tx_buf_arr;

    /* Host RX buffer mmap (exported from Host, DPA DMAs into this) */
    struct doca_mmap *host_rx_mmap;
    void *host_rx_addr;
    size_t host_rx_buf_size;

    /* Phase 2 (v2 plan): independent host RX hdr buffer. DPU sets
     * dma_desc.dst_mmap = host_hdr_rx_dpa_handle when flushing hdr
     * forward batches via reverse DMA. DPA reverse kernel uses dst_mmap
     * override (falls back to ring->host_mmap = body RX when zero). */
    struct doca_mmap *host_hdr_rx_mmap;
    void *host_hdr_rx_addr;
    size_t host_hdr_rx_buf_size;
    doca_dpa_dev_mmap_t host_hdr_rx_dpa_handle;

    /* Host's RX RQ depth (= num_slots), derived from host_rx_buf_size.
     * Used by DPA admission gate as the cap on in-flight reverse DMAs. */
    uint32_t rq_depth;

    /* === DPU-internal write cursor for reverse DMA ===
     * DPU is a pure forwarder. End-nodes do flow control end-to-end via
     * slot-based admission (slot_count × slot_size ≤ DPU_BUFFER_SIZE), so
     * DPU does not throttle. tx_producer_head only chooses the next physical
     * write offset (with wrap) inside this pod's reverse-DMA staging buffer. */
    uint32_t tx_producer_head;
};

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

    /* DPA (shared, 1 thread for all pods) */
    struct dmesh_doca_dpa_thread *dpa_thread;
	struct dmesh_doca_dpa_comch *dpa_comch;
    doca_dpa_dev_comch_producer_t remote_dpa_producer;
    doca_dpa_dev_completion_t remote_dpa_producer_comp;
    int dpa_thread_running;  /* 1 = DPA thread started */

    /* comch control path related */
    bool server_finish;             /* Controls whether server progress loop should be run */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;

    struct local_mem_bufs *producer_mem;
    struct doca_comch_producer *producer;
    struct doca_pe *producer_pe;

    uint32_t remote_consumer_id;
    doca_error_t producer_result;		  /* Holds result will be updated in producer callbacks */
	bool producer_finish;			  /* Controls whether producer progress loop should be run */
	doca_error_t consumer_result;		  /* Holds result will be updated in consumer callbacks */
	bool consumer_finish;			  /* Controls whether consumer progress loop should be run */

    int recv_msg_cnt;                  /* Counts number of messages received by consumer */
    int sent_msg_cnt;

    long unsigned int start_time_ns;
    long unsigned int end_time_ns;

    /* RX data hook (comch control path → dpumesh_ctx) */
    void (*rx_data_hook)(void *hook_ctx, const uint8_t *data, uint32_t len);
    void *rx_hook_ctx;

    /* Multi-pod table (DPU only) */
    struct pod_state pods[MAX_PODS];
    int num_pods;
    pthread_mutex_t pods_lock;

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
};

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
