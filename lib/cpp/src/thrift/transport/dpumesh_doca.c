/*
 * dpumesh_doca.c - DPUmesh DOCA transport layer implementation
 *
 * NVIDIA DOCA (Comch + DMA) backend for DPUmesh Thrift transport.
 * Replaces the SHM-based dpumesh_shm.c when built with -DWITH_DOCA=ON.
 *
 * Phase 1: TX path only. RX functions are stubs.
 */
#define _GNU_SOURCE

#include "dpumesh.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <errno.h>
#include <unistd.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <time.h>
#include <limits.h>
#include <sched.h>

#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#include "doca/common.h"
#include "doca/object.h"
#include "doca/config.h"
#include "doca/buffer.h"
#include "doca/ring.h"
#include "doca/comch_client.h"
#include "doca/comch_producer.h"
#include "doca/comch_consumer.h"
#include "doca/comch_common.h"
#include "doca/comch_msgq.h"
#include "doca/dma.h"
#include "doca/dpa_common.h"
#include "doca/mesh.h"

DOCA_LOG_REGISTER(DPUMESH_DOCA);

static const char *doca_err_str(doca_error_t rc) {
    return doca_error_get_descr(rc);
}

static void cleanup_ctx(struct dpumesh_ctx *ctx);

/* Phase 1: HDR TX pool API — declared early because cleanup_ctx uses
 * dpumesh_hdr_tx_free during pending teardown (mirror of dpumesh_tx_free). */
int      dpumesh_hdr_tx_alloc(dpumesh_ctx_t *ctx);
uint8_t *dpumesh_hdr_tx_buf(dpumesh_ctx_t *ctx, int slot);
void     dpumesh_hdr_tx_free(dpumesh_ctx_t *ctx, int slot);

/* Phase 3 internal forward declarations — needed because rx_data_hook,
 * dpumesh_init, and cleanup_ctx are all earlier in the file than the
 * mesh-side definitions. */
static void process_hdr_batch(struct dpumesh_ctx *ctx, uint32_t pos, uint32_t dma_len, int32_t src_pod_id);
static void process_chunk(struct dpumesh_ctx *ctx, uint32_t pos, uint32_t dma_len);
static void *flush_timer_fn(void *arg);
static void *sweep_fn(void *arg);
/* Phase 4: 1-credit-per-chunk return helper, defined below near dpumesh_rx_free. */
static inline void rx_dma_credit_return(struct dpumesh_ctx *ctx);

/* ====================================================================
 * dpumesh_ctx — internal state
 * ==================================================================== */

/* RX queue capacity */
/* RX queue between PE thread (producer, drains rx_dma_buffer) and the
 * application accept loop (consumer, e.g. TThreadedServer). Sized to be
 * larger than gateway's admission_cap (900) and the server's worst-case
 * concurrent in-flight, so the PE thread never has to drop on enqueue.
 * 65536 entries × ~200B = ~13MB pure RAM, same scale as `pending` pool. */
#define RX_QUEUE_SIZE 65536

/* Pending response table for client-side request/response matching.
 * Indexed by req_id % MAX_PENDING. Must exceed expected in-flight requests
 * to avoid hash collisions. Pure software array (host RAM only — no HW limit).
 * 65536 entries × ~200B = ~13 MB. */
#define MAX_PENDING 65536

/* === Lightweight wake primitives (futex direct, skip-wake-when-no-waiter) === */

/* Spin disabled. Bench workers share their core with the PE thread; any
 * spin burns CPU on the same scheduler entity, blocking the very wake
 * we're spinning to catch. Always go straight to futex_wait. */
#define PENDING_SPIN_ITER  0

static inline void cpu_pause(void) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

/* Absolute-time FUTEX_WAIT (CLOCK_REALTIME via BITSET flag). Returns 0 on
 * wake, -1 with errno on error (ETIMEDOUT, EAGAIN, EINTR). */
static inline int futex_wait_abs(_Atomic int *uaddr, int val,
                                 const struct timespec *abs_deadline) {
    return (int)syscall(SYS_futex, (void *)uaddr,
                        FUTEX_WAIT_BITSET_PRIVATE | FUTEX_CLOCK_REALTIME,
                        val, abs_deadline, NULL, FUTEX_BITSET_MATCH_ANY);
}

static inline int futex_wake_one(_Atomic int *uaddr) {
    return (int)syscall(SYS_futex, (void *)uaddr,
                        FUTEX_WAKE_PRIVATE, 1, NULL, NULL, 0);
}

static inline int futex_wake_all(_Atomic int *uaddr) {
    return (int)syscall(SYS_futex, (void *)uaddr,
                        FUTEX_WAKE_PRIVATE, INT_MAX, NULL, NULL, 0);
}

/* Pending wait primitive: atomic state acting as a futex word.
 *
 * State machine: -1 = unused, 0 = waiting, 1 = arrived, -2 = timed out
 *
 * Why no pthread_cond_t: pthread_cond_signal in the PE thread's
 * deliver_one_body() became the dominant cost — per body delivered, kernel
 * CFS wake_up_q + cgroup reweight ran. We replaced cond_wait/signal with:
 *   - workers spin-poll `state` briefly (catches fast responses, no syscall)
 *   - on miss they bump `waiters` and futex_wait on `state`
 *   - PE deliver does atomic_store(state=1) and only futex_wake when
 *     waiters > 0 (i.e., spinning workers don't trigger any wake)
 *
 * The mutex is kept for slot-lifecycle ops (tx_slot, hdr_tx_slot, desc)
 * which are infrequent and need atomic compound updates. */
typedef struct {
    pthread_mutex_t lock;          /* protects tx_slot / hdr_tx_slot / desc */
    _Atomic int state;
    _Atomic int waiters;
    sw_descriptor_t desc;
    int tx_slot;
    int hdr_tx_slot;
    struct mesh_req_id req_id_v2;
} dpumesh_pending_t;

/* Wake helpers — single point of truth for "if anyone is parked, prod them".
 * Hot path (deliver_one_body) inlines the waiters check; these are for the
 * mutex-protected cold paths (register / cancel / wait / TX_ACK cleanup). */
static inline void pending_wake_all(dpumesh_pending_t *p) {
    if (atomic_load_explicit(&p->waiters, memory_order_seq_cst) > 0) {
        futex_wake_all(&p->state);
    }
}

/* Atomic-state futex_wait-with-relock for cold-path waiters that hold
 * p->lock. Drops the lock around the syscall, takes it back on return.
 * Loops while state==wait_val; returns 0 when state changes, -1 on timeout
 * (only if abs_deadline != NULL). */
static int pending_wait_state(dpumesh_pending_t *p, int wait_val,
                              const struct timespec *abs_deadline) {
    while (atomic_load_explicit(&p->state, memory_order_acquire) == wait_val) {
        atomic_fetch_add_explicit(&p->waiters, 1, memory_order_seq_cst);
        if (atomic_load_explicit(&p->state, memory_order_acquire) != wait_val) {
            atomic_fetch_sub_explicit(&p->waiters, 1, memory_order_relaxed);
            return 0;
        }
        pthread_mutex_unlock(&p->lock);
        int rc = futex_wait_abs(&p->state, wait_val, abs_deadline);
        int err = errno;
        pthread_mutex_lock(&p->lock);
        atomic_fetch_sub_explicit(&p->waiters, 1, memory_order_relaxed);
        if (rc == -1 && err == ETIMEDOUT) return -1;
    }
    return 0;
}

/* ====== Phase 3: builders + expected ring + parked chunks ====== */

#define MAX_DST_PODS         16
#define EXPECTED_RING_CAP    1024

/* Per-dst builders are sharded into N independent (hdr, chunk) builder
 * pairs to scale lock concurrency past the per-dst single-builder
 * bottleneck. Workers hash to a shard via TLS-cached pthread_self().
 *
 * Cross-shard chunk arrival at dst can interleave with the matching hdr
 * batch from a different shard. process_chunk match path calls
 * drain_parked after consume so that whenever the expected-ring head
 * advances, any matching parked chunk is consumed. With this fix,
 * sharding is correctness-safe under arbitrary interleaving. */
#define MESH_BUILDER_SHARDS  8

/* Unified builder: holds both an hdr batch and a chunk under a single lock.
 * Reason: separate hdr/chunk locks let one worker's hdr_append precede its
 * chunk_append while another worker's full (hdr+chunk) send slips in between
 * — the resulting hdr-order vs chunk-order mismatch desync's the receiver's
 * expected-ring match check. A single lock makes "this send's hdr THEN this
 * send's chunk" atomic, so the per-batch ordering invariant always holds. */
typedef struct {
    pthread_mutex_t    lock;

    /* shared (set on first append, cleared at flush) */
    int32_t            dst_pod_id;
    struct mesh_req_id owner_req_id;
    uint64_t           first_us;
    uint8_t            builder_flags;

    /* hdr batch state — flush goes through hdr_dma_ring */
    int                hdr_tx_slot;
    uint32_t           hdr_cursor;     /* bytes written into hdr_tx slot */
    uint32_t           hdr_num;

    /* chunk state — flush goes through dma_ring */
    int                chunk_tx_slot;
    uint32_t           chunk_cursor;   /* bytes incl. 12B chunk_header */
    uint32_t           chunk_num;
    uint32_t           chunk_first_seq;
} mesh_builder_t;

/* expected_ring[src_id]: per-src queue of (req_id, body_size, ...) seen
 * via hdr_fwd arrivals. dst's chunk parser pops from head when consuming.
 * Fixed-cap ring, no alloc on enqueue. */
typedef struct {
    struct mesh_req_id req_id;
    uint32_t           size;
    int32_t            src_pod_id;
    uint64_t           trace_id;
    uint64_t           span_id;
    uint8_t            flags;
    uint64_t           arrived_us;
} expected_entry_t;

typedef struct {
    expected_entry_t buf[EXPECTED_RING_CAP];
    uint32_t         head;
    uint32_t         tail;
    uint32_t         count;
} expected_ring_t;

/* parked_chunks[src_id]: chunks that arrived before their hdr_fwd. Each
 * node points at the rx_dma_buffer slot still holding the bytes (zero-copy).
 * drain_parked() wakes when matching hdr arrives; sweep() reclaims stuck
 * entries after 1s. */
typedef struct parked_chunk_node {
    uint32_t                  rx_slot;     /* slot index in ctx->rx_dma_buffer (pos / slot_size) */
    uint32_t                  total_size;  /* DMA payload */
    uint32_t                  first_seq;
    uint32_t                  num_bodies;
    int32_t                   src_pod_id;
    uint64_t                  arrived_us;
    struct parked_chunk_node *next;
} parked_chunk_node_t;

typedef struct {
    parked_chunk_node_t *head;
    parked_chunk_node_t *tail;
    uint32_t             count;
} parked_list_t;

/* Phase 4: peer registration table. Host does NOT import the peer's mmap
 * (DOCA driver rejects host→host PCIe peer access). Instead the host only
 * tracks whether a given dst_pod_id has been published, and stamps
 * CASE_DIRECT on chunk descriptors when present. The actual host→host DMA
 * is orchestrated by the DPA forward kernel on the DPU device, which has
 * a separately-resolved peer table (DPU-device DPA handles for every
 * registered pod's host_rx_buffer). */
typedef struct {
    int32_t  pod_id;          /* -1 = empty */
    uint32_t src_id;
    uint64_t write_cursor;    /* monotonic cursor inside peer's rx_buffer (host bookkeeping) */
    uint64_t rx_buf_size;
} peer_info_t;

#define MAX_PEERS_TABLE 16

struct dpumesh_ctx {
    char app_name[64];
    char worker_id[128];
    int  pod_id;
    int  num_slots;
    int  slot_size;
    int  max_descriptors;
    /* DOCA objects */
    struct objects doca_objs;
    void *dma_buffer;          /* Host TX buffer (PCI mmap, CPU→DPU source) */
    struct dma_ring *dma_ring; /* Body forward DMA descriptor ring */
    pthread_mutex_t ring_lock; /* Serializes body dma_ring ops */
    doca_dpa_dev_mmap_t dpa_mmap_handle;  /* DPA handle for local mmap (used in TX descriptors) */

    /* Phase 4: independent forward DMA ring for hdr batches. Decouples
     * hdr/body forward-ring slot pressure entirely — neither's backoff
     * stalls the other. Separate ring_lock so concurrent hdr+body
     * enqueues from chunk_flush (which forces hdr_flush first) don't
     * serialize. */
    struct dma_ring *hdr_dma_ring;
    struct doca_mmap *hdr_ring_mmap_local;  /* host's local mmap for hdr_dma_ring */
    pthread_mutex_t hdr_ring_lock;

    /* Host RX buffer (PCI mmap, DPU→CPU destination) */
    void *rx_dma_buffer;
    struct doca_mmap *rx_dma_mmap;
    size_t rx_dma_buf_size;

    /* Phase 2 (v2 plan): independent host RX buffer for hdr forwards.
     * DPU sets dma_desc.dst_mmap = host_hdr_rx_dpa_handle when flushing hdr
     * batches via reverse DMA, so hdr lands here while body chunks land in
     * rx_dma_buffer. Both buffers carry the same DMA_COMPLETION shape; the
     * OP_HDR_BATCH flag (Phase 3) tells the host which buffer to read from. */
    void *hdr_rx_buffer;
    struct doca_mmap *hdr_rx_mmap;
    size_t hdr_rx_buf_size;

    /* Persistent buffers for initial registration to avoid stack UAF */
    struct dmesh_register_msg reg_msg;
    struct dmesh_pod_consumer_id_msg pod_cid_msg;

    /* TX slot management (body pool) */
    uint8_t *slot_bitmap;
    int      slot_hint;          /* round-robin probe start for tx_alloc */
    pthread_mutex_t slot_lock;
    pthread_cond_t  slot_cond;  /* Signaled when a TX slot is freed */

    /* Phase 1 (v2 plan): independent HDR TX pool. DOCA mmap exported to DPU
     * as DMA_HOST_TX_HDR_BUFFER. DPA forward kernel picks src mmap via
     * desc->mmap override (= hdr_tx_dpa_handle when src_body_pool_type ==
     * POOL_HOST_TX_HDR), so hdr DMAs read out of this pool instead of
     * dma_buffer. Independent bitmap / cond means hdr alloc never blocks
     * on a body-saturated bitmap (Hard Rule #6). */
    void                 *hdr_tx_buffer;
    struct doca_mmap     *hdr_tx_mmap;
    doca_dpa_dev_mmap_t   hdr_tx_dpa_handle;
    uint8_t              *hdr_tx_bitmap;
    int                   hdr_slot_hint; /* round-robin probe start for hdr_tx_alloc */
    pthread_mutex_t       hdr_slot_lock;
    pthread_cond_t        hdr_slot_cond;
    int                   hdr_num_slots;
    int                   hdr_slot_size;

    /* RX buffer pool (independent from TX) */
    void *rx_buffer;
    uint8_t *rx_slot_bitmap;
    pthread_mutex_t rx_slot_lock;
    /* Round-robin hint so rx_slot_alloc doesn't rescan from 0 every call.
     * With 4096 slots and 150k allocs/sec, the linear-from-0 scan burned
     * 300M comparisons/sec inside the lock. Hint walks forward and wraps;
     * common case is 1 probe per alloc when load is below saturation. */
    int      rx_slot_hint;

    /* RX descriptor queue (circular buffer) */
    sw_descriptor_t rx_queue[RX_QUEUE_SIZE];
    int rx_head;
    int rx_tail;
    int rx_count;
    pthread_mutex_t rx_lock;
    pthread_cond_t rx_cond;
    pthread_cond_t rx_not_full;  /* Signaled when rx_count drops, for backpressure */

    /* comch max message size (for RX data validation) */
    uint32_t comch_max_msg_size;

    /* PE progress thread */
    pthread_t pe_tid;
    volatile int pe_running;

    /* Client-side pending response table */
    dpumesh_pending_t pending[MAX_PENDING];
    atomic_uint_fast32_t next_req_id;

    /* ====== Phase 3: header/body split state ====== */
    uint32_t              src_id;          /* this pod's stable src id (= pod_id) */
    atomic_uint_fast32_t  next_seq;        /* mesh_req_id.seq counter */
    mesh_builder_t        builders[MAX_DST_PODS][MESH_BUILDER_SHARDS];
    expected_ring_t       expected[MAX_DST_PODS];     /* indexed by src_id; sparse OK */
    parked_list_t         parked[MAX_DST_PODS];
    pthread_mutex_t       expected_locks[MAX_DST_PODS];
    pthread_t             flush_tid;       /* periodic builder timeout flush */
    pthread_t             sweep_tid;       /* 1s parked/orphan sweep */
    volatile int          mesh_running;

    /* Phase 4: peer table indexed by pod_id (sparse). Populated on
     * DMESH_MSG_PEER_TOPOLOGY arrival. chunk_flush consults this; if a
     * peer entry is present, it issues a CASE_DIRECT host→host DMA via
     * the peer's rx mmap instead of going through DPU staging. */
    peer_info_t           peers[MAX_PEERS_TABLE];
    pthread_rwlock_t      peer_lock;

    /* Phase 4 diag counters — printed every 1s by sweep_fn (delta). */
    atomic_uint_fast64_t  stat_send_req;
    atomic_uint_fast64_t  stat_wait_timeout;
    atomic_uint_fast64_t  stat_chunk_direct;
    atomic_uint_fast64_t  stat_chunk_staging;
    atomic_uint_fast64_t  stat_process_chunk;
    atomic_uint_fast64_t  stat_process_hdr;
    atomic_uint_fast64_t  stat_rx_free;
    atomic_uint_fast64_t  stat_park;
};

/* ====================================================================
 * PE progress thread — drives DOCA progress engine
 * ==================================================================== */

static void *pe_progress_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    /* Distinct thread name so external pinning (test-bench.sh split mode)
     * can find this thread via /proc/$pid/task/$tid/comm and taskset it to
     * a dedicated core. Linux comm is 15 chars + NUL. */
    pthread_setname_np(pthread_self(), "dpumesh_pe");
    struct timespec ts = {0, 1000}; /* 1 µs */
    (void)ts;

    while (ctx->pe_running) {
        int progressed = 0;

        if (ctx->doca_objs.pe)
            progressed += doca_pe_progress(ctx->doca_objs.pe);

        if (ctx->doca_objs.consumer_pe)
            progressed += doca_pe_progress(ctx->doca_objs.consumer_pe);
        (void)progressed;
    }
    return NULL;
}

/* ====================================================================
 * RX data hook — called from PE progress thread via comch callback
 * ==================================================================== */

static int rx_slot_alloc(dpumesh_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->rx_slot_lock);
    int n = ctx->num_slots;
    int start = ctx->rx_slot_hint;
    for (int k = 0; k < n; k++) {
        int i = start + k;
        if (i >= n) i -= n;
        if (ctx->rx_slot_bitmap[i] == 0) {
            ctx->rx_slot_bitmap[i] = 1;
            ctx->rx_slot_hint = (i + 1 == n) ? 0 : i + 1;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&ctx->rx_slot_lock);
    return -1;
}


/*
 * Deliver a fully parsed descriptor to the pending table or RX queue.
 * Common path for both comch-based RX_DATA and DMA-based DMA_COMPLETION.
 */
static void rx_deliver_desc(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc, int slot)
{
    if (desc->flags & OP_RESPONSE) {
        uint32_t idx = desc->req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];

        int s = atomic_load_explicit(&p->state, memory_order_acquire);
        if (s == 0) {
            p->desc = *desc;
            int expected = 0;
            if (atomic_compare_exchange_strong_explicit(
                    &p->state, &expected, 1,
                    memory_order_release, memory_order_acquire)) {
                if (atomic_load_explicit(&p->waiters, memory_order_seq_cst) > 0) {
                    futex_wake_one(&p->state);
                }
                return;
            }
            s = expected;
        }
        if (s == -2) {
            pthread_mutex_lock(&p->lock);
            if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
            if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
            pthread_mutex_unlock(&p->lock);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            int expected = -2;
            if (atomic_compare_exchange_strong(&p->state, &expected, -1)) {
                if (atomic_load_explicit(&p->waiters, memory_order_seq_cst) > 0) {
                    futex_wake_all(&p->state);
                }
            }
            return;
        }
        /* state is -1 (no waiter) or 1 (duplicate) — drop the slot. */
        DOCA_LOG_ERR("RX deliver: OP_RESPONSE for req_id=%u but no waiter (state=%d)",
                     desc->req_id, s);
        pthread_mutex_lock(&ctx->rx_slot_lock);
        ctx->rx_slot_bitmap[slot] = 0;
        pthread_mutex_unlock(&ctx->rx_slot_lock);
    } else {
        pthread_mutex_lock(&ctx->rx_lock);

        /* No blocking in PE callback path: if the RX queue is full, drop
         * immediately so we don't stall the PE thread. Slot-based admission
         * at the producer side (rx_slot_alloc above + sender's tx_alloc)
         * keeps in-flight bounded; blocking here while waiting for consumers
         * starves other PE work and can deadlock under load. */
        if (ctx->rx_count >= RX_QUEUE_SIZE) {
            pthread_mutex_unlock(&ctx->rx_lock);
            DOCA_LOG_ERR("RX deliver: queue full, dropping req_id=%u", desc->req_id);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            return;
        }

        ctx->rx_queue[ctx->rx_tail] = *desc;
        ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
        ctx->rx_count++;
        pthread_cond_signal(&ctx->rx_cond);
        pthread_mutex_unlock(&ctx->rx_lock);
    }
}

/*
 * Parse + deliver one DMA-reverse entry at rx_dma_buffer[pos] whose body
 * length is dma_len. Per-request metadata (req_id, src_pod_id, dst_pod_id,
 * flags) is taken from the comch DMA_COMPLETION message — NOT from the DMA
 * payload. The DMA payload is the body itself (no in-band header).
 * Returns 0 on success, -1 on malformed/undeliverable.
 */
static int process_rx_dma_entry(dpumesh_ctx_t *ctx, uint32_t pos, uint32_t dma_len,
                                uint32_t req_id, int32_t src_pod_id,
                                int32_t dst_pod_id, int8_t flags) {
    if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->rx_dma_buf_size);
        return -1;
    }
    /* Phase 4 fix: legacy 1 RPC = 1 rx_dma slot. Now that dpumesh_rx_free
     * no longer bumps credit (it only clears the staging bitmap), we
     * return the rx_dma credit explicitly here at deliver time. The data
     * has been copied to the staging slot so DPA may reuse this rx_dma
     * slot immediately. */
    rx_dma_credit_return(ctx);
    uint8_t *body = (uint8_t *)ctx->rx_dma_buffer + pos;
    uint32_t body_len = dma_len;

    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("process_rx_dma_entry: no free RX slots, dropping req_id=%u", req_id);
        return -1;
    }
    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    if (body_len > 0)
        memcpy(dst, body, body_len);

    sw_descriptor_t desc;
    memset(&desc, 0, sizeof(desc));
    desc.req_id        = req_id;
    desc.src_pod_id    = src_pod_id;
    desc.dst_pod_id    = dst_pod_id;
    desc.flags         = flags;
    desc.header_buf_slot = -1;
    desc.body_buf_slot = slot;
    desc.body_len      = body_len;
    desc.valid         = 1;

    rx_deliver_desc(ctx, &desc, slot);
    return 0;
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_comch_msg *comch_msg = (const struct dmesh_comch_msg *)data;

    if (comch_msg->type == DMESH_MSG_TX_ACK) {
        /* === TX_ACK: per-request notification that DPU has consumed the
         * forward DMA tied to req_id — host's TX slot for this request can
         * now be released. Pure event signal; no flow-control piggyback. */
        struct dmesh_tx_ack_msg ack;
        if (len < sizeof(ack)) {
            DOCA_LOG_ERR("TX_ACK: too short (len=%u need=%zu)", len, sizeof(ack));
            return;
        }
        memcpy(&ack, data, sizeof(ack));

        uint32_t idx = ack.req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        /* Phase 1: pool_type tells us which pool. Legacy DPUs send pool_type=0
         * (POOL_NONE) which we map to BODY for compat. */
        int is_hdr = (ack.pool_type == POOL_HOST_TX_HDR);
        pthread_mutex_lock(&p->lock);
        /* TX_ACK is unambiguous evidence the DPU is finished reading this
         * slot — free unconditionally. The split-path responder
         * (dpumesh_send_response) doesn't register a waiter, so its pending
         * entries sit at state=-1; gating the free on state ∈ {0,-2} as
         * before would leak every responder slot and exhaust the hdr_tx
         * pool (1024 slots) in ~5s at 10k RPS. State transitions still
         * only apply to 0/-2 since those are the request-side lifecycles. */
        if (is_hdr) {
            if (p->hdr_tx_slot >= 0) {
                dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
                p->hdr_tx_slot = -1;
            }
        } else {
            if (p->tx_slot >= 0) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
            }
        }
        int s = atomic_load(&p->state);
        if (s == -2 && p->tx_slot < 0 && p->hdr_tx_slot < 0) {
            int expected = -2;
            if (atomic_compare_exchange_strong(&p->state, &expected, -1)) {
                if (atomic_load_explicit(&p->waiters, memory_order_seq_cst) > 0) {
                    futex_wake_all(&p->state);
                }
            }
        }
        pthread_mutex_unlock(&p->lock);

        DOCA_LOG_DBG("TX_ACK: freed %s slot for req_id=%u",
                     is_hdr ? "HDR" : "BODY", ack.req_id);
        return;
    }

    if (comch_msg->type == DMESH_MSG_DMA_COMPLETION) {
        /* === Reverse DMA notification (DPU→CPU) ===
         * DPU ARM forwards this after DPA completes DMA from DPU TX buffer
         * to Host RX buffer. Data (body only) is already at rx_dma_buffer[pos].
         * Per-request metadata (req_id, src/dst pod, flags, length) comes
         * from comp itself — not from the DMA payload. */
        struct dmesh_dma_completion_msg comp;
        if (len < sizeof(comp)) {
            DOCA_LOG_ERR("DMA_COMPLETION: too short (len=%u need=%zu)", len, sizeof(comp));
            return;
        }
        memcpy(&comp, data, sizeof(comp));

        uint32_t pos = comp.pos;
        uint32_t dma_len = comp.length;

        if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size) {
            DOCA_LOG_ERR("DMA_COMPLETION: invalid pos=%u len=%u buf_size=%zu",
                         pos, dma_len, ctx->rx_dma_buf_size);
            return;
        }

        /* Phase 3: branch on OP_HDR_BATCH / OP_CHUNK first, then legacy. */
        if (comp.flags & OP_HDR_BATCH) {
            process_hdr_batch(ctx, pos, dma_len, comp.src_pod_id);
            return;
        }
        if (comp.flags & OP_CHUNK) {
            process_chunk(ctx, pos, dma_len);
            return;
        }

        /* Legacy path: 1 RPC = 1 body, treat the full DMA as a single body. */
        if (process_rx_dma_entry(ctx, pos, dma_len,
                                 comp.req_id, comp.src_pod_id,
                                 comp.dst_pod_id, comp.flags) != 0) {
            DOCA_LOG_WARN("DMA_COMPLETION: process_rx_dma_entry failed at pos=%u len=%u",
                          pos, dma_len);
        }
        return;
    }

    if (comch_msg->type == DMESH_MSG_PEER_TOPOLOGY) {
        /* Phase 4 redesign: do NOT attempt to import peer's mmap on host —
         * DOCA driver rejects host→host peer access (DOCA_ERROR_DRIVER).
         * Just record that the peer is registered. The actual host→host
         * DMA is performed by the DPA on the DPU device, which has the
         * peer's DPA handle in its ring info (pushed separately via
         * ADD_PEER). Host only needs the pod_id to flip CASE_DIRECT. */
        const struct dmesh_peer_topology_msg *m =
            (const struct dmesh_peer_topology_msg *)data;
        if (len < sizeof(*m)) {
            DOCA_LOG_ERR("PEER_TOPOLOGY truncated: len=%u", len);
            return;
        }
        if (m->pod_id < 0 || m->pod_id >= MAX_PEERS_TABLE) {
            DOCA_LOG_ERR("PEER_TOPOLOGY: pod_id=%d out of range", m->pod_id);
            return;
        }
        pthread_rwlock_wrlock(&ctx->peer_lock);
        peer_info_t *p = &ctx->peers[m->pod_id];
        p->pod_id = m->pod_id;
        p->src_id = m->src_id;
        p->rx_buf_size = m->rx_buf_size;
        p->write_cursor = 0;
        pthread_rwlock_unlock(&ctx->peer_lock);

        DOCA_LOG_INFO("PEER_TOPOLOGY: registered peer pod_id=%d src_id=%u rx_size=%lu",
                      m->pod_id, m->src_id, (unsigned long)m->rx_buf_size);
        return;
    }

    /* === Legacy comch data path (DMESH_MSG_RX_DATA) === */
    const struct dmesh_rx_data_msg *msg = (const struct dmesh_rx_data_msg *)data;

    DOCA_LOG_DBG("rx_data_hook ENTER: len=%u body_len=%u sizeof_hdr=%zu",
                 len, msg->body_len, sizeof(struct dmesh_rx_data_msg));

    uint32_t expected = (uint32_t)sizeof(struct dmesh_rx_data_msg) + msg->body_len;
    if (len < expected) {
        DOCA_LOG_ERR("RX_DATA: truncated message: got %u, need %u", len, expected);
        return;
    }

    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("RX_DATA: no free RX slots, dropping message");
        return;
    }

    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    memcpy(dst, msg->body, msg->body_len);

    sw_descriptor_t desc;
    memcpy(&desc, msg->desc, sizeof(desc));
    desc.body_buf_slot = slot;

    DOCA_LOG_DBG("rx_data_hook DESC: req_id=%u flags=0x%x dst_pod=%d src_pod=%d slot=%d body_len=%u",
                 desc.req_id, (unsigned)(uint8_t)desc.flags, desc.dst_pod_id, desc.src_pod_id,
                 slot, desc.body_len);

    rx_deliver_desc(ctx, &desc, slot);
}

static void init_config(dpumesh_ctx_t *ctx, const dpumesh_config_t *config, const char *app_name, int worker_num) {
    const char *env_val;

    if (config && config->num_slots > 0)
        ctx->num_slots = config->num_slots;
    else if ((env_val = getenv("DPUMESH_NUM_SLOTS")) != NULL && atoi(env_val) > 0)
        ctx->num_slots = atoi(env_val);
    else
        ctx->num_slots = DPUMESH_NUM_SLOTS_DEFAULT;

    if (config && config->slot_size > 0)
        ctx->slot_size = config->slot_size;
    else if ((env_val = getenv("DPUMESH_SLOT_SIZE")) != NULL && atoi(env_val) > 0)
        ctx->slot_size = atoi(env_val);
    else
        ctx->slot_size = DPUMESH_SLOT_SIZE_DEFAULT;

    if (config && config->max_descriptors > 0)
        ctx->max_descriptors = config->max_descriptors;
    else if ((env_val = getenv("DPUMESH_MAX_DESCRIPTORS")) != NULL && atoi(env_val) > 0)
        ctx->max_descriptors = atoi(env_val);
    else
        ctx->max_descriptors = DPUMESH_MAX_DESCRIPTORS_DEFAULT;

    snprintf(ctx->app_name, sizeof(ctx->app_name), "%s", app_name);
    snprintf(ctx->worker_id, sizeof(ctx->worker_id),
             "%s-worker-%d", app_name, worker_num);

    if ((env_val = getenv("DPUMESH_POD_ID")) != NULL)
        ctx->pod_id = atoi(env_val);
    else
        ctx->pod_id = worker_num;
}

static doca_error_t init_doca_device(dpumesh_ctx_t *ctx) {
    const char *pci_addr = getenv("DPUMESH_PCI_ADDR");
    if (!pci_addr) pci_addr = "94:00.0";

    doca_log_backend_create_standard();
    fprintf(stderr, "[dpumesh] Opening DOCA device at %s...\n", pci_addr);
    return open_doca_device_with_pci(pci_addr, NULL, &ctx->doca_objs.dev);
}

static doca_error_t init_control_path(dpumesh_ctx_t *ctx) {
    doca_error_t result;

    fprintf(stderr, "[dpumesh] Connecting comch client...\n");
    result = init_comch_ctrl_path_client("DPUMesh", &ctx->doca_objs, true);
    if (result != DOCA_SUCCESS) return result;

    ctx->reg_msg.type = DMESH_MSG_REGISTER;
    ctx->reg_msg.pod_id = ctx->pod_id;
    snprintf(ctx->reg_msg.app_name, sizeof(ctx->reg_msg.app_name), "%s", ctx->app_name);

    result = client_send_msg(&ctx->doca_objs, (const char *)&ctx->reg_msg, sizeof(ctx->reg_msg));
    if (result == DOCA_SUCCESS) {
        DOCA_LOG_INFO("Sent REGISTER to DPU: pod_id=%d app=%s", ctx->pod_id, ctx->app_name);
    }
    return result;
}

static doca_error_t init_datapath(dpumesh_ctx_t *ctx) {
    doca_error_t result;

    result = init_comch_datapath_consumer(&ctx->doca_objs);
    if (result != DOCA_SUCCESS) return result;

    if (ctx->doca_objs.consumer != NULL) {
        uint32_t local_consumer_id = 0;
        result = doca_comch_consumer_get_id(ctx->doca_objs.consumer, &local_consumer_id);
        if (result != DOCA_SUCCESS) return result;

        ctx->pod_cid_msg.type = DMESH_MSG_POD_CONSUMER_ID;
        ctx->pod_cid_msg.pod_id = ctx->pod_id;
        ctx->pod_cid_msg.consumer_id = local_consumer_id;
        client_send_msg(&ctx->doca_objs, (const char *)&ctx->pod_cid_msg, sizeof(ctx->pod_cid_msg));
    }

    /* NOTE: init_comch_datapath_producer() removed — CPU→DPU uses DMA ring,
     * DPU→CPU uses reverse DMA. comch datapath producer no longer needed. */

    result = setup_dma_ring(&ctx->doca_objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) return result;
    ctx->dma_ring = ctx->doca_objs.dma_ring;

    /* Phase 4: independent forward DMA ring for hdr batches. */
    result = setup_hdr_dma_ring(&ctx->doca_objs, DMA_RING_SIZE,
                                &ctx->hdr_dma_ring, &ctx->hdr_ring_mmap_local);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate HDR DMA ring: %s", doca_err_str(result));
        return result;
    }

    size_t buf_size = (size_t)ctx->num_slots * ctx->slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->doca_objs.local_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->doca_objs.dma_buffer,
                                       buf_size,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) return result;
    ctx->dma_buffer = ctx->doca_objs.dma_buffer;

    result = export_mmap_to_remote(&ctx->doca_objs, ctx->doca_objs.local_mmap,
                                   ctx->doca_objs.dma_buffer, buf_size,
                                   DMA_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) return result;

    result = doca_mmap_dev_get_dpa_handle(ctx->doca_objs.local_mmap, ctx->doca_objs.dev, &ctx->dpa_mmap_handle);
    if (result != DOCA_SUCCESS) return result;

    /* Allocate Host RX DMA buffer (PCI mmap, DPA writes DPU→CPU data here) */
    ctx->rx_dma_buf_size = buf_size;
    result = alloc_buffer_and_set_mmap(&ctx->rx_dma_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->rx_dma_buffer,
                                       ctx->rx_dma_buf_size,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate Host RX DMA buffer: %s", doca_err_str(result));
        return result;
    }

    /* Export Host RX buffer to DPU so DPA can get mmap handle for reverse DMA */
    result = export_mmap_to_remote(&ctx->doca_objs, ctx->rx_dma_mmap,
                                   ctx->rx_dma_buffer, ctx->rx_dma_buf_size,
                                   DMA_HOST_RX_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("Failed to export Host RX buffer to DPU: %s", doca_err_str(result));
    }

    /* Phase 1: HDR TX pool. Smaller than the body pool — hdr batches are
     * tiny (96 entries × 80B = 7.5KB max per slot). 1024 × 8KB = 8MB total. */
    ctx->hdr_num_slots = 1024;
    ctx->hdr_slot_size = DPUMESH_SLOT_SIZE_DEFAULT;
    size_t hdr_buf_size = (size_t)ctx->hdr_num_slots * ctx->hdr_slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->hdr_tx_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->hdr_tx_buffer,
                                       hdr_buf_size,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate HDR TX buffer: %s", doca_err_str(result));
        return result;
    }

    result = export_mmap_to_remote(&ctx->doca_objs, ctx->hdr_tx_mmap,
                                   ctx->hdr_tx_buffer, hdr_buf_size,
                                   DMA_HOST_TX_HDR_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export HDR TX buffer to DPU: %s", doca_err_str(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(ctx->hdr_tx_mmap, ctx->doca_objs.dev,
                                          &ctx->hdr_tx_dpa_handle);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get DPA handle for HDR TX mmap: %s", doca_err_str(result));
        return result;
    }

    /* Phase 2: HDR RX buffer. Sized to match HDR TX pool — symmetric, 8MB. */
    ctx->hdr_rx_buf_size = (size_t)ctx->hdr_num_slots * ctx->hdr_slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->hdr_rx_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->hdr_rx_buffer,
                                       ctx->hdr_rx_buf_size,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate HDR RX buffer: %s", doca_err_str(result));
        return result;
    }

    result = export_mmap_to_remote(&ctx->doca_objs, ctx->hdr_rx_mmap,
                                   ctx->hdr_rx_buffer, ctx->hdr_rx_buf_size,
                                   DMA_HOST_RX_HDR_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("Failed to export HDR RX buffer to DPU: %s", doca_err_str(result));
    }

    return DOCA_SUCCESS;
}

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;

    init_config(ctx, config, app_name, worker_num);

    if (init_doca_device(ctx) != DOCA_SUCCESS) goto fail;
    if (init_control_path(ctx) != DOCA_SUCCESS) goto fail;
    if (init_datapath(ctx) != DOCA_SUCCESS) goto fail;

    ctx->slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->slot_bitmap) goto fail;
    pthread_mutex_init(&ctx->slot_lock, NULL);
    pthread_cond_init(&ctx->slot_cond, NULL);
    pthread_mutex_init(&ctx->ring_lock, NULL);
    pthread_mutex_init(&ctx->hdr_ring_lock, NULL);

    /* Phase 1: HDR TX bitmap + slot lock (independent from body pool). */
    ctx->hdr_tx_bitmap = (uint8_t *)calloc(ctx->hdr_num_slots, 1);
    if (!ctx->hdr_tx_bitmap) goto fail;
    pthread_mutex_init(&ctx->hdr_slot_lock, NULL);
    pthread_cond_init(&ctx->hdr_slot_cond, NULL);

    /* rx_buffer is the STAGING area for delivered messages — must be separate
     * from rx_dma_buffer (the DMA landing zone). If they share memory, DPU
     * can overwrite slot contents after the worker copies them out, corrupting
     * in-flight messages for the upper layer (seen as "Frame size has negative
     * value" near wrap). */
    {
        size_t rx_buf_bytes = (size_t)ctx->num_slots * ctx->slot_size;
        ctx->rx_buffer = calloc(1, rx_buf_bytes);
        if (!ctx->rx_buffer) goto fail;
    }
    ctx->rx_slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->rx_slot_bitmap) goto fail;
    pthread_mutex_init(&ctx->rx_slot_lock, NULL);

    pthread_mutex_init(&ctx->rx_lock, NULL);
    pthread_cond_init(&ctx->rx_cond, NULL);
    pthread_cond_init(&ctx->rx_not_full, NULL);

    uint32_t max_msg_sz = 0;
    if (doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(ctx->doca_objs.dev), &max_msg_sz) == DOCA_SUCCESS)
        ctx->comch_max_msg_size = max_msg_sz;

    atomic_init(&ctx->next_req_id, 1);
    for (int i = 0; i < MAX_PENDING; i++) {
        pthread_mutex_init(&ctx->pending[i].lock, NULL);
        atomic_init(&ctx->pending[i].state, -1);
        atomic_init(&ctx->pending[i].waiters, 0);
        ctx->pending[i].tx_slot = -1;
        ctx->pending[i].hdr_tx_slot = -1;
    }

    ctx->doca_objs.rx_data_hook = rx_data_hook;
    ctx->doca_objs.rx_hook_ctx = ctx;

    /* === Phase 3: mesh state init === */
    ctx->src_id = (uint32_t)ctx->pod_id;
    atomic_init(&ctx->next_seq, 1);
    for (int i = 0; i < MAX_DST_PODS; i++) {
        for (int s = 0; s < MESH_BUILDER_SHARDS; s++) {
            mesh_builder_t *b = &ctx->builders[i][s];
            pthread_mutex_init(&b->lock, NULL);
            b->dst_pod_id     = -1;
            b->hdr_tx_slot    = -1;
            b->chunk_tx_slot  = -1;
        }
        ctx->expected[i].head = ctx->expected[i].tail = ctx->expected[i].count = 0;
        ctx->parked[i].head = ctx->parked[i].tail = NULL;
        ctx->parked[i].count = 0;
        pthread_mutex_init(&ctx->expected_locks[i], NULL);
    }
    /* Phase 4: peer table init. pod_id = -1 marks empty slot. */
    for (int i = 0; i < MAX_PEERS_TABLE; i++) {
        ctx->peers[i].pod_id = -1;
        ctx->peers[i].write_cursor = 0;
    }
    pthread_rwlock_init(&ctx->peer_lock, NULL);

    ctx->mesh_running = 1;
    if (pthread_create(&ctx->flush_tid, NULL, flush_timer_fn, ctx) != 0) goto fail;
    if (pthread_create(&ctx->sweep_tid, NULL, sweep_fn, ctx) != 0) goto fail;

    ctx->pe_running = 1;
    if (pthread_create(&ctx->pe_tid, NULL, pe_progress_fn, ctx) != 0) goto fail;

    DOCA_LOG_INFO("DPUmesh DOCA initialized: worker=%s pod_id=%d", ctx->worker_id, ctx->pod_id);

    *out = ctx;
    return 0;

fail:
    cleanup_ctx(ctx);
    return -1;
}

static void cleanup_ctx(dpumesh_ctx_t *ctx) {
    if (!ctx) return;

    if (ctx->pe_running) {
        ctx->pe_running = 0;
        pthread_join(ctx->pe_tid, NULL);
    }

    /* === Phase 3: stop mesh threads + release any in-flight builders === */
    if (ctx->mesh_running) {
        ctx->mesh_running = 0;
        pthread_join(ctx->flush_tid, NULL);
        pthread_join(ctx->sweep_tid, NULL);
    }
    for (int i = 0; i < MAX_DST_PODS; i++) {
        for (int s = 0; s < MESH_BUILDER_SHARDS; s++) {
            mesh_builder_t *b = &ctx->builders[i][s];
            if (b->hdr_tx_slot >= 0) {
                dpumesh_hdr_tx_free(ctx, b->hdr_tx_slot);
                b->hdr_tx_slot = -1;
            }
            if (b->chunk_tx_slot >= 0) {
                dpumesh_tx_free(ctx, b->chunk_tx_slot);
                b->chunk_tx_slot = -1;
            }
            pthread_mutex_destroy(&b->lock);
        }
        /* free any parked chunks */
        parked_chunk_node_t *n = ctx->parked[i].head;
        while (n) { parked_chunk_node_t *next = n->next; free(n); n = next; }
        ctx->parked[i].head = ctx->parked[i].tail = NULL;
        pthread_mutex_destroy(&ctx->expected_locks[i]);
    }

    /* Phase 4 redesign: peer table holds no host-side mmap imports, just
     * registration bookkeeping — nothing to destroy beyond the lock. */
    pthread_rwlock_destroy(&ctx->peer_lock);

    /* Free resources BEFORE destroying locks they depend on.
     * Pending cleanup calls dpumesh_tx_free/rx_free which acquire
     * slot_lock/rx_slot_lock. */

    for (int i = 0; i < MAX_PENDING; i++) {
        dpumesh_pending_t *p = &ctx->pending[i];
        pthread_mutex_lock(&p->lock);
        if (atomic_load(&p->state) == 1 && p->desc.body_buf_slot >= 0) {
            dpumesh_rx_free(ctx, p->desc.body_buf_slot);
        }
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        atomic_store(&p->state, -1);
        pthread_mutex_unlock(&p->lock);
        pthread_mutex_destroy(&p->lock);
    }

    /* Destroy DMA landing-zone mmap + buffer (Host RX DMA buffer).
     * Must happen before cleanup_objects destroys the device. */
    if (ctx->rx_dma_mmap) {
        doca_mmap_destroy(ctx->rx_dma_mmap);
        ctx->rx_dma_mmap = NULL;
    }
    if (ctx->rx_dma_buffer) {
        free(ctx->rx_dma_buffer);
        ctx->rx_dma_buffer = NULL;
    }

    /* Phase 1: HDR TX pool teardown — must come before cleanup_objects()
     * destroys the device that the mmap belongs to. */
    if (ctx->hdr_tx_mmap) {
        doca_mmap_destroy(ctx->hdr_tx_mmap);
        ctx->hdr_tx_mmap = NULL;
    }
    if (ctx->hdr_tx_buffer) {
        free(ctx->hdr_tx_buffer);
        ctx->hdr_tx_buffer = NULL;
    }

    /* Phase 2: HDR RX buffer teardown — same ordering requirement. */
    if (ctx->hdr_rx_mmap) {
        doca_mmap_destroy(ctx->hdr_rx_mmap);
        ctx->hdr_rx_mmap = NULL;
    }
    if (ctx->hdr_rx_buffer) {
        free(ctx->hdr_rx_buffer);
        ctx->hdr_rx_buffer = NULL;
    }

    cleanup_objects(&ctx->doca_objs);

    pthread_mutex_destroy(&ctx->ring_lock);
    pthread_mutex_destroy(&ctx->hdr_ring_lock);
    pthread_cond_destroy(&ctx->slot_cond);
    pthread_mutex_destroy(&ctx->slot_lock);
    if (ctx->slot_bitmap) free(ctx->slot_bitmap);

    pthread_mutex_destroy(&ctx->hdr_slot_lock);
    pthread_cond_destroy(&ctx->hdr_slot_cond);
    if (ctx->hdr_tx_bitmap) free(ctx->hdr_tx_bitmap);

    pthread_mutex_destroy(&ctx->rx_slot_lock);
    if (ctx->rx_slot_bitmap) free(ctx->rx_slot_bitmap);
    if (ctx->rx_buffer) {
        free(ctx->rx_buffer);
        ctx->rx_buffer = NULL;
    }
    pthread_mutex_destroy(&ctx->rx_lock);
    pthread_cond_destroy(&ctx->rx_cond);
    pthread_cond_destroy(&ctx->rx_not_full);

    free(ctx);
}

void dpumesh_destroy(dpumesh_ctx_t *ctx) {
    if (!ctx) return;
    DOCA_LOG_INFO("Destroying DPUmesh context: worker=%s", ctx->worker_id);
    cleanup_ctx(ctx);
}

/* ====================================================================
 * TX functions
 * ==================================================================== */

int dpumesh_tx_alloc(dpumesh_ctx_t *ctx) {
    /* Backpressure: block until a TX slot is free. The caller has already
     * committed to this request, so failure here would propagate as a
     * Thrift exception — unwanted.
     *
     * Latency-tuned wait: at 44K RPS the per-request budget is ~23µs, so
     * a 1ms cond_timedwait blocks ~44 requests-worth of progress. We use
     * a 50µs re-poll backstop instead. The cond is signaled directly by
     * tx_free / TX_ACK handlers, so the timedwait fires only when a
     * signal was missed (rare race). */
    pthread_mutex_lock(&ctx->slot_lock);
    for (;;) {
        int n = ctx->num_slots;
        int start = ctx->slot_hint;
        for (int k = 0; k < n; k++) {
            int i = start + k;
            if (i >= n) i -= n;
            if (ctx->slot_bitmap[i] == 0) {
                ctx->slot_bitmap[i] = 1;
                ctx->slot_hint = (i + 1 == n) ? 0 : i + 1;
                pthread_mutex_unlock(&ctx->slot_lock);
                return i;
            }
        }

        /* 50µs re-poll backstop (was 1ms — too coarse for cap-region latency). */
        struct timespec abs_ts;
        clock_gettime(CLOCK_REALTIME, &abs_ts);
        abs_ts.tv_nsec += 50000;  /* 50 µs */
        if (abs_ts.tv_nsec >= 1000000000) {
            abs_ts.tv_nsec -= 1000000000;
            abs_ts.tv_sec += 1;
        }
        pthread_cond_timedwait(&ctx->slot_cond, &ctx->slot_lock, &abs_ts);
    }
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Flow control ensures no stale read — no memset needed */
    pthread_mutex_lock(&ctx->slot_lock);
    ctx->slot_bitmap[slot] = 0;
    pthread_cond_signal(&ctx->slot_cond);
    pthread_mutex_unlock(&ctx->slot_lock);
}

/* ====================================================================
 * Phase 1: HDR TX pool — body's mirror, independent bitmap/lock/cond
 * ==================================================================== */

int dpumesh_hdr_tx_alloc(dpumesh_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->hdr_slot_lock);
    for (;;) {
        int n = ctx->hdr_num_slots;
        int start = ctx->hdr_slot_hint;
        for (int k = 0; k < n; k++) {
            int i = start + k;
            if (i >= n) i -= n;
            if (ctx->hdr_tx_bitmap[i] == 0) {
                ctx->hdr_tx_bitmap[i] = 1;
                ctx->hdr_slot_hint = (i + 1 == n) ? 0 : i + 1;
                pthread_mutex_unlock(&ctx->hdr_slot_lock);
                return i;
            }
        }
        /* 50µs re-poll backstop — matches body pool. */
        struct timespec abs_ts;
        clock_gettime(CLOCK_REALTIME, &abs_ts);
        abs_ts.tv_nsec += 50000;
        if (abs_ts.tv_nsec >= 1000000000) {
            abs_ts.tv_nsec -= 1000000000;
            abs_ts.tv_sec += 1;
        }
        pthread_cond_timedwait(&ctx->hdr_slot_cond, &ctx->hdr_slot_lock, &abs_ts);
    }
}

uint8_t *dpumesh_hdr_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->hdr_num_slots) return NULL;
    return (uint8_t *)ctx->hdr_tx_buffer + ((size_t)slot * ctx->hdr_slot_size);
}

void dpumesh_hdr_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->hdr_num_slots) return;
    pthread_mutex_lock(&ctx->hdr_slot_lock);
    ctx->hdr_tx_bitmap[slot] = 0;
    pthread_cond_signal(&ctx->hdr_slot_cond);
    pthread_mutex_unlock(&ctx->hdr_slot_lock);
}

/* Internal enqueue that lets the caller stamp dst override fields onto the
 * dma_desc. dpumesh_enqueue (public) forwards with zeros — Phase 4's
 * chunk_flush direct path calls this directly with peer's mmap handle.
 *
 * Phase 4: hdr (OP_HDR_BATCH) uses hdr_dma_ring + hdr_ring_lock; body
 * (everything else, including CASE_DIRECT chunk) uses dma_ring + ring_lock.
 * Two rings are fully independent — no slot/credit/backoff coupling. */
static int dpumesh_enqueue_ex(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc,
                              doca_dpa_dev_mmap_t dst_mmap_override,
                              uint64_t dst_addr_override,
                              uint32_t dst_pos_override) {
    struct dma_desc *dma;
    uint32_t ring_slot;

    if (desc == NULL) {
        DOCA_LOG_ERR("ENQUEUE rejected: desc is NULL");
        return -1;
    }

    int is_hdr_ring = (desc->flags & OP_HDR_BATCH) != 0;
    struct dma_ring *ring = is_hdr_ring ? ctx->hdr_dma_ring : ctx->dma_ring;
    pthread_mutex_t *rlock = is_hdr_ring ? &ctx->hdr_ring_lock : &ctx->ring_lock;

    /* Phase 1: pool dispatch. desc->src_body_pool_type tells us which TX
     * pool holds the bytes; defaults to body for legacy callers. The
     * body_buf_slot field names the slot in whichever pool was selected. */
    int is_hdr_pool = (desc->src_body_pool_type == POOL_HOST_TX_HDR);
    int pool_slots = is_hdr_pool ? ctx->hdr_num_slots : ctx->num_slots;
    int pool_slot_size = is_hdr_pool ? ctx->hdr_slot_size : ctx->slot_size;

    if (desc->body_buf_slot < 0 || desc->body_buf_slot >= pool_slots) {
        DOCA_LOG_ERR("ENQUEUE rejected: invalid body_buf_slot=%d (pool=%s slots=%d)",
                     desc->body_buf_slot, is_hdr_pool ? "HDR" : "BODY", pool_slots);
        return -1;
    }

    if (desc->body_len > (uint32_t)pool_slot_size) {
        DOCA_LOG_ERR("ENQUEUE rejected: body_len=%u exceeds slot_size=%d (pool=%s)",
                     desc->body_len, pool_slot_size, is_hdr_pool ? "HDR" : "BODY");
        return -1;
    }

    /* Lock ring access: serializes get_next_dma_desc + descriptor fill + valid=1.
     * Phase 4: separate ring per hdr/body so backoff doesn't cross paths. */
    pthread_mutex_lock(rlock);

    {
        struct timespec backoff = {0, 1000}; /* 1µs initial */
        while (1) {
            dma = get_next_dma_desc(ring);
            if (dma)
                break;
            pthread_mutex_unlock(rlock);
            nanosleep(&backoff, NULL);
            if (backoff.tv_nsec < 50000) /* cap at 50µs */
                backoff.tv_nsec *= 2;
            pthread_mutex_lock(rlock);
        }
    }

    ring_slot = (uint32_t)(dma - ring->descs);

    /* TX slot lifetime is owned by the pending mechanism for BOTH OP_REQUEST
     * (gateway) and OP_RESPONSE (server transport):
     *   - OP_REQUEST: caller registers + attach_tx; wait_response/timeout
     *     paths or TX_ACK handler free the TX slot.
     *   - OP_RESPONSE: caller registers + attach_tx + release_async; TX_ACK
     *     handler frees the TX slot via the deferred state -2 → -1 path. */

    /* Phase 3: DPA-side handles are device-locality bound, so we never
     * write host's hdr_tx_dpa_handle into desc.mmap (the DPU device
     * resolves a different handle, propagated via ring->host_hdr_mmap).
     * For HDR pool we leave desc.mmap = 0 and rely on the OP_HDR_BATCH
     * flag bit + ring->host_hdr_mmap in the DPA forward kernel. desc.addr
     * is the absolute VA inside hdr_tx_buffer — the DPA-side hdr mmap
     * exports the same VA range. */
    dma->mmap = 0;
    if (is_hdr_pool) {
        dma->addr = (uint64_t)ctx->hdr_tx_buffer +
                    ((size_t)desc->body_buf_slot * ctx->hdr_slot_size);
    } else {
        dma->addr = (uint64_t)ctx->dma_buffer +
                    ((size_t)desc->body_buf_slot * ctx->slot_size);
    }
    dma->size = desc->body_len;
    dma->idx  = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;
    /* Phase 4: dst override fields. dpumesh_enqueue (legacy) passes zeros;
     * chunk_flush direct path passes peer's mmap + addr + pos. */
    dma->dst_mmap = dst_mmap_override;
    dma->dst_addr = dst_addr_override;
    dma->dst_pos  = dst_pos_override;

    __sync_synchronize();
    dma->valid = 1;

    DOCA_LOG_DBG("ENQUEUE: req_id=%u slot=%u len=%u ring=%s",
                 desc->req_id, ring_slot, desc->body_len,
                 is_hdr_ring ? "HDR" : "BODY");

    pthread_mutex_unlock(rlock);

    return 0;
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    return dpumesh_enqueue_ex(ctx, desc, 0, 0, 0);
}

/* ====================================================================
 * RX functions
 * ==================================================================== */

int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms) {
    pthread_mutex_lock(&ctx->rx_lock);

    struct timespec ts;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
    }

    while (ctx->rx_count == 0) {
        if (timeout_ms == 0) {
            pthread_mutex_unlock(&ctx->rx_lock);
            return -1;
        } else if (timeout_ms < 0) {
            pthread_cond_wait(&ctx->rx_cond, &ctx->rx_lock);
        } else {
            int rc = pthread_cond_timedwait(&ctx->rx_cond, &ctx->rx_lock, &ts);
            if (rc != 0) {
                pthread_mutex_unlock(&ctx->rx_lock);
                return -1;
            }
        }
    }

    *desc = ctx->rx_queue[ctx->rx_head];
    ctx->rx_head = (ctx->rx_head + 1) % RX_QUEUE_SIZE;
    ctx->rx_count--;
    pthread_cond_signal(&ctx->rx_not_full);

    pthread_mutex_unlock(&ctx->rx_lock);
    return 0;
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
}

/* Phase 4 fix: staging-only free. The rx_buffer staging slot's lifecycle is
 * per-body (deliver_one_body alloc → user free); decoupled from the
 * rx_dma_buffer slot which is per-chunk. Credit return for rx_dma_buffer
 * happens via rx_dma_credit_return() at chunk-release time only, so 1
 * rx_dma slot maps to exactly 1 credit++, not 1+N. */
void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    atomic_fetch_add(&ctx->stat_rx_free, 1);
    pthread_mutex_lock(&ctx->rx_slot_lock);
    ctx->rx_slot_bitmap[slot] = 0;
    pthread_mutex_unlock(&ctx->rx_slot_lock);
}

/* Phase 4: 1 rx_dma_buffer slot released → credit ++ by 1. Called from
 * (a) process_chunk match path (consume_chunk consumes 1 chunk),
 * (b) drain_parked match path,
 * (c) sweep_fn parked-timeout path,
 * (d) legacy process_rx_dma_entry path (1 RPC = 1 chunk-equivalent). */
static inline void rx_dma_credit_return(dpumesh_ctx_t *ctx) {
    if (ctx->dma_ring && ctx->dma_ring->descs) {
        volatile uint64_t *credit =
            (volatile uint64_t *)(ctx->dma_ring->descs + ctx->dma_ring->size);
        __sync_add_and_fetch(credit, 1);
    }
}

/* ====================================================================
 * Query / info functions
 * ==================================================================== */

int dpumesh_get_slot_size(dpumesh_ctx_t *ctx) {
    return ctx->slot_size;
}

int dpumesh_get_notify_fd(dpumesh_ctx_t *ctx) {
    (void)ctx;
    return -1;
}

int dpumesh_get_pod_id(dpumesh_ctx_t *ctx) {
    return ctx->pod_id;
}

const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx) {
    return ctx->worker_id;
}

/* ====================================================================
 * Client-side API
 * ==================================================================== */

uint32_t dpumesh_alloc_req_id(dpumesh_ctx_t *ctx) {
    return atomic_fetch_add(&ctx->next_req_id, 1);
}

/* Legacy register (v1, uint32_t req_id). Same ownership model as the v2
 * variant: caller owns the register/release lifecycle and there is no
 * defensive wait — if the slot is busy, the call fails fast. */
int dpumesh_register_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    int s = atomic_load(&p->state);
    if (s == -2) {
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        s = -1;
    }
    if (s != -1) {
        pthread_mutex_unlock(&p->lock);
        return -1;
    }

    p->tx_slot = -1;
    p->hdr_tx_slot = -1;
    memset(&p->desc, 0, sizeof(p->desc));
    atomic_store_explicit(&p->state, 0, memory_order_release);
    pthread_mutex_unlock(&p->lock);
    return 0;
}

void dpumesh_pending_attach_tx(dpumesh_ctx_t *ctx, uint32_t req_id, int tx_slot) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    p->tx_slot = tx_slot;
    pthread_mutex_unlock(&p->lock);
}

int dpumesh_wait_response(dpumesh_ctx_t *ctx, uint32_t req_id,
                          sw_descriptor_t *resp, int timeout_ms) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);

    struct timespec ts;
    int have_deadline = 0;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        have_deadline = 1;
    }

    while (atomic_load(&p->state) == 0) {
        if (timeout_ms == 0) {
            pthread_mutex_unlock(&p->lock);
            return -1;
        }
        int rc = pending_wait_state(p, 0, have_deadline ? &ts : NULL);
        if (rc == -1) {
            if (atomic_load(&p->state) == 1) break; /* response arrived at boundary */
            /* Timeout: force-free the TX slots; mark state=-2 (late TX_ACK
             * will find tx_slot=-1 and no-op). */
            if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
            if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
            atomic_store(&p->state, -2);
            pending_wake_all(p);
            pthread_mutex_unlock(&p->lock);
            return -1;
        }
    }

    if (atomic_load(&p->state) == 1) {
        *resp = p->desc;
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        pending_wake_all(p);
        pthread_mutex_unlock(&p->lock);
        return 0;
    }

    if (atomic_load(&p->state) != -2) {
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        pending_wake_all(p);
    }
    pthread_mutex_unlock(&p->lock);
    return -1;
}

void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    int s = atomic_load(&p->state);
    if (s == 1) {
        if (p->desc.body_buf_slot >= 0) {
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[p->desc.body_buf_slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        pending_wake_all(p);
    } else if (s == 0 || s == -2) {
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        pending_wake_all(p);
    }
    /* else: state == -1, already clean. */
    pthread_mutex_unlock(&p->lock);
}

/*
 * Asynchronous-release a pending entry without waiting for a response.
 *
 * Used by responder-side code (server transport) that has just enqueued an
 * OP_RESPONSE: it needs the TX slot held until DPA finishes the forward DMA,
 * confirmed by TX_ACK from DPU. This function tells the pending machinery
 * "no response is coming for this req_id; free TX on TX_ACK and clear the
 * entry". This mirrors the gateway's TX-slot lifecycle (early free on
 * TX_ACK arrival) for the responder direction.
 *
 * Only acts on state == 0; other states are left alone since they're
 * already owned by another path (response arrived, cancelled, or unused).
 */
void dpumesh_pending_release_async(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    if (atomic_load(&p->state) == 0) {
        if (p->tx_slot < 0 && p->hdr_tx_slot < 0) {
            atomic_store(&p->state, -1);
            pending_wake_all(p);
        } else {
            /* TX_ACK still pending on at least one pool — defer to -2. */
            atomic_store(&p->state, -2);
        }
    }
    pthread_mutex_unlock(&p->lock);
}

/* ====================================================================
 * Phase 3: header/body split path
 * ==================================================================== */

uint32_t dpumesh_get_src_id(dpumesh_ctx_t *ctx) { return ctx->src_id; }

int dpumesh_resolve(dpumesh_ctx_t *ctx, const char *service) {
    (void)ctx;
    /* Phase 3 static table. Phase 4+ swaps to a DPU-pushed snapshot. */
    if (!service) return -1;
    if (strcmp(service, "bench") == 0) return 10;
    if (strcmp(service, "echo")  == 0) return 11;
    return -1;
}

/* Get / register a pending entry for a 64-bit req_id. The slot index is
 * seq % MAX_PENDING. Multiple srcs cannot collide because each src only
 * registers its own (src_id, seq) pairs in its own ctx — incoming
 * responses from other srcs are matched by full mesh_req_id. */
/* Register a pending entry for a v2-keyed RPC.
 *
 * Ownership model: the caller (send_request) acquires the slot here and
 * is responsible for releasing it via wait_response_v2 (response or
 * timeout) or cancel_pending_v2. Slots are indexed by `seq % MAX_PENDING`.
 *
 * If the slot is busy, register fails fast. Two cases:
 *   (a) state == -2: a prior RPC at this idx timed out. Reclaim it now
 *       (PE already handled rx_slot cleanup; only TX-side slots may need
 *       freeing) and proceed.
 *   (b) state == 0 or 1: a prior caller didn't release. With seq advancing
 *       monotonically, the wraparound to the same idx is naturally spaced
 *       (MAX_PENDING / RPS), so this means another worker is still
 *       holding the slot — return -1 and let the caller surface the
 *       capacity error. There is no defensive wait: callers must own the
 *       full register/release lifecycle. */
static int register_pending_v2(dpumesh_ctx_t *ctx, struct mesh_req_id req_id) {
    uint32_t idx = req_id.seq % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    int s = atomic_load(&p->state);
    if (s == -2) {
        if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
        if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
        atomic_store(&p->state, -1);
        s = -1;
    }
    if (s != -1) {
        pthread_mutex_unlock(&p->lock);
        return -1;
    }

    /* req_id_v2 + slot fields must be visible BEFORE state=0 transitions
     * happen, because PE (lock-free) does an acquire-load on state then
     * uses req_id_v2 for the match check. Release-store on state
     * synchronizes with PE's acquire-load. */
    p->tx_slot = -1;
    p->hdr_tx_slot = -1;
    p->req_id_v2 = req_id;
    memset(&p->desc, 0, sizeof(p->desc));
    atomic_store_explicit(&p->state, 0, memory_order_release);
    pthread_mutex_unlock(&p->lock);
    return 0;
}

/* Attach an HDR or BODY tx_slot to a v2-keyed pending entry. */
static void pending_attach_tx_v2(dpumesh_ctx_t *ctx, struct mesh_req_id req_id,
                                 int slot, int is_hdr) {
    uint32_t idx = req_id.seq % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];
    pthread_mutex_lock(&p->lock);
    if (is_hdr) p->hdr_tx_slot = slot;
    else        p->tx_slot     = slot;
    pthread_mutex_unlock(&p->lock);
}

/* === Builder flush — caller holds the per-builder lock (hb->lock / cb->lock) === */

static int dst_pod_to_idx(int dst_pod_id) {
    if (dst_pod_id < 0 || dst_pod_id >= MAX_DST_PODS) return -1;
    return dst_pod_id;
}

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)(ts.tv_nsec / 1000);
}

#define BUILDER_FLUSH_TIMEOUT_US  1000   /* 1 ms */

/* Snapshot hdr+chunk state, reset, drop the builder lock, and enqueue both
 * descriptors. Caller holds b->lock. On return b->lock is re-acquired.
 *
 * Pairing: both are flushed in a single critical-section snapshot so that
 * the hdr batch's order matches the chunk's body order — required for the
 * dst's expected-ring match to succeed. Either or both may be empty: any
 * side with no entries is skipped. */
static void builder_flush_locked(dpumesh_ctx_t *ctx, mesh_builder_t *b) {
    if ((b->hdr_tx_slot < 0 || b->hdr_num == 0) &&
        (b->chunk_tx_slot < 0 || b->chunk_num == 0)) {
        return;
    }

    /* Snapshot hdr side. */
    int      hdr_slot     = b->hdr_tx_slot;
    uint32_t hdr_len      = b->hdr_cursor;
    uint32_t hdr_num      = b->hdr_num;

    /* Snapshot chunk side. Write the on-wire chunk_header (12B) before we
     * release the lock so the slot's content matches the desc we're about
     * to enqueue. */
    int      chunk_slot   = b->chunk_tx_slot;
    uint32_t chunk_len    = b->chunk_cursor;
    uint32_t chunk_num    = b->chunk_num;
    uint32_t chunk_fseq   = b->chunk_first_seq;
    if (chunk_slot >= 0 && chunk_num > 0) {
        struct mesh_chunk_header *ch =
            (struct mesh_chunk_header *)dpumesh_tx_buf(ctx, chunk_slot);
        ch->src_id     = b->owner_req_id.src_id;
        ch->first_seq  = chunk_fseq;
        ch->num_bodies = chunk_num;
    }

    /* Shared metadata. */
    int32_t            dst_pod  = b->dst_pod_id;
    struct mesh_req_id owner    = b->owner_req_id;
    uint8_t            bflags   = b->builder_flags;

    /* Reset builder. */
    b->dst_pod_id      = -1;
    b->first_us        = 0;
    b->hdr_tx_slot     = -1;
    b->hdr_cursor      = 0;
    b->hdr_num         = 0;
    b->chunk_tx_slot   = -1;
    b->chunk_cursor    = 0;
    b->chunk_num       = 0;
    b->chunk_first_seq = 0;

    /* Peer presence is checked once for chunk path (CASE_DIRECT vs staging). */
    pthread_rwlock_rdlock(&ctx->peer_lock);
    peer_info_t *peer = (dst_pod >= 0 && dst_pod < MAX_PEERS_TABLE)
                            ? &ctx->peers[dst_pod] : NULL;
    int direct_ok = (peer && peer->pod_id == dst_pod);
    pthread_rwlock_unlock(&ctx->peer_lock);

    pthread_mutex_unlock(&b->lock);

    /* Hdr first, then chunk. Both enqueues are independent of b->lock so
     * other workers can keep appending to a fresh batch. */
    if (hdr_slot >= 0 && hdr_num > 0) {
        sw_descriptor_t desc;
        memset(&desc, 0, sizeof(desc));
        desc.header_buf_slot     = -1;
        desc.body_buf_slot       = hdr_slot;
        desc.body_len            = hdr_len;
        desc.req_id              = owner.seq;
        desc.dst_pod_id          = dst_pod;
        desc.src_pod_id          = ctx->pod_id;
        desc.flags               = (bflags & OP_RESPONSE) | OP_HDR_BATCH | CASE_EXTERNAL;
        desc.valid               = 1;
        desc.src_body_pool_type  = POOL_HOST_TX_HDR;
        desc.src_body_pod_id     = ctx->pod_id;
        desc.src_body_buf_slot   = hdr_slot;
        desc.src_header_buf_slot = -1;
        pending_attach_tx_v2(ctx, owner, hdr_slot, /*is_hdr=*/1);
        if (dpumesh_enqueue(ctx, &desc) != 0) {
            uint32_t pidx = owner.seq % MAX_PENDING;
            dpumesh_pending_t *pp = &ctx->pending[pidx];
            pthread_mutex_lock(&pp->lock);
            if (pp->hdr_tx_slot == hdr_slot) pp->hdr_tx_slot = -1;
            pthread_mutex_unlock(&pp->lock);
            dpumesh_hdr_tx_free(ctx, hdr_slot);
        }
    }
    if (chunk_slot >= 0 && chunk_num > 0) {
        sw_descriptor_t desc;
        memset(&desc, 0, sizeof(desc));
        desc.header_buf_slot     = -1;
        desc.body_buf_slot       = chunk_slot;
        desc.body_len            = chunk_len;
        desc.req_id              = owner.seq;
        desc.dst_pod_id          = dst_pod;
        desc.src_pod_id          = ctx->pod_id;
        desc.flags               = (bflags & OP_RESPONSE) | OP_CHUNK |
                                   (direct_ok ? CASE_DIRECT : CASE_EXTERNAL);
        desc.valid               = 1;
        desc.src_body_pool_type  = POOL_HOST_TX_BODY;
        desc.src_body_pod_id     = ctx->pod_id;
        desc.src_body_buf_slot   = chunk_slot;
        desc.src_header_buf_slot = -1;
        atomic_fetch_add(direct_ok ? &ctx->stat_chunk_direct : &ctx->stat_chunk_staging, 1);
        pending_attach_tx_v2(ctx, owner, chunk_slot, /*is_hdr=*/0);
        if (dpumesh_enqueue(ctx, &desc) != 0) {
            uint32_t pidx = owner.seq % MAX_PENDING;
            dpumesh_pending_t *pp = &ctx->pending[pidx];
            pthread_mutex_lock(&pp->lock);
            if (pp->tx_slot == chunk_slot) pp->tx_slot = -1;
            pthread_mutex_unlock(&pp->lock);
            dpumesh_tx_free(ctx, chunk_slot);
        }
    }

    pthread_mutex_lock(&b->lock);
}

/* Append one (hdr_entry, body) pair atomically under b->lock. The single
 * critical section is what guarantees hdr append order matches chunk body
 * order. Caller holds b->lock. Returns 0 on success, -1 on alloc failure. */
static int builder_append_locked(dpumesh_ctx_t *ctx,
                                 mesh_builder_t *b,
                                 struct mesh_req_id req_id,
                                 const char *service,
                                 int dst_pod_id,
                                 const uint8_t *body,
                                 uint32_t body_len,
                                 uint8_t flags) {
    /* If chunk side can't fit this body, flush the pair (both sides) and
     * start fresh. We check chunk only; hdr cap is high enough that chunk
     * always fills first at typical body sizes. */
    if (b->chunk_tx_slot >= 0) {
        uint32_t after = b->chunk_cursor + body_len;
        if (b->chunk_num >= MESH_CHUNK_MAX_BODIES ||
            after > (uint32_t)ctx->slot_size) {
            builder_flush_locked(ctx, b);
        }
    }

    /* Allocate hdr slot if needed. */
    if (b->hdr_tx_slot < 0) {
        int slot = dpumesh_hdr_tx_alloc(ctx);
        if (slot < 0) return -1;
        b->hdr_tx_slot = slot;
        b->hdr_cursor  = 0;
        b->hdr_num     = 0;
    }
    /* Allocate chunk slot if needed. */
    if (b->chunk_tx_slot < 0) {
        int slot = dpumesh_tx_alloc(ctx);
        if (slot < 0) {
            /* leave hdr_tx_slot held; it'll flush on next append or timeout */
            return -1;
        }
        b->chunk_tx_slot   = slot;
        b->chunk_cursor    = sizeof(struct mesh_chunk_header);
        b->chunk_num       = 0;
        b->chunk_first_seq = req_id.seq;
    }
    /* First append into a fresh pair initializes shared metadata. */
    if (b->dst_pod_id < 0) {
        b->dst_pod_id     = dst_pod_id;
        b->owner_req_id   = req_id;
        b->first_us       = now_us();
        b->builder_flags  = flags;
    }

    /* Write hdr entry. */
    uint8_t *hbuf = dpumesh_hdr_tx_buf(ctx, b->hdr_tx_slot);
    struct mesh_hdr_req *e = (struct mesh_hdr_req *)(hbuf + b->hdr_cursor);
    memset(e, 0, sizeof(*e));
    e->req_id = req_id;
    if (service) {
        size_t n = strlen(service);
        if (n >= sizeof(e->dst_service)) n = sizeof(e->dst_service) - 1;
        memcpy(e->dst_service, service, n);
    }
    e->dst_pod_id_hint = dst_pod_id;
    e->body_size = body_len;
    e->flags = flags;
    b->hdr_cursor += sizeof(struct mesh_hdr_req);
    b->hdr_num++;

    /* Write body into chunk. */
    if (body_len > 0) {
        uint8_t *cbuf = dpumesh_tx_buf(ctx, b->chunk_tx_slot);
        memcpy(cbuf + b->chunk_cursor, body, body_len);
    }
    b->chunk_cursor += body_len;
    b->chunk_num++;

    /* Flush if hdr cap reached (hdr_num cap or hdr_slot would overflow on
     * next append). Chunk cap is handled at the top of the next call. */
    if (b->hdr_num >= MESH_HDR_BATCH_MAX_ENTRIES ||
        b->hdr_cursor + sizeof(struct mesh_hdr_req) > (uint32_t)ctx->hdr_slot_size) {
        builder_flush_locked(ctx, b);
    }
    return 0;
}

/* Per-tick timeout flush: each builder uses its own lock. No global lock. */
static void *flush_timer_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    pthread_setname_np(pthread_self(), "dpumesh_flush");
    while (ctx->mesh_running) {
        struct timespec ts = {0, 250 * 1000};  /* 250 µs */
        nanosleep(&ts, NULL);
        uint64_t now = now_us();
        for (int i = 0; i < MAX_DST_PODS; i++) {
            for (int s = 0; s < MESH_BUILDER_SHARDS; s++) {
                mesh_builder_t *b = &ctx->builders[i][s];
                pthread_mutex_lock(&b->lock);
                if (b->dst_pod_id >= 0 && now - b->first_us >= BUILDER_FLUSH_TIMEOUT_US) {
                    builder_flush_locked(ctx, b);
                }
                pthread_mutex_unlock(&b->lock);
            }
        }
    }
    return NULL;
}

/* ====== Public: send_request / send_response ====== */

/* TLS-cached shard id: each thread sticks to one (dst, shard) pair to amortize
 * the hash. Workers from the same thread serialize on the same per-shard lock,
 * but threads are spread across MESH_BUILDER_SHARDS shards. */
static inline int builder_shard(void) {
    static __thread int cached = -1;
    if (__builtin_expect(cached < 0, 0)) {
        uint64_t h = (uint64_t)pthread_self();
        h ^= h >> 33; h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        cached = (int)(h % MESH_BUILDER_SHARDS);
    }
    return cached;
}

int dpumesh_send_request(dpumesh_ctx_t *ctx, const char *service,
                         const uint8_t *body, uint32_t body_len, uint8_t flags,
                         struct mesh_req_id *out_req_id) {
    if (!ctx || !service) return -1;
    int dst = dpumesh_resolve(ctx, service);
    if (dst < 0) return -1;
    if (body_len > MESH_CHUNK_BODY_BUDGET) return -1;
    int idx = dst_pod_to_idx(dst);
    if (idx < 0) return -1;
    int sh = builder_shard();

    uint32_t seq = atomic_fetch_add(&ctx->next_seq, 1);
    struct mesh_req_id rid = { .src_id = ctx->src_id, .seq = seq };

    if (register_pending_v2(ctx, rid) < 0) return -1;

    mesh_builder_t *b = &ctx->builders[idx][sh];
    pthread_mutex_lock(&b->lock);
    if (builder_append_locked(ctx, b, rid, service, dst, body, body_len, flags) < 0) {
        pthread_mutex_unlock(&b->lock);
        /* Append failed after register succeeded: release the pending slot
         * so the next wraparound at this idx can register cleanly. */
        dpumesh_cancel_pending_v2(ctx, rid);
        return -1;
    }
    pthread_mutex_unlock(&b->lock);

    if (out_req_id) *out_req_id = rid;
    atomic_fetch_add(&ctx->stat_send_req, 1);
    return 0;
}

int dpumesh_send_response(dpumesh_ctx_t *ctx, int dst_pod_id,
                          struct mesh_req_id req_id,
                          const uint8_t *body, uint32_t body_len, uint8_t flags) {
    if (!ctx || dst_pod_id < 0) return -1;
    if (body_len > MESH_CHUNK_BODY_BUDGET) return -1;
    int idx = dst_pod_to_idx(dst_pod_id);
    if (idx < 0) return -1;
    int sh = builder_shard();

    mesh_builder_t *b = &ctx->builders[idx][sh];
    pthread_mutex_lock(&b->lock);
    if (builder_append_locked(ctx, b, req_id, /*service*/NULL, dst_pod_id,
                              body, body_len, flags) < 0) {
        pthread_mutex_unlock(&b->lock);
        return -1;
    }
    pthread_mutex_unlock(&b->lock);
    return 0;
}

/* ====== Public: wait_response_v2 / cancel_v2 / release_async_v2 ====== */

int dpumesh_wait_response_v2(dpumesh_ctx_t *ctx, struct mesh_req_id req_id,
                             sw_descriptor_t *resp, int timeout_ms) {
    uint32_t idx = req_id.seq % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    /* 1) Spin briefly. Catches sub-100µs responses with zero syscalls. */
    for (int i = 0; i < PENDING_SPIN_ITER; i++) {
        if (atomic_load_explicit(&p->state, memory_order_acquire) != 0) goto check;
        cpu_pause();
    }

    /* 2) Slow path: futex_wait until state changes or absolute deadline. */
    struct timespec deadline = {0, 0};
    int have_deadline = 0;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec  += timeout_ms / 1000;
        deadline.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) { deadline.tv_sec++; deadline.tv_nsec -= 1000000000L; }
        have_deadline = 1;
    } else if (timeout_ms == 0) {
        if (atomic_load(&p->state) != 0) goto check;
        return -1;
    }

    while (atomic_load_explicit(&p->state, memory_order_acquire) == 0) {
        atomic_fetch_add_explicit(&p->waiters, 1, memory_order_seq_cst);
        /* Recheck state under the waiters count so PE either sees us, or we
         * see PE's store. */
        if (atomic_load_explicit(&p->state, memory_order_acquire) != 0) {
            atomic_fetch_sub_explicit(&p->waiters, 1, memory_order_relaxed);
            break;
        }
        int rc = futex_wait_abs(&p->state, 0, have_deadline ? &deadline : NULL);
        int err = errno;
        atomic_fetch_sub_explicit(&p->waiters, 1, memory_order_relaxed);
        if (rc == -1 && err == ETIMEDOUT) {
            /* Race: state may have flipped 0→1 between our check and futex_wait
             * timing out. CAS-claim state=-2; if it fails state isn't 0 anymore
             * and we'll observe it via goto check. */
            int expected = 0;
            if (atomic_compare_exchange_strong(&p->state, &expected, -2)) {
                pthread_mutex_lock(&p->lock);
                if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
                if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
                pthread_mutex_unlock(&p->lock);
                atomic_fetch_add(&ctx->stat_wait_timeout, 1);
                return -1;
            }
            /* CAS lost: PE delivered, state is now 1 (or already cancelled). */
            break;
        }
        /* rc==0 or EAGAIN/EINTR: re-evaluate state. */
    }

check:
    {
        int s = atomic_load_explicit(&p->state, memory_order_acquire);
        if (s == 1 && MESH_REQ_ID_EQ(p->req_id_v2, req_id)) {
            *resp = p->desc;
            pthread_mutex_lock(&p->lock);
            if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
            if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
            pthread_mutex_unlock(&p->lock);
            atomic_store_explicit(&p->state, -1, memory_order_release);
            return 0;
        }
    }
    return -1;
}

void dpumesh_cancel_pending_v2(dpumesh_ctx_t *ctx, struct mesh_req_id req_id) {
    dpumesh_cancel_pending(ctx, req_id.seq);  /* same slot lookup */
}

void dpumesh_pending_release_async_v2(dpumesh_ctx_t *ctx, struct mesh_req_id req_id) {
    dpumesh_pending_release_async(ctx, req_id.seq);
}

/* ====== Expected ring + parked chunks ====== */

static void expected_ring_push(expected_ring_t *r, const expected_entry_t *e) {
    if (r->count >= EXPECTED_RING_CAP) return;  /* drop on overflow; sweep will warn */
    r->buf[r->tail] = *e;
    r->tail = (r->tail + 1) % EXPECTED_RING_CAP;
    r->count++;
}

static void expected_ring_pop(expected_ring_t *r) {
    if (r->count == 0) return;
    r->head = (r->head + 1) % EXPECTED_RING_CAP;
    r->count--;
}

static expected_entry_t *expected_ring_head(expected_ring_t *r) {
    if (r->count == 0) return NULL;
    return &r->buf[r->head];
}

static void parked_push(parked_list_t *l, parked_chunk_node_t *node) {
    node->next = NULL;
    if (!l->head) l->head = node;
    else          l->tail->next = node;
    l->tail = node;
    l->count++;
}

static parked_chunk_node_t *parked_pop_front(parked_list_t *l) {
    parked_chunk_node_t *n = l->head;
    if (!n) return NULL;
    l->head = n->next;
    if (!l->head) l->tail = NULL;
    l->count--;
    n->next = NULL;
    return n;
}

/* Deliver one body from a chunk to the application via the v2 pending or
 * RX queue, identical to the legacy rx_deliver_desc but keyed on
 * mesh_req_id. */
static void deliver_one_body(dpumesh_ctx_t *ctx, struct mesh_req_id rid,
                             const uint8_t *body, uint32_t size,
                             int32_t src_pod_id, uint8_t flags) {
    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("deliver_one_body: no free rx slot (src=%u seq=%u)",
                     rid.src_id, rid.seq);
        return;
    }
    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    if (size > 0) memcpy(dst, body, size);

    sw_descriptor_t desc;
    memset(&desc, 0, sizeof(desc));
    desc.req_id          = rid.seq;
    desc.src_pod_id      = src_pod_id;
    desc.dst_pod_id      = ctx->pod_id;
    desc.flags           = flags;
    desc.header_buf_slot = -1;
    desc.body_buf_slot   = slot;
    desc.body_len        = size;
    desc.valid           = 1;

    if (flags & OP_RESPONSE) {
        uint32_t idx = rid.seq % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];

        /* Fast path: lock-free CAS state 0 → 1. Writes desc before CAS so
         * the worker (acquiring after seeing state==1) observes a fully
         * populated desc. The CAS and the waiters check are both seq_cst
         * to form the Dekker-style pair with worker's add(waiters) +
         * load(state). If waiters==0 (worker still spinning or hasn't
         * called futex_wait), skip the syscall entirely. */
        int s = atomic_load(&p->state);
        if (s == 0 && MESH_REQ_ID_EQ(p->req_id_v2, rid)) {
            p->desc = desc;
            int expected = 0;
            if (atomic_compare_exchange_strong(&p->state, &expected, 1)) {
                if (atomic_load(&p->waiters) > 0) {
                    futex_wake_one(&p->state);
                }
                return;
            }
            s = expected;
        }

        if (s == -2 && MESH_REQ_ID_EQ(p->req_id_v2, rid)) {
            /* Worker already timed out. Release everything. */
            pthread_mutex_lock(&p->lock);
            if (p->tx_slot >= 0)     { dpumesh_tx_free(ctx, p->tx_slot);     p->tx_slot = -1; }
            if (p->hdr_tx_slot >= 0) { dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot); p->hdr_tx_slot = -1; }
            pthread_mutex_unlock(&p->lock);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            int expected = -2;
            if (atomic_compare_exchange_strong(&p->state, &expected, -1)) {
                if (atomic_load_explicit(&p->waiters, memory_order_seq_cst) > 0) {
                    futex_wake_all(&p->state);
                }
            }
            return;
        }

        /* Orphan OP_RESPONSE: no matching waiter (state=-1 after cancel,
         * state=1 duplicate, or req_id mismatch from slot reuse). Drop
         * the rx_slot — the pending API is the only response consumer
         * and rx_queue is not drained for responses. Mirrors the
         * rx_deliver_desc (legacy) no-waiter handling.
         *
         * Without this drop, slots accumulated whenever a worker timed
         * out then received its response late: at >=1024B body the
         * back-pressure made timeouts routine and the leak exhausted the
         * 4096-slot rx pool within seconds. */
        pthread_mutex_lock(&ctx->rx_slot_lock);
        ctx->rx_slot_bitmap[slot] = 0;
        pthread_mutex_unlock(&ctx->rx_slot_lock);
        return;
    }

    /* OP_REQUEST: push to rx_queue for echo-side workers. */
    pthread_mutex_lock(&ctx->rx_lock);
    if (ctx->rx_count >= RX_QUEUE_SIZE) {
        pthread_mutex_unlock(&ctx->rx_lock);
        DOCA_LOG_ERR("deliver_one_body: RX queue full (seq=%u)", rid.seq);
        pthread_mutex_lock(&ctx->rx_slot_lock);
        ctx->rx_slot_bitmap[slot] = 0;
        pthread_mutex_unlock(&ctx->rx_slot_lock);
        return;
    }
    ctx->rx_queue[ctx->rx_tail] = desc;
    ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
    ctx->rx_count++;
    pthread_cond_signal(&ctx->rx_cond);
    pthread_mutex_unlock(&ctx->rx_lock);
}

/* Batched OP_REQUEST delivery: pops `num` entries from `r`, allocates rx
 * slots, copies bodies, builds descriptors, then pushes the whole batch
 * into rx_queue under ONE mutex acquire and signals workers with ONE
 * pthread_cond_broadcast.
 *
 * Why: previous deliver_one_body acquired ctx->rx_lock and called
 * pthread_cond_signal per body. At ~25 bodies/chunk × ~7k chunks/sec =
 * ~170k mutex+signal pairs/sec, kernel scheduler bookkeeping (psi_group,
 * update_load, sched_clock, mark_wake_futex) dominated echo CPU — flame
 * graph at 150k load showed 89% of echo CPU spent there vs <3% in our
 * own code. Batching collapses N×{lock,unlock,signal} to 1×{lock,unlock,
 * broadcast}, returning the scheduler-overhead budget to actual work.
 *
 * Pending (OP_RESPONSE) path is untouched — it already uses lock-free CAS
 * + futex_wake_one and is not the bottleneck.
 *
 * Caller holds ctx->expected_locks[r's src_id]. */
static void deliver_chunk_to_rxq(dpumesh_ctx_t *ctx,
                                 expected_ring_t *r,
                                 const uint8_t *chunk_buf,
                                 uint32_t cursor,
                                 uint32_t num)
{
    /* num is bounded by MESH_CHUNK_MAX_BODIES (64). Stack-allocate. */
    sw_descriptor_t descs[MESH_CHUNK_MAX_BODIES];
    int slots[MESH_CHUNK_MAX_BODIES];
    uint32_t built = 0;

    /* Phase 1 (no rx_lock): pop expected entries, alloc rx slots, memcpy
     * bodies, build descriptors. rx_slot_alloc still takes rx_slot_lock
     * per body — that mutex is uncontended in steady state. */
    for (uint32_t i = 0; i < num; i++) {
        expected_entry_t e = r->buf[r->head];
        expected_ring_pop(r);
        int slot = rx_slot_alloc(ctx);
        if (slot < 0) {
            /* Out of rx slots: skip this body. Logs intentionally absent
             * per the project-wide DPU/host log strip. */
            cursor += e.size;
            continue;
        }
        const uint8_t *body = chunk_buf + cursor;
        cursor += e.size;
        if (e.size > 0) {
            uint8_t *dst = (uint8_t *)ctx->rx_buffer +
                           ((size_t)slot * ctx->slot_size);
            memcpy(dst, body, e.size);
        }
        sw_descriptor_t *d = &descs[built];
        memset(d, 0, sizeof(*d));
        d->req_id          = e.req_id.seq;
        d->src_pod_id      = e.src_pod_id;
        d->dst_pod_id      = ctx->pod_id;
        d->flags           = e.flags;
        d->header_buf_slot = -1;
        d->body_buf_slot   = slot;
        d->body_len        = e.size;
        d->valid           = 1;
        slots[built] = slot;
        built++;
    }
    if (built == 0) return;

    /* Phase 2 (single rx_lock critical section): batch push + one broadcast. */
    pthread_mutex_lock(&ctx->rx_lock);
    uint32_t fit = RX_QUEUE_SIZE - ctx->rx_count;
    uint32_t push = built <= fit ? built : fit;
    for (uint32_t i = 0; i < push; i++) {
        ctx->rx_queue[ctx->rx_tail] = descs[i];
        ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
    }
    ctx->rx_count += push;
    if (push > 0) pthread_cond_broadcast(&ctx->rx_cond);
    pthread_mutex_unlock(&ctx->rx_lock);

    /* Phase 3 (overflow only — rare with RX_QUEUE_SIZE=65536): release the
     * rx slots whose descriptors couldn't fit. */
    if (push < built) {
        pthread_mutex_lock(&ctx->rx_slot_lock);
        for (uint32_t i = push; i < built; i++)
            ctx->rx_slot_bitmap[slots[i]] = 0;
        pthread_mutex_unlock(&ctx->rx_slot_lock);
    }
}

/* Consume `num` bodies from the head of expected[src] and deliver them
 * from `chunk_buf` (which starts with the 12B chunk header). Caller holds
 * the matching expected_lock. */
static void consume_chunk(dpumesh_ctx_t *ctx, expected_ring_t *r,
                          const uint8_t *chunk_buf, uint32_t num) {
    uint32_t cursor = sizeof(struct mesh_chunk_header);
    /* All bodies in a chunk share flags (builder_flags is set once on first
     * append and persists), so OP_RESPONSE and OP_REQUEST never mix here.
     * Peek the head once and pick the path: pending/futex (already optimal)
     * for responses, batched rx_queue push for requests. */
    expected_entry_t *head = expected_ring_head(r);
    if (head && !(head->flags & OP_RESPONSE)) {
        deliver_chunk_to_rxq(ctx, r, chunk_buf, cursor, num);
        return;
    }
    for (uint32_t i = 0; i < num; i++) {
        expected_entry_t e = r->buf[r->head];
        expected_ring_pop(r);
        const uint8_t *body = chunk_buf + cursor;
        cursor += e.size;
        deliver_one_body(ctx, e.req_id, body, e.size, e.src_pod_id, e.flags);
    }
}

/* Try to drain parked chunks for `src_idx`. Caller holds expected_locks[src_idx]. */
static void drain_parked_locked(dpumesh_ctx_t *ctx, int src_idx) {
    expected_ring_t *r = &ctx->expected[src_idx];
    parked_list_t   *l = &ctx->parked[src_idx];
    while (l->head) {
        parked_chunk_node_t *n = l->head;
        if (r->count < n->num_bodies) break;
        expected_entry_t *head = expected_ring_head(r);
        if (!head || head->req_id.seq != n->first_seq) break;
        const uint8_t *chunk_buf =
            (const uint8_t *)ctx->rx_dma_buffer + ((size_t)n->rx_slot * ctx->slot_size);
        consume_chunk(ctx, r, chunk_buf, n->num_bodies);
        /* Phase 4 fix: 1 parked chunk drained → 1 credit ++. */
        rx_dma_credit_return(ctx);
        parked_pop_front(l);
        free(n);
    }
}

/* OP_HDR_BATCH handler: parse N × mesh_hdr_req entries (Phase 3 keeps the
 * src's wire format unchanged through DPU — no conversion to mesh_hdr_fwd
 * yet) and enqueue each into expected[src_id]. src_pod_id comes from the
 * comch DMA_COMPLETION msg because the whole batch shares one src. */
static void process_hdr_batch(dpumesh_ctx_t *ctx, uint32_t pos, uint32_t dma_len, int32_t src_pod_id) {
    atomic_fetch_add(&ctx->stat_process_hdr, 1);
    /* Phase 3: hdr batches land in the dedicated hdr_rx_buffer (not body RX).
     * DPU's dpu_enqueue_reverse_dma sets desc.dst_pos so comp.pos == offset
     * within hdr_rx_buffer. No credit return: hdr_rx is a wrap cursor. */
    if (!ctx->hdr_rx_buffer || pos + dma_len > ctx->hdr_rx_buf_size) {
        DOCA_LOG_ERR("process_hdr_batch: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->hdr_rx_buf_size);
        return;
    }
    const uint8_t *p = (const uint8_t *)ctx->hdr_rx_buffer + pos;
    uint32_t n_entries = dma_len / sizeof(struct mesh_hdr_req);
    uint16_t touched = 0;

    for (uint32_t i = 0; i < n_entries; i++) {
        const struct mesh_hdr_req *e =
            (const struct mesh_hdr_req *)(p + i * sizeof(struct mesh_hdr_req));
        uint32_t sid = e->req_id.src_id;
        if (sid >= MAX_DST_PODS) continue;
        pthread_mutex_lock(&ctx->expected_locks[sid]);
        expected_entry_t entry = {
            .req_id     = e->req_id,
            .size       = e->body_size,
            .src_pod_id = src_pod_id,
            .trace_id   = e->trace_id,
            .span_id    = e->span_id,
            .flags      = e->flags,
            .arrived_us = now_us(),
        };
        expected_ring_push(&ctx->expected[sid], &entry);
        pthread_mutex_unlock(&ctx->expected_locks[sid]);
        touched |= (uint16_t)(1u << sid);
    }

    /* hdr_rx_buffer is a wrap cursor; no slot bitmap to release. */

    /* Drain parked chunks for each src whose expected ring grew. */
    for (int sid = 0; sid < MAX_DST_PODS; sid++) {
        if (!(touched & (1u << sid))) continue;
        pthread_mutex_lock(&ctx->expected_locks[sid]);
        drain_parked_locked(ctx, sid);
        pthread_mutex_unlock(&ctx->expected_locks[sid]);
    }
}

/* OP_CHUNK handler: try to match head of expected[src_id] against chunk
 * header. Match → consume + release rx slot. Miss → park (zero-copy). */
static void process_chunk(dpumesh_ctx_t *ctx, uint32_t pos, uint32_t dma_len) {
    atomic_fetch_add(&ctx->stat_process_chunk, 1);
    if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size ||
        dma_len < sizeof(struct mesh_chunk_header)) {
        DOCA_LOG_ERR("process_chunk: bounds fail pos=%u len=%u", pos, dma_len);
        return;
    }
    const uint8_t *chunk_buf = (const uint8_t *)ctx->rx_dma_buffer + pos;
    const struct mesh_chunk_header *h = (const struct mesh_chunk_header *)chunk_buf;
    uint32_t sid = h->src_id;
    if (sid >= MAX_DST_PODS) {
        DOCA_LOG_ERR("process_chunk: src_id=%u out of range", sid);
        return;
    }
    uint32_t rx_slot = pos / (uint32_t)ctx->slot_size;

    pthread_mutex_lock(&ctx->expected_locks[sid]);
    expected_ring_t *r = &ctx->expected[sid];
    expected_entry_t *head = expected_ring_head(r);
    if (head && r->count >= h->num_bodies && head->req_id.seq == h->first_seq) {
        consume_chunk(ctx, r, chunk_buf, h->num_bodies);
        /* Sharding-safety: consume advances the expected head; whatever
         * parked chunk was waiting on the previous head (e.g., another
         * shard's chunk that arrived earlier) may now be matchable. */
        drain_parked_locked(ctx, sid);
        pthread_mutex_unlock(&ctx->expected_locks[sid]);
        /* Phase 4 fix: 1 chunk released → 1 credit ++ (not N — staging
         * slot frees do NOT bump credit). */
        rx_dma_credit_return(ctx);
        return;
    }
    /* Park: keep the rx_slot's bytes intact. credit NOT returned until
     * matching hdr arrives (or sweep reclaims). */
    parked_chunk_node_t *n = (parked_chunk_node_t *)calloc(1, sizeof(*n));
    if (!n) {
        pthread_mutex_unlock(&ctx->expected_locks[sid]);
        DOCA_LOG_ERR("process_chunk: park alloc failed; dropping");
        rx_dma_credit_return(ctx);
        return;
    }
    n->rx_slot    = rx_slot;
    n->total_size = dma_len;
    n->first_seq  = h->first_seq;
    n->num_bodies = h->num_bodies;
    n->arrived_us = now_us();
    parked_push(&ctx->parked[sid], n);
    atomic_fetch_add(&ctx->stat_park, 1);
    pthread_mutex_unlock(&ctx->expected_locks[sid]);
}

/* 1-second sweep: GC parked chunks and orphan expected entries older than
 * the budget. Releases rx_slot credit for each evicted parked chunk so
 * DPA can resume writing. */
#define MESH_TIMEOUT_US  1000000ULL

static void *sweep_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    pthread_setname_np(pthread_self(), "dpumesh_sweep");
    uint64_t last_stat_us = 0;
    uint64_t prev[8] = {0};
    while (ctx->mesh_running) {
        struct timespec ts = {0, 100 * 1000 * 1000}; /* 100ms */
        nanosleep(&ts, NULL);
        uint64_t now = now_us();
        if (now - last_stat_us >= 1000000) {
            uint64_t cur[8] = {
                atomic_load(&ctx->stat_send_req),
                atomic_load(&ctx->stat_wait_timeout),
                atomic_load(&ctx->stat_chunk_direct),
                atomic_load(&ctx->stat_chunk_staging),
                atomic_load(&ctx->stat_process_chunk),
                atomic_load(&ctx->stat_process_hdr),
                atomic_load(&ctx->stat_rx_free),
                atomic_load(&ctx->stat_park),
            };
            uint64_t any = 0;
            for (int i = 0; i < 8; i++) any |= (cur[i] - prev[i]);
            if (any) {
                /* Diag: snapshot current resource usage. */
                int tx_used = 0, hdr_used = 0, rx_used = 0;
                pthread_mutex_lock(&ctx->slot_lock);
                for (int i = 0; i < ctx->num_slots; i++)
                    if (ctx->slot_bitmap[i]) tx_used++;
                pthread_mutex_unlock(&ctx->slot_lock);
                pthread_mutex_lock(&ctx->hdr_slot_lock);
                for (int i = 0; i < ctx->hdr_num_slots; i++)
                    if (ctx->hdr_tx_bitmap[i]) hdr_used++;
                pthread_mutex_unlock(&ctx->hdr_slot_lock);
                pthread_mutex_lock(&ctx->rx_slot_lock);
                for (int i = 0; i < ctx->num_slots; i++)
                    if (ctx->rx_slot_bitmap[i]) rx_used++;
                pthread_mutex_unlock(&ctx->rx_slot_lock);
                int pending_active = 0;
                for (int i = 0; i < MAX_PENDING; i++) {
                    int st = ctx->pending[i].state;
                    if (st != -1) pending_active++;
                }
                int expected_total = 0, parked_total = 0;
                for (int sid = 0; sid < MAX_DST_PODS; sid++) {
                    expected_total += (int)ctx->expected[sid].count;
                    parked_total   += (int)ctx->parked[sid].count;
                }
                fprintf(stderr, "[dpumesh] stat/sec: send=%lu wait_to=%lu c_dir=%lu c_stg=%lu p_chk=%lu p_hdr=%lu rxfree=%lu park=%lu | tx=%d hdr=%d rx=%d pend=%d exp=%d prk=%d\n",
                        cur[0]-prev[0], cur[1]-prev[1], cur[2]-prev[2], cur[3]-prev[3],
                        cur[4]-prev[4], cur[5]-prev[5], cur[6]-prev[6], cur[7]-prev[7],
                        tx_used, hdr_used, rx_used, pending_active, expected_total, parked_total);
            }
            for (int i = 0; i < 8; i++) prev[i] = cur[i];
            last_stat_us = now;
        }
        for (int sid = 0; sid < MAX_DST_PODS; sid++) {
            pthread_mutex_lock(&ctx->expected_locks[sid]);
            parked_list_t *l = &ctx->parked[sid];
            while (l->head && now - l->head->arrived_us > MESH_TIMEOUT_US) {
                parked_chunk_node_t *n = parked_pop_front(l);
                DOCA_LOG_WARN("parked chunk timeout src=%d first_seq=%u num=%u",
                              sid, n->first_seq, n->num_bodies);
                /* Phase 4 fix: 1 parked chunk evicted → 1 credit ++. */
                rx_dma_credit_return(ctx);
                free(n);
            }
            expected_ring_t *r = &ctx->expected[sid];
            while (r->count > 0 &&
                   now - r->buf[r->head].arrived_us > MESH_TIMEOUT_US &&
                   (!l->head || l->head->first_seq != r->buf[r->head].req_id.seq)) {
                DOCA_LOG_WARN("orphan expected timeout src=%d seq=%u",
                              sid, r->buf[r->head].req_id.seq);
                expected_ring_pop(r);
            }
            pthread_mutex_unlock(&ctx->expected_locks[sid]);
        }
    }
    return NULL;
}
