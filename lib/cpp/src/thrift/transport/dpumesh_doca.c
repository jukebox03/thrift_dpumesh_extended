/*
 * dpumesh_doca.c - DPUmesh DOCA transport layer implementation
 *
 * NVIDIA DOCA (Comch + DMA) backend for DPUmesh Thrift transport.
 * Provides the host-side raw buffer API declared in dpumesh.h: TX/RX
 * slot pools, descriptor SQ enqueue/dequeue, and client-side
 * request/response matching over the DPU control + DMA data path.
 */

#include "dpumesh.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>        /* close() for the host epoll RX path */
#include <sys/epoll.h>     /* event-driven host PE progress (DPUMESH_HOST_EPOLL) */
#include <sys/eventfd.h>   /* readiness eventfd for native-epoll integration (dpumesh_get_event_fd) */

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
#include "doca/comch_consumer.h"
#include "doca/comch_common.h"
#include "doca/comch_msgq.h"
#include "doca/dpa_common.h"

DOCA_LOG_REGISTER(DPUMESH_DOCA);

static const char *doca_err_str(doca_error_t rc) {
    return doca_error_get_descr(rc);
}

static void cleanup_ctx(struct dpumesh_ctx *ctx);

/* ====================================================================
 * dpumesh_ctx — internal state
 * ==================================================================== */

/* RX queue between PE thread (producer, drains rx_dma_buffer) and the
 * application consumer. Sized larger than worst-case concurrent in-flight so
 * the PE thread never has to drop on enqueue. */
#define RX_QUEUE_SIZE 65536

/* Pending response table for client-side request/response matching.
 * Indexed by req_id % MAX_PENDING. Must exceed expected in-flight requests
 * to avoid hash collisions. */
#define MAX_PENDING 65536

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sw_descriptor_t desc;
    volatile int state;   /* -1=unused, -2=abandoned/timed-out, 0=waiting, 1=arrived */
    int tx_slot;          /* TX buffer slot owned by this request, -1 if none */
    uint32_t owner_req_id; /* req_id currently occupying this idx; guards the
                            * TX_ACK handler against a late/duplicate ACK after
                            * req_id reuses this idx */
} dpumesh_pending_t;

/* One cell of the lock-free bounded SPMC RX ring (Vyukov's bounded-MPMC cell/seq
 * design, producer side specialized to a single producer). `seq` carries the
 * turn-stamp: the producer may write cell i only when seq==enq_pos; a consumer
 * may read it only when seq==deq_pos+1. */
struct rxq_cell {
    sw_descriptor_t desc;
    atomic_uint_fast32_t seq;
};

struct dpumesh_ctx {
    char app_name[64];
    char worker_id[128];
    int  pod_id;
    int  num_slots;
    int  slot_size;
    int  max_descriptors;
    int  k_rings;              /* K = forward rings per pod (EU-sharding); 1 = legacy */
    /* DOCA objects */
    struct objects doca_objs;
    void *dma_buffer;          /* Host TX buffer (PCI mmap, CPU→DPU source) */
    /* K forward descriptor rings (EU-sharding). dpumesh_enqueue round-robins
     * across them via rr_counter; each ring_locks[j] serializes get_next_dma_desc
     * + fill + valid=1 for ring j (single-producer per ring). K=1 = legacy. */
    struct dma_ring *dma_rings[MAX_EU_PER_POD];
    pthread_mutex_t ring_locks[MAX_EU_PER_POD];
    atomic_uint rr_counter;
    /* Reverse credit region size = rx_dma_buf_size / k_rings. The DPA reports an
     * absolute landing pos; ring_idx = pos / rx_region_size selects which ring's
     * credit slot to return. K=1 → region = whole buffer → ring_idx always 0. */
    size_t rx_region_size;
    doca_dpa_dev_mmap_t dpa_mmap_handle;  /* DPA handle for local mmap (used in TX descriptors) */

    /* Host RX buffer (PCI mmap, DPU→CPU destination) */
    void *rx_dma_buffer;
    struct doca_mmap *rx_dma_mmap;
    size_t rx_dma_buf_size;

    /* Persistent buffer for initial registration to avoid stack UAF */
    struct dmesh_register_msg reg_msg;

    /* TX slot management — lock-free Treiber free-list of slot indices.
     * free_head packs (tag<<32 | head_index); head_index==num_slots = empty.
     * The tag (bumped per op) defeats ABA. slot_next[i] links free slots. */
    atomic_uint_fast64_t free_head;
    uint32_t *slot_next;

    /* RX descriptor queue — lock-free bounded SPMC ring (1 producer = PE
     * thread, N consumers = server workers). dpumesh_dequeue spin-polls it
     * (pure lock-free + adaptive backoff); a native epoll_wait() on the readiness
     * eventfd is the idle-sleep path. No mutex/cond on the RX landing path. */
    struct rxq_cell *rx_ring;          /* RX_QUEUE_SIZE cells (power of two) */
    /* rx_enq (producer-private, written every request) and rx_deq (CAS-hammered
     * by every worker) sit on separate cachelines: otherwise the PE's per-request
     * rx_enq store false-shares the line the workers CAS, bouncing it and
     * inflating CAS-retry CPU. */
    char _rx_pad0[64];
    atomic_uint_fast32_t rx_enq;       /* producer position (PE only) */
    char _rx_pad1[64];
    atomic_uint_fast32_t rx_deq;       /* consumer position (workers CAS) */
    char _rx_pad2[64];

    /* PE progress thread */
    pthread_t pe_tid;
    volatile int pe_running;

    /* Readiness eventfd for native-epoll integration. Lazily enabled by
     * dpumesh_get_event_fd(): once enabled, rx_deliver_desc writes the eventfd on
     * each user-visible delivery (request -> RX ring, or response -> pending) so a
     * caller blocked in a vanilla epoll_wait() on this fd wakes up. The PE thread
     * itself already sleeps on the DOCA PE notification fd (DPUMESH_HOST_EPOLL), so
     * the whole chain is notification-driven, not busy-poll. -1 = not created. */
    int notify_efd;
    volatile int notify_enabled;

    /* Client-side pending response table */
    dpumesh_pending_t pending[MAX_PENDING];
    atomic_uint_fast32_t next_req_id;
};

/* ====================================================================
 * PE progress thread — drives DOCA progress engine
 * ==================================================================== */

/* Adaptive-poll tuning: spin this many empty iterations before the first
 * back-off (catches brief inter-request gaps with no latency cost), then sleep
 * a short, capped interval per empty iteration (yields the core during
 * sustained idle). */
#define PE_IDLE_SPIN     2048
#define PE_BACKOFF_NS    20000   /* 20 us */

static void *pe_progress_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    struct doca_pe *pe = ctx->doca_objs.pe;

    /* DPUMESH_HOST_EPOLL=1: sleep on the PE notification fd instead of spinning.
     * The host comch PE receives ONLY real completions (REV_DONE / TX_ACK from
     * the DPU), each of which raises the notification fd — there is NO silent-
     * wakeup path here (unlike the DPU forward ring), so epoll is safe and cuts
     * this thread's idle CPU to ~0. Default 0 = the baked adaptive-spin loop. */
    int want_epoll = 0;
    { const char *e = getenv("DPUMESH_HOST_EPOLL"); if (e && atoi(e) != 0) want_epoll = 1; }

    doca_notification_handle_t pfd = 0;
    int ep = -1;
    if (want_epoll && pe &&
        doca_pe_get_notification_handle(pe, &pfd) == DOCA_SUCCESS) {
        ep = epoll_create1(0);
        if (ep >= 0) {
            struct epoll_event ev = { .events = EPOLLIN, .data = { .u32 = 0 } };
            if (epoll_ctl(ep, EPOLL_CTL_ADD, (int)pfd, &ev) != 0) { close(ep); ep = -1; }
        }
    }

    if (ep >= 0) {
        /* ===== Event-driven: drain → arm → re-check → block ===== */
        while (ctx->pe_running) {
            while (doca_pe_progress(pe)) { /* drain all ready completions */ }

            /* Arm, then re-check once to close the drain→arm race: a completion
             * landing between the last drain and the arm must not be stranded. */
            (void)doca_pe_request_notification(pe);
            if (doca_pe_progress(pe)) {
                (void)doca_pe_clear_notification(pe, pfd);
                continue;
            }
            /* Block until a completion raises the fd, or 200 ms so a pe_running=0
             * shutdown is observed promptly (vs a busy spin). */
            struct epoll_event evs[1];
            (void)epoll_wait(ep, evs, 1, 200);
            (void)doca_pe_clear_notification(pe, pfd);
        }
        close(ep);
        return NULL;
    }

    /* ===== Fallback: adaptive spin + nanosleep (baked default) ===== */
    uint32_t idle = 0;
    while (ctx->pe_running) {
        uint8_t did = 0;
        if (pe)
            did |= doca_pe_progress(pe);
        if (did) {
            idle = 0;                 /* work seen — keep spinning tight */
        } else if (++idle >= PE_IDLE_SPIN) {
            struct timespec t = {0, PE_BACKOFF_NS};
            nanosleep(&t, NULL);
        }
    }
    return NULL;
}

/* ====================================================================
 * RX data hook — called from PE progress thread via comch callback
 * ==================================================================== */

/* Return one unit of reverse-DMA admission credit to the DPA (it polls this
 * counter at the extra slot past the dma_ring; see dpumesh_rx_free). */
static inline void rx_credit_return(dpumesh_ctx_t *ctx, int pos)
{
    /* Per-ring credit: the reverse region a landing fell in maps 1:1 to a forward
     * ring (disjoint regions, ring j owns [j*R,(j+1)*R)). Return credit to that
     * ring's slot so the DPA EU owning rev ring j sees its own freed count. */
    int idx = (ctx->rx_region_size > 0) ? (int)((size_t)pos / ctx->rx_region_size) : 0;
    if (idx < 0 || idx >= ctx->k_rings) idx = 0;
    struct dma_ring *r = ctx->dma_rings[idx];
    if (r && r->descs) {
        volatile uint64_t *credit = (volatile uint64_t *)(r->descs + r->size);
        __sync_add_and_fetch(credit, 1);
    }
}

/* Reclaim an undeliverable RX entry (error/drop paths): return the landing
 * credit so the DPA can reuse that position. `slot` is the landing byte pos. */
static void rx_reclaim(dpumesh_ctx_t *ctx, int slot)
{
    rx_credit_return(ctx, slot);
}

/* Lock-free SPMC dequeue. Multiple worker consumers race via CAS on
 * rx_deq; the single PE producer owns rx_enq. Returns 1 and fills *out on
 * success, 0 if the ring is empty. Never blocks. */
static inline int rxq_try_pop(dpumesh_ctx_t *ctx, sw_descriptor_t *out)
{
    for (;;) {
        uint_fast32_t pos = atomic_load_explicit(&ctx->rx_deq, memory_order_relaxed);
        struct rxq_cell *c = &ctx->rx_ring[pos & (RX_QUEUE_SIZE - 1)];
        uint_fast32_t seq = atomic_load_explicit(&c->seq, memory_order_acquire);
        int_fast32_t diff = (int_fast32_t)(seq - (pos + 1));
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &ctx->rx_deq, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed)) {
                *out = c->desc;
                /* Release the cell for reuse one full lap ahead. */
                atomic_store_explicit(&c->seq, pos + RX_QUEUE_SIZE,
                                      memory_order_release);
                return 1;
            }
            /* CAS lost to another consumer — retry. */
        } else if (diff < 0) {
            return 0;  /* empty */
        }
        /* diff > 0: producer mid-write of the cell we'd claim — retry. */
    }
}

/*
 * Deliver a fully parsed descriptor to the pending table or RX queue.
 * Common path for both comch-based RX_DATA and DMA-based DMA_COMPLETION.
 */
/* Wake a caller blocked in a vanilla epoll_wait() on the readiness eventfd.
 * No-op until dpumesh_get_event_fd() enables it. Per-delivery write (no
 * coalescing) → cannot lose a wakeup; the eventfd is a plain counter, drained by
 * one read() per epoll wakeup. Safe to call from the PE thread. */
static inline void dpumesh_notify(dpumesh_ctx_t *ctx)
{
    if (ctx->notify_enabled && ctx->notify_efd >= 0) {
        uint64_t one = 1;
        ssize_t w = write(ctx->notify_efd, &one, sizeof(one));
        (void)w;
    }
}

static void rx_deliver_desc(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc, int slot)
{
    if (desc->flags & OP_RESPONSE) {
        uint32_t idx = desc->req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        pthread_mutex_lock(&p->lock);
        if (p->state == 0) {
            p->desc = *desc;
            p->state = 1;
            /* Response arrival implies the request round-tripped, so its TX slot
             * is logically free now — release it here. No-op if a TX_ACK already
             * freed it (tx_slot==-1). */
            if (p->tx_slot >= 0) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
            }
            /* Response delivered. Clients harvest via dpumesh_poll_response (a
             * lock-free state load); wake a native epoll_wait() on the eventfd. */
            dpumesh_notify(ctx);
        } else if (p->state == -2) {
            /* Cancelled request — DPA finished, now safe to free TX + RX */
            if (p->tx_slot >= 0) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
            }
            rx_reclaim(ctx, slot);
            p->state = -1;
            pthread_cond_broadcast(&p->cond);
        } else {
            DOCA_LOG_ERR("RX deliver: OP_RESPONSE for req_id=%u but no waiter (state=%d)",
                         desc->req_id, p->state);
            rx_reclaim(ctx, slot);
        }
        pthread_mutex_unlock(&p->lock);
    } else {
        /* Lock-free Vyukov single-producer enqueue (PE thread is the only
         * producer). No blocking in the PE callback path: if the ring is full,
         * drop immediately so we don't stall the PE. Slot-based admission (DPA
         * reverse credit + sender's tx_alloc) keeps in-flight bounded. */
        uint_fast32_t pos = atomic_load_explicit(&ctx->rx_enq, memory_order_relaxed);
        struct rxq_cell *c = &ctx->rx_ring[pos & (RX_QUEUE_SIZE - 1)];
        uint_fast32_t seq = atomic_load_explicit(&c->seq, memory_order_acquire);
        if ((int_fast32_t)(seq - pos) != 0) {
            DOCA_LOG_ERR("RX deliver: queue full, dropping req_id=%u", desc->req_id);
            rx_reclaim(ctx, slot);
            return;
        }
        c->desc = *desc;
        atomic_store_explicit(&c->seq, pos + 1, memory_order_release);
        atomic_store_explicit(&ctx->rx_enq, pos + 1, memory_order_relaxed);
        /* Consumers spin-poll the ring (dpumesh_dequeue); wake a native
         * epoll_wait() on the eventfd. No cond wakeup. */
        dpumesh_notify(ctx);
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
    if (!ctx->rx_dma_buffer || (size_t)pos + dma_len > ctx->rx_dma_buf_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->rx_dma_buf_size);
        return -1;
    }
    /* Body must fit one slot; the DPA caps each reverse DMA at DPA_DMA_COPY_MAX
     * (= default slot_size), but guard so a non-default slot_size can't overrun. */
    if (dma_len > (uint32_t)ctx->slot_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: len=%u exceeds slot_size=%d (req_id=%u)",
                     dma_len, ctx->slot_size, req_id);
        return -1;
    }
    uint32_t body_len = dma_len;

    /* Zero-copy: deliver the landing byte-offset `pos`; the consumer reads
     * rx_dma_buffer[pos] directly and returns the credit at rx_free. */
    int slot = (int)pos;

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
    /* Dispatch on the 1-byte type (see comch_client.c: the legacy 4-byte enum
     * carries its value in the LE low byte, and the completion uses a 1-byte
     * type at offset 0, so a single-byte read handles both). */
    uint8_t mtype = data[0];

    if (mtype == DMESH_MSG_FWD_ACK) {
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
        pthread_mutex_lock(&p->lock);
        /* owner_req_id guard: every MAX_PENDING requests reuse this idx, so a
         * late/duplicate ACK for an old req_id could otherwise free the TX slot
         * of the live request now occupying the slot. Only free if THIS req_id
         * still owns it. */
        if ((p->state == 0 || p->state == -2) && p->tx_slot >= 0 &&
            p->owner_req_id == ack.req_id) {
            /* state 0: request still waiting — free TX slot early.
             * state -2: wait_response gave up on timeout but deferred the TX
             *           free until DPA finished the forward DMA; this ACK
             *           confirms that, so we can release the slot now. */
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
            if (p->state == -2) {
                p->state = -1;
                pthread_cond_broadcast(&p->cond);
            }
        }
        pthread_mutex_unlock(&p->lock);

        DOCA_LOG_DBG("TX_ACK: freed TX slot for req_id=%u", ack.req_id);
        return;
    }

    if (mtype == DMESH_MSG_BATCH_FWD_ACK) {
        /* Batched TX_ACK: free the TX slot of each req_id (identical per-request
         * logic + owner guard as DMESH_MSG_FWD_ACK). One message → K frees. */
        if (len < 4) {
            DOCA_LOG_ERR("BATCH_TX_ACK: too short (len=%u)", len);
            return;
        }
        const struct dmesh_batch_tx_ack_msg *b = (const struct dmesh_batch_tx_ack_msg *)data;
        uint32_t n = b->count;
        if (n > BATCH_TXACK_MAX) n = BATCH_TXACK_MAX;
        if (len < 4u + 4u * n) {
            DOCA_LOG_ERR("BATCH_TX_ACK: len=%u short for count=%u", len, n);
            return;
        }
        for (uint32_t i = 0; i < n; i++) {
            uint32_t rid = b->req_ids[i];
            uint32_t idx = rid % MAX_PENDING;
            dpumesh_pending_t *p = &ctx->pending[idx];
            pthread_mutex_lock(&p->lock);
            if ((p->state == 0 || p->state == -2) && p->tx_slot >= 0 &&
                p->owner_req_id == rid) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
                if (p->state == -2) {
                    p->state = -1;
                    pthread_cond_broadcast(&p->cond);
                }
            }
            pthread_mutex_unlock(&p->lock);
        }
        return;
    }

    if (mtype == DMESH_MSG_REV_DONE) {
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

        if (!ctx->rx_dma_buffer || (size_t)pos + dma_len > ctx->rx_dma_buf_size) {
            DOCA_LOG_ERR("DMA_COMPLETION: invalid pos=%u len=%u buf_size=%zu",
                         pos, dma_len, ctx->rx_dma_buf_size);
            return;
        }

        /* Process the entry. End-node slot-based admission keeps in-flight
         * bytes ≤ buf_size, so DPU never laps. We trust DMA_COMPLETION
         * delivery (no gap-recovery scan). */
        if (process_rx_dma_entry(ctx, pos, dma_len,
                                 comp.req_id, comp.src_pod_id,
                                 comp.dst_pod_id, comp.flags) != 0) {
            DOCA_LOG_WARN("DMA_COMPLETION: process_rx_dma_entry failed at pos=%u len=%u",
                          pos, dma_len);
        }
        return;
    }

    if (mtype == DMESH_MSG_BATCH_REV_DONE) {
        /* Batched reverse-DMA notification: deliver each entry (identical per-entry
         * logic as DMESH_MSG_REV_DONE). One reaped comch msg → K deliveries, so the
         * single PE thread reaps 1/K — the 2-pod throughput lever. */
        if (len < 4) {
            DOCA_LOG_ERR("BATCH_REV_DONE: too short (len=%u)", len);
            return;
        }
        const struct dmesh_batch_rev_done_msg *b = (const struct dmesh_batch_rev_done_msg *)data;
        uint32_t n = b->count;
        if (n > BATCH_REVDONE_MAX) n = BATCH_REVDONE_MAX;
        if (len < 4u + 16u * n) {
            DOCA_LOG_ERR("BATCH_REV_DONE: len=%u short for count=%u", len, n);
            return;
        }
        for (uint32_t i = 0; i < n; i++) {
            const struct dmesh_rev_done_entry *e = &b->entries[i];
            if (!ctx->rx_dma_buffer || (size_t)e->pos + e->length > ctx->rx_dma_buf_size) {
                DOCA_LOG_ERR("BATCH_REV_DONE: invalid pos=%u len=%u buf=%zu",
                             e->pos, e->length, ctx->rx_dma_buf_size);
                continue;
            }
            if (process_rx_dma_entry(ctx, e->pos, e->length, e->req_id,
                                     e->src_pod_id, e->dst_pod_id, e->flags) != 0)
                DOCA_LOG_WARN("BATCH_REV_DONE: process_rx_dma_entry failed pos=%u len=%u",
                              e->pos, e->length);
        }
        return;
    }
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

    /* K = forward rings per pod (EU-sharding). Must match the DPU's
     * DPUMESH_RINGS_PER_POD so host TX rings pair 1:1 with DPU per-pod rings. */
    if ((env_val = getenv("DPUMESH_RINGS_PER_POD")) != NULL && atoi(env_val) > 0)
        ctx->k_rings = atoi(env_val);
    else
        ctx->k_rings = DPUMESH_RINGS_PER_POD_DEFAULT;
    if (ctx->k_rings > MAX_EU_PER_POD) ctx->k_rings = MAX_EU_PER_POD;

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

    ctx->reg_msg.type = DMESH_MSG_POD_REGISTER;
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

    /* The host datapath consumer is created here but its ID is not advertised
     * to the DPU: CPU→DPU uses the DMA ring and DPU→CPU uses reverse DMA. */

    /* K forward descriptor rings, exported in order as DMA_RING (sent BEFORE
     * DMA_BUFFER so the DPU's setup trigger sees all K before pairing). */
    for (int j = 0; j < ctx->k_rings; j++) {
        result = setup_dma_ring(&ctx->doca_objs, DMA_RING_SIZE, &ctx->dma_rings[j]);
        if (result != DOCA_SUCCESS) return result;
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

    /* Allocate Host RX DMA buffer (PCI mmap, DPA writes DPU→CPU data here).
     * Partitioned into k_rings disjoint reverse regions (rx_region_size each). */
    ctx->rx_dma_buf_size = buf_size;
    ctx->rx_region_size = ctx->rx_dma_buf_size / (ctx->k_rings > 0 ? ctx->k_rings : 1);
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

    return DOCA_SUCCESS;
}

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;
    ctx->notify_efd = -1;   /* before any goto fail: cleanup must not close fd 0 */

    init_config(ctx, config, app_name, worker_num);

    if (init_doca_device(ctx) != DOCA_SUCCESS) goto fail;
    if (init_control_path(ctx) != DOCA_SUCCESS) goto fail;
    if (init_datapath(ctx) != DOCA_SUCCESS) goto fail;

    /* Build the lock-free TX free-list: link 0->1->...->(num_slots-1)->empty,
     * head = 0 (all free). slot_next[i] = i+1; the last points to num_slots (empty). */
    ctx->slot_next = (uint32_t *)malloc((size_t)ctx->num_slots * sizeof(uint32_t));
    if (!ctx->slot_next) goto fail;
    for (int i = 0; i < ctx->num_slots; i++)
        ctx->slot_next[i] = (uint32_t)(i + 1);   /* last = num_slots = empty sentinel */
    atomic_store(&ctx->free_head, (uint_fast64_t)0);  /* tag 0, head index 0 */
    for (int j = 0; j < MAX_EU_PER_POD; j++)
        pthread_mutex_init(&ctx->ring_locks[j], NULL);
    atomic_init(&ctx->rr_counter, 0);

    /* Lock-free SPMC RX ring: seq[i] = i (cell i first writable at enq
     * position i), enq = deq = 0. */
    ctx->rx_ring = (struct rxq_cell *)malloc((size_t)RX_QUEUE_SIZE * sizeof(struct rxq_cell));
    if (!ctx->rx_ring) goto fail;
    for (uint32_t i = 0; i < RX_QUEUE_SIZE; i++)
        atomic_init(&ctx->rx_ring[i].seq, (uint_fast32_t)i);
    atomic_init(&ctx->rx_enq, (uint_fast32_t)0);
    atomic_init(&ctx->rx_deq, (uint_fast32_t)0);

    /* Readiness eventfd (non-blocking, close-on-exec). Non-fatal if it fails —
     * dpumesh_get_event_fd() then returns -1 and the caller falls back to polling. */
    ctx->notify_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ctx->notify_enabled = 0;

    atomic_init(&ctx->next_req_id, 1);
    for (int i = 0; i < MAX_PENDING; i++) {
        pthread_mutex_init(&ctx->pending[i].lock, NULL);
        pthread_cond_init(&ctx->pending[i].cond, NULL);
        ctx->pending[i].state = -1;
        ctx->pending[i].tx_slot = -1;
    }

    ctx->doca_objs.rx_data_hook = rx_data_hook;
    ctx->doca_objs.rx_hook_ctx = ctx;

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
    /* PE thread joined → no more dpumesh_notify() writers; safe to close. */
    if (ctx->notify_efd >= 0) { close(ctx->notify_efd); ctx->notify_efd = -1; }

    /* Free resources BEFORE destroying locks they depend on. */

    for (int i = 0; i < MAX_PENDING; i++) {
        dpumesh_pending_t *p = &ctx->pending[i];
        pthread_mutex_lock(&p->lock);
        if (p->state == 1 && p->desc.body_buf_slot >= 0) {
            dpumesh_rx_free(ctx, p->desc.body_buf_slot);
        }
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_mutex_unlock(&p->lock);
        pthread_mutex_destroy(&p->lock);
        pthread_cond_destroy(&p->cond);
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

    cleanup_objects(&ctx->doca_objs);

    for (int j = 0; j < MAX_EU_PER_POD; j++)
        pthread_mutex_destroy(&ctx->ring_locks[j]);
    if (ctx->slot_next) free(ctx->slot_next);
    if (ctx->rx_ring) free(ctx->rx_ring);

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
    /* Lock-free pop from the Treiber free-list (O(1), no slot_lock / no O(n) scan
     * / no cond). Backpressure: the caller has committed to this request, so when
     * the list is empty spin with a short capped backoff until a slot frees —
     * mirrors the old blocking behavior without the mutex/cond host-CPU cost. */
    struct timespec backoff = {0, 1000};  /* 1µs initial */
    for (;;) {
        uint_fast64_t old = atomic_load_explicit(&ctx->free_head, memory_order_acquire);
        int empty = 0;
        for (;;) {
            uint32_t head = (uint32_t)(old & 0xFFFFFFFFu);
            if (head >= (uint32_t)ctx->num_slots) { empty = 1; break; }
            uint_fast64_t newv = (((old >> 32) + 1) << 32) | (uint_fast64_t)ctx->slot_next[head];
            if (atomic_compare_exchange_weak_explicit(&ctx->free_head, &old, newv,
                    memory_order_acquire, memory_order_acquire))
                return (int)head;
            /* CAS failed: old reloaded — retry inner loop */
        }
        if (empty) {
            nanosleep(&backoff, NULL);
            if (backoff.tv_nsec < 50000) backoff.tv_nsec *= 2;  /* cap 50µs */
        }
    }
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Lock-free push onto the Treiber free-list (no mutex, no cond_signal/futex). */
    uint_fast64_t old = atomic_load_explicit(&ctx->free_head, memory_order_relaxed);
    for (;;) {
        uint32_t head = (uint32_t)(old & 0xFFFFFFFFu);
        ctx->slot_next[slot] = head;                 /* slot -> old head */
        uint_fast64_t newv = (((old >> 32) + 1) << 32) | (uint_fast64_t)(uint32_t)slot;
        if (atomic_compare_exchange_weak_explicit(&ctx->free_head, &old, newv,
                memory_order_release, memory_order_relaxed))
            return;
    }
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    struct dma_desc *dma;
    uint32_t ring_slot;

    if (desc == NULL) {
        DOCA_LOG_ERR("ENQUEUE rejected: desc is NULL");
        return -1;
    }

    if (desc->body_buf_slot < 0 || desc->body_buf_slot >= ctx->num_slots) {
        DOCA_LOG_ERR("ENQUEUE rejected: invalid body_buf_slot=%d (num_slots=%d)",
                     desc->body_buf_slot, ctx->num_slots);
        return -1;
    }

    if (desc->body_len > (uint32_t)ctx->slot_size) {
        DOCA_LOG_ERR("ENQUEUE rejected: body_len=%u exceeds slot_size=%d",
                     desc->body_len, ctx->slot_size);
        return -1;
    }

    /* EU-sharding: round-robin this request across the K forward rings, so the
     * pod's traffic spreads over K EUs. Each ring has its own lock (single-
     * producer per ring). K=1 → always ring 0 (legacy single-ring path).
     *
     * Flow control: end-to-end via slot-based admission only. dpumesh_tx_alloc
     * has already gated this call on free-list slot availability, and num_slots ×
     * slot_size = DPU_BUFFER_SIZE, so total in-flight bytes inside DPU's
     * buffer can never exceed buffer size. DPU/DPA do no FC of their own. */
    int ridx = (int)(atomic_fetch_add(&ctx->rr_counter, 1) % (unsigned)ctx->k_rings);
    struct dma_ring *ring = ctx->dma_rings[ridx];
    pthread_mutex_t *rlock = &ctx->ring_locks[ridx];
    pthread_mutex_lock(rlock);

    /* Block with exponential backoff until a DMA ring slot frees. DPA
     * advances the ring tail as it consumes descriptors. Backoff capped at
     * 50µs. */
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

    dma->mmap = ctx->dpa_mmap_handle;
    dma->addr = (uint64_t)ctx->dma_buffer +
                ((size_t)desc->body_buf_slot * ctx->slot_size);
    dma->size = desc->body_len;
    dma->idx  = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    DOCA_LOG_DBG("ENQUEUE: req_id=%u ring=%d slot=%u len=%u",
                 desc->req_id, ridx, ring_slot, desc->body_len);

    pthread_mutex_unlock(rlock);

    return 0;
}

/* ====================================================================
 * RX functions
 * ==================================================================== */

/* Poll-mode dequeue tuning: spin this many empty racy-checks before yielding
 * the core; at load the queue is rarely empty so the backoff is never reached
 * (no wakeup, no context switch). Mirrors the PE adaptive-poll pattern. */
#define RX_POLL_SPIN       1024
#define RX_POLL_BACKOFF_NS 20000   /* 20 us */

int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms) {
    {
        /* Lock-free spin-poll on the Vyukov RX ring; back off after sustained
         * idle. timeout_ms: 0 = non-blocking try, >0 = poll to deadline, <0 =
         * poll forever. There is NO cond-blocking path — the façade always polls. */
        struct timespec deadline; int have_dl = 0;
        if (timeout_ms > 0) {
            clock_gettime(CLOCK_MONOTONIC, &deadline);
            deadline.tv_sec  += timeout_ms / 1000;
            deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
            if (deadline.tv_nsec >= 1000000000L) { deadline.tv_sec++; deadline.tv_nsec -= 1000000000L; }
            have_dl = 1;
        }
        uint32_t idle = 0;
        for (;;) {
            if (rxq_try_pop(ctx, desc))
                return 0;
            if (timeout_ms == 0) return -1;            /* non-blocking try */
            if (++idle >= RX_POLL_SPIN) {
                if (have_dl) {
                    struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
                    if (now.tv_sec > deadline.tv_sec ||
                        (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec))
                        return -1;
                }
                struct timespec t = {0, RX_POLL_BACKOFF_NS};
                nanosleep(&t, NULL);
            }
        }
    }
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot) {
    /* slot carries the landing byte-offset (pos) into rx_dma_buffer. */
    if (slot < 0 || (size_t)slot >= ctx->rx_dma_buf_size) return NULL;
    return (uint8_t *)ctx->rx_dma_buffer + (size_t)slot;
}

void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot) {
    /* `slot` is the landing byte offset (pos), not a pool index — return the
     * admission credit to the matching ring so the DPA can reuse that position
     * (AFTER the consumer read it). */
    rx_credit_return(ctx, slot);
}

/* ====================================================================
 * Query / info functions
 * ==================================================================== */

int dpumesh_get_slot_size(dpumesh_ctx_t *ctx) {
    return ctx->slot_size;
}

/* Enable + return the readiness eventfd: a real fd that becomes readable
 * whenever an inbound request/response is delivered, so a caller can wait on it
 * with a VANILLA epoll/poll/select instead of busy-polling dequeue/poll_response.
 * The PE thread (notification-driven under DPUMESH_HOST_EPOLL=1) writes it on each
 * delivery. Drain it with a single read() of a uint64_t per wakeup, then collect
 * ready work via dpumesh_dequeue(0)/dpumesh_poll_response(). Returns -1 if the
 * eventfd could not be created. Idempotent; level-triggered-friendly. */
int dpumesh_get_event_fd(dpumesh_ctx_t *ctx) {
    if (!ctx || ctx->notify_efd < 0) return -1;
    ctx->notify_enabled = 1;
    return ctx->notify_efd;
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

int dpumesh_register_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);

    /* If slot is occupied, wait briefly for previous request to complete */
    if (p->state != -1) {
        struct timespec wait_ts;
        clock_gettime(CLOCK_REALTIME, &wait_ts);
        wait_ts.tv_sec += 2; /* 2-second collision wait budget */

        while (p->state != -1) {
            int rc = pthread_cond_timedwait(&p->cond, &p->lock, &wait_ts);
            if (rc != 0) {
                /* Stuck. If the previous request is in state -2 (timed out,
                 * TX deferred), the DPU owes us a TX_ACK — if it has not
                 * arrived after (wait_response timeout + 2s), the link is
                 * effectively dead. Reclaim the slot to keep the pending
                 * table from wedging the whole gateway; the late TX_ACK
                 * (if ever) will find state=-1 and no-op. */
                if (p->state == -2) {
                    if (p->tx_slot >= 0) {
                        dpumesh_tx_free(ctx, p->tx_slot);
                        p->tx_slot = -1;
                    }
                    DOCA_LOG_WARN("Pending slot %u reclaimed after -2 timeout (req_id=%u)",
                                  idx, req_id);
                    break;  /* fall through to claim the slot */
                }
                pthread_mutex_unlock(&p->lock);
                DOCA_LOG_ERR("Pending slot collision: req_id=%u idx=%u stuck state=%d",
                             req_id, idx, p->state);
                return -1;
            }
        }
    }

    p->state = 0;
    p->tx_slot = -1;
    p->owner_req_id = req_id;   /* claim this idx; TX_ACK frees only for this req_id */
    /* p->desc is fully overwritten by rx_deliver_desc before state=1, and only
     * read at state==1 — no need to pre-zero it. */
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

int dpumesh_poll_response(dpumesh_ctx_t *ctx, uint32_t req_id,
                          sw_descriptor_t *resp) {
    dpumesh_pending_t *p = &ctx->pending[req_id % MAX_PENDING];

    /* Lock-free fast path: while still waiting, a single acquire-load of the
     * volatile state avoids the mutex entirely. A racy 0 just polls again; any
     * non-0 is re-confirmed under the lock below (whose acquire also publishes
     * p->desc, written before the state=1 store in rx_deliver_desc). Same
     * lock-free spirit as dpumesh_dequeue's Vyukov ring check. */
    if (__atomic_load_n(&p->state, __ATOMIC_ACQUIRE) == 0)
        return 1;

    pthread_mutex_lock(&p->lock);
    int st = p->state;
    if (st == 0) {
        pthread_mutex_unlock(&p->lock);
        return 1;  /* still in flight */
    }
    if (st == 1) {
        *resp = p->desc;
        /* Response arrived = DPA finished reading the TX buffer; return it now. */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        /* Wake a register_pending() that may be colliding on this idx (rare:
         * only when in-flight ≥ MAX_PENDING). Not a per-request hot path. */
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return 0;
    }
    /* st == -2 (abandoned/cancelled) or -1 (unused/already consumed): no live
     * response to harvest. A late OP_RESPONSE RX (state -2) is reclaimed by
     * rx_deliver_desc independently. */
    pthread_mutex_unlock(&p->lock);
    return -1;
}

void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    if (p->state == 1) {
        /* Response arrived but was never consumed — reclaim RX landing + TX */
        if (p->desc.body_buf_slot >= 0)
            rx_credit_return(ctx, p->desc.body_buf_slot);
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == 0) {
        /* In-flight, no timeout yet — caller gave up; free TX now.
         * (No DPA-in-progress risk because 0 means DPA hasn't had a chance
         * to signal completion, i.e. request was cancelled pre-enqueue
         * path or immediately after enqueue failure.) */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == -2) {
        /* Timeout path: wait_response already returned -1 and left state=-2
         * pending a late TX_ACK. By the time we reach this branch the DPA is
         * guaranteed not to be reading the slot, so it is safe to force-free. */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return;
    } else {
        /* state == -1, already clean — nothing to do. */
    }
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
    if (p->state == 0) {
        if (p->tx_slot < 0) {
            /* TX_ACK already arrived between attach_tx and now — TX slot is
             * back in the pool; the entry just needs to be released so a
             * future register_pending can claim this idx. */
            p->state = -1;
            pthread_cond_broadcast(&p->cond);
        } else {
            /* TX_ACK still pending — switch to the deferred-release state
             * the TX_ACK handler already knows how to finish. */
            p->state = -2;
        }
    }
    /* state ∈ {-1, -2, 1}: someone else is finishing the entry — no-op. */
    pthread_mutex_unlock(&p->lock);
}
