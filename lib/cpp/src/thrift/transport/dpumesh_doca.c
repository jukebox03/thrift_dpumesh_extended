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

/* Decoupled TX-slot custody (kills the 2s collision-wait on conn/port reuse).
 * When a response is harvested — or the conn is closed / a server reply released —
 * BEFORE that request's TX_ACK has freed its slot, the still-held slot is parked
 * here and the pending entry drops to state -1 (reusable) immediately, so the next
 * request on the same port no longer blocks in register_pending waiting for the
 * prior ACK (the old -2/-3 stall whose backstop budget is 2s). The TX_ACK later
 * frees the parked slot by matching owner_key. Bounded; on overflow we fall back
 * to the -2/-3 backstop (correctness preserved, just the rare slow path). */
#define PENDING_DRAIN_MAX 16
struct pending_drain_slot { uint32_t owner_key; int tx_slot; };

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sw_descriptor_t desc;
    volatile int state;   /* this port(=connection)'s outbound-message lifecycle:
                           *  0 = has an outstanding in-flight message (awaiting its matched inbound)
                           *  1 = inbound message available (matched, ready to harvest)
                           * -1 = idle/reusable, -2 = abandoned (await TX_ACK), -3 = harvested (await TX_ACK) */
    int tx_slot;          /* TX slot owned by the CURRENT exchange, -1 if none */
    uint32_t owner_key;   /* (port<<16|seq) currently occupying this idx; guards the
                           * TX_ACK handler against a late/duplicate ACK after the
                           * key reuses this idx */
    /* Slots from PAST exchanges on this port still awaiting their TX_ACK, parked
     * here (decoupled) so the entry is reusable now. Each freed by its own ACK via
     * owner_key match; swap-removed on free. */
    struct pending_drain_slot drain[PENDING_DRAIN_MAX];
    int drain_n;
} dpumesh_pending_t;

/* Park the entry's still-unACKed CURRENT TX slot into its drain list so the entry
 * can be reused immediately. Returns 1 if parked (or nothing to park: tx_slot<0),
 * 0 if the drain list is full (caller keeps tx_slot + uses the -2/-3 backstop).
 * Caller must hold p->lock. */
static inline int pending_drain_park(dpumesh_pending_t *p) {
    if (p->tx_slot < 0) return 1;
    if (p->drain_n >= PENDING_DRAIN_MAX) return 0;
    p->drain[p->drain_n].owner_key = p->owner_key;
    p->drain[p->drain_n].tx_slot   = p->tx_slot;
    p->drain_n++;
    p->tx_slot = -1;
    return 1;
}

/* Connection table (connection-oriented, full-duplex — NOT request/response).
 * Index = local port [1,65535]; port 0 = BLANK (a fresh-connection message → the
 * accept queue). Each allocated port IS a connection (like a socket fd): it owns a
 * peer (pod,port), an inbound message queue, and an optional per-conn readiness
 * eventfd. The PE thread routes every inbound by dst_port → that conn's inbox and
 * wakes that conn's fd. There is NO request↔response matching: a conn just delivers
 * whatever arrives, in send order per conn, until close. Allocated from one
 * host-unique pool so client and server ports never collide (loopback-safe). */
#define DMESH_PORT_SPACE  65536
#define DMESH_ROLE_FREE   0
#define DMESH_ROLE_CLIENT 1
#define DMESH_ROLE_SERVER 2
/* Per-conn inbound queue depth (descriptors only — bodies stay in the shared RX
 * mmap, referenced by pos). Lazily malloc'd per LIVE conn (not 65536× pre-alloc).
 * Power of two. On overflow (app drains too slowly) the landing is reclaimed
 * (message dropped) + the shared RX credit still bounds total in-flight bytes. */
#define DMESH_INBOX_RING  256u
struct dmesh_port_slot {
    uint8_t          role;            /* FREE / CLIENT / SERVER */
    int16_t          peer_pod;        /* established peer pod, DMESH_POD_BLANK = not yet learned */
    uint16_t         peer_port;       /* established peer port, 0 = not yet learned */
    void            *user;            /* app's conn handle (returned by dmesh_next_ready);
                                       * set BEFORE role is published so the PE never enqueues
                                       * a port whose handle isn't visible yet. */
    /* Inbound SPSC ring: PE thread = sole producer (in_tail), the conn's owning
     * app thread = sole consumer (in_head). Lock-free. inbox==NULL until alloc. */
    sw_descriptor_t *inbox;           /* malloc'd ring[DMESH_INBOX_RING] */
    atomic_uint_fast32_t in_head;     /* consumer (app) */
    atomic_uint_fast32_t in_tail;     /* producer (PE) */
};
/* Ready-list SPSC ops (monotonic counters; PE producer, app consumer). The list
 * carries conn PORTS; dmesh_next_ready maps each to its slot->user. Provably never
 * full (≤ live conns < DMESH_PORT_SPACE; the inbox 0->1 edge admits each at most
 * once between drains), but the guard keeps a stray push from corrupting indices. */
static inline void ready_push(dpumesh_ctx_t *ctx, uint16_t port);
static inline int  ready_pop(dpumesh_ctx_t *ctx, uint16_t *port);
/* Inbound SPSC ring ops (monotonic counters; count = tail-head). */
static inline int inbox_push(struct dmesh_port_slot *psl, const sw_descriptor_t *d) {
    uint_fast32_t t = atomic_load_explicit(&psl->in_tail, memory_order_relaxed);
    uint_fast32_t h = atomic_load_explicit(&psl->in_head, memory_order_acquire);
    if (t - h >= DMESH_INBOX_RING) return 0;                 /* full */
    psl->inbox[t & (DMESH_INBOX_RING - 1)] = *d;
    atomic_store_explicit(&psl->in_tail, t + 1, memory_order_release);
    return (t == h) ? 2 : 1;   /* 2 = empty→non-empty transition (edge-trigger the fd) */
}
static inline int inbox_pop(struct dmesh_port_slot *psl, sw_descriptor_t *out) {
    uint_fast32_t h = atomic_load_explicit(&psl->in_head, memory_order_relaxed);
    uint_fast32_t t = atomic_load_explicit(&psl->in_tail, memory_order_acquire);
    if (h == t) return 0;                                    /* empty */
    *out = psl->inbox[h & (DMESH_INBOX_RING - 1)];
    atomic_store_explicit(&psl->in_head, h + 1, memory_order_release);
    return 1;
}
/* (port,seq) → 32-bit pending-table key (owner-guard value). */
static inline uint32_t dmesh_key(uint16_t port, uint16_t seq) {
    return ((uint32_t)port << 16) | (uint32_t)seq;
}
/* key → pending-table INDEX = the PORT (key's high 16 bits). Each live conn owns a
 * UNIQUE port and is single-outstanding (≤1 in-flight (port,seq) at a time), so
 * indexing by port is collision-FREE across conns — like a file descriptor indexing
 * its per-fd slot. Hashing (port,seq) instead birthday-collides even at a few
 * hundred conns, making register_pending block 2s and cascade (throughput death).
 * owner_key=(port<<16|seq) still guards a late/duplicate ACK from an old seq. */
static inline uint32_t dmesh_pidx(uint32_t key) {
    return (key >> 16) & (MAX_PENDING - 1);
}

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

    /* Client-side pending response table (keyed by (port,seq) via dmesh_key). */
    dpumesh_pending_t pending[MAX_PENDING];

    /* Endpoint port table + allocator (oriented-tuple demux). */
    struct dmesh_port_slot *ports;     /* [DMESH_PORT_SPACE] */
    pthread_mutex_t port_lock;
    uint32_t next_port;                /* bump cursor, wraps within [1,65535] */
    int32_t service_id;                /* this node's service id (SVC_NONE if client-only) */

    /* PE-published READY LIST (single channel-eventfd model). The PE pushes a
     * conn's port the moment its inbox goes empty->non-empty; the app drains this
     * list via dmesh_next_ready() instead of scanning every conn or holding a
     * per-conn fd. SPSC: PE = sole producer (ready_tail), the app event-loop =
     * sole consumer (ready_head). Sized to the port space so it never overflows
     * (each live conn appears at most once — the 0->1 edge dedups). */
    char _rl_pad0[64];
    atomic_uint_fast32_t ready_head;   /* consumer (app) */
    char _rl_pad1[64];
    atomic_uint_fast32_t ready_tail;   /* producer (PE) */
    char _rl_pad2[64];
    uint16_t ready_ring[DMESH_PORT_SPACE];
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

/* Ready-list SPSC: PE pushes a ready conn's port; the app event-loop pops it via
 * dmesh_next_ready. Monotonic counters (count = tail-head); ring index masks the
 * power-of-two DMESH_PORT_SPACE. */
static inline void ready_push(dpumesh_ctx_t *ctx, uint16_t port) {
    uint_fast32_t t = atomic_load_explicit(&ctx->ready_tail, memory_order_relaxed);
    uint_fast32_t h = atomic_load_explicit(&ctx->ready_head, memory_order_acquire);
    if (t - h >= DMESH_PORT_SPACE) return;   /* provably never full; guard anyway */
    ctx->ready_ring[t & (DMESH_PORT_SPACE - 1)] = port;
    atomic_store_explicit(&ctx->ready_tail, t + 1, memory_order_release);
}
static inline int ready_pop(dpumesh_ctx_t *ctx, uint16_t *port) {
    uint_fast32_t h = atomic_load_explicit(&ctx->ready_head, memory_order_relaxed);
    uint_fast32_t t = atomic_load_explicit(&ctx->ready_tail, memory_order_acquire);
    if (h == t) return 0;                                    /* empty */
    *port = ctx->ready_ring[h & (DMESH_PORT_SPACE - 1)];
    atomic_store_explicit(&ctx->ready_head, h + 1, memory_order_release);
    return 1;
}

/* Deliver one inbound descriptor (CONNECTION model — no request/response match).
 * Route purely by the local dst_port (like a TCP demux resolving to a socket):
 *   - BLANK(0)          → a fresh-connection message → SPMC accept ring + channel fd
 *   - an allocated conn → that conn's inbound SPSC ring + the READY LIST + channel fd
 *   - free/unknown port → stale (conn closed) → reclaim the landing credit
 * Bodies stay in the shared RX mmap (pos); only the descriptor is queued. A conn
 * just delivers whatever arrives (in send order) until close. */
static void rx_deliver_desc(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc, int slot)
{
    uint16_t dport = desc->dst_port;

    /* (1) BLANK → a fresh connection → accept queue (the "listen socket"). */
    if (dport == DMESH_PORT_BLANK) {
        uint_fast32_t pos = atomic_load_explicit(&ctx->rx_enq, memory_order_relaxed);
        struct rxq_cell *c = &ctx->rx_ring[pos & (RX_QUEUE_SIZE - 1)];
        uint_fast32_t cseq = atomic_load_explicit(&c->seq, memory_order_acquire);
        if ((int_fast32_t)(cseq - pos) != 0) {
            DOCA_LOG_ERR("RX deliver: accept queue full, dropping seq=%u", desc->seq);
            rx_reclaim(ctx, slot);
            return;
        }
        c->desc = *desc;
        atomic_store_explicit(&c->seq, pos + 1, memory_order_release);
        atomic_store_explicit(&ctx->rx_enq, pos + 1, memory_order_relaxed);
        dpumesh_notify(ctx);                       /* endpoint/listen fd */
        return;
    }

    /* (2) an allocated conn (CLIENT or SERVER) → its inbound ring + the ready list. */
    struct dmesh_port_slot *psl = &ctx->ports[dport];
    uint8_t role = __atomic_load_n(&psl->role, __ATOMIC_ACQUIRE);
    if (role != DMESH_ROLE_FREE) {
        int r = inbox_push(psl, desc);
        if (r == 0) {
            /* inbox full (app draining too slowly) → drop + reclaim the landing.
             * The shared RX credit still bounds total in-flight bytes. */
            DOCA_LOG_ERR("RX deliver: conn %u inbox full, dropping seq=%u", dport, desc->seq);
            rx_reclaim(ctx, slot);
        } else if (r == 2 && ctx->notify_enabled) {
            /* Inbox went empty→non-empty: this conn just became ready. Publish its
             * port to the READY LIST and wake the ONE channel eventfd. The app drains
             * the conn to EAGAIN, so the next 0→1 edge re-arms it (no in_ready flag
             * needed). One eventfd write per ready-batch — NOT per message — and the
             * app reads only ready conns (no scan). Gated on notify_enabled: a busy-
             * poll app (no event fd) iterates its own conns instead, so we skip the
             * list entirely for it. */
            ready_push(ctx, dport);
            dpumesh_notify(ctx);
        }
        return;
    }

    /* (3) free/unknown port → stale (conn closed) → reclaim the landing. */
    rx_reclaim(ctx, slot);
}

/*
 * Parse + deliver one DMA-reverse entry at rx_dma_buffer[pos] whose body
 * length is dma_len. Per-request metadata (req_id, src_pod_id, dst_pod_id,
 * flags) is taken from the comch DMA_COMPLETION message — NOT from the DMA
 * payload. The DMA payload is the body itself (no in-band header).
 * Returns 0 on success, -1 on malformed/undeliverable.
 */
static int process_rx_dma_entry(dpumesh_ctx_t *ctx, const struct dmesh_rev_done_entry *e) {
    uint32_t pos = e->pos, dma_len = e->length;
    if (!ctx->rx_dma_buffer || (size_t)pos + dma_len > ctx->rx_dma_buf_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->rx_dma_buf_size);
        return -1;
    }
    /* Body must fit one slot; the DPA caps each reverse DMA at DPA_DMA_COPY_MAX
     * (= default slot_size), but guard so a non-default slot_size can't overrun. */
    if (dma_len > (uint32_t)ctx->slot_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: len=%u exceeds slot_size=%d (seq=%u)",
                     dma_len, ctx->slot_size, e->seq);
        return -1;
    }

    /* Zero-copy: deliver the landing byte-offset `pos`; the consumer reads
     * rx_dma_buffer[pos] directly and returns the credit at rx_free. */
    int slot = (int)pos;

    sw_descriptor_t desc;
    memset(&desc, 0, sizeof(desc));
    desc.body_buf_slot = slot;
    desc.body_len      = dma_len;
    /* oriented tuple from the DPU completion (peer = src; dst is me) */
    desc.src_pod       = e->src_pod_id;
    desc.src_service   = e->src_service;
    desc.dst_service   = e->dst_service;
    desc.src_port      = e->src_port;
    desc.dst_port      = e->dst_port;
    desc.seq           = e->seq;
    desc.dst_pod       = (int16_t)ctx->pod_id;   /* delivered here → I am the dst pod */
    desc.valid         = 1;

    rx_deliver_desc(ctx, &desc, slot);
    return 0;
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    /* Dispatch on the 1-byte type at offset 0. The only DPU->Host messages this
     * hook receives are BATCH_FWD_ACK and BATCH_REV_DONE (the non-batched FWD_ACK/
     * REV_DONE types no longer exist); both carry a uint8_t type as their first
     * field and values stay < 256, so a single byte read is sufficient. */
    uint8_t mtype = data[0];


    if (mtype == DMESH_MSG_BATCH_FWD_ACK) {
        /* Batched TX_ACK: free the TX slot of each req_id (owner-guarded).
         * One message → K frees. */
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
            uint32_t key = dmesh_key(b->acks[i].port, b->acks[i].seq);
            uint32_t idx = dmesh_pidx(key);
            dpumesh_pending_t *p = &ctx->pending[idx];
            pthread_mutex_lock(&p->lock);
            if (p->tx_slot >= 0 && p->owner_key == key) {
                /* ack = sole free authority for a SENT slot. Free regardless of
                 * state (0=in flight, 1=response landed, -2=closed, -3=harvested);
                 * for the terminal-awaiting-ack states (-2/-3) the slot was the
                 * last thing holding the entry, so clear it to -1 (reusable). */
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
                if (p->state == -2 || p->state == -3) {
                    p->state = -1;
                    pthread_cond_broadcast(&p->cond);
                }
            } else {
                /* Not the current exchange — maybe a slot parked in drain[] by a
                 * PAST exchange on this port (decoupled so the entry could be reused
                 * before this ack arrived). Free it by owner_key; swap-remove. */
                for (int d = 0; d < p->drain_n; d++) {
                    if (p->drain[d].owner_key == key) {
                        dpumesh_tx_free(ctx, p->drain[d].tx_slot);
                        p->drain[d] = p->drain[--p->drain_n];
                        break;
                    }
                }
            }
            pthread_mutex_unlock(&p->lock);
        }
        return;
    }


    if (mtype == DMESH_MSG_BATCH_REV_DONE) {
        /* Batched reverse-DMA notification: deliver each entry. One reaped comch
         * msg → K deliveries, so the single PE thread reaps 1/K — the 2-pod lever. */
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
            if (process_rx_dma_entry(ctx, e) != 0)
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

    /* This node's service id (DPU service_table[service_id]=pod_id). Default =
     * pod_id, so a client connecting to dst_service=N reaches the pod hosting
     * service N. Override with DPUMESH_SERVICE_ID. */
    if ((env_val = getenv("DPUMESH_SERVICE_ID")) != NULL)
        ctx->service_id = atoi(env_val);
    else
        ctx->service_id = ctx->pod_id;
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
    ctx->reg_msg.service_id = ctx->service_id;   /* DPU: service_table[service_id]=pod_id */
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

    for (int i = 0; i < MAX_PENDING; i++) {
        pthread_mutex_init(&ctx->pending[i].lock, NULL);
        pthread_cond_init(&ctx->pending[i].cond, NULL);
        ctx->pending[i].state = -1;
        ctx->pending[i].tx_slot = -1;
    }

    /* Endpoint port table + allocator (oriented-tuple demux). calloc → every slot
     * role=FREE, inbox_ready=0. Ports allocated from 1 (0 = BLANK sentinel). */
    ctx->ports = (struct dmesh_port_slot *)calloc(DMESH_PORT_SPACE, sizeof(struct dmesh_port_slot));
    if (!ctx->ports) goto fail;
    pthread_mutex_init(&ctx->port_lock, NULL);
    ctx->next_port = 1;

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
        for (int d = 0; d < p->drain_n; d++)   /* decoupled-but-unACKed slots */
            dpumesh_tx_free(ctx, p->drain[d].tx_slot);
        p->drain_n = 0;
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
    if (ctx->ports) { free(ctx->ports); ctx->ports = NULL; pthread_mutex_destroy(&ctx->port_lock); }

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

    /* Loopback (dst == self) is NOW supported: demux is by dst_port (client vs
     * server socket), not by req_id origin, so a self-routed request and its reply
     * are distinguished even on the same host. The DPU may also route a service to
     * the sender's own pod. No reject here. */

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
    /* conn-sharding: a connection's messages ALL use one ring (hashed by the
     * sender's local port), so they stay FIFO on one EU → per-conn send order is
     * preserved; different conns spread across the K rings/EUs (throughput kept). */
    int ridx = (int)((unsigned)desc->src_port % (unsigned)ctx->k_rings);
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

    /* TX slot lifetime is owned by the pending mechanism for BOTH legs (the façade
     * stamps the oriented tuple; request-vs-response is not a wire flag):
     *   - client request: caller registers + attach_tx; the poll_response /
     *     cancel_pending paths or the TX_ACK handler free the TX slot.
     *   - server reply:   caller registers + attach_tx + release_async; the TX_ACK
     *     handler frees the TX slot via the deferred state -2 → -1 path. */

    dma->mmap = ctx->dpa_mmap_handle;
    dma->addr = (uint64_t)ctx->dma_buffer +
                ((size_t)desc->body_buf_slot * ctx->slot_size);
    dma->size = desc->body_len;
    /* oriented endpoint tuple → DPA passthrough → DPU routes (dst_pod==BLANK).
     * src identity is stamped from the ctx (this node), not the caller's desc. */
    dma->seq         = desc->seq;
    dma->src_port    = desc->src_port;
    dma->dst_port    = desc->dst_port;
    dma->src_service = (int8_t)ctx->service_id;
    dma->dst_service = (int8_t)desc->dst_service;
    dma->dst_pod_id  = desc->dst_pod;
    dma->src_pod_id  = ctx->pod_id;

    __sync_synchronize();
    dma->valid = 1;

    DOCA_LOG_DBG("ENQUEUE: seq=%u dst_svc=%d dst_pod=%d ring=%d slot=%u len=%u",
                 desc->seq, desc->dst_service, desc->dst_pod, ridx, ring_slot, desc->body_len);

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

/* Allocate a host-unique port (>=1) and register it as a CLIENT or SERVER socket.
 * `user` is the app's conn handle (returned later by dmesh_next_ready); it is
 * stored BEFORE role is published so the PE, which may deliver + enqueue this port
 * the instant it sees role!=FREE, never hands back a NULL handle. The role is
 * published with RELEASE so the PE's ACQUIRE-load sees a fully-initialized slot.
 * Returns 0 on exhaustion. */
uint16_t dpumesh_alloc_port(dpumesh_ctx_t *ctx, int role, void *user) {
    pthread_mutex_lock(&ctx->port_lock);
    for (uint32_t scanned = 0; scanned < DMESH_PORT_SPACE - 1; scanned++) {
        uint32_t p = ctx->next_port;
        ctx->next_port = (p + 1 >= DMESH_PORT_SPACE) ? 1 : p + 1;  /* wrap, skip 0 */
        if (p == 0) continue;
        struct dmesh_port_slot *psl = &ctx->ports[p];
        if (psl->role == DMESH_ROLE_FREE) {
            /* Per-port inbound ring is allocated once and KEPT for the lifetime of
             * the process (reused when this port number is reallocated) — never
             * freed mid-run, so a stale PE delivery can't use-after-free it. */
            if (!psl->inbox) {
                psl->inbox = (sw_descriptor_t *)malloc(DMESH_INBOX_RING * sizeof(sw_descriptor_t));
                if (!psl->inbox) { pthread_mutex_unlock(&ctx->port_lock); return 0; }
            }
            atomic_store_explicit(&psl->in_head, 0, memory_order_relaxed);
            atomic_store_explicit(&psl->in_tail, 0, memory_order_relaxed);
            psl->peer_pod   = DMESH_POD_BLANK;
            psl->peer_port  = 0;
            psl->user       = user;     /* visible before role (publish ordering below) */
            /* Publish role LAST (RELEASE) so the PE's ACQUIRE-load sees the fully
             * initialized inbox/head/tail/user before it can deliver here. */
            __atomic_store_n(&psl->role, (uint8_t)role, __ATOMIC_RELEASE);
            pthread_mutex_unlock(&ctx->port_lock);
            return (uint16_t)p;
        }
    }
    pthread_mutex_unlock(&ctx->port_lock);
    DOCA_LOG_ERR("dpumesh_alloc_port: no free ports");
    return 0;
}

/* Release a conn. role=FREE (RELEASE) first → the PE drops further deliveries as
 * stale (and dmesh_next_ready skips any ready-list entry still pointing here); then
 * reclaim any undelivered inbound (their RX credits). The inbox ring is kept for
 * the slot's next reuse. */
void dpumesh_free_port(dpumesh_ctx_t *ctx, uint16_t port) {
    if (port == 0 || port >= DMESH_PORT_SPACE) return;
    struct dmesh_port_slot *psl = &ctx->ports[port];
    __atomic_store_n(&psl->role, DMESH_ROLE_FREE, __ATOMIC_RELEASE);
    psl->user = NULL;
    if (psl->inbox) {
        sw_descriptor_t d;
        while (inbox_pop(psl, &d)) rx_credit_return(ctx, d.body_buf_slot);
    }
}

/* Pop the next inbound message descriptor for a conn (CLIENT or SERVER — one
 * path). Returns 1 + fills *out, or 0 if the conn inbox is empty. The body is in
 * the shared RX mmap at out->body_buf_slot (a landing pos). */
int dpumesh_conn_recv(dpumesh_ctx_t *ctx, uint16_t port, sw_descriptor_t *out) {
    if (port == 0 || port >= DMESH_PORT_SPACE) return 0;
    struct dmesh_port_slot *psl = &ctx->ports[port];
    if (!psl->inbox) return 0;
    return inbox_pop(psl, out);
}

/* Pop the next conn that has inbound, from the PE-published ready list, and return
 * its app handle (the `user` registered at alloc). NULL when the list is drained.
 * No scan: the PE put exactly the ready conns here. A list entry whose conn has
 * since closed (role==FREE) is skipped — its port may even have been recycled, but
 * round-robin allocation makes that astronomically distant; either way a stale
 * entry only ever costs one extra empty drain. Single-consumer (the event loop);
 * call it after waking on dmesh_get_event_fd, drain each returned conn to EAGAIN. */
void *dpumesh_next_ready(dpumesh_ctx_t *ctx) {
    uint16_t port;
    while (ready_pop(ctx, &port)) {
        struct dmesh_port_slot *psl = &ctx->ports[port];
        if (__atomic_load_n(&psl->role, __ATOMIC_ACQUIRE) != DMESH_ROLE_FREE)
            return psl->user;
        /* else: conn closed since it was enqueued → stale, skip to the next. */
    }
    return NULL;
}

/* register_pending / poll_response / cancel_pending / pending_release_async are
 * GONE: the connection model has no request↔response matching (inbound goes to the
 * conn inbox, not a matched pending slot — so NO 2s collision-wait). TX-slot
 * custody is now the drain[] list: dpumesh_tx_track records a sent slot keyed by
 * (port,seq); the BATCH_FWD_ACK handler (rx_data_hook) frees it on its ACK. A conn
 * may have many un-ACKed slots in flight (multi-outstanding) up to PENDING_DRAIN_MAX. */
void dpumesh_tx_track(dpumesh_ctx_t *ctx, uint16_t port, uint16_t seq, int tx_slot) {
    uint32_t key = dmesh_key(port, seq);
    dpumesh_pending_t *p = &ctx->pending[dmesh_pidx(key)];
    pthread_mutex_lock(&p->lock);
    if (p->drain_n < PENDING_DRAIN_MAX) {
        p->drain[p->drain_n].owner_key = key;
        p->drain[p->drain_n].tx_slot   = tx_slot;
        p->drain_n++;
    } else {
        DOCA_LOG_ERR("tx_track: drain full for port=%u (>%d in flight); slot %d untracked",
                     port, PENDING_DRAIN_MAX, tx_slot);
    }
    pthread_mutex_unlock(&p->lock);
}

/* (dpumesh_pending_attach_tx removed — superseded by dpumesh_tx_track) */

/* (dpumesh_poll_response removed — the connection model delivers inbound to the
 * conn inbox via dpumesh_conn_recv; there is no request↔response matching.) */

/* (dpumesh_cancel_pending / dpumesh_pending_release_async removed — close just
 * frees the conn via dpumesh_free_port; any un-ACKed TX slots in drain[] are
 * freed by their own TX_ACKs.) */
