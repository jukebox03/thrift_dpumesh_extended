/*
 * dpu_proxy.c — L4 engine under the L7 proxy seam (plan.md).
 *
 * Both directions (client request AND backend reply) run the SAME machinery:
 *
 *   forward completion ──▶ per-conn INPUT WINDOW (bytes in arrival order;
 *   (body landed in DPU     zero-copy views over staging; a seam buffer
 *    staging, in place)     aligns the unconsumed tail ONLY when a parse
 *                           stalls across extent boundaries)
 *                      ──▶ proxy_route (MOCK) → segs {off,len,dst}
 *                      ──▶ per-(dst pod, region) LANE: units gathered into
 *                           ONE chained-src SG-DMA per batch (ARM generic
 *                           doca_dma, staging → host RX, measured 07-03)
 *                      ──▶ batch completion → batched REV_DONE entries to the
 *                           receiver + batched TX_ACK custody release to the
 *                           senders (END-TO-END: a sender's slot is held until
 *                           the egress DMA has READ its staging bytes).
 *
 * Direction asymmetry is confined to dst resolution: a request seg's dst is
 * the proxy's decision (or the L4 default route on DEFER) and owns/creates an
 * upstream; a reply seg's dst comes from the conntrack table (the proxy only
 * confirms). Everything else — window, segs, lanes, custody — is shared.
 *
 * Delivery is a BYTE STREAM: the receiver gets each unit as consecutive
 * <=8 KB chunks (one REV_DONE entry each) and parses/frames itself. Per
 * receiving conn, delivery order == seg order (lane FIFO + in-order batch
 * retirement); a unit's chunks are never interleaved with another unit's.
 *
 * Single-threaded: everything here runs on the one DPU worker thread
 * (ingest from the consumer-PE callbacks, drain from the main loop, DMA
 * completions from the control PE) — no locks.
 *
 * The engine is ALWAYS on — it is the sole DPU→host reverse (egress) path.
 * DPUMESH_PROXY only selects the request parser (passthru default / frame / L7);
 * the data plane is identical either way.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "dpu_proxy.h"
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include "dpu_l7.h"     /* the L7 author hook (dmesh_l7_route) */

#include "object.h"
#include "dpu_worker.h"
#include "comch_server.h"
#include "dpa_common.h"
#include "buffer.h"

#include <doca_log.h>
#include <doca_error.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_ctx.h>
#include <doca_mmap.h>
#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>

#include <stdlib.h>
#include <string.h>

DOCA_LOG_REGISTER(DPU_PROXY);

/* ====== tunables ====== */

/* One REV_DONE entry (one delivered chunk) is capped at the slot size — the
 * host guards len <= slot_size and its RX credit is per-entry. */
#define PX_ENTRY_BYTES_MAX  DPUMESH_SLOT_SIZE

/* doca_dma task pool: SG batches + credit-refresh reads share it. */
#define PX_DMA_TASKS        256

/* Source pieces per SG task. Measured cap (07-03): max_buf_list_len = 64;
 * clamped further by the device capability at init. */
#define PX_SG_PIECES_MAX    64

/* Seam buffer cap: the largest contiguous view the parser can demand (a frame
 * spanning arrivals must fit here). */
#define PX_SEAM_MAX_DEFAULT (512u * 1024u)

/* Frame-mock frame cap — must stay <= the seam cap or a legal frame could
 * poison the conn via the stalled-at-seam-cap path. */
#define PX_FRAME_MAX        (256u * 1024u)
#define PX_FRAME_HDR        5u

/* L7 head-only path: the parser (dmesh_l7_route, dpu_l7.c) is given at most a
 * PX_HEAD_MAX contiguous window to read a message HEAD (length + routing key);
 * the BODY is never linearized — it ships from staging via SG. So the only copy
 * is a bounded <=PX_HEAD_MAX assembly, and only when a head straddles a slot.
 * A head larger than this poisons the conn (cf. Envoy max_request_headers_kb). */
#define PX_HEAD_MAX         (4u * 1024u)
/* Max bytes shipped as ONE egress unit on the L7 path — keeps a unit within the
 * SG piece cap (PX_SG_PIECES_MAX slots) and the host RX region. A larger message
 * streams as consecutive units, in order, to the one resolved backend. */
#define PX_L7_UNIT_MAX      (128u * 1024u)

/* Refresh the (DMA-read) host credit cache when headroom drops below this
 * many entries — same lazy scheme as the DPA reverse admission. */
#define PX_CREDIT_REFRESH_MARGIN 64

/* Pool sizes. Arrivals are bounded by total in-flight sender slots
 * (MAX_PODS x 4096); units/pieces sized to match (pass-through is 1:1). */
#define PX_ARRIVAL_POOL  (MAX_PODS * (DPU_BUFFER_SIZE / DPUMESH_SLOT_SIZE))
#define PX_UNIT_POOL     PX_ARRIVAL_POOL
#define PX_PIECE_POOL    (2 * PX_ARRIVAL_POOL)
#define PX_BATCH_POOL    1024
#define PX_CONN_HASH     (1u << 15)   /* buckets (chained) */

/* ====== internal structs ====== */

/* One arrived forward completion = one staging extent, held for custody until
 * every byte is either egress-completed or dropped; then its (port,seq) is
 * batch-TX_ACKed to the sender and the node freed. Outlives its conn (a piece
 * in flight keeps it via unfreed > 0). */
struct px_arrival {
    struct px_arrival *next;      /* window list / freelist */
    uint64_t stream_base;         /* stream offset of this extent's byte 0 */
    int32_t  pod_idx;             /* staging owner (objs->pods[] slot) */
    uint32_t staging_off;
    uint32_t len;
    uint32_t unfreed;             /* custody: bytes not yet egressed/dropped */
    uint32_t claimed_round;       /* scratch: seg-claimed bytes, one parse round */
    int32_t  ack_pod;             /* TX_ACK target (original sender, untranslated) */
    uint16_t ack_port, ack_seq;
    uint8_t  route_group;
    uint8_t  in_window;           /* 1 while linked in the conn window */
};

/* One contiguous staging extent of a unit (an SG source piece). */
struct px_piece {
    struct px_piece   *next;
    struct px_arrival *arr;       /* custody countdown target */
    int32_t  pod_idx;
    uint32_t staging_off, len;
};

/* One delivery to one receiving conn: one seg (or a FIN marker, total_len 0).
 * Self-contained (values, not conn pointers) so it survives conn teardown. */
struct px_unit {
    struct px_unit *next;
    int8_t   src_pod_id, src_service, dst_service;
    uint16_t src_port, dst_port, seq;
    uint32_t total_len;           /* 0 == FIN / notify-only (still 1 RX credit) */
    uint32_t landing_pos;         /* absolute pos in the host RX buffer (at submit) */
    uint32_t emit_off;            /* REV_DONE emission cursor (resumable) */
    uint8_t  emit_fin_done;
    int8_t   dst_pod_idx;         /* receiver pod slot (REV_DONE target; set at ship) */
    uint8_t  err;                 /* multi-thread done-queue: batch errored → skip REV_DONE */
    struct px_piece *pieces, *pieces_tail;
    int npieces;
};

/* DMA completion dispatch tag. */
struct px_op {
    int kind;                     /* 0 = SG batch, 1 = credit refresh */
    struct px_batch *batch;       /* kind 0 */
    int pod_idx, region;          /* kind 1 */
    struct doca_buf *src_buf, *dst_buf;   /* kind 1 (kind 0 keeps them in the batch) */
};

/* One submitted SG-DMA op: a FIFO prefix of a lane's units landing at one
 * contiguous dst range. Retired (entries emitted + custody released) strictly
 * in submission order per lane. */
struct px_batch {
    struct px_batch *next;
    struct px_unit  *units;       /* FIFO */
    int      nunits;
    int      pod_idx, region;
    uint32_t entries;             /* RX credits consumed */
    uint32_t bytes;
    volatile int done, error;
    struct doca_buf *src_head, *dst_buf;
    struct px_op op;
};

/* Per-(dst pod slot, region) egress lane. Region r = the slice of the host RX
 * buffer whose landings the host credits back to forward ring r — the same
 * pos→ring mapping the DPA reverse path used, so host code is untouched.
 * A receiving conn always lands in ONE lane (region = dst_port % K): its
 * delivery order is the lane FIFO. */
struct px_lane {
    /* ingest→worker inbox: ingest (main thread) appends here under inq_lock; the
     * owning egress worker splices it onto qhead (O(1)) at the top of its pump.
     * qhead/qtail and everything below are then WORKER-LOCAL (no lock). For the
     * single-thread default (n_eng==1) enqueue goes straight to qhead (no lock). */
    struct px_unit  *inq_head, *inq_tail;
    pthread_mutex_t  inq_lock;
    struct px_unit  *qhead, *qtail;   /* queued units (not yet submitted) — worker-local */
    struct px_batch *fhead, *ftail;   /* in-flight/completed batches, FIFO — worker-local */
    uint32_t cursor;                  /* next landing byte offset within the region */
    uint64_t sent_entries;            /* credits consumed (cumulative) */
    uint64_t cached_freed;            /* host freed counter, DMA-read cache */
    int      refresh_inflight;
    int      warned_no_credit_addr;
};

struct px_conn {
    dmesh_proxy_conn pub;
    struct px_conn *hnext;
    struct px_arrival *whead, *wtail; /* input window (unparsed tail kept) */
    uint64_t stream_end;              /* total bytes arrived */
    uint64_t parse_pos;               /* window cursor (consumed boundary) */
    /* seam: a contiguous copy of stream bytes [seam_base, seam_base+seam_len),
     * built only when a parse stalls across extent boundaries ("tail aligned
     * on demand"). Parser-view only — SG always gathers from staging. */
    uint8_t *seam;
    uint32_t seam_cap, seam_len;
    uint64_t seam_base;
    int      seam_on;
    int      fin_pending, dead;
    int32_t  fin_ack_pod;
    uint16_t fin_ack_port, fin_ack_seq;
    uint16_t egress_seq;              /* per-conn delivery unit counter */
    int      dst_service_set;
    dmesh_proxy_route_fn route_fn;    /* linear parser (passthru/frame); NULL for L7 */
    uint32_t parse_win_max;           /* seam cap for THIS conn (seam_max, or PX_HEAD_MAX for L7) */
    int      is_l7;                   /* 1 = head-only L7 path (px_parse_l7) */
    uint32_t msg_remaining;           /* L7: bytes left to ship of the in-progress message */
    int32_t  msg_dst;                 /* L7: backend resolved once at the message head */
};

#define MAX_ARM_ENG 8

/* One egress worker. Owns its own DOCA dma/PE/inventory + batch pool + the lanes
 * whose dst pod_idx % n_eng == id. For n_eng==1 the single engine runs INLINE on
 * the main thread (pe == objs->pe, threaded==0) — the proven single-thread path,
 * unchanged. For n_eng>=2 each engine runs on its OWN thread doing the heavy
 * per-message DOCA SG-DMA lifecycle, and hands retired units back to the ingest
 * (main) thread via the done-queue, which does REV_DONE + custody + pool free. */
struct px_engine {
    struct objects *objs;
    int      id;
    int      threaded;                /* 1 = own thread + own pe; 0 = inline on main */
    struct doca_dma           *dma;
    struct doca_ctx           *dma_ctx;
    struct doca_pe            *pe;     /* own PE (threaded) / objs->pe (inline) */
    struct doca_buf_inventory *inv;
    int      dma_tasks_inflight;
    struct px_batch *batch_mem, *batch_free;
    struct px_unit  *done_head, *done_tail;   /* worker→ingest, under done_lock */
    pthread_mutex_t  done_lock;
    struct px_unit  *emit_head, *emit_tail;   /* ingest-local resumable drain list */
    pthread_t thread;
    volatile int stop;
};

struct dmesh_proxy {
    /* Per-connection mock selection (NOT one global mock): a REQUEST stream
     * frames iff its addressed service is designated frame; every other request
     * and EVERY reply uses passthru. This is what lets vanilla (shim) apps and
     * the frame validator share one deploy — see px_service_frames(). */
    int      default_frame;              /* DPUMESH_PROXY=frame → request default = frame */
    uint8_t  svc_frame[POD_ID_SPACE];    /* DPUMESH_PROXY_FRAME_SVC csv → force-frame these services */
    uint8_t  svc_l7[POD_ID_SPACE];       /* DPUMESH_PROXY_L7_SVC csv → route via the L7 author hook */
    uint32_t seam_max;
    uint32_t sg_pieces_max;

    struct px_conn **buckets;         /* PX_CONN_HASH */

    /* fixed pools + freelists — INGEST-only (main thread): arrivals/pieces/units.
     * The batch pool moved per-engine (worker allocs+frees its own). */
    struct px_arrival *arr_mem,  *arr_free;
    struct px_piece   *piece_mem, *piece_free;
    struct px_unit    *unit_mem, *unit_free;

    struct px_lane lanes[MAX_PODS][MAX_EU_PER_POD];
    struct px_op   refresh_ops[MAX_PODS][MAX_EU_PER_POD];

    /* ARM SG-DMA egress workers (DPUMESH_ARM_EGRESS_THREADS). n_eng==1 = inline
     * on main (proven default); n_eng>=2 = n_eng worker threads. A lane is owned
     * by engine (pod_idx % n_eng). */
    struct px_engine engines[MAX_ARM_ENG];
    int n_eng;

    /* credit-read landing cells: one 64B cell per lane, DPU-local mmap */
    struct doca_mmap *scratch_mmap;
    uint8_t *scratch;

    /* counters (no hot-path logging) */
    uint64_t stat_msgs, stat_segs, stat_units, stat_batches, stat_drop_bytes;
};

#define PX_SCRATCH_CELL 64
#define PX_SCRATCH_OFF(pi, r) (((size_t)(pi) * MAX_EU_PER_POD + (size_t)(r)) * PX_SCRATCH_CELL)

/* ====== pools ====== */

static struct px_arrival *px_arrival_alloc(struct dmesh_proxy *px) {
    struct px_arrival *a = px->arr_free;
    if (a) px->arr_free = a->next;
    return a;
}
static void px_arrival_free(struct dmesh_proxy *px, struct px_arrival *a) {
    a->next = px->arr_free; px->arr_free = a;
}
static struct px_piece *px_piece_alloc(struct dmesh_proxy *px) {
    struct px_piece *p = px->piece_free;
    if (p) px->piece_free = p->next;
    return p;
}
static void px_piece_free(struct dmesh_proxy *px, struct px_piece *p) {
    p->next = px->piece_free; px->piece_free = p;
}
static struct px_unit *px_unit_alloc(struct dmesh_proxy *px) {
    struct px_unit *u = px->unit_free;
    if (u) { px->unit_free = u->next; memset(u, 0, sizeof(*u)); }
    return u;
}
static void px_unit_free_node(struct dmesh_proxy *px, struct px_unit *u) {
    while (u->pieces) {
        struct px_piece *p = u->pieces;
        u->pieces = p->next;
        px_piece_free(px, p);
    }
    u->next = px->unit_free; px->unit_free = u;
}
static struct px_batch *px_batch_alloc(struct px_engine *eng) {
    struct px_batch *b = eng->batch_free;
    if (b) { eng->batch_free = b->next; memset(b, 0, sizeof(*b)); }
    return b;
}
static void px_batch_free_node(struct px_engine *eng, struct px_batch *b) {
    b->next = eng->batch_free; eng->batch_free = b;
}

/* ====== custody ====== */

/* All of this arrival's bytes are accounted (egressed or dropped): return the
 * sender's TX slot (batched TX_ACK, original untranslated identity) and free. */
static void px_arrival_release(struct objects *objs, struct px_arrival *a) {
    batch_or_send_tx_ack(objs, find_pod_by_id(objs, a->ack_pod), a->ack_port, a->ack_seq);
    px_arrival_free(objs->proxy, a);
}

/* ====== conn table ====== */

static inline uint32_t px_conn_hash(int32_t pod, uint16_t port) {
    uint32_t k = ((uint32_t)(uint8_t)pod << 16) | port;
    return (k * 2654435761u) & (PX_CONN_HASH - 1u);
}

static struct px_conn *px_conn_find(struct dmesh_proxy *px, int32_t pod, uint16_t port) {
    struct px_conn *c = px->buckets[px_conn_hash(pod, port)];
    while (c && !(c->pub.src_pod == pod && c->pub.src_port == port))
        c = c->hnext;
    return c;
}

static struct px_conn *px_conn_get(struct dmesh_proxy *px, int32_t pod, uint16_t port,
                                   int is_reply, int create) {
    struct px_conn *c = px_conn_find(px, pod, port);
    if (c || !create)
        return c;
    c = (struct px_conn *)calloc(1, sizeof(*c));
    if (!c)
        return NULL;
    c->pub.src_pod = pod;
    c->pub.src_port = port;
    c->pub.is_reply = is_reply;
    c->pub.peer_pod = DMESH_POD_BLANK;
    c->pub.dst_service = DMESH_SVC_NONE;
    uint32_t h = px_conn_hash(pod, port);
    c->hnext = px->buckets[h];
    px->buckets[h] = c;
    return c;
}

static void px_drop_window(struct objects *objs, struct px_conn *c, const char *why);

/* The parsers (defined at the bottom). px_parse dispatches between them
 * per-connection, so they are referenced above their definitions. */
static int px_mock_passthru(struct objects *objs, dmesh_proxy_conn *conn,
                            const uint8_t *buf, uint32_t avail,
                            struct dmesh_route_seg *segs, int max, uint32_t *consumed);
static int px_mock_frame(struct objects *objs, dmesh_proxy_conn *conn,
                         const uint8_t *buf, uint32_t avail,
                         struct dmesh_route_seg *segs, int max, uint32_t *consumed);
/* Head-only L7 loop (defined near px_parse). Its own loop — not a linear
 * parser — because it ships bodies beyond the contiguous view via SG. */
static void px_parse_l7(struct objects *objs, struct px_conn *c);

/* A REQUEST stream frames iff its addressed service is designated frame
 * (DPUMESH_PROXY_FRAME_SVC), else falls back to the deploy default
 * (DPUMESH_PROXY=frame → frame; passthru/1 → passthru). */
static int px_service_frames(struct dmesh_proxy *px, int16_t svc) {
    if (svc >= 0 && svc < POD_ID_SPACE && px->svc_frame[svc])
        return 1;
    return px->default_frame;
}

/* Resolve a connection's parse path ONCE at first arrival (sets c fields):
 *   - replies always pass through (dst = the single conntrack peer, so
 *     segmentation cannot change the delivered byte stream);
 *   - a request to an L7 service (DPUMESH_PROXY_L7_SVC) → head-only L7 loop
 *     (route_fn NULL, is_l7=1, bounded head window);
 *   - a request to a frame service (or default=frame) → the frame demo;
 *   - otherwise → passthru (vanilla / shim). */
static void px_resolve_route(struct dmesh_proxy *px, struct px_conn *c,
                             int is_reply, int16_t svc) {
    c->is_l7 = 0;
    c->parse_win_max = px->seam_max;
    if (!is_reply && svc >= 0 && svc < POD_ID_SPACE && px->svc_l7[svc]) {
        c->is_l7 = 1;
        c->parse_win_max = PX_HEAD_MAX;       /* bounded head window — no whole-message seam */
        c->route_fn = NULL;                   /* uses px_parse_l7, not a linear parser */
        return;
    }
    if (is_reply)
        c->route_fn = px_mock_passthru;
    else if (px_service_frames(px, svc))
        c->route_fn = px_mock_frame;
    else
        c->route_fn = px_mock_passthru;
}

static void px_conn_del(struct objects *objs, struct px_conn *c) {
    struct dmesh_proxy *px = objs->proxy;
    if (c->parse_pos < c->stream_end)
        px_drop_window(objs, c, "conn teardown");
    /* remaining window arrivals are fully parsed; any with pending egress
     * bytes are released by batch retirement (they are self-contained). */
    while (c->whead) {
        struct px_arrival *a = c->whead;
        c->whead = a->next;
        a->in_window = 0;
        a->next = NULL;
        if (a->unfreed == 0)
            px_arrival_release(objs, a);
    }
    c->wtail = NULL;
    free(c->seam);
    struct px_conn **link = &px->buckets[px_conn_hash(c->pub.src_pod, c->pub.src_port)];
    while (*link && *link != c)
        link = &(*link)->hnext;
    if (*link)
        *link = c->hnext;
    free(c);
}

static void px_conn_del_key(struct objects *objs, int32_t pod, uint16_t port) {
    struct px_conn *c = px_conn_find(objs->proxy, pod, port);
    if (c)
        px_conn_del(objs, c);
}

/* ====== window: view / seam / advance ====== */

/* The parser's contiguous view at parse_pos: the seam if active, else the
 * remainder of the single staging extent containing the cursor (zero-copy). */
static void px_view(struct objects *objs, struct px_conn *c,
                    const uint8_t **buf, uint32_t *avail) {
    *buf = NULL; *avail = 0;
    if (c->parse_pos >= c->stream_end)
        return;
    if (c->seam_on) {
        uint64_t seam_end = c->seam_base + c->seam_len;
        if (c->parse_pos < seam_end) {
            *buf = c->seam + (uint32_t)(c->parse_pos - c->seam_base);
            *avail = (uint32_t)(seam_end - c->parse_pos);
            return;
        }
        c->seam_on = 0;
        c->seam_len = 0;
    }
    struct px_arrival *a = c->whead;
    while (a && a->stream_base + a->len <= c->parse_pos)
        a = a->next;
    if (!a)
        return;
    struct pod_state *p = &objs->pods[a->pod_idx];
    *buf = (const uint8_t *)p->dma_buffer + a->staging_off +
           (uint32_t)(c->parse_pos - a->stream_base);
    /* Per-conn contiguous staging (mirror of host TX): extend the view across
     * arrivals whose staging bytes PHYSICALLY abut in the SAME pod buffer, so a
     * message spanning arrivals is parsed with NO seam. Stops at a physical
     * discontinuity (the host TX byte-ring wrap) — the seam still bridges that. */
    uint64_t run_end  = a->stream_base + a->len;
    uint32_t phys_end = a->staging_off + a->len;
    for (struct px_arrival *n = a->next;
         n && n->pod_idx == a->pod_idx && n->staging_off == phys_end;
         n = n->next) {
        run_end  += n->len;
        phys_end += n->len;
    }
    *avail = (uint32_t)(run_end - c->parse_pos);
}

/* Head window for the L7 path: the same view as px_view but capped at
 * PX_HEAD_MAX. The parser reads only the message HEAD here; the body ships from
 * staging via SG and is never linearized. */
static void px_head_view(struct objects *objs, struct px_conn *c,
                         const uint8_t **buf, uint32_t *avail) {
    px_view(objs, c, buf, avail);
    if (*avail > PX_HEAD_MAX)
        *avail = PX_HEAD_MAX;
}

/* Copy stream bytes [from, to) out of the window's staging extents. */
static void px_copy_stream(struct objects *objs, struct px_conn *c,
                           uint8_t *dst, uint64_t from, uint64_t to) {
    struct px_arrival *a = c->whead;
    while (a && a->stream_base + a->len <= from)
        a = a->next;
    while (a && from < to) {
        uint64_t aend = a->stream_base + a->len;
        uint64_t take = (to < aend ? to : aend) - from;
        const uint8_t *src = (const uint8_t *)objs->pods[a->pod_idx].dma_buffer +
                             a->staging_off + (uint32_t)(from - a->stream_base);
        memcpy(dst, src, (size_t)take);
        dst += take;
        from += take;
        a = a->next;
    }
}

/* Grow the parser's contiguous view past the current extent boundary by
 * aligning the unconsumed tail (+ later bytes) into the seam buffer.
 * Returns 1 if the view got bigger, 0 if nothing more can be shown. */
static int px_seam_grow(struct objects *objs, struct px_conn *c) {
    struct dmesh_proxy *px = objs->proxy;
    uint64_t base, view_end;

    if (c->seam_on) {
        base = c->seam_base;
        view_end = c->seam_base + c->seam_len;
    } else {
        base = c->parse_pos;
        struct px_arrival *a = c->whead;
        while (a && a->stream_base + a->len <= c->parse_pos)
            a = a->next;
        view_end = a ? a->stream_base + a->len : c->parse_pos;
    }
    if (c->stream_end <= view_end)
        return 0;                              /* nothing beyond the view yet */

    /* Per-conn cap: seam_max for the linear (frame) path, PX_HEAD_MAX for L7
     * (the head window never grows into the body). */
    uint32_t win = c->parse_win_max ? c->parse_win_max : px->seam_max;
    uint64_t want_end = base + win;
    if (want_end > c->stream_end)
        want_end = c->stream_end;
    if (want_end <= view_end)
        return 0;                              /* window cap reached */

    uint32_t need = (uint32_t)(want_end - base);
    if (need > c->seam_cap) {
        uint32_t cap = c->seam_cap ? c->seam_cap : 8192;
        while (cap < need)
            cap *= 2;
        if (cap > px->seam_max)
            cap = px->seam_max;
        uint8_t *nb = (uint8_t *)realloc(c->seam, cap);
        if (!nb)
            return 0;                          /* OOM: behave as "can't grow" */
        c->seam = nb;
        c->seam_cap = cap;
    }
    uint32_t have = c->seam_on ? c->seam_len : 0;
    px_copy_stream(objs, c, c->seam + have, base + have, want_end);
    c->seam_base = base;
    c->seam_len = need;
    c->seam_on = 1;
    return 1;
}

/* Advance the window cursor by `consumed`. Consumed bytes NOT claimed by any
 * seg this round are DROPS — released from custody immediately. Fully-parsed
 * extents leave the window (custody continues via their pieces if in flight). */
static void px_advance(struct objects *objs, struct px_conn *c, uint32_t consumed) {
    uint64_t from = c->parse_pos, to = from + consumed;
    struct px_arrival *a = c->whead;
    while (a && a->stream_base < to) {
        uint64_t abeg = a->stream_base, aend = abeg + a->len;
        if (aend > from) {
            uint64_t cbeg = from > abeg ? from : abeg;
            uint64_t cend = to < aend ? to : aend;
            uint32_t covered = (uint32_t)(cend - cbeg);
            uint32_t dropped = covered > a->claimed_round ? covered - a->claimed_round : 0;
            a->claimed_round = 0;
            a->unfreed = a->unfreed > dropped ? a->unfreed - dropped : 0;
            objs->proxy->stat_drop_bytes += dropped;
        }
        a = a->next;
    }
    c->parse_pos = to;
    if (c->seam_on && c->parse_pos >= c->seam_base + c->seam_len) {
        c->seam_on = 0;
        c->seam_len = 0;
    }
    while (c->whead && c->whead->stream_base + c->whead->len <= c->parse_pos) {
        struct px_arrival *h = c->whead;
        c->whead = h->next;
        if (!c->whead)
            c->wtail = NULL;
        h->in_window = 0;
        h->next = NULL;
        if (h->unfreed == 0)
            px_arrival_release(objs, h);
    }
}

/* Drop every unparsed byte of the window (no segs). Sender slots come back
 * via the drop accounting in px_advance. */
static void px_drop_window(struct objects *objs, struct px_conn *c, const char *why) {
    uint32_t remaining = (uint32_t)(c->stream_end - c->parse_pos);
    if (remaining) {
        DOCA_LOG_WARN("proxy: dropping %u buffered bytes of conn (%d:%u): %s",
                      remaining, c->pub.src_pod, c->pub.src_port, why);
        px_advance(objs, c, remaining);
    }
}

static void px_poison(struct objects *objs, struct px_conn *c, const char *why) {
    DOCA_LOG_ERR("proxy: poisoning conn (%d:%u): %s", c->pub.src_pod, c->pub.src_port, why);
    px_drop_window(objs, c, why);
    c->dead = 1;
}

/* ====== lanes / units ====== */

static inline uint32_t px_unit_entries(const struct px_unit *u) {
    return u->total_len ? (u->total_len + PX_ENTRY_BYTES_MAX - 1) / PX_ENTRY_BYTES_MAX : 1;
}

static void px_lane_enqueue(struct dmesh_proxy *px, int pod_idx, int region, struct px_unit *u) {
    struct px_lane *ln = &px->lanes[pod_idx][region];
    u->next = NULL;
    if (px->n_eng > 1) {
        /* hand to the owning worker's per-lane inbox (spliced onto qhead in pump) */
        pthread_mutex_lock(&ln->inq_lock);
        if (ln->inq_tail) ln->inq_tail->next = u; else ln->inq_head = u;
        ln->inq_tail = u;
        pthread_mutex_unlock(&ln->inq_lock);
    } else {
        if (ln->qtail) ln->qtail->next = u; else ln->qhead = u;
        ln->qtail = u;
    }
    px->stat_units++;   /* ingest-only writer */
}

/* route_group of the arrival holding stream offset `off` (for the L4 default
 * route of a DEFERred request seg — parity with the legacy per-slot pin). */
static uint8_t px_rg_at(struct px_conn *c, uint64_t off) {
    struct px_arrival *a = c->whead;
    while (a && a->stream_base + a->len <= off)
        a = a->next;
    return (a && a->stream_base <= off) ? a->route_group : 0;
}

/* Queue one FIN/notify-only unit (0 length, 1 RX credit) to a lane. */
static void px_queue_fin_unit(struct objects *objs, struct px_conn *c,
                              struct pod_state *dst_pod,
                              uint16_t out_src_port, uint16_t out_dst_port) {
    struct dmesh_proxy *px = objs->proxy;
    struct px_unit *u = px_unit_alloc(px);
    if (!u) {
        DOCA_LOG_ERR("proxy: unit pool empty — FIN to pod %d port %u lost",
                     dst_pod->pod_id, out_dst_port);
        return;
    }
    struct pod_state *sp = find_pod_by_id(objs, c->pub.src_pod);
    u->src_pod_id = (int8_t)c->pub.src_pod;
    u->src_service = sp ? (int8_t)sp->service_id : (int8_t)DMESH_SVC_NONE;
    u->dst_service = (int8_t)c->pub.dst_service;
    u->src_port = out_src_port;
    u->dst_port = out_dst_port;
    u->seq = ++c->egress_seq;
    u->total_len = 0;
    u->dst_pod_idx = (int8_t)(dst_pod - objs->pods);
    int K = dst_pod->k_rings > 0 ? dst_pod->k_rings : 1;
    px_lane_enqueue(px, (int)(dst_pod - objs->pods), (int)(out_dst_port % (uint16_t)K), u);
}

/* Execute one seg: resolve its destination, build the unit's staging pieces,
 * claim custody, queue on the destination lane. */
static void px_ship_seg(struct objects *objs, struct px_conn *c,
                        const struct dmesh_route_seg *s) {
    struct dmesh_proxy *px = objs->proxy;
    struct dpu_conntrack *ct = objs->conntrack;
    uint64_t sbeg = c->parse_pos + s->off, send_ = sbeg + s->len;
    int32_t dst_pod;
    uint16_t out_src_port, out_dst_port;

    if (c->pub.is_reply) {
        /* dst comes from the conntrack table; the proxy only confirms. */
        dst_pod = c->pub.peer_pod;
        out_dst_port = c->pub.peer_port;
        out_src_port = c->pub.src_port;
        if (s->dst >= 0 && s->dst != dst_pod)
            DOCA_LOG_WARN("proxy: reply seg dst=%d overridden by conntrack (pod %d)",
                          s->dst, dst_pod);
    } else {
        dst_pod = s->dst;
        if (dst_pod == DMESH_SEG_DST_DEFER)
            dst_pod = dpu_route_l4(objs, c->pub.dst_service, px_rg_at(c, sbeg));
        if (dst_pod < 0) {
            DOCA_LOG_ERR("proxy: unroutable seg (svc=%d) — %u bytes dropped",
                         c->pub.dst_service, s->len);
            return;                            /* unclaimed → drop-accounted */
        }
    }

    struct pod_state *tp = find_pod_by_id(objs, dst_pod);
    if (!tp || !pod_data_ready(tp) || !tp->host_rx_mmap || !tp->host_rx_addr) {
        DOCA_LOG_ERR("proxy: dst pod %d not ready — %u bytes dropped", dst_pod, s->len);
        return;
    }

    if (!c->pub.is_reply) {
        uint16_t uP = dpu_upstream_find(ct, c->pub.src_pod, c->pub.src_port, dst_pod);
        if (uP == 0)
            uP = dpu_upstream_create(ct, c->pub.src_pod, c->pub.src_port, dst_pod);
        if (uP == 0) {
            DOCA_LOG_ERR("proxy: upstream space full (%d:%u -> pod %d) — seg dropped",
                         c->pub.src_pod, c->pub.src_port, dst_pod);
            return;
        }
        out_src_port = uP;
        out_dst_port = uP;
    }

    struct px_unit *u = px_unit_alloc(px);
    if (!u) {
        DOCA_LOG_ERR("proxy: unit pool empty — %u bytes dropped", s->len);
        return;
    }
    struct pod_state *sp = find_pod_by_id(objs, c->pub.src_pod);
    u->src_pod_id = (int8_t)c->pub.src_pod;
    u->src_service = sp ? (int8_t)sp->service_id : (int8_t)DMESH_SVC_NONE;
    u->dst_service = (int8_t)c->pub.dst_service;
    u->src_port = out_src_port;
    u->dst_port = out_dst_port;
    u->seq = ++c->egress_seq;
    u->total_len = s->len;
    u->dst_pod_idx = (int8_t)(tp - objs->pods);

    /* map the stream range onto staging extents (zero-copy SG sources) */
    struct px_arrival *a = c->whead;
    while (a && a->stream_base + a->len <= sbeg)
        a = a->next;
    uint64_t pos = sbeg;
    while (pos < send_ && a) {
        uint64_t aend = a->stream_base + a->len;
        uint64_t take_end = send_ < aend ? send_ : aend;
        struct px_piece *p = px_piece_alloc(px);
        if (!p) {
            DOCA_LOG_ERR("proxy: piece pool empty — %u bytes dropped", s->len);
            px_unit_free_node(px, u);
            return;                            /* nothing claimed yet */
        }
        p->arr = a;
        p->pod_idx = a->pod_idx;
        p->staging_off = a->staging_off + (uint32_t)(pos - a->stream_base);
        p->len = (uint32_t)(take_end - pos);
        p->next = NULL;
        if (u->pieces_tail)
            u->pieces_tail->next = p;
        else
            u->pieces = p;
        u->pieces_tail = p;
        u->npieces++;
        pos = take_end;
        if (pos >= aend)
            a = a->next;
    }
    if (pos < send_) {
        DOCA_LOG_ERR("proxy: seg maps past the window (bug) — dropped");
        px_unit_free_node(px, u);
        return;
    }
    for (struct px_piece *p = u->pieces; p; p = p->next)
        p->arr->claimed_round += p->len;

    int K = tp->k_rings > 0 ? tp->k_rings : 1;
    px_lane_enqueue(px, (int)(tp - objs->pods), (int)(out_dst_port % (uint16_t)K), u);
    px->stat_segs++;
}

static void px_exec_segs(struct objects *objs, struct px_conn *c,
                         const struct dmesh_route_seg *segs, int n, uint32_t consumed) {
    uint32_t prev_end = 0;
    for (int i = 0; i < n; i++) {
        const struct dmesh_route_seg *s = &segs[i];
        if (s->len == 0) {
            DOCA_LOG_WARN("proxy: 0-length seg ignored (0-length wire msg is the FIN)");
            continue;
        }
        if (s->off < prev_end || (uint64_t)s->off + s->len > consumed) {
            DOCA_LOG_ERR("proxy: seg out of contract (off=%u len=%u consumed=%u) — dropped",
                         s->off, s->len, consumed);
            continue;
        }
        prev_end = s->off + s->len;
        px_ship_seg(objs, c, s);
    }
}

/* ====== parse loop ====== */

static void px_parse(struct objects *objs, struct px_conn *c) {
    struct dmesh_proxy *px = objs->proxy;
    struct dmesh_route_seg segs[DMESH_PROXY_SEG_MAX];

    if (c->is_l7) {                            /* head-only path — its own loop */
        px_parse_l7(objs, c);
        return;
    }

    while (c->parse_pos < c->stream_end) {
        if (c->pub.is_reply) {
            /* the reply's dst is predetermined — L4 conntrack lookup */
            struct dpu_upstream *u = (c->pub.src_port >= DMESH_UPORT_BASE)
                ? &objs->conntrack->upstream[c->pub.src_port] : NULL;
            if (!u || !u->in_use) {
                px_drop_window(objs, c, "stale upstream (client closed)");
                return;
            }
            c->pub.peer_pod = u->client_pod;
            c->pub.peer_port = u->client_port;
        }
        const uint8_t *buf;
        uint32_t avail, consumed = 0;
        px_view(objs, c, &buf, &avail);
        if (avail == 0)
            break;
        /* Per-connection parser/router, resolved at first arrival. */
        dmesh_proxy_route_fn fn = c->route_fn ? c->route_fn : px_mock_passthru;
        int n = fn(objs, &c->pub, buf, avail, segs, DMESH_PROXY_SEG_MAX, &consumed);
        if (n < 0) {
            px_poison(objs, c, "proxy_route returned error");
            return;
        }
        if (consumed > avail)
            consumed = avail;
        if (consumed > 0) {
            if (n > 0)
                px_exec_segs(objs, c, segs, n, consumed);
            px_advance(objs, c, consumed);
            continue;
        }
        /* consumed == 0 → the parser needs a bigger contiguous view or more
         * input. Align the tail into the seam; if the view is already maximal
         * and capped while more bytes exist, the parser can never proceed. */
        if (!px_seam_grow(objs, c)) {
            if (c->seam_on && c->seam_len >= c->parse_win_max &&
                c->stream_end > c->seam_base + c->parse_win_max)
                px_poison(objs, c, "parser stalled at seam cap");
            break;
        }
    }
}

/* Head-only L7 loop: parse each message's HEAD in a bounded window, then stream
 * its bytes to the resolved backend via SG — the body is never linearized. */
static void px_parse_l7(struct objects *objs, struct px_conn *c) {
    struct dmesh_l7_ctx ctx;
    ctx.service     = c->pub.dst_service;
    ctx.client_pod  = c->pub.src_pod;
    ctx.client_port = c->pub.src_port;

    while (c->parse_pos < c->stream_end) {
        if (c->msg_remaining == 0) {           /* at a message boundary → parse its head */
            /* Re-anchor the head window at THIS message: a seam grown for a
             * previous head is capped relative to its own base, so reusing it
             * for a later message could stall short of PX_HEAD_MAX. Dropping it
             * lets px_head_view re-anchor (zero-copy tail, or a fresh bounded
             * seam) at parse_pos. */
            if (c->seam_on && c->parse_pos > c->seam_base) {
                c->seam_on = 0;
                c->seam_len = 0;
            }
            const uint8_t *buf;
            uint32_t avail;
            px_head_view(objs, c, &buf, &avail);   /* <= PX_HEAD_MAX contiguous */
            if (avail == 0)
                break;
            int32_t target = c->pub.dst_service;
            int r = dmesh_l7_route(buf, avail, &ctx, &target);
            if (r < 0) {
                px_poison(objs, c, "l7 route error");
                return;
            }
            if (r == 0) {                      /* head not fully seen yet */
                if (avail >= PX_HEAD_MAX) {    /* head bigger than the window → poison */
                    px_poison(objs, c, "l7 head exceeds PX_HEAD_MAX");
                    return;
                }
                if (!px_seam_grow(objs, c))    /* assemble more head bytes (bounded), or wait */
                    break;
                continue;
            }
            c->msg_remaining = (uint32_t)r;    /* total message length (body need not be here) */
            /* Resolve the backend ONCE so every chunk of this message pins to it
             * (in order). A content-chosen service resolves per-message; the
             * default keeps the L4 route + route-affinity of the head arrival. */
            c->msg_dst = (target == c->pub.dst_service)
                         ? dpu_route_l4(objs, c->pub.dst_service, px_rg_at(c, c->parse_pos))
                         : dpu_route_l4(objs, (int16_t)target, 0);
        }

        /* Ship the next chunk of the in-progress message from staging via SG.
         * Bounded so one unit stays within the SG piece cap + host RX region. */
        uint64_t arrived = c->stream_end - c->parse_pos;
        if (arrived == 0)
            break;                             /* body incomplete → wait for more arrivals */
        uint32_t chunk = c->msg_remaining;
        if ((uint64_t)chunk > arrived)
            chunk = (uint32_t)arrived;
        if (chunk > PX_L7_UNIT_MAX)
            chunk = PX_L7_UNIT_MAX;

        struct dmesh_route_seg seg;
        seg.off = 0;                           /* px_ship_seg: sbeg = parse_pos + off */
        seg.len = chunk;
        seg.dst = c->msg_dst;                  /* concrete pod (<0 → drop-accounted inside) */
        px_ship_seg(objs, c, &seg);
        px_advance(objs, c, chunk);            /* claimed bytes drop nothing; frees spent arrivals */
        c->msg_remaining -= chunk;
    }
}

/* ====== FIN ====== */

static void px_try_fin(struct objects *objs, struct px_conn *c) {
    struct dpu_conntrack *ct = objs->conntrack;
    if (!c->fin_pending)
        return;
    /* FIN = no more input: an unconsumed tail is a truncated unit — drop it
     * (the parser could never complete it). */
    if (c->parse_pos < c->stream_end)
        px_drop_window(objs, c, "FIN with unconsumed tail");

    if (!c->pub.is_reply) {
        /* client FIN: fan-out teardown of every upstream this conn opened —
         * each 0-len unit rides ITS lane, i.e. BEHIND that upstream's data. */
        for (int i = 0; i < objs->num_pods; i++) {
            int32_t b = objs->pods[i].pod_id;
            uint16_t uP = dpu_upstream_find(ct, c->pub.src_pod, c->pub.src_port, b);
            if (uP == 0)
                continue;
            struct pod_state *B = find_pod_by_id(objs, b);
            if (B && pod_data_ready(B) && B->host_rx_addr)
                px_queue_fin_unit(objs, c, B, uP, uP);
            dpu_upstream_free(ct, uP);
            px_conn_del_key(objs, b, uP);      /* reply-stream state of this upstream */
        }
    } else {
        struct dpu_upstream *u = (c->pub.src_port >= DMESH_UPORT_BASE)
            ? &ct->upstream[c->pub.src_port] : NULL;
        if (u && u->in_use) {
            struct pod_state *cp = find_pod_by_id(objs, u->client_pod);
            if (cp && pod_data_ready(cp) && cp->host_rx_addr)
                px_queue_fin_unit(objs, c, cp, c->pub.src_port, u->client_port);
            /* upstream itself is freed by the CLIENT's FIN fan-out (legacy parity) */
        }
    }
    batch_or_send_tx_ack(objs, find_pod_by_id(objs, c->fin_ack_pod),
                         c->fin_ack_port, c->fin_ack_seq);
    px_conn_del(objs, c);
}

/* ====== ingest (forward completion → per-conn input window) ====== */

int px_ingest_forward(struct objects *objs, void *ventry) {
    struct dmesh_proxy *px = objs->proxy;
    dpu_comp_entry_t *e = (dpu_comp_entry_t *)ventry;

    struct pod_state *fwd = (e->pod_idx >= 0 && e->pod_idx < objs->num_pods)
        ? &objs->pods[e->pod_idx] : NULL;
    if (!fwd || !pod_data_ready(fwd) || !fwd->dma_buffer) {
        DOCA_LOG_ERR("proxy ingest: invalid pod_idx=%d seq=%u", e->pod_idx, e->seq);
        batch_or_send_tx_ack(objs, find_pod_by_id(objs, e->src_pod_id), e->src_port, e->seq);
        return -1;
    }

    int is_reply = (e->dst_pod_id != DMESH_POD_BLANK);
    struct px_conn *c = px_conn_get(px, e->src_pod_id, e->src_port, is_reply, 1);
    if (!c)
        return 0;                              /* alloc pressure — retry */
    if (c->pub.is_reply != is_reply) {
        /* a port number cannot flip roles (client < UPORT_BASE <= uP) */
        DOCA_LOG_ERR("proxy ingest: conn (%d:%u) role flip (reply=%d->%d)",
                     e->src_pod_id, e->src_port, c->pub.is_reply, is_reply);
        c->pub.is_reply = is_reply;
    }
    if (!c->dst_service_set) {
        c->pub.dst_service = e->dst_service;
        c->dst_service_set = 1;
        /* Resolve this connection's parse path once (L7 / frame / passthru). */
        px_resolve_route(px, c, is_reply, e->dst_service);
    }

    if (e->length == 0) {                      /* FIN */
        c->fin_pending = 1;
        c->fin_ack_pod = e->src_pod_id;
        c->fin_ack_port = e->src_port;
        c->fin_ack_seq = e->seq;
        px_try_fin(objs, c);                   /* window is drained per-arrival */
        return 1;
    }

    if (c->dead) {                             /* poisoned stream: drop + ack */
        batch_or_send_tx_ack(objs, find_pod_by_id(objs, e->src_pod_id), e->src_port, e->seq);
        px->stat_drop_bytes += e->length;
        return -1;
    }

    struct px_arrival *a = px_arrival_alloc(px);
    if (!a)
        return 0;                              /* pool full — retry (backpressure) */
    a->stream_base = c->stream_end;
    a->pod_idx = e->pod_idx;
    a->staging_off = e->buf_offset;
    a->len = e->length;
    a->unfreed = e->length;
    a->claimed_round = 0;
    a->ack_pod = e->src_pod_id;
    a->ack_port = e->src_port;
    a->ack_seq = e->seq;
    a->route_group = e->route_group;
    a->in_window = 1;
    a->next = NULL;
    if (c->wtail)
        c->wtail->next = a;
    else
        c->whead = a;
    c->wtail = a;
    c->stream_end += a->len;
    px->stat_msgs++;

    px_parse(objs, c);
    /* px_parse may have poisoned + FIN may already be latched */
    if (c->fin_pending)
        px_try_fin(objs, c);
    return 1;
}

/* ====== egress: SG-DMA submit / completion / retirement ====== */

static void px_dma_done_cb(struct doca_dma_task_memcpy *t, union doca_data tud,
                           union doca_data cud) {
    struct px_engine *eng = (struct px_engine *)cud.ptr;
    struct objects *objs = eng->objs;
    struct px_op *op = (struct px_op *)tud.ptr;
    doca_task_free(doca_dma_task_memcpy_as_task(t));
    eng->dma_tasks_inflight--;
    if (op->kind == 1) {                       /* credit refresh landed */
        struct px_lane *ln = &objs->proxy->lanes[op->pod_idx][op->region];
        ln->cached_freed = *(volatile uint64_t *)
            (objs->proxy->scratch + PX_SCRATCH_OFF(op->pod_idx, op->region));
        ln->refresh_inflight = 0;
        if (op->src_buf) { doca_buf_dec_refcount(op->src_buf, NULL); op->src_buf = NULL; }
        if (op->dst_buf) { doca_buf_dec_refcount(op->dst_buf, NULL); op->dst_buf = NULL; }
        return;
    }
    struct px_batch *b = op->batch;
    if (b->src_head) { doca_buf_dec_refcount(b->src_head, NULL); b->src_head = NULL; }
    if (b->dst_buf)  { doca_buf_dec_refcount(b->dst_buf, NULL);  b->dst_buf = NULL; }
    b->done = 1;                               /* retired in px_drain (in order) */
}

static void px_dma_err_cb(struct doca_dma_task_memcpy *t, union doca_data tud,
                          union doca_data cud) {
    struct px_engine *eng = (struct px_engine *)cud.ptr;
    struct objects *objs = eng->objs;
    struct px_op *op = (struct px_op *)tud.ptr;
    doca_error_t st = doca_task_get_status(doca_dma_task_memcpy_as_task(t));
    doca_task_free(doca_dma_task_memcpy_as_task(t));
    eng->dma_tasks_inflight--;
    if (op->kind == 1) {
        DOCA_LOG_ERR("proxy: credit refresh DMA failed: %s", doca_error_get_descr(st));
        objs->proxy->lanes[op->pod_idx][op->region].refresh_inflight = 0;
        if (op->src_buf) { doca_buf_dec_refcount(op->src_buf, NULL); op->src_buf = NULL; }
        if (op->dst_buf) { doca_buf_dec_refcount(op->dst_buf, NULL); op->dst_buf = NULL; }
        return;
    }
    struct px_batch *b = op->batch;
    DOCA_LOG_ERR("proxy: SG-DMA batch failed (pod slot %d region %d, %u bytes): %s",
                 b->pod_idx, b->region, b->bytes, doca_error_get_descr(st));
    if (b->src_head) { doca_buf_dec_refcount(b->src_head, NULL); b->src_head = NULL; }
    if (b->dst_buf)  { doca_buf_dec_refcount(b->dst_buf, NULL);  b->dst_buf = NULL; }
    b->error = 1;
    b->done = 1;
}

/* Kick a lazy DMA read of the host's freed counter for lane (pod, region).
 * Same counter the DPA reverse admission used: the extra slot past forward
 * ring `region`'s descs, bumped by the host per freed landing. */
static void px_lane_refresh_credit(struct objects *objs, struct px_engine *eng,
                                   int pod_idx, struct pod_state *pod, int region,
                                   struct px_lane *ln) {
    struct dmesh_proxy *px = objs->proxy;
    if (ln->refresh_inflight || eng->dma_tasks_inflight >= PX_DMA_TASKS)
        return;
    if (!pod->ring_mmaps[region] || !pod->ring_host_addrs[region]) {
        if (!ln->warned_no_credit_addr) {
            DOCA_LOG_ERR("proxy: pod %d ring %d has no host credit address — lane stalled",
                         pod->pod_id, region);
            ln->warned_no_credit_addr = 1;
        }
        return;
    }
    uint8_t *host_credit = (uint8_t *)pod->ring_host_addrs[region] +
                           (size_t)DMA_RING_SIZE * sizeof(struct dma_desc);
    uint8_t *cell = px->scratch + PX_SCRATCH_OFF(pod_idx, region);
    struct px_op *op = &px->refresh_ops[pod_idx][region];
    struct doca_buf *src = NULL, *dst = NULL;
    doca_error_t ret;

    ret = doca_buf_inventory_buf_get_by_addr(eng->inv, pod->ring_mmaps[region],
                                             host_credit, sizeof(uint64_t), &src);
    if (ret != DOCA_SUCCESS)
        return;
    if (doca_buf_set_data(src, host_credit, sizeof(uint64_t)) != DOCA_SUCCESS)
        goto fail;
    ret = doca_buf_inventory_buf_get_by_addr(eng->inv, px->scratch_mmap,
                                             cell, sizeof(uint64_t), &dst);
    if (ret != DOCA_SUCCESS)
        goto fail;

    op->kind = 1;
    op->pod_idx = pod_idx;
    op->region = region;
    op->src_buf = src;
    op->dst_buf = dst;
    union doca_data ud = { .ptr = op };
    struct doca_dma_task_memcpy *t = NULL;
    ret = doca_dma_task_memcpy_alloc_init(eng->dma, src, dst, ud, &t);
    if (ret != DOCA_SUCCESS)
        goto fail;
    if (doca_task_try_submit(doca_dma_task_memcpy_as_task(t)) != DOCA_SUCCESS) {
        doca_task_free(doca_dma_task_memcpy_as_task(t));
        goto fail;
    }
    ln->refresh_inflight = 1;
    eng->dma_tasks_inflight++;
    return;
fail:
    if (src) doca_buf_dec_refcount(src, NULL);
    if (dst) doca_buf_dec_refcount(dst, NULL);
    op->src_buf = op->dst_buf = NULL;
}

/* Append one REV_DONE entry into the destination pod's existing batch
 * accumulator (flushed on full here / on the main loop's idle flush).
 * Returns 0 on send-pool backpressure (caller retains + retries). */
static int px_emit_rev_entry(struct objects *objs, struct pod_state *pod,
                             const struct dmesh_rev_done_entry *e) {
    if (pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        flush_rev_done_batch(objs, pod);
    if (pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        return 0;
    pod->rev_done_batch[pod->rev_done_batch_n++] = *e;
    if (pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        flush_rev_done_batch(objs, pod);
    return 1;
}

/* Retire completed batches of one lane, strictly in submission order:
 * emit each unit's REV_DONE entries (<=8KB chunks), then release the custody
 * its pieces hold. Resumable on comch send-pool backpressure. */
static int px_lane_emit(struct objects *objs, struct px_engine *eng,
                        struct pod_state *pod, struct px_lane *ln) {
    struct dmesh_proxy *px = objs->proxy;
    int did = 0;

    while (ln->fhead && ln->fhead->done) {
        struct px_batch *b = ln->fhead;
        while (b->units) {
            struct px_unit *u = b->units;
            if (!b->error) {
                struct dmesh_rev_done_entry e;
                memset(&e, 0, sizeof(e));
                e.src_pod_id = u->src_pod_id;
                e.src_service = u->src_service;
                e.dst_service = u->dst_service;
                e.src_port = u->src_port;
                e.dst_port = u->dst_port;
                e.seq = u->seq;
                if (u->total_len == 0) {       /* FIN / notify-only */
                    if (!u->emit_fin_done) {
                        e.length = 0;
                        e.pos = u->landing_pos;
                        if (!px_emit_rev_entry(objs, pod, &e))
                            return did;        /* backpressure — resume later */
                        u->emit_fin_done = 1;
                        did = 1;
                    }
                } else {
                    while (u->emit_off < u->total_len) {
                        uint32_t elen = u->total_len - u->emit_off;
                        if (elen > PX_ENTRY_BYTES_MAX)
                            elen = PX_ENTRY_BYTES_MAX;
                        e.length = (uint16_t)elen;
                        e.pos = u->landing_pos + u->emit_off;
                        if (!px_emit_rev_entry(objs, pod, &e))
                            return did;
                        u->emit_off += elen;
                        did = 1;
                    }
                }
            }
            /* custody: the SG op has read (or abandoned) these staging bytes */
            for (struct px_piece *p = u->pieces; p; p = p->next) {
                struct px_arrival *a = p->arr;
                a->unfreed = a->unfreed > p->len ? a->unfreed - p->len : 0;
                if (a->unfreed == 0 && !a->in_window)
                    px_arrival_release(objs, a);
            }
            b->units = u->next;
            px_unit_free_node(px, u);
            did = 1;
        }
        if (b->error)                          /* host never frees these landings */
            ln->sent_entries -= b->entries;
        ln->fhead = b->next;
        if (!ln->fhead)
            ln->ftail = NULL;
        px_batch_free_node(eng, b);
    }
    return did;
}

static int px_lane_submit(struct objects *objs, struct px_engine *eng, int pod_idx,
                          struct pod_state *pod, int region, struct px_lane *ln);

/* ===== multi-thread egress: worker pump + ingest done-queue drain =====
 * n_eng>=2 only. The worker owns lanes' cursor/credit/qhead/fhead + its own
 * doca dma/pe/inv/batch pool; it retires completed batches into the engine
 * done-queue (units, not batches — batch pool stays worker-local). The ingest
 * (main) thread drains the done-queue for REV_DONE + custody + unit/piece free,
 * so unit/piece/arrival pools + comch stay INGEST-only (no locks). */

/* worker: retire completed batches of one lane into the engine done-queue. */
static void px_lane_retire(struct px_engine *eng, struct px_lane *ln) {
    struct px_unit *head = NULL, *tail = NULL;
    while (ln->fhead && ln->fhead->done) {
        struct px_batch *b = ln->fhead;
        if (b->error)
            ln->sent_entries -= b->entries;   /* host never freed these landings */
        for (struct px_unit *u = b->units; u; ) {
            struct px_unit *nx = u->next;
            u->err = (uint8_t)b->error;
            u->next = NULL;
            if (tail) tail->next = u; else head = u;
            tail = u;
            u = nx;
        }
        b->units = NULL;
        ln->fhead = b->next;
        if (!ln->fhead) ln->ftail = NULL;
        px_batch_free_node(eng, b);
    }
    if (head) {
        pthread_mutex_lock(&eng->done_lock);
        if (eng->done_tail) eng->done_tail->next = head; else eng->done_head = head;
        eng->done_tail = tail;
        pthread_mutex_unlock(&eng->done_lock);
    }
}

/* ingest: drain one engine's done-queue → REV_DONE + custody + free. Resumable
 * on comch send-pool backpressure (units stay on the engine's ingest-local
 * emit list). Single-consumer (main thread). */
static int px_engine_emit(struct objects *objs, struct px_engine *eng) {
    struct dmesh_proxy *px = objs->proxy;
    /* pull the worker's completed units into the ingest-local drain list */
    pthread_mutex_lock(&eng->done_lock);
    if (eng->done_head) {
        if (eng->emit_tail) eng->emit_tail->next = eng->done_head;
        else eng->emit_head = eng->done_head;
        eng->emit_tail = eng->done_tail;
        eng->done_head = eng->done_tail = NULL;
    }
    pthread_mutex_unlock(&eng->done_lock);

    int did = 0;
    while (eng->emit_head) {
        struct px_unit *u = eng->emit_head;
        struct pod_state *pod = &objs->pods[(int)u->dst_pod_idx];
        if (!u->err) {
            struct dmesh_rev_done_entry e;
            memset(&e, 0, sizeof(e));
            e.src_pod_id = u->src_pod_id;
            e.src_service = u->src_service;
            e.dst_service = u->dst_service;
            e.src_port = u->src_port;
            e.dst_port = u->dst_port;
            e.seq = u->seq;
            if (u->total_len == 0) {           /* FIN / notify-only */
                if (!u->emit_fin_done) {
                    e.length = 0;
                    e.pos = u->landing_pos;
                    if (!px_emit_rev_entry(objs, pod, &e))
                        return did;             /* backpressure — resume later */
                    u->emit_fin_done = 1;
                    did = 1;
                }
            } else {
                while (u->emit_off < u->total_len) {
                    uint32_t elen = u->total_len - u->emit_off;
                    if (elen > PX_ENTRY_BYTES_MAX)
                        elen = PX_ENTRY_BYTES_MAX;
                    e.length = (uint16_t)elen;
                    e.pos = u->landing_pos + u->emit_off;
                    if (!px_emit_rev_entry(objs, pod, &e))
                        return did;
                    u->emit_off += elen;
                    did = 1;
                }
            }
        }
        /* custody: the SG op has read (or abandoned) these staging bytes */
        for (struct px_piece *p = u->pieces; p; p = p->next) {
            struct px_arrival *a = p->arr;
            a->unfreed = a->unfreed > p->len ? a->unfreed - p->len : 0;
            if (a->unfreed == 0 && !a->in_window)
                px_arrival_release(objs, a);
        }
        eng->emit_head = u->next;
        if (!eng->emit_head) eng->emit_tail = NULL;
        px_unit_free_node(px, u);
        did = 1;
    }
    return did;
}

/* worker: one pass over this engine's owned lanes (splice inbox→qhead, submit,
 * retire). Owns lanes where pod_idx % n_eng == eng->id. */
static int px_engine_pump(struct objects *objs, struct px_engine *eng) {
    struct dmesh_proxy *px = objs->proxy;
    int progressed = 0;
    int npods = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
    for (int i = eng->id; i < npods; i += px->n_eng) {
        struct pod_state *pod = &objs->pods[i];
        int K = pod->k_rings > 0 ? pod->k_rings : 1;
        for (int r = 0; r < K; r++) {
            struct px_lane *ln = &px->lanes[i][r];
            /* splice the ingest inbox onto the worker-local qhead (O(1)) */
            if (ln->inq_head) {
                pthread_mutex_lock(&ln->inq_lock);
                struct px_unit *ih = ln->inq_head, *it = ln->inq_tail;
                ln->inq_head = ln->inq_tail = NULL;
                pthread_mutex_unlock(&ln->inq_lock);
                if (ih) {
                    if (ln->qtail) ln->qtail->next = ih; else ln->qhead = ih;
                    ln->qtail = it;
                    progressed = 1;
                }
            }
            if (ln->fhead) px_lane_retire(eng, ln);
            if (ln->qhead) progressed |= px_lane_submit(objs, eng, i, pod, r, ln);
        }
    }
    return progressed;
}

/* egress worker thread body (n_eng>=2). Busy-polls its own PE + lanes; a short
 * yield when idle keeps it off a hot spin while still low-latency under load. */
static void *px_worker_main(void *arg) {
    struct px_engine *eng = (struct px_engine *)arg;
    struct objects *objs = eng->objs;
    unsigned idle = 0;
    while (!eng->stop) {
        int p = doca_pe_progress(eng->pe) ? 1 : 0;
        p |= px_engine_pump(objs, eng);
        if (p) { idle = 0; continue; }
        /* Idle: spin-yield briefly, then back off to a short sleep. An active
         * worker always makes progress and never reaches the sleep; only a
         * worker with no assigned pods (n_eng > active dst pods) or a real lull
         * sleeps — so an over-provisioned n_eng degrades gracefully instead of
         * oversubscribing the ARM cores. */
        if (++idle < 4096) sched_yield();
        else { struct timespec ts = { 0, 50000 }; nanosleep(&ts, NULL); }  /* 50us */
    }
    /* drain remaining completions so no doca task/buf leaks at shutdown */
    for (int n = 0; n < 100000 && eng->dma_tasks_inflight > 0; n++)
        doca_pe_progress(eng->pe);
    return NULL;
}

/* Submit queued units of one lane as SG batches while credits, region space,
 * pieces and task slots allow. */
static int px_lane_submit(struct objects *objs, struct px_engine *eng, int pod_idx,
                          struct pod_state *pod, int region, struct px_lane *ln) {
    struct dmesh_proxy *px = objs->proxy;
    int did = 0;

    if (!ln->qhead)
        return 0;
    if (!pod_data_ready(pod) || !pod->host_rx_addr || !pod->host_rx_mmap)
        return 0;

    int K = pod->k_rings > 0 ? pod->k_rings : 1;
    uint64_t region_size = pod->host_rx_buf_size / (uint64_t)K;
    uint32_t rq = pod->rq_depth / (uint32_t)K;
    if (region_size == 0 || rq == 0)
        return 0;

    while (ln->qhead && eng->dma_tasks_inflight < PX_DMA_TASKS) {
        /* admission: count credits, refreshed lazily via a DMA read */
        uint64_t inflight = ln->sent_entries - ln->cached_freed;
        uint32_t avail_entries = inflight < rq ? (uint32_t)(rq - inflight) : 0;
        uint32_t first_needed = px_unit_entries(ln->qhead);
        if (avail_entries < first_needed ||
            avail_entries < first_needed + PX_CREDIT_REFRESH_MARGIN)
            px_lane_refresh_credit(objs, eng, pod_idx, pod, region, ln);
        if (avail_entries < first_needed)
            break;                             /* wait for the refresh */

        if (ln->qhead->total_len > region_size) {
            DOCA_LOG_ERR("proxy: unit of %u bytes exceeds region (%llu) — dropped",
                         ln->qhead->total_len, (unsigned long long)region_size);
            struct px_unit *u = ln->qhead;
            ln->qhead = u->next;
            if (!ln->qhead) ln->qtail = NULL;
            for (struct px_piece *p = u->pieces; p; p = p->next) {
                p->arr->unfreed = p->arr->unfreed > p->len ? p->arr->unfreed - p->len : 0;
                if (p->arr->unfreed == 0 && !p->arr->in_window)
                    px_arrival_release(objs, p->arr);
            }
            px_unit_free_node(px, u);
            continue;
        }
        if (ln->cursor + ln->qhead->total_len > region_size)
            ln->cursor = 0;                    /* wrap (count-credit model) */

        struct px_batch *b = px_batch_alloc(eng);
        if (!b)
            break;

        /* take a FIFO prefix that fits pieces/credits/region-tail */
        struct px_unit *take_head = NULL, *take_tail = NULL;
        uint32_t pieces = 0, bytes = 0, entries = 0;
        int nunits = 0;
        while (ln->qhead) {
            struct px_unit *u = ln->qhead;
            uint32_t ue = px_unit_entries(u);
            if (nunits > 0 &&
                (pieces + (uint32_t)u->npieces > px->sg_pieces_max ||
                 entries + ue > avail_entries ||
                 ln->cursor + bytes + u->total_len > region_size))
                break;
            if (nunits == 0 && (uint32_t)u->npieces > px->sg_pieces_max) {
                /* single over-wide unit: cannot be one SG op — should not
                 * happen (a seg spans <= consumed <= seam_max/8K extents),
                 * but drop rather than wedge the lane. */
                DOCA_LOG_ERR("proxy: unit with %d pieces exceeds SG cap %u — dropped",
                             u->npieces, px->sg_pieces_max);
                ln->qhead = u->next;
                if (!ln->qhead) ln->qtail = NULL;
                for (struct px_piece *p = u->pieces; p; p = p->next) {
                    p->arr->unfreed = p->arr->unfreed > p->len ? p->arr->unfreed - p->len : 0;
                    if (p->arr->unfreed == 0 && !p->arr->in_window)
                        px_arrival_release(objs, p->arr);
                }
                px_unit_free_node(px, u);
                continue;
            }
            u->landing_pos = (uint32_t)((uint64_t)region * region_size + ln->cursor + bytes);
            ln->qhead = u->next;
            if (!ln->qhead)
                ln->qtail = NULL;
            u->next = NULL;
            if (take_tail)
                take_tail->next = u;
            else
                take_head = u;
            take_tail = u;
            nunits++;
            pieces += (uint32_t)u->npieces;
            bytes += u->total_len;
            entries += ue;
        }
        if (!take_head) {
            px_batch_free_node(eng, b);
            break;
        }

        b->units = take_head;
        b->nunits = nunits;
        b->pod_idx = pod_idx;
        b->region = region;
        b->entries = entries;
        b->bytes = bytes;
        b->op.kind = 0;
        b->op.batch = b;

        uint64_t dst_off = (uint64_t)region * region_size + ln->cursor;

        if (bytes == 0) {                      /* notify-only batch (FINs) */
            b->done = 1;
        } else {
            /* build the chained SG source + single contiguous dst, submit */
            struct doca_buf *src_head = NULL, *dst = NULL;
            doca_error_t ret = DOCA_SUCCESS;
            for (struct px_unit *u = take_head; u && ret == DOCA_SUCCESS; u = u->next) {
                for (struct px_piece *p = u->pieces; p; p = p->next) {
                    struct pod_state *sp = &objs->pods[p->pod_idx];
                    void *addr = (uint8_t *)sp->dma_buffer + p->staging_off;
                    struct doca_buf *buf = NULL;
                    ret = doca_buf_inventory_buf_get_by_addr(eng->inv, sp->local_mmap,
                                                             addr, p->len, &buf);
                    if (ret != DOCA_SUCCESS)
                        break;
                    ret = doca_buf_set_data(buf, addr, p->len);
                    if (ret != DOCA_SUCCESS) {
                        doca_buf_dec_refcount(buf, NULL);
                        break;
                    }
                    if (!src_head) {
                        src_head = buf;
                    } else {
                        ret = doca_buf_chain_list(src_head, buf);
                        if (ret != DOCA_SUCCESS) {
                            doca_buf_dec_refcount(buf, NULL);
                            break;
                        }
                    }
                }
            }
            if (ret == DOCA_SUCCESS)
                ret = doca_buf_inventory_buf_get_by_addr(eng->inv, pod->host_rx_mmap,
                        (uint8_t *)pod->host_rx_addr + dst_off, bytes, &dst);
            struct doca_dma_task_memcpy *t = NULL;
            if (ret == DOCA_SUCCESS) {
                union doca_data ud = { .ptr = &b->op };
                ret = doca_dma_task_memcpy_alloc_init(eng->dma, src_head, dst, ud, &t);
                if (ret == DOCA_SUCCESS) {
                    ret = doca_task_try_submit(doca_dma_task_memcpy_as_task(t));
                    if (ret != DOCA_SUCCESS)
                        doca_task_free(doca_dma_task_memcpy_as_task(t));
                }
            }
            if (ret != DOCA_SUCCESS) {
                /* transient (inventory/task pool): put the units back at the
                 * lane HEAD in order and retry next drain pass. */
                if (src_head) doca_buf_dec_refcount(src_head, NULL);
                if (dst) doca_buf_dec_refcount(dst, NULL);
                take_tail->next = ln->qhead;
                ln->qhead = take_head;
                if (!ln->qtail)
                    ln->qtail = take_tail;
                px_batch_free_node(eng, b);
                break;
            }
            b->src_head = src_head;
            b->dst_buf = dst;
            eng->dma_tasks_inflight++;
        }

        ln->cursor += bytes;
        ln->sent_entries += entries;
        b->next = NULL;
        if (ln->ftail)
            ln->ftail->next = b;
        else
            ln->fhead = b;
        ln->ftail = b;
        px->stat_batches++;
        did = 1;
    }
    return did;
}

int px_drain(struct objects *objs) {
    struct dmesh_proxy *px = objs->proxy;
    if (!px)
        return 0;
    int progressed = 0;
    if (px->n_eng > 1) {
        /* n_eng>=2: the egress workers do submit+SG-DMA+retire on their own
         * threads; the ingest (main) thread only drains each engine's done-queue
         * for REV_DONE + custody + pool free. */
        for (int e = 0; e < px->n_eng; e++)
            progressed |= px_engine_emit(objs, &px->engines[e]);
        return progressed;
    }
    /* single-thread default: engine 0 runs inline on the main thread. */
    struct px_engine *eng = &px->engines[0];
    int npods = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
    for (int i = 0; i < npods; i++) {
        struct pod_state *pod = &objs->pods[i];
        int K = pod->k_rings > 0 ? pod->k_rings : 1;
        for (int r = 0; r < K; r++) {
            struct px_lane *ln = &px->lanes[i][r];
            if (ln->fhead)
                progressed |= px_lane_emit(objs, eng, pod, ln);
            if (ln->qhead)
                progressed |= px_lane_submit(objs, eng, i, pod, r, ln);
        }
    }
    return progressed;
}

/* ====== mocks (plan.md §테스트 — the L7 is NOT built here) ====== */

/* pass-through: one seg per contiguous arrived run, destination deferred to the
 * L4 default route (service table + route-affinity pins). The parity/regression
 * mock — works with any byte stream, no app framing. */
static int px_mock_passthru(struct objects *objs, dmesh_proxy_conn *conn,
                            const uint8_t *buf, uint32_t avail,
                            struct dmesh_route_seg *segs, int max, uint32_t *consumed) {
    (void)objs; (void)buf; (void)max;
    segs[0].off = 0;
    segs[0].len = avail;
    segs[0].dst = conn->is_reply ? conn->peer_pod : DMESH_SEG_DST_DEFER;
    *consumed = avail;
    return 1;
}

/* frame: deterministic byte-stream framing the mock understands (plan.md):
 *   [u32 LE total_len (incl. 5B header)][u8 svc][payload]
 * Consumes only WHOLE frames (an incomplete tail is kept → exercises the
 * window/seam), routes each frame gateway-style by its svc byte via the
 * service table (svc 0xFF or unknown → defer to the client's addressed
 * service). Replies pass through frame-whole to the conntrack peer. */
static int px_mock_frame(struct objects *objs, dmesh_proxy_conn *conn,
                         const uint8_t *buf, uint32_t avail,
                         struct dmesh_route_seg *segs, int max, uint32_t *consumed) {
    uint32_t off = 0;
    int n = 0;
    while (n < max && avail - off >= PX_FRAME_HDR) {
        uint32_t flen;
        memcpy(&flen, buf + off, sizeof(flen));
        if (flen < PX_FRAME_HDR || flen > PX_FRAME_MAX)
            return -1;                         /* corrupt framing → poison */
        if (avail - off < flen)
            break;                             /* incomplete frame — wait/grow */
        int32_t dst;
        if (conn->is_reply) {
            dst = conn->peer_pod;
        } else {
            uint8_t svc = buf[off + 4];
            dst = DMESH_SEG_DST_DEFER;
            if (svc != 0xFF && svc < POD_ID_SPACE && objs->service_table[svc] >= 0)
                dst = objs->service_table[svc];
        }
        segs[n].off = off;
        segs[n].len = flen;
        segs[n].dst = dst;
        n++;
        off += flen;
    }
    *consumed = off;
    return n;
}

/* (The L7 path is NOT a linear dmesh_proxy_route_fn — it ships bodies beyond the
 * contiguous view via SG, so it lives in px_parse_l7 above, calling the author
 * hook dmesh_l7_route directly.) */

/* ====== init ====== */

/* Parse a csv of service ids from `env` into a POD_ID_SPACE flag table.
 * Returns the number of distinct in-range ids set. */
static int px_parse_svc_csv(const char *env, uint8_t *table) {
    int count = 0;
    if (!env || !*env)
        return 0;
    const char *p = env;
    while (*p) {
        char *end;
        long v = strtol(p, &end, 10);
        if (end == p) break;                       /* not a number → stop */
        if (v >= 0 && v < POD_ID_SPACE && !table[v]) {
            table[(int)v] = 1;
            count++;
        }
        p = end;
        while (*p == ',' || *p == ' ') p++;
    }
    return count;
}

int px_init(struct objects *objs) {
    const char *env = getenv("DPUMESH_PROXY");
    objs->proxy = NULL;

    /* UNIFIED DATA PLANE: the SG-DMA egress engine is ALWAYS on — it is the sole
     * DPU→host reverse path (the legacy DPA reverse machinery is gone). DPUMESH_PROXY
     * no longer toggles the engine; it only picks the deploy-default REQUEST parser:
     * unset / "passthru" / "1" → passthru (one seg per arrived message, dst = the §5
     * L4 route: metadata-only, the old legacy behaviour), "frame" → frame-all. */
    struct dmesh_proxy *px = (struct dmesh_proxy *)calloc(1, sizeof(*px));
    if (!px)
        return DOCA_ERROR_NO_MEMORY;

    /* Deploy default request mode (DPUMESH_PROXY=frame → frame-all, else passthru).
     * Per-service overrides: DPUMESH_PROXY_L7_SVC (author hook, checked first) and
     * DPUMESH_PROXY_FRAME_SVC (frame demo). */
    px->default_frame = (env && strcmp(env, "frame") == 0) ? 1 : 0;
    int n_l7_svc    = px_parse_svc_csv(getenv("DPUMESH_PROXY_L7_SVC"),    px->svc_l7);
    int n_frame_svc = px_parse_svc_csv(getenv("DPUMESH_PROXY_FRAME_SVC"), px->svc_frame);
    px->seam_max = PX_SEAM_MAX_DEFAULT;

    px->buckets = (struct px_conn **)calloc(PX_CONN_HASH, sizeof(*px->buckets));
    px->arr_mem = (struct px_arrival *)calloc(PX_ARRIVAL_POOL, sizeof(*px->arr_mem));
    px->piece_mem = (struct px_piece *)calloc(PX_PIECE_POOL, sizeof(*px->piece_mem));
    px->unit_mem = (struct px_unit *)calloc(PX_UNIT_POOL, sizeof(*px->unit_mem));
    if (!px->buckets || !px->arr_mem || !px->piece_mem || !px->unit_mem)
        goto oom;
    for (int i = PX_ARRIVAL_POOL - 1; i >= 0; i--) px_arrival_free(px, &px->arr_mem[i]);
    for (int i = PX_PIECE_POOL - 1; i >= 0; i--)   px_piece_free(px, &px->piece_mem[i]);
    for (int i = PX_UNIT_POOL - 1; i >= 0; i--)    { px->unit_mem[i].next = px->unit_free; px->unit_free = &px->unit_mem[i]; }
    /* per-lane ingest→worker inbox locks (used only when n_eng>1) */
    for (int i = 0; i < MAX_PODS; i++)
        for (int r = 0; r < MAX_EU_PER_POD; r++)
            pthread_mutex_init(&px->lanes[i][r].inq_lock, NULL);

    /* SG piece cap from the device (measured 64; clamp defensively) */
    px->sg_pieces_max = PX_SG_PIECES_MAX;
    {
        uint32_t cap = 0;
        if (doca_dma_cap_task_memcpy_get_max_buf_list_len(doca_dev_as_devinfo(objs->dev),
                                                          &cap) == DOCA_SUCCESS &&
            cap > 0 && cap < px->sg_pieces_max)
            px->sg_pieces_max = cap;
    }

    /* Egress worker count. DPUMESH_ARM_EGRESS_THREADS: 1 (default/unset) = the
     * proven inline path on the main thread; >=2 spawns that many egress worker
     * threads (each its own DOCA dma/PE/inventory/batch pool), lanes sharded by
     * dst pod_idx % n_eng. */
    int n_eng = 1;
    { const char *te = getenv("DPUMESH_ARM_EGRESS_THREADS");
      if (te && *te) { int v = atoi(te);
                       if (v >= 1 && v <= MAX_ARM_ENG) n_eng = v; } }
    px->n_eng = n_eng;

    /* Per-engine DOCA dma/pe/inventory/batch-pool. n_eng==1 shares the control
     * PE (objs->pe, progressed by the main loop); each threaded engine owns a PE
     * its worker busy-polls. The ctx user_data = the engine (completion cbs). */
    doca_error_t ret = DOCA_SUCCESS;
    for (int e = 0; e < n_eng; e++) {
        struct px_engine *eng = &px->engines[e];
        eng->objs = objs;
        eng->id = e;
        eng->threaded = (n_eng > 1);
        pthread_mutex_init(&eng->done_lock, NULL);
        eng->batch_mem = (struct px_batch *)calloc(PX_BATCH_POOL, sizeof(*eng->batch_mem));
        if (!eng->batch_mem) { ret = DOCA_ERROR_NO_MEMORY; goto fail; }
        for (int i = PX_BATCH_POOL - 1; i >= 0; i--) px_batch_free_node(eng, &eng->batch_mem[i]);

        ret = doca_dma_create(objs->dev, &eng->dma);
        if (ret != DOCA_SUCCESS) goto fail;
        eng->dma_ctx = doca_dma_as_ctx(eng->dma);
        ret = doca_dma_task_memcpy_set_conf(eng->dma, px_dma_done_cb, px_dma_err_cb, PX_DMA_TASKS);
        if (ret != DOCA_SUCCESS) goto fail;
        if (eng->threaded) {
            ret = doca_pe_create(&eng->pe);
            if (ret != DOCA_SUCCESS) goto fail;
        } else {
            eng->pe = objs->pe;   /* inline: main progresses the control PE */
        }
        ret = doca_pe_connect_ctx(eng->pe, eng->dma_ctx);
        if (ret != DOCA_SUCCESS) goto fail;
        { union doca_data ud = { .ptr = eng };
          ret = doca_ctx_set_user_data(eng->dma_ctx, ud);
          if (ret != DOCA_SUCCESS) goto fail; }
        ret = doca_ctx_start(eng->dma_ctx);
        if (ret != DOCA_SUCCESS) goto fail;
        ret = doca_buf_inventory_create((size_t)PX_DMA_TASKS * (px->sg_pieces_max + 1) + 128,
                                        &eng->inv);
        if (ret != DOCA_SUCCESS) goto fail;
        ret = doca_buf_inventory_start(eng->inv);
        if (ret != DOCA_SUCCESS) goto fail;
    }

    /* credit-read landing cells (shared mmap; each cell touched by one engine) */
    ret = alloc_buffer_and_set_mmap(&px->scratch_mmap, objs->dev,
                                    (void **)&px->scratch,
                                    (size_t)MAX_PODS * MAX_EU_PER_POD * PX_SCRATCH_CELL,
                                    DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
    if (ret != DOCA_SUCCESS) goto fail;

    objs->proxy = px;   /* publish before spawning so workers see a ready proxy */

    if (n_eng > 1) {
        for (int e = 0; e < n_eng; e++) {
            if (pthread_create(&px->engines[e].thread, NULL, px_worker_main,
                               &px->engines[e]) != 0) {
                DOCA_LOG_ERR("proxy: failed to spawn egress worker %d", e);
                objs->proxy = NULL;
                ret = DOCA_ERROR_OPERATING_SYSTEM;
                goto fail;
            }
        }
    }
    DOCA_LOG_WARN("DPU PROXY MODE ON (SG-DMA egress, egress-threads=%d; request-default=%s, "
                  "l7-services=%d, frame-services=%d, replies=passthru, seam_max=%u, sg_pieces=%u)",
                  n_eng, px->default_frame ? "frame" : "passthru", n_l7_svc, n_frame_svc,
                  px->seam_max, px->sg_pieces_max);
    return DOCA_SUCCESS;

oom:
    ret = DOCA_ERROR_NO_MEMORY;
fail:
    DOCA_LOG_ERR("proxy init failed: %s", doca_error_get_descr(ret));
    free(px->buckets); free(px->arr_mem); free(px->piece_mem);
    free(px->unit_mem);
    for (int e = 0; e < MAX_ARM_ENG; e++) free(px->engines[e].batch_mem);
    free(px);
    return ret;
}
