/*
 * dpm.h — socket/epoll-style façade over the DPUmesh C API. Header-only.
 *
 *   socket()/bind()/listen()  ->  dmesh_create_channel()      (registers a service)
 *   accept()                  ->  dmesh_accept()              (one new conn)
 *   connect()                 ->  dmesh_connect(svc)          (one new conn)
 *   read()/recv()             ->  dmesh_read()                (next inbound message)
 *   write()/send()            ->  dmesh_write() + dmesh_flush()  (buffer, then ship)
 *   close()                   ->  dmesh_close()
 *   epoll on ONE channel fd   ->  native epoll on dmesh_event_fd(s)   (the only fd)
 *   epoll_wait readiness list ->  dmesh_accept() (new conns) + dmesh_next_ready() (data)
 *
 * CONNECTION-ORIENTED, FULL-DUPLEX (TCP-like), NOT request/response:
 *   - A `dmesh_conn_t` is a persistent connection, alive until close. The transport
 *     delivers ALL inbound messages on a conn to the app — it does NO request↔
 *     response matching (that's the app's job, if it wants RPC semantics).
 *   - MODEL B (the DPU owns every connection): a CLIENT addresses only a SERVICE.
 *     `dmesh_connect(svc)` is local (no round-trip, no pod chosen). Every
 *     `write+flush` ships with dst_pod=BLANK and the DPU load-balances it PER
 *     MESSAGE to a backend, owning the upstream. The client NEVER learns or pins a
 *     pod — it keeps addressing the service. A backend `dmesh_accept()`s a conn the
 *     DPU created to it (learning its DPU-facing peer), replies to it, and the DPU
 *     maps the reply back to the client.
 *   - Ordering: in send order only on a connection to ONE backend. Under per-message
 *     LB a single conn's replies can arrive OUT OF ORDER, so if you keep several
 *     requests outstanding, carry a req-id in the body and match on it (never on
 *     arrival order). Single-outstanding (write→flush→read one at a time) is
 *     structurally paired and needs no correlation.
 *   - Teardown: dmesh_close() sends a FIN (a zero-length message on the same conn),
 *     which rides behind all prior data and makes the peer's read() return 0 (EOF);
 *     the peer then closes, reclaiming its slot. read()==0 ⇒ peer closed ⇒ close.
 *     (A user 0-length send is a no-op; zero length on the wire is the FIN alone.)
 *     Concurrent close is safe: a FIN landing on an already-freed conn is dropped.
 *   - Each message is one whole <= slot_size (8 KB) body, delivered atomically.
 *   - NON-BLOCKING: read/accept/next_ready return EAGAIN/NULL when nothing is ready;
 *     sleep on ONE channel fd (dmesh_event_fd). On wake, drain dmesh_accept() (new
 *     conns) then dmesh_next_ready() (conns with inbound — the PE names them, so no
 *     scan, no per-conn fd). SEND IS EXPLICIT: write buffers, flush ships.
 */
#ifndef DPM_H
#define DPM_H

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>

#include "dpumesh.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ===== Handles ===== */

/* Endpoint — one per process. Wraps one DOCA device context + this node's service. */
typedef struct dmesh_channel {
    dpumesh_ctx_t *ctx;
    int            pod_id;
    int            slot_size;   /* cached max body size */
    uint32_t       next_group;  /* GLOBAL rolling route-affinity id source (atomic across
                                 * all conns → 1..255): concurrent large messages get
                                 * DISTINCT groups. A per-conn counter would collide —
                                 * every conn's first large message would pick id 1. */
} dmesh_channel_t;

/* Connection — a persistent full-duplex link to one peer. */
typedef struct dmesh_conn {
    dmesh_channel_t *ep;
    void     *user_data;       /* APP-OWNED (like epoll's data.ptr): you set it, and
                                * dmesh_next_ready hands this conn back so you read it.
                                * The transport never touches it. */
    int       role;            /* DMESH_ROLE_CLIENT (connect) | DMESH_ROLE_SERVER (accept) */

    /* addressing */
    uint16_t  local_port;      /* my port (this conn's id) */
    int16_t   dst_service;     /* peer service (the service connected to / caller's) */
    int16_t   remote_pod;      /* SERVER: the DPU-facing peer pod (learned at accept).
                                * CLIENT: always DMESH_POD_BLANK (Model B never pins). */
    uint16_t  remote_port;     /* SERVER: the peer uP (learned at accept); CLIENT: 0. */
    uint8_t   peer_closed;      /* received the peer's FIN → reads return EOF (sticky) */
    uint16_t  seq;             /* per-conn OUTBOUND message counter */

    /* inbound (the message currently being read out; rx_slot>=0 ⇒ one is loaded) */
    int            rx_slot;    /* landing byte-offset in host RX buffer; -1 = none */
    const uint8_t *rx_buf;
    uint32_t       rx_len;
    uint32_t       rx_pos;

    /* outbound (buffered until flush) */
    int       tx_slot;         /* -1 = no message buffered */
    uint8_t  *tx_buf;
    uint32_t  tx_len;

    /* route-affinity for auto-chunked large messages. cur_group = the current
     * (un-flushed) message's route_group: 0 while it still fits one slot, assigned on
     * its FIRST slot overflow (from the channel's GLOBAL id source) so every chunk of
     * the message pins to ONE backend, cleared at flush. A single-slot message keeps
     * cur_group=0 → normal per-message LB. */
    uint8_t   cur_group;
} dmesh_conn_t;

/* ===== Endpoint lifecycle ===== */

static inline dmesh_channel_t *dmesh_create_channel(const char *app_name, int pod_id) {
    dmesh_channel_t *s = (dmesh_channel_t *)calloc(1, sizeof(*s));
    if (!s) return NULL;
    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    if (dpumesh_init(&s->ctx, app_name, pod_id, &cfg) != 0 || !s->ctx) {
        free(s);
        return NULL;
    }
    s->pod_id    = dpumesh_get_pod_id(s->ctx);
    s->slot_size = dpumesh_get_slot_size(s->ctx);
    return s;
}

static inline void dmesh_destroy_channel(dmesh_channel_t *s) {
    if (!s) return;
    if (s->ctx) dpumesh_destroy(s->ctx);
    free(s);
}

static inline int dmesh_pod_id(dmesh_channel_t *s)  { return s->pod_id; }
static inline int dmesh_msg_max(dmesh_channel_t *s) { return s->slot_size; }
/* The ONE channel fd: readable when a NEW connection is pending (accept) OR any
 * conn has inbound (next_ready). Calling it enables readiness delivery. */
static inline int dmesh_event_fd(dmesh_channel_t *s) { return dpumesh_get_event_fd(s->ctx); }

/* ===== internal helpers ===== */

/* Return the held RX-landing credit and clear the inbound view. */
static inline void conn_free_rx(dmesh_conn_t *c) {
    if (c->rx_slot >= 0) dpumesh_rx_free(c->ep->ctx, c->rx_slot);
    c->rx_slot = -1; c->rx_buf = NULL; c->rx_len = 0; c->rx_pos = 0;
}

/* ===== Connection setup ===== */

/* accept(): NON-BLOCKING. Pops the next NEW connection from the accept queue,
 * allocates a SERVER conn that learns the peer (pod,port), and returns it holding
 * the first message body. NULL+EAGAIN if none pending; NULL+ENOMEM on alloc
 * failure (the message is dropped, its RX credit reclaimed). */
static inline dmesh_conn_t *dmesh_accept(dmesh_channel_t *s) {
    sw_descriptor_t req;
    if (dpumesh_dequeue(s->ctx, &req, 0) < 0 || !req.valid) {
        errno = EAGAIN;
        return NULL;
    }
    dmesh_conn_t *c = (dmesh_conn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; dpumesh_rx_free(s->ctx, req.body_buf_slot); return NULL; }
    /* Model B: the PE created a SERVER_PENDING slot at message-1 delivery (port =
     * req.dst_port = uP, with any pipelined messages 2..P already coalesced in its
     * inbox). Promote it to a live SERVER conn and attach THIS handle so
     * dmesh_next_ready returns it. */
    uint16_t ps = dpumesh_accept_port(s->ctx, req.dst_port, c);
    if (ps == 0) { dpumesh_rx_free(s->ctx, req.body_buf_slot); free(c); errno = ENOMEM; return NULL; }

    c->ep          = s;
    c->role        = DMESH_ROLE_SERVER;
    c->local_port  = ps;                 /* == req.dst_port == uP */
    c->remote_pod  = req.src_pod;        /* learned peer (for replies + further sends) */
    c->remote_port = req.src_port;
    c->dst_service = req.src_service;
    c->seq         = 0;
    c->rx_slot     = req.body_buf_slot;  /* the first message (held; read returns it) */
    c->rx_buf      = dpumesh_rx_buf(s->ctx, req.body_buf_slot);
    c->rx_len      = req.body_len;
    c->rx_pos      = 0;
    c->tx_slot     = -1;
    return c;
}

/* connect(): bind a CLIENT conn to a logical SERVICE. Local; no round-trip. The
 * conn is established (peer learned) on its first inbound. NULL+ENOMEM on OOM. */
static inline dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int dst_service) {
    dmesh_conn_t *c = (dmesh_conn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; return NULL; }
    uint16_t pc = dpumesh_alloc_port(s->ctx, DMESH_ROLE_CLIENT, c);   /* c = the port's handle */
    if (pc == 0) { free(c); errno = ENOMEM; return NULL; }
    c->ep          = s;
    c->role        = DMESH_ROLE_CLIENT;
    c->local_port  = pc;
    c->dst_service = (int16_t)dst_service;
    c->remote_pod  = DMESH_POD_BLANK;
    c->remote_port = DMESH_PORT_BLANK;
    c->seq         = 0;
    c->rx_slot     = -1;
    c->tx_slot     = -1;
    return c;
}

/* Pop the next conn that has inbound, from the channel's ready list (the PE puts
 * ready conns here, so there is NO scan and NO per-conn fd). Returns the SAME conn
 * handle you created at accept/connect, or NULL when drained. After waking on
 * dmesh_event_fd(s), loop dmesh_next_ready() and drain each returned conn to EAGAIN.
 * Single-consumer: call it from your one event-loop thread. */
static inline dmesh_conn_t *dmesh_next_ready(dmesh_channel_t *s) {
    return (dmesh_conn_t *)dpumesh_next_ready(s->ctx);
}

/* ===== read / write / send ===== */

/* read(): return up to `len` bytes of the NEXT inbound message on this conn.
 * One message is atomic (<= slot_size); a single read with a big enough buffer
 * returns the whole message. When a message is fully consumed its RX credit is
 * freed and the next read fetches a new message. Learns the peer on first inbound.
 *   >0 bytes, 0 = EOF (peer closed the conn via FIN; sticky), -1 = EAGAIN.
 * A zero-length inbound message IS the FIN marker (user 0-length sends are no-ops),
 * so read()==0 means the peer closed — close this conn (like a BSD socket EOF). */
static inline ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len) {
    if (c->peer_closed) return 0;             /* EOF is sticky once the FIN arrived */
    if (c->rx_slot < 0) {                     /* no message loaded → fetch the next */
        sw_descriptor_t d;
        if (!dpumesh_conn_recv(c->ep->ctx, c->local_port, &d)) { errno = EAGAIN; return -1; }
        if (d.body_len == 0) {                /* FIN marker → EOF: reclaim its landing, latch closed */
            dpumesh_rx_free(c->ep->ctx, d.body_buf_slot);
            c->peer_closed = 1;
            return 0;
        }
        c->rx_slot = d.body_buf_slot;
        c->rx_buf  = dpumesh_rx_buf(c->ep->ctx, d.body_buf_slot);
        c->rx_len  = d.body_len;
        c->rx_pos  = 0;
        /* Model B: a CLIENT does NOT learn/pin a peer — it keeps addressing its
         * service (dst_pod=BLANK) and the DPU owns the upstream. A SERVER conn
         * already learned its peer (client_pod, uP) at accept. So there is no
         * learn-on-read here. */
    }
    size_t avail = c->rx_len - c->rx_pos;
    size_t n = (len < avail) ? len : avail;
    if (n && c->rx_buf) memcpy(buf, c->rx_buf + c->rx_pos, n);
    c->rx_pos += (uint32_t)n;
    if (c->rx_pos >= c->rx_len)           /* message consumed → free credit, next read fetches a new one */
        conn_free_rx(c);
    return (ssize_t)n;
}

/* INTERNAL: ensure a TX slot (a new outbound message) is allocated. */
static inline int dmesh_tx_ensure(dmesh_conn_t *c) {
    if (c->tx_slot >= 0) return 0;
    int slot = dpumesh_tx_alloc(c->ep->ctx);
    if (slot < 0) { errno = EAGAIN; return -1; }
    c->tx_slot = slot;
    c->tx_buf  = dpumesh_tx_buf(c->ep->ctx, slot);
    c->tx_len  = 0;
    return 0;
}

/* INTERNAL: ship the currently-buffered TX slot as one message stamped with
 * route_group byte `rg` (0 = normal per-message LB; 1..255 = pin all chunks sharing
 * this id to one backend). Resets the TX buffer. Returns 0, -1 (EBADMSG) on fault. */
static inline int dmesh_ship_slot(dmesh_conn_t *c, uint8_t rg) {
    dpumesh_ctx_t *ctx = c->ep->ctx;
    c->seq++;                                        /* per-conn outbound message id */
    sw_descriptor_t d;
    memset(&d, 0, sizeof(d));
    d.body_buf_slot = c->tx_slot;
    d.body_len      = c->tx_len;
    d.src_port      = c->local_port;
    d.seq           = c->seq;
    d.dst_service   = c->dst_service;
    if (c->role == DMESH_ROLE_CLIENT) {              /* client → service (per-message LB) */
        d.dst_pod  = DMESH_POD_BLANK;
        d.dst_port = DMESH_PORT_BLANK;
    } else {                                         /* server → its learned peer */
        d.dst_pod  = c->remote_pod;
        d.dst_port = c->remote_port;
    }
    d.route_group = rg;
    d.valid = 1;
    if (dpumesh_enqueue(ctx, &d) < 0) {
        dpumesh_tx_free(ctx, c->tx_slot);
        c->tx_slot = -1; c->tx_buf = NULL; c->tx_len = 0;
        errno = EBADMSG;
        return -1;
    }
    /* The DPU's BATCH_FWD_ACK frees this slot (it may be DMA-read until delivered). */
    dpumesh_tx_track(ctx, c->local_port, c->seq, c->tx_slot);
    c->tx_slot = -1; c->tx_buf = NULL; c->tx_len = 0;
    return 0;
}

/* write(): BUFFER outbound body bytes into the current message (shipped by flush).
 * Consecutive writes accumulate; the first write after a flush starts a NEW message.
 * A message LARGER than one slot is AUTO-CHUNKED across slots — on overflow the full
 * slot ships as a chunk (all chunks of the message share one route_group so the DPU
 * pins them to ONE backend, reassemble-able even under per-message LB) and a fresh
 * slot continues. So the app just writes ANY length; there is no size limit and no
 * EMSGSIZE. The receiver is a plain dmesh_read loop (it frames its own length). */
static inline ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    int cap = c->ep->slot_size;
    size_t done = 0;
    while (done < len) {
        if (dmesh_tx_ensure(c) < 0) return done ? (ssize_t)done : -1;
        size_t room = (size_t)cap - c->tx_len;
        if (room == 0) {                             /* slot full + more to write → spill */
            if (c->cur_group == 0) {                 /* first overflow → claim a GLOBAL group */
                uint32_t g = __atomic_fetch_add(&c->ep->next_group, 1u, __ATOMIC_RELAXED);
                c->cur_group = (uint8_t)((g % 255u) + 1u);   /* 1..255, never 0 */
            }
            if (dmesh_ship_slot(c, c->cur_group) < 0) /* ship the full slot as a chunk */
                return done ? (ssize_t)done : -1;
            continue;                                /* next iter tx_ensure gets a fresh slot */
        }
        size_t n = len - done; if (n > room) n = room;
        memcpy(c->tx_buf + c->tx_len, p + done, n);
        c->tx_len += (uint32_t)n;
        done += n;
    }
    return (ssize_t)len;
}

/* sendfile(): append up to count bytes from in_fd into the current message. */
static inline ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count) {
    if (dmesh_tx_ensure(c) < 0) return -1;
    int cap = c->ep->slot_size;
    size_t room = (size_t)cap - c->tx_len;
    if (count > room) count = room;
    if (count == 0) { errno = EMSGSIZE; return -1; }
    ssize_t n;
    if (offset) n = pread(in_fd, c->tx_buf + c->tx_len, count, *offset);
    else        n = read (in_fd, c->tx_buf + c->tx_len, count);
    if (n <= 0) return n;
    c->tx_len += (uint32_t)n;
    if (offset) *offset += n;
    return n;
}

/* flush(): ship the buffered message's final chunk. REQUIRED to send. A message that
 * spanned slots (auto-chunked in write → cur_group set) ships this last chunk with the
 * same route_group (so it too pins to the message's backend), then clears the group
 * context; a single-slot message ships route_group=0 (normal per-message LB). A CLIENT
 * ships dst_pod=BLANK (DPU LB's per message); a SERVER ships to its learned peer. An
 * EMPTY flush (no bytes buffered) is a NO-OP (a 0-length wire message is the FIN only,
 * never a user message).
 *   0 = sent (or nothing to send); -1 EBADMSG = descriptor fault (close the conn). */
static inline int dmesh_flush(dmesh_conn_t *c) {
    if (c->tx_slot < 0 || c->tx_len == 0) {          /* nothing buffered → no-op (0-len = FIN only) */
        if (c->tx_slot >= 0) { dpumesh_tx_free(c->ep->ctx, c->tx_slot); c->tx_slot = -1; c->tx_buf = NULL; c->tx_len = 0; }
        c->cur_group = 0;
        return 0;
    }
    uint8_t rg = c->cur_group;                       /* 0 = single-slot (normal LB); else the group */
    int r = dmesh_ship_slot(c, rg);
    c->cur_group = 0;                                /* clear: next write = a new message/unit */
    return r;                                        /* 0 or -1 (EBADMSG) */
}

/* INTERNAL: send a FIN — a zero-length message addressed to the established peer.
 * It rides the SAME conn-shard ring as this conn's data (src_port), so it arrives
 * AFTER every prior message (ordering preserved). The peer's PE delivers it to the
 * conn inbox as a 0-length descriptor → the peer's read() returns EOF → the peer
 * closes → its port slot is reclaimed (no cross-run conn accumulation).
 * Best-effort: if no TX slot is free we skip the FIN; the peer/DPU then keep that
 * conn + upstream until the port/uP is reused (there is NO idle reaper — apply your
 * own wall-clock timeout for a service that never answers). A CLIENT FINs to its
 * service (dst_pod=BLANK; the DPU frees the upstream it created); a SERVER FINs to
 * its learned peer. */
static inline void dmesh_send_fin(dmesh_conn_t *c) {
    dpumesh_ctx_t *ctx = c->ep->ctx;
    int slot = dpumesh_tx_alloc(ctx);
    if (slot < 0) return;                                  /* no slot → best-effort skip */
    c->seq++;
    sw_descriptor_t d;
    memset(&d, 0, sizeof(d));
    d.body_buf_slot = slot;
    d.body_len      = 0;                                   /* FIN marker (0-length) */
    d.src_port      = c->local_port;
    d.seq           = c->seq;
    d.dst_service   = c->dst_service;
    d.dst_pod       = c->remote_pod;                       /* the learned peer conn */
    d.dst_port      = c->remote_port;
    d.valid         = 1;
    if (dpumesh_enqueue(ctx, &d) < 0) { dpumesh_tx_free(ctx, slot); return; }
    dpumesh_tx_track(ctx, c->local_port, c->seq, slot);    /* freed by its own TX_ACK */
}

/* close(): graceful close. If established, sends a FIN so the peer reclaims its conn
 * (no leak across runs). Then frees the conn: reclaims held RX credit + recycles the
 * eventfd; un-ACKed sent TX slots are freed by their own TX_ACKs. A buffered-but-
 * unflushed message is discarded (flush first). Safe on NULL. Returns 0. */
static inline int dmesh_close(dmesh_conn_t *c) {
    if (!c) return 0;
    dpumesh_ctx_t *ctx = c->ep->ctx;
    /* Send a FIN so the peer (and the DPU-owned upstream) is torn down. A SERVER
     * FINs once established; a CLIENT FINs if it ever sent (seq>0) so the DPU frees
     * the upstream it created. Skip if we're closing on the peer's FIN. */
    if (!c->peer_closed && (c->role == DMESH_ROLE_SERVER || c->seq > 0))
        dmesh_send_fin(c);
    conn_free_rx(c);                                       /* return the held RX credit */
    if (c->tx_slot >= 0) dpumesh_tx_free(ctx, c->tx_slot); /* buffered, never flushed */
    if (c->local_port)   dpumesh_free_port(ctx, c->local_port);
    free(c);
    return 0;
}

/* ===== Large messages (> slot_size) — transparent, NO special API =====
 * There is no write_large/read_large. A payload larger than one slot is handled by
 * plain dmesh_write + dmesh_flush: write AUTO-CHUNKS across slots and pins every chunk
 * of the message to ONE backend (a shared route_group, §route-affinity), so the chunks
 * arrive in order on that backend and can be reassembled. The receiver is a plain
 * dmesh_read LOOP: read chunks in arrival order and concatenate until you have the
 * length YOUR protocol declares (framing/completeness is the app's job, like a byte
 * stream). Because the chunks are pinned + in order, arrival order == send order — if
 * affinity ever failed the chunks would arrive out of order and the app's content
 * check would catch it. bench/bench_sock.c (mode=3) is the worked example. */

/* ===== Event-loop integration =====
 * ONE fd: dmesh_event_fd(s). Register it in a vanilla epoll set; it becomes readable
 * when a new conn is pending OR any conn has inbound. On EPOLLIN: drain the fd
 * (read() a uint64_t), then loop dmesh_accept() for new conns and dmesh_next_ready()
 * for conns with data (drain each to EAGAIN). No per-conn fds, no dmesh_epoll_*
 * wrappers — the PE-published ready list replaces both the scan and the per-conn fd. */

#ifdef __cplusplus
}
#endif

#endif /* DPM_H */
