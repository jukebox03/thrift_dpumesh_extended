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
 *   - A `dmesh_conn_t` is a persistent connection between two endpoints (a local
 *     port and a peer (pod,port)), alive until close. The transport delivers ALL
 *     inbound messages on a conn to the app — it does NO request↔response matching
 *     (that's the app's job, if it wants RPC semantics).
 *   - Establishment: `dmesh_connect(svc)` is local (no round-trip). The FIRST
 *     `write+flush` is the establishing message (dst_pod=BLANK → the DPU routes the
 *     service to a backend pod). The destination `dmesh_accept()`s it → a server
 *     conn that learns the peer (pod,port). The client learns the backend (pod,port)
 *     from its first inbound and addresses it directly thereafter. Then both sides
 *     send/receive freely on the same conn until close (the accept side cannot send
 *     before it has received the first message — it has no peer to address yet).
 *   - Ordering: messages on ONE conn are delivered in send order (conn-sharding).
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
#include <sched.h>
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
    int16_t   remote_pod;      /* peer pod; DMESH_POD_BLANK until learned */
    uint16_t  remote_port;     /* peer port; 0 until learned */
    uint8_t   established;      /* learned the peer (pod,port)? */
    uint8_t   peer_closed;      /* received the peer's FIN → reads return EOF (sticky) */
    uint16_t  seq;             /* per-conn OUTBOUND message counter */

    /* inbound (the message currently being read out) */
    int            rx_slot;    /* landing byte-offset in host RX buffer; -1 = none */
    const uint8_t *rx_buf;
    uint32_t       rx_len;
    uint32_t       rx_pos;
    int            rx_ready;

    /* outbound (buffered until flush) */
    int       tx_slot;         /* -1 = no message buffered */
    uint8_t  *tx_buf;
    uint32_t  tx_len;
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
/* The endpoint "listen" fd: readable when a NEW connection is pending (accept). */
static inline int dmesh_event_fd(dmesh_channel_t *s) { return dpumesh_get_event_fd(s->ctx); }

/* ===== internal helpers ===== */

/* Return the held RX-landing credit and clear the inbound view. */
static inline void conn_free_rx(dmesh_conn_t *c) {
    if (c->rx_slot >= 0) dpumesh_rx_free(c->ep->ctx, c->rx_slot);
    c->rx_slot = -1; c->rx_buf = NULL; c->rx_len = 0; c->rx_pos = 0; c->rx_ready = 0;
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
    /* Register THIS conn as the port's handle so dmesh_next_ready returns it. */
    uint16_t ps = dpumesh_alloc_port(s->ctx, DMESH_ROLE_SERVER, c);
    if (ps == 0) { dpumesh_rx_free(s->ctx, req.body_buf_slot); free(c); errno = ENOMEM; return NULL; }

    c->ep          = s;
    c->role        = DMESH_ROLE_SERVER;
    c->local_port  = ps;
    c->remote_pod  = req.src_pod;        /* learned peer (for replies + further sends) */
    c->remote_port = req.src_port;
    c->dst_service = req.src_service;
    c->established = 1;
    c->seq         = 0;
    c->rx_slot     = req.body_buf_slot;  /* the first message (held; read returns it) */
    c->rx_buf      = dpumesh_rx_buf(s->ctx, req.body_buf_slot);
    c->rx_len      = req.body_len;
    c->rx_pos      = 0;
    c->rx_ready    = 1;
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
    c->established = 0;
    c->seq         = 0;
    c->rx_slot     = -1;
    c->tx_slot     = -1;
    return c;
}

/* The peer of this conn: the resolved peer pod once established, else the service. */
static inline int32_t dmesh_peer(dmesh_conn_t *c) {
    return (c->remote_pod != DMESH_POD_BLANK) ? c->remote_pod : c->dst_service;
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
    if (!c->rx_ready) {
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
        c->rx_ready = 1;
        if (!c->established) {            /* learn the peer (pod,port) from first inbound */
            c->remote_pod  = d.src_pod;
            c->remote_port = d.src_port;
            c->dst_service = d.src_service;
            c->established = 1;
        }
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

/* write(): BUFFER outbound body bytes into the current message (shipped by flush).
 * Consecutive writes accumulate; the first write after a flush starts a NEW message.
 * No single-outstanding restriction — you can flush many messages without reading. */
static inline ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len) {
    if (dmesh_tx_ensure(c) < 0) return -1;
    int cap = c->ep->slot_size;
    if (c->tx_len + len > (uint32_t)cap) { errno = EMSGSIZE; return -1; }
    memcpy(c->tx_buf + c->tx_len, buf, len);
    c->tx_len += (uint32_t)len;
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

/* flush(): ship the buffered message. CLIENT first message → dst_pod=BLANK (the DPU
 * routes the service); once established → straight to the learned peer (pod,port).
 * The TX slot is freed by the DPU's TX_ACK (dpumesh_tx_track). REQUIRED to send.
 * An EMPTY flush (no bytes buffered) is a NO-OP: a zero-length message on the wire
 * is the FIN marker (dmesh_close), never a user message — so flushing nothing sends
 * nothing rather than spuriously closing the peer.
 *   0 sent (or nothing to send); -1 EAGAIN (no TX slot) / EBADMSG (descriptor fault). */
static inline int dmesh_flush(dmesh_conn_t *c) {
    dpumesh_ctx_t *ctx = c->ep->ctx;
    if (c->tx_slot < 0 || c->tx_len == 0) {          /* nothing buffered → no-op (0-len = FIN only) */
        if (c->tx_slot >= 0) { dpumesh_tx_free(ctx, c->tx_slot); c->tx_slot = -1; c->tx_buf = NULL; c->tx_len = 0; }
        return 0;
    }

    c->seq++;                                        /* per-conn outbound message id */

    sw_descriptor_t d;
    memset(&d, 0, sizeof(d));
    d.body_buf_slot = c->tx_slot;
    d.body_len      = c->tx_len;
    d.src_port      = c->local_port;
    d.seq           = c->seq;
    d.dst_service   = c->dst_service;
    if (c->role == DMESH_ROLE_CLIENT && !c->established) {
        d.dst_pod  = DMESH_POD_BLANK;                /* first message → DPU resolves the service */
        d.dst_port = DMESH_PORT_BLANK;
    } else {
        d.dst_pod  = c->remote_pod;                  /* direct to the established peer conn */
        d.dst_port = c->remote_port;
    }
    d.valid = 1;

    if (dpumesh_enqueue(ctx, &d) < 0) {
        dpumesh_tx_free(ctx, c->tx_slot);
        c->tx_slot = -1; c->tx_buf = NULL; c->tx_len = 0;
        errno = EBADMSG;
        return -1;
    }
    /* The DPU's TX_ACK frees this slot (it may still be DMA-read until delivered). */
    dpumesh_tx_track(ctx, c->local_port, c->seq, c->tx_slot);

    c->tx_slot = -1;                                 /* handed off; next write starts a new message */
    c->tx_buf  = NULL;
    c->tx_len  = 0;
    return 0;
}

/* INTERNAL: send a FIN — a zero-length message addressed to the established peer.
 * It rides the SAME conn-shard ring as this conn's data (src_port), so it arrives
 * AFTER every prior message (ordering preserved). The peer's PE delivers it to the
 * conn inbox as a 0-length descriptor → the peer's read() returns EOF → the peer
 * closes → its port slot is reclaimed (no cross-run conn accumulation).
 * Best-effort: if no TX slot is free we skip it (the peer reclaims via idle-GC).
 * Only the established peer can be addressed; an un-established CLIENT has no peer. */
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
    if (c->established && !c->peer_closed)                 /* tell the peer to reclaim its slot */
        dmesh_send_fin(c);                                 /* (skip if we're closing on THEIR FIN) */
    conn_free_rx(c);                                       /* return the held RX credit */
    if (c->tx_slot >= 0) dpumesh_tx_free(ctx, c->tx_slot); /* buffered, never flushed */
    if (c->local_port)   dpumesh_free_port(ctx, c->local_port);
    free(c);
    return 0;
}

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
