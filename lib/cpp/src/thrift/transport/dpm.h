/*
 * dpm.h — socket/epoll-style façade over the DPUmesh C API (oriented endpoint
 * tuple model — design-endpoint-tuple.md). Header-only.
 *
 *     socket()/bind()/listen()  ->  dmesh_create_channel()   (registers a service)
 *     accept()                  ->  dmesh_accept()           (allocates a server port)
 *     connect()                 ->  dmesh_connect(svc)       (allocates a client port)
 *     read()                    ->  dmesh_read()
 *     write()                   ->  dmesh_write()  (buffers; dmesh_flush ships)
 *     sendfile()                ->  dmesh_sendfile()
 *     close()                   ->  dmesh_close()
 *     epoll_*()                 ->  UNCHANGED native epoll on dmesh_event_fd(s)
 *
 * ADDRESSING (TCP-faithful, no direction flag):
 *   - A message carries src=(pod,port) and dst=(service,pod,port) + a per-conn
 *     seq. The receiver demuxes by dst_port: a CLIENT port → reply; a SERVER port
 *     → established request; port 0 (BLANK) → a fresh connection request (accept
 *     queue). Request vs response is implicit in WHICH local socket the tuple
 *     resolves to — never signaled. Self-routing / loopback works (client and
 *     server ports are distinct, even on one host).
 *   - dmesh_connect(svc) binds a logical SERVICE; the DPU routes the FIRST request
 *     (dst_pod=BLANK) to a backend pod (connection-level / sticky LB). The client
 *     learns the backend (pod,port) from the first reply and talks to it directly
 *     thereafter — so one accepted server conn carries MANY exchanges (persistent).
 *
 * SEMANTICS: message/RPC channel, not a byte stream. One whole <= slot_size (8 KB)
 * body arrives atomically. NON-BLOCKING RX (EAGAIN). One conn carries one
 * outstanding exchange at a time. SEND IS EXPLICIT: write buffers, flush ships.
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

/* Endpoint — one per process. Wraps one DOCA device context + this node's
 * registered service. */
typedef struct dmesh_channel {
    dpumesh_ctx_t *ctx;
    int            pod_id;
    int            slot_size;   /* cached max body size */
} dmesh_channel_t;

/* Connection — one peer conversation (oriented tuple). */
typedef struct dmesh_conn {
    dmesh_channel_t *ep;
    int       role;            /* DMESH_ROLE_CLIENT (connect) | DMESH_ROLE_SERVER (accept) */

    /* addressing tuple */
    uint16_t  local_port;      /* my port: pc (client) or ps (server) */
    int16_t   dst_service;     /* peer service: callee (client) / caller (server) */
    int16_t   remote_pod;      /* peer pod; DMESH_POD_BLANK on a client pre-establish */
    uint16_t  remote_port;     /* peer port; 0 on a client pre-establish */
    uint8_t   established;      /* learned the peer (pod,port)? */
    uint16_t  seq;             /* current exchange seq (match key with local_port) */

    /* inbound body (SERVER: the request; CLIENT: the reply) */
    int            rx_slot;    /* landing byte-offset in host RX buffer; -1 = none */
    const uint8_t *rx_buf;
    uint32_t       rx_len;
    uint32_t       rx_pos;
    int            rx_ready;

    /* outbound body (buffered until flush) */
    int       tx_slot;         /* -1 = not allocated */
    uint8_t  *tx_buf;
    uint32_t  tx_len;
    int       sent;
    int       pending;         /* registered in pending[(local_port,seq)] ? */
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
static inline int dmesh_event_fd(dmesh_channel_t *s) { return dpumesh_get_event_fd(s->ctx); }

/* ===== internal helpers ===== */

/* Return any held RX-landing credit and clear the inbound view. */
static inline void conn_free_rx(dmesh_conn_t *c) {
    if (c->rx_slot >= 0) dpumesh_rx_free(c->ep->ctx, c->rx_slot);
    c->rx_slot = -1; c->rx_buf = NULL; c->rx_len = 0; c->rx_pos = 0; c->rx_ready = 0;
}

/* ===== Connection setup ===== */

/* accept(): NON-BLOCKING. Pops the next fresh connection request from the accept
 * queue, allocates a SERVER port (the persistent conn endpoint), and returns a
 * conn already holding the request body. NULL+EAGAIN if none pending; NULL+ENOMEM
 * on alloc failure (the request is dropped, its RX slot reclaimed). */
static inline dmesh_conn_t *dmesh_accept(dmesh_channel_t *s) {
    sw_descriptor_t req;
    if (dpumesh_dequeue(s->ctx, &req, 0) < 0 || !req.valid) {
        errno = EAGAIN;
        return NULL;
    }
    dmesh_conn_t *c = (dmesh_conn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; dpumesh_rx_free(s->ctx, req.body_buf_slot); return NULL; }
    uint16_t ps = dpumesh_alloc_port(s->ctx, DMESH_ROLE_SERVER);
    if (ps == 0) { dpumesh_rx_free(s->ctx, req.body_buf_slot); free(c); errno = ENOMEM; return NULL; }

    c->ep          = s;
    c->role        = DMESH_ROLE_SERVER;
    c->local_port  = ps;
    c->remote_pod  = req.src_pod;        /* the client (for replies) */
    c->remote_port = req.src_port;
    c->dst_service = req.src_service;     /* caller's service (for the reply mirror) */
    c->established = 1;
    c->seq         = req.seq;            /* echo on the reply */
    c->rx_slot     = req.body_buf_slot;
    c->rx_buf      = dpumesh_rx_buf(s->ctx, req.body_buf_slot);
    c->rx_len      = req.body_len;
    c->rx_pos      = 0;
    c->rx_ready    = 1;                  /* request body already here */
    c->tx_slot     = -1;
    return c;
}

/* connect(): bind a CLIENT conn to a logical SERVICE. Allocates a client port; no
 * round-trip. The DPU resolves the service → backend on the first request; the
 * conn learns the backend (pod,port) from the first reply. NULL+ENOMEM on OOM. */
static inline dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int dst_service) {
    dmesh_conn_t *c = (dmesh_conn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; return NULL; }
    uint16_t pc = dpumesh_alloc_port(s->ctx, DMESH_ROLE_CLIENT);
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

/* The peer of this exchange: the resolved peer pod once established, else the
 * service connected to. */
static inline int32_t dmesh_peer(dmesh_conn_t *c) {
    return (c->remote_pod != DMESH_POD_BLANK) ? c->remote_pod : c->dst_service;
}

/* INTERNAL: poll a CLIENT conn for its reply. 1=ready, 0=not yet, -1=abandoned. */
static inline int dmesh_poll_reply(dmesh_conn_t *c) {
    if (c->rx_ready) return 1;
    if (!c->sent)    return 0;
    sw_descriptor_t resp;
    int r = dpumesh_poll_response(c->ep->ctx, c->local_port, c->seq, &resp);
    if (r == 1) return 0;
    if (r < 0)  return -1;
    /* Reply arrived. On the FIRST exchange, learn the resolved backend POD so
     * subsequent requests go direct to it (connection-level sticky). M3: the
     * client does NOT pin to a server port — established requests keep
     * dst_port=0 (→ the backend's accept queue), so remote_port stays 0. */
    if (!c->established) {
        c->remote_pod  = resp.src_pod;
        c->established = 1;
    }
    c->rx_slot  = resp.body_buf_slot;
    c->rx_buf   = dpumesh_rx_buf(c->ep->ctx, resp.body_buf_slot);
    c->rx_len   = resp.body_len;
    c->rx_pos   = 0;
    c->rx_ready = 1;
    c->tx_slot  = -1;            /* poll_response already freed the TX slot */
    c->pending  = 0;            /* and consumed the pending entry          */
    return 1;
}

/* INTERNAL: poll a SERVER conn for the next established request (single inbox). */
static inline int dmesh_poll_request_conn(dmesh_conn_t *c) {
    if (c->rx_ready) return 1;
    sw_descriptor_t req;
    if (!dpumesh_poll_request(c->ep->ctx, c->local_port, &req)) return 0;
    c->seq         = req.seq;
    c->remote_pod  = req.src_pod;
    c->remote_port = req.src_port;
    c->dst_service = req.src_service;
    c->rx_slot     = req.body_buf_slot;
    c->rx_buf      = dpumesh_rx_buf(c->ep->ctx, req.body_buf_slot);
    c->rx_len      = req.body_len;
    c->rx_pos      = 0;
    c->rx_ready    = 1;
    return 1;
}

/* ===== read / write / send ===== */

/* read(): copy inbound body bytes. CLIENT: polls for the reply (flush the request
 * first). SERVER: returns the accepted request, then (after a reply) polls the
 * inbox for the next established request on this conn.
 *   >0 bytes, 0 = end of message, -1 = would-block (EAGAIN) / abandoned (ECONNRESET). */
static inline ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len) {
    if (!c->rx_ready) {
        int r = (c->role == DMESH_ROLE_CLIENT) ? dmesh_poll_reply(c)
                                               : dmesh_poll_request_conn(c);
        if (r == 0) { errno = EAGAIN;     return -1; }
        if (r < 0)  { errno = ECONNRESET; return -1; }
    }
    if (!c->rx_buf || c->rx_pos >= c->rx_len) return 0;   /* EOF */
    size_t avail = c->rx_len - c->rx_pos;
    size_t n = (len < avail) ? len : avail;
    memcpy(buf, c->rx_buf + c->rx_pos, n);
    c->rx_pos += (uint32_t)n;
    return (ssize_t)n;
}

/* INTERNAL: ensure a TX slot is allocated. 0 ok, -1 backpressure (EAGAIN). */
static inline int dmesh_tx_ensure(dmesh_conn_t *c) {
    if (c->tx_slot >= 0) return 0;
    int slot = dpumesh_tx_alloc(c->ep->ctx);
    if (slot < 0) { errno = EAGAIN; return -1; }
    c->tx_slot = slot;
    c->tx_buf  = dpumesh_tx_buf(c->ep->ctx, slot);
    c->tx_len  = 0;
    return 0;
}

/* INTERNAL: reset a CLIENT conn for a new request after its reply was read. Frees
 * the prior reply's RX landing; keeps the conn binding + seq counter. */
static inline void conn_reset_exchange(dmesh_conn_t *c) {
    conn_free_rx(c);
    c->sent = 0; c->pending = 0;
    /* tx_slot already -1 (handed off at flush); a fresh one is taken on write. */
}

/* INTERNAL: begin a new outbound message. A CLIENT conn whose reply was read
 * auto-resets for the next request; otherwise a 2nd write while outstanding is
 * EINVAL. Then ensure a TX slot. */
static inline int conn_begin_tx(dmesh_conn_t *c) {
    if (c->sent) {
        if (c->role == DMESH_ROLE_CLIENT && c->rx_ready) conn_reset_exchange(c);
        else { errno = EINVAL; return -1; }
    }
    return dmesh_tx_ensure(c);
}

/* write(): BUFFER outbound body bytes (shipped by dmesh_flush). */
static inline ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len) {
    if (conn_begin_tx(c) < 0) return -1;
    int cap = c->ep->slot_size;
    if (c->tx_len + len > (uint32_t)cap) { errno = EMSGSIZE; return -1; }
    memcpy(c->tx_buf + c->tx_len, buf, len);
    c->tx_len += (uint32_t)len;
    return (ssize_t)len;
}

/* sendfile(): append up to count bytes from in_fd (capped at slot_size). */
static inline ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count) {
    if (conn_begin_tx(c) < 0) return -1;
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

/* flush(): ship the buffered message. CLIENT: a request (first → routed by
 * service with dst_pod=BLANK; established → direct to the learned backend).
 * SERVER: a reply, mirrored back to the caller; the conn is then ready for the
 * next request. REQUIRED to send.
 *   0 sent; -1 EINVAL (already sent) / EAGAIN (pending collision) / EBADMSG (desc fault). */
static inline int dmesh_flush(dmesh_conn_t *c) {
    dpumesh_ctx_t *ctx = c->ep->ctx;
    if (c->sent) { errno = EINVAL; return -1; }
    if (dmesh_tx_ensure(c) < 0) return -1;          /* allow zero-length messages */

    if (c->role == DMESH_ROLE_CLIENT)
        c->seq++;                                   /* new per-conn seq for this request */

    if (!c->pending) {
        if (dpumesh_register_pending(ctx, c->local_port, c->seq) < 0) { errno = EAGAIN; return -1; }
        c->pending = 1;
    }

    sw_descriptor_t d;
    memset(&d, 0, sizeof(d));
    d.body_buf_slot = c->tx_slot;
    d.body_len      = c->tx_len;
    d.src_port      = c->local_port;
    d.seq           = c->seq;
    d.dst_service   = c->dst_service;
    if (c->role == DMESH_ROLE_CLIENT && !c->established) {
        d.dst_pod  = DMESH_POD_BLANK;     /* first request → DPU resolves dst_service */
        d.dst_port = DMESH_PORT_BLANK;
    } else {
        d.dst_pod  = c->remote_pod;       /* direct: established client / server reply */
        d.dst_port = c->remote_port;
    }
    d.valid = 1;

    if (dpumesh_enqueue(ctx, &d) < 0) {
        dpumesh_cancel_pending(ctx, c->local_port, c->seq);
        c->pending = 0;
        errno = EBADMSG;
        return -1;
    }
    dpumesh_pending_attach_tx(ctx, c->local_port, c->seq, c->tx_slot);

    c->tx_slot = -1;
    c->tx_buf  = NULL;
    c->tx_len  = 0;

    if (c->role == DMESH_ROLE_SERVER) {
        /* Reply shipped: no response expected on (ps,seq); the TX_ACK frees the
         * TX slot. M3: one reply per accepted request — the app closes this conn
         * next (which frees ps + the request's RX credit). No conn reuse. */
        dpumesh_pending_release_async(ctx, c->local_port, c->seq);
        c->pending = 0;
        c->sent = 1;
    } else {
        c->sent = 1;                                /* await the reply (dmesh_read) */
    }
    return 0;
}

/* close(): free the conn's port/slots/pending. Does NOT send (flush first). */
static inline int dmesh_close(dmesh_conn_t *c) {
    if (!c) return 0;
    dpumesh_ctx_t *ctx = c->ep->ctx;
    conn_free_rx(c);                                     /* return held RX credit */
    if (c->tx_slot >= 0 && !c->sent) dpumesh_tx_free(ctx, c->tx_slot);  /* buffered, never flushed */
    if (c->pending)                  dpumesh_cancel_pending(ctx, c->local_port, c->seq);
    if (c->local_port)               dpumesh_free_port(ctx, c->local_port);
    free(c);
    return 0;
}

/* ===== Event-loop integration =====
 * No dmesh_epoll_* wrappers. Register dmesh_event_fd(s) in native epoll like a
 * listen socket; on EPOLLIN drain it (read() a uint64_t) and accept()/read(). */

#ifdef __cplusplus
}
#endif

#endif /* DPM_H */
