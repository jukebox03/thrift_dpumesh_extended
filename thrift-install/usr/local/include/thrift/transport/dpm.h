/*
 * dpm.h — socket/epoll-style façade over the DPUmesh C API.
 *
 * Header-only. Lets you port an ordinary non-blocking epoll server/client to
 * DPUmesh by swapping the BSD-socket calls for their `_dpm` equivalents:
 *
 *     socket()/bind()/listen()  ->  socket_dpm()      (folded into one)
 *     accept()                  ->  accept_dpm()
 *     connect()                 ->  connect_dpm()
 *     read()                    ->  read_dpm()
 *     write()                   ->  write_dpm()  (read/close ships it; send optional)
 *     sendfile()                ->  sendfile_dpm()
 *     close()                   ->  close_dpm()
 *     epoll_create()/_ctl()/_wait() -> UNCHANGED — use native kernel epoll, and
 *         register event_fd_dpm(s) (a real fd) like a listen socket. When it
 *         is readable, drain it (one read() of a uint64_t) and accept_dpm()
 *         the pending request(s). No epoll_*_dpm wrappers exist.
 *
 * SEMANTIC DIFFERENCES vs BSD sockets (by design — read these):
 *   1. MESSAGE-oriented, not a byte stream. One request maps to one response,
 *      each body <= slot_size (HARD CAP 8 KB — the DPA dma_copy limit). A whole
 *      message arrives ATOMICALLY: accept_dpm() already holds the full
 *      request body, so there is no partial-read state machine and no EPOLLOUT
 *      dance for the body.
 *   2. NON-BLOCKING RX. accept_dpm / read_dpm and epoll readiness never
 *      block; "not ready" returns the would-block sentinel with errno=EAGAIN.
 *      The TX side (write/sendfile/send) never blocks on the PEER, but it
 *      BUSY-SPINS (capped ~50µs backoff) while the LOCAL TX-slot pool or DMA ring
 *      is saturated (dpumesh_tx_alloc/enqueue self-throttle by spinning, they
 *      never fail with backpressure), and send's register_pending can block up to
 *      ~2s on a req_id collision. There is no cond-blocking mode. To SLEEP until
 *      a request/response is ready, wait on event_fd_dpm(s) with native
 *      epoll/poll/select — it is notification-driven (no busy-poll).
 *   3. SINGLE-SHOT conn. One dpmconn_t carries exactly one request/response; after
 *      flush_dpm() it cannot be written/sent again — close_dpm() and make a
 *      new connect_dpm()/accept_dpm().
 *   4. ADDRESS = pod_id (a small integer), not an IP/port. A "connection" is one
 *      request/response conversation, not a persistent stream; reuse is per-call.
 *   5. write_dpm() BUFFERS into the TX slot; the message is transmitted
 *      AUTOMATICALLY by the next read_dpm() (client) or close_dpm()
 *      (server), so the write->read / write->close patterns need NO explicit send.
 *      flush_dpm() remains available to flush explicitly (e.g. a pipelined
 *      async client that ships now and harvests the response later).
 *
 * Thread-safety: the underlying ctx is internally locked, so multiple threads may
 * each run their own accept/handle loop on one endpoint. A single dpmconn_t is NOT
 * thread-safe — use one per thread.
 */
#ifndef DPM_H
#define DPM_H

/* Requires POSIX (pread/read/sched_yield) — compile with the project's default
 * flags (gcc gnu11) or define _GNU_SOURCE; the rest of DPUmesh is POSIX/Linux. */

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

/* Endpoint — the bound, listening socket. One per process (wraps one DOCA
 * device context). */
typedef struct dpm_endpoint {
    dpumesh_ctx_t *ctx;
    int            pod_id;
    int            slot_size;   /* cached max body size (avoids a per-write lib call) */
} dpm_t;

/* Connection — one request/response conversation (≈ an accepted/connected fd). */
typedef struct dpm_conn {
    dpm_t   *ep;
    int      is_server;        /* 1 = accepted request, 0 = client request */
    int32_t  peer_pod;         /* server: requester; client: target */
    uint32_t req_id;
    int      req_assigned;     /* client: req_id allocated yet? (0 is a valid id at wrap) */
    int8_t   req_flags;        /* server: inbound flags (mirrored onto the reply) */

    /* Inbound body (server: request, client: response). */
    int            rx_slot;    /* -1 = none */
    const uint8_t *rx_buf;
    uint32_t       rx_len;
    uint32_t       rx_pos;
    int            rx_ready;   /* body available to read? */

    /* Outbound body (server: response, client: request) — buffered until send. */
    int       tx_slot;         /* -1 = not yet allocated */
    uint8_t  *tx_buf;
    uint32_t  tx_len;
    int       sent;            /* enqueued? */
    int       pending;         /* register_pending outstanding? */

    void     *user_data;       /* like epoll_event.data.ptr — yours to use */
} dpmconn_t;

/* ===== Endpoint lifecycle (socket + bind + listen, folded) ===== */

/* Create an endpoint bound to `pod_id` with identity `app_name`. RX is
 * non-blocking poll-only (the transport has no cond-blocking path). Returns NULL
 * on failure (errno set by init). `pod_id` is overridden by env DPUMESH_POD_ID. */
static inline dpm_t *socket_dpm(const char *app_name, int pod_id) {
    dpm_t *s = (dpm_t *)calloc(1, sizeof(*s));
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

/* Destroy the endpoint and release all DOCA resources. */
static inline void destroy_dpm(dpm_t *s) {
    if (!s) return;
    if (s->ctx) dpumesh_destroy(s->ctx);
    free(s);
}

/* This endpoint's pod_id, and the configured max body size (slot_size). */
static inline int pod_id_dpm(dpm_t *s)    { return s->pod_id; }
static inline int msg_max_dpm(dpm_t *s)   { return s->slot_size; }

/* Readiness fd for NATIVE epoll/poll/select: becomes readable when an inbound
 * request/response is delivered. Register it like a listen socket; on wakeup,
 * drain it (one read() of a uint64_t) and accept_dpm()/read_dpm() the
 * ready work. Returns -1 if unavailable (fall back to polling accept_dpm).
 * Notification-driven — no busy-poll. */
static inline int event_fd_dpm(dpm_t *s) { return dpumesh_get_event_fd(s->ctx); }

/* ===== Connection setup ===== */

/* INTERNAL: build a server conn from a dequeued request descriptor. */
static inline dpmconn_t *server_conn_dpm(dpm_t *s, const sw_descriptor_t *req) {
    dpmconn_t *c = (dpmconn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; dpumesh_rx_free(s->ctx, req->body_buf_slot); return NULL; }
    c->ep        = s;
    c->is_server = 1;
    c->peer_pod  = req->src_pod_id;
    c->req_id    = req->req_id;
    c->req_flags = req->flags;
    c->rx_slot   = req->body_buf_slot;
    c->rx_buf    = dpumesh_rx_buf(s->ctx, req->body_buf_slot);
    c->rx_len    = req->body_len;
    c->rx_pos    = 0;
    c->rx_ready  = 1;          /* request body is already here (atomic) */
    c->tx_slot   = -1;
    return c;
}

/* accept(): NON-BLOCKING. Returns a new server connection holding the next
 * incoming request (body already available via read_dpm), or NULL with
 * errno=EAGAIN if no request is pending. A NULL with errno!=EAGAIN (ENOMEM) is
 * a rare conn-alloc failure: the dequeued request is dropped and its RX slot
 * reclaimed (no leak). An accept-until-NULL loop treats it as "drained", which
 * is safe — it just skips that one request. */
static inline dpmconn_t *accept_dpm(dpm_t *s) {
    sw_descriptor_t req;
    if (dpumesh_dequeue(s->ctx, &req, 0) < 0 || !req.valid) {
        errno = EAGAIN;
        return NULL;
    }
    return server_conn_dpm(s, &req);
}

/* connect(): create a client connection to `dst_pod_id`. No round-trip — this
 * only binds the target. Returns NULL on OOM. */
static inline dpmconn_t *connect_dpm(dpm_t *s, int dst_pod_id) {
    dpmconn_t *c = (dpmconn_t *)calloc(1, sizeof(*c));
    if (!c) { errno = ENOMEM; return NULL; }
    c->ep       = s;
    c->is_server = 0;
    c->peer_pod = (int32_t)dst_pod_id;
    c->rx_slot  = -1;
    c->tx_slot  = -1;
    return c;
}

/* ===== Per-connection user data (mirrors epoll_event.data.ptr) ===== */
static inline void  set_data_dpm(dpmconn_t *c, void *p) { c->user_data = p; }
static inline void *get_data_dpm(dpmconn_t *c)          { return c->user_data; }
static inline int   is_server_dpm(dpmconn_t *c)         { return c->is_server; }
static inline int32_t peer_dpm(dpmconn_t *c)            { return c->peer_pod; }

/* INTERNAL: poll a client conn for its response. 1=ready (rx populated),
 * 0=not yet, -1=abandoned. */
static inline int client_poll_dpm(dpmconn_t *c) {
    if (c->rx_ready) return 1;
    if (!c->sent)    return 0;          /* nothing requested yet */
    sw_descriptor_t resp;
    int r = dpumesh_poll_response(c->ep->ctx, c->req_id, &resp);
    if (r == 1) return 0;
    if (r < 0)  return -1;
    c->rx_slot  = resp.body_buf_slot;
    c->rx_buf   = dpumesh_rx_buf(c->ep->ctx, resp.body_buf_slot);
    c->rx_len   = resp.body_len;
    c->rx_pos   = 0;
    c->rx_ready = 1;
    c->tx_slot  = -1;            /* poll_response already freed the TX slot */
    c->pending  = 0;            /* and consumed the pending entry          */
    return 1;
}

/* ===== read / write / send ===== */

/* flush_dpm() is defined below; forward-declared here for autoflush_dpm(). */
static inline int flush_dpm(dpmconn_t *c);

/* INTERNAL: implicitly transmit a buffered-but-unsent outbound message. This is
 * what lets the socket-style write->read (client) and write->close patterns work
 * with NO explicit flush_dpm() call. No-op if nothing is buffered (tx_slot<0)
 * or it was already sent. Returns flush_dpm()'s result (0 ok; -1 errno=EAGAIN
 * on a transient the caller's next read retries). */
static inline int autoflush_dpm(dpmconn_t *c) {
    if (c->sent || c->tx_slot < 0) return 0;
    return flush_dpm(c);
}

/* read(): copy inbound body bytes into buf (advancing the read cursor). On a
 * client conn it first auto-flushes any buffered request (implicit send), so
 * write_dpm()+read_dpm() needs no explicit flush_dpm() call.
 *   >0 = bytes copied
 *    0 = end of message (whole body consumed)
 *   -1 = would-block (client: response not arrived; errno=EAGAIN) OR
 *        abandoned (errno=ECONNRESET).
 * NB: on the client path ECONNRESET means this req_id's pending entry was
 * reclaimed/aliased (a req_id-table collision) — it is NOT peer death. A dead
 * or unregistered dst_pod_id is never detected: its response simply never
 * arrives, so read_dpm stays EAGAIN forever (apply your own wall-clock
 * timeout). */
static inline ssize_t read_dpm(dpmconn_t *c, void *buf, size_t len) {
    if (!c->rx_ready) {
        if (!c->is_server && autoflush_dpm(c) < 0) return -1;   /* errno set by send */
        int r = client_poll_dpm(c);
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

/* INTERNAL: ensure a TX slot is allocated. 0 ok, -1 backpressure (errno=EAGAIN). */
static inline int tx_ensure_dpm(dpmconn_t *c) {
    if (c->tx_slot >= 0) return 0;
    int slot = dpumesh_tx_alloc(c->ep->ctx);
    if (slot < 0) { errno = EAGAIN; return -1; }   /* TX pool exhausted */
    c->tx_slot = slot;
    c->tx_buf  = dpumesh_tx_buf(c->ep->ctx, slot);
    c->tx_len  = 0;
    return 0;
}

/* INTERNAL: reset a CLIENT conn for a new request after its previous exchange
 * completed (response delivered + read). Frees the prior response's RX slot and
 * clears the per-exchange state; the peer binding is kept and the next flush takes
 * a fresh req_id. This is what makes a conn REUSABLE — connect once, then loop
 * write -> read -> write -> read ..., and close at the end. */
static inline void conn_reset_exchange(dpmconn_t *c) {
    if (c->rx_slot >= 0) dpumesh_rx_free(c->ep->ctx, c->rx_slot);
    c->rx_slot = -1; c->rx_buf = NULL; c->rx_len = 0; c->rx_pos = 0; c->rx_ready = 0;
    c->sent = 0; c->req_assigned = 0; c->pending = 0;
    /* tx_slot is already -1 (handed off at flush); a fresh one is taken on write. */
}

/* INTERNAL: begin building a new outbound message. If the conn already sent its
 * message, a CLIENT whose response has been delivered auto-resets for a new
 * request (sequential reuse); otherwise reject with EINVAL — one conn carries one
 * outstanding exchange at a time (a write before the response arrives, or a 2nd
 * reply on a server conn). Then ensure a TX slot. 0 ok; -1 (EINVAL / EAGAIN). */
static inline int conn_begin_tx(dpmconn_t *c) {
    if (c->sent) {
        if (!c->is_server && c->rx_ready) conn_reset_exchange(c);
        else { errno = EINVAL; return -1; }
    }
    return tx_ensure_dpm(c);
}

/* write(): BUFFER outbound body bytes (transmitted later by flush_dpm()). On a
 * client conn whose previous response was already read, the first write of the
 * next message auto-starts a NEW request (reuse — see conn_begin_tx).
 *   >0 = bytes buffered (always == len on success)
 *   -1 = message would exceed slot_size (errno=EMSGSIZE), or a write while a
 *        request is still outstanding / on an already-sent server conn
 *        (errno=EINVAL). Acquiring a TX slot busy-spins under saturation. */
static inline ssize_t write_dpm(dpmconn_t *c, const void *buf, size_t len) {
    if (conn_begin_tx(c) < 0) return -1;
    int cap = c->ep->slot_size;
    if (c->tx_len + len > (uint32_t)cap) { errno = EMSGSIZE; return -1; }
    memcpy(c->tx_buf + c->tx_len, buf, len);
    c->tx_len += (uint32_t)len;
    return (ssize_t)len;
}

/* sendfile(): append up to `count` bytes from in_fd into the outbound body
 * (still capped at slot_size). If `offset` is non-NULL the file is read from
 * *offset and *offset is advanced. Returns bytes appended (0 at EOF), -1 on
 * error/backpressure. Ship with flush_dpm() afterwards. */
static inline ssize_t sendfile_dpm(dpmconn_t *c, int in_fd, off_t *offset, size_t count) {
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

/* send(): transmit the buffered outbound message (client: request; server:
 * response, matched to the inbound req_id). USUALLY OPTIONAL for the happy path —
 * read_dpm() (client) and close_dpm() (server) auto-flush. Call it explicitly
 * when you need to CONFIRM or RETRY delivery: it returns the send status (0/-1)
 * and a transient -1 (EAGAIN) can be retried with the body still buffered —
 * whereas close_dpm()'s auto-flush can only REPORT a failure (the conn is freed,
 * so it can't be retried). Also use it to flush early (e.g. a pipelined async
 * client that sends now and reads the response later). After send, a client conn awaits
 * its response (read_dpm, optionally via native epoll on event_fd_dpm);
 * a server conn is complete. A CLIENT conn is REUSABLE after its response is read
 * (the next write starts a new request); a conn carries one outstanding exchange
 * at a time.
 *   0  = sent
 *  -1, errno=EINVAL  = conn already sent (single-shot) — NOT retryable.
 *  -1, errno=EAGAIN  = req_id pending-table collision. RETRYABLE (body kept),
 *        but this errno is returned only AFTER a ~2s hard wait inside
 *        register_pending — it is NOT retry-soon backpressure, and a retry with
 *        the same req_id may block another ~2s until the colliding slot frees.
 *        Rare (only when in-flight ≥ MAX_PENDING).
 *  -1, errno=EBADMSG = permanent descriptor-validation fault from enqueue —
 *        NOT retryable; close the conn. Unreachable via the façade.
 * The TX-slot / DMA-ring acquisition busy-spins, it never fails with -1. */
static inline int flush_dpm(dpmconn_t *c) {
    dpumesh_ctx_t *ctx = c->ep->ctx;
    if (c->sent) { errno = EINVAL; return -1; }
    if (tx_ensure_dpm(c) < 0) return -1;          /* allow zero-length messages */

    if (!c->is_server && !c->req_assigned) {
        c->req_id = dpumesh_alloc_req_id(ctx);
        c->req_assigned = 1;
    }

    if (!c->pending) {
        if (dpumesh_register_pending(ctx, c->req_id) < 0) { errno = EAGAIN; return -1; }
        c->pending = 1;
    }

    sw_descriptor_t d;
    memset(&d, 0, sizeof(d));
    d.header_buf_slot = -1;
    d.header_len      = 0;
    d.body_buf_slot   = c->tx_slot;
    d.body_len        = c->tx_len;
    d.req_id          = c->req_id;
    d.dst_pod_id      = c->peer_pod;
    d.src_pod_id      = c->ep->pod_id;
    d.flags           = c->is_server ? (int8_t)((c->req_flags & ~OP_REQUEST) | OP_RESPONSE)
                                     : (int8_t)(OP_REQUEST | CASE_EXTERNAL);
    d.valid           = 1;

    if (dpumesh_enqueue(ctx, &d) < 0) {
        /* enqueue only fails on a PERMANENT descriptor-validation fault (null
         * desc / slot out of range / body_len > slot_size); ring saturation
         * busy-spins instead. So this is NOT retryable — report EBADMSG, not
         * EAGAIN. (Unreachable via the façade: write_dpm caps the body at
         * slot_size and tx_alloc always yields a valid slot.) */
        dpumesh_cancel_pending(ctx, c->req_id);
        c->pending = 0;
        errno = EBADMSG;
        return -1;
    }
    dpumesh_pending_attach_tx(ctx, c->req_id, c->tx_slot);

    if (c->is_server) {
        /* fire-and-forget: TX_ACK frees the TX slot, no response expected */
        dpumesh_pending_release_async(ctx, c->req_id);
        c->pending = 0;
    }
    /* TX ownership handed to the pending/TX_ACK path. */
    c->tx_slot = -1;
    c->tx_buf  = NULL;
    c->tx_len  = 0;
    c->sent    = 1;
    return 0;
}

/* close(): ship any buffered-but-unsent message (e.g. a server response built
 * with write_dpm()), then release the conn's slots/pending and free it. Safe on
 * NULL.
 *
 * ONE-WAY (fire-and-forget): a CLIENT that does write -> close WITHOUT a read
 * sends the request and does not wait for a response — the TX slot is freed by
 * the DPU's TX_ACK (no leak) and any reply the peer sends is silently dropped.
 *   0  = ok (message shipped, or nothing was buffered)
 *  -1  = the buffered message FAILED to ship (rare — e.g. a req_id collision).
 *        The conn is freed EITHER WAY, so -1 means "not delivered and NOT
 *        retryable on this conn." For guaranteed delivery, call flush_dpm()
 *        explicitly (and retry on EAGAIN) BEFORE close_dpm(). */
static inline int close_dpm(dpmconn_t *c) {
    if (!c) return 0;
    int flushed = autoflush_dpm(c);   /* 0 = shipped / nothing buffered; -1 = ship failed */
    dpumesh_ctx_t *ctx = c->ep->ctx;
    if (c->rx_slot >= 0)            dpumesh_rx_free(ctx, c->rx_slot);
    if (c->tx_slot >= 0 && !c->sent) dpumesh_tx_free(ctx, c->tx_slot);   /* unsent */
    if (c->pending)                 dpumesh_cancel_pending(ctx, c->req_id);
    free(c);
    return flushed;
}

/* ===== Event-loop integration =====
 * There are NO epoll_*_dpm wrappers. Use NATIVE kernel epoll/poll/select and
 * register event_fd_dpm(s) like a listen socket:
 *
 *   int dfd = event_fd_dpm(s);                 // -1 if unavailable
 *   epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);      // ev.events = EPOLLIN
 *   ... epoll_wait(epfd, events, n, timeout) ...   // sleeps until inbound activity
 *   // on a dfd event: drain it, then accept the pending request(s):
 *   uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}   // drain (EAGAIN when empty)
 *   dpmconn_t *c; while ((c = accept_dpm(s)) != NULL) { handle(c); }
 *
 * The fd is raised per delivery and drained by the read(); a level-triggered
 * EPOLLIN plus the accept-until-EAGAIN loop processes every queued request. dfd
 * mixes freely with real sockets in the same epoll set. */

#ifdef __cplusplus
}
#endif

#endif /* DPM_H */
