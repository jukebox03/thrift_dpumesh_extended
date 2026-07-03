/*
 * dmesh_preload.c — LD_PRELOAD socket-compatibility shim over the DPUmesh
 * native API (dpm.h). Lets an UNMODIFIED, dynamically-linked POSIX socket
 * application run over DPUmesh: selected TCP connects/listens are transparently
 * backed by dmesh connections; every other fd passes through to the kernel.
 *
 * LAYERING (see plan.md): the native dmesh_* API stays the optimized product;
 * this shim is a compatibility layer that deliberately re-buys the per-conn-fd
 * readiness model (measured ~half the native ceiling) in exchange for POSIX
 * transparency. Nothing here is on the native hot path.
 *
 * ACTIVATION (env; everything else is untouched kernel TCP):
 *   DMESH_PRELOAD_LISTEN=<port>          listen() on this TCP port becomes the
 *                                        dmesh service listener
 *   DMESH_PRELOAD_SVC=<svc>              service_id this process advertises
 *                                        (required with LISTEN; absent = pure client)
 *   DMESH_PRELOAD_MAP=<port>=<svc>[,..]  connect() to these TCP ports → dmesh
 *   DMESH_PRELOAD_DEBUG=1                stderr diagnostics
 *
 * FD REALIZATION — the design keystone: when a socket becomes dmesh-backed, the
 * shim creates a PRIVATE eventfd and dup2()s it over the app's fd number. The
 * app's fd is then a real kernel fd (same open file description as the private
 * one), so epoll/poll/select/close/dup ALL work natively — none of them are
 * interposed. The dispatcher asserts readability by writing the private eventfd;
 * the read path drains it at EAGAIN edges.
 *
 * ORDERING — a socket promises byte-stream total order on a connection, but
 * DPUmesh load-balances PER MESSAGE (replies can interleave across backends).
 * Every shim client conn is therefore dmesh_pin_route()'d: all its messages
 * (and its FIN) carry one route-affinity group, so the DPU pins the whole conn
 * to the backend picked for its first message — connection-level LB, exactly
 * what a TCP proxy gives you.
 *
 * THREAD MODEL — api.md contract: dmesh_accept/dmesh_next_ready are single-
 * consumer. The shim's dispatcher thread is that consumer; it also EXCLUSIVELY
 * runs dmesh_close (app close() only queues), so a ready-list pop can never
 * race a conn free. App threads touch only their own conns (per-entry mutex).
 *
 * LOST-WAKEUP DISCIPLINE — the ready list re-arms only on an inbox empty→
 * non-empty edge, so: (a) every successful read RE-ASSERTS the eventfd (over-
 * asserting is safe: one spurious wake, then the EAGAIN path drains); (b) the
 * EAGAIN path drains the eventfd and RETRIES once, closing the window where a
 * signal lands between the failed read and the drain. Signals can be delayed
 * one hop but never lost.
 *
 * KNOWN LIMITS (v1, documented): no fork-shared sockets (DOCA is not fork-
 * safe), half-close is approximate (a FIN tears the upstream down — replies
 * after shutdown(SHUT_WR) are undeliverable), most SO_* options are accepted
 * no-ops, AF_INET SOCK_STREAM only, one dmesh listener per process. BYPASSES:
 * Go binaries (raw syscalls) and stdio FILE* wrappers over a socket (glibc
 * stdio calls its internal __read/__write, not the PLT) never enter the shim.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <netinet/in.h>

#include "thrift/transport/dpm.h"

/* ================= real libc entry points (lazy dlsym) ================= */

#define REAL_DECL(name, ret, ...) \
    static ret (*real_##name)(__VA_ARGS__); \
    static void resolve_##name(void) { real_##name = dlsym(RTLD_NEXT, #name); }

REAL_DECL(socket,      int,     int, int, int)
REAL_DECL(connect,     int,     int, const struct sockaddr *, socklen_t)
REAL_DECL(bind,        int,     int, const struct sockaddr *, socklen_t)
REAL_DECL(listen,      int,     int, int)
REAL_DECL(accept,      int,     int, struct sockaddr *, socklen_t *)
REAL_DECL(accept4,     int,     int, struct sockaddr *, socklen_t *, int)
REAL_DECL(read,        ssize_t, int, void *, size_t)
REAL_DECL(write,       ssize_t, int, const void *, size_t)
REAL_DECL(recv,        ssize_t, int, void *, size_t, int)
REAL_DECL(send,        ssize_t, int, const void *, size_t, int)
REAL_DECL(recvfrom,    ssize_t, int, void *, size_t, int, struct sockaddr *, socklen_t *)
REAL_DECL(sendto,      ssize_t, int, const void *, size_t, int, const struct sockaddr *, socklen_t)
REAL_DECL(recvmsg,     ssize_t, int, struct msghdr *, int)
REAL_DECL(sendmsg,     ssize_t, int, const struct msghdr *, int)
REAL_DECL(readv,       ssize_t, int, const struct iovec *, int)
REAL_DECL(writev,      ssize_t, int, const struct iovec *, int)
REAL_DECL(close,       int,     int)
REAL_DECL(shutdown,    int,     int, int)
REAL_DECL(fcntl,       int,     int, int, ...)
REAL_DECL(ioctl,       int,     int, unsigned long, ...)
REAL_DECL(getsockopt,  int,     int, int, int, void *, socklen_t *)
REAL_DECL(setsockopt,  int,     int, int, int, const void *, socklen_t)
REAL_DECL(getsockname, int,     int, struct sockaddr *, socklen_t *)
REAL_DECL(getpeername, int,     int, struct sockaddr *, socklen_t *)
REAL_DECL(dup,         int,     int)
REAL_DECL(dup2,        int,     int, int)
REAL_DECL(dup3,        int,     int, int, int)
REAL_DECL(sendfile,    ssize_t, int, int, off_t *, size_t)
/* LFS aliases: may be absent from libc (dlsym NULL) → fall back to the plain form. */
REAL_DECL(fcntl64,     int,     int, int, ...)
REAL_DECL(sendfile64,  ssize_t, int, int, off_t *, size_t)

/* variadic real fcntl/ioctl need the va-form; glibc's are (int, int/ulong, arg) */
static pthread_once_t g_resolve_once = PTHREAD_ONCE_INIT;
static void resolve_all(void) {
    resolve_socket(); resolve_connect(); resolve_bind(); resolve_listen();
    resolve_accept(); resolve_accept4(); resolve_read(); resolve_write();
    resolve_recv(); resolve_send(); resolve_recvfrom(); resolve_sendto();
    resolve_recvmsg(); resolve_sendmsg(); resolve_readv(); resolve_writev();
    resolve_close(); resolve_shutdown(); resolve_fcntl(); resolve_ioctl();
    resolve_getsockopt(); resolve_setsockopt(); resolve_getsockname();
    resolve_getpeername(); resolve_dup(); resolve_dup2(); resolve_dup3();
    resolve_sendfile(); resolve_fcntl64(); resolve_sendfile64();
}
#define ENSURE_REAL() pthread_once(&g_resolve_once, resolve_all)

/* ============================ configuration ============================ */

#define PRELOAD_MAX_MAP 16

static int      g_debug;
static int      g_listen_port = -1;      /* DMESH_PRELOAD_LISTEN */
static int      g_svc         = -12345;  /* DMESH_PRELOAD_SVC (sentinel = unset) */
static int      g_map_n;
static uint16_t g_map_port[PRELOAD_MAX_MAP];
static int      g_map_svc[PRELOAD_MAX_MAP];

#define DBG(...) do { if (g_debug) { fprintf(stderr, "[dmesh_preload] " __VA_ARGS__); fputc('\n', stderr); } } while (0)

__attribute__((constructor))
static void preload_ctor(void) {
    const char *e;
    g_debug = (e = getenv("DMESH_PRELOAD_DEBUG")) && atoi(e);
    if ((e = getenv("DMESH_PRELOAD_LISTEN"))) g_listen_port = atoi(e);
    if ((e = getenv("DMESH_PRELOAD_SVC")))    g_svc = atoi(e);
    if ((e = getenv("DMESH_PRELOAD_MAP"))) {
        char buf[512]; strncpy(buf, e, sizeof buf - 1); buf[sizeof buf - 1] = 0;
        for (char *save = NULL, *tok = strtok_r(buf, ",", &save);
             tok && g_map_n < PRELOAD_MAX_MAP; tok = strtok_r(NULL, ",", &save)) {
            char *eq = strchr(tok, '=');
            if (!eq) continue;
            *eq = 0;
            g_map_port[g_map_n] = (uint16_t)atoi(tok);
            g_map_svc[g_map_n]  = atoi(eq + 1);
            g_map_n++;
        }
    }
    DBG("ctor: listen=%d svc=%d map_n=%d", g_listen_port, g_svc, g_map_n);
}

static int map_lookup(uint16_t port) {
    for (int i = 0; i < g_map_n; i++)
        if (g_map_port[i] == port) return g_map_svc[i];
    return -1;
}

/* ============================== fd table =============================== */

#define PRELOAD_MAX_FDS 65536

typedef struct pfd {
    dmesh_conn_t *conn;        /* NULL for the listener entry */
    int  efd;                  /* PRIVATE eventfd (CLOEXEC); app fds are kernel dups */
    int  listener;
    int  nonblock;
    int  wr_closed;
    int  rd_closed;
    int  refs;                 /* app fd aliases (dup); private efd NOT counted */
    int  closing;              /* queued for dispatcher dmesh_close */
    long rcv_timeout_ms;       /* SO_RCVTIMEO; 0 = block forever */
    uint16_t lport;            /* synthesized getsockname port */
    uint16_t pport;            /* synthesized getpeername port */
    pthread_mutex_t mu;
    struct pfd *q_next;        /* accept- / close-queue linkage */
} pfd_t;

static pfd_t *g_fds[PRELOAD_MAX_FDS];
static pthread_mutex_t g_tbl_mu = PTHREAD_MUTEX_INITIALIZER;

static pfd_t *pfd_get(int fd) {
    if (fd < 0 || fd >= PRELOAD_MAX_FDS) return NULL;
    return g_fds[fd];                    /* torn-read safe: pointer store/load */
}

static pfd_t *pfd_new(dmesh_conn_t *c) {
    pfd_t *e = calloc(1, sizeof(*e));
    if (!e) return NULL;
    e->conn = c;
    e->efd  = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (e->efd < 0) { free(e); return NULL; }
    pthread_mutex_init(&e->mu, NULL);
    return e;
}

static void efd_signal(pfd_t *e) {
    uint64_t one = 1;
    ssize_t r = real_write(e->efd, &one, sizeof one);
    (void)r;                             /* counter overflow (impossible here) only */
}

static void efd_drain(pfd_t *e) {
    uint64_t v;
    while (real_read(e->efd, &v, sizeof v) > 0) {}
}

/* ===================== channel + dispatcher thread ===================== */

static dmesh_channel_t *g_ch;
static int  g_wake_fd = -1;              /* wakes the dispatcher for the close queue */
static pfd_t *g_listener;                /* the (single) dmesh listener entry */
static int  g_listener_closed;           /* a listener existed and was closed: inbound
                                          * conns can never be accepted → close them at
                                          * wrap instead of queueing forever. Distinct
                                          * from "not listening YET" (NULL + flag 0),
                                          * where pre-listen conns legitimately queue. */
static pthread_mutex_t g_q_mu = PTHREAD_MUTEX_INITIALIZER;
static pfd_t *g_accept_head, *g_accept_tail;   /* dispatcher → accept() */
static pfd_t *g_close_head;                    /* close() → dispatcher */

#define FREE_GRACE 256                          /* delayed-free ring (see reap) */
static pfd_t *g_grace[FREE_GRACE];
static int g_grace_i;

static void accept_q_push(pfd_t *e) {
    pthread_mutex_lock(&g_q_mu);
    e->q_next = NULL;
    if (g_accept_tail) g_accept_tail->q_next = e; else g_accept_head = e;
    g_accept_tail = e;
    pthread_mutex_unlock(&g_q_mu);
}

static pfd_t *accept_q_pop(void) {
    pthread_mutex_lock(&g_q_mu);
    pfd_t *e = g_accept_head;
    if (e) {
        g_accept_head = e->q_next;
        if (!g_accept_head) g_accept_tail = NULL;
        e->q_next = NULL;
    }
    pthread_mutex_unlock(&g_q_mu);
    return e;
}

static void close_q_push(pfd_t *e) {
    pthread_mutex_lock(&g_q_mu);
    e->q_next = g_close_head;
    g_close_head = e;
    pthread_mutex_unlock(&g_q_mu);
    uint64_t one = 1;
    ssize_t r = real_write(g_wake_fd, &one, sizeof one);
    (void)r;
}

static void *dispatcher_main(void *arg) {
    (void)arg;
    int ch_fd = dmesh_event_fd(g_ch);    /* enables readiness delivery (api.md) */
    struct pollfd pfds[2] = {
        { .fd = ch_fd,     .events = POLLIN },
        { .fd = g_wake_fd, .events = POLLIN },
    };
    DBG("dispatcher up (ch_fd=%d wake_fd=%d)", ch_fd, g_wake_fd);
    for (;;) {
        (void)poll(pfds, 2, -1);

        if (pfds[0].revents & POLLIN) {
            uint64_t v;
            while (real_read(ch_fd, &v, sizeof v) > 0) {}

            /* New inbound connections → wrap + queue + signal the listener. */
            dmesh_conn_t *c;
            for (;;) {
                errno = 0;
                c = dmesh_accept(g_ch);
                if (!c) {
                    /* EAGAIN = drained. ENOMEM = dmesh_accept silently DROPPED a
                     * pending first message (alloc failure or a duplicate/reused-uP
                     * accept) — the peer will hang waiting; make it visible, and keep
                     * draining (one queue entry was consumed; more may be pending). */
                    if (errno == ENOMEM) {
                        fprintf(stderr, "[dmesh_preload] accept DROPPED a pending conn "
                                        "(ENOMEM/dup-uP)\n");
                        continue;
                    }
                    break;
                }
                if (g_listener == NULL && g_listener_closed) {
                    dmesh_close(c);              /* listener gone for good → FIN back */
                    continue;
                }
                pfd_t *e = pfd_new(c);
                if (!e) { dmesh_close(c); continue; }
                e->pport = c->remote_port;
                e->lport = c->local_port;
                c->user_data = e;
                /* dmesh_accept returns the conn HOLDING its first message (plus
                 * any coalesced pipelined ones) — that delivery predates this
                 * entry, so the ready list will never re-edge for it. Assert
                 * readability up front or an epoll app never reads it. */
                efd_signal(e);
                accept_q_push(e);
                pfd_t *l = g_listener;
                if (l) efd_signal(l);
                DBG("accepted conn (peer pod=%d port=%u)", c->remote_pod, c->remote_port);
            }

            /* Conns with inbound → assert their eventfd. user_data is set by
             * THIS thread (accept above) or before first send (connect), so a
             * ready conn always carries its entry. */
            while ((c = dmesh_next_ready(g_ch)) != NULL) {
                pfd_t *e = (pfd_t *)c->user_data;
                if (e) efd_signal(e);
            }
        }

        if (pfds[1].revents & POLLIN) {
            uint64_t v;
            while (real_read(g_wake_fd, &v, sizeof v) > 0) {}
            for (;;) {                    /* reap the close queue */
                pthread_mutex_lock(&g_q_mu);
                pfd_t *e = g_close_head;
                if (e) g_close_head = e->q_next;
                pthread_mutex_unlock(&g_q_mu);
                if (!e) break;
                if (e->listener) {
                    /* Nobody can accept the queued conns anymore — close them (FIN).
                     * Pushed to the close queue we are draining, so they are reaped
                     * in this very loop. Runs AFTER any racing accept-wrap above
                     * (same thread), so no conn can slip into the queue behind us. */
                    pfd_t *p;
                    while ((p = accept_q_pop()) != NULL) close_q_push(p);
                }
                pthread_mutex_lock(&e->mu);      /* serialize vs in-flight read/write */
                dmesh_conn_t *c2 = e->conn;
                e->conn = NULL;
                int fin_sent = e->wr_closed;     /* shutdown(SHUT_WR) already shipped a FIN */
                pthread_mutex_unlock(&e->mu);
                if (c2) {
                    /* Suppress dmesh_close's FIN when shutdown already sent one — a
                     * second FIN is harmless on the DPU (teardown fan-out finds no
                     * upstream) but wastes a TX slot + ACK round trip. peer_closed
                     * only gates the FIN; RX credits/port are still reclaimed. */
                    if (fin_sent) c2->peer_closed = 1;
                    dmesh_close(c2);              /* single-thread vs next_ready: safe */
                }
                real_close(e->efd);               /* immediately unblocks poll()ers */
                /* GRACE-DELAYED free: a thread blocked in read() on another
                 * thread's close() wakes from the efd close (POLLNVAL) and may
                 * still touch the entry (mu, conn==NULL → EOF). Deferring the
                 * free by a 256-reap ring makes that window safe in practice. */
                pfd_t *old = g_grace[g_grace_i];
                g_grace[g_grace_i] = e;
                g_grace_i = (g_grace_i + 1) % FREE_GRACE;
                if (old) { pthread_mutex_destroy(&old->mu); free(old); }
            }
        }
    }
    return NULL;
}

static pthread_mutex_t g_ch_mu = PTHREAD_MUTEX_INITIALIZER;

/* Create the channel + dispatcher. Called under g_ch_mu; leaves g_ch NULL on
 * failure so a later attempt RETRIES (a register timeout — e.g. the DPU came
 * up after us, or its pod table was momentarily full — must not latch a
 * long-lived process into permanent failure). */
static void channel_init(void) {
    /* Advertise DMESH_PRELOAD_SVC if set (server or mixed process); else a pure
     * client. One channel per process, created on first mapped socket op. */
    int svc = (g_svc != -12345) ? g_svc : DMESH_SVC_NONE;
    dmesh_channel_t *ch = dmesh_create_channel(svc);
    if (!ch) { DBG("dmesh_create_channel(%d) FAILED (will retry)", svc); return; }
    if (dmesh_event_fd(ch) < 0) {       /* no readiness fd → dispatcher would be deaf */
        dmesh_destroy_channel(ch);
        DBG("dmesh_event_fd unavailable (will retry)");
        return;
    }
    if (g_wake_fd < 0)                  /* kept across a failed attempt (no re-create leak) */
        g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    pthread_t t;
    g_ch = ch;                          /* dispatcher reads g_ch */
    if (g_wake_fd < 0 || pthread_create(&t, NULL, dispatcher_main, NULL) != 0) {
        g_ch = NULL;
        dmesh_destroy_channel(ch);
        DBG("dispatcher start FAILED");
        return;
    }
    pthread_detach(t);
    DBG("channel up: svc=%d pod=%d msg_max=%d", svc, dmesh_pod_id(ch), dmesh_msg_max(ch));
}

static int ensure_channel(void) {
    pthread_mutex_lock(&g_ch_mu);
    if (!g_ch) channel_init();
    int ok = (g_ch != NULL);
    pthread_mutex_unlock(&g_ch_mu);
    return ok ? 0 : -1;
}

/* Occupy the app's fd number with a kernel dup of the private eventfd. */
static int install_fd(int fd, pfd_t *e) {
    if (fd < 0 || fd >= PRELOAD_MAX_FDS) { errno = EMFILE; return -1; }  /* g_fds bound */
    if (real_dup2(e->efd, fd) < 0) return -1;    /* closes the old TCP socket */
    pthread_mutex_lock(&g_tbl_mu);
    g_fds[fd] = e;
    e->refs++;
    pthread_mutex_unlock(&g_tbl_mu);
    return 0;
}

/* ====================== blocking-wait helper ====================== */

static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Wait until the entry's eventfd is readable. timeout_ms 0 = forever.
 * Returns 0 on ready, -1 with errno=EAGAIN on timeout. */
static int wait_ready(pfd_t *e, long timeout_ms) {
    struct pollfd p = { .fd = e->efd, .events = POLLIN };
    int r = poll(&p, 1, timeout_ms > 0 ? (int)timeout_ms : -1);
    if (r > 0) return 0;
    errno = EAGAIN;
    return -1;
}

/* ========================= data-path helpers ========================== */

/* One receive attempt honoring the lost-wakeup discipline. Returns >0 bytes,
 * 0 EOF, or -1/EAGAIN (never blocks). Caller holds e->mu. */
static ssize_t rx_once(pfd_t *e, void *buf, size_t len, int peek) {
    dmesh_conn_t *c = e->conn;
    if (!c || e->rd_closed) return 0;

    if (peek) {
        if (c->rx_slot < 0) {                     /* load the next message, consume 0 */
            char dummy;
            ssize_t l = dmesh_read(c, &dummy, 0);
            if (l < 0) return -1;                 /* EAGAIN */
            if (c->peer_closed) return 0;         /* the load hit the FIN */
        }
        size_t avail = c->rx_len - c->rx_pos;
        size_t n = len < avail ? len : avail;
        if (n) memcpy(buf, c->rx_buf + c->rx_pos, n);
        if (n) efd_signal(e);                     /* data still pending → stay readable */
        return (ssize_t)n;
    }

    ssize_t n = dmesh_read(c, buf, len);
    if (n > 0) {
        efd_signal(e);        /* re-assert: more may be pending; over-assert is safe */
        return n;
    }
    if (n == 0) return 0;                          /* EOF (peer FIN) */

    /* EAGAIN: drain the eventfd, then retry ONCE — closes the race where the
     * dispatcher signaled between the failed read and the drain. */
    efd_drain(e);
    n = dmesh_read(c, buf, len);
    if (n > 0) { efd_signal(e); return n; }
    if (n == 0) return 0;
    errno = EAGAIN;
    return -1;
}

static ssize_t shim_recv(pfd_t *e, void *buf, size_t len, int flags) {
    if (e->listener) { errno = ENOTCONN; return -1; }
    int peek    = flags & MSG_PEEK;
    int waitall = flags & MSG_WAITALL;
    int block   = !(e->nonblock || (flags & MSG_DONTWAIT));
    /* SO_RCVTIMEO caps the WHOLE call, not each wait — partial wakes must not
     * restart the clock. 0 = block forever. */
    long deadline = (block && e->rcv_timeout_ms > 0) ? now_ms() + e->rcv_timeout_ms : 0;
    size_t got = 0;

    for (;;) {
        pthread_mutex_lock(&e->mu);
        ssize_t n = rx_once(e, (char *)buf + got, len - got, peek);
        pthread_mutex_unlock(&e->mu);

        if (n > 0) {
            got += (size_t)n;
            if (peek || !waitall || got >= len) return (ssize_t)got;
            continue;                              /* MSG_WAITALL: keep collecting */
        }
        if (n == 0) return (ssize_t)got;           /* EOF: return what we have */
        if (got > 0 && !waitall) return (ssize_t)got;
        if (!block) { errno = EAGAIN; return -1; }
        long left = 0;                             /* 0 = forever */
        if (deadline) {
            left = deadline - now_ms();
            if (left <= 0) left = -1;              /* already expired */
        }
        if (left < 0 || wait_ready(e, left) < 0) {
            if (got > 0) return (ssize_t)got;
            errno = EAGAIN;                        /* SO_RCVTIMEO expiry */
            return -1;
        }
    }
}

/* Gather-send: ALL iovs accumulate into ONE message (dmesh_write buffers and
 * auto-chunks any length; a pinned conn keeps chunks on its backend), shipped by
 * a single flush — so writev(header, body) costs one message, not two. Slot
 * exhaustion (EAGAIN) is retried here — TCP send() has no partial-then-remember
 * contract we could map it to. */
static ssize_t shim_send_iov(pfd_t *e, const struct iovec *iov, int cnt) {
    if (e->listener) { errno = ENOTCONN; return -1; }
    size_t total = 0;
    for (int i = 0; i < cnt; i++) total += iov[i].iov_len;
    if (total == 0) return 0;

    pthread_mutex_lock(&e->mu);
    dmesh_conn_t *c = e->conn;
    if (!c || e->wr_closed) {
        pthread_mutex_unlock(&e->mu);
        errno = EPIPE;
        return -1;
    }
    size_t sent = 0;
    for (int i = 0; i < cnt; i++) {
        const char *p = (const char *)iov[i].iov_base;
        size_t len = iov[i].iov_len, done = 0;
        while (done < len) {
            ssize_t w = dmesh_write(c, p + done, len - done);
            if (w > 0) { done += (size_t)w; continue; }
            if (errno != EAGAIN) {
                dmesh_flush(c);                    /* best-effort: ship what buffered */
                pthread_mutex_unlock(&e->mu);
                errno = ECONNRESET;
                sent += done;
                return sent ? (ssize_t)sent : -1;
            }
            sched_yield();                         /* TX pool momentarily empty */
        }
        sent += done;
    }
    int fr = dmesh_flush(c);
    pthread_mutex_unlock(&e->mu);
    if (fr < 0) { errno = ECONNRESET; return -1; }
    return (ssize_t)total;
}

static ssize_t shim_send(pfd_t *e, const void *buf, size_t len, int flags) {
    (void)flags;
    struct iovec iov = { (void *)buf, len };
    return shim_send_iov(e, &iov, 1);
}

/* ========================= socket-call surface ========================= */

int connect(int fd, const struct sockaddr *addr, socklen_t alen) {
    ENSURE_REAL();
    if (pfd_get(fd)) { errno = EISCONN; return -1; }
    if (g_map_n > 0 && addr && addr->sa_family == AF_INET && alen >= sizeof(struct sockaddr_in)) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)addr;
        int svc = map_lookup(ntohs(sin->sin_port));
        if (svc >= 0) {
            /* AF_INET SOCK_STREAM only (api.md §7) — a UDP connect() to a mapped
             * port must stay kernel. */
            int so_type = 0; socklen_t tl = sizeof so_type;
            if (real_getsockopt(fd, SOL_SOCKET, SO_TYPE, &so_type, &tl) < 0 ||
                so_type != SOCK_STREAM)
                return real_connect(fd, addr, alen);
            if (ensure_channel() < 0) { errno = ENETUNREACH; return -1; }
            dmesh_conn_t *c = dmesh_connect(g_ch, svc);
            if (!c) return -1;                     /* ENOMEM */
            dmesh_pin_route(c);                    /* socket contract: one backend, total order */
            pfd_t *e = pfd_new(c);
            if (!e) { dmesh_close(c); errno = ENOMEM; return -1; }
            /* preserve O_NONBLOCK the app may have set on the TCP socket */
            int fl = real_fcntl(fd, F_GETFL);
            e->nonblock = (fl >= 0 && (fl & O_NONBLOCK)) ? 1 : 0;
            e->lport = c->local_port;
            e->pport = ntohs(sin->sin_port);
            c->user_data = e;                      /* before any send → before any ready */
            if (install_fd(fd, e) < 0) {
                dmesh_close(c); real_close(e->efd); free(e);
                errno = ENOMEM;
                return -1;
            }
            DBG("connect fd=%d port=%u → svc=%d (pinned)", fd, e->pport, svc);
            return 0;                              /* local: immediate success is legal */
        }
    }
    return real_connect(fd, addr, alen);
}

int bind(int fd, const struct sockaddr *addr, socklen_t alen) {
    ENSURE_REAL();
    /* Pass through — the real bind also reserves the port so a half-configured
     * deployment fails loudly instead of silently double-serving. listen()
     * decides on conversion by re-reading the bound port. */
    return real_bind(fd, addr, alen);
}

int listen(int fd, int backlog) {
    ENSURE_REAL();
    if (g_listen_port >= 0 && g_svc != -12345 && !pfd_get(fd)) {
        struct sockaddr_in sin; socklen_t sl = sizeof sin;
        if (real_getsockname(fd, (struct sockaddr *)&sin, &sl) == 0 &&
            sin.sin_family == AF_INET && ntohs(sin.sin_port) == (uint16_t)g_listen_port) {
            if (ensure_channel() < 0) { errno = EADDRNOTAVAIL; return -1; }
            pfd_t *e = pfd_new(NULL);
            if (!e) { errno = ENOMEM; return -1; }
            e->listener = 1;
            int fl = real_fcntl(fd, F_GETFL);
            e->nonblock = (fl >= 0 && (fl & O_NONBLOCK)) ? 1 : 0;
            e->lport = (uint16_t)g_listen_port;
            if (install_fd(fd, e) < 0) {
                real_close(e->efd); free(e);
                errno = ENOMEM;
                return -1;
            }
            g_listener = e;
            g_listener_closed = 0;                /* re-listen resumes queueing */
            pthread_mutex_lock(&g_q_mu);          /* conns that arrived pre-listen */
            int pending = g_accept_head != NULL;
            pthread_mutex_unlock(&g_q_mu);
            if (pending) efd_signal(e);
            DBG("listen fd=%d port=%d → dmesh svc=%d", fd, g_listen_port, g_svc);
            return 0;
        }
    }
    return real_listen(fd, backlog);
}

static int shim_accept(int fd, struct sockaddr *addr, socklen_t *alen, int flags) {
    pfd_t *l = pfd_get(fd);
    if (!l) return -2;                             /* not ours */
    if (!l->listener) { errno = EINVAL; return -1; }

    pfd_t *e;
    for (;;) {
        e = accept_q_pop();
        if (e) break;
        efd_drain(l);
        e = accept_q_pop();                        /* close the signal race */
        if (e) break;
        if (l->nonblock || (flags & SOCK_NONBLOCK)) { errno = EAGAIN; return -1; }
        if (wait_ready(l, 0) < 0) return -1;
    }

    int newfd = real_dup(e->efd);                  /* app-visible fd (clears CLOEXEC) */
    if (newfd < 0) { close_q_push(e); return -1; }
    if (newfd >= PRELOAD_MAX_FDS) { real_close(newfd); close_q_push(e); errno = EMFILE; return -1; }
    e->nonblock = (flags & SOCK_NONBLOCK) ? 1 : 0;
    if (flags & SOCK_CLOEXEC) real_fcntl(newfd, F_SETFD, FD_CLOEXEC);
    pthread_mutex_lock(&g_tbl_mu);
    g_fds[newfd] = e;
    e->refs++;
    pthread_mutex_unlock(&g_tbl_mu);

    if (addr && alen && *alen >= (socklen_t)sizeof(struct sockaddr_in)) {
        struct sockaddr_in sin;
        memset(&sin, 0, sizeof sin);
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        sin.sin_port = htons(e->pport);
        memcpy(addr, &sin, sizeof sin);
        *alen = sizeof sin;
    }
    DBG("accept → fd=%d (peer port=%u)", newfd, e->pport);
    return newfd;
}

int accept(int fd, struct sockaddr *addr, socklen_t *alen) {
    ENSURE_REAL();
    int r = shim_accept(fd, addr, alen, 0);
    return (r == -2) ? real_accept(fd, addr, alen) : r;
}

int accept4(int fd, struct sockaddr *addr, socklen_t *alen, int flags) {
    ENSURE_REAL();
    int r = shim_accept(fd, addr, alen, flags);
    return (r == -2) ? real_accept4(fd, addr, alen, flags) : r;
}

/* ------------------------------ data calls ----------------------------- */

ssize_t read(int fd, void *buf, size_t len) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_read(fd, buf, len);
    return shim_recv(e, buf, len, 0);
}

ssize_t recv(int fd, void *buf, size_t len, int flags) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_recv(fd, buf, len, flags);
    return shim_recv(e, buf, len, flags);
}

ssize_t recvfrom(int fd, void *buf, size_t len, int flags,
                 struct sockaddr *src, socklen_t *slen) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_recvfrom(fd, buf, len, flags, src, slen);
    if (src && slen) *slen = 0;
    return shim_recv(e, buf, len, flags);
}

ssize_t recvmsg(int fd, struct msghdr *msg, int flags) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_recvmsg(fd, msg, flags);
    ssize_t total = 0;
    msg->msg_controllen = 0;
    msg->msg_flags = 0;
    for (size_t i = 0; i < msg->msg_iovlen; i++) {
        ssize_t n = shim_recv(e, msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len,
                              flags | (total ? MSG_DONTWAIT : 0));
        if (n < 0) return total ? total : -1;
        total += n;
        if (n < (ssize_t)msg->msg_iov[i].iov_len) break;   /* short read = stop */
    }
    return total;
}

ssize_t readv(int fd, const struct iovec *iov, int cnt) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_readv(fd, iov, cnt);
    ssize_t total = 0;
    for (int i = 0; i < cnt; i++) {
        ssize_t n = shim_recv(e, iov[i].iov_base, iov[i].iov_len,
                              total ? MSG_DONTWAIT : 0);
        if (n < 0) return total ? total : -1;
        total += n;
        if (n < (ssize_t)iov[i].iov_len) break;
    }
    return total;
}

ssize_t write(int fd, const void *buf, size_t len) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_write(fd, buf, len);
    return shim_send(e, buf, len, 0);
}

ssize_t send(int fd, const void *buf, size_t len, int flags) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_send(fd, buf, len, flags);
    return shim_send(e, buf, len, flags);
}

ssize_t sendto(int fd, const void *buf, size_t len, int flags,
               const struct sockaddr *dst, socklen_t dlen) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_sendto(fd, buf, len, flags, dst, dlen);
    return shim_send(e, buf, len, flags);          /* connected: dst ignored */
}

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
    ENSURE_REAL();
    (void)flags;
    pfd_t *e = pfd_get(fd);
    if (!e) return real_sendmsg(fd, msg, flags);
    return shim_send_iov(e, msg->msg_iov, (int)msg->msg_iovlen);
}

ssize_t writev(int fd, const struct iovec *iov, int cnt) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_writev(fd, iov, cnt);
    return shim_send_iov(e, iov, cnt);
}

static ssize_t sendfile_common(int out_fd, int in_fd, off_t *offset, size_t count,
                               ssize_t (*realsf)(int, int, off_t *, size_t)) {
    pfd_t *e = pfd_get(out_fd);
    if (!e) return realsf(out_fd, in_fd, offset, count);
    char buf[8192];
    size_t done = 0;
    while (done < count) {
        size_t want = count - done; if (want > sizeof buf) want = sizeof buf;
        ssize_t r = offset ? pread(in_fd, buf, want, *offset + (off_t)done)
                           : real_read(in_fd, buf, want);
        if (r < 0) return done ? (ssize_t)done : -1;
        if (r == 0) break;
        ssize_t w = shim_send(e, buf, (size_t)r, 0);
        if (w < 0) return done ? (ssize_t)done : -1;
        done += (size_t)w;
        if (!offset) continue;
    }
    if (offset) *offset += (off_t)done;
    else if (done) { /* non-offset form consumed in_fd via read: already advanced */ }
    return (ssize_t)done;
}

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count) {
    ENSURE_REAL();
    return sendfile_common(out_fd, in_fd, offset, count, real_sendfile);
}

ssize_t sendfile64(int out_fd, int in_fd, off_t *offset, size_t count) {
    ENSURE_REAL();
    return sendfile_common(out_fd, in_fd, offset, count,
                           real_sendfile64 ? real_sendfile64 : real_sendfile);
}

/* --------------------------- lifecycle calls --------------------------- */

int close(int fd) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_close(fd);
    pthread_mutex_lock(&g_tbl_mu);
    g_fds[fd] = NULL;
    int last = (--e->refs == 0);
    pthread_mutex_unlock(&g_tbl_mu);
    real_close(fd);                               /* the kernel dup */
    if (last) {
        if (e == g_listener) {
            g_listener = NULL;
            g_listener_closed = 1;
            /* Orphan the pending accept queue: nobody can accept these conns now.
             * Queue them for dmesh_close (FIN) — the dispatcher's reap of `e`
             * re-drains, closing the race with a concurrent accept-wrap. */
            pfd_t *p;
            while ((p = accept_q_pop()) != NULL) close_q_push(p);
        }
        close_q_push(e);                          /* dmesh_close runs on the dispatcher */
        DBG("close fd=%d (last ref)", fd);
    }
    return 0;
}

int shutdown(int fd, int how) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_shutdown(fd, how);
    if (e->listener) { errno = ENOTCONN; return -1; }
    pthread_mutex_lock(&e->mu);
    if ((how == SHUT_WR || how == SHUT_RDWR) && !e->wr_closed) {
        e->wr_closed = 1;
        /* Approximate half-close: ship buffered bytes, then a FIN. NOTE: the
         * FIN frees the DPU upstream — replies sent after it are undeliverable
         * (the transport has no true half-close; documented limit). */
        if (e->conn) {
            dmesh_flush(e->conn);
            if (!e->conn->peer_closed) dmesh_send_fin(e->conn);
        }
    }
    if (how == SHUT_RD || how == SHUT_RDWR) e->rd_closed = 1;
    pthread_mutex_unlock(&e->mu);
    if (how == SHUT_RD || how == SHUT_RDWR) efd_signal(e);   /* unblock readers → EOF */
    return 0;
}

/* ------------------------- fd-flag / name calls ------------------------ */

static int fcntl_common(int fd, int cmd, void *arg, int (*realf)(int, int, ...)) {
    pfd_t *e = pfd_get(fd);
    if (!e) return realf(fd, cmd, arg);

    switch (cmd) {
    case F_GETFL: {
        int fl = realf(fd, F_GETFL, 0);
        if (fl < 0) return fl;
        return e->nonblock ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK);
    }
    case F_SETFL:
        e->nonblock = ((long)(intptr_t)arg & O_NONBLOCK) ? 1 : 0;
        return 0;
    case F_DUPFD:
    case F_DUPFD_CLOEXEC: {
        int nf = realf(fd, cmd, arg);
        if (nf >= 0 && nf < PRELOAD_MAX_FDS) {
            pthread_mutex_lock(&g_tbl_mu);
            g_fds[nf] = e;
            e->refs++;
            pthread_mutex_unlock(&g_tbl_mu);
        }
        return nf;
    }
    default:
        return realf(fd, cmd, arg);
    }
}

int fcntl(int fd, int cmd, ...) {
    ENSURE_REAL();
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    return fcntl_common(fd, cmd, arg, real_fcntl);
}

int fcntl64(int fd, int cmd, ...) {
    ENSURE_REAL();
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    return fcntl_common(fd, cmd, arg, real_fcntl64 ? real_fcntl64 : real_fcntl);
}

int ioctl(int fd, unsigned long req, ...) {
    ENSURE_REAL();
    va_list ap;
    va_start(ap, req);
    void *arg = va_arg(ap, void *);
    va_end(ap);

    pfd_t *e = pfd_get(fd);
    if (!e) return real_ioctl(fd, req, arg);

    if (req == FIONBIO && arg) { e->nonblock = *(int *)arg ? 1 : 0; return 0; }
    if (req == FIONREAD && arg) {
        int n = 0;
        pthread_mutex_lock(&e->mu);
        if (e->conn && e->conn->rx_slot >= 0)
            n = (int)(e->conn->rx_len - e->conn->rx_pos);
        pthread_mutex_unlock(&e->mu);
        *(int *)arg = n;                            /* best-effort: loaded message only */
        return 0;
    }
    return real_ioctl(fd, req, arg);
}

int setsockopt(int fd, int level, int optname, const void *val, socklen_t len) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_setsockopt(fd, level, optname, val, len);
    if (level == SOL_SOCKET && optname == SO_RCVTIMEO && val && len >= sizeof(struct timeval)) {
        const struct timeval *tv = (const struct timeval *)val;
        e->rcv_timeout_ms = tv->tv_sec * 1000L + tv->tv_usec / 1000L;
    }
    return 0;                                       /* accepted no-op (TCP_NODELAY, …) */
}

int getsockopt(int fd, int level, int optname, void *val, socklen_t *len) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_getsockopt(fd, level, optname, val, len);
    if (level == SOL_SOCKET && val && len) {
        switch (optname) {
        case SO_ERROR:                              /* connect() is local: never pending */
            if (*len >= sizeof(int)) { *(int *)val = 0; *len = sizeof(int); }
            return 0;
        case SO_TYPE:                               /* apps sanity-check this */
            if (*len >= sizeof(int)) { *(int *)val = SOCK_STREAM; *len = sizeof(int); }
            return 0;
        case SO_SNDBUF:
        case SO_RCVBUF:                             /* NEVER 0 — apps size buffers by it */
            if (*len >= sizeof(int)) { *(int *)val = 262144; *len = sizeof(int); }
            return 0;
        case SO_RCVTIMEO:                           /* mirror what setsockopt stored */
            if (*len >= sizeof(struct timeval)) {
                struct timeval tv = { e->rcv_timeout_ms / 1000,
                                      (e->rcv_timeout_ms % 1000) * 1000 };
                memcpy(val, &tv, sizeof tv);
                *len = sizeof tv;
            }
            return 0;
        }
    }
    if (val && len && *len >= sizeof(int)) { *(int *)val = 0; *len = sizeof(int); }
    return 0;                                       /* unknown: report "off/disabled" */
}

static int synth_name(uint16_t port, struct sockaddr *addr, socklen_t *alen) {
    if (!addr || !alen) { errno = EFAULT; return -1; }
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof sin);
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sin.sin_port = htons(port);
    socklen_t n = *alen < (socklen_t)sizeof sin ? *alen : (socklen_t)sizeof sin;
    memcpy(addr, &sin, n);
    *alen = sizeof sin;
    return 0;
}

int getsockname(int fd, struct sockaddr *addr, socklen_t *alen) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_getsockname(fd, addr, alen);
    return synth_name(e->lport, addr, alen);
}

int getpeername(int fd, struct sockaddr *addr, socklen_t *alen) {
    ENSURE_REAL();
    pfd_t *e = pfd_get(fd);
    if (!e) return real_getpeername(fd, addr, alen);
    if (e->listener) { errno = ENOTCONN; return -1; }
    return synth_name(e->pport, addr, alen);
}

/* ------------------------------- dup calls ----------------------------- */

static void track_alias(int oldfd, int newfd) {
    if (newfd < 0 || newfd >= PRELOAD_MAX_FDS) return;
    pfd_t *e = pfd_get(oldfd);
    if (!e) return;
    pthread_mutex_lock(&g_tbl_mu);
    g_fds[newfd] = e;
    e->refs++;
    pthread_mutex_unlock(&g_tbl_mu);
}

int dup(int fd) {
    ENSURE_REAL();
    int nf = real_dup(fd);
    if (nf >= 0) track_alias(fd, nf);
    return nf;
}

int dup2(int fd, int nfd) {
    ENSURE_REAL();
    if (pfd_get(nfd) && nfd != fd) close(nfd);      /* our close semantics on the target */
    int nf = real_dup2(fd, nfd);
    if (nf >= 0 && nf != fd) track_alias(fd, nf);
    return nf;
}

int dup3(int fd, int nfd, int flags) {
    ENSURE_REAL();
    if (pfd_get(nfd) && nfd != fd) close(nfd);
    int nf = real_dup3(fd, nfd, flags);
    if (nf >= 0 && nf != fd) track_alias(fd, nf);
    return nf;
}
