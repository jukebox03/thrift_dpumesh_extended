/*
 * echo_sock.c — a non-blocking epoll echo server over DPUmesh (CONNECTION model,
 * single channel fd + PE-published READY LIST).
 *
 * Exactly a BSD-socket epoll server, but with ONE fd:
 *     dmesh_event_fd(s)   = the channel fd — readable when a new conn is pending
 *                           OR any existing conn has inbound.
 *     dmesh_accept(s)     = pop a new connection (holds its first message).
 *     dmesh_next_ready(s) = pop the next existing conn that has inbound — the PE
 *                           names the ready conns, so there is NO scan and NO
 *                           per-conn fd. Returns the SAME conn handle.
 *     dmesh_read/write+flush/close.
 *
 * Connections are PERSISTENT and full-duplex: accept once, then read+echo every
 * message until the peer closes (dmesh_read == 0 → EOF/FIN → dmesh_close). One
 * message arrives atomically (<= 8 KB), so each dmesh_read returns a whole message.
 * The app holds NO conn table and does NO per-conn epoll_ctl — the transport hands
 * each ready conn back via dmesh_next_ready.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <sched.h>
#include <sys/epoll.h>

#include "thrift/transport/dpm.h"

#define MAX_EVENTS 8        /* only one fd is registered, so this is plenty */

/* Read + echo every available message on a conn, draining to EAGAIN (REQUIRED —
 * the ready list re-arms a conn only on its inbox empty→non-empty edge, so leaving
 * a message unread would stall it). Adds the echoed count to *n_msgs. Returns 1 if
 * the peer closed (EOF/FIN) — the caller must dmesh_close() it; 0 on would-block. */
static int echo_drain(dmesh_conn_t *c, unsigned long *n_msgs)
{
    static __thread char buf[8192];
    ssize_t n;
    for (;;) {
        n = dmesh_read(c, buf, sizeof buf);
        if (n > 0) {                                      /* a whole message → echo it */
            dmesh_write(c, buf, (size_t)n);
            dmesh_flush(c);                               /* ship the echo */
            (*n_msgs)++;
        } else if (n == 0) {                              /* EOF: peer sent FIN */
            return 1;
        } else {                                          /* n<0 (EAGAIN) → nothing more now */
            return 0;
        }
    }
}

int main(void)
{
    int worker_id = 11;
    if (getenv("BENCH_WORKER_ID"))
        worker_id = atoi(getenv("BENCH_WORKER_ID"));

    dmesh_channel_t *s = dmesh_create_channel("echo-sock", worker_id);
    if (!s) { fprintf(stderr, "[echo_sock] dmesh_create_channel failed\n"); return 1; }

    int dfd = dmesh_event_fd(s);                          /* the ONE channel fd */
    if (dfd < 0) { fprintf(stderr, "[echo_sock] event_fd unavailable\n"); return 1; }

    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev = { .events = EPOLLIN, .data = { .fd = dfd } };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev) < 0) { perror("epoll_ctl"); return 1; }

    fprintf(stderr, "[echo_sock] ready (connection model, ready-list): pod_id=%d channel_fd=%d\n",
            dmesh_pod_id(s), dfd);

    struct epoll_event events[MAX_EVENTS];
    unsigned long recv_total = 0;
    for (;;) {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds < 0) { if (errno == EINTR) continue; perror("epoll_wait"); break; }

        /* The channel fd fired. Drain it (level counter → edge), then service both
         * sources: new conns, then conns the PE flagged ready. No scan of all conns. */
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) { }

        dmesh_conn_t *c;
        while ((c = dmesh_accept(s)) != NULL)              /* new connections (first msg in hand) */
            if (echo_drain(c, &recv_total)) dmesh_close(c);
        while ((c = dmesh_next_ready(s)) != NULL)          /* existing conns with inbound */
            if (echo_drain(c, &recv_total)) dmesh_close(c); /* EOF (peer FIN) → reclaim the conn */

        if (recv_total && (recv_total % 100000) < 64)
            fprintf(stderr, "[echo_sock] recv_total=%lu\n", recv_total);
    }

    dmesh_destroy_channel(s);
    return 0;
}
