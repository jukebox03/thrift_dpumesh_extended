/*
 * echo_sock.c — a non-blocking epoll echo server over DPUmesh. The BSD-socket
 * calls map to their `dmesh_` twins:
 *     socket()/bind()/listen()  ->  dmesh_create_channel()
 *     <listen fd>               ->  dmesh_event_fd()      (register in NATIVE epoll)
 *     accept()                  ->  dmesh_accept()
 *     read()                    ->  dmesh_read()
 *     write()                   ->  dmesh_write()  (dmesh_close ships it; no send)
 *     close()                   ->  dmesh_close()
 *     epoll_create/_ctl/_wait   ->  UNCHANGED (native kernel epoll)
 *
 * The epoll loop is standard Linux I/O, but this is NOT a byte-for-byte socket
 * port — it is *simpler*. A request arrives ATOMICALLY (one whole <= 8 KB
 * message), so the byte-stream read loop collapses to a single dmesh_read, with no
 * partial-read accumulation and no EPOLLOUT send dance.
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

#define MAX_EVENTS 64

int main(void)
{
    int worker_id = 11;
    if (getenv("BENCH_WORKER_ID"))
        worker_id = atoi(getenv("BENCH_WORKER_ID"));

    /* socket() + bind() + listen() */
    dmesh_channel_t *s = dmesh_create_channel("echo-sock", worker_id);
    if (!s) { fprintf(stderr, "[echo_sock] dmesh_create_channel failed\n"); return 1; }

    /* The DPUmesh readiness fd plays the role of the listen socket. */
    int dfd = dmesh_event_fd(s);
    if (dfd < 0) { fprintf(stderr, "[echo_sock] event_fd unavailable\n"); return 1; }

    /* ---- vanilla kernel epoll, unchanged ---- */
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = dfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev) < 0) { perror("epoll_ctl"); return 1; }

    fprintf(stderr, "[echo_sock] ready: pod_id=%d event_fd=%d (native epoll)\n",
            dmesh_pod_id(s), dfd);

    struct epoll_event events[MAX_EVENTS];
    unsigned long recv_total = 0;   /* received-request counter (delivery cross-check) */
    for (;;) {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);   /* sleeps until activity */
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }
        for (int n = 0; n < nfds; n++) {
            if (events[n].data.fd != dfd)
                continue;

            /* Drain the readiness eventfd (level-triggered: one+ writes pending). */
            uint64_t cnt;
            while (read(dfd, &cnt, sizeof cnt) > 0) { /* drain */ }

            /* accept() every queued request (non-blocking; NULL/EAGAIN = drained). */
            dmesh_conn_t *c;
            while ((c = dmesh_accept(s)) != NULL) {
                /* The whole request body arrives atomically (<= 8 KB) and buf is
                 * sized to the max, so ONE dmesh_read returns it in full — no
                 * byte-stream accumulation loop. */
                char buf[8192];
                ssize_t n = dmesh_read(c, buf, sizeof buf);

                /* echo it straight back; flush ships it (close no longer sends). */
                if (n > 0) {
                    dmesh_write(c, buf, (size_t)n);
                    dmesh_flush(c);
                }

                dmesh_close(c);
                if ((++recv_total % 200000) == 0)
                    fprintf(stderr, "[echo_sock] recv_total=%lu\n", recv_total);
            }
        }
    }

    dmesh_destroy_channel(s);
    return 0;
}
