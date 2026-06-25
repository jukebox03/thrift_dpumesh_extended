/*
 * echo_sock.c — a plain non-blocking epoll echo server, ported to DPUmesh by
 * swapping the BSD-socket calls for their `dpm_` twins.
 *
 * The ENTIRE port from an ordinary TCP epoll echo server is:
 *     socket()/bind()/listen()  ->  dpm_socket()
 *     <listen fd>               ->  dpm_event_fd()      (register in NATIVE epoll)
 *     accept()                  ->  dpm_accept()
 *     read()                    ->  dpm_read()
 *     write()                   ->  dpm_write()  (dpm_close ships it; no send)
 *     close()                   ->  dpm_close()
 *     epoll_create/_ctl/_wait   ->  UNCHANGED (native kernel epoll)
 *
 * Everything else — the epoll loop, the readiness dispatch, errno/EAGAIN — is
 * standard Linux I/O. This is the whole point: a normal epoll server runs over
 * the DPU transport with nothing but the `dpm_` prefix.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sched.h>
#include <sys/epoll.h>

#include "thrift/transport/dpm.h"

#define MAX_EVENTS 64

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    int worker_id = 11;
    if (getenv("BENCH_WORKER_ID"))
        worker_id = atoi(getenv("BENCH_WORKER_ID"));

    /* socket() + bind() + listen() */
    dpm_t *s = dpm_socket("echo-sock", worker_id);
    if (!s) { fprintf(stderr, "[echo_sock] dpm_socket failed\n"); return 1; }

    /* The DPUmesh readiness fd plays the role of the listen socket. */
    int dfd = dpm_event_fd(s);
    if (dfd < 0) { fprintf(stderr, "[echo_sock] event_fd unavailable\n"); return 1; }

    /* ---- vanilla kernel epoll, unchanged ---- */
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = dfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev) < 0) { perror("epoll_ctl"); return 1; }

    fprintf(stderr, "[echo_sock] ready: pod_id=%d event_fd=%d (native epoll)\n",
            dpm_pod_id(s), dfd);

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
            dpmconn_t *c;
            while ((c = dpm_accept(s)) != NULL) {
                /* read() the whole request (arrives atomically, <= 8 KB) */
                char buf[8192];
                ssize_t off = 0, r;
                while ((r = dpm_read(c, buf + off, sizeof buf - (size_t)off)) > 0)
                    off += r;

                /* write() it straight back; close() ships it (implicit send) — a
                 * normal read/write/close echo server, no explicit send call. */
                if (off > 0)
                    dpm_write(c, buf, (size_t)off);

                dpm_close(c);
                if ((++recv_total % 200000) == 0)
                    fprintf(stderr, "[echo_sock] recv_total=%lu\n", recv_total);
            }
        }
    }

    dpm_destroy(s);
    return 0;
}
