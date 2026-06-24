/*
 * echo_sock.c — a plain non-blocking epoll echo server, ported to DPUmesh by
 * swapping the BSD-socket calls for their `_dpm` twins.
 *
 * The ENTIRE port from an ordinary TCP epoll echo server is:
 *     socket()/bind()/listen()  ->  socket_dpm()
 *     <listen fd>               ->  event_fd_dpm()      (register in NATIVE epoll)
 *     accept()                  ->  accept_dpm()
 *     read()                    ->  read_dpm()
 *     write()                   ->  write_dpm()  (close_dpm ships it; no send)
 *     close()                   ->  close_dpm()
 *     epoll_create/_ctl/_wait   ->  UNCHANGED (native kernel epoll)
 *
 * Everything else — the epoll loop, the readiness dispatch, errno/EAGAIN — is
 * standard Linux I/O. This is the whole point: a normal epoll server runs over
 * the DPU transport with nothing but the `_dpm` suffix.
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
    dpm_t *s = socket_dpm("echo-sock", worker_id);
    if (!s) { fprintf(stderr, "[echo_sock] socket_dpm failed\n"); return 1; }

    /* The DPUmesh readiness fd plays the role of the listen socket. */
    int dfd = event_fd_dpm(s);
    if (dfd < 0) { fprintf(stderr, "[echo_sock] event_fd unavailable\n"); return 1; }

    /* ---- vanilla kernel epoll, unchanged ---- */
    int epfd = epoll_create1(0);
    if (epfd < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = dfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev) < 0) { perror("epoll_ctl"); return 1; }

    fprintf(stderr, "[echo_sock] ready: pod_id=%d event_fd=%d (native epoll)\n",
            pod_id_dpm(s), dfd);

    struct epoll_event events[MAX_EVENTS];
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
            while ((c = accept_dpm(s)) != NULL) {
                /* read() the whole request (arrives atomically, <= 8 KB) */
                char buf[8192];
                ssize_t off = 0, r;
                while ((r = read_dpm(c, buf + off, sizeof buf - (size_t)off)) > 0)
                    off += r;

                /* write() it straight back; close() ships it (implicit send) — a
                 * normal read/write/close echo server, no explicit send call. */
                if (off > 0)
                    write_dpm(c, buf, (size_t)off);

                close_dpm(c);
            }
        }
    }

    destroy_dpm(s);
    return 0;
}
