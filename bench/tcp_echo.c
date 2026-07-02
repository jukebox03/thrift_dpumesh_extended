/*
 * tcp_echo.c — a deliberately VANILLA POSIX TCP epoll echo server.
 *
 * No DPUmesh headers, no transport knowledge: socket/bind/listen/accept4/
 * epoll/read/write/close only. It runs unmodified over kernel TCP, and the
 * SAME binary runs over DPUmesh under LD_PRELOAD=libdmesh_preload.so with
 * DMESH_PRELOAD_LISTEN=<port> DMESH_PRELOAD_SVC=<svc> — that transparency is
 * the point of the test.
 *
 * Usage: tcp_echo [port]        (default 9095)
 */
#define _GNU_SOURCE            /* accept4 */
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define MAX_EVENTS 64

static int set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    return fl < 0 ? -1 : fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* Write all of buf; on EAGAIN wait for writability (POLLOUT). */
static int write_full(int fd, const char *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, buf + done, len - done);
        if (n > 0) { done += (size_t)n; continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd p = { .fd = fd, .events = POLLOUT };
            (void)poll(&p, 1, 1000);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 9095;
    signal(SIGPIPE, SIG_IGN);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof sin);
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&sin, sizeof sin) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 512) < 0) { perror("listen"); return 1; }
    set_nonblock(lfd);

    int ep = epoll_create1(0);
    if (ep < 0) { perror("epoll_create1"); return 1; }
    struct epoll_event ev = { .events = EPOLLIN };
    ev.data.fd = lfd;
    epoll_ctl(ep, EPOLL_CTL_ADD, lfd, &ev);

    printf("tcp_echo: listening on %d\n", port);
    fflush(stdout);

    static char buf[65536];
    struct epoll_event events[MAX_EVENTS];
    for (;;) {
        int nev = epoll_wait(ep, events, MAX_EVENTS, -1);
        if (nev < 0) { if (errno == EINTR) continue; perror("epoll_wait"); return 1; }
        for (int i = 0; i < nev; i++) {
            int fd = events[i].data.fd;
            if (fd == lfd) {
                for (;;) {
                    int cfd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK);
                    if (cfd < 0) break;                     /* EAGAIN: drained */
                    struct epoll_event cev = { .events = EPOLLIN };
                    cev.data.fd = cfd;
                    epoll_ctl(ep, EPOLL_CTL_ADD, cfd, &cev);
                }
                continue;
            }
            for (;;) {                                      /* drain this conn */
                ssize_t n = read(fd, buf, sizeof buf);
                if (n > 0) {
                    if (write_full(fd, buf, (size_t)n) < 0) { close(fd); break; }
                    continue;
                }
                if (n == 0) { close(fd); break; }           /* peer EOF */
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                close(fd);                                  /* hard error */
                break;
            }
        }
    }
}
