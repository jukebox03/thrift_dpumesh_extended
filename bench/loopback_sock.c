/*
 * loopback_sock.c — self-routing / loopback validator for the oriented-tuple
 * model. ONE pod is BOTH the server and the client of its OWN service:
 *
 *   create_channel(pod_id)  ->  registers service_id = pod_id (DPU default)
 *   echo thread             ->  accept -> read -> write -> flush -> close  (server)
 *   RUN handler             ->  connect(own service) -> write/flush/read   (client)
 *
 * The client's request is dst=(own service, pod=BLANK, port=BLANK): the DPU
 * resolves the service to THIS pod and loops it back. On this single host the
 * request lands with dst_port=uP (>= DMESH_UPORT_BASE -> server accept) while the
 * reply lands with dst_port=pc (< BASE -> client conn). The port-range split keeps
 * both kinds of conn in the one ports[] table — that is exactly what this proves.
 *
 * Uses ONLY the façade (dpm.h). No changes to the transport. Control-TCP daemon
 * like bench_sock.c: `RUN <N> <SIZE>` runs N loopback round-trips and replies
 * `OK <ok> <fail> <served> <p50us>`.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "thrift/transport/dpm.h"

#define CTRL_PORT 9092

static dmesh_channel_t *g_s   = NULL;
static int              g_service = 0;     /* own service id (declared at create_channel) */
static atomic_int       g_stop  = 0;
static atomic_long      g_served = 0;      /* requests the echo side replied to */

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
static int cmp_d(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* ---- server side: drain this pod's accept queue, echo every request ---- */
static void *echo_fn(void *arg) {
    (void)arg;
    static dmesh_conn_t *cl[4096];
    int ncl = 0;
    char b[8192];
    while (!atomic_load(&g_stop)) {
        int did = 0;
        dmesh_conn_t *c;
        while (ncl < 4096 && (c = dmesh_accept(g_s)) != NULL) { cl[ncl++] = c; did = 1; }
        for (int i = 0; i < ncl; ) {
            ssize_t n;
            while ((n = dmesh_read(cl[i], b, sizeof b)) > 0) {      /* drain to EAGAIN, echo each */
                dmesh_write(cl[i], b, (size_t)n); dmesh_flush(cl[i]);
                atomic_fetch_add(&g_served, 1); did = 1;
            }
            if (n == 0) { dmesh_close(cl[i]); cl[i] = cl[--ncl]; }  /* EOF (client FIN) → drop */
            else i++;
        }
        if (!did) { struct timespec t = {0, 2000}; nanosleep(&t, NULL); }  /* 2us idle */
    }
    for (int i = 0; i < ncl; i++) dmesh_close(cl[i]);
    return NULL;
}

/* ---- client side: N round-trips to our OWN service (reusable conn) ----
 * zc != 0 → ZERO-COPY send: fill transport memory from dmesh_alloc directly and
 * dmesh_slot_register it (no dmesh_write memcpy). size may exceed one slot (the
 * region auto-chunks + route-pins); the reply is reassembled with a read loop. */
static void run_loopback(int conn_fd, long N, int size, int zc) {
    char reply[160];
    int msgmax = dmesh_msg_max(g_s);
    /* zero-copy may span slots (arena); plain path stays single-slot (<= msgmax). */
    if (N < 1 || size < 1 || (!zc && size > msgmax)) {
        int n = snprintf(reply, sizeof reply, "ERR bad args (size<=%d for non-zc)\n", msgmax);
        write(conn_fd, reply, (size_t)n); return;
    }
    uint8_t *body = malloc((size_t)size), *rb = malloc((size_t)size);
    double  *lat  = malloc((size_t)N * sizeof(double));
    if (!body || !rb || !lat) { free(body); free(rb); free(lat);
        write(conn_fd, "ERR oom\n", 8); return; }

    dmesh_conn_t *c = dmesh_connect(g_s, g_service);
    long ok = 0, fail = 0; size_t ns = 0;
    for (long i = 0; i < N && c && !atomic_load(&g_stop); i++) {
        uint8_t p = (uint8_t)('A' + (i & 0xf));
        double t0 = now_sec();
        int sent;
        if (zc) {
            /* ZERO-COPY: reserve `size` contiguous bytes in the conn's byte-ring, fill
             * transport DMA memory directly, commit + flush (no memcpy). flush carves
             * it into <= slot_size descriptors; the reader reassembles the byte stream. */
            uint8_t *bz = dmesh_alloc(c, (size_t)size);
            if (!bz) { fail++; dmesh_close(c); c = dmesh_connect(g_s, g_service); continue; }
            bz[0] = p; bz[size / 2] = p; bz[size - 1] = p;
            dmesh_commit(c, (size_t)size);
            sent = (dmesh_flush(c) >= 0);
        } else {
            body[0] = p; body[size / 2] = p; body[size - 1] = p;
            sent = (dmesh_write(c, body, (size_t)size) >= 0 && dmesh_flush(c) >= 0);
        }
        if (!sent) { fail++; dmesh_close(c); c = dmesh_connect(g_s, g_service); continue; }
        /* read the reply — reassemble up to `size` bytes (large = multiple chunks). */
        double tw = now_sec(); size_t got = 0; int timedout = 0;
        while (got < (size_t)size) {
            ssize_t n = dmesh_read(c, rb + got, (size_t)size - got);
            if (n > 0) { got += (size_t)n; continue; }
            if (n == 0) break;                            /* peer closed */
            if (now_sec() - tw > 5.0) { timedout = 1; break; }  /* timeout */
            sched_yield();                                /* EAGAIN */
        }
        int bad = timedout || got != (size_t)size ||
                  rb[0] != p || rb[size / 2] != p || rb[size - 1] != p;
        if (bad) { fail++; dmesh_close(c); c = dmesh_connect(g_s, g_service); }
        else     { ok++; lat[ns++] = (now_sec() - t0) * 1e6; }
    }
    if (c) dmesh_close(c);

    qsort(lat, ns, sizeof(double), cmp_d);
    double p50 = ns ? lat[ns / 2] : 0.0;
    int rn = snprintf(reply, sizeof reply, "OK %ld %ld %ld %.1f\n",
                      ok, fail, atomic_load(&g_served), p50);
    write(conn_fd, reply, (size_t)rn);
    fprintf(stderr, "[loopback] DONE N=%ld ok=%ld fail=%ld served=%ld p50=%.1fus\n",
            N, ok, fail, atomic_load(&g_served), p50);
    free(body); free(rb); free(lat);
}

static void handle_ctrl(int fd) {
    char buf[128];
    ssize_t n = read(fd, buf, sizeof buf - 1);
    if (n <= 0) { close(fd); return; }
    buf[n] = '\0';
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    char cmd[16] = {0}; long N = 0; int size = 0, zc = 0;
    if (sscanf(buf, "%15s %ld %d %d", cmd, &N, &size, &zc) >= 1 && strcmp(cmd, "RUN") == 0)
        run_loopback(fd, N, size, zc);   /* 4th arg (optional) = zero-copy flag */
    else
        write(fd, "ERR use: RUN <N> <SIZE> [ZC]\n", 29);
    close(fd);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    int service_id = 12;   /* this node advertises AND connects to its own service */
    if (getenv("BENCH_WORKER_ID")) service_id = atoi(getenv("BENCH_WORKER_ID"));

    g_s = dmesh_create_channel(service_id);
    if (!g_s) { fprintf(stderr, "[loopback] create_channel failed\n"); return 1; }
    g_service = service_id;   /* connect to OUR OWN service; DPU routes it back to us */
    fprintf(stderr, "[loopback] ready: pod_id=%d own_service=%d\n", dmesh_pod_id(g_s), g_service);

    pthread_t et; pthread_create(&et, NULL, echo_fn, NULL);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
    struct sockaddr_in a; memset(&a, 0, sizeof a);
    a.sin_family = AF_INET; a.sin_port = htons(CTRL_PORT); a.sin_addr.s_addr = INADDR_ANY;
    if (bind(srv, (struct sockaddr *)&a, sizeof a) < 0 || listen(srv, 4) < 0) {
        perror("listen"); return 1;
    }
    fprintf(stderr, "[loopback] control LISTEN on :%d\n", CTRL_PORT);
    for (;;) {
        int conn = accept(srv, NULL, NULL);
        if (conn < 0) { if (errno == EINTR) continue; perror("accept"); continue; }
        handle_ctrl(conn);
    }
}
