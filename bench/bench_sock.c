/*
 * bench_sock.c — DPUmesh load-generator, written with ONLY the socket/epoll
 * façade (dpumesh_sock.h). A port of an ordinary async request/response client:
 *
 *     socket()/bind()  ->  socket_dpumesh()
 *     connect()        ->  connect_dpumesh()
 *     write()          ->  write_dpumesh()  (+ send_dpumesh() to ship the request)
 *     read()           ->  read_dpumesh()   (the response; EAGAIN until it arrives)
 *     close()          ->  close_dpumesh()
 *
 * Each in-flight request is one dpmconn_t (single-shot). A worker keeps a window
 * of W conns: it connect/write/send to launch, and read_dpumesh (non-blocking) to
 * harvest. The control daemon / pacing / latency stats are identical to the
 * raw-API bench — only the per-request data path uses the façade.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "thrift/transport/dpumesh_sock.h"

#define CTRL_PORT          9092
#define MAX_WORKERS        4096
#define WORKER_STACK_BYTES (128 * 1024)
#define WAIT_TIMEOUT_MS    5000
#define DRAIN_GRACE_SEC    5

static dpm_t *g_s = NULL;            /* the façade endpoint (shared, thread-safe) */
static int    g_dst_pod_id = 11;
static int    g_async_threads = 4;

/* ------------------------------------------------------------ time helpers */
static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------ per-worker  */
typedef struct {
    long       budget;
    double     interval_sec;
    int        msg_size;
    int        inflight;
    double     start_at;
    atomic_int *stop;
    atomic_long *ok;
    atomic_long *fail;
    double    *samples;
    size_t     n_samples;
    size_t     cap;
} worker_t;

/* One in-flight request = one façade connection. */
typedef struct {
    dpmconn_t *c;
    double     scheduled;   /* t0 for latency (coordinated-omission: scheduled time) */
    double     launched;    /* send time, for the wall-clock timeout */
    long       j;           /* logical request index (body pattern) */
    int        active;
} inflight_slot_t;

/* Async generator over the façade: a window of W single-shot conns. */
static void *worker_fn_async(void *arg) {
    worker_t *w = (worker_t *)arg;
    int W = w->inflight > 0 ? w->inflight : 1;

    inflight_slot_t *fl = calloc((size_t)W, sizeof(inflight_slot_t));
    uint8_t *body = malloc((size_t)w->msg_size);   /* request scratch */
    uint8_t *rb   = malloc((size_t)w->msg_size);   /* response scratch */
    if (!fl || !body || !rb) { free(fl); free(body); free(rb); atomic_store(w->stop, 1); return NULL; }
    memset(body, 0, (size_t)w->msg_size);

    long next_j = 0, completed = 0;
    const double timeout_s = (double)WAIT_TIMEOUT_MS / 1000.0;

    while (completed < w->budget) {
        if (atomic_load(w->stop)) break;
        int did_work = 0;

        /* ---- 1. Launch due requests into free window slots ---- */
        for (int s = 0; s < W && next_j < w->budget; s++) {
            if (fl[s].active) continue;
            double scheduled = w->start_at + (double)next_j * w->interval_sec;
            if (now_sec() < scheduled) break;          /* paced */

            dpmconn_t *c = connect_dpumesh(g_s, g_dst_pod_id);     /* connect() */
            if (!c) break;                              /* transient OOM: retry next sweep */

            uint8_t p = (uint8_t)('A' + (next_j & 0xf));
            body[0] = p; body[w->msg_size / 2] = p; body[w->msg_size - 1] = p;
            write_dpumesh(c, body, (size_t)w->msg_size);           /* write() (buffers) */
            if (send_dpumesh(c) < 0) {                              /* send() */
                close_dpumesh(c);
                atomic_fetch_add(w->fail, 1);
                next_j++; completed++; did_work = 1; continue;
            }
            fl[s].c = c; fl[s].scheduled = scheduled; fl[s].launched = now_sec();
            fl[s].j = next_j; fl[s].active = 1;
            next_j++; did_work = 1;
        }

        /* ---- 2. Harvest completed (and time out stalled) slots ---- */
        for (int s = 0; s < W; s++) {
            if (!fl[s].active) continue;
            ssize_t n = read_dpumesh(fl[s].c, rb, (size_t)w->msg_size);   /* read() */
            if (n < 0 && errno == EAGAIN) {            /* response not in yet */
                if (now_sec() - fl[s].launched > timeout_s) {
                    close_dpumesh(fl[s].c);
                    atomic_fetch_add(w->fail, 1);
                    fl[s].active = 0; completed++; did_work = 1;
                }
                continue;
            }
            int bad;
            if (n < 0) {                                /* ECONNRESET / abandoned */
                bad = 1;
            } else {
                uint8_t expect = (uint8_t)('A' + (fl[s].j & 0xf));
                bad = (n != (ssize_t)w->msg_size) ||
                      (n > 0 && (rb[0] != expect || rb[n / 2] != expect || rb[n - 1] != expect));
            }
            close_dpumesh(fl[s].c);                     /* close() */
            if (bad) {
                atomic_fetch_add(w->fail, 1);
            } else {
                double lat_us = (now_sec() - fl[s].scheduled) * 1e6;
                if (w->n_samples < w->cap) w->samples[w->n_samples++] = lat_us;
                atomic_fetch_add(w->ok, 1);
            }
            fl[s].active = 0; completed++; did_work = 1;
        }

        /* ---- 3. Yield when a full sweep made no progress (lean when idle) ---- */
        if (!did_work) {
            struct timespec ts = {0, 5000};   /* 5 us */
            nanosleep(&ts, NULL);
        }
    }

    /* Drain slots still active at stop/deadline — close_dpumesh cancels the
     * pending entry + reclaims the TX slot (no leak across back-to-back runs). */
    for (int s = 0; s < W; s++)
        if (fl[s].active) close_dpumesh(fl[s].c);
    free(fl); free(body); free(rb);
    return NULL;
}

/* ------------------------------------------------------------ watchdog  */
typedef struct {
    atomic_int *stop;
    atomic_int *early_exit;
    int         deadline_sec;
} wd_arg_t;

static void *watchdog_fn(void *arg) {
    wd_arg_t *wa = (wd_arg_t *)arg;
    double t0 = now_sec();
    while (now_sec() - t0 < (double)wa->deadline_sec) {
        if (atomic_load(wa->early_exit)) return NULL;
        struct timespec ts = {0, 100000000};  /* 100ms */
        nanosleep(&ts, NULL);
    }
    atomic_store(wa->stop, 1);
    return NULL;
}

/* ------------------------------------------------------------ aggregation */
static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}
static double pct(double *sorted, size_t n, double p) {
    if (n == 0) return 0.0;
    size_t idx = (size_t)(p * (double)(n - 1));
    if (idx >= n) idx = n - 1;
    return sorted[idx];
}

/* ------------------------------------------------------------ run command */
static void run_test(int conn_fd, int rps, int dur, int msg_size, int conns) {
    char reply[256];

    if (rps < 1 || dur < 1 || msg_size < 1) {
        const char *e = "ERR invalid args (need rps>=1 dur>=1 size>=1)\n";
        write(conn_fd, e, strlen(e));
        return;
    }
    if (msg_size > dpm_msg_max(g_s)) {
        int n = snprintf(reply, sizeof(reply), "ERR size %d > slot_size %d\n",
                         msg_size, dpm_msg_max(g_s));
        write(conn_fd, reply, (size_t)n);
        return;
    }

    int n_workers = g_async_threads;
    if (n_workers < 1) n_workers = 1;
    if (n_workers > MAX_WORKERS) n_workers = MAX_WORKERS;
    int C = (conns > 0) ? conns : (rps / 100);
    if (C < 1) C = 1;
    int inflight = (C + n_workers - 1) / n_workers;
    if (inflight < 1) inflight = 1;

    long total_budget = (long)rps * (long)dur;
    long per_worker   = total_budget / n_workers;
    long remainder    = total_budget % n_workers;
    double interval_sec = (double)n_workers / (double)rps;

    fprintf(stderr, "[bench_sock] RUN rps=%d dur=%d size=%d conns=%d "
                    "(workers=%d inflight=%d interval=%.3fus per worker)\n",
            rps, dur, msg_size, conns, n_workers, inflight, interval_sec * 1e6);

    pthread_t  *tids   = calloc((size_t)n_workers, sizeof(pthread_t));
    worker_t   *wargs  = calloc((size_t)n_workers, sizeof(worker_t));
    if (!tids || !wargs) {
        free(tids); free(wargs);
        const char *e = "ERR oom\n";
        write(conn_fd, e, strlen(e));
        return;
    }

    atomic_int  stop = 0;
    atomic_long ok = 0, fail = 0;
    double start = now_sec() + 0.05;

    pthread_attr_t worker_attr;
    pthread_attr_init(&worker_attr);
    pthread_attr_setstacksize(&worker_attr, WORKER_STACK_BYTES);

    for (int i = 0; i < n_workers; i++) {
        wargs[i].budget       = per_worker + (i < remainder ? 1 : 0);
        wargs[i].interval_sec = interval_sec;
        wargs[i].msg_size     = msg_size;
        wargs[i].inflight     = inflight;
        wargs[i].start_at     = start;
        wargs[i].stop         = &stop;
        wargs[i].ok           = &ok;
        wargs[i].fail         = &fail;
        wargs[i].cap          = (size_t)wargs[i].budget;
        wargs[i].samples      = calloc(wargs[i].cap ? wargs[i].cap : 1, sizeof(double));
        wargs[i].n_samples    = 0;
        if (!wargs[i].samples) atomic_store(&stop, 1);
        if (pthread_create(&tids[i], &worker_attr, worker_fn_async, &wargs[i]) != 0) {
            atomic_store(&stop, 1);
            tids[i] = 0;
        }
    }
    pthread_attr_destroy(&worker_attr);

    atomic_int wd_early = 0;
    wd_arg_t wa = { .stop = &stop, .early_exit = &wd_early, .deadline_sec = dur + DRAIN_GRACE_SEC };
    pthread_t wd_tid;
    pthread_create(&wd_tid, NULL, watchdog_fn, &wa);

    for (int i = 0; i < n_workers; i++)
        if (tids[i]) pthread_join(tids[i], NULL);
    atomic_store(&wd_early, 1);
    pthread_join(wd_tid, NULL);

    double wall = now_sec() - start;
    if (wall < 1e-6) wall = 1e-6;

    size_t total = 0;
    for (int i = 0; i < n_workers; i++) total += wargs[i].n_samples;
    double *all = calloc(total ? total : 1, sizeof(double));
    size_t off = 0;
    for (int i = 0; i < n_workers; i++) {
        if (wargs[i].n_samples) {
            memcpy(all + off, wargs[i].samples, wargs[i].n_samples * sizeof(double));
            off += wargs[i].n_samples;
        }
        free(wargs[i].samples);
    }
    qsort(all, total, sizeof(double), cmp_double);

    long ok_n   = atomic_load(&ok);
    long fail_n = atomic_load(&fail);
    double rps_ach = (double)ok_n / wall;
    double mb_s = (rps_ach * (double)msg_size * 2.0) / (1024.0 * 1024.0);
    double p50  = pct(all, total, 0.50);
    double p99  = pct(all, total, 0.99);
    double p999 = pct(all, total, 0.999);

    free(all); free(tids); free(wargs);

    int n = snprintf(reply, sizeof(reply), "OK %.1f %.1f %.1f %.1f %ld %ld %.2f\n",
                     rps_ach, p50, p99, p999, ok_n, fail_n, mb_s);
    write(conn_fd, reply, (size_t)n);
    fprintf(stderr, "[bench_sock] DONE %s", reply);
}

/* ------------------------------------------------------------ control TCP */
static int parse_run_line(const char *line, int *rps, int *dur, int *size, int *conns) {
    char cmd[16] = {0};
    *conns = 0;
    int n = sscanf(line, "%15s %d %d %d %d", cmd, rps, dur, size, conns);
    if (n < 4) return -1;
    if (strcmp(cmd, "RUN") != 0) return -1;
    return 0;
}

static void handle_ctrl(int conn_fd) {
    char buf[256];
    ssize_t n = read(conn_fd, buf, sizeof(buf) - 1);
    if (n <= 0) { close(conn_fd); return; }
    buf[n] = '\0';
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    char *cr = strchr(buf, '\r'); if (cr) *cr = '\0';

    if (strncmp(buf, "PING", 4) == 0) {
        const char *p = "PONG\n";
        write(conn_fd, p, strlen(p));
        close(conn_fd);
        return;
    }
    int rps, dur, size, conns;
    if (parse_run_line(buf, &rps, &dur, &size, &conns) < 0) {
        const char *e = "ERR bad command (use: RUN <rps> <dur> <size> [<conns>])\n";
        write(conn_fd, e, strlen(e));
        close(conn_fd);
        return;
    }
    run_test(conn_fd, rps, dur, size, conns);
    close(conn_fd);
}

static int ctrl_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); close(fd); return -1; }
    if (listen(fd, 4) < 0) { perror("listen"); close(fd); return -1; }
    return fd;
}

/* ------------------------------------------------------------ main        */
int main(void) {
    signal(SIGPIPE, SIG_IGN);

    int worker_id = 10;
    if (getenv("BENCH_WORKER_ID"))  worker_id      = atoi(getenv("BENCH_WORKER_ID"));
    if (getenv("BENCH_DST_POD_ID")) g_dst_pod_id   = atoi(getenv("BENCH_DST_POD_ID"));
    if (getenv("ASYNC_THREADS"))    g_async_threads = atoi(getenv("ASYNC_THREADS"));

    g_s = socket_dpumesh("bench-sock", worker_id);     /* socket() + bind() */
    if (!g_s) { fprintf(stderr, "[bench_sock] socket_dpumesh failed\n"); return 1; }
    fprintf(stderr, "[bench_sock] ready: pod_id=%d dst_pod_id=%d (façade client)\n",
            dpm_pod_id(g_s), g_dst_pod_id);

    int srv = ctrl_listen(CTRL_PORT);
    if (srv < 0) return 1;
    fprintf(stderr, "[bench_sock] control LISTEN on :%d\n", CTRL_PORT);

    for (;;) {
        int conn = accept(srv, NULL, NULL);
        if (conn < 0) { if (errno == EINTR) continue; perror("accept"); continue; }
        handle_ctrl(conn);
    }
}
