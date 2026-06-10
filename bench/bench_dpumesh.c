/*
 * bench_dpumesh.c — DPUmesh load-generator daemon (A안)
 *
 * Lifecycle:
 *   1. dpumesh_init() once at startup, register as pod_id=10 (DPUMESH_POD_ID env).
 *   2. Listen on TCP control port 9092.
 *   3. On "RUN <rps> <duration_sec> <msg_size> [<conns>]" line:
 *        spawn worker threads that fire dpumesh requests at the given rate,
 *        wait for responses, record per-request latency.
 *      After duration, aggregate and reply with a single line:
 *        "OK <rps_achieved> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>"
 *      On parse error: "ERR <message>".
 *
 * The dpumesh context lives across runs — no re-init between experiments.
 * dst_pod_id defaults to 11 (echo_dpumesh).
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

#include <thrift/transport/dpumesh.h>

#define CTRL_PORT          9092
/* Upper bound on closed-loop concurrency (worker threads). */
#define MAX_WORKERS        4096
#define WORKER_STACK_BYTES (128 * 1024)
#define WAIT_TIMEOUT_MS    5000
#define DRAIN_GRACE_SEC    5

static dpumesh_ctx_t *g_ctx = NULL;
static int            g_dst_pod_id = 11;

/* Async (poll-based) client: generator threads each keep a window of in-flight
 * requests harvested via dpumesh_poll_response (no per-request wakeup). */
static int g_async_threads = 4;

/* ------------------------------------------------------------ time helpers */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------ per-worker  */

typedef struct {
    int        worker_id;
    long       budget;          /* total req count for this worker */
    double     interval_sec;
    int        msg_size;
    int        inflight;        /* async: per-thread in-flight window (1 = blocking) */
    double     start_at;        /* common wall start */
    atomic_int *stop;
    atomic_long *ok;
    atomic_long *fail;
    /* per-worker latency log (us) */
    double    *samples;
    size_t     n_samples;
    size_t     cap;
} worker_t;

/* ------------------------------------------------ per-worker (async/poll) */

/* One in-flight request inside a generator thread's window. */
typedef struct {
    uint32_t req_id;
    double   scheduled;   /* t0 for latency (coordinated-omission: scheduled time) */
    double   launched;    /* send time, for the wall-clock timeout */
    long     j;           /* logical request index (body pattern) */
    int      active;
} inflight_slot_t;

/* Async generator: maintains a window of `inflight` outstanding requests rather
 * than blocking one-per-thread. Sends are paced by scheduled time; completions
 * are harvested with dpumesh_poll_response (no per-request wakeup). */
static void *worker_fn_async(void *arg) {
    worker_t *w = (worker_t *)arg;
    int32_t my_pod = dpumesh_get_pod_id(g_ctx);
    int W = w->inflight > 0 ? w->inflight : 1;

    inflight_slot_t *fl = calloc((size_t)W, sizeof(inflight_slot_t));
    if (!fl) { atomic_store(w->stop, 1); return NULL; }

    long next_j = 0;       /* next logical request to launch */
    long completed = 0;    /* launched-and-resolved (ok + fail) */
    const double timeout_s = (double)WAIT_TIMEOUT_MS / 1000.0;

    while (completed < w->budget) {
        if (atomic_load(w->stop)) break;
        int did_work = 0;

        /* ---- 1. Launch due requests into free window slots ---- */
        for (int s = 0; s < W && next_j < w->budget; s++) {
            if (fl[s].active) continue;
            double scheduled = w->start_at + (double)next_j * w->interval_sec;
            if (now_sec() < scheduled) break;   /* paced: nothing newer is due */

            uint32_t req_id = dpumesh_alloc_req_id(g_ctx);
            int tx_slot = dpumesh_tx_alloc(g_ctx);
            if (tx_slot < 0)
                break;   /* TX-slot backpressure (not a failure): retry next sweep */

            if (dpumesh_register_pending(g_ctx, req_id) < 0) {
                dpumesh_tx_free(g_ctx, tx_slot);
                atomic_fetch_add(w->fail, 1);
                next_j++; completed++; did_work = 1;
                continue;
            }

            uint8_t *buf = dpumesh_tx_buf(g_ctx, tx_slot);
            /* TRANSPORT-ONLY: fill only the 3 validated bytes (not an 8KB memset). */
            { uint8_t p = (uint8_t)('A' + (next_j & 0xf));
              buf[0] = p; buf[w->msg_size / 2] = p; buf[w->msg_size - 1] = p; }

            sw_descriptor_t desc;
            memset(&desc, 0, sizeof(desc));
            desc.header_buf_slot     = -1;
            desc.body_buf_slot       = tx_slot;
            desc.body_len            = (uint32_t)w->msg_size;
            desc.req_id              = req_id;
            desc.dst_pod_id          = g_dst_pod_id;
            desc.src_pod_id          = my_pod;
            desc.flags               = OP_REQUEST | CASE_EXTERNAL;
            desc.valid               = 1;

            if (dpumesh_enqueue(g_ctx, &desc) < 0) {
                dpumesh_tx_free(g_ctx, tx_slot);
                dpumesh_cancel_pending(g_ctx, req_id);
                atomic_fetch_add(w->fail, 1);
                next_j++; completed++; did_work = 1;
                continue;
            }
            dpumesh_pending_attach_tx(g_ctx, req_id, tx_slot);

            fl[s].req_id    = req_id;
            fl[s].scheduled = scheduled;
            fl[s].launched  = now_sec();
            fl[s].j         = next_j;
            fl[s].active    = 1;
            next_j++;
            did_work = 1;
        }

        /* ---- 2. Harvest completed (and time out stalled) slots ---- */
        for (int s = 0; s < W; s++) {
            if (!fl[s].active) continue;
            sw_descriptor_t resp;
            int r = dpumesh_poll_response(g_ctx, fl[s].req_id, &resp);
            if (r == 1) {
                /* Still in flight — enforce the same wall-clock timeout as the
                 * blocking path so a lost response can't wedge the window. */
                if (now_sec() - fl[s].launched > timeout_s) {
                    dpumesh_cancel_pending(g_ctx, fl[s].req_id);
                    atomic_fetch_add(w->fail, 1);
                    fl[s].active = 0; completed++; did_work = 1;
                }
                continue;
            }
            if (r == 0) {
                int bad = 0;
                if (resp.body_buf_slot >= 0) {
                    const uint8_t *rb = dpumesh_rx_buf(g_ctx, resp.body_buf_slot);
                    uint8_t expect = (uint8_t)('A' + (fl[s].j & 0xf));
                    uint32_t bl = resp.body_len;
                    bad = (rb == NULL) ||
                          (bl != (uint32_t)w->msg_size) ||
                          (bl > 0 && (rb[0] != expect || rb[bl / 2] != expect ||
                                      rb[bl - 1] != expect));
                    dpumesh_rx_free(g_ctx, resp.body_buf_slot);
                }
                if (bad) {
                    atomic_fetch_add(w->fail, 1);
                } else {
                    double lat_us = (now_sec() - fl[s].scheduled) * 1e6;
                    if (w->n_samples < w->cap)
                        w->samples[w->n_samples++] = lat_us;
                    atomic_fetch_add(w->ok, 1);
                }
            } else {
                /* r == -1: abandoned/error */
                dpumesh_cancel_pending(g_ctx, fl[s].req_id);
                atomic_fetch_add(w->fail, 1);
            }
            fl[s].active = 0; completed++; did_work = 1;
        }

        /* ---- 3. Yield when a full sweep made no progress (lean when idle) ---- */
        if (!did_work) {
            struct timespec ts = {0, 5000};   /* 5 us */
            nanosleep(&ts, NULL);
        }
    }

    /* Drain slots still active at stop/deadline: harvest if arrived, else cancel
     * so the transport reclaims the TX slot (no leak across back-to-back runs). */
    for (int s = 0; s < W; s++) {
        if (!fl[s].active) continue;
        sw_descriptor_t resp;
        if (dpumesh_poll_response(g_ctx, fl[s].req_id, &resp) == 0) {
            if (resp.body_buf_slot >= 0) dpumesh_rx_free(g_ctx, resp.body_buf_slot);
        } else {
            dpumesh_cancel_pending(g_ctx, fl[s].req_id);
        }
    }
    free(fl);
    return NULL;
}

/* ------------------------------------------------------------ watchdog  */

/* Hard-deadline watchdog: set *stop=1 after deadline_sec elapses, but exit
 * early if main signals via *early_exit (workers finished naturally). */
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

    /* Concurrency model: a few generator threads, each holding a window of
     * `inflight` outstanding requests, for the target total concurrency C. */
    int n_workers = g_async_threads;
    if (n_workers < 1) n_workers = 1;
    if (n_workers > MAX_WORKERS) n_workers = MAX_WORKERS;
    int C = (conns > 0) ? conns : (rps / 100);   /* target total concurrency */
    if (C < 1) C = 1;
    int inflight = (C + n_workers - 1) / n_workers;   /* per-thread window */
    if (inflight < 1) inflight = 1;

    /* Spread RPS evenly across workers */
    long total_budget = (long)rps * (long)dur;
    long per_worker   = total_budget / n_workers;
    long remainder    = total_budget % n_workers;
    double interval_sec = (double)n_workers / (double)rps;

    fprintf(stderr, "[bench] RUN rps=%d dur=%d size=%d conns=%d "
                    "(workers=%d inflight=%d interval=%.3fus per worker)\n",
            rps, dur, msg_size, conns, n_workers, inflight,
            interval_sec * 1e6);

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

    double start = now_sec() + 0.05;  /* small offset so all workers warm before t=0 */

    /* Small per-thread stack so MAX_WORKERS threads fit in host RAM. */
    pthread_attr_t worker_attr;
    pthread_attr_init(&worker_attr);
    pthread_attr_setstacksize(&worker_attr, WORKER_STACK_BYTES);

    void *(*wfn)(void *) = worker_fn_async;
    for (int i = 0; i < n_workers; i++) {
        wargs[i].worker_id    = i;
        wargs[i].budget       = per_worker + (i < remainder ? 1 : 0);
        wargs[i].interval_sec = interval_sec;
        wargs[i].msg_size     = msg_size;
        wargs[i].inflight     = inflight;
        wargs[i].start_at     = start;
        wargs[i].stop         = &stop;
        wargs[i].ok           = &ok;
        wargs[i].fail         = &fail;
        wargs[i].cap          = (size_t)wargs[i].budget;
        wargs[i].samples      = calloc(wargs[i].cap, sizeof(double));
        wargs[i].n_samples    = 0;
        if (!wargs[i].samples) {
            atomic_store(&stop, 1);
        }
        if (pthread_create(&tids[i], &worker_attr, wfn, &wargs[i]) != 0) {
            atomic_store(&stop, 1);
            tids[i] = 0;
        }
    }
    pthread_attr_destroy(&worker_attr);

    /* Watchdog: hard deadline at dur + DRAIN_GRACE_SEC. Main thread joins
     * workers immediately; if they beat the deadline, signal the watchdog. */
    atomic_int wd_early = 0;
    wd_arg_t wa;
    wa.stop         = &stop;
    wa.early_exit   = &wd_early;
    wa.deadline_sec = dur + DRAIN_GRACE_SEC;
    pthread_t wd_tid;
    pthread_create(&wd_tid, NULL, watchdog_fn, &wa);

    for (int i = 0; i < n_workers; i++)
        if (tids[i]) pthread_join(tids[i], NULL);

    /* Workers all finished — let the watchdog exit early so we don't wait
     * out the rest of its deadline. */
    atomic_store(&wd_early, 1);
    pthread_join(wd_tid, NULL);

    double wall = now_sec() - start;
    if (wall < 1e-6) wall = 1e-6;

    /* Pool latency samples */
    size_t total = 0;
    for (int i = 0; i < n_workers; i++) total += wargs[i].n_samples;
    double *all = calloc(total ? total : 1, sizeof(double));
    size_t off = 0;
    for (int i = 0; i < n_workers; i++) {
        if (wargs[i].n_samples) {
            memcpy(all + off, wargs[i].samples,
                   wargs[i].n_samples * sizeof(double));
            off += wargs[i].n_samples;
        }
        free(wargs[i].samples);
    }
    qsort(all, total, sizeof(double), cmp_double);

    long ok_n   = atomic_load(&ok);
    long fail_n = atomic_load(&fail);
    double rps_ach = (double)ok_n / wall;
    /* RTT throughput: req+resp body, approx */
    double mb_s = (rps_ach * (double)msg_size * 2.0) / (1024.0 * 1024.0);
    double p50  = pct(all, total, 0.50);
    double p99  = pct(all, total, 0.99);
    double p999 = pct(all, total, 0.999);

    free(all);
    free(tids);
    free(wargs);

    int n = snprintf(reply, sizeof(reply),
                     "OK %.1f %.1f %.1f %.1f %ld %ld %.2f\n",
                     rps_ach, p50, p99, p999, ok_n, fail_n, mb_s);
    write(conn_fd, reply, (size_t)n);
    fprintf(stderr, "[bench] DONE %s", reply);
}

/* ------------------------------------------------------------ control TCP */

static int parse_run_line(const char *line,
                          int *rps, int *dur, int *size, int *conns) {
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
    /* trim */
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
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(fd); return -1;
    }
    if (listen(fd, 4) < 0) {
        perror("listen"); close(fd); return -1;
    }
    return fd;
}

/* ------------------------------------------------------------ main        */

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    signal(SIGPIPE, SIG_IGN);

    int worker_id = 10;
    if (getenv("BENCH_WORKER_ID"))
        worker_id = atoi(getenv("BENCH_WORKER_ID"));
    if (getenv("BENCH_DST_POD_ID"))
        g_dst_pod_id = atoi(getenv("BENCH_DST_POD_ID"));
    if (getenv("ASYNC_THREADS"))
        g_async_threads = atoi(getenv("ASYNC_THREADS"));

    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    cfg.async_client = 1;   /* bench is a poll-based async client */
    int rc = dpumesh_init(&g_ctx, "bench-dpumesh", worker_id, &cfg);
    if (rc != 0 || !g_ctx) {
        fprintf(stderr, "[bench] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    fprintf(stderr, "[bench] ready: pod_id=%d, dst_pod_id=%d\n",
            dpumesh_get_pod_id(g_ctx), g_dst_pod_id);

    int srv = ctrl_listen(CTRL_PORT);
    if (srv < 0) return 1;
    fprintf(stderr, "[bench] control LISTEN on :%d\n", CTRL_PORT);

    while (1) {
        int conn = accept(srv, NULL, NULL);
        if (conn < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        handle_ctrl(conn);
    }
    /* unreachable */
}
