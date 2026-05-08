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
/* MAX_WORKERS sets the upper bound on closed-loop concurrency. With wrk2-style
 * scheduled-time latency measurement (t0 = scheduled, not now-when-sent), the
 * cap no longer hides saturation in the latency tail — but a low cap still
 * limits achievable throughput to MAX_WORKERS / mean_latency. 4096 is large
 * enough that for any workload where p99 < 75ms at 50K rps, the bench is not
 * the bottleneck. Stack size is shrunk to 128KB so 4096 × 128KB = 512MB worth
 * of stacks, well within 30GB host RAM. */
#define MAX_WORKERS        4096
#define WORKER_STACK_BYTES (128 * 1024)
#define WAIT_TIMEOUT_MS    5000
#define DRAIN_GRACE_SEC    5

static dpumesh_ctx_t *g_ctx = NULL;
static int            g_dst_pod_id = 11;

/* ------------------------------------------------------------ time helpers */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void sleep_until(double target) {
    double now = now_sec();
    if (target <= now) return;
    double diff = target - now;
    struct timespec ts;
    ts.tv_sec  = (time_t)diff;
    ts.tv_nsec = (long)((diff - (double)ts.tv_sec) * 1e9);
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------ per-worker  */

typedef struct {
    int        worker_id;
    long       budget;          /* total req count for this worker */
    double     interval_sec;
    int        msg_size;
    double     start_at;        /* common wall start */
    atomic_int *stop;
    atomic_long *ok;
    atomic_long *fail;
    /* per-worker latency log (us) */
    double    *samples;
    size_t     n_samples;
    size_t     cap;
} worker_t;

static void *worker_fn(void *arg) {
    worker_t *w = (worker_t *)arg;
    int32_t my_pod = dpumesh_get_pod_id(g_ctx);

    for (long i = 0; i < w->budget; i++) {
        if (atomic_load(w->stop)) break;

        /* wrk2-style scheduled-time semantics. Latency is measured from the
         * tick this request was *supposed* to fire, not from when the worker
         * actually got around to sending it. Fixes coordinated omission:
         * when service slows down, this iter's scheduled is far in the past,
         * sleep_until returns immediately, and t0 = scheduled captures the
         * full queuing wait + service time — i.e. what a real client paced
         * at this rate would observe, not just the part visible to a worker
         * that froze in lockstep with the system. */
        double scheduled = w->start_at + (double)i * w->interval_sec;
        sleep_until(scheduled);

        double t0 = scheduled;

        uint32_t req_id = dpumesh_alloc_req_id(g_ctx);
        int tx_slot = dpumesh_tx_alloc(g_ctx);
        if (tx_slot < 0) {
            atomic_fetch_add(w->fail, 1);
            continue;
        }

        if (dpumesh_register_pending(g_ctx, req_id) < 0) {
            dpumesh_tx_free(g_ctx, tx_slot);
            atomic_fetch_add(w->fail, 1);
            continue;
        }

        uint8_t *buf = dpumesh_tx_buf(g_ctx, tx_slot);
        /* Fill with deterministic pattern (cheap) */
        memset(buf, (int)('A' + (i & 0xf)), (size_t)w->msg_size);

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
        desc.src_body_pool_type  = POOL_HOST_TX_BODY;
        desc.src_body_pod_id     = my_pod;
        desc.src_body_buf_slot   = tx_slot;
        desc.src_header_buf_slot = -1;

        if (dpumesh_enqueue(g_ctx, &desc) < 0) {
            dpumesh_tx_free(g_ctx, tx_slot);
            dpumesh_cancel_pending(g_ctx, req_id);
            atomic_fetch_add(w->fail, 1);
            continue;
        }
        dpumesh_pending_attach_tx(g_ctx, req_id, tx_slot);

        sw_descriptor_t resp;
        if (dpumesh_wait_response(g_ctx, req_id, &resp, WAIT_TIMEOUT_MS) < 0) {
            dpumesh_cancel_pending(g_ctx, req_id);
            atomic_fetch_add(w->fail, 1);
            continue;
        }
        if (resp.body_buf_slot >= 0)
            dpumesh_rx_free(g_ctx, resp.body_buf_slot);

        double lat_us = (now_sec() - t0) * 1e6;
        if (w->n_samples < w->cap)
            w->samples[w->n_samples++] = lat_us;
        atomic_fetch_add(w->ok, 1);
    }
    return NULL;
}

/* ------------------------------------------------------------ watchdog  */

/* Hard-deadline watchdog: set *stop=1 after deadline_sec elapses, but exit
 * early if main signals via *early_exit (workers finished naturally). This
 * lets wall time reflect actual completion when workers beat the deadline,
 * instead of being floored by a fixed sleep. Polling at 100ms is fine —
 * we never need sub-second resolution on the deadline. */
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

    int n_workers = (conns > 0) ? conns : (rps / 100);
    if (n_workers < 1) n_workers = 1;
    if (n_workers > MAX_WORKERS) n_workers = MAX_WORKERS;

    /* Spread RPS evenly across workers */
    long total_budget = (long)rps * (long)dur;
    long per_worker   = total_budget / n_workers;
    long remainder    = total_budget % n_workers;
    double interval_sec = (double)n_workers / (double)rps;

    fprintf(stderr, "[bench] RUN rps=%d dur=%d size=%d conns=%d (workers=%d, "
                    "interval=%.3fus per worker)\n",
            rps, dur, msg_size, conns, n_workers, interval_sec * 1e6);

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

    /* Use small per-thread stack — n_workers can reach MAX_WORKERS=4096, and
     * the default 8MB stack would consume ~32GB. 128KB is plenty for our
     * worker_fn locals (a sw_descriptor_t is 64B, no recursion, no big arrays). */
    pthread_attr_t worker_attr;
    pthread_attr_init(&worker_attr);
    pthread_attr_setstacksize(&worker_attr, WORKER_STACK_BYTES);

    for (int i = 0; i < n_workers; i++) {
        wargs[i].worker_id    = i;
        wargs[i].budget       = per_worker + (i < remainder ? 1 : 0);
        wargs[i].interval_sec = interval_sec;
        wargs[i].msg_size     = msg_size;
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
        if (pthread_create(&tids[i], &worker_attr, worker_fn, &wargs[i]) != 0) {
            atomic_store(&stop, 1);
            tids[i] = 0;
        }
    }
    pthread_attr_destroy(&worker_attr);

    /* Watchdog: hard deadline at dur + DRAIN_GRACE_SEC.
     * Main thread joins workers immediately so wall time reflects actual
     * completion. If workers beat the deadline, signal watchdog to exit.
     * Without this, a fixed sleep(deadline) inflates wall time and the
     * reported RPS is artificially capped at ok / deadline. */
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

    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
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
