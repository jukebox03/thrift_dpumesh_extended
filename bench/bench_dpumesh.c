/*
 * bench_dpumesh.c — Single-thread open-loop saturation load generator (v5).
 *
 * Why this shape (vs old multi-thread paced bench):
 *   plan.md §3 flame analysis showed the old bench burned ~90% of its CPU
 *   in CFS scheduler bookkeeping driven by 50–4096 worker threads each
 *   doing per-fire clock_nanosleep + per-reap futex_wait. dpumesh transport
 *   itself was 2.5–2.8% — the measured cap was the bench's thread model,
 *   not the transport. This rewrite removes that model entirely:
 *     - 1 worker thread (was N) → no scheduler thrash.
 *     - No rate pacing (sleep_until removed) → open-loop saturation.
 *     - Deep K-ring of in-flight RPCs → covers RTT, throughput-bound.
 *     - Non-blocking reap via wait_response_v2(timeout=0) → atomic load,
 *       no syscall on the happy path.
 *     - When ring is full and head not ready, block on head via
 *       wait_response_v2(timeout=ms) → futex_wait yields the core to the
 *       PE thread (which shares the 1 core in plan.md baseline). Exactly
 *       one futex per ring rotation, not per RPC.
 *
 * What this measures:
 *   "How many RPC/sec can dpumesh transport sustain when bench-side
 *    overhead is removed?" Throughput cap and steady-state latency
 *    quantiles at saturation.
 *
 * Protocol (compatible with test-bench.sh's existing RUN line):
 *   RUN <rps_hint> <dur_sec> <msg_size> [<K>]
 *     - rps_hint: ignored (saturation is open-loop). Kept for
 *       script compat; the original arg position.
 *     - dur_sec : how long to fire (drain phase follows automatically).
 *     - msg_size: bytes per request body.
 *     - K       : ring depth (in-flight cap). 0 → default 256.
 *   Response: "OK <rps_achieved> <p50_us> <p99_us> <p999_us> <ok> <fail> <mb_per_sec>\n"
 *
 * Always uses the Phase 3 split path (dpumesh_send_request /
 * wait_response_v2). Legacy BENCH_SPLIT=0 mode is no longer exercised
 * by this binary; the legacy transport code still lives in
 * dpumesh_doca.c for Hard Rule #2 compliance.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <thrift/transport/dpumesh.h>
#include <thrift/transport/doca/mesh.h>

#define CTRL_PORT        9092
#define DEFAULT_K        256
#define MAX_K            4096
#define WAIT_TIMEOUT_MS  5000
#define SAMPLES_RING     (1u << 20)   /* 1M latency samples; circular */

static dpumesh_ctx_t *g_ctx = NULL;
static char           g_service[64] = "echo";
static int            g_default_k   = DEFAULT_K;

/* ------------------------------------------------------------ time */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------ sample log */

/* Circular latency log. Captures the most recent SAMPLES_RING samples.
 * For runs that record >> SAMPLES_RING samples, percentiles reflect
 * steady-state. For shorter runs (e.g. K=1 at low RPS), the buffer
 * contains every sample, so warmup is included — strip the first K
 * samples at percentile time to keep ring-fill out of the quantiles. */
typedef struct {
    double  *buf;
    uint64_t total;
    size_t   head;
} sample_log_t;

static int log_init(sample_log_t *L) {
    L->buf = calloc(SAMPLES_RING, sizeof(double));
    if (!L->buf) return -1;
    L->total = 0;
    L->head = 0;
    return 0;
}

static inline void log_push(sample_log_t *L, double lat_us) {
    L->buf[L->head] = lat_us;
    L->head = (L->head + 1) % SAMPLES_RING;
    L->total++;
}

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* Compute p50/p99/p999, dropping the first `skip` samples in arrival order. */
static void log_pct(sample_log_t *L, size_t skip,
                    double *p50, double *p99, double *p999) {
    size_t in_buf = (L->total < SAMPLES_RING) ? (size_t)L->total : SAMPLES_RING;
    size_t skip_in_buf = (L->total < SAMPLES_RING) ? skip : 0;
    if (skip_in_buf >= in_buf) skip_in_buf = 0;
    size_t n = in_buf - skip_in_buf;
    if (n == 0) { *p50 = *p99 = *p999 = 0; return; }

    double *tmp = malloc(n * sizeof(double));
    if (!tmp) { *p50 = *p99 = *p999 = 0; return; }

    if (L->total < SAMPLES_RING) {
        /* Buffer wasn't wrapped; samples are at [0 .. total). */
        memcpy(tmp, L->buf + skip_in_buf, n * sizeof(double));
    } else {
        /* Wrapped. Oldest sample is at L->head, newest at head-1. We don't
         * skip in this case (n >> K, warmup already pushed out). */
        size_t first = L->head;
        size_t part1 = SAMPLES_RING - first;
        memcpy(tmp, L->buf + first, part1 * sizeof(double));
        memcpy(tmp + part1, L->buf, first * sizeof(double));
    }

    qsort(tmp, n, sizeof(double), cmp_double);
    *p50  = tmp[(size_t)(0.50  * (n - 1))];
    *p99  = tmp[(size_t)(0.99  * (n - 1))];
    *p999 = tmp[(size_t)(0.999 * (n - 1))];
    free(tmp);
}

/* ------------------------------------------------------------ saturation */

typedef struct {
    struct mesh_req_id rid;
    double             fire_t;
} slot_t;

/* Saturation loop. Single thread, fires up to K in-flight, reaps in FIFO
 * order. Returns wall time, ok/fail counts, latency samples via out params. */
static void saturate(int K, int dur_sec, int msg_size,
                     uint64_t *out_ok, uint64_t *out_fail,
                     double *out_wall, sample_log_t *L) {
    slot_t *ring = calloc((size_t)K, sizeof(slot_t));
    if (!ring) { *out_ok = 0; *out_fail = 0; *out_wall = 0; return; }
    int head = 0, tail = 0, count = 0;
    uint64_t ok = 0, fail = 0;

    uint8_t body[MESH_CHUNK_BODY_BUDGET];
    uint32_t blen = (uint32_t)msg_size;
    if (blen > sizeof(body)) blen = sizeof(body);
    memset(body, 'A', blen);

    double t0 = now_sec();
    double deadline = t0 + (double)dur_sec;
    int stop_send = 0;

    while (1) {
        if (!stop_send && now_sec() >= deadline) stop_send = 1;

        /* 1. Non-blocking FIFO reap. wait_response_v2(timeout=0) is a pure
         *    atomic load on state — no syscall if not ready. */
        int reaped = 0;
        while (count > 0) {
            sw_descriptor_t resp;
            int rc = dpumesh_wait_response_v2(g_ctx, ring[head].rid, &resp, 0);
            if (rc < 0) break;
            if (resp.body_buf_slot >= 0)
                dpumesh_rx_free(g_ctx, resp.body_buf_slot);
            log_push(L, (now_sec() - ring[head].fire_t) * 1e6);
            ok++;
            head = (head + 1) % K;
            count--;
            reaped++;
        }

        /* 2. Termination: no more sends + ring empty. */
        if (stop_send && count == 0) break;

        /* 3. Fire while ring has room. send_request may block internally on
         *    tx/hdr slot cond_wait when the transport is saturated — that's
         *    desired backpressure (gives PE the core to drain). */
        int fired = 0;
        while (!stop_send && count < K) {
            struct mesh_req_id rid;
            int rc = dpumesh_send_request(g_ctx, g_service, body, blen,
                                          OP_REQUEST, &rid);
            if (rc < 0) { fail++; break; }
            ring[tail].rid = rid;
            ring[tail].fire_t = now_sec();
            tail = (tail + 1) % K;
            count++;
            fired++;
        }

        /* 4. If we made no progress (no reap + no fire) and there's something
         *    in flight, block on the head with bounded wait. futex_wait
         *    inside wait_response_v2 yields the core to PE thread — this is
         *    the only syscall in the hot path, at most once per ring
         *    rotation (i.e. ~RPS/K times/sec). */
        if (reaped == 0 && fired == 0) {
            if (count == 0) {
                /* Quiescent (e.g. drain phase saw stop_send && count==0 above
                 * already; reaching here means stop_send=0 and we can't fire.
                 * Could be transient transport refusal; brief pause.) */
                struct timespec ts = {0, 1000};
                nanosleep(&ts, NULL);
            } else {
                sw_descriptor_t resp;
                int rc = dpumesh_wait_response_v2(g_ctx, ring[head].rid, &resp,
                                                  WAIT_TIMEOUT_MS);
                if (rc < 0) {
                    dpumesh_cancel_pending_v2(g_ctx, ring[head].rid);
                    fail++;
                } else {
                    if (resp.body_buf_slot >= 0)
                        dpumesh_rx_free(g_ctx, resp.body_buf_slot);
                    log_push(L, (now_sec() - ring[head].fire_t) * 1e6);
                    ok++;
                }
                head = (head + 1) % K;
                count--;
            }
        }
    }

    *out_wall = now_sec() - t0;
    *out_ok = ok;
    *out_fail = fail;
    free(ring);
}

/* ------------------------------------------------------------ command */

static void run_test(int conn_fd, int rps_hint, int dur, int msg_size, int K) {
    char reply[256];
    if (dur < 1 || msg_size < 1) {
        const char *e = "ERR bad args (dur>=1, size>=1)\n";
        write(conn_fd, e, strlen(e));
        return;
    }
    if (K < 1) K = g_default_k;
    if (K > MAX_K) K = MAX_K;

    fprintf(stderr,
            "[bench] open-loop saturation: dur=%ds size=%dB K=%d "
            "(rps hint %d ignored)\n", dur, msg_size, K, rps_hint);

    sample_log_t L;
    if (log_init(&L) < 0) {
        const char *e = "ERR oom\n";
        write(conn_fd, e, strlen(e));
        return;
    }

    uint64_t ok = 0, fail = 0;
    double wall = 0;
    saturate(K, dur, msg_size, &ok, &fail, &wall, &L);
    if (wall < 1e-6) wall = 1e-6;

    /* Strip first K samples (ring-fill / warmup) when computing quantiles. */
    double p50 = 0, p99 = 0, p999 = 0;
    log_pct(&L, (size_t)K, &p50, &p99, &p999);
    free(L.buf);

    double rps_ach = (double)ok / wall;
    double mb_s = (rps_ach * (double)msg_size * 2.0) / (1024.0 * 1024.0);

    int n = snprintf(reply, sizeof(reply),
                     "OK %.1f %.1f %.1f %.1f %lu %lu %.2f\n",
                     rps_ach, p50, p99, p999,
                     (unsigned long)ok, (unsigned long)fail, mb_s);
    write(conn_fd, reply, (size_t)n);
    fprintf(stderr, "[bench] DONE %s", reply);
}

static int parse_run_line(const char *line,
                          int *rps, int *dur, int *size, int *K) {
    char cmd[16] = {0};
    *K = 0;
    int n = sscanf(line, "%15s %d %d %d %d", cmd, rps, dur, size, K);
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
        write(conn_fd, "PONG\n", 5);
        close(conn_fd);
        return;
    }

    int rps, dur, size, K;
    if (parse_run_line(buf, &rps, &dur, &size, &K) < 0) {
        const char *e = "ERR bad command (RUN <rps_hint> <dur> <size> [K])\n";
        write(conn_fd, e, strlen(e));
        close(conn_fd);
        return;
    }
    run_test(conn_fd, rps, dur, size, K);
    close(conn_fd);
}

static int ctrl_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(fd); return -1;
    }
    if (listen(fd, 4) < 0) { perror("listen"); close(fd); return -1; }
    return fd;
}

/* ------------------------------------------------------------ main */

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    signal(SIGPIPE, SIG_IGN);

    int worker_id = 10;
    if (getenv("BENCH_WORKER_ID")) worker_id = atoi(getenv("BENCH_WORKER_ID"));
    if (getenv("BENCH_SERVICE"))
        snprintf(g_service, sizeof(g_service), "%s", getenv("BENCH_SERVICE"));
    if (getenv("BENCH_K")) {
        int k = atoi(getenv("BENCH_K"));
        if (k > 0) g_default_k = k;
    }

    fprintf(stderr,
            "[bench] mode=open-loop saturation, service=%s, default K=%d\n",
            g_service, g_default_k);

    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int rc = dpumesh_init(&g_ctx, "bench-dpumesh", worker_id, &cfg);
    if (rc != 0 || !g_ctx) {
        fprintf(stderr, "[bench] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    fprintf(stderr, "[bench] ready: pod_id=%d\n", dpumesh_get_pod_id(g_ctx));

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
}
