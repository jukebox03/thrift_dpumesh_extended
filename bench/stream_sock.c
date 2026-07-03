/*
 * stream_sock.c — byte-stream / L7-proxy validator for the DPU frame mock
 * (plan.md). Proves the L4 engine (dpu_proxy.c) end to end: a per-conn input
 * window, the mock length-prefix parser, per-destination SG-DMA egress, custody,
 * and byte-stream delivery.
 *
 * The DPU must run with DPUMESH_PROXY=frame. The mock understands the wire
 * framing this app emits:
 *
 *     [u32 total_len (incl. this 5B header)][u8 svc][payload ...]
 *
 * and routes EACH whole frame to service_table[svc] as a byte stream (a >8 KB
 * frame is delivered as consecutive <=8 KB chunks — that is the byte-stream
 * contract; the receiver reframes itself). The frame boundary is decided by the
 * DPU parser, NOT by dmesh_write boundaries: this app may pack several frames
 * into one write, or let dmesh_write auto-chunk a large frame across slots — the
 * parser reframes from the length prefix either way (window + seam).
 *
 * Like loopback_sock.c this pod is BOTH the client and (via its echo thread) a
 * server of its OWN service, so the default run is self-contained and its
 * served-BYTE counter gives an exact-count proof with no cross-pod scraping:
 *
 *   client: build frames -> dmesh_write+flush -> read the echoed frames back,
 *           verify BYTE-EXACT (header + payload pattern) + reassembled length.
 *   echo:   accept -> read byte-stream -> echo verbatim -> count bytes served.
 *
 * `RUN <N> <SIZE> [<SVC_LIST>] [<FRAMES_PER_WRITE>]`:
 *   N     round-trips, SIZE  payload bytes per frame (frame = 5 + SIZE),
 *   SVC_LIST comma list of destination service bytes to round-robin the frames
 *            across (default = our own service = pure loopback; e.g. "11,13,14"
 *            fans out to the echo-dpumesh backends — high fan-out load),
 *   FRAMES_PER_WRITE  how many frames to pack into one dmesh_write byte-stream
 *            (default 1; >1 exercises multi-frame-per-window parsing).
 * Reply: `OK <ok> <fail> <served_bytes> <p50us>`.
 *
 * Uses ONLY the façade (dpm.h). No changes to the transport.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
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

#define CTRL_PORT   9092
#define FRAME_HDR   5u                 /* [u32 total_len][u8 svc] — matches the DPU mock */
#define FRAME_MAX   (256u * 1024u)     /* == PX_FRAME_MAX in dpu_proxy.c (mock poison cap) */
#define MAX_SVCS    16
#define MAX_FPW     64                 /* frames packed per write cap */

static dmesh_channel_t *g_s      = NULL;
static int              g_service = 16;    /* own service id (self-routing default) */
static atomic_int       g_stop   = 0;
static atomic_long      g_served = 0;      /* BYTES the echo side wrote back */

static double now_sec(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
static int cmp_d(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

/* Deterministic, position-dependent payload byte so a misrouted / truncated /
 * mis-ordered reply is caught byte-exactly. Keyed by (iteration, dest svc). */
static inline uint8_t pat(long iter, uint8_t svc, uint32_t j) {
    return (uint8_t)(iter * 131u + svc * 17u + j);
}

/* Write one frame into buf: [u32 total][u8 svc][payload]. Returns total bytes. */
static uint32_t build_frame(uint8_t *buf, long iter, uint8_t svc, uint32_t size) {
    uint32_t total = FRAME_HDR + size;
    memcpy(buf, &total, sizeof(total));        /* native LE, as the mock reads it */
    buf[4] = svc;
    for (uint32_t j = 0; j < size; j++)
        buf[FRAME_HDR + j] = pat(iter, svc, j);
    return total;
}

/* ---- server side: drain this pod's accept queue, echo every byte ---- */
static void *echo_fn(void *arg) {
    (void)arg;
    static dmesh_conn_t *cl[4096];
    int ncl = 0;
    static char b[65536];
    while (!atomic_load(&g_stop)) {
        int did = 0;
        dmesh_conn_t *c;
        while (ncl < 4096 && (c = dmesh_accept(g_s)) != NULL) { cl[ncl++] = c; did = 1; }
        for (int i = 0; i < ncl; ) {
            ssize_t n;
            while ((n = dmesh_read(cl[i], b, sizeof b)) > 0) {   /* drain to EAGAIN, echo each chunk */
                dmesh_write(cl[i], b, (size_t)n); dmesh_flush(cl[i]);
                atomic_fetch_add(&g_served, (long)n); did = 1;
            }
            if (n == 0) { dmesh_close(cl[i]); cl[i] = cl[--ncl]; }  /* EOF (client FIN) → drop */
            else i++;
        }
        if (!did) { struct timespec t = {0, 2000}; nanosleep(&t, NULL); }  /* 2us idle */
    }
    for (int i = 0; i < ncl; i++) dmesh_close(cl[i]);
    return NULL;
}

/* ---- client side: N round-trips of framed byte-stream to svc_list ---- */
static void run_stream(int conn_fd, long N, uint32_t size,
                       const int *svcs, int nsvc, int fpw) {
    char reply[192];
    if (N < 1 || nsvc < 1 || fpw < 1 || fpw > MAX_FPW ||
        FRAME_HDR + size > FRAME_MAX) {
        int n = snprintf(reply, sizeof reply,
                         "ERR bad args (size<=%u, fpw<=%d)\n", FRAME_MAX - FRAME_HDR, MAX_FPW);
        write(conn_fd, reply, (size_t)n); return;
    }

    uint32_t frame_bytes = FRAME_HDR + size;
    size_t   burst = (size_t)frame_bytes * (size_t)fpw;   /* bytes per dmesh_write */
    uint8_t *sbuf = malloc(burst), *rbuf = malloc(burst);
    double  *lat  = malloc((size_t)N * sizeof(double));
    if (!sbuf || !rbuf || !lat) { free(sbuf); free(rbuf); free(lat);
        write(conn_fd, "ERR oom\n", 8); return; }

    /* One reusable conn per destination service (a real gateway fans out from one
     * downstream conn; here one conn per dst keeps request/reply demux trivial). */
    dmesh_conn_t *conns[MAX_SVCS] = {0};
    for (int k = 0; k < nsvc; k++) conns[k] = dmesh_connect(g_s, svcs[k]);

    /* Early-abort: a misconfigured run (e.g. an unroutable dst svc) fails every
     * round-trip on the 5 s reply timeout — 20000×5 s would wedge for hours. Bail
     * after a burst of consecutive failures so the operator gets fast feedback. */
    const long MAX_CONSEC_FAIL = 40;
    long consec_fail = 0;

    long ok = 0, fail = 0; size_t ns = 0;
    for (long i = 0; i < N && !atomic_load(&g_stop); i++) {
        if (consec_fail >= MAX_CONSEC_FAIL) {
            fprintf(stderr, "[stream] ABORT after %ld consecutive failures "
                    "(unroutable dst? check svc list vs registered services / DPUMESH_PROXY=frame)\n",
                    consec_fail);
            break;
        }
        int k = (int)(i % nsvc);
        uint8_t svc = (uint8_t)svcs[k];
        dmesh_conn_t *c = conns[k];
        if (!c) { fail++; consec_fail++; continue; }

        /* pack `fpw` frames of this iteration into one byte-stream write */
        for (int f = 0; f < fpw; f++)
            build_frame(sbuf + (size_t)f * frame_bytes, i, svc, size);

        double t0 = now_sec();
        int sent = (dmesh_write(c, sbuf, burst) >= 0 && dmesh_flush(c) >= 0);
        if (!sent) { fail++; consec_fail++; dmesh_close(c); conns[k] = dmesh_connect(g_s, svcs[k]); continue; }

        /* reply is the echoed byte-stream — reassemble exactly `burst` bytes */
        double tw = now_sec(); size_t got = 0; int timedout = 0;
        while (got < burst) {
            ssize_t n = dmesh_read(c, rbuf + got, burst - got);
            if (n > 0) { got += (size_t)n; continue; }
            if (n == 0) break;                              /* peer closed */
            if (now_sec() - tw > 5.0) { timedout = 1; break; }
            sched_yield();                                  /* EAGAIN */
        }
        int bad = timedout || got != burst || memcmp(sbuf, rbuf, burst) != 0;
        if (bad) { fail++; consec_fail++; dmesh_close(c); conns[k] = dmesh_connect(g_s, svcs[k]); }
        else     { ok++; consec_fail = 0; lat[ns++] = (now_sec() - t0) * 1e6; }
    }
    for (int k = 0; k < nsvc; k++) if (conns[k]) dmesh_close(conns[k]);

    qsort(lat, ns, sizeof(double), cmp_d);
    double p50 = ns ? lat[ns / 2] : 0.0;
    int rn = snprintf(reply, sizeof reply, "OK %ld %ld %ld %.1f\n",
                      ok, fail, atomic_load(&g_served), p50);
    write(conn_fd, reply, (size_t)rn);
    fprintf(stderr, "[stream] DONE N=%ld size=%u fpw=%d nsvc=%d ok=%ld fail=%ld served_bytes=%ld p50=%.1fus\n",
            N, size, fpw, nsvc, ok, fail, atomic_load(&g_served), p50);
    free(sbuf); free(rbuf); free(lat);
}

static void handle_ctrl(int fd) {
    char buf[256];
    ssize_t n = read(fd, buf, sizeof buf - 1);
    if (n <= 0) { close(fd); return; }
    buf[n] = '\0';
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';

    char cmd[16] = {0}, svclist[128] = {0};
    long N = 0; uint32_t size = 0; int fpw = 1;
    int m = sscanf(buf, "%15s %ld %u %127s %d", cmd, &N, &size, svclist, &fpw);
    if (m < 3 || strcmp(cmd, "RUN") != 0) {
        write(fd, "ERR use: RUN <N> <SIZE> [<SVC_LIST>] [<FPW>]\n", 45);
        close(fd); return;
    }
    /* default svc list = our own service (pure loopback, self-contained proof).
     * "self"/"-"/empty (and any non-numeric token) → own service, so an omitted
     * SVC_LIST arg is unambiguous even though `RUN N SIZE <fpw>` would otherwise
     * let sscanf slide fpw into the svclist slot. */
    int svcs[MAX_SVCS], nsvc = 0;
    if (m >= 4 && svclist[0] && strcmp(svclist, "self") != 0 && strcmp(svclist, "-") != 0) {
        char *save = NULL, *tok = strtok_r(svclist, ",", &save);
        while (tok && nsvc < MAX_SVCS) { svcs[nsvc++] = atoi(tok); tok = strtok_r(NULL, ",", &save); }
    }
    if (nsvc == 0) { svcs[0] = g_service; nsvc = 1; }
    if (m < 5) fpw = 1;
    run_stream(fd, N, size, svcs, nsvc, fpw);
    close(fd);
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    if (getenv("BENCH_WORKER_ID")) g_service = atoi(getenv("BENCH_WORKER_ID"));

    g_s = dmesh_create_channel(g_service);
    if (!g_s) { fprintf(stderr, "[stream] create_channel failed\n"); return 1; }
    fprintf(stderr, "[stream] ready: pod_id=%d own_service=%d msg_max=%d\n",
            dmesh_pod_id(g_s), g_service, dmesh_msg_max(g_s));

    pthread_t et; pthread_create(&et, NULL, echo_fn, NULL);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
    struct sockaddr_in a; memset(&a, 0, sizeof a);
    a.sin_family = AF_INET; a.sin_port = htons(CTRL_PORT); a.sin_addr.s_addr = INADDR_ANY;
    if (bind(srv, (struct sockaddr *)&a, sizeof a) < 0 || listen(srv, 4) < 0) {
        perror("listen"); return 1;
    }
    fprintf(stderr, "[stream] control LISTEN on :%d\n", CTRL_PORT);
    for (;;) {
        int conn = accept(srv, NULL, NULL);
        if (conn < 0) { if (errno == EINTR) continue; perror("accept"); continue; }
        handle_ctrl(conn);
    }
}
