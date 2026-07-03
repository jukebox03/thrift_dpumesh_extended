/*
 * tcp_client.c — a deliberately VANILLA blocking TCP client, driven over stdin.
 *
 * No DPUmesh headers: socket/connect/write/read/close + pthreads only. The same
 * binary runs over kernel TCP, and over DPUmesh under
 * LD_PRELOAD=libdmesh_preload.so with DMESH_PRELOAD_MAP=<port>=<svc>.
 *
 * THREAD-PER-CONN: every RUN opens <conns> FRESH connections and drives each
 * from its OWN thread with blocking single-outstanding round trips — the classic
 * thread-per-conn shape, so aggregate throughput scales with <conns> instead of
 * being serialized at 1/RTT (the previous single-thread client measured latency
 * only). Also exercises the shim's multi-threaded blocking paths for real.
 *
 * It stays a single long-lived PROCESS (one dmesh channel = one DPU pod
 * registration) while every RUN opens fresh connections — so conn churn
 * (connect/FIN/close) is exercised per RUN without burning registration slots.
 *
 * Usage:  tcp_client <host> <port>
 * stdin:  RUN <n_msgs> <size> <conns>\n     (repeatable; n_msgs split across conns)
 *         QUIT\n (or EOF)
 * stdout: RESULT <ok> <fail> <p50us> <p99us> <rps>\n   (one per RUN;
 *         rps = ok / wall-time of the whole RUN incl. connect+close)
 *
 * Per message: fill a per-(msg,offset) pattern, write it all, read exactly
 * <size> bytes back (short reads legal — byte stream), verify every byte.
 * A content mismatch or premature EOF counts as fail.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define MAX_CONNS 1024
#define MAX_SIZE  (1 << 20)

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static int write_full(int fd, const char *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, buf + done, len - done);
        if (n > 0) { done += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

/* 0 = ok, -1 = EOF/error, -2 = timeout (SO_RCVTIMEO expired — reply lost). */
static int read_full(int fd, char *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, buf + done, len - done);
        if (n > 0) { done += (size_t)n; continue; }
        if (n == 0) return -1;                     /* EOF mid-message */
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
        return -1;
    }
    return 0;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return x < y ? -1 : x > y;
}

typedef struct {
    struct sockaddr_in sin;
    long idx;               /* connection index (diagnostics) */
    long size;
    long base, quota;       /* this thread owns messages [base, base+quota) */
    uint32_t *lat;          /* slice of the global array: lat[base..], nlat used */
    long nlat;
    unsigned long long ok, fail;
} worker_t;

static void *worker_fn(void *arg) {
    worker_t *w = (worker_t *)arg;
    char *out = malloc((size_t)w->size), *in = malloc((size_t)w->size);
    int fd = -1;
    if (!out || !in) goto all_fail;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0 || connect(fd, (struct sockaddr *)&w->sin, sizeof w->sin) < 0)
        goto all_fail;
    /* A lost message must become a COUNTED FAILURE, not a permanent hang: over a
     * transport that can drop (unlike kernel TCP loopback) an unbounded blocking
     * read would wedge the whole RUN. Plain POSIX — stays vanilla. */
    struct timeval tv = { 5, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

    for (long k = 0; k < w->quota; k++) {
        long i = w->base + k;                      /* global msg index → unique pattern */
        for (long j = 0; j < w->size; j++)
            out[j] = (char)(0x5A ^ (i * 131) ^ (j * 7));
        uint64_t t0 = now_us();
        if (write_full(fd, out, (size_t)w->size) < 0) { w->fail++; continue; }
        int r = read_full(fd, in, (size_t)w->size);
        if (r == -2) {                             /* reply lost → conn is suspect: stop it */
            fprintf(stderr, "tcp_client: TIMEOUT conn=%ld fd=%d at msg %ld/%ld\n",
                    w->idx, fd, k, w->quota);
            w->fail++;
            break;
        }
        if (r < 0)                                    { w->fail++; continue; }
        if (memcmp(out, in, (size_t)w->size) != 0)    { w->fail++; continue; }
        uint64_t dt = now_us() - t0;
        w->lat[w->nlat++] = dt > 0xFFFFFFFFull ? 0xFFFFFFFFu : (uint32_t)dt;
        w->ok++;
    }
    close(fd);
    free(out); free(in);
    return NULL;

all_fail:                                          /* no conn → whole quota fails */
    w->fail += (unsigned long long)w->quota;
    if (fd >= 0) close(fd);
    free(out); free(in);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: %s <host> <port>\n", argv[0]); return 1; }
    const char *host = argv[1];
    int port = atoi(argv[2]);
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);

    char line[256];
    static worker_t w[MAX_CONNS];
    static pthread_t th[MAX_CONNS];

    while (fgets(line, sizeof line, stdin)) {
        long n_msgs, size, conns;
        if (strncmp(line, "QUIT", 4) == 0) break;
        if (sscanf(line, "RUN %ld %ld %ld", &n_msgs, &size, &conns) != 3) {
            printf("RESULT 0 0 0 0 0\n");
            continue;
        }
        if (conns < 1) conns = 1;
        if (conns > MAX_CONNS) conns = MAX_CONNS;
        if (size < 1) size = 1;
        if (size > MAX_SIZE) size = MAX_SIZE;
        if (n_msgs < 1) n_msgs = 1;
        if (conns > n_msgs) conns = n_msgs;        /* ≥1 msg per thread */

        uint32_t *lat = malloc((size_t)n_msgs * sizeof(uint32_t));
        if (!lat) { printf("RESULT 0 %ld 0 0 0\n", n_msgs); continue; }

        struct sockaddr_in sin;
        memset(&sin, 0, sizeof sin);
        sin.sin_family = AF_INET;
        sin.sin_port = htons((uint16_t)port);
        inet_pton(AF_INET, host, &sin.sin_addr);

        long per = n_msgs / conns, rem = n_msgs % conns, base = 0;
        uint64_t t0 = now_us();
        long started = 0;
        for (long c = 0; c < conns; c++) {
            w[c] = (worker_t){ .sin = sin, .idx = c, .size = size, .base = base,
                               .quota = per + (c < rem ? 1 : 0),
                               .lat = lat + base, .nlat = 0, .ok = 0, .fail = 0 };
            base += w[c].quota;
            if (pthread_create(&th[c], NULL, worker_fn, &w[c]) != 0) {
                w[c].fail = (unsigned long long)w[c].quota;   /* count, don't join */
                th[c] = 0;
                continue;
            }
            started++;
        }
        unsigned long long ok = 0, fail = 0;
        long nlat = 0;
        for (long c = 0; c < conns; c++) {
            if (th[c]) pthread_join(th[c], NULL);
            ok += w[c].ok; fail += w[c].fail;
        }
        uint64_t elapsed = now_us() - t0;
        (void)started;

        /* compact per-thread latency slices into one contiguous run for qsort */
        for (long c = 0; c < conns; c++) {
            if (w[c].lat != lat + nlat)
                memmove(lat + nlat, w[c].lat, (size_t)w[c].nlat * sizeof(uint32_t));
            nlat += w[c].nlat;
        }

        uint32_t p50 = 0, p99 = 0;
        if (nlat > 0) {
            qsort(lat, (size_t)nlat, sizeof(uint32_t), cmp_u32);
            p50 = lat[nlat / 2];
            p99 = lat[(long)((double)nlat * 0.99) < nlat ? (long)((double)nlat * 0.99) : nlat - 1];
        }
        unsigned long rps = elapsed ? (unsigned long)((double)ok * 1e6 / (double)elapsed) : 0;
        free(lat);
        printf("RESULT %llu %llu %u %u %lu\n", ok, fail, p50, p99, rps);
    }
    return 0;
}
