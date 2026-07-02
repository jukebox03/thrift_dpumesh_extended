/*
 * tcp_client.c — a deliberately VANILLA blocking TCP client, driven over stdin.
 *
 * No DPUmesh headers: socket/connect/write/read/close only. The same binary
 * runs over kernel TCP, and over DPUmesh under LD_PRELOAD=libdmesh_preload.so
 * with DMESH_PRELOAD_MAP=<port>=<svc>.
 *
 * It stays a single long-lived PROCESS (one dmesh channel = one DPU pod
 * registration) while every RUN opens FRESH connections — so conn churn
 * (connect/FIN/close) is exercised per RUN without burning registration slots.
 *
 * Usage:  tcp_client <host> <port>
 * stdin:  RUN <n_msgs> <size> <conns>\n     (repeatable)
 *         QUIT\n (or EOF)
 * stdout: RESULT <ok> <fail> <p50us> <p99us>\n   (one per RUN)
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

static int read_full(int fd, char *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, buf + done, len - done);
        if (n > 0) { done += (size_t)n; continue; }
        if (n == 0) return -1;                     /* EOF mid-message */
        if (errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: %s <host> <port>\n", argv[0]); return 1; }
    const char *host = argv[1];
    int port = atoi(argv[2]);
    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);

    char line[256];
    char *out = malloc(MAX_SIZE), *in = malloc(MAX_SIZE);
    if (!out || !in) { fprintf(stderr, "oom\n"); return 1; }

    while (fgets(line, sizeof line, stdin)) {
        long n_msgs, size, conns;
        if (strncmp(line, "QUIT", 4) == 0) break;
        if (sscanf(line, "RUN %ld %ld %ld", &n_msgs, &size, &conns) != 3) {
            printf("RESULT 0 0 0 0\n");
            continue;
        }
        if (conns < 1) conns = 1;
        if (conns > MAX_CONNS) conns = MAX_CONNS;
        if (size < 1) size = 1;
        if (size > MAX_SIZE) size = MAX_SIZE;
        if (n_msgs < 1) n_msgs = 1;

        int fds[MAX_CONNS];
        long live = 0;
        struct sockaddr_in sin;
        memset(&sin, 0, sizeof sin);
        sin.sin_family = AF_INET;
        sin.sin_port = htons((uint16_t)port);
        inet_pton(AF_INET, host, &sin.sin_addr);
        for (long i = 0; i < conns; i++) {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0 || connect(fd, (struct sockaddr *)&sin, sizeof sin) < 0) {
                if (fd >= 0) close(fd);
                break;
            }
            fds[live++] = fd;
        }
        if (live == 0) { printf("RESULT 0 %ld 0 0\n", n_msgs); continue; }

        uint32_t *lat = malloc((size_t)n_msgs * sizeof(uint32_t));
        uint64_t ok = 0, fail = 0;
        long nlat = 0;

        for (long i = 0; i < n_msgs; i++) {
            int fd = fds[i % live];
            for (long j = 0; j < size; j++)
                out[j] = (char)(0x5A ^ (i * 131) ^ (j * 7));
            uint64_t t0 = now_us();
            if (write_full(fd, out, (size_t)size) < 0) { fail++; continue; }
            if (read_full(fd, in, (size_t)size) < 0)   { fail++; continue; }
            if (memcmp(out, in, (size_t)size) != 0)    { fail++; continue; }
            uint64_t dt = now_us() - t0;
            if (lat && nlat < n_msgs) lat[nlat++] = dt > 0xFFFFFFFFull ? 0xFFFFFFFFu : (uint32_t)dt;
            ok++;
        }

        for (long i = 0; i < live; i++) close(fds[i]);

        uint32_t p50 = 0, p99 = 0;
        if (lat && nlat > 0) {
            qsort(lat, (size_t)nlat, sizeof(uint32_t), cmp_u32);
            p50 = lat[nlat / 2];
            p99 = lat[(long)((double)nlat * 0.99) < nlat ? (long)((double)nlat * 0.99) : nlat - 1];
        }
        free(lat);
        printf("RESULT %llu %llu %u %u\n",
               (unsigned long long)ok, (unsigned long long)fail, p50, p99);
    }
    free(out); free(in);
    return 0;
}
