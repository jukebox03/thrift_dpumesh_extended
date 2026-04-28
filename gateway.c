/*
 * gateway.c - DPUmesh TCP Gateway (Host-side, epoll thread-pool model)
 *
 * Architecture:
 *   - N worker threads (default 32, override via DPUMESH_GATEWAY_WORKERS env).
 *   - Each worker owns its own listen socket on the same port via SO_REUSEPORT.
 *     Kernel hashes incoming conns across workers — no cross-worker fd handoff.
 *   - Each worker runs its own epoll loop (EPOLLET, non-blocking).
 *   - When a complete Thrift frame arrives, the worker handles it synchronously
 *     (dpumesh enqueue + wait_response + send). Head-of-line within a worker is
 *     bounded by the dpumesh service time (~100µs) and N workers parallelize.
 *
 *   This replaces the previous pthread-per-connection model that created
 *   thousands of OS threads at high client concurrency, causing scheduler
 *   thrashing and the throughput-collapse pattern observed at >60k RPS.
 *
 * Build (via test-dpumesh.sh which knows the right libs):
 *   gcc -o gateway gateway.c -Ilib/cpp/src -Lbuild-doca/lib -lthriftd \
 *       -Wl,-rpath,/usr/local/lib -lpthread
 *
 * Run:
 *   sudo DPUMESH_PCI_ADDR=94:00.0 ./gateway
 */

#define _GNU_SOURCE  /* accept4, SOCK_NONBLOCK, MSG_NOSIGNAL */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <thrift/transport/dpumesh.h>

#define GATEWAY_PORT          9091
#define DEFAULT_WORKERS       32
#define MAX_EVENTS            512
#define INITIAL_RECV_BUF      (16 * 1024)   /* grows on demand */
#define MAX_FRAME_SIZE        (4 * 1024 * 1024)
#define RESPONSE_TIMEOUT_MS   30000
#define LISTEN_BACKLOG        4096

/* Admission control: cap concurrent in-flight requests below the dpumesh
 * resource limits (DPU buffer slots = 1024, DMA ring = 1024). When the cap
 * is reached, workers block on g_inflight_cond before reading the next frame
 * from any connection in their epoll set. The worker's epoll loop is paused
 * → TCP recv buffers fill on its connections → kernel TCP advertise window
 * shrinks → clients block on send. End-to-end backpressure with no
 * exception responses, no buffer-full errors, no slot leaks. */
#define GATEWAY_MAX_INFLIGHT  900   /* < 1024 dpumesh slot pool */

static atomic_int g_inflight = 0;
static pthread_mutex_t g_inflight_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_inflight_cond = PTHREAD_COND_INITIALIZER;

static dpumesh_ctx_t *g_ctx = NULL;
static volatile int g_running = 1;
static int g_num_workers = DEFAULT_WORKERS;

/* Block until in-flight count drops below the cap, then reserve a slot.
 * Caller must call admission_release() after the request is fully processed
 * (response sent or terminal failure path). */
static void admission_acquire(void)
{
    pthread_mutex_lock(&g_inflight_lock);
    while (atomic_load_explicit(&g_inflight, memory_order_acquire)
           >= GATEWAY_MAX_INFLIGHT) {
        if (!g_running) {
            pthread_mutex_unlock(&g_inflight_lock);
            return;
        }
        pthread_cond_wait(&g_inflight_cond, &g_inflight_lock);
    }
    atomic_fetch_add_explicit(&g_inflight, 1, memory_order_release);
    pthread_mutex_unlock(&g_inflight_lock);
}

static void admission_release(void)
{
    int prev = atomic_fetch_sub_explicit(&g_inflight, 1, memory_order_release);
    /* Wake one waiter — there's exactly one slot newly available. */
    if (prev <= GATEWAY_MAX_INFLIGHT) {
        pthread_mutex_lock(&g_inflight_lock);
        pthread_cond_signal(&g_inflight_cond);
        pthread_mutex_unlock(&g_inflight_lock);
    }
}

/* Per-connection state — owned exclusively by one worker. */
typedef struct conn_state {
    int      fd;
    uint8_t *buf;          /* receive buffer, grown on demand */
    size_t   buf_cap;      /* allocated bytes */
    size_t   buf_pos;      /* current bytes read into buf */
    uint32_t frame_len;    /* set after the 4-byte length prefix arrives, else 0 */
} conn_state_t;

/* ------------------------------------------------------------ helpers */

static int set_tcp_nodelay(int fd)
{
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

/* Send all bytes; returns total sent or -1 on error. Used on synchronous response path. */
static ssize_t tcp_send_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += (size_t)n;
    }
    return (ssize_t)sent;
}

/* Build and send a Thrift TApplicationException frame on error paths.
 * Keeps the TCP connection alive — client sees an RPC error but can
 * reuse the same connection for the next request. */
static int send_thrift_exception(int fd, const uint8_t *req_buf, size_t req_len,
                                 const char *message)
{
    if (req_len < 12) return -1;

    uint32_t name_len = ((uint32_t)req_buf[8]  << 24) |
                        ((uint32_t)req_buf[9]  << 16) |
                        ((uint32_t)req_buf[10] << 8)  |
                         (uint32_t)req_buf[11];
    if (12 + name_len + 4 > req_len) return -1;

    const uint8_t *name = req_buf + 12;
    uint32_t seq_id = ((uint32_t)req_buf[12 + name_len]     << 24) |
                      ((uint32_t)req_buf[13 + name_len]     << 16) |
                      ((uint32_t)req_buf[14 + name_len]     << 8)  |
                       (uint32_t)req_buf[15 + name_len];

    uint32_t msg_len = (uint32_t)strlen(message);
    uint32_t payload_len = 4 + 4 + name_len + 4 + 3 + 4 + msg_len + 3 + 4 + 1;
    uint8_t buf[512];
    if (4 + payload_len > sizeof(buf)) return -1;

    uint8_t *p = buf;
    *p++ = (payload_len >> 24) & 0xff; *p++ = (payload_len >> 16) & 0xff;
    *p++ = (payload_len >> 8)  & 0xff; *p++ =  payload_len        & 0xff;
    *p++ = 0x80; *p++ = 0x01; *p++ = 0x00; *p++ = 0x03;     /* version + EXCEPTION */
    *p++ = (name_len >> 24) & 0xff; *p++ = (name_len >> 16) & 0xff;
    *p++ = (name_len >> 8)  & 0xff; *p++ =  name_len        & 0xff;
    memcpy(p, name, name_len); p += name_len;
    *p++ = (seq_id >> 24) & 0xff; *p++ = (seq_id >> 16) & 0xff;
    *p++ = (seq_id >> 8)  & 0xff; *p++ =  seq_id        & 0xff;
    *p++ = 11; *p++ = 0x00; *p++ = 0x01;                     /* field 1: STRING msg */
    *p++ = (msg_len >> 24) & 0xff; *p++ = (msg_len >> 16) & 0xff;
    *p++ = (msg_len >> 8)  & 0xff; *p++ =  msg_len        & 0xff;
    memcpy(p, message, msg_len); p += msg_len;
    *p++ = 8; *p++ = 0x00; *p++ = 0x02;                      /* field 2: I32 type */
    *p++ = 0x00; *p++ = 0x00; *p++ = 0x00; *p++ = 0x06;      /* INTERNAL_ERROR */
    *p++ = 0x00;                                              /* STOP */

    return (int)tcp_send_all(fd, buf, (size_t)(p - buf));
}

/* ------------------------------------------------------------ conn lifecycle */

static conn_state_t *conn_new(int fd)
{
    conn_state_t *cs = (conn_state_t *)calloc(1, sizeof(*cs));
    if (!cs) return NULL;
    cs->fd      = fd;
    cs->buf     = (uint8_t *)malloc(INITIAL_RECV_BUF);
    cs->buf_cap = cs->buf ? INITIAL_RECV_BUF : 0;
    if (!cs->buf) { free(cs); return NULL; }
    return cs;
}

static void conn_free(conn_state_t *cs)
{
    if (!cs) return;
    if (cs->fd >= 0) close(cs->fd);
    free(cs->buf);
    free(cs);
}

static int conn_buf_grow(conn_state_t *cs, size_t need)
{
    if (need <= cs->buf_cap) return 0;
    if (need > MAX_FRAME_SIZE) return -1;
    size_t new_cap = cs->buf_cap ? cs->buf_cap * 2 : INITIAL_RECV_BUF;
    while (new_cap < need) new_cap *= 2;
    uint8_t *p = (uint8_t *)realloc(cs->buf, new_cap);
    if (!p) return -1;
    cs->buf     = p;
    cs->buf_cap = new_cap;
    return 0;
}

/* ------------------------------------------------------------ request path */

/* Process one fully-received Thrift frame. Synchronous: enqueue + wait + send.
 * Returns 0 on success (TCP can keep going), -1 on fatal TCP error. */
static int process_request(conn_state_t *cs)
{
    const uint8_t *req_buf = cs->buf;
    size_t  req_len = 4 + cs->frame_len;
    int     rc;

    uint32_t req_id = dpumesh_alloc_req_id(g_ctx);

    int tx_slot = dpumesh_tx_alloc(g_ctx);
    if (tx_slot < 0) {
        send_thrift_exception(cs->fd, req_buf, req_len, "DPUmesh busy");
        return 0;
    }

    rc = dpumesh_register_pending(g_ctx, req_id);
    if (rc < 0) {
        dpumesh_tx_free(g_ctx, tx_slot);
        send_thrift_exception(cs->fd, req_buf, req_len, "DPUmesh busy");
        return 0;
    }

    uint8_t *tx_buf = dpumesh_tx_buf(g_ctx, tx_slot);
    memcpy(tx_buf, req_buf, req_len);

    sw_descriptor_t desc;
    memset(&desc, 0, sizeof(desc));
    desc.header_buf_slot      = -1;
    desc.body_buf_slot        = tx_slot;
    desc.body_len             = (uint32_t)req_len;
    desc.req_id               = req_id;
    desc.dst_pod_id           = 0;  /* forward to unique-id-service (pod_id=0) */
    desc.src_pod_id           = dpumesh_get_pod_id(g_ctx);
    desc.flags                = OP_REQUEST | CASE_EXTERNAL;
    desc.valid                = 1;
    desc.src_body_pool_type   = POOL_HOST_TX_BODY;
    desc.src_body_pod_id      = dpumesh_get_pod_id(g_ctx);
    desc.src_body_buf_slot    = tx_slot;
    desc.src_header_buf_slot  = -1;

    rc = dpumesh_enqueue(g_ctx, &desc);
    if (rc < 0) {
        dpumesh_tx_free(g_ctx, tx_slot);
        dpumesh_cancel_pending(g_ctx, req_id);
        send_thrift_exception(cs->fd, req_buf, req_len, "DPUmesh enqueue failed");
        return 0;
    }
    dpumesh_pending_attach_tx(g_ctx, req_id, tx_slot);

    sw_descriptor_t resp;
    rc = dpumesh_wait_response(g_ctx, req_id, &resp, RESPONSE_TIMEOUT_MS);
    if (rc < 0) {
        dpumesh_cancel_pending(g_ctx, req_id);
        send_thrift_exception(cs->fd, req_buf, req_len, "DPUmesh timeout");
        return 0;
    }

    if (resp.body_buf_slot >= 0 && resp.body_len > 0) {
        uint8_t *resp_data = dpumesh_rx_buf(g_ctx, resp.body_buf_slot);
        if (resp_data) {
            ssize_t sent = tcp_send_all(cs->fd, resp_data, resp.body_len);
            dpumesh_rx_free(g_ctx, resp.body_buf_slot);
            if (sent < 0) return -1;          /* peer gone */
        } else {
            dpumesh_rx_free(g_ctx, resp.body_buf_slot);
        }
    }
    return 0;
}

/* Drain readable bytes, dispatch each complete frame. Returns 0 on success,
 * -1 if the connection should be closed. */
static int conn_drain(conn_state_t *cs)
{
    while (g_running) {
        size_t need = (cs->frame_len == 0)
                          ? (4 - cs->buf_pos)
                          : (4 + cs->frame_len - cs->buf_pos);

        if (need == 0) {
            /* Whole frame in buffer — admission gate, then process it.
             * admission_acquire() blocks the worker thread when in-flight cap
             * is reached. The worker's epoll loop is paused while blocked, so
             * TCP recv buffers fill on its connections and clients see
             * natural TCP-level backpressure. No exception responses,
             * no leaks. */
            admission_acquire();
            int rc = process_request(cs);
            admission_release();
            if (rc < 0) return -1;
            cs->buf_pos   = 0;
            cs->frame_len = 0;
            continue;
        }

        if (conn_buf_grow(cs, cs->buf_pos + need) < 0) return -1;

        ssize_t n = recv(cs->fd, cs->buf + cs->buf_pos, need, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;     /* drained */
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;                                          /* peer closed */
        cs->buf_pos += (size_t)n;

        if (cs->frame_len == 0 && cs->buf_pos >= 4) {
            cs->frame_len = ((uint32_t)cs->buf[0] << 24) |
                            ((uint32_t)cs->buf[1] << 16) |
                            ((uint32_t)cs->buf[2] << 8)  |
                             (uint32_t)cs->buf[3];
            if (cs->frame_len == 0 || cs->frame_len > MAX_FRAME_SIZE) {
                fprintf(stderr, "[gateway] invalid frame_len=%u\n", cs->frame_len);
                return -1;
            }
            if (conn_buf_grow(cs, 4 + cs->frame_len) < 0) {
                fprintf(stderr, "[gateway] buffer grow failed for frame_len=%u\n",
                        cs->frame_len);
                return -1;
            }
        }
    }
    return 0;
}

/* ------------------------------------------------------------ worker */

static int make_listen_socket(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) { perror("[gateway] socket"); return -1; }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
        perror("[gateway] SO_REUSEADDR");
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
        perror("[gateway] SO_REUSEPORT (kernel < 3.9?)");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[gateway] bind");
        close(fd);
        return -1;
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
        perror("[gateway] listen");
        close(fd);
        return -1;
    }
    return fd;
}

static void *worker_fn(void *arg)
{
    int wid = (int)(intptr_t)arg;

    int listen_fd = make_listen_socket(GATEWAY_PORT);
    if (listen_fd < 0) {
        fprintf(stderr, "[gateway] worker %d: listen failed, exiting\n", wid);
        return NULL;
    }

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) { perror("[gateway] epoll_create1"); close(listen_fd); return NULL; }

    /* Listen socket sentinel — recognized via NULL data.ptr */
    struct epoll_event ev;
    ev.events   = EPOLLIN;                    /* level-triggered for accept */
    ev.data.ptr = NULL;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("[gateway] epoll_ctl(listen)");
        close(epfd); close(listen_fd); return NULL;
    }

    struct epoll_event events[MAX_EVENTS];

    while (g_running) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, 500);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("[gateway] epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            conn_state_t *cs = (conn_state_t *)events[i].data.ptr;

            if (cs == NULL) {
                /* Listen socket: accept everything currently pending. */
                while (1) {
                    struct sockaddr_in caddr;
                    socklen_t          clen = sizeof(caddr);
                    int cfd = accept4(listen_fd, (struct sockaddr *)&caddr, &clen,
                                      SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("[gateway] accept4");
                        break;
                    }
                    set_tcp_nodelay(cfd);

                    conn_state_t *ncs = conn_new(cfd);
                    if (!ncs) { close(cfd); continue; }

                    struct epoll_event cev;
                    cev.events   = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    cev.data.ptr = ncs;
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) < 0) {
                        perror("[gateway] epoll_ctl(add conn)");
                        conn_free(ncs);
                        continue;
                    }
                }
                continue;
            }

            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, cs->fd, NULL);
                conn_free(cs);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                if (conn_drain(cs) < 0) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, cs->fd, NULL);
                    conn_free(cs);
                }
            }
        }
    }

    close(epfd);
    close(listen_fd);
    return NULL;
}

/* ------------------------------------------------------------ main */

static void on_sigterm(int signo)
{
    (void)signo;
    g_running = 0;
    /* Wake any worker thread blocked in admission_acquire() so it can observe
     * g_running == 0 and exit cleanly. */
    pthread_mutex_lock(&g_inflight_lock);
    pthread_cond_broadcast(&g_inflight_cond);
    pthread_mutex_unlock(&g_inflight_lock);
}

int main(void)
{
    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;

    printf("=== DPUmesh TCP Gateway (epoll thread-pool) ===\n");

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  on_sigterm);
    signal(SIGTERM, on_sigterm);

    const char *envw = getenv("DPUMESH_GATEWAY_WORKERS");
    if (envw && *envw) {
        int v = atoi(envw);
        if (v > 0 && v <= 256) g_num_workers = v;
    }

    printf("[gateway] Initializing DPUmesh...\n");
    int rc = dpumesh_init(&g_ctx, "gateway", 1, &cfg);
    if (rc != 0) {
        fprintf(stderr, "[gateway] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    printf("[gateway] DPUmesh initialized (pod_id=%d)\n", dpumesh_get_pod_id(g_ctx));

    printf("[gateway] Waiting 5s for DPU initialization...\n");
    sleep(5);

    printf("[gateway] Starting %d worker threads (SO_REUSEPORT + per-worker epoll), port %d\n",
           g_num_workers, GATEWAY_PORT);

    pthread_t *tids = (pthread_t *)calloc((size_t)g_num_workers, sizeof(pthread_t));
    if (!tids) { fprintf(stderr, "[gateway] tids calloc failed\n"); return 1; }

    for (int i = 0; i < g_num_workers; i++) {
        if (pthread_create(&tids[i], NULL, worker_fn, (void *)(intptr_t)i) != 0) {
            perror("[gateway] pthread_create");
            return 1;
        }
    }

    for (int i = 0; i < g_num_workers; i++) {
        pthread_join(tids[i], NULL);
    }

    free(tids);
    dpumesh_destroy(g_ctx);
    return 0;
}
