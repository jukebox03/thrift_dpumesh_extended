/*
 * gateway.c - DPUmesh TCP Gateway (Host-side)
 *
 * Accepts TCP connections, forwards requests via DPUmesh,
 * waits for responses, and sends them back over TCP.
 *
 * Build:
 *   gcc -o gateway gateway.c -Ilib/cpp/src -Lbuild-doca/lib -lthriftd \
 *       -Wl,-rpath,/home/jukebox/thrift_dpumesh_extended/build-doca/lib \
 *       -lpthread
 *
 * Run:
 *   sudo DPUMESH_PCI_ADDR=94:00.0 ./gateway
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <thrift/transport/dpumesh.h>

#define GATEWAY_PORT     9091
#define RECV_BUF_SIZE    (1024 * 1024)
#define RESPONSE_TIMEOUT 30000  /* 30s */

static dpumesh_ctx_t *g_ctx = NULL;

/* Receive a complete Thrift framed message: 4-byte length + payload */
static ssize_t tcp_recv_framed(int fd, uint8_t *buf, size_t max_len)
{
    ssize_t total = 0;

    /* Read until we have at least 4 bytes (frame header) */
    while (total < 4) {
        ssize_t n = recv(fd, buf + total, max_len - total, 0);
        if (n <= 0) return n;
        total += n;
    }

    /* Parse frame length (big-endian) */
    uint32_t frame_len = ((uint32_t)buf[0] << 24) |
                         ((uint32_t)buf[1] << 16) |
                         ((uint32_t)buf[2] << 8)  |
                         ((uint32_t)buf[3]);

    size_t expected = 4 + frame_len;
    if (expected > max_len) {
        fprintf(stderr, "[gateway] Frame too large: %zu > %zu\n", expected, max_len);
        return -1;
    }

    /* Read remaining payload */
    while ((size_t)total < expected) {
        ssize_t n = recv(fd, buf + total, expected - total, 0);
        if (n <= 0) return n;
        total += n;
    }

    return total;
}

/* Send all bytes over TCP */
static ssize_t tcp_send_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return n;
        sent += n;
    }
    return (ssize_t)sent;
}

struct conn_arg {
    int client_fd;
    struct sockaddr_in client_addr;
};

static void *handle_connection(void *arg)
{
    struct conn_arg *ca = (struct conn_arg *)arg;
    int client_fd = ca->client_fd;
    uint8_t *recv_buf = NULL;
    int rc;

    recv_buf = (uint8_t *)malloc(RECV_BUF_SIZE);
    if (!recv_buf) {
        fprintf(stderr, "[gateway] malloc failed\n");
        goto done;
    }

    /* Persistent connection loop — keep handling requests until client disconnects */
    while (1) {
        /* Receive request */
        ssize_t recv_len = tcp_recv_framed(client_fd, recv_buf, RECV_BUF_SIZE);
        if (recv_len <= 0)
            break;  /* client closed or error */

        printf("[gateway] Received %zd bytes from %s:%d\n",
               recv_len, inet_ntoa(ca->client_addr.sin_addr),
               ntohs(ca->client_addr.sin_port));

        /* Allocate req_id and register pending */
        uint32_t req_id = dpumesh_alloc_req_id(g_ctx);

        rc = dpumesh_register_pending(g_ctx, req_id);
        if (rc < 0) {
            fprintf(stderr, "[gateway] register_pending failed for req_id=%u\n", req_id);
            break;
        }

        /* Allocate TX slot and copy data */
        int tx_slot = dpumesh_tx_alloc(g_ctx);
        if (tx_slot < 0) {
            fprintf(stderr, "[gateway] TX pool full\n");
            dpumesh_cancel_pending(g_ctx, req_id);
            break;
        }

        uint8_t *tx_buf = dpumesh_tx_buf(g_ctx, tx_slot);
        memcpy(tx_buf, recv_buf, recv_len);

        /* Build request descriptor */
        sw_descriptor_t desc;
        memset(&desc, 0, sizeof(desc));
        desc.header_buf_slot = -1;
        desc.body_buf_slot = tx_slot;
        desc.body_len = (uint32_t)recv_len;
        desc.req_id = req_id;
        desc.dst_pod_id = 0;
        desc.src_pod_id = dpumesh_get_pod_id(g_ctx);
        desc.flags = OP_REQUEST | CASE_EXTERNAL;
        desc.valid = 1;
        desc.src_body_pool_type = POOL_HOST_TX_BODY;
        desc.src_body_pod_id = dpumesh_get_pod_id(g_ctx);
        desc.src_body_buf_slot = tx_slot;
        desc.src_header_buf_slot = -1;

        /* Enqueue to DPUmesh */
        rc = dpumesh_enqueue(g_ctx, &desc);
        if (rc < 0) {
            fprintf(stderr, "[gateway] enqueue failed\n");
            dpumesh_tx_free(g_ctx, tx_slot);
            dpumesh_cancel_pending(g_ctx, req_id);
            break;
        }

        printf("[gateway] Forwarded req_id=%u (%zd bytes), waiting for response...\n",
               req_id, recv_len);

        /* Wait for response */
        sw_descriptor_t resp;
        rc = dpumesh_wait_response(g_ctx, req_id, &resp, RESPONSE_TIMEOUT);
        if (rc < 0) {
            fprintf(stderr, "[gateway] Response timeout for req_id=%u\n", req_id);
            dpumesh_tx_free(g_ctx, tx_slot);
            dpumesh_cancel_pending(g_ctx, req_id);
            break;
        }

        /* Free TX slot — DMA copy is done since response arrived */
        dpumesh_tx_free(g_ctx, tx_slot);

        /* Send response back to TCP client */
        if (resp.body_buf_slot >= 0 && resp.body_len > 0) {
            uint8_t *resp_data = dpumesh_rx_buf(g_ctx, resp.body_buf_slot);
            if (resp_data) {
                ssize_t sent = tcp_send_all(client_fd, resp_data, resp.body_len);
                printf("[gateway] Sent %zd bytes response for req_id=%u\n", sent, req_id);
            }
            dpumesh_rx_free(g_ctx, resp.body_buf_slot);
        }
    } /* end while(1) */

done:
    close(client_fd);
    free(recv_buf);
    free(ca);
    return NULL;
}

int main(void)
{
    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int listen_fd, opt = 1;
    struct sockaddr_in addr;

    printf("=== DPUmesh TCP Gateway ===\n");

    /* Initialize DPUmesh */
    printf("[gateway] Initializing DPUmesh...\n");
    int rc = dpumesh_init(&g_ctx, "gateway", 1, &cfg);  /* pod_id=1 */
    if (rc != 0) {
        fprintf(stderr, "[gateway] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    printf("[gateway] DPUmesh initialized (pod_id=%d)\n", dpumesh_get_pod_id(g_ctx));

    /* Wait for DPU DPA init */
    printf("[gateway] Waiting 5s for DPU initialization...\n");
    sleep(5);

    /* Create TCP listener */
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("[gateway] socket");
        return 1;
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(GATEWAY_PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("[gateway] bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 128) < 0) {
        perror("[gateway] listen");
        close(listen_fd);
        return 1;
    }

    printf("[gateway] Listening on port %d\n", GATEWAY_PORT);

    /* Accept loop */
    while (1) {
        struct conn_arg *ca = (struct conn_arg *)malloc(sizeof(*ca));
        socklen_t addr_len = sizeof(ca->client_addr);

        ca->client_fd = accept(listen_fd, (struct sockaddr *)&ca->client_addr, &addr_len);
        if (ca->client_fd < 0) {
            perror("[gateway] accept");
            free(ca);
            continue;
        }

        /* Handle each connection in a thread */
        pthread_t tid;
        pthread_create(&tid, NULL, handle_connection, ca);
        pthread_detach(tid);
    }

    close(listen_fd);
    dpumesh_destroy(g_ctx);
    return 0;
}
