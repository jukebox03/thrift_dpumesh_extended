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
#include <signal.h>
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

/* Send a Thrift TApplicationException frame to the client.
 * Keeps the TCP connection alive — client sees an RPC error but can
 * reuse the same connection for the next request. */
static int send_thrift_exception(int fd, const uint8_t *req_buf, ssize_t req_len,
                                 const char *message)
{
    /* req_buf layout from tcp_recv_framed:
     * [0..3] frame_length  [4..7] version+type  [8..11] name_len
     * [12..12+N-1] name    [12+N..15+N] seq_id */
    if (req_len < 12) return -1;

    uint32_t name_len = ((uint32_t)req_buf[8] << 24) | ((uint32_t)req_buf[9] << 16) |
                         ((uint32_t)req_buf[10] << 8) | (uint32_t)req_buf[11];
    if ((ssize_t)(12 + name_len + 4) > req_len) return -1;

    const uint8_t *name = req_buf + 12;
    uint32_t seq_id = ((uint32_t)req_buf[12+name_len] << 24) |
                       ((uint32_t)req_buf[13+name_len] << 16) |
                       ((uint32_t)req_buf[14+name_len] << 8) |
                       (uint32_t)req_buf[15+name_len];

    uint32_t msg_len = (uint32_t)strlen(message);
    /* payload = version(4) + name_len(4) + name + seq_id(4) +
     *           field1_hdr(3) + msg_len(4) + msg +
     *           field2_hdr(3) + type(4) + stop(1) */
    uint32_t payload_len = 4 + 4 + name_len + 4 + 3 + 4 + msg_len + 3 + 4 + 1;
    uint8_t buf[512];
    if (4 + payload_len > sizeof(buf)) return -1;

    uint8_t *p = buf;
    /* Frame length */
    *p++ = (payload_len >> 24) & 0xff; *p++ = (payload_len >> 16) & 0xff;
    *p++ = (payload_len >> 8)  & 0xff; *p++ =  payload_len        & 0xff;
    /* Version 1 + type EXCEPTION(3) */
    *p++ = 0x80; *p++ = 0x01; *p++ = 0x00; *p++ = 0x03;
    /* Method name */
    *p++ = (name_len >> 24) & 0xff; *p++ = (name_len >> 16) & 0xff;
    *p++ = (name_len >> 8)  & 0xff; *p++ =  name_len        & 0xff;
    memcpy(p, name, name_len); p += name_len;
    /* Seq ID */
    *p++ = (seq_id >> 24) & 0xff; *p++ = (seq_id >> 16) & 0xff;
    *p++ = (seq_id >> 8)  & 0xff; *p++ =  seq_id        & 0xff;
    /* TApplicationException field 1: message (STRING=11, id=1) */
    *p++ = 11; *p++ = 0x00; *p++ = 0x01;
    *p++ = (msg_len >> 24) & 0xff; *p++ = (msg_len >> 16) & 0xff;
    *p++ = (msg_len >> 8)  & 0xff; *p++ =  msg_len        & 0xff;
    memcpy(p, message, msg_len); p += msg_len;
    /* TApplicationException field 2: type (I32=8, id=2), INTERNAL_ERROR=6 */
    *p++ = 8; *p++ = 0x00; *p++ = 0x02;
    *p++ = 0x00; *p++ = 0x00; *p++ = 0x00; *p++ = 0x06;
    /* STOP */
    *p++ = 0x00;

    return (int)tcp_send_all(fd, buf, (size_t)(p - buf));
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

        uint32_t req_id = dpumesh_alloc_req_id(g_ctx);

        /* Allocate TX slot first — needed before register so attach_tx works */
        int tx_slot = dpumesh_tx_alloc(g_ctx);
        if (tx_slot < 0) {
            fprintf(stderr, "[gateway] TX pool full\n");
            send_thrift_exception(client_fd, recv_buf, recv_len, "DPUmesh busy");
            continue;
        }

        rc = dpumesh_register_pending(g_ctx, req_id);
        if (rc < 0) {
            fprintf(stderr, "[gateway] register_pending failed for req_id=%u\n", req_id);
            dpumesh_tx_free(g_ctx, tx_slot);
            send_thrift_exception(client_fd, recv_buf, recv_len, "DPUmesh busy");
            continue;
        }

        /* Copy data into TX buffer */
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
            send_thrift_exception(client_fd, recv_buf, recv_len, "DPUmesh enqueue failed");
            continue;
        }

        /* TX slot is now in the ring — DPA may DMA from it.
         * Attach to pending so deferred cleanup works on timeout. */
        dpumesh_pending_attach_tx(g_ctx, req_id, tx_slot);

        printf("[gateway] Forwarded req_id=%u (%zd bytes), waiting for response...\n",
               req_id, recv_len);

        /* Wait for response */
        sw_descriptor_t resp;
        rc = dpumesh_wait_response(g_ctx, req_id, &resp, RESPONSE_TIMEOUT);
        if (rc < 0) {
            fprintf(stderr, "[gateway] Response timeout for req_id=%u\n", req_id);
            /* TX slot cleanup is deferred — wait_response set state=-2,
             * rx_data_hook will free TX when DPA finishes. */
            dpumesh_cancel_pending(g_ctx, req_id);
            send_thrift_exception(client_fd, recv_buf, recv_len, "DPUmesh timeout");
            continue;
        }

        /* Response arrived — DPA is done with TX buffer, safe to free */
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

    signal(SIGPIPE, SIG_IGN);

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
