/*
 * echo_server.c - DPUmesh echo server (pod_id=0)
 *
 * Receives requests via DPUmesh, echoes them back as OP_RESPONSE.
 * Used for multi-pod testing before integrating with real Thrift services.
 *
 * Build:
 *   gcc -o echo_server echo_server.c -Ilib/cpp/src -Lbuild-doca/lib -lthriftd \
 *       -Wl,-rpath,/home/jukebox/thrift_dpumesh_extended/build-doca/lib -lpthread
 *
 * Run:
 *   sudo DPUMESH_PCI_ADDR=94:00.0 ./echo_server
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>

#include <thrift/transport/dpumesh.h>

static dpumesh_ctx_t *g_ctx = NULL;
static volatile int running = 1;

static void sigint_handler(int sig) {
    (void)sig;
    running = 0;
}

int main(void)
{
    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int rc;

    signal(SIGINT, sigint_handler);

    printf("=== DPUmesh Echo Server (pod_id=0) ===\n");

    /* Initialize DPUmesh as pod_id=0 */
    rc = dpumesh_init(&g_ctx, "echo_server", 0, &cfg);
    if (rc != 0) {
        fprintf(stderr, "[echo] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    printf("[echo] Initialized: pod_id=%d\n", dpumesh_get_pod_id(g_ctx));

    /* Wait for DPU DPA init */
    printf("[echo] Waiting 5s for DPU initialization...\n");
    sleep(5);

    printf("[echo] Ready, waiting for requests...\n");

    uint32_t req_count = 0;

    while (running) {
        /* Dequeue incoming request */
        sw_descriptor_t desc;
        rc = dpumesh_dequeue(g_ctx, &desc, 1000);  /* 1s timeout */
        if (rc != 0)
            continue;  /* timeout, retry */

        req_count++;
        printf("[echo] Request #%u: req_id=%u, body_len=%u, src_pod=%d, flags=0x%02x\n",
               req_count, desc.req_id, desc.body_len, desc.src_pod_id, desc.flags);

        /* Read request data from RX buffer */
        uint8_t *rx_data = dpumesh_rx_buf(g_ctx, desc.body_buf_slot);
        if (!rx_data) {
            fprintf(stderr, "[echo] RX buf null for slot %d\n", desc.body_buf_slot);
            dpumesh_rx_free(g_ctx, desc.body_buf_slot);
            continue;
        }

        /* Allocate TX slot and copy data (echo) */
        int tx_slot = dpumesh_tx_alloc(g_ctx);
        if (tx_slot < 0) {
            fprintf(stderr, "[echo] TX pool full\n");
            dpumesh_rx_free(g_ctx, desc.body_buf_slot);
            continue;
        }

        uint8_t *tx_buf = dpumesh_tx_buf(g_ctx, tx_slot);
        memcpy(tx_buf, rx_data, desc.body_len);

        /* Free RX slot (done reading) */
        dpumesh_rx_free(g_ctx, desc.body_buf_slot);

        /* Build response descriptor */
        sw_descriptor_t resp;
        memset(&resp, 0, sizeof(resp));
        resp.header_buf_slot = -1;
        resp.body_buf_slot = tx_slot;
        resp.body_len = desc.body_len;
        resp.req_id = desc.req_id;          /* echo back same req_id */
        resp.dst_pod_id = desc.src_pod_id;  /* route back to sender */
        resp.src_pod_id = dpumesh_get_pod_id(g_ctx);
        resp.flags = OP_RESPONSE | CASE_INGRESS;
        resp.valid = 1;
        resp.src_body_pool_type = POOL_HOST_TX_BODY;
        resp.src_body_pod_id = dpumesh_get_pod_id(g_ctx);
        resp.src_body_buf_slot = tx_slot;
        resp.src_header_buf_slot = -1;

        /* Enqueue response */
        rc = dpumesh_enqueue(g_ctx, &resp);
        if (rc < 0) {
            fprintf(stderr, "[echo] enqueue failed\n");
            dpumesh_tx_free(g_ctx, tx_slot);
            continue;
        }

        printf("[echo] Response sent: req_id=%u, %u bytes → pod %d\n",
               desc.req_id, desc.body_len, desc.src_pod_id);

        /* Note: TX slot will be freed after DMA completes.
         * For simplicity, we free it here since DMA is very fast (<1ms). */
        dpumesh_tx_free(g_ctx, tx_slot);
    }

    printf("\n[echo] Shutting down (%u requests served)\n", req_count);
    dpumesh_destroy(g_ctx);
    return 0;
}
