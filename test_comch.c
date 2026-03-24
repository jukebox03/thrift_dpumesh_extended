/*
 * test_comch.c - Minimal test: dpumesh_init() → comch handshake
 *
 * Build:
 *   gcc -o test_comch test_comch.c -Llib/cpp -lthriftd -Ilib/cpp/src \
 *       -L/opt/mellanox/doca/lib/x86_64-linux-gnu -Wl,-rpath,/opt/mellanox/doca/lib/x86_64-linux-gnu \
 *       -lpthread
 *
 * Run (after dpumesh_dpu is running on DPU):
 *   DPUMESH_PCI_ADDR=94:00.0 ./test_comch
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <thrift/transport/dpumesh.h>

int main(void)
{
    dpumesh_ctx_t *ctx = NULL;
    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int rc;

    printf("=== DPUmesh comch connection test ===\n");
    printf("Calling dpumesh_init()...\n");

    rc = dpumesh_init(&ctx, "test", 0, &cfg);
    if (rc != 0) {
        fprintf(stderr, "FAIL: dpumesh_init() returned %d\n", rc);
        return 1;
    }

    printf("OK: dpumesh_init() succeeded\n");
    printf("  worker_id = %s\n", dpumesh_get_worker_id(ctx));
    printf("  pod_id    = %d\n", dpumesh_get_pod_id(ctx));
    printf("  slot_size = %d\n", dpumesh_get_slot_size(ctx));

    /* Wait for DPU to finish initialization (DPA init takes ~1-2s) */
    printf("Waiting 5s for DPU to complete initialization...\n");
    sleep(5);

    /* Test TX alloc/free */
    int slot = dpumesh_tx_alloc(ctx);
    if (slot >= 0) {
        uint8_t *buf = dpumesh_tx_buf(ctx, slot);
        printf("  tx_alloc  = slot %d (buf=%p)\n", slot, (void *)buf);
        memcpy(buf, "hello", 5);
        dpumesh_tx_free(ctx, slot);
        printf("  tx_free   = OK\n");
    } else {
        printf("  tx_alloc  = FAIL\n");
    }

    /* Test enqueue (TX path to DPU via DMA ring) */
    slot = dpumesh_tx_alloc(ctx);
    if (slot >= 0) {
        uint8_t *buf = dpumesh_tx_buf(ctx, slot);
        memcpy(buf, "DPUmesh E2E test payload", 24);
        sw_descriptor_t desc = {0};
        desc.body_buf_slot = slot;
        desc.body_len = 24;
        desc.req_id = 42;
        desc.dst_pod_id = 0;
        desc.src_pod_id = dpumesh_get_pod_id(ctx);
        desc.flags = 0;
        desc.valid = 1;
        desc.src_body_pool_type = POOL_HOST_TX_BODY;
        desc.src_body_pod_id = dpumesh_get_pod_id(ctx);
        desc.src_body_buf_slot = slot;
        rc = dpumesh_enqueue(ctx, &desc);
        printf("  enqueue   = %s (req_id=42, slot=%d)\n",
               rc == 0 ? "OK" : "FAIL", slot);
    }

    /* Wait for DMA completion */
    printf("Waiting 3s for DMA completion...\n");
    sleep(3);

    printf("Calling dpumesh_destroy()...\n");
    dpumesh_destroy(ctx);
    printf("OK: cleanup done\n");

    return 0;
}
