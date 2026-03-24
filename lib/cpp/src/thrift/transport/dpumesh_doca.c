/*
 * dpumesh_doca.c - DPUmesh DOCA transport layer implementation
 *
 * NVIDIA DOCA (Comch + DMA) backend for DPUmesh Thrift transport.
 * Replaces the SHM-based dpumesh_shm.c when built with -DWITH_DOCA=ON.
 *
 * Phase 1: TX path only. RX functions are stubs.
 */

#include "dpumesh.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>

#include "doca/common.h"
#include "doca/object.h"
#include "doca/config.h"
#include "doca/buffer.h"
#include "doca/ring.h"
#include "doca/comch_client.h"
#include "doca/comch_producer.h"
#include "doca/comch_consumer.h"
#include "doca/comch_common.h"
#include "doca/comch_msgq.h"
#include "doca/dma.h"
#include "doca/dpa_common.h"

DOCA_LOG_REGISTER(DPUMESH_DOCA);

/* ====================================================================
 * dpumesh_ctx — internal state
 * ==================================================================== */

/* RX queue capacity */
#define RX_QUEUE_SIZE 512

struct dpumesh_ctx {
    char app_name[64];
    char worker_id[128];
    int  pod_id;
    int  num_slots;
    int  slot_size;
    int  max_descriptors;

    /* DOCA objects */
    struct objects doca_objs;
    void *dma_buffer;
    struct doca_mmap *dma_buffer_mmap;
    struct dma_ring *dma_ring;
    doca_dpa_dev_mmap_t dpa_mmap_handle;  /* DPA handle for local mmap (used in TX descriptors) */

    /* TX slot management */
    uint8_t *slot_bitmap;
    pthread_mutex_t slot_lock;

    /* RX buffer pool (independent from TX) */
    void *rx_buffer;
    uint8_t *rx_slot_bitmap;
    pthread_mutex_t rx_slot_lock;

    /* RX descriptor queue (circular buffer) */
    sw_descriptor_t rx_queue[RX_QUEUE_SIZE];
    int rx_head;
    int rx_tail;
    int rx_count;
    pthread_mutex_t rx_lock;
    pthread_cond_t rx_cond;

    /* comch max message size (for RX data validation) */
    uint32_t comch_max_msg_size;

    /* PE progress thread */
    pthread_t pe_tid;
    volatile int pe_running;
};

/* ====================================================================
 * PE progress thread — drives DOCA progress engine
 * ==================================================================== */

static void *pe_progress_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    struct timespec ts = {0, 1000}; /* 1 µs */

    while (ctx->pe_running) {
        if (ctx->doca_objs.pe) {
            if (doca_pe_progress(ctx->doca_objs.pe) == 0)
                nanosleep(&ts, &ts);
        } else {
            nanosleep(&ts, &ts);
        }
    }
    return NULL;
}

/* ====================================================================
 * RX data hook — called from PE progress thread via comch callback
 * ==================================================================== */

static int rx_slot_alloc(dpumesh_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->rx_slot_lock);
    for (int i = 0; i < ctx->num_slots; i++) {
        if (ctx->rx_slot_bitmap[i] == 0) {
            ctx->rx_slot_bitmap[i] = 1;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&ctx->rx_slot_lock);
    return -1;
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_rx_data_msg *msg = (const struct dmesh_rx_data_msg *)data;

    /* Validate body_len against actual received length */
    uint32_t expected = (uint32_t)sizeof(struct dmesh_rx_data_msg) + msg->body_len;
    if (len < expected) {
        DOCA_LOG_ERR("RX_DATA: truncated message: got %u, need %u", len, expected);
        return;
    }

    /* Allocate an RX slot for the body */
    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("RX_DATA: no free RX slots, dropping message");
        return;
    }

    /* Copy body into RX buffer pool */
    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    memcpy(dst, msg->body, msg->body_len);

    /* Build descriptor: copy from message, then patch body_buf_slot to local slot */
    sw_descriptor_t desc;
    memcpy(&desc, msg->desc, sizeof(desc));
    desc.body_buf_slot = slot;

    /* Push to RX queue */
    pthread_mutex_lock(&ctx->rx_lock);
    if (ctx->rx_count >= RX_QUEUE_SIZE) {
        pthread_mutex_unlock(&ctx->rx_lock);
        DOCA_LOG_ERR("RX_DATA: RX queue full, dropping message");
        /* Free the slot we just allocated */
        pthread_mutex_lock(&ctx->rx_slot_lock);
        ctx->rx_slot_bitmap[slot] = 0;
        pthread_mutex_unlock(&ctx->rx_slot_lock);
        return;
    }
    ctx->rx_queue[ctx->rx_tail] = desc;
    ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
    ctx->rx_count++;
    pthread_cond_signal(&ctx->rx_cond);
    pthread_mutex_unlock(&ctx->rx_lock);
}

/* ====================================================================
 * dpumesh_init — DOCA initialization sequence
 * ==================================================================== */

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    doca_error_t result;
    dpumesh_ctx_t *ctx;
    const char *env_val;

    ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;

    /* ---- Resolve config ---- */
    if (config && config->num_slots > 0)
        ctx->num_slots = config->num_slots;
    else if ((env_val = getenv("DPUMESH_NUM_SLOTS")) != NULL && atoi(env_val) > 0)
        ctx->num_slots = atoi(env_val);
    else
        ctx->num_slots = DPUMESH_NUM_SLOTS_DEFAULT;

    if (config && config->slot_size > 0)
        ctx->slot_size = config->slot_size;
    else if ((env_val = getenv("DPUMESH_SLOT_SIZE")) != NULL && atoi(env_val) > 0)
        ctx->slot_size = atoi(env_val);
    else
        ctx->slot_size = DPUMESH_SLOT_SIZE_DEFAULT;

    if (config && config->max_descriptors > 0)
        ctx->max_descriptors = config->max_descriptors;
    else if ((env_val = getenv("DPUMESH_MAX_DESCRIPTORS")) != NULL && atoi(env_val) > 0)
        ctx->max_descriptors = atoi(env_val);
    else
        ctx->max_descriptors = DPUMESH_MAX_DESCRIPTORS_DEFAULT;

    snprintf(ctx->app_name, sizeof(ctx->app_name), "%s", app_name);
    snprintf(ctx->worker_id, sizeof(ctx->worker_id),
             "%s-worker-%d", app_name, worker_num);

    /* pod_id: from env or default to worker_num */
    if ((env_val = getenv("DPUMESH_POD_ID")) != NULL)
        ctx->pod_id = atoi(env_val);
    else
        ctx->pod_id = worker_num;

    /* ---- PCI address from env ---- */
    const char *pci_addr = getenv("DPUMESH_PCI_ADDR");
    if (!pci_addr)
        pci_addr = "94:00.0";

    /* ---- DOCA logging ---- */
    doca_log_backend_create_standard();

    /* ---- Open DOCA device ---- */
    memset(&ctx->doca_objs, 0, sizeof(ctx->doca_objs));

    fprintf(stderr, "[dpumesh] Opening DOCA device at %s...\n", pci_addr);
    result = open_doca_device_with_pci(pci_addr, NULL, &ctx->doca_objs.dev);
    if (result != DOCA_SUCCESS) {
        fprintf(stderr, "[dpumesh] FAIL: open_doca_device_with_pci: %s\n", doca_error_get_descr(result));
        goto fail;
    }
    fprintf(stderr, "[dpumesh] Device opened OK\n");

    /* ---- Comch control path (client) ---- */
    fprintf(stderr, "[dpumesh] Connecting comch client...\n");
    result = init_comch_ctrl_path_client("DPUMesh", &ctx->doca_objs, true);
    if (result != DOCA_SUCCESS) {
        fprintf(stderr, "[dpumesh] FAIL: comch client: %s\n", doca_error_get_descr(result));
        goto fail_cleanup;
    }
    fprintf(stderr, "[dpumesh] Comch client connected OK\n");

    /* ---- Comch datapath producer ---- */
    result = init_comch_datapath_producer(&ctx->doca_objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch datapath producer: %s",
                     doca_error_get_descr(result));
        goto fail_cleanup;
    }

    /* ---- DMA ring ---- */
    result = setup_dma_ring(&ctx->doca_objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA ring: %s",
                     doca_error_get_descr(result));
        goto fail_cleanup;
    }
    ctx->dma_ring = ctx->doca_objs.dma_ring;

    /* ---- Allocate DMA buffer (num_slots * slot_size) ---- */
    size_t buf_size = (size_t)ctx->num_slots * ctx->slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->doca_objs.local_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->doca_objs.dma_buffer,
                                       buf_size,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA buffer: %s",
                     doca_error_get_descr(result));
        goto fail_cleanup;
    }
    ctx->dma_buffer = ctx->doca_objs.dma_buffer;
    ctx->dma_buffer_mmap = ctx->doca_objs.local_mmap;

    /* ---- Export DMA buffer mmap to DPU ---- */
    result = export_mmap_to_remote(&ctx->doca_objs, ctx->doca_objs.local_mmap,
                                   ctx->doca_objs.dma_buffer, buf_size,
                                   DMA_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap to DPU: %s",
                     doca_error_get_descr(result));
        goto fail_cleanup;
    }

    /* ---- Cache DPA mmap handle for TX descriptor fill ---- */
    result = doca_mmap_dev_get_dpa_handle(ctx->doca_objs.local_mmap,
                                          ctx->doca_objs.dev,
                                          &ctx->dpa_mmap_handle);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get DPA mmap handle: %s",
                     doca_error_get_descr(result));
        goto fail_cleanup;
    }

    /* ---- TX slot bitmap + mutex ---- */
    ctx->slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->slot_bitmap)
        goto fail_cleanup;
    pthread_mutex_init(&ctx->slot_lock, NULL);

    /* ---- RX buffer pool + slot bitmap ---- */
    size_t rx_buf_size = (size_t)ctx->num_slots * ctx->slot_size;
    ctx->rx_buffer = calloc(1, rx_buf_size);
    if (!ctx->rx_buffer)
        goto fail_cleanup;
    ctx->rx_slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->rx_slot_bitmap)
        goto fail_cleanup;
    pthread_mutex_init(&ctx->rx_slot_lock, NULL);

    /* ---- RX descriptor queue ---- */
    ctx->rx_head = 0;
    ctx->rx_tail = 0;
    ctx->rx_count = 0;
    pthread_mutex_init(&ctx->rx_lock, NULL);
    pthread_cond_init(&ctx->rx_cond, NULL);

    /* ---- Query and cache comch max_msg_size ---- */
    {
        uint32_t max_msg_sz = 0;
        result = doca_comch_cap_get_max_msg_size(
            doca_dev_as_devinfo(ctx->doca_objs.dev), &max_msg_sz);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to query comch max msg size: %s",
                         doca_error_get_descr(result));
            goto fail_cleanup;
        }
        ctx->comch_max_msg_size = max_msg_sz;
    }

    /* ---- Register RX data hook ---- */
    ctx->doca_objs.rx_data_hook = rx_data_hook;
    ctx->doca_objs.rx_hook_ctx = ctx;

    /* ---- Start PE progress thread ---- */
    ctx->pe_running = 1;
    if (pthread_create(&ctx->pe_tid, NULL, pe_progress_fn, ctx) != 0) {
        DOCA_LOG_ERR("Failed to create PE progress thread");
        goto fail_cleanup;
    }

    DOCA_LOG_INFO("DPUmesh DOCA initialized: worker=%s pod_id=%d pci=%s "
                  "slots=%d slot_size=%d",
                  ctx->worker_id, ctx->pod_id, pci_addr,
                  ctx->num_slots, ctx->slot_size);

    *out = ctx;
    return 0;

fail_cleanup:
    cleanup_objects(&ctx->doca_objs);
fail:
    if (ctx->slot_bitmap)
        free(ctx->slot_bitmap);
    free(ctx);
    return -1;
}

/* ====================================================================
 * dpumesh_destroy
 * ==================================================================== */

void dpumesh_destroy(dpumesh_ctx_t *ctx) {
    if (!ctx) return;

    /* Stop PE thread */
    ctx->pe_running = 0;
    pthread_join(ctx->pe_tid, NULL);

    /* Cleanup DOCA objects */
    cleanup_objects(&ctx->doca_objs);

    /* Free TX bitmap + mutex */
    pthread_mutex_destroy(&ctx->slot_lock);
    free(ctx->slot_bitmap);

    /* Free RX resources */
    pthread_mutex_destroy(&ctx->rx_slot_lock);
    free(ctx->rx_slot_bitmap);
    free(ctx->rx_buffer);
    pthread_mutex_destroy(&ctx->rx_lock);
    pthread_cond_destroy(&ctx->rx_cond);

    free(ctx);
}

/* ====================================================================
 * TX functions
 * ==================================================================== */

int dpumesh_tx_alloc(dpumesh_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->slot_lock);
    for (int i = 0; i < ctx->num_slots; i++) {
        if (ctx->slot_bitmap[i] == 0) {
            ctx->slot_bitmap[i] = 1;
            pthread_mutex_unlock(&ctx->slot_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&ctx->slot_lock);
    return -1;
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    pthread_mutex_lock(&ctx->slot_lock);
    ctx->slot_bitmap[slot] = 0;
    pthread_mutex_unlock(&ctx->slot_lock);
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    struct dma_desc *dma = get_next_dma_desc(ctx->dma_ring);
    if (!dma)
        return -1;

    /* Fill DMA descriptor: point to the TX slot's buffer region */
    dma->mmap = ctx->dpa_mmap_handle;
    dma->addr = (uint64_t)ctx->dma_buffer +
                ((size_t)desc->body_buf_slot * ctx->slot_size);
    dma->size = desc->body_len;
    dma->idx  = desc->req_id;

    /* Memory barrier to ensure fields are visible before valid flag */
    __sync_synchronize();
    dma->valid = 1;

    return 0;
}

/* ====================================================================
 * RX functions (comch control path temporary implementation)
 * ==================================================================== */

int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms) {
    pthread_mutex_lock(&ctx->rx_lock);

    while (ctx->rx_count == 0) {
        if (timeout_ms == 0) {
            /* Non-blocking */
            pthread_mutex_unlock(&ctx->rx_lock);
            return -1;
        } else if (timeout_ms < 0) {
            /* Block forever */
            pthread_cond_wait(&ctx->rx_cond, &ctx->rx_lock);
        } else {
            /* Timed wait */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec  += timeout_ms / 1000;
            ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000L;
            }
            int rc = pthread_cond_timedwait(&ctx->rx_cond, &ctx->rx_lock, &ts);
            if (rc != 0) {
                /* Timeout or error */
                pthread_mutex_unlock(&ctx->rx_lock);
                return -1;
            }
        }
    }

    /* Pop from head */
    *desc = ctx->rx_queue[ctx->rx_head];
    ctx->rx_head = (ctx->rx_head + 1) % RX_QUEUE_SIZE;
    ctx->rx_count--;

    pthread_mutex_unlock(&ctx->rx_lock);
    return 0;
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    pthread_mutex_lock(&ctx->rx_slot_lock);
    ctx->rx_slot_bitmap[slot] = 0;
    pthread_mutex_unlock(&ctx->rx_slot_lock);
}

/* ====================================================================
 * Query / info functions
 * ==================================================================== */

int dpumesh_get_slot_size(dpumesh_ctx_t *ctx) {
    return ctx->slot_size;
}

int dpumesh_get_notify_fd(dpumesh_ctx_t *ctx) {
    /* No notify fd in DOCA mode (PE-based) */
    (void)ctx;
    return -1;
}

int dpumesh_get_pod_id(dpumesh_ctx_t *ctx) {
    return ctx->pod_id;
}

const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx) {
    return ctx->worker_id;
}
