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
#include <stdatomic.h>

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

static const char *doca_err_str(doca_error_t rc) {
    return doca_error_get_descr(rc);
}

static void cleanup_ctx(struct dpumesh_ctx *ctx);

/* ====================================================================
 * dpumesh_ctx — internal state
 * ==================================================================== */

/* RX queue capacity */
#define RX_QUEUE_SIZE 512

/* Pending response table for client-side request/response matching */
#define MAX_PENDING 4096

/* Inflight TX table for ACK-based slot release (server response path) */
#define MAX_TX_INFLIGHT 2048

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sw_descriptor_t desc;
    volatile int state;   /* -1=unused, 0=waiting, 1=arrived */
} dpumesh_pending_t;

typedef struct {
    uint32_t req_id;
    int32_t dst_pod_id;
    int slot;
    int in_use;
} dpumesh_tx_inflight_t;

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
    struct dma_ring *dma_ring;
    pthread_mutex_t ring_lock;  /* Serializes get_next_dma_desc + descriptor fill + valid=1 */
    doca_dpa_dev_mmap_t dpa_mmap_handle;  /* DPA handle for local mmap (used in TX descriptors) */

    /* Doorbell pool (matching dma_ring size) for async comch sends */
    struct dmesh_new_desc_msg *doorbell_pool;

    /* Persistent buffers for initial registration to avoid stack UAF */
    struct dmesh_register_msg reg_msg;
    struct dmesh_pod_consumer_id_msg pod_cid_msg;

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
    pthread_cond_t rx_not_full;  /* Signaled when rx_count drops, for backpressure */

    /* comch max message size (for RX data validation) */
    uint32_t comch_max_msg_size;

    /* PE progress thread */
    pthread_t pe_tid;
    volatile int pe_running;

    /* Client-side pending response table */
    dpumesh_pending_t pending[MAX_PENDING];
    atomic_uint_fast32_t next_req_id;

    /* TX inflight table for ACK-based release */
    dpumesh_tx_inflight_t tx_inflight[MAX_TX_INFLIGHT];
    pthread_mutex_t tx_inflight_lock;
    int tx_inflight_lock_ready;
};

/* ====================================================================
 * PE progress thread — drives DOCA progress engine
 * ==================================================================== */

static void *pe_progress_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    struct timespec ts = {0, 1000}; /* 1 µs */

    while (ctx->pe_running) {
        int progressed = 0;

        if (ctx->doca_objs.pe)
            progressed += doca_pe_progress(ctx->doca_objs.pe);

        if (ctx->doca_objs.consumer_pe)
            progressed += doca_pe_progress(ctx->doca_objs.consumer_pe);

        if (ctx->doca_objs.producer_pe)
            progressed += doca_pe_progress(ctx->doca_objs.producer_pe);

        if (progressed == 0)
            nanosleep(&ts, &ts);
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

static int tx_inflight_register(dpumesh_ctx_t *ctx, uint32_t req_id,
                                int32_t dst_pod_id, int slot)
{
    int retries = 0;
    const int max_retries = 50;
    struct timespec backoff = {0, 100000}; /* 100µs */

retry:
    {
        int free_idx = -1;

        pthread_mutex_lock(&ctx->tx_inflight_lock);
        for (int i = 0; i < MAX_TX_INFLIGHT; i++) {
            if (ctx->tx_inflight[i].in_use) {
                if (ctx->tx_inflight[i].req_id == req_id &&
                    ctx->tx_inflight[i].dst_pod_id == dst_pod_id) {
                    pthread_mutex_unlock(&ctx->tx_inflight_lock);
                    DOCA_LOG_ERR("TX inflight duplicate req_id=%u dst_pod=%d",
                                 req_id, dst_pod_id);
                    return -1;
                }
            } else if (free_idx < 0) {
                free_idx = i;
            }
        }

        if (free_idx >= 0) {
            ctx->tx_inflight[free_idx].req_id = req_id;
            ctx->tx_inflight[free_idx].dst_pod_id = dst_pod_id;
            ctx->tx_inflight[free_idx].slot = slot;
            ctx->tx_inflight[free_idx].in_use = 1;
            pthread_mutex_unlock(&ctx->tx_inflight_lock);
            DOCA_LOG_INFO("TX inflight registered: req_id=%u dst_pod=%d slot=%d idx=%d",
                          req_id, dst_pod_id, slot, free_idx);
            return 0;
        }

        pthread_mutex_unlock(&ctx->tx_inflight_lock);
    }

    if (retries < max_retries) {
        nanosleep(&backoff, NULL);
        retries++;
        goto retry;
    }

    DOCA_LOG_ERR("TX inflight table full after %d retries for req_id=%u dst_pod=%d",
                 max_retries, req_id, dst_pod_id);
    return -1;
}

static int pending_is_waiting(dpumesh_ctx_t *ctx, uint32_t req_id)
{
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];
    int waiting;

    pthread_mutex_lock(&p->lock);
    waiting = (p->state == 0);
    pthread_mutex_unlock(&p->lock);
    return waiting;
}

static void tx_inflight_ack_hook(void *hook_ctx, const uint8_t *data, uint32_t len)
{
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_tx_ack_msg *ack = (const struct dmesh_tx_ack_msg *)data;

    if (len < sizeof(struct dmesh_tx_ack_msg)) {
        DOCA_LOG_ERR("TX_ACK: invalid len=%u", len);
        return;
    }

    pthread_mutex_lock(&ctx->tx_inflight_lock);
    for (int i = 0; i < MAX_TX_INFLIGHT; i++) {
        if (!ctx->tx_inflight[i].in_use)
            continue;
        if (ctx->tx_inflight[i].req_id == ack->req_id &&
            ctx->tx_inflight[i].dst_pod_id == ack->dst_pod_id) {
            int slot = ctx->tx_inflight[i].slot;
            ctx->tx_inflight[i].in_use = 0;
            pthread_mutex_unlock(&ctx->tx_inflight_lock);
            dpumesh_tx_free(ctx, slot);
            DOCA_LOG_INFO("TX_ACK matched inflight: req_id=%u dst_pod=%d slot=%d idx=%d",
                          ack->req_id, ack->dst_pod_id, slot, i);
            return;
        }
    }
    pthread_mutex_unlock(&ctx->tx_inflight_lock);

    if (pending_is_waiting(ctx, ack->req_id)) {
        DOCA_LOG_INFO("TX_ACK without inflight (expected request path): req_id=%u dst_pod=%d",
                      ack->req_id, ack->dst_pod_id);
    } else {
        DOCA_LOG_WARN("TX_ACK orphan: no inflight/pending entry for req_id=%u dst_pod=%d",
                      ack->req_id, ack->dst_pod_id);
    }
}

static void tx_inflight_cleanup(dpumesh_ctx_t *ctx)
{
    pthread_mutex_lock(&ctx->tx_inflight_lock);
    for (int i = 0; i < MAX_TX_INFLIGHT; i++) {
        if (ctx->tx_inflight[i].in_use) {
            int slot = ctx->tx_inflight[i].slot;
            ctx->tx_inflight[i].in_use = 0;
            dpumesh_tx_free(ctx, slot);
        }
    }
    pthread_mutex_unlock(&ctx->tx_inflight_lock);
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_rx_data_msg *msg = (const struct dmesh_rx_data_msg *)data;

    DOCA_LOG_INFO("rx_data_hook ENTER: len=%u body_len=%u sizeof_hdr=%zu",
                  len, msg->body_len, sizeof(struct dmesh_rx_data_msg));

    uint32_t expected = (uint32_t)sizeof(struct dmesh_rx_data_msg) + msg->body_len;
    if (len < expected) {
        DOCA_LOG_ERR("RX_DATA: truncated message: got %u, need %u", len, expected);
        return;
    }

    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("RX_DATA: no free RX slots, dropping message");
        return;
    }

    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    memcpy(dst, msg->body, msg->body_len);

    sw_descriptor_t desc;
    memcpy(&desc, msg->desc, sizeof(desc));
    desc.body_buf_slot = slot;

    DOCA_LOG_INFO("rx_data_hook DESC: req_id=%u flags=0x%x dst_pod=%d src_pod=%d slot=%d body_len=%u",
                  desc.req_id, (unsigned)(uint8_t)desc.flags, desc.dst_pod_id, desc.src_pod_id,
                  slot, desc.body_len);

    if (desc.flags & OP_RESPONSE) {
        uint32_t idx = desc.req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        pthread_mutex_lock(&p->lock);
        if (p->state == 0) {
            p->desc = desc;
            p->state = 1;
            pthread_cond_signal(&p->cond);
        } else {
            DOCA_LOG_ERR("RX_DATA: OP_RESPONSE for req_id=%u but no waiter (state=%d)",
                         desc.req_id, p->state);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
        pthread_mutex_unlock(&p->lock);
    } else {
        pthread_mutex_lock(&ctx->rx_lock);

        /* Backpressure: wait briefly for space instead of dropping immediately */
        if (ctx->rx_count >= RX_QUEUE_SIZE) {
            struct timespec bp_ts;
            clock_gettime(CLOCK_REALTIME, &bp_ts);
            bp_ts.tv_nsec += 50000000L; /* 50ms wait budget */
            if (bp_ts.tv_nsec >= 1000000000L) {
                bp_ts.tv_sec++;
                bp_ts.tv_nsec -= 1000000000L;
            }

            while (ctx->rx_count >= RX_QUEUE_SIZE) {
                int bp_rc = pthread_cond_timedwait(&ctx->rx_not_full, &ctx->rx_lock, &bp_ts);
                if (bp_rc != 0) {
                    pthread_mutex_unlock(&ctx->rx_lock);
                    DOCA_LOG_ERR("RX_DATA: RX queue full after backpressure wait, dropping message");
                    pthread_mutex_lock(&ctx->rx_slot_lock);
                    ctx->rx_slot_bitmap[slot] = 0;
                    pthread_mutex_unlock(&ctx->rx_slot_lock);
                    return;
                }
            }
        }

        ctx->rx_queue[ctx->rx_tail] = desc;
        ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
        ctx->rx_count++;
        pthread_cond_signal(&ctx->rx_cond);
        pthread_mutex_unlock(&ctx->rx_lock);
    }
}

static void init_config(dpumesh_ctx_t *ctx, const dpumesh_config_t *config, const char *app_name, int worker_num) {
    const char *env_val;

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

    if ((env_val = getenv("DPUMESH_POD_ID")) != NULL)
        ctx->pod_id = atoi(env_val);
    else
        ctx->pod_id = worker_num;
}

static doca_error_t init_doca_device(dpumesh_ctx_t *ctx) {
    const char *pci_addr = getenv("DPUMESH_PCI_ADDR");
    if (!pci_addr) pci_addr = "94:00.0";

    doca_log_backend_create_standard();
    fprintf(stderr, "[dpumesh] Opening DOCA device at %s...\n", pci_addr);
    return open_doca_device_with_pci(pci_addr, NULL, &ctx->doca_objs.dev);
}

static doca_error_t init_control_path(dpumesh_ctx_t *ctx) {
    doca_error_t result;

    fprintf(stderr, "[dpumesh] Connecting comch client...\n");
    result = init_comch_ctrl_path_client("DPUMesh", &ctx->doca_objs, true);
    if (result != DOCA_SUCCESS) return result;

    ctx->reg_msg.type = DMESH_MSG_REGISTER;
    ctx->reg_msg.pod_id = ctx->pod_id;
    snprintf(ctx->reg_msg.app_name, sizeof(ctx->reg_msg.app_name), "%s", ctx->app_name);
    
    result = client_send_msg(&ctx->doca_objs, (const char *)&ctx->reg_msg, sizeof(ctx->reg_msg));
    if (result == DOCA_SUCCESS) {
        DOCA_LOG_INFO("Sent REGISTER to DPU: pod_id=%d app=%s", ctx->pod_id, ctx->app_name);
    }
    return result;
}

static doca_error_t init_datapath(dpumesh_ctx_t *ctx) {
    doca_error_t result;

    result = init_comch_datapath_consumer(&ctx->doca_objs);
    if (result != DOCA_SUCCESS) return result;

    if (ctx->doca_objs.consumer != NULL) {
        uint32_t local_consumer_id = 0;
        result = doca_comch_consumer_get_id(ctx->doca_objs.consumer, &local_consumer_id);
        if (result != DOCA_SUCCESS) return result;

        ctx->pod_cid_msg.type = DMESH_MSG_POD_CONSUMER_ID;
        ctx->pod_cid_msg.pod_id = ctx->pod_id;
        ctx->pod_cid_msg.consumer_id = local_consumer_id;
        client_send_msg(&ctx->doca_objs, (const char *)&ctx->pod_cid_msg, sizeof(ctx->pod_cid_msg));
    }

    result = init_comch_datapath_producer(&ctx->doca_objs);
    if (result != DOCA_SUCCESS) return result;

    result = setup_dma_ring(&ctx->doca_objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) return result;
    ctx->dma_ring = ctx->doca_objs.dma_ring;

    size_t buf_size = (size_t)ctx->num_slots * ctx->slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->doca_objs.local_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->doca_objs.dma_buffer,
                                       buf_size,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) return result;
    ctx->dma_buffer = ctx->doca_objs.dma_buffer;

    result = export_mmap_to_remote(&ctx->doca_objs, ctx->doca_objs.local_mmap,
                                   ctx->doca_objs.dma_buffer, buf_size,
                                   DMA_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) return result;

    return doca_mmap_dev_get_dpa_handle(ctx->doca_objs.local_mmap, ctx->doca_objs.dev, &ctx->dpa_mmap_handle);
}

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;

    init_config(ctx, config, app_name, worker_num);

    if (init_doca_device(ctx) != DOCA_SUCCESS) goto fail;
    if (init_control_path(ctx) != DOCA_SUCCESS) goto fail;
    if (init_datapath(ctx) != DOCA_SUCCESS) goto fail;

    ctx->doorbell_pool = (struct dmesh_new_desc_msg *)calloc(DMA_RING_SIZE, sizeof(struct dmesh_new_desc_msg));
    if (!ctx->doorbell_pool) goto fail;

    ctx->slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->slot_bitmap) goto fail;
    pthread_mutex_init(&ctx->slot_lock, NULL);
    pthread_mutex_init(&ctx->ring_lock, NULL);

    ctx->rx_buffer = calloc(1, (size_t)ctx->num_slots * ctx->slot_size);
    if (!ctx->rx_buffer) goto fail;
    ctx->rx_slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->rx_slot_bitmap) goto fail;
    pthread_mutex_init(&ctx->rx_slot_lock, NULL);

    pthread_mutex_init(&ctx->rx_lock, NULL);
    pthread_cond_init(&ctx->rx_cond, NULL);
    pthread_cond_init(&ctx->rx_not_full, NULL);

    uint32_t max_msg_sz = 0;
    if (doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(ctx->doca_objs.dev), &max_msg_sz) == DOCA_SUCCESS)
        ctx->comch_max_msg_size = max_msg_sz;

    atomic_init(&ctx->next_req_id, 1);
    for (int i = 0; i < MAX_PENDING; i++) {
        pthread_mutex_init(&ctx->pending[i].lock, NULL);
        pthread_cond_init(&ctx->pending[i].cond, NULL);
        ctx->pending[i].state = -1;
    }

    pthread_mutex_init(&ctx->tx_inflight_lock, NULL);
    ctx->tx_inflight_lock_ready = 1;
    ctx->doca_objs.rx_data_hook = rx_data_hook;
    ctx->doca_objs.rx_hook_ctx = ctx;
    ctx->doca_objs.tx_ack_hook = tx_inflight_ack_hook;
    ctx->doca_objs.tx_ack_hook_ctx = ctx;

    ctx->pe_running = 1;
    if (pthread_create(&ctx->pe_tid, NULL, pe_progress_fn, ctx) != 0) goto fail;

    DOCA_LOG_INFO("DPUmesh DOCA initialized: worker=%s pod_id=%d", ctx->worker_id, ctx->pod_id);

    *out = ctx;
    return 0;

fail:
    cleanup_ctx(ctx);
    return -1;
}

static void cleanup_ctx(dpumesh_ctx_t *ctx) {
    if (!ctx) return;

    if (ctx->pe_running) {
        ctx->pe_running = 0;
        pthread_join(ctx->pe_tid, NULL);
    }

    cleanup_objects(&ctx->doca_objs);

    pthread_mutex_destroy(&ctx->ring_lock);
    pthread_mutex_destroy(&ctx->slot_lock);
    if (ctx->slot_bitmap) free(ctx->slot_bitmap);

    pthread_mutex_destroy(&ctx->rx_slot_lock);
    if (ctx->rx_slot_bitmap) free(ctx->rx_slot_bitmap);
    if (ctx->rx_buffer) free(ctx->rx_buffer);
    pthread_mutex_destroy(&ctx->rx_lock);
    pthread_cond_destroy(&ctx->rx_cond);
    pthread_cond_destroy(&ctx->rx_not_full);

    for (int i = 0; i < MAX_PENDING; i++) {
        pthread_mutex_destroy(&ctx->pending[i].lock);
        pthread_cond_destroy(&ctx->pending[i].cond);
    }

    if (ctx->tx_inflight_lock_ready) {
        tx_inflight_cleanup(ctx);
        pthread_mutex_destroy(&ctx->tx_inflight_lock);
    }

    if (ctx->doorbell_pool) free(ctx->doorbell_pool);

    free(ctx);
}

void dpumesh_destroy(dpumesh_ctx_t *ctx) {
    if (!ctx) return;
    DOCA_LOG_INFO("Destroying DPUmesh context: worker=%s", ctx->worker_id);
    cleanup_ctx(ctx);
}

/* ====================================================================
 * TX functions
 * ==================================================================== */

int dpumesh_tx_alloc(dpumesh_ctx_t *ctx) {
    const int max_retry = 1000;
    struct timespec backoff = {0, 10000}; /* 10us initial */

    for (int retry = 0; retry < max_retry; retry++) {
        pthread_mutex_lock(&ctx->slot_lock);
        for (int i = 0; i < ctx->num_slots; i++) {
            if (ctx->slot_bitmap[i] == 0) {
                ctx->slot_bitmap[i] = 1;
                pthread_mutex_unlock(&ctx->slot_lock);
                return i;
            }
        }
        pthread_mutex_unlock(&ctx->slot_lock);

        if (retry == 0) continue;  /* first miss: immediate retry */
        nanosleep(&backoff, NULL);
        if (backoff.tv_nsec < 1000000)  /* cap at 1ms */
            backoff.tv_nsec *= 2;
    }
    DOCA_LOG_ERR("TX alloc failed: all %d slots busy after %d retries", ctx->num_slots, max_retry);
    return -1;
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Clear TX buffer before releasing slot to prevent stale data */
    uint8_t *buf = (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
    memset(buf, 0, ctx->slot_size);
    pthread_mutex_lock(&ctx->slot_lock);
    ctx->slot_bitmap[slot] = 0;
    pthread_mutex_unlock(&ctx->slot_lock);
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    struct dma_desc *dma;
    uint32_t ring_slot;

    if (desc == NULL) {
        DOCA_LOG_ERR("ENQUEUE rejected: desc is NULL");
        return -1;
    }

    if (desc->body_buf_slot < 0 || desc->body_buf_slot >= ctx->num_slots) {
        DOCA_LOG_ERR("ENQUEUE rejected: invalid body_buf_slot=%d (num_slots=%d)",
                     desc->body_buf_slot, ctx->num_slots);
        return -1;
    }

    if (desc->body_len > (uint32_t)ctx->slot_size) {
        DOCA_LOG_ERR("ENQUEUE rejected: body_len=%u exceeds slot_size=%d",
                     desc->body_len, ctx->slot_size);
        return -1;
    }

    /* Lock ring access: serializes get_next_dma_desc + descriptor fill + valid=1 */
    pthread_mutex_lock(&ctx->ring_lock);

    /* Retry with exponential backoff if DMA ring is temporarily full */
    {
        int ring_retry = 0;
        const int max_ring_retry = 1000;
        struct timespec backoff = {0, 10000}; /* 10µs initial */
        while (ring_retry < max_ring_retry) {
            dma = get_next_dma_desc(ctx->dma_ring);
            if (dma)
                break;
            pthread_mutex_unlock(&ctx->ring_lock);
            nanosleep(&backoff, NULL);
            if (backoff.tv_nsec < 1000000) /* cap at 1ms */
                backoff.tv_nsec *= 2;
            pthread_mutex_lock(&ctx->ring_lock);
            ring_retry++;
        }
        if (!dma) {
            pthread_mutex_unlock(&ctx->ring_lock);
            DOCA_LOG_ERR("ENQUEUE failed: DMA ring exhausted after %d retries", max_ring_retry);
            return -1;
        }
    }

    ring_slot = (uint32_t)(dma - ctx->dma_ring->descs);

    /* tx_inflight_register has its own lock, safe to call under ring_lock */
    if ((desc->flags & OP_RESPONSE) &&
        tx_inflight_register(ctx, desc->req_id, desc->dst_pod_id,
                             desc->body_buf_slot) != 0) {
        pthread_mutex_unlock(&ctx->ring_lock);
        return -1;
    }

    dma->mmap = ctx->dpa_mmap_handle;
    dma->addr = (uint64_t)ctx->dma_buffer +
                ((size_t)desc->body_buf_slot * ctx->slot_size);
    dma->size = desc->body_len;
    dma->idx  = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    DOCA_LOG_INFO("ENQUEUE publish: req_id=%u ring_slot=%u tx_slot=%d len=%u dst_pod=%d flags=0x%x addr=0x%lx",
                  desc->req_id, ring_slot, desc->body_buf_slot, desc->body_len,
                  desc->dst_pod_id, (unsigned int)(uint8_t)desc->flags, (unsigned long)dma->addr);

    /* Use persistent doorbell from pool to avoid stack UAF for async comch send.
     * ring_slot is protected by dma->valid bit (DPA won't reuse slot until done). */
    struct dmesh_new_desc_msg *doorbell = &ctx->doorbell_pool[ring_slot];
    doorbell->type = DMESH_MSG_NEW_DESC;
    doorbell->src_pod_id = ctx->pod_id;
    doorbell->addr = dma->addr;
    doorbell->size = dma->size;
    doorbell->req_id = (uint32_t)dma->idx;
    doorbell->dst_pod_id = dma->dst_pod_id;
    doorbell->flags = dma->flags;

    pthread_mutex_unlock(&ctx->ring_lock);

    /* client_send_msg copies payload internally, safe to call unlocked */
    doca_error_t result = client_send_msg(&ctx->doca_objs, (const char *)doorbell, sizeof(*doorbell));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("ENQUEUE: client_send_msg failed: %s", doca_error_get_descr(result));
        return -1;
    }

    return 0;
}

/* ====================================================================
 * RX functions
 * ==================================================================== */

int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms) {
    pthread_mutex_lock(&ctx->rx_lock);

    struct timespec ts;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
    }

    while (ctx->rx_count == 0) {
        if (timeout_ms == 0) {
            pthread_mutex_unlock(&ctx->rx_lock);
            return -1;
        } else if (timeout_ms < 0) {
            pthread_cond_wait(&ctx->rx_cond, &ctx->rx_lock);
        } else {
            int rc = pthread_cond_timedwait(&ctx->rx_cond, &ctx->rx_lock, &ts);
            if (rc != 0) {
                pthread_mutex_unlock(&ctx->rx_lock);
                return -1;
            }
        }
    }

    *desc = ctx->rx_queue[ctx->rx_head];
    ctx->rx_head = (ctx->rx_head + 1) % RX_QUEUE_SIZE;
    ctx->rx_count--;
    pthread_cond_signal(&ctx->rx_not_full);

    pthread_mutex_unlock(&ctx->rx_lock);
    return 0;
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Clear RX buffer before releasing slot to prevent stale data */
    uint8_t *buf = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    memset(buf, 0, ctx->slot_size);
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
    (void)ctx;
    return -1;
}

int dpumesh_get_pod_id(dpumesh_ctx_t *ctx) {
    return ctx->pod_id;
}

const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx) {
    return ctx->worker_id;
}

/* ====================================================================
 * Client-side API
 * ==================================================================== */

uint32_t dpumesh_alloc_req_id(dpumesh_ctx_t *ctx) {
    return atomic_fetch_add(&ctx->next_req_id, 1);
}

int dpumesh_register_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);

    /* If slot is occupied, wait briefly for previous request to complete */
    if (p->state != -1) {
        struct timespec wait_ts;
        clock_gettime(CLOCK_REALTIME, &wait_ts);
        wait_ts.tv_sec += 2; /* 2-second collision wait budget */

        while (p->state != -1) {
            int rc = pthread_cond_timedwait(&p->cond, &p->lock, &wait_ts);
            if (rc != 0) {
                pthread_mutex_unlock(&p->lock);
                DOCA_LOG_ERR("Pending slot collision: req_id=%u idx=%u stuck state=%d",
                             req_id, idx, p->state);
                return -1;
            }
        }
    }

    p->state = 0;
    memset(&p->desc, 0, sizeof(p->desc));
    pthread_mutex_unlock(&p->lock);
    return 0;
}

int dpumesh_wait_response(dpumesh_ctx_t *ctx, uint32_t req_id,
                          sw_descriptor_t *resp, int timeout_ms) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);

    struct timespec ts;
    if (timeout_ms > 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec  += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
    }

    while (p->state == 0) {
        if (timeout_ms < 0) {
            pthread_cond_wait(&p->cond, &p->lock);
        } else if (timeout_ms == 0) {
            pthread_mutex_unlock(&p->lock);
            return -1;
        } else {
            int rc = pthread_cond_timedwait(&p->cond, &p->lock, &ts);
            if (rc != 0) {
                /* Check if response arrived during timeout boundary */
                if (p->state == 1)
                    break; /* fall through to success path below */
                p->state = -1;
                pthread_cond_broadcast(&p->cond);
                pthread_mutex_unlock(&p->lock);
                return -1;
            }
        }
    }

    if (p->state == 1) {
        *resp = p->desc;
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return 0;
    }

    p->state = -1;
    pthread_cond_broadcast(&p->cond);
    pthread_mutex_unlock(&p->lock);
    return -1;
}

void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    if (p->state == 1) {
        /* Response arrived but was never consumed -- free the RX slot to prevent leak */
        if (p->desc.body_buf_slot >= 0) {
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[p->desc.body_buf_slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
    }
    p->state = -1;
    pthread_cond_broadcast(&p->cond);
    pthread_mutex_unlock(&p->lock);
}
