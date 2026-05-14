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

/* Phase 1: HDR TX pool API — declared early because cleanup_ctx uses
 * dpumesh_hdr_tx_free during pending teardown (mirror of dpumesh_tx_free). */
int      dpumesh_hdr_tx_alloc(dpumesh_ctx_t *ctx);
uint8_t *dpumesh_hdr_tx_buf(dpumesh_ctx_t *ctx, int slot);
void     dpumesh_hdr_tx_free(dpumesh_ctx_t *ctx, int slot);

/* ====================================================================
 * dpumesh_ctx — internal state
 * ==================================================================== */

/* RX queue capacity */
/* RX queue between PE thread (producer, drains rx_dma_buffer) and the
 * application accept loop (consumer, e.g. TThreadedServer). Sized to be
 * larger than gateway's admission_cap (900) and the server's worst-case
 * concurrent in-flight, so the PE thread never has to drop on enqueue.
 * 65536 entries × ~200B = ~13MB pure RAM, same scale as `pending` pool. */
#define RX_QUEUE_SIZE 65536

/* Pending response table for client-side request/response matching.
 * Indexed by req_id % MAX_PENDING. Must exceed expected in-flight requests
 * to avoid hash collisions. Pure software array (host RAM only — no HW limit).
 * 65536 entries × ~200B = ~13 MB. */
#define MAX_PENDING 65536

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sw_descriptor_t desc;
    volatile int state;   /* -1=unused, -2=cancelled(tx deferred), 0=waiting, 1=arrived */
    int tx_slot;          /* BODY TX buffer slot owned by this request, -1 if none.
                           * Kept named `tx_slot` for legacy compat; semantically
                           * this is the body pool slot (POOL_HOST_TX_BODY). */
    /* Phase 1 (v2 plan): independent HDR TX pool. Same lifecycle rules as
     * tx_slot but tracked separately so a single user RPC can simultaneously
     * own a body slot and a hdr slot without clobbering. TX_ACK.pool_type
     * tells the handler which one to free. */
    int hdr_tx_slot;      /* HDR TX buffer slot, -1 if none */
} dpumesh_pending_t;

struct dpumesh_ctx {
    char app_name[64];
    char worker_id[128];
    int  pod_id;
    int  num_slots;
    int  slot_size;
    int  max_descriptors;
    /* DOCA objects */
    struct objects doca_objs;
    void *dma_buffer;          /* Host TX buffer (PCI mmap, CPU→DPU source) */
    struct dma_ring *dma_ring;
    pthread_mutex_t ring_lock;  /* Serializes get_next_dma_desc + descriptor fill + valid=1 */
    doca_dpa_dev_mmap_t dpa_mmap_handle;  /* DPA handle for local mmap (used in TX descriptors) */

    /* Host RX buffer (PCI mmap, DPU→CPU destination) */
    void *rx_dma_buffer;
    struct doca_mmap *rx_dma_mmap;
    size_t rx_dma_buf_size;

    /* Persistent buffers for initial registration to avoid stack UAF */
    struct dmesh_register_msg reg_msg;
    struct dmesh_pod_consumer_id_msg pod_cid_msg;

    /* TX slot management (body pool) */
    uint8_t *slot_bitmap;
    pthread_mutex_t slot_lock;
    pthread_cond_t  slot_cond;  /* Signaled when a TX slot is freed */

    /* Phase 1 (v2 plan): independent HDR TX pool. DOCA mmap exported to DPU
     * as DMA_HOST_TX_HDR_BUFFER. DPA forward kernel picks src mmap via
     * desc->mmap override (= hdr_tx_dpa_handle when src_body_pool_type ==
     * POOL_HOST_TX_HDR), so hdr DMAs read out of this pool instead of
     * dma_buffer. Independent bitmap / cond means hdr alloc never blocks
     * on a body-saturated bitmap (Hard Rule #6). */
    void                 *hdr_tx_buffer;
    struct doca_mmap     *hdr_tx_mmap;
    doca_dpa_dev_mmap_t   hdr_tx_dpa_handle;
    uint8_t              *hdr_tx_bitmap;
    pthread_mutex_t       hdr_slot_lock;
    pthread_cond_t        hdr_slot_cond;
    int                   hdr_num_slots;
    int                   hdr_slot_size;

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


/*
 * Deliver a fully parsed descriptor to the pending table or RX queue.
 * Common path for both comch-based RX_DATA and DMA-based DMA_COMPLETION.
 */
static void rx_deliver_desc(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc, int slot)
{
    if (desc->flags & OP_RESPONSE) {
        uint32_t idx = desc->req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        pthread_mutex_lock(&p->lock);
        if (p->state == 0) {
            p->desc = *desc;
            p->state = 1;
            pthread_cond_signal(&p->cond);
        } else if (p->state == -2) {
            /* Cancelled request — DPA finished, now safe to free both pools + RX */
            if (p->tx_slot >= 0) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
            }
            if (p->hdr_tx_slot >= 0) {
                dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
                p->hdr_tx_slot = -1;
            }
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            p->state = -1;
            pthread_cond_broadcast(&p->cond);
        } else {
            DOCA_LOG_ERR("RX deliver: OP_RESPONSE for req_id=%u but no waiter (state=%d)",
                         desc->req_id, p->state);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
        pthread_mutex_unlock(&p->lock);
    } else {
        pthread_mutex_lock(&ctx->rx_lock);

        /* No blocking in PE callback path: if the RX queue is full, drop
         * immediately so we don't stall the PE thread. Slot-based admission
         * at the producer side (rx_slot_alloc above + sender's tx_alloc)
         * keeps in-flight bounded; blocking here while waiting for consumers
         * starves other PE work and can deadlock under load. */
        if (ctx->rx_count >= RX_QUEUE_SIZE) {
            pthread_mutex_unlock(&ctx->rx_lock);
            DOCA_LOG_ERR("RX deliver: queue full, dropping req_id=%u", desc->req_id);
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
            return;
        }

        ctx->rx_queue[ctx->rx_tail] = *desc;
        ctx->rx_tail = (ctx->rx_tail + 1) % RX_QUEUE_SIZE;
        ctx->rx_count++;
        pthread_cond_signal(&ctx->rx_cond);
        pthread_mutex_unlock(&ctx->rx_lock);
    }
}

/*
 * Parse + deliver one DMA-reverse entry at rx_dma_buffer[pos] whose body
 * length is dma_len. Per-request metadata (req_id, src_pod_id, dst_pod_id,
 * flags) is taken from the comch DMA_COMPLETION message — NOT from the DMA
 * payload. The DMA payload is the body itself (no in-band header).
 * Returns 0 on success, -1 on malformed/undeliverable.
 */
static int process_rx_dma_entry(dpumesh_ctx_t *ctx, uint32_t pos, uint32_t dma_len,
                                uint32_t req_id, int32_t src_pod_id,
                                int32_t dst_pod_id, int8_t flags) {
    if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->rx_dma_buf_size);
        return -1;
    }
    uint8_t *body = (uint8_t *)ctx->rx_dma_buffer + pos;
    uint32_t body_len = dma_len;

    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("process_rx_dma_entry: no free RX slots, dropping req_id=%u", req_id);
        return -1;
    }
    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    if (body_len > 0)
        memcpy(dst, body, body_len);

    sw_descriptor_t desc;
    memset(&desc, 0, sizeof(desc));
    desc.req_id        = req_id;
    desc.src_pod_id    = src_pod_id;
    desc.dst_pod_id    = dst_pod_id;
    desc.flags         = flags;
    desc.header_buf_slot = -1;
    desc.body_buf_slot = slot;
    desc.body_len      = body_len;
    desc.valid         = 1;

    rx_deliver_desc(ctx, &desc, slot);
    return 0;
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_comch_msg *comch_msg = (const struct dmesh_comch_msg *)data;

    if (comch_msg->type == DMESH_MSG_TX_ACK) {
        /* === TX_ACK: per-request notification that DPU has consumed the
         * forward DMA tied to req_id — host's TX slot for this request can
         * now be released. Pure event signal; no flow-control piggyback. */
        struct dmesh_tx_ack_msg ack;
        if (len < sizeof(ack)) {
            DOCA_LOG_ERR("TX_ACK: too short (len=%u need=%zu)", len, sizeof(ack));
            return;
        }
        memcpy(&ack, data, sizeof(ack));

        uint32_t idx = ack.req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        /* Phase 1: pool_type tells us which pool. Legacy DPUs send pool_type=0
         * (POOL_NONE) which we map to BODY for compat. */
        int is_hdr = (ack.pool_type == POOL_HOST_TX_HDR);
        pthread_mutex_lock(&p->lock);
        if (p->state == 0 || p->state == -2) {
            if (is_hdr) {
                if (p->hdr_tx_slot >= 0) {
                    dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
                    p->hdr_tx_slot = -1;
                }
            } else {
                if (p->tx_slot >= 0) {
                    dpumesh_tx_free(ctx, p->tx_slot);
                    p->tx_slot = -1;
                }
            }
            /* state=-2 transitions to -1 only when BOTH slots are free —
             * a single user RPC may have owned both pools, so a partial
             * ACK leaves the entry deferred. */
            if (p->state == -2 && p->tx_slot < 0 && p->hdr_tx_slot < 0) {
                p->state = -1;
                pthread_cond_broadcast(&p->cond);
            }
        }
        pthread_mutex_unlock(&p->lock);

        DOCA_LOG_DBG("TX_ACK: freed %s slot for req_id=%u",
                     is_hdr ? "HDR" : "BODY", ack.req_id);
        return;
    }

    if (comch_msg->type == DMESH_MSG_DMA_COMPLETION) {
        /* === Reverse DMA notification (DPU→CPU) ===
         * DPU ARM forwards this after DPA completes DMA from DPU TX buffer
         * to Host RX buffer. Data (body only) is already at rx_dma_buffer[pos].
         * Per-request metadata (req_id, src/dst pod, flags, length) comes
         * from comp itself — not from the DMA payload. */
        struct dmesh_dma_completion_msg comp;
        if (len < sizeof(comp)) {
            DOCA_LOG_ERR("DMA_COMPLETION: too short (len=%u need=%zu)", len, sizeof(comp));
            return;
        }
        memcpy(&comp, data, sizeof(comp));

        uint32_t pos = comp.pos;
        uint32_t dma_len = comp.length;

        if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size) {
            DOCA_LOG_ERR("DMA_COMPLETION: invalid pos=%u len=%u buf_size=%zu",
                         pos, dma_len, ctx->rx_dma_buf_size);
            return;
        }

        /* Process the entry. End-node slot-based admission keeps in-flight
         * bytes ≤ buf_size, so DPU never laps. We trust DMA_COMPLETION
         * delivery (no gap-recovery scan). */
        if (process_rx_dma_entry(ctx, pos, dma_len,
                                 comp.req_id, comp.src_pod_id,
                                 comp.dst_pod_id, comp.flags) != 0) {
            DOCA_LOG_WARN("DMA_COMPLETION: process_rx_dma_entry failed at pos=%u len=%u",
                          pos, dma_len);
        }
        return;
    }

    /* === Legacy comch data path (DMESH_MSG_RX_DATA) === */
    const struct dmesh_rx_data_msg *msg = (const struct dmesh_rx_data_msg *)data;

    DOCA_LOG_DBG("rx_data_hook ENTER: len=%u body_len=%u sizeof_hdr=%zu",
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

    DOCA_LOG_DBG("rx_data_hook DESC: req_id=%u flags=0x%x dst_pod=%d src_pod=%d slot=%d body_len=%u",
                 desc.req_id, (unsigned)(uint8_t)desc.flags, desc.dst_pod_id, desc.src_pod_id,
                 slot, desc.body_len);

    rx_deliver_desc(ctx, &desc, slot);
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

    /* NOTE: init_comch_datapath_producer() removed — CPU→DPU uses DMA ring,
     * DPU→CPU uses reverse DMA. comch datapath producer no longer needed. */

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

    result = doca_mmap_dev_get_dpa_handle(ctx->doca_objs.local_mmap, ctx->doca_objs.dev, &ctx->dpa_mmap_handle);
    if (result != DOCA_SUCCESS) return result;

    /* Allocate Host RX DMA buffer (PCI mmap, DPA writes DPU→CPU data here) */
    ctx->rx_dma_buf_size = buf_size;
    result = alloc_buffer_and_set_mmap(&ctx->rx_dma_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->rx_dma_buffer,
                                       ctx->rx_dma_buf_size,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate Host RX DMA buffer: %s", doca_err_str(result));
        return result;
    }

    /* Export Host RX buffer to DPU so DPA can get mmap handle for reverse DMA */
    result = export_mmap_to_remote(&ctx->doca_objs, ctx->rx_dma_mmap,
                                   ctx->rx_dma_buffer, ctx->rx_dma_buf_size,
                                   DMA_HOST_RX_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("Failed to export Host RX buffer to DPU: %s", doca_err_str(result));
    }

    /* Phase 1: HDR TX pool. Smaller than the body pool — hdr batches are
     * tiny (96 entries × 80B = 7.5KB max per slot). 1024 × 8KB = 8MB total. */
    ctx->hdr_num_slots = 1024;
    ctx->hdr_slot_size = DPUMESH_SLOT_SIZE_DEFAULT;
    size_t hdr_buf_size = (size_t)ctx->hdr_num_slots * ctx->hdr_slot_size;
    result = alloc_buffer_and_set_mmap(&ctx->hdr_tx_mmap,
                                       ctx->doca_objs.dev,
                                       &ctx->hdr_tx_buffer,
                                       hdr_buf_size,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate HDR TX buffer: %s", doca_err_str(result));
        return result;
    }

    result = export_mmap_to_remote(&ctx->doca_objs, ctx->hdr_tx_mmap,
                                   ctx->hdr_tx_buffer, hdr_buf_size,
                                   DMA_HOST_TX_HDR_BUFFER, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export HDR TX buffer to DPU: %s", doca_err_str(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(ctx->hdr_tx_mmap, ctx->doca_objs.dev,
                                          &ctx->hdr_tx_dpa_handle);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get DPA handle for HDR TX mmap: %s", doca_err_str(result));
        return result;
    }

    return DOCA_SUCCESS;
}

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;

    init_config(ctx, config, app_name, worker_num);

    if (init_doca_device(ctx) != DOCA_SUCCESS) goto fail;
    if (init_control_path(ctx) != DOCA_SUCCESS) goto fail;
    if (init_datapath(ctx) != DOCA_SUCCESS) goto fail;

    ctx->slot_bitmap = (uint8_t *)calloc(ctx->num_slots, 1);
    if (!ctx->slot_bitmap) goto fail;
    pthread_mutex_init(&ctx->slot_lock, NULL);
    pthread_cond_init(&ctx->slot_cond, NULL);
    pthread_mutex_init(&ctx->ring_lock, NULL);

    /* Phase 1: HDR TX bitmap + slot lock (independent from body pool). */
    ctx->hdr_tx_bitmap = (uint8_t *)calloc(ctx->hdr_num_slots, 1);
    if (!ctx->hdr_tx_bitmap) goto fail;
    pthread_mutex_init(&ctx->hdr_slot_lock, NULL);
    pthread_cond_init(&ctx->hdr_slot_cond, NULL);

    /* rx_buffer is the STAGING area for delivered messages — must be separate
     * from rx_dma_buffer (the DMA landing zone). If they share memory, DPU
     * can overwrite slot contents after the worker copies them out, corrupting
     * in-flight messages for the upper layer (seen as "Frame size has negative
     * value" near wrap). */
    {
        size_t rx_buf_bytes = (size_t)ctx->num_slots * ctx->slot_size;
        ctx->rx_buffer = calloc(1, rx_buf_bytes);
        if (!ctx->rx_buffer) goto fail;
    }
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
        ctx->pending[i].tx_slot = -1;
        ctx->pending[i].hdr_tx_slot = -1;
    }

    ctx->doca_objs.rx_data_hook = rx_data_hook;
    ctx->doca_objs.rx_hook_ctx = ctx;

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

    /* Free resources BEFORE destroying locks they depend on.
     * Pending cleanup calls dpumesh_tx_free/rx_free which acquire
     * slot_lock/rx_slot_lock. */

    for (int i = 0; i < MAX_PENDING; i++) {
        dpumesh_pending_t *p = &ctx->pending[i];
        pthread_mutex_lock(&p->lock);
        if (p->state == 1 && p->desc.body_buf_slot >= 0) {
            dpumesh_rx_free(ctx, p->desc.body_buf_slot);
        }
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_mutex_unlock(&p->lock);
        pthread_mutex_destroy(&p->lock);
        pthread_cond_destroy(&p->cond);
    }

    /* Destroy DMA landing-zone mmap + buffer (Host RX DMA buffer).
     * Must happen before cleanup_objects destroys the device. */
    if (ctx->rx_dma_mmap) {
        doca_mmap_destroy(ctx->rx_dma_mmap);
        ctx->rx_dma_mmap = NULL;
    }
    if (ctx->rx_dma_buffer) {
        free(ctx->rx_dma_buffer);
        ctx->rx_dma_buffer = NULL;
    }

    /* Phase 1: HDR TX pool teardown — must come before cleanup_objects()
     * destroys the device that the mmap belongs to. */
    if (ctx->hdr_tx_mmap) {
        doca_mmap_destroy(ctx->hdr_tx_mmap);
        ctx->hdr_tx_mmap = NULL;
    }
    if (ctx->hdr_tx_buffer) {
        free(ctx->hdr_tx_buffer);
        ctx->hdr_tx_buffer = NULL;
    }

    cleanup_objects(&ctx->doca_objs);

    pthread_mutex_destroy(&ctx->ring_lock);
    pthread_cond_destroy(&ctx->slot_cond);
    pthread_mutex_destroy(&ctx->slot_lock);
    if (ctx->slot_bitmap) free(ctx->slot_bitmap);

    pthread_mutex_destroy(&ctx->hdr_slot_lock);
    pthread_cond_destroy(&ctx->hdr_slot_cond);
    if (ctx->hdr_tx_bitmap) free(ctx->hdr_tx_bitmap);

    pthread_mutex_destroy(&ctx->rx_slot_lock);
    if (ctx->rx_slot_bitmap) free(ctx->rx_slot_bitmap);
    if (ctx->rx_buffer) {
        free(ctx->rx_buffer);
        ctx->rx_buffer = NULL;
    }
    pthread_mutex_destroy(&ctx->rx_lock);
    pthread_cond_destroy(&ctx->rx_cond);
    pthread_cond_destroy(&ctx->rx_not_full);

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
    /* Backpressure: block until a TX slot is free. The caller has already
     * committed to this request, so failure here would propagate as a
     * Thrift exception — unwanted.
     *
     * Latency-tuned wait: at 44K RPS the per-request budget is ~23µs, so
     * a 1ms cond_timedwait blocks ~44 requests-worth of progress. We use
     * a 50µs re-poll backstop instead. The cond is signaled directly by
     * tx_free / TX_ACK handlers, so the timedwait fires only when a
     * signal was missed (rare race). */
    pthread_mutex_lock(&ctx->slot_lock);
    for (;;) {
        for (int i = 0; i < ctx->num_slots; i++) {
            if (ctx->slot_bitmap[i] == 0) {
                ctx->slot_bitmap[i] = 1;
                pthread_mutex_unlock(&ctx->slot_lock);
                return i;
            }
        }

        /* 50µs re-poll backstop (was 1ms — too coarse for cap-region latency). */
        struct timespec abs_ts;
        clock_gettime(CLOCK_REALTIME, &abs_ts);
        abs_ts.tv_nsec += 50000;  /* 50 µs */
        if (abs_ts.tv_nsec >= 1000000000) {
            abs_ts.tv_nsec -= 1000000000;
            abs_ts.tv_sec += 1;
        }
        pthread_cond_timedwait(&ctx->slot_cond, &ctx->slot_lock, &abs_ts);
    }
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return NULL;
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Flow control ensures no stale read — no memset needed */
    pthread_mutex_lock(&ctx->slot_lock);
    ctx->slot_bitmap[slot] = 0;
    pthread_cond_signal(&ctx->slot_cond);
    pthread_mutex_unlock(&ctx->slot_lock);
}

/* ====================================================================
 * Phase 1: HDR TX pool — body's mirror, independent bitmap/lock/cond
 * ==================================================================== */

int dpumesh_hdr_tx_alloc(dpumesh_ctx_t *ctx) {
    pthread_mutex_lock(&ctx->hdr_slot_lock);
    for (;;) {
        for (int i = 0; i < ctx->hdr_num_slots; i++) {
            if (ctx->hdr_tx_bitmap[i] == 0) {
                ctx->hdr_tx_bitmap[i] = 1;
                pthread_mutex_unlock(&ctx->hdr_slot_lock);
                return i;
            }
        }
        /* 50µs re-poll backstop — matches body pool. */
        struct timespec abs_ts;
        clock_gettime(CLOCK_REALTIME, &abs_ts);
        abs_ts.tv_nsec += 50000;
        if (abs_ts.tv_nsec >= 1000000000) {
            abs_ts.tv_nsec -= 1000000000;
            abs_ts.tv_sec += 1;
        }
        pthread_cond_timedwait(&ctx->hdr_slot_cond, &ctx->hdr_slot_lock, &abs_ts);
    }
}

uint8_t *dpumesh_hdr_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->hdr_num_slots) return NULL;
    return (uint8_t *)ctx->hdr_tx_buffer + ((size_t)slot * ctx->hdr_slot_size);
}

void dpumesh_hdr_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->hdr_num_slots) return;
    pthread_mutex_lock(&ctx->hdr_slot_lock);
    ctx->hdr_tx_bitmap[slot] = 0;
    pthread_cond_signal(&ctx->hdr_slot_cond);
    pthread_mutex_unlock(&ctx->hdr_slot_lock);
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    struct dma_desc *dma;
    uint32_t ring_slot;

    if (desc == NULL) {
        DOCA_LOG_ERR("ENQUEUE rejected: desc is NULL");
        return -1;
    }

    /* Phase 1: pool dispatch. desc->src_body_pool_type tells us which TX
     * pool holds the bytes; defaults to body for legacy callers. The
     * body_buf_slot field names the slot in whichever pool was selected. */
    int is_hdr_pool = (desc->src_body_pool_type == POOL_HOST_TX_HDR);
    int pool_slots = is_hdr_pool ? ctx->hdr_num_slots : ctx->num_slots;
    int pool_slot_size = is_hdr_pool ? ctx->hdr_slot_size : ctx->slot_size;

    if (desc->body_buf_slot < 0 || desc->body_buf_slot >= pool_slots) {
        DOCA_LOG_ERR("ENQUEUE rejected: invalid body_buf_slot=%d (pool=%s slots=%d)",
                     desc->body_buf_slot, is_hdr_pool ? "HDR" : "BODY", pool_slots);
        return -1;
    }

    if (desc->body_len > (uint32_t)pool_slot_size) {
        DOCA_LOG_ERR("ENQUEUE rejected: body_len=%u exceeds slot_size=%d (pool=%s)",
                     desc->body_len, pool_slot_size, is_hdr_pool ? "HDR" : "BODY");
        return -1;
    }

    /* Lock ring access: serializes get_next_dma_desc + descriptor fill + valid=1.
     *
     * Flow control: end-to-end via slot-based admission only. dpumesh_tx_alloc
     * has already gated this call on slot_bitmap availability, and num_slots ×
     * slot_size = DPU_BUFFER_SIZE, so total in-flight bytes inside DPU's
     * buffer can never exceed buffer size. DPU/DPA do no FC of their own. */
    pthread_mutex_lock(&ctx->ring_lock);

    /* Block with exponential backoff until a DMA ring slot frees. DPA
     * advances the ring tail as it consumes descriptors. backoff capped at
     * 50µs (was 1ms — at 44K RPS, 1ms = 44 requests-worth of latency
     * stalled in this loop while DPA is actively draining the ring). */
    {
        struct timespec backoff = {0, 1000}; /* 1µs initial */
        while (1) {
            dma = get_next_dma_desc(ctx->dma_ring);
            if (dma)
                break;
            pthread_mutex_unlock(&ctx->ring_lock);
            nanosleep(&backoff, NULL);
            if (backoff.tv_nsec < 50000) /* cap at 50µs */
                backoff.tv_nsec *= 2;
            pthread_mutex_lock(&ctx->ring_lock);
        }
    }

    ring_slot = (uint32_t)(dma - ctx->dma_ring->descs);

    /* TX slot lifetime is owned by the pending mechanism for BOTH OP_REQUEST
     * (gateway) and OP_RESPONSE (server transport):
     *   - OP_REQUEST: caller registers + attach_tx; wait_response/timeout
     *     paths or TX_ACK handler free the TX slot.
     *   - OP_RESPONSE: caller registers + attach_tx + release_async; TX_ACK
     *     handler frees the TX slot via the deferred state -2 → -1 path. */

    /* Phase 1: src override via desc->mmap. DPA's forward kernel picks
     *   src_mmap = desc->mmap ? desc->mmap : ring->host_mmap
     * and skips the default range check when desc->mmap != 0. Body path
     * keeps desc->mmap = 0 to preserve the original behavior (DPA falls
     * back to ring->host_mmap = body DPA handle) — pre-Phase-1 code
     * always wrote ctx->dpa_mmap_handle here, but the DPA kernel never
     * read it, so this is a no-op semantically. HDR path writes the
     * dedicated hdr DPA handle. */
    if (is_hdr_pool) {
        dma->mmap = ctx->hdr_tx_dpa_handle;
        dma->addr = (uint64_t)ctx->hdr_tx_buffer +
                    ((size_t)desc->body_buf_slot * ctx->hdr_slot_size);
    } else {
        dma->mmap = 0;
        dma->addr = (uint64_t)ctx->dma_buffer +
                    ((size_t)desc->body_buf_slot * ctx->slot_size);
    }
    dma->size = desc->body_len;
    dma->idx  = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    DOCA_LOG_DBG("ENQUEUE: req_id=%u slot=%u len=%u",
                 desc->req_id, ring_slot, desc->body_len);

    pthread_mutex_unlock(&ctx->ring_lock);

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
    /* Flow control ensures no overwrite of unconsumed data — no memset needed */
    pthread_mutex_lock(&ctx->rx_slot_lock);
    ctx->rx_slot_bitmap[slot] = 0;
    pthread_mutex_unlock(&ctx->rx_slot_lock);

    /* DPA-poll credit return: bump the counter at the LAST (extra) slot of
     * the dma_ring buffer. DPA polls this slot via the same buf_arr it
     * already uses for forward dma_desc reads — no separate buf_arr, no
     * extra PCIe read mechanism. ~10ns on host hot path. */
    if (ctx->dma_ring && ctx->dma_ring->descs) {
        volatile uint64_t *credit =
            (volatile uint64_t *)(ctx->dma_ring->descs + ctx->dma_ring->size);
        __sync_add_and_fetch(credit, 1);
    }
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
                /* Stuck. If the previous request is in state -2 (timed out,
                 * TX deferred), the DPU owes us a TX_ACK — if it has not
                 * arrived after (wait_response timeout + 2s), the link is
                 * effectively dead. Reclaim the slot to keep the pending
                 * table from wedging the whole gateway; the late TX_ACK
                 * (if ever) will find state=-1 and no-op. */
                if (p->state == -2) {
                    if (p->tx_slot >= 0) {
                        dpumesh_tx_free(ctx, p->tx_slot);
                        p->tx_slot = -1;
                    }
                    if (p->hdr_tx_slot >= 0) {
                        dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
                        p->hdr_tx_slot = -1;
                    }
                    DOCA_LOG_WARN("Pending slot %u reclaimed after -2 timeout (req_id=%u)",
                                  idx, req_id);
                    break;  /* fall through to claim the slot */
                }
                pthread_mutex_unlock(&p->lock);
                DOCA_LOG_ERR("Pending slot collision: req_id=%u idx=%u stuck state=%d",
                             req_id, idx, p->state);
                return -1;
            }
        }
    }

    p->state = 0;
    p->tx_slot = -1;
    p->hdr_tx_slot = -1;
    memset(&p->desc, 0, sizeof(p->desc));
    pthread_mutex_unlock(&p->lock);
    return 0;
}

void dpumesh_pending_attach_tx(dpumesh_ctx_t *ctx, uint32_t req_id, int tx_slot) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    p->tx_slot = tx_slot;
    pthread_mutex_unlock(&p->lock);
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
                /* Timeout: force-free the TX slot here.
                 *
                 * The previous design deferred the free to TX_ACK arrival
                 * out of fear that DPA might still be reading the slot.
                 * That fear is unfounded at the wait_response timeout
                 * scale (RESPONSE_TIMEOUT_MS, default 30s) — DPA forward
                 * DMA completes in microseconds. Deferring instead caused
                 * a real problem: TX_ACK can be permanently lost when the
                 * DPU's deferred-ack queue overflows under sustained
                 * comch backpressure (see deferred_tx_acks DROP path in
                 * dpu_worker.c). A lost TX_ACK leaked the slot until
                 * either cancel_pending was invoked or req_id wrapped
                 * MAX_PENDING (~65k requests later) — the slow drift
                 * behind the intermittent test hangs. By 30s, regardless
                 * of TX_ACK delivery, the DPA has long finished, so
                 * reclaiming here is safe.
                 *
                 * State stays at -2 so a late RX of OP_RESPONSE still
                 * cleans up the rx_slot path (rx_deliver_desc handles
                 * state=-2). A late TX_ACK now finds tx_slot=-1 and
                 * no-ops — non-load-bearing for slot lifetime. */
                if (p->tx_slot >= 0) {
                    dpumesh_tx_free(ctx, p->tx_slot);
                    p->tx_slot = -1;
                }
                if (p->hdr_tx_slot >= 0) {
                    dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
                    p->hdr_tx_slot = -1;
                }
                p->state = -2;
                pthread_cond_broadcast(&p->cond);
                pthread_mutex_unlock(&p->lock);
                return -1;
            }
        }
    }

    if (p->state == 1) {
        *resp = p->desc;
        /* Response arrived = DPA finished reading TX buffers. Free both
         * pools now to return slots ASAP under high load. */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return 0;
    }

    /* Unreachable in practice — state left the wait loop without being 1
     * only via the timeout/-2 branch above. Clear both pool slots defensively. */
    if (p->state != -2) {
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    }
    pthread_mutex_unlock(&p->lock);
    return -1;
}

void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    if (p->state == 1) {
        /* Response arrived but was never consumed — free RX + TX (both pools) */
        if (p->desc.body_buf_slot >= 0) {
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[p->desc.body_buf_slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == 0) {
        /* In-flight, no timeout yet — caller gave up; free TX (both pools) now. */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == -2) {
        /* Timeout path: force-free both pools (see body-only comment above
         * for the rationale on why 30s after enqueue is always safe). */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        if (p->hdr_tx_slot >= 0) {
            dpumesh_hdr_tx_free(ctx, p->hdr_tx_slot);
            p->hdr_tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return;
    } else {
        /* state == -1, already clean — nothing to do. */
    }
    pthread_mutex_unlock(&p->lock);
}

/*
 * Asynchronous-release a pending entry without waiting for a response.
 *
 * Used by responder-side code (server transport) that has just enqueued an
 * OP_RESPONSE: it needs the TX slot held until DPA finishes the forward DMA,
 * confirmed by TX_ACK from DPU. This function tells the pending machinery
 * "no response is coming for this req_id; free TX on TX_ACK and clear the
 * entry". This mirrors the gateway's TX-slot lifecycle (early free on
 * TX_ACK arrival) for the responder direction.
 *
 * Only acts on state == 0; other states are left alone since they're
 * already owned by another path (response arrived, cancelled, or unused).
 */
void dpumesh_pending_release_async(dpumesh_ctx_t *ctx, uint32_t req_id) {
    uint32_t idx = req_id % MAX_PENDING;
    dpumesh_pending_t *p = &ctx->pending[idx];

    pthread_mutex_lock(&p->lock);
    if (p->state == 0) {
        /* Phase 1: BOTH pool slots must be free before we can transition
         * to -1 (a single user RPC may have owned both). If either is
         * still attached, defer to TX_ACK via state=-2. */
        if (p->tx_slot < 0 && p->hdr_tx_slot < 0) {
            p->state = -1;
            pthread_cond_broadcast(&p->cond);
        } else {
            /* TX_ACK still pending on at least one pool — switch to deferred
             * release. TX_ACK handler tracks both slots and flips to -1 when
             * both are back. */
            p->state = -2;
        }
    }
    /* state ∈ {-1, -2, 1}: someone else is finishing the entry — no-op. */
    pthread_mutex_unlock(&p->lock);
}
