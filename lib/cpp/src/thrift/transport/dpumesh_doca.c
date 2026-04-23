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

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sw_descriptor_t desc;
    volatile int state;   /* -1=unused, -2=cancelled(tx deferred), 0=waiting, 1=arrived */
    int tx_slot;          /* TX buffer slot owned by this request, -1 if none */
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

    /* Flow control state (Host as sender of CPU→DPU) */
    uint32_t fc_tx_producer_head;
    uint32_t fc_tx_last_consumer_tail;  /* last known DPU RX consumption */

    /* Flow control state (Host as receiver of DPU→CPU) */
    uint32_t fc_rx_consumer_tail;

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

    /* Deferred TX slot free: ring_slot → tx_slot mapping.
     * When DPA processes a ring descriptor (clears valid=0) and the slot is
     * reused by get_next_dma_desc, the associated TX slot can be freed.
     * -1 means no TX slot to free for that ring slot. */
    int ring_tx_slot_map[DMA_RING_SIZE];

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
            /* Cancelled request — DPA finished, now safe to free TX + RX */
            if (p->tx_slot >= 0) {
                dpumesh_tx_free(ctx, p->tx_slot);
                p->tx_slot = -1;
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
         * immediately so we don't stall the PE thread. The DMA counter
         * gating (fc_rx_consumer_tail) is the proper backpressure signal;
         * blocking here while waiting for consumers starves other PE work
         * and can deadlock under load. */
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
 * Parse + deliver one DMA-reverse entry at rx_dma_buffer[pos] whose total DMA
 * length is dma_len. On success delivers the descriptor via rx_deliver_desc
 * and clears sw_descriptor.valid in the buffer so a future scan cannot
 * re-process a stale copy after the buffer wraps. Returns 0 on success, -1
 * if the entry is malformed or can't be delivered. Caller is responsible for
 * advancing fc_rx_consumer_tail — this function only touches the single
 * entry's bytes.
 */
static int process_rx_dma_entry(dpumesh_ctx_t *ctx, uint32_t pos, uint32_t dma_len) {
    if (!ctx->rx_dma_buffer || pos + dma_len > ctx->rx_dma_buf_size) {
        DOCA_LOG_ERR("process_rx_dma_entry: bounds fail pos=%u len=%u buf=%zu",
                     pos, dma_len, ctx->rx_dma_buf_size);
        return -1;
    }
    uint8_t *buf = (uint8_t *)ctx->rx_dma_buffer + pos;

    if (dma_len < sizeof(struct fc_header)) return -1;
    struct fc_header *hdr = (struct fc_header *)buf;
    uint32_t payload_len = hdr->payload_len;
    if (sizeof(struct fc_header) + payload_len > dma_len) return -1;

    uint8_t *payload = buf + sizeof(struct fc_header);
    if (payload_len < sizeof(sw_descriptor_t)) return -1;

    sw_descriptor_t desc;
    memcpy(&desc, payload, sizeof(desc));

    /* Clear valid byte in the buffer so a later scan can distinguish a
     * fresh DPU-written entry (valid=1) from a stale already-processed one
     * after the producer wraps past this offset. */
    ((sw_descriptor_t *)payload)->valid = 0;

    uint32_t body_len = payload_len - sizeof(sw_descriptor_t);
    uint8_t *body = payload + sizeof(sw_descriptor_t);

    int slot = rx_slot_alloc(ctx);
    if (slot < 0) {
        DOCA_LOG_ERR("process_rx_dma_entry: no free RX slots, dropping req_id=%u", desc.req_id);
        return -1;
    }
    uint8_t *dst = (uint8_t *)ctx->rx_buffer + ((size_t)slot * ctx->slot_size);
    if (body_len > 0)
        memcpy(dst, body, body_len);

    desc.body_buf_slot = slot;
    desc.body_len = body_len;

    rx_deliver_desc(ctx, &desc, slot);
    return 0;
}

/*
 * Recovery scan: when a DMA_COMPLETION arrives at end_pos but our
 * fc_rx_consumer_tail is still at start_pos < end_pos (mod buffer wrap),
 * a previous notification was lost. The DPU has already DMA'd entries into
 * the gap and they sit in the RX buffer with sw_descriptor.valid == 1. We
 * walk the gap, processing each valid entry, using the size declared in its
 * own fc_header.payload_len to know how far to step. Stops as soon as we hit
 * a slot where valid != 1 or the header looks bogus, since any further
 * stepping would be guessing.
 */
static void scan_and_recover_rx_gap(dpumesh_ctx_t *ctx, uint32_t start_pos, uint32_t end_pos) {
    uint32_t buf_size = (uint32_t)ctx->rx_dma_buf_size;
    if (buf_size == 0) return;
    uint32_t scan_pos = start_pos;
    /* Safety: limit iterations to the number of 128-byte slots in the buffer */
    uint32_t max_iter = buf_size / 128 + 1;

    int recovered = 0;
    while (scan_pos != end_pos && max_iter-- > 0) {
        if (scan_pos + sizeof(struct fc_header) + sizeof(sw_descriptor_t) > buf_size) {
            DOCA_LOG_WARN("scan_and_recover: scan_pos=%u too close to end for header+desc", scan_pos);
            break;
        }
        uint8_t *buf = (uint8_t *)ctx->rx_dma_buffer + scan_pos;
        struct fc_header *hdr = (struct fc_header *)buf;
        sw_descriptor_t *sdesc = (sw_descriptor_t *)(buf + sizeof(struct fc_header));

        if (sdesc->valid != 1) {
            DOCA_LOG_WARN("scan_and_recover: no valid entry at pos=%u (valid=%d) — stop",
                          scan_pos, (int)sdesc->valid);
            break;
        }
        uint32_t payload_len = hdr->payload_len;
        uint32_t max_payload = (uint32_t)ctx->slot_size; /* conservative upper bound */
        if (payload_len < sizeof(sw_descriptor_t) || payload_len > max_payload) {
            DOCA_LOG_WARN("scan_and_recover: bad payload_len=%u at pos=%u — stop",
                          payload_len, scan_pos);
            break;
        }
        uint32_t dma_len = (uint32_t)sizeof(struct fc_header) + payload_len;
        if (scan_pos + dma_len > buf_size) {
            DOCA_LOG_WARN("scan_and_recover: entry at pos=%u overflows buffer (dma_len=%u)",
                          scan_pos, dma_len);
            break;
        }

        DOCA_LOG_WARN("scan_and_recover: recovering lost entry at pos=%u req_id=%u dma_len=%u",
                      scan_pos, sdesc->req_id, dma_len);
        if (process_rx_dma_entry(ctx, scan_pos, dma_len) == 0)
            recovered++;

        uint32_t padded = (dma_len + 127) & ~(uint32_t)127;
        scan_pos += padded;
        if (scan_pos >= buf_size) scan_pos = 0;
    }

    if (recovered > 0)
        DOCA_LOG_WARN("scan_and_recover: recovered %d lost entries (start=%u end=%u)",
                      recovered, start_pos, end_pos);
}

static void rx_data_hook(void *hook_ctx, const uint8_t *data, uint32_t len) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)hook_ctx;
    const struct dmesh_comch_msg *comch_msg = (const struct dmesh_comch_msg *)data;

    if (comch_msg->type == DMESH_MSG_TX_ACK) {
        /* === TX_ACK: forward DMA consumed, free sender's TX slot ===
         * DPU ARM sends this when CPU→DPU DMA completes — data copied
         * to DPU buffer, Host TX slot can be freed immediately.
         * ack.consumer_tail piggybacks DPU's rx_consumer_tail so we can
         * refresh the flow-control window even on an idle link. */
        struct dmesh_tx_ack_msg ack;
        if (len < sizeof(ack)) {
            DOCA_LOG_ERR("TX_ACK: too short (len=%u need=%zu)", len, sizeof(ack));
            return;
        }
        memcpy(&ack, data, sizeof(ack));

        /* Refresh flow-control window from piggybacked tail. Ignore bogus
         * values (first ACK before any real consumption may still be 0
         * which is fine; only guard against out-of-range). */
        if (ack.consumer_tail < DPU_BUFFER_SIZE)
            ctx->fc_tx_last_consumer_tail = ack.consumer_tail;

        uint32_t idx = ack.req_id % MAX_PENDING;
        dpumesh_pending_t *p = &ctx->pending[idx];
        pthread_mutex_lock(&p->lock);
        if ((p->state == 0 || p->state == -2) && p->tx_slot >= 0) {
            /* state 0: request still waiting — free TX slot early.
             * state -2: wait_response gave up on timeout but deferred the TX
             *           free until DPA finished the forward DMA; this ACK
             *           confirms that, so we can release the slot now. */
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
            if (p->state == -2) {
                p->state = -1;
                pthread_cond_broadcast(&p->cond);
            }
        }
        pthread_mutex_unlock(&p->lock);

        DOCA_LOG_INFO("TX_ACK: freed TX slot for req_id=%u tail=%u", ack.req_id, ack.consumer_tail);
        return;
    }

    if (comch_msg->type == DMESH_MSG_DMA_COMPLETION) {
        /* === Reverse DMA path (DPU→CPU) ===
         * DPU ARM forwards this after DPA completes DMA from DPU TX buffer
         * to Host RX buffer. Data is already at rx_dma_buffer[pos].
         * Layout at pos: fc_header(8B) + sw_descriptor(64B) + body(N). */
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
        if (dma_len < sizeof(struct fc_header)) {
            DOCA_LOG_ERR("DMA_COMPLETION: too short for fc_header (len=%u)", dma_len);
            return;
        }

        uint8_t *buf = (uint8_t *)ctx->rx_dma_buffer + pos;
        struct fc_header *hdr = (struct fc_header *)buf;

        /* Update flow control: DPU told us how much of our TX buffer it consumed.
         * Ignore values >= DPU_BUFFER_SIZE — the DPU's first response may
         * piggyback an uninitialized rx_consumer_tail before any data is consumed. */
        if (hdr->consumer_tail < DPU_BUFFER_SIZE)
            ctx->fc_tx_last_consumer_tail = hdr->consumer_tail;

        /* Snapshot the old tail BEFORE advancing — we need it to detect a gap
         * caused by a lost comch DMA_COMPLETION. The DPU writes reverse-DMA
         * entries into the RX buffer in order, but the control-path notification
         * can occasionally be dropped; when that happens the host's tail lags
         * the producer by one or more entries and requests stall until a later
         * notification lets us catch up. */
        uint32_t old_tail = ctx->fc_rx_consumer_tail;

        /* Gap recovery FIRST, then process the current entry — both copy data
         * out of rx_dma_buffer into rx_buffer slots. We must complete these
         * copies BEFORE advancing fc_rx_consumer_tail, otherwise DPU can
         * treat pos..pos+padded as free and overwrite it with new reverse-DMA
         * data while we are still reading. Seen as Thrift "Frame size has
         * negative value" / "Received an oversized frame" at wrap boundaries. */
        if (old_tail != pos) {
            DOCA_LOG_WARN("DMA_COMPLETION: gap detected (old_tail=%u, pos=%u) — scanning for lost entries",
                          old_tail, pos);
            scan_and_recover_rx_gap(ctx, old_tail, pos);
        }

        int proc_rc = process_rx_dma_entry(ctx, pos, dma_len);
        if (proc_rc != 0) {
            DOCA_LOG_WARN("DMA_COMPLETION: process_rx_dma_entry failed at pos=%u len=%u — advancing tail to avoid DPU wedge",
                          pos, dma_len);
        }

        /* Now safe to release this region back to the DPU. We ALWAYS advance
         * the tail here (even if process returned -1) — the DPU does not
         * distinguish "delivered" vs "dropped at app layer"; it only needs
         * to know the region is reusable, and failing to advance would
         * permanently wedge DPU→Host flow. The copy-out happened inside
         * process_rx_dma_entry before this point, so advancing is safe. */
        uint32_t padded = (dma_len + 127) & ~(uint32_t)127;
        ctx->fc_rx_consumer_tail = pos + padded;
        if (ctx->fc_rx_consumer_tail >= (uint32_t)ctx->rx_dma_buf_size)
            ctx->fc_rx_consumer_tail = 0;

        DOCA_LOG_INFO("DMA_COMPLETION: pos=%u len=%u fc_tail=%u new_rx_tail=%u",
                      pos, dma_len, hdr->consumer_tail, ctx->fc_rx_consumer_tail);
        return;
    }

    /* === Legacy comch data path (DMESH_MSG_RX_DATA) === */
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
    pthread_mutex_init(&ctx->ring_lock, NULL);

    /* rx_buffer is the STAGING area for delivered messages — must be separate
     * from rx_dma_buffer (the DMA landing zone). If they share memory, the
     * DPU can overwrite a slot's contents after the consumer_tail advance
     * releases that region for reuse, corrupting in-flight messages for
     * the upper layer (seen as "Frame size has negative value" near wrap). */
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
    }

    /* Initialize ring_tx_slot_map for deferred TX slot free */
    for (int i = 0; i < DMA_RING_SIZE; i++)
        ctx->ring_tx_slot_map[i] = -1;

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

    /* Deferred TX slot free: free any remaining mapped TX slots */
    for (int i = 0; i < DMA_RING_SIZE; i++) {
        if (ctx->ring_tx_slot_map[i] >= 0) {
            dpumesh_tx_free(ctx, ctx->ring_tx_slot_map[i]);
            ctx->ring_tx_slot_map[i] = -1;
        }
    }

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

    cleanup_objects(&ctx->doca_objs);

    pthread_mutex_destroy(&ctx->ring_lock);
    pthread_mutex_destroy(&ctx->slot_lock);
    if (ctx->slot_bitmap) free(ctx->slot_bitmap);

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

/*
 * Reclaim TX slots from completed ring descriptors (valid=0).
 * OP_RESPONSE TX slots are deferred in ring_tx_slot_map until ring reuse.
 * This function proactively frees them to prevent deadlock when tx_alloc
 * can't find free slots and dpumesh_enqueue can't be called to recycle them.
 * Caller must NOT hold slot_lock (this function acquires ring_lock then slot_lock).
 */
static int reclaim_ring_tx_slots(dpumesh_ctx_t *ctx) {
    int freed = 0;
    pthread_mutex_lock(&ctx->ring_lock);
    for (int i = 0; i < DMA_RING_SIZE; i++) {
        if (ctx->ring_tx_slot_map[i] >= 0) {
            struct dma_desc *d = &ctx->dma_ring->descs[i];
            __sync_synchronize();
            if (!d->valid) {
                dpumesh_tx_free(ctx, ctx->ring_tx_slot_map[i]);
                ctx->ring_tx_slot_map[i] = -1;
                freed++;
            }
        }
    }
    pthread_mutex_unlock(&ctx->ring_lock);
    return freed;
}

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

        /* Reclaim TX slots from completed ring descriptors to break
         * the deadlock: tx_alloc waits for free slots, but slots are
         * only freed inside dpumesh_enqueue which can't run until
         * tx_alloc returns. */
        if (reclaim_ring_tx_slots(ctx) > 0)
            continue;  /* freed some — retry immediately */

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
    /* Reserve fc_header space at the beginning of each slot.
     * Caller writes body starting after the header area. */
    return (uint8_t *)ctx->dma_buffer + ((size_t)slot * ctx->slot_size)
           + sizeof(struct fc_header);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    if (slot < 0 || slot >= ctx->num_slots) return;
    /* Flow control ensures no stale read — no memset needed */
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

    if (desc->body_len > (uint32_t)(ctx->slot_size - (int)sizeof(struct fc_header))) {
        DOCA_LOG_ERR("ENQUEUE rejected: body_len=%u exceeds usable slot_size=%d (slot=%d - fc_header=%zu)",
                     desc->body_len, (int)(ctx->slot_size - sizeof(struct fc_header)),
                     ctx->slot_size, sizeof(struct fc_header));
        return -1;
    }

    /* Lock ring access: serializes get_next_dma_desc + descriptor fill + valid=1 */
    pthread_mutex_lock(&ctx->ring_lock);

    /* Flow control: ensure DPU RX buffer has space for this transfer.
     * fc_tx_producer_head mirrors the DPA's pos[ring_idx] advancement.
     * fc_tx_last_consumer_tail is piggybacked from DPU via reverse DMA fc_header.
     * Without this check, the DPA overwrites unconsumed data in the DPU buffer
     * under high load, causing data corruption or silent drops. */
    {
        uint32_t dma_size = (uint32_t)(sizeof(struct fc_header) + desc->body_len);
        uint32_t padded_len = (dma_size + 127) & ~(uint32_t)127;
        uint32_t buf_size = DPU_BUFFER_SIZE;
        int fc_retry = 0;
        const int max_fc_retry = 10000;
        struct timespec fc_backoff = {0, 10000}; /* 10µs initial */

        while (1) {
            uint32_t head = ctx->fc_tx_producer_head;
            uint32_t tail = ctx->fc_tx_last_consumer_tail;
            uint32_t used = (head >= tail) ? (head - tail) : (buf_size - tail + head);
            uint32_t available = buf_size - used;

            if (padded_len <= available) {
                /* Check contiguous space: wrap-around wastes gap at end */
                if (head + padded_len <= buf_size)
                    break;  /* fits without wrap */
                if (padded_len <= tail)
                    break;  /* fits after wrap to 0 */
            }

            if (++fc_retry >= max_fc_retry) {
                pthread_mutex_unlock(&ctx->ring_lock);
                DOCA_LOG_ERR("ENQUEUE: DPU buffer full after %d retries "
                             "(head=%u tail=%u need=%u avail=%u)",
                             max_fc_retry, head, tail, padded_len, available);
                return -1;
            }
            /* Release lock while waiting — PE thread updates fc_tx_last_consumer_tail */
            pthread_mutex_unlock(&ctx->ring_lock);
            nanosleep(&fc_backoff, NULL);
            if (fc_backoff.tv_nsec < 1000000) fc_backoff.tv_nsec *= 2;
            pthread_mutex_lock(&ctx->ring_lock);
        }
    }

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

    /* Deferred TX slot free: if this ring slot previously held data for
     * an OP_RESPONSE, the DPA has now cleared valid=0 (get_next_dma_desc
     * checked), so the old TX slot is safe to free. */
    if (ctx->ring_tx_slot_map[ring_slot] >= 0) {
        dpumesh_tx_free(ctx, ctx->ring_tx_slot_map[ring_slot]);
        ctx->ring_tx_slot_map[ring_slot] = -1;
    }

    /* Track TX slot for OP_RESPONSE — freed when ring slot is reused.
     * For OP_REQUEST, pending table handles TX slot lifetime. */
    if (desc->flags & OP_RESPONSE) {
        ctx->ring_tx_slot_map[ring_slot] = desc->body_buf_slot;
    }

    /* Write fc_header at slot base (before body data).
     * Body was written by caller at base + sizeof(fc_header). */
    {
        uint8_t *slot_base = (uint8_t *)ctx->dma_buffer +
                             ((size_t)desc->body_buf_slot * ctx->slot_size);
        struct fc_header *hdr = (struct fc_header *)slot_base;
        hdr->consumer_tail = ctx->fc_rx_consumer_tail;
        hdr->payload_len = desc->body_len;
    }

    dma->mmap = ctx->dpa_mmap_handle;
    dma->addr = (uint64_t)ctx->dma_buffer +
                ((size_t)desc->body_buf_slot * ctx->slot_size);
    dma->size = sizeof(struct fc_header) + desc->body_len;
    dma->idx  = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    /* Advance DPU buffer producer head — mirrors DPA's pos advancement.
     * DPA: if (pos + padded > buf_size) pos = 0; pos += padded;
     *       if (pos >= buf_size) pos = 0; */
    {
        uint32_t dma_size = (uint32_t)(sizeof(struct fc_header) + desc->body_len);
        uint32_t padded_len = (dma_size + 127) & ~(uint32_t)127;
        if (ctx->fc_tx_producer_head + padded_len > DPU_BUFFER_SIZE)
            ctx->fc_tx_producer_head = 0;
        ctx->fc_tx_producer_head += padded_len;
        if (ctx->fc_tx_producer_head >= DPU_BUFFER_SIZE)
            ctx->fc_tx_producer_head = 0;
    }

    DOCA_LOG_DBG("ENQUEUE: req_id=%u slot=%u len=%u fc_head=%u fc_tail=%u",
                 desc->req_id, ring_slot, desc->body_len,
                 ctx->fc_tx_producer_head, ctx->fc_tx_last_consumer_tail);

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
                /* Timeout: do NOT force-free the TX slot. The DPA may still
                 * be reading from it; freeing now risks corrupting whatever
                 * request recycles that slot next. Defer the free to the
                 * TX_ACK handler (state=-2 → handler frees + sets -1).
                 * If a late response arrives first, the RX path handles it
                 * under state=-2 as well. */
                p->state = -2;
                pthread_cond_broadcast(&p->cond);
                pthread_mutex_unlock(&p->lock);
                return -1;
            }
        }
    }

    if (p->state == 1) {
        *resp = p->desc;
        /* Response arrived = DPA finished reading TX buffer. Free TX now
         * to return the slot to the pool ASAP under high load. */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
        pthread_mutex_unlock(&p->lock);
        return 0;
    }

    /* Unreachable in practice — state left the wait loop without being 1
     * only via the timeout/-2 branch above. Clear the slot defensively. */
    if (p->state != -2) {
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
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
        /* Response arrived but was never consumed — free RX + TX */
        if (p->desc.body_buf_slot >= 0) {
            pthread_mutex_lock(&ctx->rx_slot_lock);
            ctx->rx_slot_bitmap[p->desc.body_buf_slot] = 0;
            pthread_mutex_unlock(&ctx->rx_slot_lock);
        }
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == 0) {
        /* In-flight, no timeout yet — caller gave up; free TX now.
         * (No DPA-in-progress risk because 0 means DPA hasn't had a chance
         * to signal completion, i.e. request was cancelled pre-enqueue
         * path or immediately after enqueue failure.) */
        if (p->tx_slot >= 0) {
            dpumesh_tx_free(ctx, p->tx_slot);
            p->tx_slot = -1;
        }
        p->state = -1;
        pthread_cond_broadcast(&p->cond);
    } else if (p->state == -2) {
        /* Timeout path: wait_response already deferred the TX free.
         * Do NOT force-free here — DPA may still be reading from the slot.
         * The TX_ACK handler (or a late response) will release it and
         * transition state -2 → -1. Leaving state=-2 preserves that. */
    } else {
        /* state == -1, already clean — nothing to do. */
    }
    pthread_mutex_unlock(&p->lock);
}
