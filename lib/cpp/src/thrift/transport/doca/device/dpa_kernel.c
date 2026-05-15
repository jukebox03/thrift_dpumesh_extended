#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

#define DMA_DIAG_EMPTY_WAIT_WARN_LOOPS  0x100000
#define DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS  0x800000

/* Spin iterations waiting for DPU consumer to resubmit recv tasks */
#define DPA_CONSUMER_WAIT_LOOPS  0x200000

/* Max DMA size for doca_dpa_dev_comch_producer_dma_copy.
 * HW supports up to 8KB per single call with 128B-aligned addresses.
 * Verified via DPUMesh_doca byte-level verification test. */
#define DPA_DMA_COPY_MAX  8192

/* Alignment requirements for doca_dpa_dev_comch_producer_dma_copy:
 * - Source and destination addresses: 64B aligned
 *   (ensured by CACHE_ALIGN=128 buffer allocation + 128B-aligned offsets)
 * - Transfer size per call: 128B aligned, max 8KB */
#define DMA_COPY_SIZE_ALIGN  128
#define ALIGN_UP_128(x)  (((x) + (DMA_COPY_SIZE_ALIGN - 1)) & ~(uint32_t)(DMA_COPY_SIZE_ALIGN - 1))

/* Producer send slot capacity — must match PRODUCER_SLOT_CAPACITY in dpa.h.
 * Duplicated here because DPA kernel cannot include DPU-side headers. */
#define PRODUCER_SLOT_CAPACITY  1024

/* Forward declarations */
static void drain_producer_completions(struct dpa_thread_arg *thread_arg);
static int ensure_producer_slot(struct dpa_thread_arg *thread_arg);

/* DPA-local cache of host's freed_cumulative, refreshed lazily.
 * process_one_rev_desc reads from this cache (no PCIe access in reverse
 * path) to avoid the window/cache race seen with reverse-path reads. */
uint64_t dpa_cached_freed[MAX_DPA_RINGS] = {0};

/* Per-ring count of reverse DMAs DPA has issued (admission accounting).
 * Was a static local in process_one_rev_desc; promoted to file scope so
 * drain_all_rings can use it for lazy-refresh decisions. */
uint64_t dpa_sent_count[MAX_DPA_RINGS] = {0};

/* Phase 4: per-peer counters for CASE_DIRECT admission gate. Mirror the
 * reverse-path pattern but keyed by dst_pod_id instead of ring index. */
uint64_t dpa_direct_sent[MAX_DPA_PEERS] = {0};
uint64_t dpa_direct_cached_freed[MAX_DPA_PEERS] = {0};

/* Lazy-refresh margin: refresh credit when computed inflight is within
 * this many slots of the cap. Smaller = fewer PCIe reads but tighter
 * margin; larger = more reads, more headroom. 64 leaves enough buffer
 * to refresh before false-positive defer at the cap. */
#define CREDIT_REFRESH_MARGIN 64

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
 */

__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg)
{
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

	return 0;
}

static void handle_dpu_msg(struct dpa_thread_arg *thread_arg, const struct comch_msg *msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;

    switch(msg->type) {
        case COMCH_MSG_TYPE_DMA_REQ: {
            struct comch_dma_req_msg *dma_msg = (struct comch_dma_req_msg *)msg;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;

            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id)) {
                break;
            }

            /* Chunked DMA via dma_copy (max 8KB per call, 128B-aligned size).
             * Each dma_copy consumes one producer slot + one consumer recv task.
             * Intermediate chunks: DMA_CHUNK type (consumer ignores).
             * Final chunk: sends actual immediate data.
             * On consumer timeout, abort to avoid sending corrupt partial data. */
            {
                uint32_t total = dma_msg->length;
                uint32_t offset = 0;
                int num_chunks = 0;
                int aborted = 0;
                enum comch_msg_type chunk_type = COMCH_MSG_TYPE_DMA_CHUNK;

                while (offset < total) {
                    uint32_t remaining = total - offset;
                    uint32_t chunk = remaining;
                    if (chunk > DPA_DMA_COPY_MAX)
                        chunk = DPA_DMA_COPY_MAX;
                    chunk = ALIGN_UP_128(chunk);

                    /* Ensure producer slot available */
                    if (ensure_producer_slot(thread_arg) != 0) {
                        aborted = 1;
                        break;
                    }

                    /* Ensure DPU consumer has recv tasks */
                    {
                        uint32_t wait = 0;
                        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                            wait++;
                            if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                                aborted = 1;
                                break;
                            }
                        }
                        if (aborted)
                            break;
                    }

                    if (remaining <= chunk) {
                        doca_dpa_dev_comch_producer_dma_copy(producer,
                                                dpu_consumer_id,
                                                dma_msg->dst_mmap,
                                                dma_msg->dst_addr + offset,
                                                dma_msg->src_mmap,
                                                dma_msg->src_addr + offset,
                                                chunk,
                                                (const uint8_t *)"test_dma_imm",
                                                sizeof("test_dma_imm"),
                                                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                    } else {
                        doca_dpa_dev_comch_producer_dma_copy(producer,
                                                dpu_consumer_id,
                                                dma_msg->dst_mmap,
                                                dma_msg->dst_addr + offset,
                                                dma_msg->src_mmap,
                                                dma_msg->src_addr + offset,
                                                chunk,
                                                (const uint8_t *)&chunk_type,
                                                sizeof(chunk_type),
                                                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                    }
                    thread_arg->producer_slots_inflight++;
                    offset += chunk;
                    num_chunks++;
                }
            }
            break;
        }
        case COMCH_MSG_TYPE_ADD_RING: {
            struct comch_add_ring_msg *add_msg = (struct comch_add_ring_msg *)msg;
            if (thread_arg->num_rings < MAX_DPA_RINGS) {
                thread_arg->rings[thread_arg->num_rings] = add_msg->ring;
                thread_arg->num_rings++;
            } else {
            }
            break;
        }
        case COMCH_MSG_TYPE_ADD_REV_RING: {
            struct comch_add_rev_ring_msg *add_msg = (struct comch_add_rev_ring_msg *)msg;
            /* Check if ring for this pod_id already exists (update case) */
            int found = 0;
            for (uint32_t ri = 0; ri < thread_arg->num_rev_rings; ri++) {
                if (thread_arg->rev_rings[ri].pod_id == add_msg->ring.pod_id) {
                    thread_arg->rev_rings[ri] = add_msg->ring;
                    found = 1;
                    break;
                }
            }
            if (!found) {
                if (thread_arg->num_rev_rings < MAX_DPA_RINGS) {
                    thread_arg->rev_rings[thread_arg->num_rev_rings] = add_msg->ring;
                    thread_arg->num_rev_rings++;
                } else {
                }
            }
            break;
        }
        case COMCH_MSG_TYPE_ADD_PEER: {
            struct comch_add_peer_msg *add_msg = (struct comch_add_peer_msg *)msg;
            int32_t pid = add_msg->peer.pod_id;
            if (pid >= 0 && pid < MAX_DPA_PEERS) {
                thread_arg->peers[pid] = add_msg->peer;
                thread_arg->peer_write_cursor[pid] = 0;
            }
            break;
        }
        case COMCH_MSG_TYPE_TRIGGER:
            break;
        default:
            break;
    }
}

static void handle_msgs(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    struct comch_msg *msg;
    uint32_t msg_size;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
    doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    uint32_t num_msgs = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        if (msg == NULL)
            continue;
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
        doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
}

/*
 * Drain all pending producer completions (send_imm acks).
 * Each dma_copy consumes one send slot.
 * This function acknowledges completed sends to free those slots
 * and decrements the inflight counter accordingly.
 * Without this, the producer runs out of send slots after PRODUCER_SLOT_CAPACITY
 * sends and subsequent calls silently fail.
 */
static void drain_producer_completions(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_t producer_comp = thread_arg->dpa_producer_comp;
    doca_dpa_dev_completion_element_t elem;
    uint32_t count = 0;

    while (doca_dpa_dev_get_completion(producer_comp, &elem) != 0) {
        count++;
    }

    if (count > 0) {
        doca_dpa_dev_completion_ack(producer_comp, count);
        if (thread_arg->producer_slots_inflight >= count)
            thread_arg->producer_slots_inflight -= count;
        else
            thread_arg->producer_slots_inflight = 0;
    }
}

/*
 * Ensure at least one producer send slot is available before calling dma_copy.
 * Drains completions and spins until inflight < PRODUCER_SLOT_CAPACITY.
 * Returns 0 on success, -1 on timeout.
 */
static int ensure_producer_slot(struct dpa_thread_arg *thread_arg)
{
    if (thread_arg->producer_slots_inflight < PRODUCER_SLOT_CAPACITY)
        return 0;

    drain_producer_completions(thread_arg);
    if (thread_arg->producer_slots_inflight < PRODUCER_SLOT_CAPACITY)
        return 0;

    uint32_t wait = 0;
    while (thread_arg->producer_slots_inflight >= PRODUCER_SLOT_CAPACITY) {
        drain_producer_completions(thread_arg);
        wait++;
        if (wait >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
            return -1;
        }
    }
    return 0;
}

/*
 * Process one descriptor from the given ring.
 * Returns 1 if a descriptor was processed, 0 if ring was idle.
 */
static int process_one_desc(struct dpa_thread_arg *thread_arg,
                            uint32_t r)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    struct dpa_ring_info *ring = &thread_arg->rings[r];
    struct comch_dma_comp_msg comp;
    doca_dpa_dev_buf_t buf;
    doca_dpa_dev_uintptr_t dev_ptr;
    struct dma_desc *desc;

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    if (ring->dpu_mmap == 0) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    if (ring->host_mmap == 0) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    if (desc->size == 0) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Phase 3 (v2 plan): src override based on desc->flags.
     *  - OP_HDR_BATCH: read from host's hdr_tx_buffer via ring->host_hdr_mmap.
     *    Both the mmap handle and the base VA come from ring info because
     *    DPA-side handles are device-locality-bound — host's resolved value
     *    is not valid on the DPU device. Range check uses host_hdr_addr.
     *  - desc->mmap != 0: legacy Phase 1 override path (kept for backward
     *    compat; no current caller writes a non-zero desc->mmap on forward).
     *  - default (body): ring->host_mmap, range-check host_addr. */
    doca_dpa_dev_mmap_t src_mmap;
    uint64_t src_range_base;
    uint64_t src_range_size;
    if (desc->flags & OP_HDR_BATCH) {
        src_mmap = ring->host_hdr_mmap ? ring->host_hdr_mmap : ring->host_mmap;
        src_range_base = ring->host_hdr_mmap ? ring->host_hdr_addr : ring->host_addr;
        src_range_size = ring->host_hdr_mmap ? (uint64_t)8388608ULL : ring->host_buf_size;
    } else if (desc->mmap) {
        src_mmap = desc->mmap;
        src_range_base = 0;
        src_range_size = 0;
    } else {
        src_mmap = ring->host_mmap;
        src_range_base = ring->host_addr;
        src_range_size = ring->host_buf_size;
    }

    if (src_range_size != 0) {
        uint64_t src_addr = desc->addr;
        uint64_t src_len = (uint64_t)desc->size;
        uint64_t host_end = src_range_base + src_range_size;
        uint64_t src_end = src_addr + src_len;

        if (host_end < src_range_base || src_end < src_addr ||
            src_addr < src_range_base || src_end > host_end) {
            desc->valid = 0;
            __dpa_thread_window_writeback();
            thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
            return 1;
        }
    }

    {
        uint32_t empty_wait_loops = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            empty_wait_loops++;
            if (empty_wait_loops >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
                /* Clear descriptor to unblock ring slot */
                desc->valid = 0;
                __dpa_thread_window_writeback();
                thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
                return 1;
            }
        }
        (void)empty_wait_loops;
    }

    /* Compute 128B-aligned total — dma_copy requires 128B-aligned transfer sizes,
     * so the actual space consumed in the DPU buffer is the rounded-up total. */
    uint32_t padded_total = ALIGN_UP_128(desc->size);

    /* Phase 4: CASE_DIRECT path — single-shot dma_copy from src host's body
     * mmap directly to dst host's rx_dma_buffer. Host doesn't import the
     * peer's mmap (DOCA driver rejects host→host peer access); instead
     * DPU pushes the peer's DPU-device DPA handle via ADD_PEER and DPA
     * looks it up here by dst_pod_id. DMA path bypasses both DPU staging
     * and reverse-DMA — single dma_copy, no DPU memory occupancy. */
    int is_direct = 0;
    doca_dpa_dev_mmap_t dst_mmap_used = 0;
    uint64_t dst_base = 0;
    uint32_t dst_pos_for_comp = 0;
    if ((desc->flags & CASE_DIRECT) &&
        desc->dst_pod_id >= 0 && desc->dst_pod_id < MAX_DPA_PEERS &&
        thread_arg->peers[desc->dst_pod_id].pod_id == desc->dst_pod_id &&
        thread_arg->peers[desc->dst_pod_id].rx_mmap != 0) {
        struct dpa_peer_info *pe = &thread_arg->peers[desc->dst_pod_id];

        /* Phase 4 admission gate (v1.0.0 pattern): inflight = sent - freed.
         * If at or above peer's rq_depth, defer — leave desc->valid set so
         * we retry next iteration after drain_all_rings refreshes cached
         * freed counter via PCIe read of peer's credit slot. */
        if (pe->credit_buf_arr != 0 && pe->rq_depth != 0) {
            uint64_t inflight = dpa_direct_sent[desc->dst_pod_id] -
                                dpa_direct_cached_freed[desc->dst_pod_id];
            if (inflight >= (uint64_t)pe->rq_depth) {
                return 0;
            }
        }

        uint32_t aligned = ALIGN_UP_128(desc->size);
        uint64_t cursor = thread_arg->peer_write_cursor[desc->dst_pod_id];
        if (cursor + aligned > pe->rx_buf_size)
            cursor = 0;
        dst_mmap_used = pe->rx_mmap;
        dst_base = pe->rx_addr + cursor;
        dst_pos_for_comp = (uint32_t)cursor;
        thread_arg->peer_write_cursor[desc->dst_pod_id] = cursor + aligned;
        is_direct = 1;
    }

    if (is_direct) {
        /* nothing extra to do — dst_mmap_used / dst_base already set above. */
    } else {
        /* Check if padded descriptor fits in DPU buffer at all */
        if (padded_total > ring->dpu_buf_size) {
            desc->valid = 0;
            __dpa_thread_window_writeback();
            thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
            return 1;
        }
        if (thread_arg->pos[r] + padded_total > ring->dpu_buf_size)
            thread_arg->pos[r] = 0;
        dst_mmap_used = ring->dpu_mmap;
        dst_base = ring->dpu_addr + thread_arg->pos[r];
    }

    /* Build completion message with routing info.
     * Use comch_dma_comp_msg directly (25 bytes) instead of comch_msg union (~52 bytes)
     * to stay within the 32-byte immediate data limit of doca_dpa_dev_comch_producer_dma_copy(). */
    comp.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    comp.pos = is_direct ? dst_pos_for_comp : thread_arg->pos[r];
    comp.length = desc->size;
    comp.req_id = (uint32_t)desc->idx;
    comp.src_pod_id = ring->pod_id;
    comp.dst_pod_id = desc->dst_pod_id;
    comp.flags = desc->flags;

    /* Chunked DMA via dma_copy (max 8KB per call, 128B-aligned size).
     * Each dma_copy consumes one producer send slot AND one consumer recv task.
     * Intermediate chunks: DMA_CHUNK type (consumer ignores but resubmits task).
     * Final chunk: real comch_dma_comp_msg (consumer processes).
     * Between chunks: drain producer completions and verify consumer availability
     * to prevent silent slot exhaustion.
     * On consumer timeout: abort transfer — no final completion sent, host will
     * timeout. This prevents sending corrupt partial data to the DPU worker. */
    int num_chunks = 0;
    int aborted = 0;
    {
        uint32_t total = desc->size;
        uint32_t offset = 0;
        enum comch_msg_type chunk_type = COMCH_MSG_TYPE_DMA_CHUNK;

        while (offset < total) {
            uint32_t remaining = total - offset;
            uint32_t chunk = remaining;
            if (chunk > DPA_DMA_COPY_MAX)
                chunk = DPA_DMA_COPY_MAX;
            chunk = ALIGN_UP_128(chunk);

            /* Ensure producer slot available before dma_copy */
            if (ensure_producer_slot(thread_arg) != 0) {
                aborted = 1;
                break;
            }

            /* Ensure DPU consumer has recv tasks */
            {
                uint32_t wait = 0;
                while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                    wait++;
                    if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                        aborted = 1;
                        break;
                    }
                }
                if (aborted)
                    break;
            }

            if (remaining <= chunk) {
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            dst_mmap_used,
                                            dst_base + offset,
                                            src_mmap,
                                            desc->addr + offset,
                                            chunk,
                                            (uint8_t *)&comp,
                                            sizeof(struct comch_dma_comp_msg),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            } else {
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            dst_mmap_used,
                                            dst_base + offset,
                                            src_mmap,
                                            desc->addr + offset,
                                            chunk,
                                            (uint8_t *)&chunk_type,
                                            sizeof(chunk_type),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            }
            thread_arg->producer_slots_inflight++;
            offset += chunk;
            num_chunks++;
        }
    }

    /* Clear descriptor and advance ring index.
     * On abort: don't advance pos (partial DMA data is abandoned in DPU buffer).
     * No final completion sent, so host will timeout — better than corrupt data.
     * Advance pos by padded_total (128B-aligned) to keep DPU buffer positions
     * aligned for subsequent dma_copy calls. CASE_DIRECT bypasses DPU staging,
     * so no pos cursor to advance. */
    if (!aborted && !is_direct) {
        thread_arg->pos[r] += padded_total;
        if (thread_arg->pos[r] >= ring->dpu_buf_size)
            thread_arg->pos[r] = 0;
    }
    if (!aborted && is_direct) {
        /* Phase 4: account direct DMA toward peer's rq_depth. */
        dpa_direct_sent[desc->dst_pod_id]++;
    }

    __dpa_thread_window_writeback();
    desc->mmap = 0;
    desc->addr = 0;
    desc->size = 0;
    desc->idx = 0;
    desc->dst_pod_id = 0;
    desc->flags = 0;
    desc->dst_mmap = 0;
    desc->dst_pos = 0;
    desc->dst_addr = 0;
    __dpa_thread_window_writeback();
    desc->valid = 0;
    __dpa_thread_window_writeback();

    thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
    return num_chunks;  /* count all chunks sent (including before abort) for batch tracking */
}

/* drain_rev_producer_completions removed: reverse DMA completions now use
 * the same msgq producer as forward direction (DPA → DPU ARM). */

/*
 * Process one reverse descriptor (DPU→CPU) from ring r.
 * Source = DPU TX buffer (dpu_mmap/dpu_addr in ring info).
 * Destination = Host RX buffer (host_mmap/host_addr in ring info).
 * Completion sent to Host via rev_dpa_producer.
 * Returns number of dma_copy chunks issued, 0 if ring idle.
 */
static int process_one_rev_desc(struct dpa_thread_arg *thread_arg, uint32_t r)
{
    struct dpa_ring_info *ring = &thread_arg->rev_rings[r];
    /* Use same msgq producer as forward direction — DPU ARM receives all
     * completions and forwards to Host via comch control path. */
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    struct comch_dma_comp_msg comp;
    doca_dpa_dev_buf_t buf;
    doca_dpa_dev_uintptr_t dev_ptr;
    struct dma_desc *desc;

    /* sent_count promoted to file scope as dpa_sent_count so drain_all_rings
     * can use it for lazy credit refresh decisions. */

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->rev_desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    /* Under in-place forwarding desc->mmap is the SOURCE of reverse DMA
     * (set by DPU ARM in dpu_enqueue_reverse_dma to the original sender's
     * local_mmap DPA handle). Fall back to ring->dpu_mmap if a legacy
     * desc arrives with mmap=0 — keeps the path working during a partial
     * deploy. */
    doca_dpa_dev_mmap_t src_mmap = desc->mmap ? desc->mmap : ring->dpu_mmap;
    uint64_t src_base = desc->mmap ? desc->addr : (ring->dpu_addr + desc->addr);

    /* Phase 3: destination override based on desc->flags.
     *  - OP_HDR_BATCH: write into dst's hdr_rx_buffer via ring->host_hdr_rx_mmap
     *    (DPU-side resolved handle). desc->dst_addr already holds the absolute
     *    VA inside that buffer; desc->dst_pos echoes the offset to host's comp.
     *  - desc->dst_mmap != 0: legacy Phase 2 override.
     *  - default: ring->host_mmap (body RX). */
    doca_dpa_dev_mmap_t dst_mmap_used;
    int dst_overridden;
    if (desc->flags & OP_HDR_BATCH) {
        dst_mmap_used = ring->host_hdr_rx_mmap ? ring->host_hdr_rx_mmap : ring->host_mmap;
        dst_overridden = (ring->host_hdr_rx_mmap != 0);
    } else if (desc->dst_mmap) {
        dst_mmap_used = desc->dst_mmap;
        dst_overridden = 1;
    } else {
        dst_mmap_used = ring->host_mmap;
        dst_overridden = 0;
    }

    if (desc->size == 0 || dst_mmap_used == 0 || src_mmap == 0) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* === Admission gate using cached_freed[] (refreshed in drain_all_rings) ===
     * dpa_sent_count[r] - dpa_cached_freed[r] = inflight reverse DMAs.
     * If >= rq_depth, host's RX RQ is at capacity — defer (return 0; desc
     * stays valid, retry next iter when cache is refreshed). Phase 2:
     * skip for dst_overridden case — that path writes to a separate buffer
     * (hdr_rx_buffer) whose flow control is managed by Phase 3+ logic,
     * not the body credit ring. */
    if (!dst_overridden && ring->host_credit_buf_arr != 0 && ring->rq_depth != 0) {
        if (dpa_sent_count[r] - dpa_cached_freed[r] >= ring->rq_depth) {
            return 0;
        }
    }

    /* Wait for DPU consumer availability */
    {
        uint32_t wait = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            wait++;
            if (wait >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
                desc->valid = 0;
                __dpa_thread_window_writeback();
                thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
                return 1;
            }
        }
    }

    uint32_t padded_total = ALIGN_UP_128(desc->size);

    if (!dst_overridden && padded_total > ring->host_buf_size) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Wrap around if padded DMA would exceed Host RX buffer boundary.
     * Override path manages its own partition cursor (Phase 3+ chunk_flush
     * / hdr_outbound flush write absolute addresses), so skip. */
    if (!dst_overridden && thread_arg->rev_pos[r] + padded_total > ring->host_buf_size)
        thread_arg->rev_pos[r] = 0;

    /* Compute destination base for dma_copy. Override path uses caller-supplied
     * absolute address; default path uses ring->host_addr + cursor. */
    uint64_t dst_base = dst_overridden
        ? desc->dst_addr
        : (ring->host_addr + thread_arg->rev_pos[r]);

    /* Build completion message for DPU ARM (which forwards to Host).
     * Reverse direction: src=DPU TX buffer, dst=Host RX buffer.
     *
     * src_pod_id is the ORIGINAL forward sender (DPU set it on the desc
     * during dpu_enqueue_reverse_dma). The reverse ring's ring->pod_id
     * is the RECEIVER pod, not the source — using it here would lose
     * the original sender identity, which the receiving host needs for
     * routing. */
    comp.type = COMCH_MSG_TYPE_REV_DMA_COMPLETED;
    /* Phase 3: when dst is overridden (e.g. hdr forward → hdr_rx_buffer),
     * the host can't derive the offset from rev_pos[r] because rev_pos is
     * the body-RX cursor. DPU supplies the offset via desc->dst_pos. */
    comp.pos = dst_overridden ? desc->dst_pos : thread_arg->rev_pos[r];
    comp.length = desc->size;
    comp.req_id = (uint32_t)desc->idx;
    comp.src_pod_id = desc->src_pod_id;
    comp.dst_pod_id = desc->dst_pod_id;
    comp.flags = desc->flags;

    /* Chunked DMA: src=dpu buffer, dst=host buffer */
    int num_chunks = 0;
    int aborted = 0;
    {
        uint32_t total = desc->size;
        uint32_t offset = 0;
        enum comch_msg_type chunk_type = COMCH_MSG_TYPE_DMA_CHUNK;

        while (offset < total) {
            uint32_t remaining = total - offset;
            uint32_t chunk = remaining;
            if (chunk > DPA_DMA_COPY_MAX)
                chunk = DPA_DMA_COPY_MAX;
            chunk = ALIGN_UP_128(chunk);

            /* Ensure producer slot available before dma_copy */
            if (ensure_producer_slot(thread_arg) != 0) {
                aborted = 1;
                break;
            }

            /* Ensure DPU consumer has recv tasks */
            {
                uint32_t wait = 0;
                while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                    wait++;
                    if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                        aborted = 1;
                        break;
                    }
                }
                if (aborted) break;
            }

            if (remaining <= chunk) {
                /* Final chunk: src=src_pod's dma_buffer (in-place), dst=Host RX
                 * (or hdr_rx_buffer when dst_overridden). */
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            dst_mmap_used,    /* dst override aware */
                                            dst_base + offset,
                                            src_mmap,         /* src = src pod buf */
                                            src_base + offset,
                                            chunk,
                                            (uint8_t *)&comp,
                                            sizeof(struct comch_dma_comp_msg),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            } else {
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            dst_mmap_used,
                                            dst_base + offset,
                                            src_mmap,
                                            src_base + offset,
                                            chunk,
                                            (uint8_t *)&chunk_type,
                                            sizeof(chunk_type),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            }
            thread_arg->producer_slots_inflight++;
            offset += chunk;
            num_chunks++;
        }
    }

    if (!aborted) {
        if (!dst_overridden) {
            thread_arg->rev_pos[r] += padded_total;
            if (thread_arg->rev_pos[r] >= ring->host_buf_size)
                thread_arg->rev_pos[r] = 0;
            /* Successfully consumed one host RX slot (admission accounting). */
            dpa_sent_count[r]++;
        }
        /* Override path: caller (Phase 3+ hdr/chunk flush) tracks its own
         * partition cursor and is not subject to the body-RX admission
         * accounting; nothing to do here. */
    }

    __dpa_thread_window_writeback();
    desc->mmap = 0;
    desc->addr = 0;
    desc->size = 0;
    desc->idx = 0;
    desc->dst_pod_id = 0;
    desc->flags = 0;
    desc->dst_mmap = 0;
    desc->dst_pos = 0;
    desc->dst_addr = 0;
    __dpa_thread_window_writeback();
    desc->valid = 0;
    __dpa_thread_window_writeback();

    thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
    return num_chunks;
}

/*
 * Drain all valid descriptors across all rings (both directions).
 * Returns total number of dma_copy calls issued.
 * Each process_one_desc/process_one_rev_desc call ensures producer slot
 * and consumer availability before every dma_copy via ensure_producer_slot().
 */
static int drain_all_rings(struct dpa_thread_arg *thread_arg)
{
    int total_dma_calls = 0;
    int found;

    do {
        found = 0;
        handle_msgs(thread_arg);

        /* Forward rings (CPU→DPU) */
        uint32_t nr = thread_arg->num_rings;
        for (uint32_t r = 0; r < nr; r++) {
            int chunks = process_one_desc(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }

        /* Lazy credit refresh: only PCIe-read the credit slot when computed
         * inflight is approaching the cap. At low/medium load, inflight is
         * tiny relative to rq_depth so this never fires — zero overhead.
         * At cap, refreshes once every ~(rq_depth - MARGIN) sends. The
         * credit slot lives at index DMA_RING_SIZE within each pod's
         * forward buf_arr (host's dma_ring extended by 1 slot). */
        uint32_t nr_rev = thread_arg->num_rev_rings;
        for (uint32_t r = 0; r < nr_rev; r++) {
            struct dpa_ring_info *rev = &thread_arg->rev_rings[r];
            if (rev->host_credit_buf_arr == 0 || rev->rq_depth == 0)
                continue;
            uint64_t inflight = dpa_sent_count[r] - dpa_cached_freed[r];
            if (inflight + CREDIT_REFRESH_MARGIN < (uint64_t)rev->rq_depth)
                continue;  /* still plenty of headroom — skip PCIe read */
            /* Credit slot is at the END of forward dma_ring (one past
             * DMA_RING_SIZE descriptors). buf_arr_size is forward ring's
             * buf_arr size; index = forward ring count. The forward
             * ring_info_buf_arr_size used in process_one_desc is its own
             * (DMA_RING_SIZE), so credit at index = DMA_RING_SIZE. */
            doca_dpa_dev_buf_t cbuf =
                doca_dpa_dev_buf_array_get_buf(rev->host_credit_buf_arr,
                                               /* slot index */ DMA_RING_SIZE);
            doca_dpa_dev_uintptr_t cptr = doca_dpa_dev_buf_get_external_ptr(cbuf);
            volatile uint64_t *fp = (volatile uint64_t *)cptr;
            __dpa_thread_window_read_inv();
            dpa_cached_freed[r] = *fp;
        }

        /* Reverse rings (DPU→CPU) */
        for (uint32_t r = 0; r < nr_rev; r++) {
            int chunks = process_one_rev_desc(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }

        /* Phase 4: lazy refresh of peer credit slots (CASE_DIRECT admission
         * gate). Same shape as the reverse-ring refresh above — only PCIe-
         * read peer's credit counter when inflight is near the cap. */
        for (uint32_t pid = 0; pid < MAX_DPA_PEERS; pid++) {
            struct dpa_peer_info *pe = &thread_arg->peers[pid];
            if (pe->pod_id < 0 || pe->credit_buf_arr == 0 || pe->rq_depth == 0)
                continue;
            uint64_t inflight = dpa_direct_sent[pid] - dpa_direct_cached_freed[pid];
            if (inflight + CREDIT_REFRESH_MARGIN < (uint64_t)pe->rq_depth)
                continue;
            doca_dpa_dev_buf_t cbuf =
                doca_dpa_dev_buf_array_get_buf(pe->credit_buf_arr, DMA_RING_SIZE);
            doca_dpa_dev_uintptr_t cptr = doca_dpa_dev_buf_get_external_ptr(cbuf);
            volatile uint64_t *fp = (volatile uint64_t *)cptr;
            __dpa_thread_window_read_inv();
            dpa_direct_cached_freed[pid] = *fp;
        }
    } while (found > 0);

    return total_dma_calls;
}

__dpa_global__ void hello_world(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;


    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    /* DPA scheduling model verified empirically: this function is RE-ENTERED
     * on every activation (after yield + new event), and a single activation
     * can spin for 30M+ iterations across many seconds without being killed.
     * The "12 s max kernel runtime timer" concern from earlier comments did
     * not reproduce. Yield is kept as good cooperative-scheduling practice —
     * spinning when there is no work wastes EU cycles for no benefit. */
    while (1) {
        handle_msgs(thread_arg);
        int chunks = drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);
        if (chunks == 0)
            doca_dpa_dev_thread_reschedule();
    }
}