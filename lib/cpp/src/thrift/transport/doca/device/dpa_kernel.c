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

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
 */

__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
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
                        DOCA_DPA_DEV_LOG_INFO("DMA_REQ: producer slot timeout (chunks=%d)\n", num_chunks);
                        aborted = 1;
                        break;
                    }

                    /* Ensure DPU consumer has recv tasks */
                    {
                        uint32_t wait = 0;
                        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                            wait++;
                            if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                                DOCA_DPA_DEV_LOG_INFO("DMA_REQ: consumer timeout (chunks=%d)\n", num_chunks);
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
                DOCA_DPA_DEV_LOG_INFO("ADD_RING received: pod_id=%d, buf_arr_size=%u\n",
                                     add_msg->ring.pod_id, add_msg->ring.buf_arr_size);
                thread_arg->num_rings++;
                DOCA_DPA_DEV_LOG_INFO("Added ring: pod_id=%d, num_rings=%u\n",
                                     add_msg->ring.pod_id, thread_arg->num_rings);
            } else {
                DOCA_DPA_DEV_LOG_INFO("Ring add failed: too many rings=%u\n", thread_arg->num_rings);
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
                    DOCA_DPA_DEV_LOG_INFO("ADD_REV_RING updated: pod_id=%d host_mmap=0x%lx\n",
                                         add_msg->ring.pod_id, add_msg->ring.host_mmap);
                    found = 1;
                    break;
                }
            }
            if (!found) {
                if (thread_arg->num_rev_rings < MAX_DPA_RINGS) {
                    thread_arg->rev_rings[thread_arg->num_rev_rings] = add_msg->ring;
                    DOCA_DPA_DEV_LOG_INFO("ADD_REV_RING received: pod_id=%d, buf_arr_size=%u\n",
                                         add_msg->ring.pod_id, add_msg->ring.buf_arr_size);
                    thread_arg->num_rev_rings++;
                } else {
                    DOCA_DPA_DEV_LOG_INFO("Rev ring add failed: too many=%u\n", thread_arg->num_rev_rings);
                }
            }
            break;
        }
        case COMCH_MSG_TYPE_TRIGGER:
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
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
            DOCA_DPA_DEV_LOG_INFO("ensure_producer_slot: timeout (inflight=%u)\n",
                                 thread_arg->producer_slots_inflight);
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
        DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Local Protection Error]: invalid dst mmap handle (ring=%u slot=%u req_id=%u dpu_mmap=0)\n",
                             r, thread_arg->desc_idx[r], (uint32_t)desc->idx);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    if (ring->host_mmap == 0) {
        DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Remote Access Error]: invalid src mmap handle (ring=%u slot=%u req_id=%u host_mmap=0)\n",
                             r, thread_arg->desc_idx[r], (uint32_t)desc->idx);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    if (desc->size == 0) {
        DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Local Length Error]: zero-length descriptor (ring=%u slot=%u req_id=%u)\n",
                             r, thread_arg->desc_idx[r], (uint32_t)desc->idx);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    {
        uint64_t src_addr = desc->addr;
        uint64_t src_len = (uint64_t)desc->size;
        uint64_t host_base = ring->host_addr;
        uint64_t host_size = ring->host_buf_size;
        uint64_t host_end = host_base + host_size;
        uint64_t src_end = src_addr + src_len;

        if (host_size == 0 || host_end < host_base || src_end < src_addr ||
            src_addr < host_base || src_end > host_end) {
            DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Remote Access Error]: src out of host mmap range\n");
            DOCA_DPA_DEV_LOG_INFO("Descriptor source out of host range: ring=%u slot=%u req_id=%u src=[0x%lx..0x%lx) host=[0x%lx..0x%lx) len=%u\n",
                                 r, thread_arg->desc_idx[r], (uint32_t)desc->idx,
                                 src_addr, src_end, host_base, host_end, desc->size);
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
                DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Receiver Not Ready]: timeout waiting consumer credits (ring=%u ring_pod=%d slot=%u req_id=%u consumer_id=%u loops=%u). Dropping descriptor.\n",
                                     r,
                                     ring->pod_id,
                                     thread_arg->desc_idx[r],
                                     (uint32_t)desc->idx,
                                     dpu_consumer_id,
                                     empty_wait_loops);
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

    /* Check if padded descriptor fits in DPU buffer at all */
    if (padded_total > ring->dpu_buf_size) {
        DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Local Length Error]: requested length exceeds DPU destination buffer\n");
        DOCA_DPA_DEV_LOG_INFO("Descriptor too large for DPU buffer: ring=%u slot=%u req_id=%u size=%u padded=%u dpu_buf_size=%u\n",
                             r, thread_arg->desc_idx[r], (uint32_t)desc->idx,
                             desc->size, padded_total, ring->dpu_buf_size);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Wrap around if padded DMA would exceed DPU buffer boundary */
    if (thread_arg->pos[r] + padded_total > ring->dpu_buf_size)
        thread_arg->pos[r] = 0;

    /* Build completion message with routing info.
     * Use comch_dma_comp_msg directly (25 bytes) instead of comch_msg union (~52 bytes)
     * to stay within the 32-byte immediate data limit of doca_dpa_dev_comch_producer_dma_copy(). */
    comp.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    comp.pos = thread_arg->pos[r];
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
                DOCA_DPA_DEV_LOG_INFO("FWD: producer slot timeout (ring=%u chunks=%d req_id=%u)\n",
                                     r, num_chunks, (uint32_t)desc->idx);
                aborted = 1;
                break;
            }

            /* Ensure DPU consumer has recv tasks */
            {
                uint32_t wait = 0;
                while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                    wait++;
                    if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                        DOCA_DPA_DEV_LOG_INFO("FWD: consumer timeout (ring=%u chunks=%d req_id=%u)\n",
                                             r, num_chunks, (uint32_t)desc->idx);
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
                                            ring->dpu_mmap,
                                            ring->dpu_addr + thread_arg->pos[r] + offset,
                                            ring->host_mmap,
                                            desc->addr + offset,
                                            chunk,
                                            (uint8_t *)&comp,
                                            sizeof(struct comch_dma_comp_msg),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            } else {
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            ring->dpu_mmap,
                                            ring->dpu_addr + thread_arg->pos[r] + offset,
                                            ring->host_mmap,
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
     * aligned for subsequent dma_copy calls. */
    if (!aborted) {
        thread_arg->pos[r] += padded_total;
        if (thread_arg->pos[r] >= ring->dpu_buf_size)
            thread_arg->pos[r] = 0;
    }

    __dpa_thread_window_writeback();
    desc->mmap = 0;
    desc->addr = 0;
    desc->size = 0;
    desc->idx = 0;
    desc->dst_pod_id = 0;
    desc->flags = 0;
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

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->rev_desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    if (desc->size == 0 || ring->host_mmap == 0 || ring->dpu_mmap == 0) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
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

    if (padded_total > ring->host_buf_size) {
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Wrap around if padded DMA would exceed Host RX buffer boundary */
    if (thread_arg->rev_pos[r] + padded_total > ring->host_buf_size)
        thread_arg->rev_pos[r] = 0;

    /* Build completion message for DPU ARM (which forwards to Host).
     * Reverse direction: src=DPU TX buffer, dst=Host RX buffer.
     *
     * src_pod_id is the ORIGINAL forward sender (DPU set it on the desc
     * during dpu_enqueue_reverse_dma). The reverse ring's ring->pod_id
     * is the RECEIVER pod, not the source — using it here would lose
     * the original sender identity, which the receiving host needs for
     * routing. */
    comp.type = COMCH_MSG_TYPE_REV_DMA_COMPLETED;
    comp.pos = thread_arg->rev_pos[r];
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
                DOCA_DPA_DEV_LOG_INFO("REV: producer slot timeout (ring=%u chunks=%d req_id=%u)\n",
                                     r, num_chunks, (uint32_t)desc->idx);
                aborted = 1;
                break;
            }

            /* Ensure DPU consumer has recv tasks */
            {
                uint32_t wait = 0;
                while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                    wait++;
                    if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                        DOCA_DPA_DEV_LOG_INFO("REV: consumer timeout (ring=%u chunks=%d req_id=%u)\n",
                                             r, num_chunks, (uint32_t)desc->idx);
                        aborted = 1;
                        break;
                    }
                }
                if (aborted) break;
            }

            if (remaining <= chunk) {
                /* Final chunk: src=DPU TX, dst=Host RX */
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            ring->host_mmap,  /* dst = Host RX */
                                            ring->host_addr + thread_arg->rev_pos[r] + offset,
                                            ring->dpu_mmap,   /* src = DPU TX */
                                            ring->dpu_addr + desc->addr + offset,
                                            chunk,
                                            (uint8_t *)&comp,
                                            sizeof(struct comch_dma_comp_msg),
                                            DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            } else {
                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            dpu_consumer_id,
                                            ring->host_mmap,
                                            ring->host_addr + thread_arg->rev_pos[r] + offset,
                                            ring->dpu_mmap,
                                            ring->dpu_addr + desc->addr + offset,
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
        thread_arg->rev_pos[r] += padded_total;
        if (thread_arg->rev_pos[r] >= ring->host_buf_size)
            thread_arg->rev_pos[r] = 0;
    }

    __dpa_thread_window_writeback();
    desc->mmap = 0;
    desc->addr = 0;
    desc->size = 0;
    desc->idx = 0;
    desc->dst_pod_id = 0;
    desc->flags = 0;
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

        /* Reverse rings (DPU→CPU) */
        uint32_t nr_rev = thread_arg->num_rev_rings;
        for (uint32_t r = 0; r < nr_rev; r++) {
            int chunks = process_one_rev_desc(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }
    } while (found > 0);

    return total_dma_calls;
}

__dpa_global__ void hello_world(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("hello_world: num_rings=%u\n", thread_arg->num_rings);

    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("[PAIRCHK] run_dma_manager arg: consumer_comp=0x%lx producer_comp=0x%lx consumer=0x%lx producer=0x%lx consumer_id=%u num_rings=%u\n",
                         thread_arg->dpa_consumer_comp,
                         thread_arg->dpa_producer_comp,
                         thread_arg->dpa_consumer,
                         thread_arg->dpa_producer,
                         thread_arg->dpu_consumer_id,
                         thread_arg->num_rings);

    DOCA_DPA_DEV_LOG_INFO("entering hybrid spin/yield loop (consumer_id=%u)\n",
                         thread_arg->dpu_consumer_id);

    /* Hybrid: spin while there is work (cache-hot, low per-desc overhead),
     * yield to RTOS only when both rings are empty AND no producer slots
     * are pending. This:
     *   - resets the 12 s max kernel runtime timer whenever the workload
     *     pauses, avoiding the silent fatal termination observed under
     *     pure-spin earlier
     *   - sustains the ~40 k+ DMA/s rate during bursts because there is
     *     no wake/reschedule overhead inside the work batch
     *   - relies on the DPU forwarding a TRIGGER (host WAKE_DPA on enqueue,
     *     DPU rev DMA enqueue) to fire consumer_comp and re-activate the
     *     thread when a new desc arrives during idle */
    while (1) {
        handle_msgs(thread_arg);
        int chunks = drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);
        if (chunks == 0)
            doca_dpa_dev_thread_reschedule();
    }
}