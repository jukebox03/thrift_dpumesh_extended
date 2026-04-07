#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

#define DMA_DIAG_EMPTY_WAIT_WARN_LOOPS  0x100000
#define DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS  0x800000

/* Max descriptors to process before pausing for DPU consumer to catch up.
 * Set well below CC_DPA_MAX_MSG_NUM (1024) — each desc sends one completion
 * to the DPU consumer, consuming one recv task. After this many, we spin-wait
 * for the consumer to drain and resubmit, preventing silent message loss. */
#define DPA_DRAIN_BATCH_LIMIT  512

/* Spin iterations waiting for DPU consumer to resubmit recv tasks */
#define DPA_CONSUMER_WAIT_LOOPS  0x200000

/* Max DMA size per post_memcpy chunk. post_memcpy has a HW size limit;
 * tune this value based on experimentation. */
#define DPA_MEMCPY_CHUNK_MAX  (128 * 1024)

/* Forward declarations */
static void drain_async_ops_completions(struct dpa_thread_arg *thread_arg);
static int wait_one_dma_completion(struct dpa_thread_arg *thread_arg);

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

            DOCA_DPA_DEV_LOG_INFO("Received DMA REQ msg from host: producer=0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u\n",
                                producer,
                                dma_msg->src_mmap,
                                 dma_msg->dst_mmap,
                                 dma_msg->src_addr,
                                 dma_msg->dst_addr,
                                 dma_msg->length);

            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id)) {
                DOCA_DPA_DEV_LOG_INFO("Host consumer is empty, cannot send DMA completion\n");
                break;
            }

            /* DMA via post_memcpy (chunked) + notify via post_send_imm_only */
            {
                uint32_t total = dma_msg->length;
                uint32_t offset = 0;
                int dma_ok = 1;

                while (offset < total) {
                    uint32_t chunk = total - offset;
                    if (chunk > DPA_MEMCPY_CHUNK_MAX)
                        chunk = DPA_MEMCPY_CHUNK_MAX;

                    doca_dpa_dev_post_memcpy(
                        thread_arg->dpa_async_ops,
                        dma_msg->dst_mmap,
                        dma_msg->dst_addr + offset,
                        dma_msg->src_mmap,
                        dma_msg->src_addr + offset,
                        chunk,
                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

                    if (!wait_one_dma_completion(thread_arg)) {
                        DOCA_DPA_DEV_LOG_INFO("DMA_REQ: DMA timeout at offset=%u\n", offset);
                        dma_ok = 0;
                        break;
                    }
                    offset += chunk;
                }

                if (dma_ok) {
                    doca_dpa_dev_comch_producer_post_send_imm_only(
                        producer,
                        dpu_consumer_id,
                        (const uint8_t *)"test_dma_imm",
                        sizeof("test_dma_imm"),
                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
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
        case COMCH_MSG_TYPE_NEW_DESC: {
            /* Doorbell: just a wake-up signal, poll_desc_rings() handles DMA */
            struct comch_new_desc_msg *nd = (struct comch_new_desc_msg *)&msg->new_desc_msg;
            DOCA_DPA_DEV_LOG_INFO("NEW_DESC doorbell: pod=%d req_id=%u size=%u dst=%d\n",
                                  nd->src_pod_id, nd->req_id, nd->size, nd->dst_pod_id);
            break;
        }
        case COMCH_MSG_TYPE_TRIGGER:
            /* No-op: just waking up the thread via completion event */
            DOCA_DPA_DEV_LOG_INFO("Trigger received\n");
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

    /* Arm FIRST, then drain — standard CQ notification pattern.
     * If a completion arrives between arm and drain, it will fire
     * the notification and wake the thread after reschedule.
     * If we drain-then-arm, a completion arriving in that gap is lost. */
    doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);

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
 * Each post_send_imm_only() consumes one send slot.
 * This function acknowledges completed sends to free those slots.
 * Without this, the producer runs out of send slots after CC_DPA_MAX_MSG_NUM
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
    }
}

/*
 * Drain all pending async_ops completions (post_memcpy DMA acks).
 * Each post_memcpy() consumes one async_ops slot.
 */
static void drain_async_ops_completions(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_t comp = thread_arg->dpa_async_ops_comp;
    doca_dpa_dev_completion_element_t elem;
    uint32_t count = 0;

    while (doca_dpa_dev_get_completion(comp, &elem) != 0) {
        count++;
    }

    if (count > 0) {
        doca_dpa_dev_completion_ack(comp, count);
    }
}

/*
 * Wait for exactly one DMA completion from async_ops.
 * Returns 1 on success, 0 on timeout.
 */
static int wait_one_dma_completion(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_t comp = thread_arg->dpa_async_ops_comp;
    doca_dpa_dev_completion_element_t elem;
    uint32_t loops = 0;

    while (doca_dpa_dev_get_completion(comp, &elem) == 0) {
        loops++;
        if (loops >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
            DOCA_DPA_DEV_LOG_INFO("DMA timeout (loops=%u)\n", loops);
            return 0;
        }
    }
    doca_dpa_dev_completion_ack(comp, 1);
    return 1;
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

    DOCA_DPA_DEV_LOG_INFO("FOUND valid desc: ring=%u slot=%u req_id=%u size=%u dst_pod=%d addr=0x%lx\n",
                          r, thread_arg->desc_idx[r], (uint32_t)desc->idx, desc->size,
                          desc->dst_pod_id, desc->addr);

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
            if ((empty_wait_loops % DMA_DIAG_EMPTY_WAIT_WARN_LOOPS) == 0) {
                DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Receiver Not Ready]: consumer has no posted recv tasks/credits yet (ring=%u ring_pod=%d slot=%u req_id=%u consumer_id=%u loops=%u producer=0x%lx)\n",
                                      r,
                                      ring->pod_id,
                                      thread_arg->desc_idx[r],
                                      (uint32_t)desc->idx,
                                      dpu_consumer_id,
                                      empty_wait_loops,
                                      producer);
            }

            if (empty_wait_loops >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
                DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Receiver Not Ready]: timeout waiting consumer credits (ring=%u ring_pod=%d slot=%u req_id=%u consumer_id=%u loops=%u). Will retry later.\n",
                                      r,
                                      ring->pod_id,
                                      thread_arg->desc_idx[r],
                                      (uint32_t)desc->idx,
                                      dpu_consumer_id,
                                      empty_wait_loops);
                return 0;
            }
        }
        if (empty_wait_loops > 0) {
            DOCA_DPA_DEV_LOG_INFO("Consumer credits available: ring=%u ring_pod=%d slot=%u req_id=%u waited_loops=%u\n",
                                  r,
                                  ring->pod_id,
                                  thread_arg->desc_idx[r],
                                  (uint32_t)desc->idx,
                                  empty_wait_loops);
        }
    }

    /* Check if descriptor fits in DPU buffer at all */
    if (desc->size > ring->dpu_buf_size) {
        DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Local Length Error]: requested length exceeds DPU destination buffer\n");
        DOCA_DPA_DEV_LOG_INFO("Descriptor too large for DPU buffer: ring=%u slot=%u req_id=%u size=%u dpu_buf_size=%u\n",
                              r, thread_arg->desc_idx[r], (uint32_t)desc->idx,
                              desc->size, ring->dpu_buf_size);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Wrap around if DMA would exceed DPU buffer boundary */
    if (thread_arg->pos[r] + desc->size > ring->dpu_buf_size)
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

    DOCA_DPA_DEV_LOG_INFO("DMA completion msg prepared: req_id=%u src_pod=%d dst_pod=%d pos=%u len=%u flags=0x%x\n",
                          comp.req_id,
                          comp.src_pod_id,
                          comp.dst_pod_id,
                          comp.pos,
                          comp.length,
                          (unsigned int)(uint8_t)comp.flags);

    DOCA_DPA_DEV_LOG_INFO("DMA copy args: producer=0x%lx producer_comp=0x%lx consumer_id=%u dst_mmap=%u dst_addr=0x%lx src_mmap=%u src_addr=0x%lx len=%u\n",
                          producer,
                          thread_arg->dpa_producer_comp,
                          dpu_consumer_id,
                          ring->dpu_mmap,
                          ring->dpu_addr + thread_arg->pos[r],
                          ring->host_mmap,
                          desc->addr,
                          desc->size);

    /* DMA: Host buffer → DPU local buffer via post_memcpy.
     * Split into DPA_MEMCPY_CHUNK_MAX chunks — post_memcpy has a HW size limit.
     * Each chunk: post_memcpy + wait for completion.
     * After all chunks land, notify DPU consumer with post_send_imm_only. */
    {
        uint32_t total = desc->size;
        uint32_t offset = 0;

        while (offset < total) {
            uint32_t chunk = total - offset;
            if (chunk > DPA_MEMCPY_CHUNK_MAX)
                chunk = DPA_MEMCPY_CHUNK_MAX;

            doca_dpa_dev_post_memcpy(
                thread_arg->dpa_async_ops,
                ring->dpu_mmap,
                ring->dpu_addr + thread_arg->pos[r] + offset,
                ring->host_mmap,
                desc->addr + offset,
                chunk,
                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

            if (!wait_one_dma_completion(thread_arg)) {
                DOCA_DPA_DEV_LOG_INFO("DMA timeout at offset=%u (ring=%u req_id=%u)\n",
                                      offset, r, (uint32_t)desc->idx);
                desc->valid = 0;
                __dpa_thread_window_writeback();
                thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
                return 1;
            }
            offset += chunk;
        }
    }

    /* All DMA chunks landed — notify DPU consumer (immediate data only, max 32B) */
    doca_dpa_dev_comch_producer_post_send_imm_only(
        producer,
        dpu_consumer_id,
        (const uint8_t *)&comp,
        sizeof(struct comch_dma_comp_msg),
        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    DOCA_DPA_DEV_LOG_INFO("DMA+notify submitted: ring=%u slot=%u req_id=%u size=%u chunks=%u\n",
                          r, thread_arg->desc_idx[r], (uint32_t)desc->idx, desc->size,
                          (desc->size + DPA_MEMCPY_CHUNK_MAX - 1) / DPA_MEMCPY_CHUNK_MAX);

    thread_arg->pos[r] += desc->size;

    if (thread_arg->pos[r] >= ring->dpu_buf_size) {
        DOCA_DPA_DEV_LOG_INFO("Ring buffer wrap-around: ring=%u pos=%u buf_size=%u\n",
                              r, thread_arg->pos[r], ring->dpu_buf_size);
        thread_arg->pos[r] = 0;
    }

    __dpa_thread_window_writeback();
    /* Clear descriptor fields first, then mark invalid last.
     * valid=0 signals the host that this slot is free to write.
     * If we clear valid first, the host may start writing new data
     * while we're still zeroing fields — causing corruption under load. */
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
    return 1;
}

/*
 * Drain all valid descriptors across all rings.
 * Returns total number of descriptors processed.
 */
static int drain_all_rings(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    int total = 0;
    int found;

    do {
        found = 0;
        handle_msgs(thread_arg);

        /* Free producer send slots and async_ops DMA slots */
        drain_producer_completions(thread_arg);
        drain_async_ops_completions(thread_arg);

        uint32_t nr = thread_arg->num_rings;
        for (uint32_t r = 0; r < nr; r++) {
            if (process_one_desc(thread_arg, r))
                found++;
        }
        total += found;

        /* After DPA_DRAIN_BATCH_LIMIT completions sent, the DPU consumer's
         * recv tasks may be nearly exhausted. Pause and wait for the DPU to
         * process callbacks and resubmit tasks before continuing. */
        if (total >= DPA_DRAIN_BATCH_LIMIT) {
            /* Drain producer + async_ops completions to free slots before pause */
            drain_producer_completions(thread_arg);
            drain_async_ops_completions(thread_arg);

            /* Wait until consumer runs empty (all our completions delivered) */
            uint32_t wait = 0;
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) != 1) {
                wait++;
                if (wait >= DPA_CONSUMER_WAIT_LOOPS)
                    break;
            }
            /* Now wait until consumer has tasks again (DPU resubmitted) */
            wait = 0;
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                wait++;
                if (wait >= DPA_CONSUMER_WAIT_LOOPS) {
                    DOCA_DPA_DEV_LOG_INFO("drain: consumer still empty after wait, total=%d\n", total);
                    break;
                }
            }
            DOCA_DPA_DEV_LOG_INFO("drain batch pause: %d descs processed, resuming\n", total);
            total = 0;  /* reset counter for next batch */
        }
    } while (found > 0);

    return total;
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

    DOCA_DPA_DEV_LOG_INFO("[PAIRCHK] run_dma_manager arg: consumer_comp=0x%lx producer_comp=0x%lx consumer=0x%lx producer=0x%lx consumer_id=%u num_rings=%u async_ops=0x%lx async_ops_comp=0x%lx\n",
                          thread_arg->dpa_consumer_comp,
                          thread_arg->dpa_producer_comp,
                          thread_arg->dpa_consumer,
                          thread_arg->dpa_producer,
                          thread_arg->dpu_consumer_id,
                          thread_arg->num_rings,
                          thread_arg->dpa_async_ops,
                          thread_arg->dpa_async_ops_comp);

    /* Arm completion notification once before the first drain cycle. */
    doca_dpa_dev_comch_consumer_completion_request_notification(thread_arg->dpa_consumer_comp);
    DOCA_DPA_DEV_LOG_INFO("completion notification armed (consumer_id=%u)\n",
                          thread_arg->dpu_consumer_id);

    /* Handle the trigger message from DPU consumer first */
    handle_msgs(thread_arg);

    /* Event loop: process all pending work, then yield until next doorbell */
    while (1) {
        drain_all_rings(thread_arg);

        /* Free any remaining producer send + async_ops DMA slots before going idle */
        drain_producer_completions(thread_arg);
        drain_async_ops_completions(thread_arg);

        /* Final check: handle_msgs arms notification then drains.
         * If a doorbell arrived between drain_all_rings exit and here,
         * we catch it now instead of sleeping through it. */
        handle_msgs(thread_arg);

        /* Spin-poll rings before sleeping.
         * thread_reschedule() restarts the function from the top, so any
         * notification event that fired while we were running is lost
         * (DOCA DPA events are edge-triggered). Spin-polling the ring
         * buffer directly catches descriptors whose doorbell notification
         * was consumed during execution. */
        {
            uint32_t nr = thread_arg->num_rings;
            int spin_found = 0;
            for (uint32_t spin = 0; spin < 100000; spin++) {
                for (uint32_t r = 0; r < nr; r++) {
                    __dpa_thread_window_read_inv();
                    if (process_one_desc(thread_arg, r))
                        spin_found++;
                }
                if (spin_found > 0)
                    break;
            }
            if (spin_found > 0) {
                drain_producer_completions(thread_arg);
                drain_async_ops_completions(thread_arg);
                continue;  /* found late work — loop back to drain_all_rings */
            }
        }

        DOCA_DPA_DEV_LOG_INFO("idle, rescheduling (num_rings=%u)\n",
                              thread_arg->num_rings);
        doca_dpa_dev_thread_reschedule();
    }
}