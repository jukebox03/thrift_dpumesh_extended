#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

#define DMA_DIAG_EMPTY_WAIT_WARN_LOOPS  0x100000
#define DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS  0x800000
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

            doca_dpa_dev_comch_producer_dma_copy(producer,
                                    dpu_consumer_id,
                                    dma_msg->dst_mmap,
                                    dma_msg->dst_addr,
                                    dma_msg->src_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->length,
                                    (const uint8_t *)"test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
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
    /* Always re-arm notification so next message wakes the thread */
    doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
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
        uint64_t dst_addr = ring->dpu_addr + thread_arg->pos[r];
        uint64_t dst_len = (uint64_t)desc->size;
        uint64_t dpu_base = ring->dpu_addr;
        uint64_t dpu_size = (uint64_t)ring->dpu_buf_size;
        uint64_t dpu_end = dpu_base + dpu_size;
        uint64_t dst_end = dst_addr + dst_len;

        if (dpu_size == 0 || dpu_end < dpu_base || dst_end < dst_addr ||
            dst_addr < dpu_base || dst_end > dpu_end) {
            DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Local Protection Error]: dst out of DPU mmap range\n");
            DOCA_DPA_DEV_LOG_INFO("Descriptor destination out of DPU range: ring=%u slot=%u req_id=%u dst=[0x%lx..0x%lx) dpu=[0x%lx..0x%lx) len=%u\n",
                                  r, thread_arg->desc_idx[r], (uint32_t)desc->idx,
                                  dst_addr, dst_end, dpu_base, dpu_end, desc->size);
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

    /* Wrap around if DMA would exceed DPU buffer boundary */
    if (thread_arg->pos[r] + desc->size > ring->dpu_buf_size)
        thread_arg->pos[r] = 0;

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

    /* DMA copy: Host buffer → DPU local buffer.
     * Fire-and-forget for the DMA itself (OPTIMIZE_REPORTS suppresses producer completion).
     * Then send completion notification separately via post_send_imm_only so
     * DPU consumer always receives it regardless of DMA success/failure. */
    doca_dpa_dev_comch_producer_dma_copy(producer,
                                dpu_consumer_id,
                                ring->dpu_mmap,
                                ring->dpu_addr + thread_arg->pos[r],
                                ring->host_mmap,
                                desc->addr,
                                desc->size,
                                (uint8_t *)&comp,
                                sizeof(struct comch_dma_comp_msg),
                                DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS |
                                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    DOCA_DPA_DEV_LOG_INFO("DMA copy submitted (fire-and-forget): ring=%u slot=%u req_id=%u size=%u\n",
                          r, thread_arg->desc_idx[r], (uint32_t)desc->idx, desc->size);

    thread_arg->pos[r] += desc->size;

    if (thread_arg->pos[r] >= ring->dpu_buf_size) {
        DOCA_DPA_DEV_LOG_INFO("Ring buffer wrap-around: ring=%u pos=%u buf_size=%u\n",
                              r, thread_arg->pos[r], ring->dpu_buf_size);
        thread_arg->pos[r] = 0;
    }

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
    int total = 0;
    int found;

    do {
        found = 0;
        handle_msgs(thread_arg);

        uint32_t nr = thread_arg->num_rings;
        for (uint32_t r = 0; r < nr; r++) {
            if (process_one_desc(thread_arg, r))
                found++;
        }
        total += found;
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

    DOCA_DPA_DEV_LOG_INFO("[PAIRCHK] run_dma_manager arg: consumer_comp=0x%lx producer_comp=0x%lx consumer=0x%lx producer=0x%lx consumer_id=%u num_rings=%u\n",
                          thread_arg->dpa_consumer_comp,
                          thread_arg->dpa_producer_comp,
                          thread_arg->dpa_consumer,
                          thread_arg->dpa_producer,
                          thread_arg->dpu_consumer_id,
                          thread_arg->num_rings);

    /* Arm completion notification once before the first drain cycle. */
    doca_dpa_dev_comch_consumer_completion_request_notification(thread_arg->dpa_consumer_comp);
    DOCA_DPA_DEV_LOG_INFO("completion notification armed (consumer_id=%u)\n",
                          thread_arg->dpu_consumer_id);

    /* Handle the trigger message from DPU consumer first */
    handle_msgs(thread_arg);

    /* Event loop: process all pending work, then yield until next doorbell */
    while (1) {
        drain_all_rings(thread_arg);

        /* No more work — yield. Thread resumes on next completion event
         * (NEW_DESC doorbell or ADD_RING from DPU). handle_msgs always
         * re-arms notification, so the next message will wake us. */
        DOCA_DPA_DEV_LOG_INFO("idle, rescheduling (num_rings=%u)\n",
                              thread_arg->num_rings);
        doca_dpa_dev_thread_reschedule();
    }
}