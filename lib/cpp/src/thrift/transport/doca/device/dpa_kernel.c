#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
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
    struct comch_msg msg;
    doca_dpa_dev_buf_t buf;
    doca_dpa_dev_uintptr_t dev_ptr;
    struct dma_desc *desc;

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    DOCA_DPA_DEV_LOG_INFO("FOUND valid desc: ring=%u slot=%u req_id=%u size=%u dst_pod=%d addr=0x%lx\n",
                          r, thread_arg->desc_idx[r], (uint32_t)desc->idx, desc->size,
                          desc->dst_pod_id, desc->addr);

    while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
    }

    /* Wrap around if DMA would exceed DPU buffer boundary */
    if (thread_arg->pos[r] + desc->size > ring->dpu_buf_size)
        thread_arg->pos[r] = 0;

    /* Build completion message with routing info */
    msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg.dma_comp_msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg.dma_comp_msg.pos = thread_arg->pos[r];
    msg.dma_comp_msg.length = desc->size;
    msg.dma_comp_msg.req_id = (uint32_t)desc->idx;
    msg.dma_comp_msg.src_pod_id = ring->pod_id;
    msg.dma_comp_msg.dst_pod_id = desc->dst_pod_id;
    msg.dma_comp_msg.flags = desc->flags;

    DOCA_DPA_DEV_LOG_INFO("DMA completion msg prepared: req_id=%u src_pod=%d dst_pod=%d pos=%u len=%u flags=0x%x\n",
                          msg.dma_comp_msg.req_id,
                          msg.dma_comp_msg.src_pod_id,
                          msg.dma_comp_msg.dst_pod_id,
                          msg.dma_comp_msg.pos,
                          msg.dma_comp_msg.length,
                          (unsigned int)(uint8_t)msg.dma_comp_msg.flags);

    /* 1. Drain any stale completions before issuing new DMA */
    {
        doca_dpa_dev_completion_element_t stale_comp;
        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &stale_comp) != 0) {
        }
    }

    /* 2. DMA copy: Host buffer → DPU local buffer + Send completion to DPU */
    doca_dpa_dev_comch_producer_dma_copy(producer,
                                dpu_consumer_id,
                                ring->dpu_mmap,
                                ring->dpu_addr + thread_arg->pos[r],
                                ring->host_mmap,
                                desc->addr,
                                desc->size,
                                (uint8_t *)&msg,
                                sizeof(struct comch_msg),
                                DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    /* 3. Wait for DMA completion */
    {
        doca_dpa_dev_completion_element_t dma_comp;
        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &dma_comp) == 0) {
        }
    }

    DOCA_DPA_DEV_LOG_INFO("DMA copy issued: ring=%u slot=%u req_id=%u src_addr=0x%lx size=%u\n",
                          r, thread_arg->desc_idx[r], (uint32_t)desc->idx, desc->addr, desc->size);

    thread_arg->pos[r] += desc->size;

    /* Clear valid flag so host can reuse this slot */
    desc->valid = 0;
    __dpa_thread_window_writeback();

    /* Advance to next ring slot */
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