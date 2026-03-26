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

static inline uint32_t bswap32(uint32_t x) {
    return ((x & 0x000000FF) << 24) |
           ((x & 0x0000FF00) << 8)  |
           ((x & 0x00FF0000) >> 8)  |
           
           ((x & 0xFF000000) >> 24);
}

__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

	return 0;
}

static void send_msgs(struct dpa_thread_arg *thread_arg, int num_msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    doca_dpa_dev_completion_element_t comp;
    struct comch_msg msg;
    uint64_t start, end, tick;
    doca_dpa_dev_completion_type_t t;
    tick = __dpa_thread_time();

    DOCA_DPA_DEV_LOG_INFO("tick frequency: %lu\n", tick);
    for (int i = 0; i < num_msg; i++) {
        start =  __dpa_thread_time();
        doca_dpa_dev_comch_producer_post_send_imm_only(producer,
                                                   dpu_consumer_id,
                                                   (uint8_t *)&msg,
                                                   sizeof(struct comch_msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        
        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0) {
        }

        end =  __dpa_thread_time();
        t = doca_dpa_dev_get_completion_type(comp);
                                                
        DOCA_DPA_DEV_LOG_INFO("type: %d, cycles: %lu\n", t, end - start);
    }
}
static void handle_dpu_msg(struct dpa_thread_arg *thread_arg, const struct comch_msg *msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    // doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_completion_element_t comp;
    uint64_t start, end;

    switch(msg->type) {
        case COMCH_MSG_TYPE_DMA_REQ:
            struct comch_dma_req_msg *dma_msg = (struct comch_dma_req_msg *)msg;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;
            // producer = dma_msg->dpa_producer;
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
                                    "test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            break;
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
        case COMCH_MSG_TYPE_TRIGGER:
            /* No-op: just waking up the thread via completion event */
            DOCA_DPA_DEV_LOG_INFO("Trigger received\n");

            /* DPA->DPU datapath ping to verify producer->consumer delivery path. */
            {
                struct comch_msg ping;
                ping.type = COMCH_MSG_TYPE_TRIGGER;

                while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                }

                doca_dpa_dev_comch_producer_post_send_imm_only(
                    producer,
                    dpu_consumer_id,
                    (uint8_t *)&ping,
                    sizeof(struct comch_msg),
                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

                DOCA_DPA_DEV_LOG_INFO("Sent DPA->DPU ping imm (type=%d, consumer_id=%u)\n",
                                      ping.type, dpu_consumer_id);
            }
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
    static uint32_t empty_polls = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        if (msg == NULL) {
            DOCA_DPA_DEV_LOG_INFO("handle_msgs: got completion with NULL imm (msg_size=%u)\n", msg_size);
        } else {
            DOCA_DPA_DEV_LOG_INFO("handle_msgs: completion msg_size=%u type=%u\n",
                                  msg_size, (uint32_t)msg->type);
        }
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    // send_msgs(thread_arg, num_msgs);
    if (num_msgs != 0) {
        DOCA_DPA_DEV_LOG_INFO("handle_msgs: completion drained num_msgs=%u\n", num_msgs);
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
        empty_polls = 0;
    } else {
        if ((++empty_polls & 0x3FFFFF) == 0) {
            DOCA_DPA_DEV_LOG_INFO("handle_msgs: no completion yet (empty_polls=%u)\n", empty_polls);
        }
    }
    // DOCA_DPA_DEV_LOG_INFO("Handled %u msgs from host\n", num_msgs);
}

/*
 * Multi-ring round-robin polling.
 * Polls all registered rings (one per pod) in a non-blocking fashion.
 */
static void poll_desc_rings(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    doca_dpa_dev_completion_element_t prod_comp;
    struct comch_msg msg;
    doca_dpa_dev_uintptr_t dev_ptr;
    doca_dpa_dev_buf_t buf;
    struct dma_desc *desc;

    uint32_t desc_idx[MAX_DPA_RINGS] = {0};  /* per-ring position */
    uint32_t pos[MAX_DPA_RINGS] = {0};       /* per-ring DMA buffer position */

    uint32_t poll_count = 0;
    uint32_t debug_count = 0;
    uint32_t no_prod_comp_count = 0;
    uint32_t last_nr = 0;

    while (1) {
        /* Periodically check for new messages (e.g., ADD_RING) from DPU */
        if ((poll_count++ & 0xFFFF) == 0)
            handle_msgs(thread_arg);

        /* Periodic debug: log every ~16M iterations to show we're alive */
        if ((debug_count++ & 0xFFFFFF) == 0) {
            uint32_t nr_dbg = thread_arg->num_rings;
            DOCA_DPA_DEV_LOG_INFO("poll alive: num_rings=%u, poll=%u\n",
                                  nr_dbg, debug_count);
        }

        uint32_t nr = thread_arg->num_rings;
        if (nr == 0) {
            /* No rings yet — spin wait for first pod */
            if (nr != last_nr) {
                DOCA_DPA_DEV_LOG_INFO("Waiting for first ring (num_rings=0)\n");
                last_nr = nr;
            }
            continue;
        }
        
        /* Notify when first ring arrives */
        if (nr != last_nr) {
            DOCA_DPA_DEV_LOG_INFO("Rings now available: num_rings=%u (was %u)\n", nr, last_nr);
            last_nr = nr;
        }

        for (uint32_t r = 0; r < nr; r++) {
            struct dpa_ring_info *ring = &thread_arg->rings[r];

            /* Get descriptor at current ring index */
            buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, desc_idx[r]);
            dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
            desc = (struct dma_desc *)dev_ptr;

            /* Non-blocking check: skip if not valid */
            __dpa_thread_window_read_inv();
            if (!desc->valid)
                continue;

            DOCA_DPA_DEV_LOG_INFO("FOUND valid desc: ring=%u slot=%u req_id=%u size=%u dst_pod=%d addr=0x%lx\n",
                                  r, desc_idx[r], (uint32_t)desc->idx, desc->size,
                                  desc->dst_pod_id, desc->addr);

            /* Wait for consumer space */
            uint32_t wait_spins = 0;
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                if ((++wait_spins & 0xFFFFF) == 0) {
                    DOCA_DPA_DEV_LOG_INFO("WAIT consumer space: ring=%u req_id=%u consumer_id=%u spins=%u\n",
                                          r, (uint32_t)desc->idx, dpu_consumer_id, wait_spins);
                }
            }
            if (wait_spins > 0) {
                DOCA_DPA_DEV_LOG_INFO("ACQUIRE consumer space: ring=%u req_id=%u consumer_id=%u spins=%u\n",
                                      r, (uint32_t)desc->idx, dpu_consumer_id, wait_spins);
            }

            /* Build completion message with routing info */
            msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
            msg.dma_comp_msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
            msg.dma_comp_msg.pos = pos[r];
            msg.dma_comp_msg.length = desc->size;
            msg.dma_comp_msg.req_id = (uint32_t)desc->idx;
            msg.dma_comp_msg.src_pod_id = ring->pod_id;
            msg.dma_comp_msg.dst_pod_id = desc->dst_pod_id;
            msg.dma_comp_msg.flags = desc->flags;

            /* DMA copy: Host buffer → DPU local buffer */
            doca_dpa_dev_comch_producer_dma_copy(producer,
                                        dpu_consumer_id,
                                        ring->dpu_mmap,
                                        ring->dpu_addr + pos[r],
                                        ring->host_mmap,
                                        desc->addr,
                                        desc->size,
                                        (uint8_t *)&msg,
                                        sizeof(struct comch_msg),
                                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

            DOCA_DPA_DEV_LOG_INFO("DMA copy submit returned: ring=%u req_id=%u consumer_id=%u imm_size=%u\n",
                                  r, (uint32_t)desc->idx, dpu_consumer_id, (uint32_t)sizeof(struct comch_msg));

            if (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &prod_comp) != 0) {
                doca_dpa_dev_completion_type_t t = doca_dpa_dev_get_completion_type(prod_comp);
                DOCA_DPA_DEV_LOG_INFO("producer completion observed: ring=%u req_id=%u type=%u\n",
                                      r, (uint32_t)desc->idx, (uint32_t)t);
            } else {
                if ((++no_prod_comp_count & 0x3FFFF) == 0) {
                    DOCA_DPA_DEV_LOG_INFO("producer completion not yet visible: ring=%u req_id=%u count=%u\n",
                                          r, (uint32_t)desc->idx, no_prod_comp_count);
                }
            }

            DOCA_DPA_DEV_LOG_INFO("DMA copy issued: ring=%u slot=%u req_id=%u src_addr=0x%lx size=%u\n",
                                  r, desc_idx[r], (uint32_t)desc->idx, desc->addr, desc->size);

            pos[r] += desc->size;
            if (pos[r] >= ring->dpu_buf_size) {
                pos[r] = 0;
            }

            /* Clear valid flag so host can reuse this slot */
            desc->valid = 0;
            __dpa_thread_window_writeback();

            /* Advance to next ring slot */
            desc_idx[r] = (desc_idx[r] + 1) % ring->buf_arr_size;
        }
    }
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

    /* Arm completion notification once before the first poll/drain cycle. */
    doca_dpa_dev_comch_consumer_completion_request_notification(thread_arg->dpa_consumer_comp);
    DOCA_DPA_DEV_LOG_INFO("completion notification armed (consumer_id=%u)\n",
                          thread_arg->dpu_consumer_id);

    /* Handle the trigger message from DPU consumer first */
    handle_msgs(thread_arg);

    /* Go directly into polling loop — no reschedule (would stop the thread) */
    poll_desc_rings(thread_arg);
}