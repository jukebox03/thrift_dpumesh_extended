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
    doca_dpa_dev_completion_element_t comp;
    struct comch_msg msg;
    uint64_t start, end, tick;
    doca_dpa_dev_completion_type_t t;
    tick = __dpa_thread_time();

    DOCA_DPA_DEV_LOG_INFO("tick frequency: %lu\n", tick);
    for (int i = 0; i < num_msg; i++) {
        start =  __dpa_thread_time();
        doca_dpa_dev_comch_producer_post_send_imm_only(producer,
                                                   /*consumer_id=*/1,
                                                   (uint8_t *)&msg,
                                                   sizeof(struct comch_msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            								    //    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);
        
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
                
            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1)) {
                DOCA_DPA_DEV_LOG_INFO("Host consumer is empty, cannot send DMA completion\n");
                break;
            }

            doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    dma_msg->dst_mmap,
                                    dma_msg->dst_addr,
                                    dma_msg->src_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->length,
                                    "test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
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
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    // send_msgs(thread_arg, num_msgs);
    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
    // DOCA_DPA_DEV_LOG_INFO("Handled %u msgs from host\n", num_msgs);
}

static void poll_desc_ring(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    struct comch_dma_comp_msg msg;
    doca_dpa_dev_uintptr_t dev_ptr;
    doca_dpa_dev_buf_t buf;
    struct dma_desc *desc;
    int desc_idx = 0;
    uint32_t ring_size = thread_arg->buf_arr_size;
    uint32_t buf_size = thread_arg->buf_size;

    buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, desc_idx);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    desc = (struct dma_desc *)dev_ptr;
        while (!desc->valid) {
            __dpa_thread_window_read_inv();
        }

    /* polling descriptor ring in host memory */
    while (1) {

        // desc = (struct dma_desc *)dev_ptr;
        // while (!desc->valid) {
        //     __dpa_thread_window_read_inv();
        // }

        /* if consumer is empty, wait */
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            DOCA_DPA_DEV_LOG_INFO("Consumer is empty, waiting for messages...\n");
        }

        msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
        msg.pos = thread_arg->pos;
        msg.length = desc->size;

        if (thread_arg->pos +desc->size > buf_size) {
            DOCA_DPA_DEV_LOG_INFO("Reached end of buffer, resetting position\n");
        }
        
        doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    thread_arg->dpu_mmap,
                                    thread_arg->src_addr + thread_arg->pos,
                                    thread_arg->host_mmap,
                                    desc->addr,
                                    desc->size,
                                    (uint8_t *)&msg,
                                    sizeof(struct comch_dma_comp_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | 
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

        thread_arg->pos += desc->size;
        if (thread_arg->pos >= buf_size) {
            thread_arg->pos = 0;
        }
        
        // desc_idx = (desc_idx + 1) % ring_size;
        // buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, desc_idx);
        // dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    }
}

__dpa_global__ void hello_world(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx\n", thread_arg->dpa_buf_arr);
    doca_dpa_dev_buf_t buf = 0;
    doca_dpa_dev_uintptr_t dev_ptr = 0;
    uint64_t buf_len = 0;
    uintptr_t addr = 0;
    DOCA_DPA_DEV_LOG_INFO("buf arr: 0x%lx\n", thread_arg->dpa_buf_arr);
    buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    buf_len = doca_dpa_dev_buf_get_len(buf);
    addr = doca_dpa_dev_buf_get_addr(buf);

    // handle_msgs(thread_arg);
    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("Starting DMA manager thread...\n");
    DOCA_DPA_DEV_LOG_INFO("DPA buffer array handle: 0x%lx, size: %u\n", thread_arg->dpa_buf_arr, thread_arg->buf_arr_size);
    DOCA_DPA_DEV_LOG_INFO("DPU mmap: %u, addr: %p host mmap: %u\n", thread_arg->dpu_mmap, thread_arg->src_addr, thread_arg->host_mmap);

    poll_desc_ring(thread_arg);
    
    doca_dpa_dev_thread_reschedule();
}