#include "dpa.h"

#include <doca_error.h>
#include <doca_log.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_comch_msgq.h>
#include <doca_buf_array.h>
#include <doca_mmap.h>

#include "object.h"
#include "dpa_common.h"
#include "dpu_worker.h"
#include "comch_server.h"
#include "../dpumesh.h"
#include "ring.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

DOCA_LOG_REGISTER(DPA);

#ifdef DOCA_ARCH_DPU
/* Kernel function declaration (resolved from dpa_program.a stubs, DPU only) */
extern doca_dpa_func_t hello_world;
extern doca_dpa_func_t run_dma_manager;
extern doca_dpa_func_t thread_init_rpc;

extern struct doca_dpa_app *DPU_mesh_dpa_app;
#endif

#define TEST_DPA_MEMORY

/*
 * Callback invoked once a message is received from DPA successfully
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_cb(struct doca_comch_consumer_task_post_recv *recv_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
	(void)task_user_data;

	doca_error_t result;
    uint32_t data_len;
    struct comch_msg *msg;

	struct objects *objs = ctx_user_data.ptr;
	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);
    
    data_len = doca_comch_consumer_task_post_recv_get_imm_data_len(recv_task);
    msg = (struct comch_msg *)doca_comch_consumer_task_post_recv_get_imm_data(recv_task);

    switch (msg->type) {
        case COMCH_MSG_TYPE_DMA_COMPLETED: {
            struct comch_dma_comp_msg *comp_msg = (struct comch_dma_comp_msg *)msg;
            int32_t src_pod_id = comp_msg->src_pod_id;
            int32_t dst_pod_id = comp_msg->dst_pod_id;
            uint32_t req_id = comp_msg->req_id;

            /* Find the source pod's local DMA buffer for data */
            struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);
            uint8_t *data = NULL;
            if (src_pod && src_pod->dma_buffer) {
                data = (uint8_t *)src_pod->dma_buffer + comp_msg->pos;
            } else {
                DOCA_LOG_ERR("DMA completed but src_pod %d not found or no buffer", src_pod_id);
                break;
            }
            uint32_t data_len = comp_msg->length;

            DOCA_LOG_INFO("DMA completed: src_pod=%d, dst_pod=%d, req_id=%u, pos=%u, len=%u",
                          src_pod_id, dst_pod_id, req_id, comp_msg->pos, data_len);

            /* Route to destination pod */
            int echo_mode = (dst_pod_id == -1 ||
                             dst_pod_id == src_pod_id ||
                             objs->num_pods <= 1);

            if (!echo_mode) {
                struct pod_state *dst = find_pod_by_id(objs, dst_pod_id);
                if (dst && dst->connection) {
                    /* Forward to destination pod */
                    sw_descriptor_t fwd_desc;
                    memset(&fwd_desc, 0, sizeof(fwd_desc));
                    fwd_desc.header_buf_slot = -1;
                    fwd_desc.body_buf_slot = -1;
                    fwd_desc.body_len = data_len;
                    fwd_desc.req_id = req_id;
                    fwd_desc.src_pod_id = src_pod_id;
                    fwd_desc.dst_pod_id = dst_pod_id;
                    fwd_desc.flags = comp_msg->flags;
                    fwd_desc.valid = 1;

                    doca_error_t fwd_result = server_send_rx_data_to(
                        objs, dst->connection,
                        &fwd_desc, sizeof(fwd_desc), data, data_len);
                    if (fwd_result != DOCA_SUCCESS)
                        DOCA_LOG_ERR("Forward to pod %d failed for req_id=%u: %s",
                                     dst_pod_id, req_id, doca_error_get_descr(fwd_result));
                    else
                        DOCA_LOG_INFO("Forwarded %u bytes to pod %d for req_id=%u",
                                      data_len, dst_pod_id, req_id);
                } else {
                    DOCA_LOG_ERR("DMA completed: dst_pod=%d not found", dst_pod_id);
                }
            } else {
                /* Echo mode: same pod or single-pod testing */
                sw_descriptor_t echo_desc;
                memset(&echo_desc, 0, sizeof(echo_desc));
                echo_desc.header_buf_slot = -1;
                echo_desc.body_buf_slot = -1;
                echo_desc.body_len = data_len;
                echo_desc.req_id = req_id;
                echo_desc.flags = OP_RESPONSE;
                echo_desc.valid = 1;

                doca_error_t echo_result = server_send_rx_data(objs,
                    &echo_desc, sizeof(echo_desc), data, data_len);
                if (echo_result != DOCA_SUCCESS)
                    DOCA_LOG_ERR("Echo back failed for req_id=%u: %s",
                                 req_id, doca_error_get_descr(echo_result));
            }
            break;
        }
        default:
            DOCA_LOG_ERR("Received unknown message type: %u", msg->type);
            break;
    }

    objs->recv_msg_cnt++;

	result = doca_task_submit(task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("DPA MsgQ receive callback failed: Failed to resubmit receive task - %s",
			     doca_error_get_name(result));
		doca_task_free(task);
	}
}

/*
 * Callback invoked once consumer encounters a receive error
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_error_cb(struct doca_comch_consumer_task_post_recv *recv_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
	(void)task_user_data;
	(void)ctx_user_data;

	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);

	doca_task_free(task);
}
/*
 * Callback invoked once a message is sent to DPA successfully
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_cb(struct doca_comch_producer_task_send *send_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
    doca_error_t result;
    struct comch_msg msg;
	(void)task_user_data;
	
    
    struct objects *objs = (struct objects *)ctx_user_data.ptr;
    objs->sent_msg_cnt++;
    
    // DOCA_LOG_INFO("Sent msg to DPA successfully, cnt: %d", objs->sent_msg_cnt);
    doca_comch_producer_task_send_set_imm_data(send_task, (uint8_t *)&msg, sizeof(struct comch_msg));
    
	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    doca_task_free(task);

    /* Below code is used to resubmit the send task if needed, 
     * but this leads to deadlock in our current design
    do {
        result = doca_task_submit(task);
    } while (result == DOCA_ERROR_AGAIN);
    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to resubmit msgQ send task - %s",
            doca_error_get_name(result));
            doca_task_free(task);
    }
    */
}

/*
 * Callback invoked once producer encounters a send error
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_error_cb(struct doca_comch_producer_task_send *send_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
	(void)task_user_data;
	(void)ctx_user_data;

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    DOCA_LOG_ERR("Failed to send msg");
	doca_task_free(task);
}

/*
 * Callback invoked once consumer/producer state changes
 *
 * @user_data [in]: The user data associated with the context
 * @ctx [in]: The consumer/producer context
 * @prev_state [in]: The previous state
 * @next_state [in]: The new state
 */
void dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb(const union doca_data user_data,
							  struct doca_ctx *ctx,
							  enum doca_ctx_states prev_state,
							  enum doca_ctx_states next_state)
{
	(void)ctx;
	(void)prev_state;

	// struct nvmf_doca_io *io = user_data.ptr;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
        DOCA_LOG_ERR("DPA comch msgQ state is idle.");
		// nvmf_doca_dpa_comch_stop_continue(&io->comch);
		break;
    case DOCA_CTX_STATE_STARTING:
    case DOCA_CTX_STATE_RUNNING:
        DOCA_LOG_ERR("DPA comch msgQ state is running.");
	case DOCA_CTX_STATE_STOPPING:
	default:
		break;
	}
}

#ifdef DOCA_ARCH_DPU

doca_error_t
init_dpa_objects(struct objects *objs)
{
    doca_error_t result;
    struct dmesh_doca_dpa_thread *dpa_thread;

    if (!objs->dpa_thread) {
		objs->dpa_thread = malloc(sizeof(struct dmesh_doca_dpa_thread));
		if (!objs->dpa_thread) {
			DOCA_LOG_ERR("Failed to allocate memory for dpa_thread");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

	if (!objs->dpa_comch) {
		objs->dpa_comch = malloc(sizeof(struct dmesh_doca_dpa_comch));
		if (!objs->dpa_comch) {
			DOCA_LOG_ERR("Failed to allocate memory for dpa_comch");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

    result = doca_dpa_create(objs->dev, &objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DPA with error = %s", doca_error_get_name(result));
        return result;
    }

    result = doca_dpa_set_app(objs->dpa_thread->dpa, DPU_mesh_dpa_app);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA application with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    result = doca_dpa_start(objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DOCA DPA with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    DOCA_LOG_INFO("Init DOCA DPA done.");
    return DOCA_SUCCESS;

destroy_dpa:
    doca_dpa_destroy(objs->dpa_thread->dpa);
    objs->dpa_thread->dpa = NULL;
    return result;
}

doca_error_t
launch_dpa_kernel(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;

    result = doca_dpa_kernel_launch_update_set(dpa_thread->dpa, 
                    NULL, 0,
                    NULL, 0,
                    1,
                    &hello_world);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to launch DPA kernel with error = %s", doca_error_get_name(result));
        return result;
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;

    result = doca_dpa_mem_alloc(dpa_thread->dpa, sizeof(struct dpa_thread_arg), &dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to alloc dpa mem: %s",
            doca_error_get_descr(result));
        return result;
    }

// #ifdef TEST_DPA_MEMORY
//     result = doca_dpa_mem_alloc(dpa_thread->dpa, 1024, &dpa_thread->buf);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to alloc dpa mem for buffer: %s",
//             doca_error_get_descr(result));
//         return result;
//     }

//     char *temp = "Hello from Host to DPA via DPA memory!";
//     result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->buf,
//                                 temp, strlen(temp) + 1);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to copy data from host to DPA memory: %s",
//             doca_error_get_descr(result));
//         return result;
//     }

//     DOCA_LOG_INFO("Copied data to DPA memory at device pointer: 0x%lx", dpa_thread->buf);
// #endif

    result = doca_dpa_thread_create(dpa_thread->dpa, &dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create dpa thread: %s",
            doca_error_get_descr(result));
        return result;
    }
    
    result = doca_dpa_thread_set_func_arg(dpa_thread->thread, run_dma_manager, dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA thread func: %s",
            doca_error_get_descr(result));
        return result;
    }
    
    result = doca_dpa_thread_start(dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DPA thread: %s",
            doca_error_get_descr(result));
        return result;
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_msgq_create(const struct dmesh_doca_dpa_msgq_create_attr *attr,
                            struct dmesh_doca_dpa_msgq *msgq)
{
    doca_error_t result;
    struct doca_ctx *consumer_ctx;
    struct doca_ctx *producer_ctx;

    memset(msgq, 0, sizeof(*msgq));

    msgq->is_send = attr->is_send;

    if (msgq->pe == NULL) {
        result = doca_pe_create(&msgq->pe);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create PE - %s",
                    doca_error_get_name(result));
            return result;
        }
    }

    result = doca_comch_msgq_create(attr->dev, &msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create comch msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_msgq_set_max_num_consumers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num consumers - %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_msgq_set_max_num_producers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num producers - %s",
                doca_error_get_name(result));
        return result;
    }
    
    /* if true, DPA is consumer */
    if (attr->is_send) {
        result = doca_comch_msgq_set_dpa_consumer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa consumer - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* else, DPA is producer */
        result = doca_comch_msgq_set_dpa_producer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa producer - %s",
                    doca_error_get_name(result));
            return result;
        }
    }
    
    result = doca_comch_msgq_start(msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_msgq_consumer_create(msgq->msgq, &msgq->consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create msgq consumer - %s",
                doca_error_get_name(result));
        return result;
    }
    
    consumer_ctx = doca_comch_consumer_as_ctx(msgq->consumer);
    result = doca_comch_consumer_set_imm_data_len(msgq->consumer, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len to 64 - %s",
                doca_error_get_name(result));
        return result;
    }
    
    if (attr->is_send) {
        /* consumer on DPA */
        result = doca_ctx_set_datapath_on_dpa(consumer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer datapath on dpa - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_completion(msgq->consumer, attr->consumer_comp, 0);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer completion - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_dev_max_num_recv(msgq->consumer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer max # of recv messages - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* consumer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(consumer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer ctx user data - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(consumer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, consumer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect consumer to pe - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_task_post_recv_set_conf(msgq->consumer,
                                        dmesh_doca_dpa_msgq_recv_cb,
                                        dmesh_doca_dpa_msgq_recv_error_cb,
                                        attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer task config - %s",
                    doca_error_get_name(result));
            return result;
        }
    }

    result = doca_ctx_start(consumer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start consumer ctx - %s", 
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_msgq_producer_create(msgq->msgq, &msgq->producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create msgq producer - %s", 
                doca_error_get_name(result));
        return result;
    }
    producer_ctx = doca_comch_producer_as_ctx(msgq->producer);
    if (attr->is_send) {
        /* producer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(producer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer ctx user data - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(producer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, producer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect producer to pe - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_task_send_set_conf(msgq->producer,
                                dmesh_doca_dpa_msgq_send_cb,
                                dmesh_doca_dpa_msgq_send_error_cb,
                                attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer task config - %s", 
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* producer on DPA */
        result = doca_ctx_set_datapath_on_dpa(producer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer datapath on dpa - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_set_dev_max_num_send(msgq->producer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer max # of send messages - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_dpa_completion_attach(msgq->producer, attr->producer_comp);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to attach producer dpa completion - %s", 
                    doca_error_get_name(result));
            return result;
        }
    }
    result = doca_ctx_start(producer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer ctx - %s",
                doca_error_get_name(result));
        return result;
    }

    /* Pre-post recv tasks if MsgQ is used for receiving from DPA */
    if (attr->is_send == false) {
        for (uint32_t idx = 0; idx < attr->max_num_msg; idx++) {
            struct doca_comch_consumer_task_post_recv *recv_task;
            result = doca_comch_consumer_task_post_recv_alloc_init(msgq->consumer, NULL, &recv_task);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to allocate consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(recv_task));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
        }
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_comch_create(struct objects *objs)
{
    struct dmesh_doca_dpa_comch *comch = objs->dpa_comch;
    struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_thread;
    uint32_t max_num_recv, imm_data_len;
    doca_error_t result;
    
    memset(comch, 0, sizeof(*comch));

    result = doca_comch_consumer_completion_create(&(comch->consumer_comp));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create consumer completion - %s",
                doca_error_get_name(result));
        assert(0);
        return result;
    }
    
    result = doca_comch_consumer_completion_set_max_num_recv(comch->consumer_comp,
            CC_DPA_MAX_MSG_NUM);    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num recv - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_set_imm_data_len(comch->consumer_comp, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len - %s",
            doca_error_get_name(result));
        return result;
        }
        
    result = doca_comch_consumer_completion_set_dpa_thread(comch->consumer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_start(comch->consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start consumer completion - %s",
			     doca_error_get_name(result));
		return result;
	}

    result = doca_dpa_completion_create(dpa_thread->dpa, CC_DPA_MAX_MSG_NUM, &comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_set_thread(comch->producer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread to producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_start(comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer completion - %s",
                doca_error_get_name(result));
        return result;
    }

    return DOCA_SUCCESS;
}

/*
 * Fills the DPA thread argument with the relevant DPA handles to be later copied to the DPA thread
 *
 * @arg [out]: The returned thread argument that was filled
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
/*
 * Fill shared comch handles into DPA thread arg (no ring info yet — added per-pod).
 */
static doca_error_t
dmesh_fill_dpa_thread_arg(struct objects *objs, struct dpa_thread_arg *arg)
{
    doca_error_t result;
    struct dmesh_doca_dpa_comch *comch = objs->dpa_comch;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
    doca_dpa_dev_completion_t dpa_producer_comp;
    doca_dpa_dev_comch_producer_t dpa_producer;
    doca_dpa_dev_comch_consumer_t dpa_consumer;

    result = doca_comch_consumer_completion_get_dpa_handle(comch->consumer_comp, &dpa_consumer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer completion DPA handle: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_get_dpa_handle(comch->producer_comp, &dpa_producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer completion DPA handle: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_comch_consumer_get_dpa_handle(comch->send.consumer, &dpa_consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer DPA handle: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_comch_producer_get_dpa_handle(comch->recv.producer, &dpa_producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer DPA handle: %s", doca_error_get_name(result));
        return result;
    }

    memset(arg, 0, sizeof(*arg));
    arg->dpa_consumer_comp = dpa_consumer_comp;
    arg->dpa_producer_comp = dpa_producer_comp;
    arg->dpa_consumer = dpa_consumer;
    arg->dpa_producer = dpa_producer;
    arg->num_rings = 0;  /* rings added dynamically via setup_pod_dma */

    DOCA_LOG_INFO("DPA thread arg: consumer_comp=0x%lx, producer_comp=0x%lx, consumer=0x%lx, producer=0x%lx",
        arg->dpa_consumer_comp, arg->dpa_producer_comp,
        arg->dpa_consumer, arg->dpa_producer);

    return DOCA_SUCCESS;
}

/*
 *  Initialize and run the DOCA DPA thread
 *
 */
doca_error_t
dmesh_doca_run_dpa_thread(struct objects *objs, struct dmesh_doca_dpa_thread *dpa_thread, struct dmesh_doca_dpa_comch *comch)
{
    doca_error_t result;
    struct dpa_thread_arg arg;

    result = dmesh_fill_dpa_thread_arg(objs, &arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to fill dpa thread argument - %s",
            doca_error_get_name(result));
        return result;
    }

    uint64_t rpc_ret;
    uint32_t num_msg = CC_DPA_MAX_MSG_NUM;
    result = doca_dpa_rpc(dpa_thread->dpa, 
                        thread_init_rpc,
                        &rpc_ret,
                        arg.dpa_consumer,
                        num_msg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to issue init thread RPC - %s",
            doca_error_get_name(result));
        return result;
    }

    if (rpc_ret != 0) {
        DOCA_LOG_ERR("Failed to init thread RPC");
        return result;
    }

    result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg, 
                                &arg, sizeof(struct dpa_thread_arg));
    if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to update DPA thread argument - %s",
			     doca_error_get_name(result));
		return result;
	}

    result = doca_dpa_thread_run(dpa_thread->thread);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to run DPA thread - %s",
			     doca_error_get_name(result));
		return result;
	}             

    return DOCA_SUCCESS;
}

/*
 * Send message to DPA using NVMf DOCA DPA MsgQ
 *
 * @msgq [in]: The MsgQ to be used for the send operation
 * @msg [in]: The message to send
 * @msg_size [in]: The message size
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size)
{
	doca_error_t result;
    union doca_data user_data;

	struct doca_comch_producer_task_send *send_task;
	result = doca_comch_producer_task_send_alloc_init(msgq->producer,
							  NULL,
							  msg,
							  msg_size,
							  /*consumer_id=*/1,
							  &send_task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
			     doca_error_get_name(result));
		return result;
	}

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);

    user_data.ptr = msgq;
    doca_task_set_user_data(task, user_data);

    do {
        result = doca_task_submit(task);
    } while (result == DOCA_ERROR_AGAIN);
    
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
			     doca_error_get_name(result));
		doca_task_free(task);
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t 
dmesh_doca_dpa_msgq_send_bulk(struct dmesh_doca_dpa_msgq *msgq, uint32_t num_msg,
                                void *msg, uint32_t msg_size)
{
	struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;
	doca_error_t result;
    int i;
    struct comch_msg *comch_msg = (struct comch_msg *)msg;

    for (i = 0; i < num_msg; i++) {
        result = doca_comch_producer_task_send_alloc_init(msgq->producer,
                                  NULL,
                                  msg,
                                  msg_size,
                                  /*consumer_id=*/1,
                                  &send_task);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
                     doca_error_get_name(result));
            return result;
        }
        task = doca_comch_producer_task_send_as_task(send_task);
        result = doca_task_submit(task);
        if (result != DOCA_SUCCESS) {
            if (result != DOCA_ERROR_AGAIN)
                DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
                     doca_error_get_name(result));
            doca_task_free(task);
            return result;
        }
    }
    // DOCA_LOG_INFO("Sent mesg done.");
	return DOCA_SUCCESS;
}

doca_error_t
setup_dpa_buf_array(struct objects *objs, size_t num_elem, struct doca_mmap *mmap)
{
    return setup_dpa_buf_array_pod(objs, num_elem, mmap, &objs->buf_arr);
}

/*
 * Create a DPA buffer array for a specific mmap (per-pod version).
 */
doca_error_t
setup_dpa_buf_array_pod(struct objects *objs, size_t num_elem,
                        struct doca_mmap *mmap, struct doca_buf_arr **out_buf_arr)
{
    doca_error_t result;

    result = doca_buf_arr_create(num_elem, out_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create buffer array: %s", doca_error_get_descr(result));
        return result;
    }

    result = doca_buf_arr_set_target_dpa(*out_buf_arr, objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array target DPA: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_set_params(*out_buf_arr, mmap, sizeof(struct dma_desc), 0);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array params: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_start(*out_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start buffer array: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    return DOCA_SUCCESS;

destroy_buf_arr:
    doca_buf_arr_destroy(*out_buf_arr);
    *out_buf_arr = NULL;
    return result;
}

#include "buffer.h"
#include "dma.h"
#include "comch_common.h"

#define DPU_BUFFER_SIZE (1024 * 1024)

/*
 * Fill ring info for a specific pod.
 */
static doca_error_t
dmesh_fill_dpa_ring_info(struct objects *objs, struct pod_state *pod,
                         struct dpa_ring_info *ring_info)
{
    doca_error_t result;
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    doca_dpa_dev_mmap_t host_mmap, dpu_mmap;

    result = doca_buf_arr_get_dpa_handle(pod->buf_arr, &dpa_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get buf array DPA handle: %s", doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(pod->remote_mmap, objs->dev, &host_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get host mmap DPA handle: %s", doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(pod->local_mmap, objs->dev, &dpu_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get DPU mmap DPA handle: %s", doca_error_get_name(result));
        return result;
    }

    ring_info->buf_arr = dpa_buf_arr;
    ring_info->buf_arr_size = DMA_RING_SIZE;
    ring_info->host_mmap = host_mmap;
    ring_info->dpu_mmap = dpu_mmap;
    ring_info->dpu_addr = (uint64_t)pod->dma_buffer;
    ring_info->dpu_buf_size = DPU_BUFFER_SIZE;
    ring_info->pod_id = pod->pod_id;

    return DOCA_SUCCESS;
}

/*
 * Per-pod DMA setup. Called when both ring_mmap and remote_mmap arrive.
 * 1. Create buf_arr for pod's ring_mmap
 * 2. Allocate local DMA buffer for pod
 * 3. Export local buffer to Host
 * 4. Fill DPA ring info
 * 5. Update DPA thread arg (h2d_memcpy)
 * 6. If first pod, run DPA thread
 */
doca_error_t
setup_pod_dma(struct objects *objs, struct pod_state *pod)
{
    doca_error_t result;

    DOCA_LOG_INFO("setup_pod_dma: pod_id=%d", pod->pod_id);

    /* 1. Create per-pod buf_arr over ring_mmap */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE, pod->ring_mmap, &pod->buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: buf_arr failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }

    /* 2. Allocate local DMA buffer (DPU working buffer) + PCI export */
    result = alloc_buffer_and_set_mmap(&pod->local_mmap, objs->dev,
                                       &pod->dma_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: alloc buffer failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }

    /* 3. Export local DMA buffer mmap back to Host */
    result = export_mmap_to_remote(objs, pod->local_mmap, pod->dma_buffer,
                                    DPU_BUFFER_SIZE, DMA_BUFFER, DPU_TO_HOST);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("setup_pod_dma: export to host failed (may be OK if Host doesn't need it): %s",
                      doca_error_get_descr(result));
        /* Non-fatal for now */
    }

    /* 4. Fill DPA ring info for this pod */
    struct dpa_ring_info ring_info;
    result = dmesh_fill_dpa_ring_info(objs, pod, &ring_info);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: fill ring info failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }

    /* 5. Update DPA thread arg: write ring info first, then increment num_rings */
    struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_thread;
    uint32_t ring_idx = objs->dpa_thread_running ? 0 : 0;  /* will use current num_rings */

    /* We need to read current arg, update, and write back.
     * Since we write the full arg via h2d_memcpy, build it locally. */
    struct dpa_thread_arg arg;

    if (!objs->dpa_thread_running) {
        /* First pod: fill shared handles + first ring */
        result = dmesh_fill_dpa_thread_arg(objs, &arg);
        if (result != DOCA_SUCCESS)
            return result;
        arg.rings[0] = ring_info;
        arg.num_rings = 1;
    } else {
        /* Subsequent pods: read current arg from DPA, add ring, write back */
        result = doca_dpa_d2h_memcpy(dpa_thread->dpa, &arg, dpa_thread->arg,
                                      sizeof(struct dpa_thread_arg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: d2h_memcpy failed: %s", doca_error_get_descr(result));
            return result;
        }
        if (arg.num_rings >= MAX_DPA_RINGS) {
            DOCA_LOG_ERR("setup_pod_dma: max rings reached (%d)", MAX_DPA_RINGS);
            return DOCA_ERROR_FULL;
        }
        arg.rings[arg.num_rings] = ring_info;
        arg.num_rings++;
    }

    result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg,
                                  &arg, sizeof(struct dpa_thread_arg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: h2d_memcpy failed: %s", doca_error_get_descr(result));
        return result;
    }

    /* 6. If first pod, run DPA thread */
    if (!objs->dpa_thread_running) {
        uint64_t rpc_ret;
        uint32_t num_msg = CC_DPA_MAX_MSG_NUM;
        result = doca_dpa_rpc(dpa_thread->dpa, thread_init_rpc, &rpc_ret,
                              arg.dpa_consumer, num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: thread_init_rpc failed: %s", doca_error_get_descr(result));
            return result;
        }

        result = doca_dpa_thread_run(dpa_thread->thread);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: dpa_thread_run failed: %s", doca_error_get_descr(result));
            return result;
        }
        objs->dpa_thread_running = 1;
        DOCA_LOG_INFO("DPA thread set to runnable (pod_id=%d), sending trigger msg", pod->pod_id);

        /* Send trigger message to DPA consumer to activate the thread.
         * DPA thread only runs when its attached completion ctx fires.
         * The consumer_comp is attached to the thread, so sending any
         * message via the DPU→DPA msgq triggers the first execution. */
        {
            struct comch_msg trigger;
            trigger.type = COMCH_MSG_TYPE_DMA_COMPLETED;  /* reuse existing type */
            trigger.dma_comp_msg.pos = 0;
            trigger.dma_comp_msg.length = 0;
            trigger.dma_comp_msg.req_id = 0;
            trigger.dma_comp_msg.src_pod_id = -1;
            trigger.dma_comp_msg.dst_pod_id = -1;
            result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                               &trigger, sizeof(trigger));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_WARN("Trigger msg to DPA failed: %s (thread may not start)",
                              doca_error_get_descr(result));
            } else {
                DOCA_LOG_INFO("Trigger msg sent to DPA, thread should activate");
            }
        }
    } else {
        DOCA_LOG_INFO("Added ring for pod_id=%d (num_rings=%u)", pod->pod_id, arg.num_rings);
    }

    pod->dma_ready = 1;
    return DOCA_SUCCESS;
}
#endif /* DOCA_ARCH_DPU */