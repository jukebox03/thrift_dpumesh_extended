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
#include "comch_common.h"
#include "dpu_worker.h"
#include "comch_producer.h"
#include "comch_consumer.h"
#include "../dpumesh.h"
#include "ring.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
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
    static uint64_t recv_cb_count = 0;

	doca_error_t result;
    uint32_t data_len;

	struct objects *objs = ctx_user_data.ptr;
	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);

    data_len = doca_comch_consumer_task_post_recv_get_imm_data_len(recv_task);

    /* DPA sends comch_dma_comp_msg directly (<=32 bytes) rather than the full
     * comch_msg union, so read raw bytes and dispatch by the leading type field. */
    uint8_t *raw = (uint8_t *)doca_comch_consumer_task_post_recv_get_imm_data(recv_task);
    recv_cb_count++;


    if (raw == NULL) {
        goto resubmit_recv_task;
    }

    if (data_len < sizeof(enum comch_msg_type)) {
        goto resubmit_recv_task;
    }

    enum comch_msg_type msg_type = *(enum comch_msg_type *)raw;

    switch (msg_type) {
        case COMCH_MSG_TYPE_DMA_COMPLETED: {
            if (data_len < sizeof(struct comch_dma_comp_msg)) {
                break;
            }
            struct comch_dma_comp_msg *comp_msg = (struct comch_dma_comp_msg *)raw;
            int32_t src_pod_id = comp_msg->src_pod_id;
            int32_t dst_pod_id = comp_msg->dst_pod_id;
            uint32_t req_id = comp_msg->req_id;

            /* Find the source pod's local DMA buffer for data */
            struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);
            if (!src_pod || !src_pod->dma_buffer) {
                break;
            }

            /* Body is the entire DMA payload — no in-band header.
             * length / pos / req_id / pod ids / flags travel via comp_msg. */
            uint32_t payload_len = comp_msg->length;
            uint32_t body_offset = comp_msg->pos;


            /* Enqueue for deferred processing in main loop.
             * TX_ACK + reverse DMA routing handled there — never send
             * from inside this callback (re-entrant PE corruption risk). */
            dpu_comp_entry_t entry;
            entry.entry_type = COMP_ENTRY_FORWARD;
            entry.src_pod_id = src_pod_id;
            entry.dst_pod_id = dst_pod_id;
            entry.req_id = req_id;
            entry.length = payload_len;
            entry.flags = comp_msg->flags;
            /* Phase 4: forward chunk slot info so DPU's main loop can issue
             * body DMA itself. */
            entry.src_chunk_buf_slot = comp_msg->src_chunk_buf_slot;
            entry.src_chunk_buf_len  = comp_msg->src_chunk_buf_len;
            entry.body_dma_done    = 0;
            entry.hdr_rev_dma_done = 0;

            /* Zero-copy: record buffer offset instead of heap-copying.
             * End-node slot-based admission keeps in-flight bytes ≤ buf_size
             * so DPA cannot lap unconsumed data. */
            entry.buf_offset = body_offset;
            entry.pod_idx = -1; /* will be resolved by pod lookup */
            for (int pi = 0; pi < objs->num_pods; pi++) {
                if (objs->pods[pi].pod_id == src_pod_id) {
                    entry.pod_idx = pi;
                    break;
                }
            }

            if (comp_queue_enqueue(&objs->comp_queue, &entry) != 0) {
                /* zero-copy: no heap data to free */
            } else {
            }
            break;
        }
        case COMCH_MSG_TYPE_DMA_CHUNK:
            /* Intermediate DMA chunk landed — no action needed, just resubmit recv */
            break;
        case COMCH_MSG_TYPE_REV_DMA_COMPLETED: {
            /* Reverse DMA completed (DPU→CPU): DPA has DMA'd data from DPU TX
             * buffer to Host RX buffer. Enqueue for DPU worker to forward
             * completion notification to the destination Host pod via comch. */
            if (data_len < sizeof(struct comch_dma_comp_msg)) {
                break;
            }
            struct comch_dma_comp_msg *rev_comp = (struct comch_dma_comp_msg *)raw;

            dpu_comp_entry_t rev_entry;
            rev_entry.entry_type = COMP_ENTRY_REV_NOTIFY;
            rev_entry.src_pod_id = rev_comp->src_pod_id;
            rev_entry.dst_pod_id = rev_comp->dst_pod_id;
            rev_entry.req_id = rev_comp->req_id;
            rev_entry.length = rev_comp->length;
            rev_entry.flags = rev_comp->flags;
            rev_entry.buf_offset = rev_comp->pos;  /* position in Host RX buffer */
            rev_entry.pod_idx = -1;
            /* Reverse path: no chunk slot. */
            rev_entry.src_chunk_buf_slot = -1;
            rev_entry.src_chunk_buf_len  = 0;
            rev_entry.body_dma_done    = 1;  /* not used on rev path */
            rev_entry.hdr_rev_dma_done = 1;

            if (comp_queue_enqueue(&objs->comp_queue, &rev_entry) != 0) {
            } else {
            }
            break;
        }
        case COMCH_MSG_TYPE_TRIGGER:
            break;
        default:
            break;
    }

    objs->recv_msg_cnt++;

resubmit_recv_task:
    /* Backpressure: if comp_queue is nearly full, defer recv task resubmission.
     * DPA will see consumer_empty and naturally pause, giving DPU time to drain.
     * Main loop resubmits when queue drops below BP_LOW.
     * If submit fails (e.g. transient state), also stash so the main loop retries
     * rather than losing the task. */
    if (comp_queue_usage(&objs->comp_queue) >= COMP_QUEUE_BP_HIGH &&
        objs->num_deferred_recv < MAX_DEFERRED_RECV) {
        objs->deferred_recv[objs->num_deferred_recv++] = task;
    } else {
        result = doca_task_submit(task);
        if (result != DOCA_SUCCESS) {
            if (objs->num_deferred_recv < MAX_DEFERRED_RECV) {
                objs->deferred_recv[objs->num_deferred_recv++] = task;
            } else {
            }
        }
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
	static uint64_t recv_err_count = 0;
	recv_err_count++;

	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);
	doca_error_t status = doca_task_get_status(task);


	/* Resubmit to keep the recv task alive — do not free. */
	doca_error_t resubmit = doca_task_submit(task);
	if (resubmit != DOCA_SUCCESS) {
	}
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
	void *payload_copy = task_user_data.ptr;
	
    
    struct objects *objs = (struct objects *)ctx_user_data.ptr;
    objs->sent_msg_cnt++;

	if (payload_copy != NULL)
		free(payload_copy);
    
	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    doca_task_free(task);
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
    void *payload_copy = task_user_data.ptr;
    (void)ctx_user_data;

    struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    if (payload_copy != NULL) {
        free(payload_copy);
    }
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
	(void)prev_state;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		break;
    case DOCA_CTX_STATE_STARTING:
        break;
    case DOCA_CTX_STATE_RUNNING:
        (void)ctx;
        break;
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

    if (!objs->dpa_thread) {
		objs->dpa_thread = malloc(sizeof(struct dmesh_doca_dpa_thread));
		if (!objs->dpa_thread) {
			return DOCA_ERROR_NO_MEMORY;
		}
	}

	if (!objs->dpa_comch) {
		objs->dpa_comch = malloc(sizeof(struct dmesh_doca_dpa_comch));
		if (!objs->dpa_comch) {
			return DOCA_ERROR_NO_MEMORY;
		}
	}

    result = doca_dpa_create(objs->dev, &objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_dpa_set_app(objs->dpa_thread->dpa, DPU_mesh_dpa_app);
    if (result != DOCA_SUCCESS) {
        goto destroy_dpa;
    }

    /* DPA log level kept at ERROR. INFO produces per-DMA / per-trigger lines
     * that pile up at chain throughput rates (54K RPS × 4 dma_copy + 1 kHz
     * keepalive → GB/min). Forwarded to /tmp/dpumesh_dpu_bench.log on DPU,
     * filling /tmp and stalling sshd writes (banner-exchange hang seen). */
    result = doca_dpa_set_log_level(objs->dpa_thread->dpa, DOCA_DPA_DEV_LOG_LEVEL_ERROR);
    if (result != DOCA_SUCCESS) {
    }

    result = doca_dpa_start(objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        goto destroy_dpa;
    }

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
        return result;
    }
    
    result = doca_dpa_thread_set_func_arg(dpa_thread->thread, run_dma_manager, dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    
    result = doca_dpa_thread_start(dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
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
    uint32_t consumer_id;

    memset(msgq, 0, sizeof(*msgq));

    msgq->is_send = attr->is_send;

    if (msgq->pe == NULL) {
        result = doca_pe_create(&msgq->pe);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }

    result = doca_comch_msgq_create(attr->dev, &msgq->msgq);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    
    result = doca_comch_msgq_set_max_num_consumers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_comch_msgq_set_max_num_producers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    
    /* if true, DPA is consumer */
    if (attr->is_send) {
        result = doca_comch_msgq_set_dpa_consumer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    } else {
        /* else, DPA is producer */
        result = doca_comch_msgq_set_dpa_producer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }
    
    result = doca_comch_msgq_start(msgq->msgq);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    
    result = doca_comch_msgq_consumer_create(msgq->msgq, &msgq->consumer);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_comch_consumer_get_id(msgq->consumer, &consumer_id);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    msgq->target_consumer_id = consumer_id;
    
    consumer_ctx = doca_comch_consumer_as_ctx(msgq->consumer);
    /* DPU→DPA direction: must fit the largest message (ADD_RING, NEW_DESC, etc.) */
    result = doca_comch_consumer_set_imm_data_len(msgq->consumer, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        return result;
    }
    
    if (attr->is_send) {
        /* consumer on DPA */
        result = doca_ctx_set_datapath_on_dpa(consumer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_consumer_set_completion(msgq->consumer, attr->consumer_comp, 0);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_consumer_set_dev_max_num_recv(msgq->consumer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    } else {
        /* consumer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(consumer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_ctx_set_state_changed_cb(consumer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, consumer_ctx);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_consumer_task_post_recv_set_conf(msgq->consumer,
                                        dmesh_doca_dpa_msgq_recv_cb,
                                        dmesh_doca_dpa_msgq_recv_error_cb,
                                        attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }

    result = doca_ctx_start(consumer_ctx);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_comch_msgq_producer_create(msgq->msgq, &msgq->producer);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    producer_ctx = doca_comch_producer_as_ctx(msgq->producer);
    if (attr->is_send) {
        /* producer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(producer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_ctx_set_state_changed_cb(producer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, producer_ctx);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_producer_task_send_set_conf(msgq->producer,
                                dmesh_doca_dpa_msgq_send_cb,
                                dmesh_doca_dpa_msgq_send_error_cb,
                                attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    } else {
        /* producer on DPA */
        result = doca_ctx_set_datapath_on_dpa(producer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_producer_set_dev_max_num_send(msgq->producer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        result = doca_comch_producer_dpa_completion_attach(msgq->producer, attr->producer_comp);
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }
    result = doca_ctx_start(producer_ctx);
    if (result != DOCA_SUCCESS) {
        return result;
    }


    if (attr->is_send == false) {
        for (uint32_t idx = 0; idx < attr->max_num_msg; idx++) {
            struct doca_comch_consumer_task_post_recv *recv_task;
            result = doca_comch_consumer_task_post_recv_alloc_init(msgq->consumer, NULL, &recv_task);
            if (result != DOCA_SUCCESS) {
                return result;
            }
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(recv_task));
            if (result != DOCA_SUCCESS) {
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
    doca_error_t result;
    
    (void)dpa_thread;
    memset(comch, 0, sizeof(*comch));

    result = doca_comch_consumer_completion_create(&(comch->consumer_comp));
    if (result != DOCA_SUCCESS) {
        assert(0);
        return result;
    }
    
    result = doca_comch_consumer_completion_set_max_num_recv(comch->consumer_comp,
            CC_DPA_MAX_MSG_NUM);    
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Must match the consumer's imm_data_len (consumer <= completion required by DOCA).
     * DPA actually sends only sizeof(comch_dma_comp_msg) bytes, but the buffer
     * must be large enough for the consumer's configured imm size. */
    result = doca_comch_consumer_completion_set_imm_data_len(comch->consumer_comp, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        return result;
        }
        
    result = doca_comch_consumer_completion_set_dpa_thread(comch->consumer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_comch_consumer_completion_start(comch->consumer_comp);
	if (result != DOCA_SUCCESS) {
		return result;
	}

    result = doca_dpa_completion_create(dpa_thread->dpa, CC_DPA_MAX_MSG_NUM, &comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_dpa_completion_set_thread(comch->producer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_dpa_completion_start(comch->producer_comp);
    if (result != DOCA_SUCCESS) {
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
    uint32_t send_consumer_id;
    uint32_t recv_consumer_id;
    uint32_t dpu_consumer_id;

    result = doca_comch_consumer_completion_get_dpa_handle(comch->consumer_comp, &dpa_consumer_comp);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_dpa_completion_get_dpa_handle(comch->producer_comp, &dpa_producer_comp);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_comch_consumer_get_dpa_handle(comch->send.consumer, &dpa_consumer);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_comch_consumer_get_id(comch->send.consumer, &send_consumer_id);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = doca_comch_consumer_get_id(comch->recv.consumer, &recv_consumer_id);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    dpu_consumer_id = recv_consumer_id;

    if (send_consumer_id != recv_consumer_id) {
    }

    result = doca_comch_producer_get_dpa_handle(comch->recv.producer, &dpa_producer);
    if (result != DOCA_SUCCESS) {
        return result;
    }


    memset(arg, 0, sizeof(*arg));
    arg->dpa_consumer_comp = dpa_consumer_comp;
    arg->dpa_producer_comp = dpa_producer_comp;
    arg->dpa_consumer = dpa_consumer;
    arg->dpa_producer = dpa_producer;
    arg->dpu_consumer_id = dpu_consumer_id;
    arg->num_rings = 0;  /* rings added dynamically via setup_pod_dma */



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
        return result;
    }

    if (rpc_ret != 0) {
        return result;
    }

    result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg, 
                                &arg, sizeof(struct dpa_thread_arg));
    if (result != DOCA_SUCCESS) {
		return result;
	}

    result = doca_dpa_thread_run(dpa_thread->thread);
	if (result != DOCA_SUCCESS) {
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
    void *msg_copy;

	struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;

    msg_copy = malloc(msg_size);
    if (msg_copy == NULL) {
        return DOCA_ERROR_NO_MEMORY;
    }
    memcpy(msg_copy, msg, msg_size);
	result = doca_comch_producer_task_send_alloc_init(msgq->producer,
							  NULL,
                                msg_copy,
							  msg_size,
                              msgq->target_consumer_id,
							  &send_task);
	if (result != DOCA_SUCCESS) {
        free(msg_copy);
		return result;
	}

    task = doca_comch_producer_task_send_as_task(send_task);

    user_data.ptr = msg_copy;
    doca_task_set_user_data(task, user_data);

    int retry = 0;
    const int max_retry = 10000;
    do {
        result = doca_task_submit(task);
        if (result == DOCA_ERROR_AGAIN) {
            doca_pe_progress(msgq->pe);
            retry++;
        }
    } while (result == DOCA_ERROR_AGAIN && retry < max_retry);

	if (result != DOCA_SUCCESS) {
        free(msg_copy);
		doca_task_free(task);
		return result;
	}

	return DOCA_SUCCESS;
}

/* Non-blocking variant: returns DOCA_ERROR_AGAIN immediately on submit
 * failure, no PE progress, no retry. For hot-path DPU→DPA TRIGGER signals
 * where the rev desc is already on the ring and a missed trigger is
 * recoverable by the next successful send. Used by comch_server.c WAKE_DPA
 * forwarder and dpu_worker.c reverse DMA trigger. */
doca_error_t
dmesh_doca_dpa_msgq_send_try(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size)
{
    doca_error_t result;
    union doca_data user_data;
    void *msg_copy;
    struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;

    msg_copy = malloc(msg_size);
    if (msg_copy == NULL)
        return DOCA_ERROR_NO_MEMORY;
    memcpy(msg_copy, msg, msg_size);

    result = doca_comch_producer_task_send_alloc_init(msgq->producer, NULL,
                                                       msg_copy, msg_size,
                                                       msgq->target_consumer_id,
                                                       &send_task);
    if (result != DOCA_SUCCESS) {
        free(msg_copy);
        return result;
    }

    task = doca_comch_producer_task_send_as_task(send_task);
    user_data.ptr = msg_copy;
    doca_task_set_user_data(task, user_data);

    result = doca_task_submit(task);
    if (result != DOCA_SUCCESS) {
        free(msg_copy);
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
    union doca_data user_data;
    void *msg_copy;
    int i;

    for (i = 0; i < num_msg; i++) {
        msg_copy = malloc(msg_size);
        if (msg_copy == NULL) {
            return DOCA_ERROR_NO_MEMORY;
        }
        memcpy(msg_copy, msg, msg_size);
        result = doca_comch_producer_task_send_alloc_init(msgq->producer,
                                  NULL,
                              msg_copy,
                                  msg_size,
							  msgq->target_consumer_id,
                                  &send_task);
        if (result != DOCA_SUCCESS) {
            free(msg_copy);
            return result;
        }
        task = doca_comch_producer_task_send_as_task(send_task);
        user_data.ptr = msg_copy;
        doca_task_set_user_data(task, user_data);
        int retry = 0;
        const int max_retry = 10000;
        do {
            result = doca_task_submit(task);
            if (result == DOCA_ERROR_AGAIN) {
                doca_pe_progress(msgq->pe);
                retry++;
            }
        } while (result == DOCA_ERROR_AGAIN && retry < max_retry);
        if (result != DOCA_SUCCESS) {
            free(msg_copy);
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
        return result;
    }

    result = doca_buf_arr_set_target_dpa(*out_buf_arr, objs->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_set_params(*out_buf_arr, mmap, sizeof(struct dma_desc), 0);
    if (result != DOCA_SUCCESS) {
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_start(*out_buf_arr);
    if (result != DOCA_SUCCESS) {
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
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(pod->remote_mmap, objs->dev, &host_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(pod->local_mmap, objs->dev, &dpu_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Cache the DPA handle for reverse-DMA in-place forwarding: when this
     * pod is the forward sender, dpu_enqueue_reverse_dma writes this handle
     * into desc->mmap so DPA reads from THIS pod's dma_buffer directly. */
    pod->local_mmap_dpa_handle = dpu_mmap;

    ring_info->buf_arr = dpa_buf_arr;
    ring_info->buf_arr_size = DMA_RING_SIZE;
    ring_info->host_mmap = host_mmap;
    ring_info->host_addr = (uint64_t)pod->remote_addr;
    ring_info->host_buf_size = (uint64_t)pod->remote_buf_size;
    ring_info->dpu_mmap = dpu_mmap;
    ring_info->dpu_addr = (uint64_t)pod->dma_buffer;
    ring_info->dpu_buf_size = DPU_BUFFER_SIZE;
    ring_info->pod_id = pod->pod_id;
    /* Phase 3: hdr mmaps. Already resolved against DPU device in
     * comch_common.c process_mmap_msg. If they aren't yet available
     * (mmap_msg arrived after setup_pod_dma) they stay 0, and the
     * forward kernel falls back to body mmap — degraded but safe. */
    ring_info->host_hdr_mmap = pod->remote_hdr_dpa_handle;
    ring_info->host_hdr_rx_mmap = pod->host_hdr_rx_dpa_handle;
    ring_info->host_hdr_addr = (uint64_t)pod->remote_hdr_addr;

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


    /* 1. Create per-pod buf_arr over ring_mmap. Size is DMA_RING_SIZE + 1
     * because host's setup_dma_ring allocates one extra slot at the end
     * for the RX credit counter; DPA reads it via this same buf_arr at
     * index DMA_RING_SIZE (no separate buf_arr needed). */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE + 1, pod->ring_mmap, &pod->buf_arr);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Phase 4: separate buf_arr for hdr_ring_mmap so DPA handles two
     * independent forward rings (body + hdr) per pod. */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE + 1, pod->hdr_ring_mmap, &pod->hdr_buf_arr);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* 2. Allocate local DMA buffer (DPU working buffer) + PCI export */
    result = alloc_buffer_and_set_mmap(&pod->local_mmap, objs->dev,
                                       &pod->dma_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* 3. Export local DMA buffer mmap back to Host */
    result = export_mmap_to_remote(objs, pod->local_mmap, pod->dma_buffer,
                                    DPU_BUFFER_SIZE, DMA_BUFFER, DPU_TO_HOST);
    if (result != DOCA_SUCCESS) {
        /* Non-fatal for now */
    }

    /* 4. Fill DPA ring info for this pod */
    struct dpa_ring_info ring_info;
    result = dmesh_fill_dpa_ring_info(objs, pod, &ring_info);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Phase 4: hdr ring info — same shape as body ring info but with hdr
     * buf_arr / addr. dpu_mmap is shared with body's local_mmap since hdr
     * forward DMA still uses DPU staging (only chunk uses CASE_DIRECT). */
    struct dpa_ring_info hdr_ring_info = ring_info;  /* copy shared fields */
    doca_dpa_dev_buf_arr_t hdr_buf_arr_h = 0;
    result = doca_buf_arr_get_dpa_handle(pod->hdr_buf_arr, &hdr_buf_arr_h);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    hdr_ring_info.buf_arr = hdr_buf_arr_h;
    /* hdr ring's host_mmap is the host's hdr_tx_buffer (Phase 1 pool),
     * already resolved earlier. host_addr same as body's? No — body ring
     * uses pod->remote_addr (body buffer base); hdr ring uses pod->remote_hdr_addr. */
    hdr_ring_info.host_mmap = pod->remote_hdr_dpa_handle;
    hdr_ring_info.host_addr = (uint64_t)pod->remote_hdr_addr;
    hdr_ring_info.host_buf_size = (uint64_t)pod->remote_hdr_buf_size;
    /* hdr ring's host_hdr_mmap is the body's host_mmap fallback — unused
     * for OP_HDR_BATCH on this ring because src_mmap derivation in the
     * forward kernel already uses host_hdr_mmap. */

    /* 5. Update DPA thread arg: write ring info first, then increment num_rings */
    struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_thread;
    struct dpa_thread_arg arg;

    if (!objs->dpa_thread_running) {
        /* First pod: fill shared handles + body ring + hdr ring, write via h2d_memcpy */
        result = dmesh_fill_dpa_thread_arg(objs, &arg);
        if (result != DOCA_SUCCESS)
            return result;
        arg.rings[0] = ring_info;
        arg.rings[1] = hdr_ring_info;
        arg.num_rings = 2;

        result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg,
                                      &arg, sizeof(struct dpa_thread_arg));
        if (result != DOCA_SUCCESS) {
            return result;
        }
    } else {
        /* Subsequent pods: send two ADD_RING messages (body + hdr). */
        struct comch_add_ring_msg add_msg;
        memset(&add_msg, 0, sizeof(add_msg));
        add_msg.type = COMCH_MSG_TYPE_ADD_RING;
        add_msg.ring = ring_info;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &add_msg, sizeof(add_msg));
        if (result != DOCA_SUCCESS) {
            return result;
        }
        memset(&add_msg, 0, sizeof(add_msg));
        add_msg.type = COMCH_MSG_TYPE_ADD_RING;
        add_msg.ring = hdr_ring_info;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &add_msg, sizeof(add_msg));
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }

    /* 6. If first pod, run DPA thread */
    if (!objs->dpa_thread_running) {
        uint64_t rpc_ret;
        uint32_t num_msg = CC_DPA_MAX_MSG_NUM;
        result = doca_dpa_rpc(dpa_thread->dpa, thread_init_rpc, &rpc_ret,
                              arg.dpa_consumer, num_msg);
        if (result != DOCA_SUCCESS) {
            return result;
        }

        result = doca_dpa_thread_run(dpa_thread->thread);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        objs->dpa_thread_running = 1;

        /* Send trigger message to DPA consumer to activate the thread.
         * DPA thread only runs when its attached completion ctx fires.
         * The consumer_comp is attached to the thread, so sending any
         * message via the DPU→DPA msgq triggers the first execution. */
        {
            struct comch_msg trigger;
            memset(&trigger, 0, sizeof(trigger));
            trigger.type = COMCH_MSG_TYPE_TRIGGER;
            result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                               &trigger, sizeof(trigger));
            if (result != DOCA_SUCCESS) {
            } else {
            }
        }
    } else {
    }

    /* === Phase 4: DPU-OWNED forward ring (CPU→host-dst body DMA) === */

    /* Allocate a DPU-local mmap for the ring storage (NO extra credit slot —
     * this ring doesn't need v1.0.0-style FC: the peer's existing forward
     * dma_ring already carries the credit counter at index DMA_RING_SIZE and
     * is shared via the peer table for CASE_DIRECT admission). */
    result = setup_dpu_tx_ring(objs->dev, DMA_RING_SIZE,
                               &pod->dpu_fwd_ring, &pod->dpu_fwd_ring_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE, pod->dpu_fwd_ring_mmap,
                                      &pod->dpu_fwd_buf_arr);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Register the DPU forward ring with DPA. Carries the SAME mmaps as the
     * host-owned forward ring (this pod's body buffer is the SRC; dpu_mmap
     * is a non-zero placeholder for DPA's validation, never used by the
     * CASE_DIRECT path). The only difference: descriptors are written by
     * DPU (dpu_enqueue_body_dma, added in Stage B3), not by host. */
    {
        struct dpa_ring_info dpu_fwd_ring_info = ring_info;
        doca_dpa_dev_buf_arr_t dpu_fwd_buf_arr_h = 0;
        result = doca_buf_arr_get_dpa_handle(pod->dpu_fwd_buf_arr, &dpu_fwd_buf_arr_h);
        if (result != DOCA_SUCCESS) {
            return result;
        }
        dpu_fwd_ring_info.buf_arr      = dpu_fwd_buf_arr_h;
        dpu_fwd_ring_info.buf_arr_size = DMA_RING_SIZE;
        /* host_mmap / host_addr / host_buf_size stay = src body buffer.
         * dpu_mmap / dpu_addr stay = pod local buffer (non-zero validator).
         * host_hdr_* are unused on this ring (body path only); leave whatever
         * ring_info has — DPA's forward kernel only consults host_hdr_mmap
         * when desc->flags & OP_HDR_BATCH, and DPU body enqueues never set
         * that bit on this ring. */

        struct comch_add_ring_msg add_msg;
        memset(&add_msg, 0, sizeof(add_msg));
        add_msg.type = COMCH_MSG_TYPE_ADD_RING;
        add_msg.ring = dpu_fwd_ring_info;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &add_msg, sizeof(add_msg));
        if (result != DOCA_SUCCESS) {
            return result;
        }
    }

    /* === Reverse direction (DPU→CPU) setup === */

    /* 7. Allocate DPU TX buffer for reverse direction */
    result = alloc_buffer_and_set_mmap(&pod->tx_mmap, objs->dev,
                                       &pod->tx_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    pod->tx_buf_size = DPU_BUFFER_SIZE;

    /* 8. Create DPU→CPU descriptor ring */
    result = setup_dpu_tx_ring(objs->dev, DMA_RING_SIZE,
                               &pod->tx_ring, &pod->tx_ring_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* 9. Create buf_arr for reverse ring */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE, pod->tx_ring_mmap, &pod->tx_buf_arr);
    if (result != DOCA_SUCCESS) {
        return result;
    }


    /* 10. Fill reverse ring info and send ADD_REV_RING to DPA.
     * Credit-handle reuses the FORWARD pod->buf_arr — host writes the freed
     * counter to the LAST slot (index DMA_RING_SIZE) of the dma_ring buffer,
     * and DPA polls that slot via the same buf_arr it already uses for
     * forward desc reads. No separate buf_arr needed. */
    {
        struct dpa_ring_info rev_ring_info;
        doca_dpa_dev_buf_arr_t dpa_buf_arr;
        doca_dpa_dev_mmap_t dpu_tx_mmap_h, host_rx_mmap_h;
        doca_dpa_dev_buf_arr_t fwd_buf_arr_h = 0;

        result = doca_buf_arr_get_dpa_handle(pod->tx_buf_arr, &dpa_buf_arr);
        if (result != DOCA_SUCCESS) return result;

        result = doca_mmap_dev_get_dpa_handle(pod->tx_mmap, objs->dev, &dpu_tx_mmap_h);
        if (result != DOCA_SUCCESS) return result;

        /* Use Host RX mmap if available */
        host_rx_mmap_h = 0;
        if (pod->host_rx_mmap) {
            result = doca_mmap_dev_get_dpa_handle(pod->host_rx_mmap, objs->dev, &host_rx_mmap_h);
            if (result != DOCA_SUCCESS) {
            }
        }

        /* Forward buf_arr handle — credit slot is at index DMA_RING_SIZE within it */
        if (pod->buf_arr) {
            result = doca_buf_arr_get_dpa_handle(pod->buf_arr, &fwd_buf_arr_h);
            if (result != DOCA_SUCCESS) {
                fwd_buf_arr_h = 0;
            }
        }

        memset(&rev_ring_info, 0, sizeof(rev_ring_info));
        rev_ring_info.buf_arr = dpa_buf_arr;
        rev_ring_info.buf_arr_size = DMA_RING_SIZE;
        /* For reverse: dpu=source (TX), host=destination (RX) */
        rev_ring_info.dpu_mmap = dpu_tx_mmap_h;
        rev_ring_info.dpu_addr = (uint64_t)pod->tx_buffer;
        rev_ring_info.dpu_buf_size = DPU_BUFFER_SIZE;
        rev_ring_info.host_mmap = host_rx_mmap_h;
        rev_ring_info.host_addr = (uint64_t)pod->host_rx_addr;
        rev_ring_info.host_buf_size = (uint32_t)pod->host_rx_buf_size;
        rev_ring_info.pod_id = pod->pod_id;
        rev_ring_info.host_credit_buf_arr = fwd_buf_arr_h;  /* same buf_arr, slot DMA_RING_SIZE */
        rev_ring_info.rq_depth = pod->rq_depth;
        /* Phase 3: hdr-side mmap for reverse path. dst is the pod that
         * receives hdr forwards, so host_hdr_rx_dpa_handle / addr are the
         * destination of OP_HDR_BATCH reverse DMA. */
        rev_ring_info.host_hdr_rx_mmap = pod->host_hdr_rx_dpa_handle;
        rev_ring_info.host_hdr_addr = (uint64_t)pod->host_hdr_rx_addr;

        struct comch_add_rev_ring_msg rev_msg;
        memset(&rev_msg, 0, sizeof(rev_msg));
        rev_msg.type = COMCH_MSG_TYPE_ADD_REV_RING;
        rev_msg.ring = rev_ring_info;

        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &rev_msg, sizeof(rev_msg));
        if (result != DOCA_SUCCESS) {
        } else {
        }

        /* Phase 4: register this pod's host_rx_mmap with the DPA peer table
         * so any other pod can DMA directly into it via CASE_DIRECT.
         * Also wire the v1.0.0-style credit return: peer's forward dma_ring
         * (fwd_buf_arr_h) carries a credit counter at slot DMA_RING_SIZE
         * that the peer's host increments on rx_free. DPA uses it to gate
         * direct DMAs against the peer's rx_dma_buffer cap. */
        struct comch_add_peer_msg ap_msg;
        memset(&ap_msg, 0, sizeof(ap_msg));
        ap_msg.type = COMCH_MSG_TYPE_ADD_PEER;
        ap_msg.peer.pod_id = pod->pod_id;
        ap_msg.peer.rx_mmap = host_rx_mmap_h;
        ap_msg.peer.rx_addr = (uint64_t)pod->host_rx_addr;
        ap_msg.peer.rx_buf_size = pod->host_rx_buf_size;
        ap_msg.peer.credit_buf_arr = fwd_buf_arr_h;
        ap_msg.peer.rq_depth = pod->rq_depth;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &ap_msg, sizeof(ap_msg));
        if (result != DOCA_SUCCESS) {
        } else {
        }
    }

    /* Initialize DPU-internal write cursor for reverse DMA. DPU is a pure
     * forwarder; this only chooses the next physical write offset. */
    pod->tx_producer_head = 0;

    pod->dma_ready = 1;
    return DOCA_SUCCESS;
}

/*
 * Update the DPA reverse ring's host_rx_mmap after it arrives late.
 * Sends ADD_REV_RING with updated host mmap handle so DPA can
 * actually perform DPU→CPU DMA (previously host_mmap was 0).
 */
doca_error_t
update_rev_ring_host_rx(struct objects *objs, struct pod_state *pod)
{
    doca_error_t result;

    if (!pod->host_rx_mmap || !pod->tx_buf_arr) {
        return DOCA_ERROR_NOT_FOUND;
    }

    struct dpa_ring_info rev_ring_info;
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    doca_dpa_dev_mmap_t dpu_tx_mmap_h, host_rx_mmap_h;
    doca_dpa_dev_buf_arr_t credit_buf_arr_h = 0;

    result = doca_buf_arr_get_dpa_handle(pod->tx_buf_arr, &dpa_buf_arr);
    if (result != DOCA_SUCCESS) return result;

    result = doca_mmap_dev_get_dpa_handle(pod->tx_mmap, objs->dev, &dpu_tx_mmap_h);
    if (result != DOCA_SUCCESS) return result;

    result = doca_mmap_dev_get_dpa_handle(pod->host_rx_mmap, objs->dev, &host_rx_mmap_h);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* Forward buf_arr handle — credit slot is at index DMA_RING_SIZE within it */
    if (pod->buf_arr) {
        result = doca_buf_arr_get_dpa_handle(pod->buf_arr, &credit_buf_arr_h);
        if (result != DOCA_SUCCESS) {
            credit_buf_arr_h = 0;
        }
    }

    memset(&rev_ring_info, 0, sizeof(rev_ring_info));
    rev_ring_info.buf_arr = dpa_buf_arr;
    rev_ring_info.buf_arr_size = DMA_RING_SIZE;
    rev_ring_info.dpu_mmap = dpu_tx_mmap_h;
    rev_ring_info.dpu_addr = (uint64_t)pod->tx_buffer;
    rev_ring_info.dpu_buf_size = DPU_BUFFER_SIZE;
    rev_ring_info.host_mmap = host_rx_mmap_h;
    rev_ring_info.host_addr = (uint64_t)pod->host_rx_addr;
    rev_ring_info.host_buf_size = (uint32_t)pod->host_rx_buf_size;
    rev_ring_info.pod_id = pod->pod_id;
    rev_ring_info.host_credit_buf_arr = credit_buf_arr_h;
    rev_ring_info.rq_depth = pod->rq_depth;
    /* Phase 3: hdr-side mmap for reverse path. */
    rev_ring_info.host_hdr_rx_mmap = pod->host_hdr_rx_dpa_handle;
    rev_ring_info.host_hdr_addr = (uint64_t)pod->host_hdr_rx_addr;

    struct comch_add_rev_ring_msg rev_msg;
    memset(&rev_msg, 0, sizeof(rev_msg));
    rev_msg.type = COMCH_MSG_TYPE_ADD_REV_RING;
    rev_msg.ring = rev_ring_info;

    result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                       &rev_msg, sizeof(rev_msg));
    if (result != DOCA_SUCCESS) {
        return result;
    }

    return DOCA_SUCCESS;
}

#endif /* DOCA_ARCH_DPU */