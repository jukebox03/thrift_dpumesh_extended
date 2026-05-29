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

	struct objects *objs = ctx_user_data.ptr;
	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);

    data_len = doca_comch_consumer_task_post_recv_get_imm_data_len(recv_task);

    /* DPA sends comch_dma_comp_msg directly (<=32 bytes) rather than the full
     * comch_msg union, so read raw bytes and dispatch by the leading type field. */
    uint8_t *raw = (uint8_t *)doca_comch_consumer_task_post_recv_get_imm_data(recv_task);

    if (raw == NULL) {
        DOCA_LOG_ERR("DPA MsgQ recv callback entered with NULL imm data (len=%u)", data_len);
        goto resubmit_recv_task;
    }

    /* Type field is the first byte (uint8_t in the packed comch_dma_comp_msg).
     * Any imm payload of ours has at least the 1-byte type at offset 0. */
    if (data_len < 1) {
        DOCA_LOG_ERR("DPA MsgQ recv: imm data too short for type field (len=%u)", data_len);
        goto resubmit_recv_task;
    }

    enum comch_msg_type msg_type = (enum comch_msg_type)raw[0];

    switch (msg_type) {
        case COMCH_MSG_TYPE_DMA_COMPLETED: {
            if (data_len < sizeof(struct comch_dma_comp_msg)) {
                DOCA_LOG_ERR("DPA MsgQ recv: DMA_COMPLETED too short (len=%u, need=%zu)",
                             data_len, sizeof(struct comch_dma_comp_msg));
                break;
            }
            struct comch_dma_comp_msg *comp_msg = (struct comch_dma_comp_msg *)raw;
            int32_t src_pod_id = comp_msg->src_pod_id;
            int32_t dst_pod_id = comp_msg->dst_pod_id;
            uint32_t req_id = comp_msg->req_id;

            /* Find the source pod's local DMA buffer for data */
            struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);
            if (!src_pod || !src_pod->dma_buffer) {
                DOCA_LOG_ERR("DMA completed but src_pod %d not found or no buffer", src_pod_id);
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

            /* Zero-copy: record buffer offset instead of heap-copying.
             * End-node slot-based admission keeps in-flight bytes ≤ buf_size
             * so DPA cannot lap unconsumed data. */
            entry.buf_offset = body_offset;
            /* src_pod was already resolved above via find_pod_by_id (ACQUIRE-
             * gated); derive its index directly instead of an unguarded re-scan
             * of pods[] (which could observe a half-published slot and runs on
             * the per-RTT hot path). */
            entry.pod_idx = (int)(src_pod - objs->pods);

            if (comp_queue_enqueue(&objs->comp_queue, &entry) != 0) {
                DOCA_LOG_ERR("Completion queue full, dropping req_id=%u (src=%d, dst=%d)",
                             req_id, src_pod_id, dst_pod_id);
                /* zero-copy: no heap data to free */
            }
            break;
        }
        case COMCH_MSG_TYPE_REV_DMA_COMPLETED: {
            /* Reverse DMA completed (DPU→CPU): DPA has DMA'd data from DPU TX
             * buffer to Host RX buffer. Enqueue for DPU worker to forward
             * completion notification to the destination Host pod via comch. */
            if (data_len < sizeof(struct comch_dma_comp_msg)) {
                DOCA_LOG_ERR("REV_DMA_COMPLETED too short (len=%u, need=%zu)",
                             data_len, sizeof(struct comch_dma_comp_msg));
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

            if (comp_queue_enqueue(&objs->comp_queue, &rev_entry) != 0) {
                DOCA_LOG_ERR("Completion queue full, dropping REV_DMA req_id=%u",
                             rev_comp->req_id);
            }
            break;
        }
        case COMCH_MSG_TYPE_TRIGGER:
            break;
        default:
            DOCA_LOG_ERR("Received unknown message type: %u", msg_type);
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
                DOCA_LOG_WARN("DPA MsgQ recv resubmit failed: %s; deferred",
                              doca_error_get_name(result));
            } else {
                DOCA_LOG_ERR("DPA MsgQ recv resubmit failed and deferred list full: %s",
                             doca_error_get_name(result));
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

	DOCA_LOG_ERR("DPA MsgQ recv ERROR callback #%lu: status=%s(%d)",
	             recv_err_count, doca_error_get_descr(status), (int)status);

	/* Resubmit to keep the recv task alive — do not free. */
	doca_error_t resubmit = doca_task_submit(task);
	if (resubmit != DOCA_SUCCESS) {
		DOCA_LOG_ERR("DPA MsgQ recv resubmit after error failed: %s", doca_error_get_name(resubmit));
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
    DOCA_LOG_ERR("Failed to send msg");
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
        DOCA_LOG_ERR("DPA comch msgQ state is idle.");
		break;
    case DOCA_CTX_STATE_STARTING:
        DOCA_LOG_INFO("DPA comch msgQ state is starting.");
        break;
    case DOCA_CTX_STATE_RUNNING:
        DOCA_LOG_INFO("DPA comch msgQ ctx RUNNING.");
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

    /* DPA log level kept at ERROR. INFO produces per-DMA / per-trigger lines
     * that pile up at chain throughput rates (54K RPS × 4 dma_copy + 1 kHz
     * keepalive → GB/min). Forwarded to /tmp/dpumesh_dpu_bench.log on DPU,
     * filling /tmp and stalling sshd writes (banner-exchange hang seen). */
    result = doca_dpa_set_log_level(objs->dpa_thread->dpa, DOCA_DPA_DEV_LOG_LEVEL_ERROR);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("Failed to set DPA log level: %s", doca_error_get_name(result));
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
    uint32_t consumer_id;

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

    result = doca_comch_consumer_get_id(msgq->consumer, &consumer_id);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get msgq consumer id - %s",
                doca_error_get_name(result));
        return result;
    }
    msgq->target_consumer_id = consumer_id;
    DOCA_LOG_INFO("[PAIRCHK] msgq_create begin: is_send=%d target_consumer_id=%u max_num_msg=%u",
                  (int)attr->is_send, msgq->target_consumer_id, attr->max_num_msg);
    
    consumer_ctx = doca_comch_consumer_as_ctx(msgq->consumer);
    /* DPU→DPA direction: must fit the largest message (ADD_RING, NEW_DESC, etc.) */
    result = doca_comch_consumer_set_imm_data_len(msgq->consumer, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len to %zu - %s",
                sizeof(struct comch_msg), doca_error_get_name(result));
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
        DOCA_LOG_INFO("[PAIRCHK] msgq_create(is_send=1): consumer completion attached: consumer=%p consumer_comp=%p",
                  (void *)msgq->consumer, (void *)attr->consumer_comp);
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
        DOCA_LOG_INFO("[PAIRCHK] msgq_create(is_send=0): producer completion attached: producer=%p producer_comp=%p",
                      (void *)msgq->producer, (void *)attr->producer_comp);
    }
    result = doca_ctx_start(producer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer ctx - %s",
                doca_error_get_name(result));
        return result;
    }

    DOCA_LOG_INFO("[PAIRCHK] msgq_create done: is_send=%d consumer=%p producer=%p target_consumer_id=%u",
                  (int)attr->is_send, (void *)msgq->consumer, (void *)msgq->producer,
                  msgq->target_consumer_id);

    if (attr->is_send == false) {
        for (uint32_t idx = 0; idx < attr->max_num_msg; idx++) {
            struct doca_comch_consumer_task_post_recv *recv_task;
            result = doca_comch_consumer_task_post_recv_alloc_init(msgq->consumer, NULL, &recv_task);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to alloc recv task at idx=%u: %s", idx, doca_error_get_name(result));
                return result;
            }
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(recv_task));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit recv task at idx=%u: %s", idx, doca_error_get_name(result));
                return result;
            }
        }
        DOCA_LOG_INFO("msgq_create(is_send=0): pre-posted %u recv tasks", attr->max_num_msg);
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

    /* Must match the consumer's imm_data_len (consumer <= completion required by DOCA).
     * DPA actually sends only sizeof(comch_dma_comp_msg) bytes, but the buffer
     * must be large enough for the consumer's configured imm size. */
    result = doca_comch_consumer_completion_set_imm_data_len(comch->consumer_comp, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set completion imm data len to %zu - %s",
            sizeof(struct comch_msg),
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
    uint32_t send_consumer_id;
    uint32_t recv_consumer_id;
    uint32_t dpu_consumer_id;

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
    result = doca_comch_consumer_get_id(comch->send.consumer, &send_consumer_id);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get send.consumer ID: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_comch_consumer_get_id(comch->recv.consumer, &recv_consumer_id);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get recv.consumer ID: %s", doca_error_get_name(result));
        return result;
    }
    dpu_consumer_id = recv_consumer_id;

    if (send_consumer_id != recv_consumer_id) {
        DOCA_LOG_INFO("DPA MsgQ consumer IDs differ: send.consumer=%u recv.consumer=%u (using recv.consumer)",
                      send_consumer_id, recv_consumer_id);
    }

    result = doca_comch_producer_get_dpa_handle(comch->recv.producer, &dpa_producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer DPA handle: %s", doca_error_get_name(result));
        return result;
    }

    DOCA_LOG_INFO("[PAIRCHK] fill_arg handles: send.consumer=%p recv.consumer=%p recv.producer=%p producer_comp=%p",
                  (void *)comch->send.consumer, (void *)comch->recv.consumer,
                  (void *)comch->recv.producer, (void *)comch->producer_comp);

    memset(arg, 0, sizeof(*arg));
    arg->dpa_consumer_comp = dpa_consumer_comp;
    arg->dpa_producer_comp = dpa_producer_comp;
    arg->dpa_consumer = dpa_consumer;
    arg->dpa_producer = dpa_producer;
    arg->dpu_consumer_id = dpu_consumer_id;
    arg->num_rings = 0;  /* rings added dynamically via setup_pod_dma */

    DOCA_LOG_INFO("DPA thread arg: consumer_comp=0x%lx, producer_comp=0x%lx, consumer=0x%lx, producer=0x%lx, dpu_consumer_id=%u (send.consumer=%u recv.consumer=%u)",
        arg->dpa_consumer_comp, arg->dpa_producer_comp,
        arg->dpa_consumer, arg->dpa_producer, arg->dpu_consumer_id,
        send_consumer_id, recv_consumer_id);

    DOCA_LOG_INFO("[PAIRCHK] fill_arg ids: send_consumer_id=%u recv_consumer_id=%u dpu_consumer_id=%u",
                  send_consumer_id, recv_consumer_id, dpu_consumer_id);

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
    DOCA_LOG_INFO("[PAIRCHK] run_dpa_thread pre-rpc: arg.consumer=0x%lx arg.producer=0x%lx arg.consumer_comp=0x%lx arg.producer_comp=0x%lx consumer_id=%u",
                  arg.dpa_consumer, arg.dpa_producer,
                  arg.dpa_consumer_comp, arg.dpa_producer_comp, arg.dpu_consumer_id);
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
    void *msg_copy;

	struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;

    msg_copy = malloc(msg_size);
    if (msg_copy == NULL) {
        DOCA_LOG_ERR("DPA MsgQ send failed: payload copy allocation failed");
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
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
			     doca_error_get_name(result));
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
		DOCA_LOG_ERR("DPA MsgQ send failed: %s (retries=%d, msg_size=%u)",
			     doca_error_get_name(result), retry, msg_size);
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
            DOCA_LOG_ERR("DPA MsgQ bulk send failed: payload copy allocation failed at idx=%d", i);
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
            DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
                     doca_error_get_name(result));
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
            DOCA_LOG_ERR("DPA MsgQ bulk send failed: %s (retries=%d, msg_size=%u, idx=%d)",
                     doca_error_get_name(result), retry, msg_size, i);
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

    /* 1. Create per-pod buf_arr over ring_mmap. Size is DMA_RING_SIZE + 1
     * because host's setup_dma_ring allocates one extra slot at the end
     * for the RX credit counter; DPA reads it via this same buf_arr at
     * index DMA_RING_SIZE (no separate buf_arr needed). */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE + 1, pod->ring_mmap, &pod->buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: buf_arr failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }

    /* 2. Allocate local DMA buffer (DPU working buffer) + PCI export */
    result = alloc_buffer_and_set_mmap(&pod->local_mmap, objs->dev,
                                       &pod->dma_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
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
    struct dpa_thread_arg arg;

    if (!objs->dpa_thread_running) {
        /* First pod: fill shared handles + first ring, write via h2d_memcpy */
        result = dmesh_fill_dpa_thread_arg(objs, &arg);
        if (result != DOCA_SUCCESS)
            return result;
        arg.rings[0] = ring_info;
        arg.num_rings = 1;

        result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg,
                                      &arg, sizeof(struct dpa_thread_arg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: h2d_memcpy failed: %s", doca_error_get_descr(result));
            return result;
        }
    } else {
        /* Subsequent pods: send ADD_RING message via comch msgq to DPA thread.
         * This avoids DPA local memory cache coherency issues with h2d_memcpy
         * — the DPA thread updates its own data structures directly. */
        struct comch_add_ring_msg add_msg;
        memset(&add_msg, 0, sizeof(add_msg));
        add_msg.type = COMCH_MSG_TYPE_ADD_RING;
        add_msg.ring = ring_info;

        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &add_msg, sizeof(add_msg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: send ADD_RING to DPA failed: %s",
                         doca_error_get_descr(result));
            return result;
        }
        DOCA_LOG_INFO("Sent ADD_RING to DPA for pod_id=%d", pod->pod_id);
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
            memset(&trigger, 0, sizeof(trigger));
            trigger.type = COMCH_MSG_TYPE_TRIGGER;
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
        DOCA_LOG_INFO("Sent ADD_RING msg to DPA for pod_id=%d", pod->pod_id);
    }

    /* === Reverse direction (DPU→CPU) setup === */

    /* 7. Allocate DPU TX buffer for reverse direction */
    result = alloc_buffer_and_set_mmap(&pod->tx_mmap, objs->dev,
                                       &pod->tx_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: alloc TX buffer failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }
    pod->tx_buf_size = DPU_BUFFER_SIZE;

    /* 8. Create DPU→CPU descriptor ring */
    result = setup_dpu_tx_ring(objs->dev, DMA_RING_SIZE,
                               &pod->tx_ring, &pod->tx_ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: TX ring failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }

    /* 9. Create buf_arr for reverse ring */
    result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE, pod->tx_ring_mmap, &pod->tx_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: TX buf_arr failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
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
                DOCA_LOG_WARN("setup_pod_dma: host_rx_mmap DPA handle failed: %s",
                              doca_error_get_descr(result));
            }
        }

        /* Forward buf_arr handle — credit slot is at index DMA_RING_SIZE within it */
        if (pod->buf_arr) {
            result = doca_buf_arr_get_dpa_handle(pod->buf_arr, &fwd_buf_arr_h);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_WARN("setup_pod_dma: fwd buf_arr DPA handle failed: %s",
                              doca_error_get_descr(result));
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

        struct comch_add_rev_ring_msg rev_msg;
        memset(&rev_msg, 0, sizeof(rev_msg));
        rev_msg.type = COMCH_MSG_TYPE_ADD_REV_RING;
        rev_msg.ring = rev_ring_info;

        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                           &rev_msg, sizeof(rev_msg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_WARN("setup_pod_dma: send ADD_REV_RING failed: %s",
                          doca_error_get_descr(result));
        } else {
            DOCA_LOG_INFO("Sent ADD_REV_RING to DPA for pod_id=%d (rq_depth=%u, credit_at_slot=%d)",
                          pod->pod_id, pod->rq_depth, DMA_RING_SIZE);
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
        DOCA_LOG_WARN("update_rev_ring_host_rx: missing mmap or buf_arr for pod %d", pod->pod_id);
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
        DOCA_LOG_ERR("update_rev_ring_host_rx: host_rx_mmap DPA handle failed: %s",
                      doca_error_get_descr(result));
        return result;
    }

    /* Forward buf_arr handle — credit slot is at index DMA_RING_SIZE within it */
    if (pod->buf_arr) {
        result = doca_buf_arr_get_dpa_handle(pod->buf_arr, &credit_buf_arr_h);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_WARN("update_rev_ring_host_rx: fwd buf_arr DPA handle failed: %s",
                          doca_error_get_descr(result));
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

    struct comch_add_rev_ring_msg rev_msg;
    memset(&rev_msg, 0, sizeof(rev_msg));
    rev_msg.type = COMCH_MSG_TYPE_ADD_REV_RING;
    rev_msg.ring = rev_ring_info;

    result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                                       &rev_msg, sizeof(rev_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("update_rev_ring_host_rx: send ADD_REV_RING failed: %s",
                      doca_error_get_descr(result));
        return result;
    }

    DOCA_LOG_INFO("Updated DPA reverse ring with Host RX mmap for pod %d", pod->pod_id);
    return DOCA_SUCCESS;
}

#endif /* DOCA_ARCH_DPU */