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
#include "comch_consumer.h"
#include "../dpumesh.h"
#include "ring.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <time.h>

DOCA_LOG_REGISTER(DPA);

#ifdef DOCA_ARCH_DPU
/* Kernel function declaration (resolved from dpa_program.a stubs, DPU only) */
extern doca_dpa_func_t run_dma_manager;
extern doca_dpa_func_t thread_init_rpc;

extern struct doca_dpa_app *DPU_mesh_dpa_app;
#endif

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

    enum dpa_msg_type msg_type = (enum dpa_msg_type)raw[0];

    switch (msg_type) {
        case DPA_MSG_FWD_DONE: {
            if (data_len < sizeof(struct comch_dma_comp_msg)) {
                DOCA_LOG_ERR("DPA MsgQ recv: DMA_COMPLETED too short (len=%u, need=%zu)",
                             data_len, sizeof(struct comch_dma_comp_msg));
                break;
            }
            struct comch_dma_comp_msg *comp_msg = (struct comch_dma_comp_msg *)raw;
            int32_t src_pod_id = comp_msg->src_pod_id;
            int32_t dst_pod_id = comp_msg->dst_pod_id;   /* may be DMESH_POD_BLANK → resolve */
            uint16_t seq = comp_msg->seq;

            /* Find the source pod's local DMA buffer. pod_data_ready ACQUIRE-loads
             * dma_ready so the dma_buffer/handle reads below see the
             * RELEASE-published setup fields. */
            struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);
            if (!src_pod || !pod_data_ready(src_pod) || !src_pod->dma_buffer) {
                DOCA_LOG_ERR("DMA completed but src_pod %d not found or not ready", src_pod_id);
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
            entry.src_service = (int16_t)src_pod->service_id;  /* derived (not on 16B wire) */
            entry.dst_service = comp_msg->dst_service;
            entry.src_port = comp_msg->src_port;
            entry.dst_port = comp_msg->dst_port;
            entry.seq = seq;
            entry.length = payload_len;
            entry.route_group = comp_msg->route_group;  /* forward route-affinity key (0 = normal LB) */

            /* Zero-copy: record buffer offset instead of heap-copying.
             * End-node slot-based admission keeps in-flight bytes ≤ buf_size
             * so DPA cannot lap unconsumed data. */
            entry.buf_offset = body_offset;
            /* Derive src_pod index directly from the ACQUIRE-gated pointer
             * resolved above (avoids an unguarded re-scan of pods[]). */
            entry.pod_idx = (int)(src_pod - objs->pods);

            if (ingest_push(objs, &entry) != 0) {
                DOCA_LOG_ERR("Completion queue full, dropping seq=%u (src=%d, dst=%d)",
                             seq, src_pod_id, dst_pod_id);
                /* zero-copy: no heap data to free */
            }
            break;
        }
        case DPA_MSG_REV_DONE: {
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
            struct pod_state *rev_src = find_pod_by_id(objs, rev_comp->src_pod_id);
            rev_entry.src_service = rev_src ? (int16_t)rev_src->service_id : (int16_t)DMESH_SVC_NONE;
            rev_entry.dst_service = rev_comp->dst_service;
            rev_entry.src_port = rev_comp->src_port;
            rev_entry.dst_port = rev_comp->dst_port;
            rev_entry.seq = rev_comp->seq;
            rev_entry.length = rev_comp->length;
            rev_entry.buf_offset = rev_comp->pos;  /* position in Host RX buffer */
            rev_entry.pod_idx = -1;

            if (ingest_push(objs, &rev_entry) != 0) {
                DOCA_LOG_ERR("Completion queue full, dropping REV_DMA seq=%u",
                             rev_comp->seq);
            }
            break;
        }
        case DPA_MSG_WAKE:
            break;
        default:
            DOCA_LOG_ERR("Received unknown message type: %u", msg_type);
            break;
    }

resubmit_recv_task:
    /* Backpressure: if comp_queue is nearly full, defer recv task resubmission
     * so DPA sees consumer_empty and pauses; main loop resubmits when the queue
     * drops below BP_LOW. On submit failure also stash for the main loop to
     * retry rather than losing the task. */
    if (ingest_usage(objs) >= COMP_QUEUE_BP_HIGH &&
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

	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);
	doca_error_t status = doca_task_get_status(task);

	DOCA_LOG_ERR("DPA MsgQ recv ERROR callback: status=%s(%d)",
	             doca_error_get_descr(status), (int)status);

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
	(void)ctx_user_data;

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
	(void)ctx;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
        DOCA_LOG_ERR("DPA comch msgQ state is idle.");
		break;
    case DOCA_CTX_STATE_STARTING:
        DOCA_LOG_INFO("DPA comch msgQ state is starting.");
        break;
    case DOCA_CTX_STATE_RUNNING:
        DOCA_LOG_INFO("DPA comch msgQ ctx RUNNING.");
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

    /* Resolve N = number of DPA EU threads (multi-EU data plane),
     * clamped to [1, MAX_DPA_RINGS]. */
    if (objs->num_dpa_threads <= 0) {
        /* Default 4 = the measured production config (2-pod pair × K=2 rings = 4
         * active EUs). test-bench.sh sets DPUMESH_DPA_THREADS explicitly, so this
         * default only governs a direct (non-deploy) invocation — it just keeps
         * that off the single-EU (~74K) floor. */
        int n = 4;
        const char *env = getenv("DPUMESH_DPA_THREADS");
        if (env && *env) {
            n = atoi(env);
            if (n < 1) n = 1;
            if (n > MAX_DPA_RINGS) n = MAX_DPA_RINGS;
        }
        objs->num_dpa_threads = n;
    }
    /* Resolve K = rings per pod (EU-sharding), clamped to [1, num_dpa_threads]:
     * a pod cannot spread across more EUs than exist. */
    if (objs->k_rings <= 0) {
        int k = DPUMESH_RINGS_PER_POD_DEFAULT;
        const char *kenv = getenv("DPUMESH_RINGS_PER_POD");
        if (kenv && *kenv) {
            k = atoi(kenv);
            if (k < 1) k = 1;
        }
        if (k > objs->num_dpa_threads) k = objs->num_dpa_threads;
        if (k > MAX_EU_PER_POD) k = MAX_EU_PER_POD;
        objs->k_rings = k;
    }
    DOCA_LOG_INFO("DPA multi-EU: num_dpa_threads=%d k_rings=%d (MAX_DPA_RINGS=%d)",
                  objs->num_dpa_threads, objs->k_rings, MAX_DPA_RINGS);

    for (int k = 0; k < objs->num_dpa_threads; k++) {
        if (!objs->dpa_threads[k]) {
            objs->dpa_threads[k] = calloc(1, sizeof(struct dmesh_doca_dpa_thread));
            if (!objs->dpa_threads[k]) {
                DOCA_LOG_ERR("Failed to allocate memory for dpa_threads[%d]", k);
                return DOCA_ERROR_NO_MEMORY;
            }
        }
        if (!objs->dpa_comches[k]) {
            objs->dpa_comches[k] = calloc(1, sizeof(struct dmesh_doca_dpa_comch));
            if (!objs->dpa_comches[k]) {
                DOCA_LOG_ERR("Failed to allocate memory for dpa_comches[%d]", k);
                return DOCA_ERROR_NO_MEMORY;
            }
        }
    }

    /* One DPA device shared by all EU threads (handles are device-level). */
    result = doca_dpa_create(objs->dev, &objs->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DPA with error = %s", doca_error_get_name(result));
        return result;
    }

    result = doca_dpa_set_app(objs->dpa, DPU_mesh_dpa_app);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA application with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    /* DPA log level kept at ERROR: INFO emits per-DMA / per-trigger lines that
     * flood the DPU log on the hot path. */
    result = doca_dpa_set_log_level(objs->dpa, DOCA_DPA_DEV_LOG_LEVEL_ERROR);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_WARN("Failed to set DPA log level: %s", doca_error_get_name(result));
    }

    result = doca_dpa_start(objs->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DOCA DPA with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    /* Point every EU thread struct at the shared device. */
    for (int k = 0; k < objs->num_dpa_threads; k++)
        objs->dpa_threads[k]->dpa = objs->dpa;

    DOCA_LOG_INFO("Init DOCA DPA done.");
    return DOCA_SUCCESS;

destroy_dpa:
    doca_dpa_destroy(objs->dpa);
    objs->dpa = NULL;
    return result;
}

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread, int eu_id)
{
    doca_error_t result;

    result = doca_dpa_mem_alloc(dpa_thread->dpa, sizeof(struct dpa_thread_arg), &dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to alloc dpa mem: %s",
            doca_error_get_descr(result));
        return result;
    }

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

    /* Pin this thread to a distinct EU so N threads land on N distinct EUs.
     * eu_id < 0 = leave placement relaxed. Affinity must be set between create
     * and start. The affinity object is intentionally NOT destroyed (lives for
     * the process lifetime) to avoid a use-after-free if the SDK references it
     * past thread_start. */
    if (eu_id >= 0) {
        struct doca_dpa_eu_affinity *affinity = NULL;
        doca_error_t ar = doca_dpa_eu_affinity_create(dpa_thread->dpa, &affinity);
        if (ar == DOCA_SUCCESS) {
            ar = doca_dpa_eu_affinity_set(affinity, (unsigned int)eu_id);
            if (ar == DOCA_SUCCESS)
                ar = doca_dpa_thread_set_affinity(dpa_thread->thread, affinity);
        }
        if (ar != DOCA_SUCCESS)
            DOCA_LOG_WARN("EU affinity for eu_id=%d failed: %s (continuing relaxed)",
                          eu_id, doca_error_get_descr(ar));
        else
            DOCA_LOG_INFO("DPA thread pinned to EU %d", eu_id);
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
dmesh_doca_dpa_comch_create(struct objects *objs, int idx)
{
    struct dmesh_doca_dpa_comch *comch = objs->dpa_comches[idx];
    struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_threads[idx];
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
 * Fill shared comch (consumer/producer) DPA handles into the DPA thread arg.
 * No ring info is set here — rings are added dynamically per-pod via
 * setup_pod_dma (arg->num_rings starts at 0).
 *
 * @arg [out]: the thread argument that is filled in
 * @return: DOCA_SUCCESS on success, a DOCA error otherwise
 */
static doca_error_t
dmesh_fill_dpa_thread_arg(struct objects *objs, int idx, struct dpa_thread_arg *arg)
{
    doca_error_t result;
    struct dmesh_doca_dpa_comch *comch = objs->dpa_comches[idx];
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

    memset(arg, 0, sizeof(*arg));
    arg->eu_index = (uint32_t)idx;   /* selects this EU's row in the per-EU admission globals */
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

    return DOCA_SUCCESS;
}

/*
 * Send message to DPA over the DOCA Comch MsgQ
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
		DOCA_LOG_ERR("DPA MsgQ send failed: failed to allocate send task - %s",
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
 * failure, no PE progress, no retry. For hot-path DPU→DPA wake signals where
 * a missed trigger is recoverable by the next successful send. */
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

    result = doca_buf_arr_set_target_dpa(*out_buf_arr, objs->dpa);
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

/*
 * Fill ring info for a specific pod.
 */
static doca_error_t
dmesh_fill_dpa_ring_info(struct objects *objs, struct pod_state *pod, int j,
                         uint64_t dpu_region, struct dpa_ring_info *ring_info)
{
    doca_error_t result;
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    doca_dpa_dev_mmap_t host_mmap, dpu_mmap;

    /* Forward ring j reads its own descriptor ring (buf_arrs[j]) but shares the
     * host TX data mmap (descriptors carry absolute addrs) and writes into the
     * pod's DPU staging at the disjoint region [j*dpu_region, (j+1)*dpu_region)
     * so K EUs never collide. region_off makes the reported pos absolute. */
    result = doca_buf_arr_get_dpa_handle(pod->buf_arrs[j], &dpa_buf_arr);
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
    ring_info->dpu_addr = (uint64_t)pod->dma_buffer + (uint64_t)j * dpu_region;
    ring_info->dpu_buf_size = (uint32_t)dpu_region;
    ring_info->region_off = (uint32_t)((uint64_t)j * dpu_region);
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

    int N = objs->num_dpa_threads;
    int K = objs->k_rings > 0 ? objs->k_rings : 1;
    if (K > N) K = N;
    pod->k_rings = K;
    uint64_t dpu_region = (uint64_t)DPU_BUFFER_SIZE / (uint64_t)K;

    /* 1. One DPU staging buffer shared by the K forward rings, partitioned into
     * K disjoint regions of dpu_region bytes (forward ring j writes region j) +
     * export to host. */
    result = alloc_buffer_and_set_mmap(&pod->local_mmap, objs->dev,
                                       &pod->dma_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("setup_pod_dma: alloc buffer failed for pod %d: %s",
                     pod->pod_id, doca_error_get_descr(result));
        return result;
    }
    /* (The per-pod DPU staging buffer is NOT exported to the host: the host never
     * reads it — its reverse path lands into its own rx_dma_buffer. The old
     * DPU_TO_HOST export was dead, and misrouted to the first connection when
     * >1 pod was registered; removed.) */

    /* 2. Forward rings: ring j -> EU k_j = (pod_id*K + j) % N. K consecutive EUs
     * per pod; consecutive pods land on disjoint EU sets. The FIRST ring on each
     * EU does h2d_memcpy + thread_run + WAKE; later rings (this pod or others)
     * send ADD_RING. */
    for (int j = 0; j < K; j++) {
        int k_j = (pod->pod_id * K + j) % N;
        result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE + 1, pod->ring_mmaps[j], &pod->buf_arrs[j]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: buf_arr[%d] failed for pod %d: %s",
                         j, pod->pod_id, doca_error_get_descr(result));
            return result;
        }

        struct dpa_ring_info ring_info;
        result = dmesh_fill_dpa_ring_info(objs, pod, j, dpu_region, &ring_info);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: fill ring info[%d] failed for pod %d: %s",
                         j, pod->pod_id, doca_error_get_descr(result));
            return result;
        }

        struct dmesh_doca_dpa_thread *dpa_thread = objs->dpa_threads[k_j];
        if (!objs->dpa_thread_running[k_j]) {
            struct dpa_thread_arg arg;
            result = dmesh_fill_dpa_thread_arg(objs, k_j, &arg);
            if (result != DOCA_SUCCESS)
                return result;
            arg.rings[0] = ring_info;
            arg.num_rings = 1;
            result = doca_dpa_h2d_memcpy(objs->dpa, dpa_thread->arg,
                                          &arg, sizeof(struct dpa_thread_arg));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("setup_pod_dma: h2d_memcpy failed (EU %d): %s",
                             k_j, doca_error_get_descr(result));
                return result;
            }
            uint64_t rpc_ret;
            result = doca_dpa_rpc(objs->dpa, thread_init_rpc, &rpc_ret,
                                  arg.dpa_consumer, (uint32_t)CC_DPA_MAX_MSG_NUM);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("setup_pod_dma: thread_init_rpc failed (EU %d): %s",
                             k_j, doca_error_get_descr(result));
                return result;
            }
            result = doca_dpa_thread_run(dpa_thread->thread);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("setup_pod_dma: dpa_thread_run failed (EU %d): %s",
                             k_j, doca_error_get_descr(result));
                return result;
            }
            objs->dpa_thread_running[k_j] = 1;
            objs->dpa_thread_running_any = 1;

            struct comch_msg trigger;
            memset(&trigger, 0, sizeof(trigger));
            trigger.type = DPA_MSG_WAKE;
            result = dmesh_doca_dpa_msgq_send(&objs->dpa_comches[k_j]->send,
                                               &trigger, sizeof(trigger));
            if (result != DOCA_SUCCESS)
                DOCA_LOG_WARN("Trigger msg to EU %d failed: %s", k_j, doca_error_get_descr(result));
            DOCA_LOG_INFO("EU %d started (pod_id=%d ring=%d)", k_j, pod->pod_id, j);
        } else {
            struct comch_add_ring_msg add_msg;
            memset(&add_msg, 0, sizeof(add_msg));
            add_msg.type = DPA_MSG_RING_ADD;
            add_msg.ring = ring_info;
            result = dmesh_doca_dpa_msgq_send(&objs->dpa_comches[k_j]->send,
                                               &add_msg, sizeof(add_msg));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("setup_pod_dma: send ADD_RING to EU %d failed: %s",
                             k_j, doca_error_get_descr(result));
                return result;
            }
            DOCA_LOG_INFO("Sent ADD_RING to EU %d (pod_id=%d ring=%d)", k_j, pod->pod_id, j);
        }
    }

    /* 3. Reverse rings: ring j -> EU k_j (same EU as forward ring j). Each writes
     * a disjoint host RX region [j*host_region, (j+1)*host_region); credit comes
     * from forward ring j's buf_arr (slot DMA_RING_SIZE); admission depth is
     * rq_depth/K per region. host_rx may be absent now — update_rev_ring_host_rx
     * re-sends ADD_REV_RING with the real handle/region when it arrives. */
    uint64_t host_region = (pod->host_rx_buf_size) ? pod->host_rx_buf_size / (uint64_t)K : 0;
    for (int j = 0; j < K; j++) {
        int k_j = (pod->pod_id * K + j) % N;
        result = setup_dpu_tx_ring(objs->dev, DMA_RING_SIZE,
                                   &pod->tx_rings[j], &pod->tx_ring_mmaps[j]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: TX ring[%d] failed for pod %d: %s",
                         j, pod->pod_id, doca_error_get_descr(result));
            return result;
        }
        result = setup_dpa_buf_array_pod(objs, DMA_RING_SIZE, pod->tx_ring_mmaps[j], &pod->tx_buf_arrs[j]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("setup_pod_dma: TX buf_arr[%d] failed for pod %d: %s",
                         j, pod->pod_id, doca_error_get_descr(result));
            return result;
        }

        struct dpa_ring_info rev_ring_info;
        doca_dpa_dev_buf_arr_t dpa_buf_arr;
        doca_dpa_dev_mmap_t host_rx_mmap_h = 0;
        doca_dpa_dev_buf_arr_t fwd_buf_arr_h = 0;

        result = doca_buf_arr_get_dpa_handle(pod->tx_buf_arrs[j], &dpa_buf_arr);
        if (result != DOCA_SUCCESS) return result;
        if (pod->host_rx_mmap) {
            result = doca_mmap_dev_get_dpa_handle(pod->host_rx_mmap, objs->dev, &host_rx_mmap_h);
            if (result != DOCA_SUCCESS)
                DOCA_LOG_WARN("setup_pod_dma: host_rx_mmap DPA handle failed: %s", doca_error_get_descr(result));
        }
        result = doca_buf_arr_get_dpa_handle(pod->buf_arrs[j], &fwd_buf_arr_h);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_WARN("setup_pod_dma: fwd buf_arr[%d] DPA handle failed: %s", j, doca_error_get_descr(result));
            fwd_buf_arr_h = 0;
        }

        memset(&rev_ring_info, 0, sizeof(rev_ring_info));
        rev_ring_info.buf_arr = dpa_buf_arr;
        rev_ring_info.buf_arr_size = DMA_RING_SIZE;
        rev_ring_info.host_mmap = host_rx_mmap_h;
        rev_ring_info.host_addr = (uint64_t)pod->host_rx_addr + (uint64_t)j * host_region;
        rev_ring_info.host_buf_size = host_region;
        rev_ring_info.region_off = (uint32_t)((uint64_t)j * host_region);
        rev_ring_info.pod_id = pod->pod_id;
        rev_ring_info.host_credit_buf_arr = fwd_buf_arr_h;  /* forward ring j's credit slot */
        rev_ring_info.rq_depth = pod->rq_depth / (uint32_t)K;

        struct comch_add_rev_ring_msg rev_msg;
        memset(&rev_msg, 0, sizeof(rev_msg));
        rev_msg.type = DPA_MSG_REV_RING_ADD;
        rev_msg.ring = rev_ring_info;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comches[k_j]->send,
                                           &rev_msg, sizeof(rev_msg));
        if (result != DOCA_SUCCESS)
            DOCA_LOG_WARN("setup_pod_dma: send ADD_REV_RING to EU %d failed: %s", k_j, doca_error_get_descr(result));
        else
            DOCA_LOG_INFO("Sent ADD_REV_RING to EU %d (pod_id=%d ring=%d rq_depth=%u host_off=%u)",
                          k_j, pod->pod_id, j, rev_ring_info.rq_depth, rev_ring_info.region_off);
    }

    /* RELEASE publication: all data-plane fields (dma_buffer,
     * local_mmap_dpa_handle, tx_ring, ...) are written above; publish dma_ready
     * last so a reader that ACQUIRE-loads dma_ready==1 (pod_data_ready) is
     * guaranteed to see them. */
    __atomic_store_n(&pod->dma_ready, 1, __ATOMIC_RELEASE);
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
    int N = objs->num_dpa_threads;
    int K = pod->k_rings > 0 ? pod->k_rings : 1;

    if (!pod->host_rx_mmap) {
        DOCA_LOG_WARN("update_rev_ring_host_rx: missing host_rx_mmap for pod %d", pod->pod_id);
        return DOCA_ERROR_NOT_FOUND;
    }

    doca_dpa_dev_mmap_t host_rx_mmap_h;
    result = doca_mmap_dev_get_dpa_handle(pod->host_rx_mmap, objs->dev, &host_rx_mmap_h);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("update_rev_ring_host_rx: host_rx_mmap DPA handle failed: %s",
                      doca_error_get_descr(result));
        return result;
    }

    /* Re-send ADD_REV_RING for each of the K reverse rings with the now-available
     * host RX mmap + the partitioned region. The kernel updates the matching
     * pod_id rev ring in place (one per EU). */
    uint64_t host_region = pod->host_rx_buf_size / (uint64_t)K;
    for (int j = 0; j < K; j++) {
        int k_j = (pod->pod_id * K + j) % N;
        if (!pod->tx_buf_arrs[j]) {
            DOCA_LOG_WARN("update_rev_ring_host_rx: missing tx_buf_arr[%d] for pod %d", j, pod->pod_id);
            continue;
        }
        struct dpa_ring_info rev_ring_info;
        doca_dpa_dev_buf_arr_t dpa_buf_arr;
        doca_dpa_dev_buf_arr_t credit_buf_arr_h = 0;

        result = doca_buf_arr_get_dpa_handle(pod->tx_buf_arrs[j], &dpa_buf_arr);
        if (result != DOCA_SUCCESS) return result;
        result = doca_buf_arr_get_dpa_handle(pod->buf_arrs[j], &credit_buf_arr_h);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_WARN("update_rev_ring_host_rx: fwd buf_arr[%d] DPA handle failed: %s", j, doca_error_get_descr(result));
            credit_buf_arr_h = 0;
        }

        memset(&rev_ring_info, 0, sizeof(rev_ring_info));
        rev_ring_info.buf_arr = dpa_buf_arr;
        rev_ring_info.buf_arr_size = DMA_RING_SIZE;
        rev_ring_info.host_mmap = host_rx_mmap_h;
        rev_ring_info.host_addr = (uint64_t)pod->host_rx_addr + (uint64_t)j * host_region;
        rev_ring_info.host_buf_size = host_region;
        rev_ring_info.region_off = (uint32_t)((uint64_t)j * host_region);
        rev_ring_info.pod_id = pod->pod_id;
        rev_ring_info.host_credit_buf_arr = credit_buf_arr_h;
        rev_ring_info.rq_depth = pod->rq_depth / (uint32_t)K;

        struct comch_add_rev_ring_msg rev_msg;
        memset(&rev_msg, 0, sizeof(rev_msg));
        rev_msg.type = DPA_MSG_REV_RING_ADD;
        rev_msg.ring = rev_ring_info;
        result = dmesh_doca_dpa_msgq_send(&objs->dpa_comches[k_j]->send,
                                           &rev_msg, sizeof(rev_msg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("update_rev_ring_host_rx: send ADD_REV_RING to EU %d failed: %s",
                          k_j, doca_error_get_descr(result));
            return result;
        }
        DOCA_LOG_INFO("Updated rev ring host RX: pod %d ring %d (EU %d host_off=%u rq_depth=%u)",
                      pod->pod_id, j, k_j, rev_ring_info.region_off, rev_ring_info.rq_depth);
    }
    return DOCA_SUCCESS;
}

#endif /* DOCA_ARCH_DPU */