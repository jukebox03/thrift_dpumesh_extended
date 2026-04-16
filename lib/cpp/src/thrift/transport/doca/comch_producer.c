#include "comch_producer.h"
#include "comch_consumer.h"
#include "object.h"
#include "buffer.h"

#include <doca_log.h>
#include <doca_error.h>
#include <time.h>
#include <string.h>

DOCA_LOG_REGISTER(COMCH_PRODUCER);

/**
 * Callback for producer send task successful completion
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void producer_send_task_completion_callback(struct doca_comch_producer_task_send *task,
						   union doca_data task_user_data,
						   union doca_data ctx_user_data)
{
	struct objects *objs;
	const struct doca_buf *buf;

	(void)task_user_data;

	objs = (struct objects *)(ctx_user_data.ptr);
	objs->producer_result = DOCA_SUCCESS;
	objs->sent_msg_cnt++;
	DOCA_LOG_INFO("Datapath producer send completed (sent_msg_cnt=%d)", objs->sent_msg_cnt);

	buf = doca_comch_producer_task_send_get_buf(task);
	if (buf)
		(void)doca_buf_dec_refcount((struct doca_buf *)buf, NULL);
	doca_task_free(doca_comch_producer_task_send_as_task(task));
}

/**
 * Callback for producer send task completion with error
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void producer_send_task_completion_err_callback(struct doca_comch_producer_task_send *task,
						       union doca_data task_user_data,
						       union doca_data ctx_user_data)
{
	struct objects *objs;
	const struct doca_buf *buf;

	(void)task_user_data;

	objs = (struct objects *)(ctx_user_data.ptr);
	objs->producer_result = doca_task_get_status(doca_comch_producer_task_send_as_task(task));
	DOCA_LOG_ERR("Producer message failed to send with error = %s",
		     doca_error_get_name(objs->producer_result));

	buf = doca_comch_producer_task_send_get_buf(task);
	if (buf)
		(void)doca_buf_dec_refcount((struct doca_buf *)buf, NULL);
	doca_task_free(doca_comch_producer_task_send_as_task(task));
	DOCA_LOG_ERR("Producer send task cleaned after error");
}

/**
 * Callback triggered whenever CC producer context state changes
 *
 * @user_data [in]: User data associated with the CC producer context.
 * @ctx [in]: The CC client context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void producer_state_changed_callback(const union doca_data user_data,
					    struct doca_ctx *ctx,
					    enum doca_ctx_states prev_state,
					    enum doca_ctx_states next_state)
{
	(void)ctx;
	(void)prev_state;

	struct objects *objs = (struct objects *)user_data.ptr;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("CC producer context has been stopped");
		/* We can stop progressing the PE */
		objs->producer_finish = true;
		break;
	case DOCA_CTX_STATE_STARTING:
		/**
		 * The context is in starting state.
		 */
		DOCA_LOG_INFO("CC producer context entered into starting state");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("CC producer context is running");
		// objs->producer_result = prepare_producer_tasks(objs->producer, objs->producer_mem, objs->remote_consumer_id);
		// if (objs->producer_result != DOCA_SUCCESS) {
		// 	DOCA_LOG_ERR("Failed to submit producer send task with error = %s",
		// 		     doca_error_get_name(objs->producer_result));
		// 	(void)doca_ctx_stop(doca_comch_producer_as_ctx(objs->producer));
		// }
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * The context is in stopping, this can happen when fatal error encountered or when stopping context.
		 * doca_pe_progress() will cause all tasks to be flushed, and finally transition state to idle
		 */
		DOCA_LOG_INFO("CC producer context entered into stopping state");
		break;
	default:
		break;
	}
}

doca_error_t init_comch_producer(struct doca_comch_connection *connection,
				 struct comch_producer_cb_config *cfg,
				 struct doca_comch_producer **producer,
				 struct doca_pe **pe)
{
	doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;

	DOCA_LOG_INFO("Initializing CC producer");

	result = doca_pe_create(pe);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
		return result;
	}

	result = doca_comch_producer_create(connection, producer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create producer with error = %s", doca_error_get_name(result));
		goto destroy_pe;
	}

	ctx = doca_comch_producer_as_ctx(*producer);

	result = doca_pe_connect_ctx(*pe, ctx);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed adding pe context to producer with error = %s", doca_error_get_name(result));
		goto destroy_producer;
	}

	result = doca_ctx_set_state_changed_cb(ctx, cfg->ctx_state_changed_cb);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed setting state change callback with error = %s", doca_error_get_name(result));
		goto destroy_producer;
	}

	result = doca_comch_producer_task_send_set_conf(*producer,
							cfg->send_task_comp_cb,
							cfg->send_task_comp_err_cb,
							CC_DATA_PATH_TASK_NUM);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed setting producer send task cbs with error = %s", doca_error_get_name(result));
		goto destroy_producer;
	}

	user_data.ptr = cfg->ctx_user_data;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_producer;
	}

	result = doca_ctx_start(ctx);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start producer context with error = %s", doca_error_get_name(result));
		goto destroy_producer;
	}

	return DOCA_SUCCESS;

destroy_producer:
	doca_comch_producer_destroy(*producer);
	*producer = NULL;
destroy_pe:
	doca_pe_destroy(*pe);
	*pe = NULL;
	return result;
}


