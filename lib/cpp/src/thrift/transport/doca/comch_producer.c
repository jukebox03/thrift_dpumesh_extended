#include "comch_producer.h"
#include "comch_consumer.h"
#include "object.h"
#include "buffer.h"

#include <doca_log.h>
#include <doca_error.h>
#include <time.h>

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
	uint8_t *data;
	doca_error_t result;
	struct timespec ts = {
		.tv_sec = 0,
		.tv_nsec = 10000,
	};

	(void)task_user_data;

	objs = (struct objects *)(ctx_user_data.ptr);
	objs->producer_result = DOCA_SUCCESS;
	objs->sent_msg_cnt++;

	buf = doca_comch_producer_task_send_get_buf(task);
	if (!buf) {
		DOCA_LOG_ERR("Failed to get doca buf from producer task");
		goto free_task;
	}

	/* data_len is not modified */
	result = doca_buf_get_data(buf, (void **)&data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get data pointer, error = %s", doca_error_get_name(result));
		goto free_buf;
	}

	do {
		result = doca_task_submit(doca_comch_producer_task_send_as_task(task));
		if (result == DOCA_ERROR_AGAIN)
			nanosleep(&ts, &ts);

	} while (result == DOCA_ERROR_AGAIN);
	
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed resubmitting send task with error = %s", 
				doca_error_get_name(result));
		goto free_buf;
	}

	// DOCA_LOG_INFO("producer resubmitted task with msg idx: %d", objs->msg_idx);
	return;

free_buf:
	(void)doca_buf_dec_refcount((struct doca_buf *)buf, NULL);
free_task:
	doca_task_free(doca_comch_producer_task_send_as_task(task));
	// (void)doca_ctx_stop(doca_comch_producer_as_ctx(objs->producer));
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
	(void)doca_buf_dec_refcount((struct doca_buf *)buf, NULL);
	doca_task_free(doca_comch_producer_task_send_as_task(task));
	(void)doca_ctx_stop(doca_comch_producer_as_ctx(objs->producer));
}

/**
 * Use producers to send a msg
 *
 * @data_path [in]: CC data path resources
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t prepare_producer_tasks(struct doca_comch_producer *producer, struct local_mem_bufs *pmem,
											uint32_t remote_consumer_id)
{
	struct doca_comch_producer_task_send *producer_task;
	struct doca_buf *buf;
	struct doca_task *task_obj;
	doca_error_t result;
	int i;
	uint32_t *data;
	size_t data_len;

	for (i = 0; i < CC_DATA_PATH_TASK_NUM; i++) {
		result = doca_buf_pool_buf_alloc(pmem->bpool, &buf);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to allocate doca buf from producer bpool with error = %s", doca_error_get_name(result));
			return result;
		}

		result = doca_buf_get_data(buf, (void **)&data);
		if (result != DOCA_SUCCESS) {
			(void)doca_buf_dec_refcount(buf, NULL);
			DOCA_LOG_ERR("Failed to get data pointer from doca buf with error = %s", doca_error_get_name(result));
			return result;
		}

		result = doca_buf_set_data_len(buf, CC_DATA_PATH_MSG_SIZE);
		if (result != DOCA_SUCCESS) {
			(void)doca_buf_dec_refcount(buf, NULL);
			DOCA_LOG_ERR("Failed to set data length to doca buf with error = %s", doca_error_get_name(result));
			return result;
		}
		
		result = doca_buf_get_data_len(buf, &data_len);

		result = doca_comch_producer_task_send_alloc_init(producer,
							  buf,
							  NULL,
							  0,
							  remote_consumer_id,
							  &producer_task);
		if (result != DOCA_SUCCESS) {
			(void)doca_buf_dec_refcount(buf, NULL);
			DOCA_LOG_ERR("Failed to allocate task for producer with error = %s", doca_error_get_name(result));
			return result;
		}

		task_obj = doca_comch_producer_task_send_as_task(producer_task);
		do {
			result = doca_task_submit(task_obj);
		} while (result == DOCA_ERROR_AGAIN);
		if (result != DOCA_SUCCESS) {
			(void)doca_buf_dec_refcount(buf, NULL);
			doca_task_free(task_obj);
			DOCA_LOG_ERR("Failed submitting send task with error = %s", doca_error_get_name(result));
			return result;
		}
	}

	return DOCA_SUCCESS;
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

doca_error_t
init_comch_datapath_producer(struct objects *objs)
{
    doca_error_t result;
    struct local_mem_bufs *pmem;
    struct timespec ts = {
        .tv_sec = 0,
        .tv_nsec = SLEEP_IN_NANOS,
    };
	struct timespec last, now;
	double elapsed;
	clock_gettime(CLOCK_MONOTONIC, &last);

    struct comch_producer_cb_config producer_cb_cfg = {
        .send_task_comp_cb = producer_send_task_completion_callback,
        .send_task_comp_err_cb = producer_send_task_completion_err_callback,
        .ctx_user_data = objs,
        .ctx_state_changed_cb = producer_state_changed_callback
    };

	DOCA_LOG_INFO("Initializing CC data path producer");
	
    /* wait for the new consumer */
    while (objs->remote_consumer_id == 0) {
		if (doca_pe_progress(objs->pe) == 0)
		nanosleep(&ts, &ts);
    }
	DOCA_LOG_INFO("Received remote consumer id: %u", objs->remote_consumer_id);

    if (objs->remote_consumer_id == INVALID_CONSUMER_ID)
        return DOCA_ERROR_UNEXPECTED;

    /* allocate producer mem */
    if (!objs->producer_mem) {
        objs->producer_mem = calloc(1, sizeof(struct local_mem_bufs));
        if (!objs->producer_mem) {
            DOCA_LOG_ERR("Failed to allocate memory for producer mem buffers");
            return DOCA_ERROR_NO_MEMORY;
        }
    }

    /* initialize producer memory */
    pmem = objs->producer_mem;
    pmem->need_alloc_mem = true;
    result = init_local_mem_bufs(pmem, objs->dev, BUF_INV_TYPE_POOL, CC_DATA_PATH_MSG_SIZE, CC_DATA_PATH_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init producer memory with error = %s", doca_error_get_name(result));
        return result;
    }   
	DOCA_LOG_INFO("Producer memory initialized");
	/* query DOCA producer capabilities of DOCA device */
	uint32_t max_producers, max_buf_list_len, max_buf_size;
	result = doca_comch_producer_cap_get_max_producers(doca_dev_as_devinfo(objs->dev), &max_producers);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get max producers with error = %s", doca_error_get_name(result));
		return result;
	}
	result = doca_comch_producer_cap_get_max_buf_size(doca_dev_as_devinfo(objs->dev), &max_buf_size);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get max buf size with error = %s", doca_error_get_name(result));
		return result;
	}
	result = doca_comch_producer_cap_get_max_buf_list_len(doca_dev_as_devinfo(objs->dev), &max_buf_list_len);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get max buf list len with error = %s", doca_error_get_name(result));
		return result;
	}
	DOCA_LOG_INFO("Consumer capabilities - max consumers: %u, max_buf_size: %u, max buf list len: %u",
		      max_producers, max_buf_size, max_buf_list_len);


    /* init a CC producer */
    result = init_comch_producer(objs->connection,
                     &producer_cb_cfg,
                     &(objs->producer),
                     &(objs->producer_pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init a producer with error = %s", doca_error_get_name(result));
        clean_local_mem_bufs(pmem);
        return result;
    }
    
	enum doca_ctx_states state;
	do {
		doca_pe_progress(objs->producer_pe);
		doca_ctx_get_state(doca_comch_producer_as_ctx(objs->producer), &state);
	} while (state != DOCA_CTX_STATE_RUNNING);
	DOCA_LOG_INFO("Producer context is now in RUNNING state");

    return objs->producer_result;
}