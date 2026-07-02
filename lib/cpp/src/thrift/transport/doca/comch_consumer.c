#include "comch_consumer.h"
#include "object.h"
#include "buffer.h"

#include <doca_comch.h>
#include <doca_comch_consumer.h>
#include <doca_log.h>
#include <doca_error.h>

DOCA_LOG_REGISTER(COMCH_CONSUMER);
/**
 * Callback for new consumer arrival event
 *
 * @event [in]: New remote consumer event object
 * @comch_connection [in]: The connection related to the consumer
 * @id [in]: The ID of the new remote consumer
 */
void server_new_consumer_callback(struct doca_comch_event_consumer *event,
				  struct doca_comch_connection *comch_connection,
				  uint32_t id)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	doca_error_t result;

	/* This argument is not in use */
	(void)event;
	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	if (comch_server == NULL) {
		DOCA_LOG_ERR("Failed to get comch server from connection");
		return;
	}

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	DOCA_LOG_INFO("Got a new remote consumer with ID = [%d]", id);
}

void client_new_consumer_callback(struct doca_comch_event_consumer *event,
				  struct doca_comch_connection *comch_connection,
				  uint32_t id)
{
	union doca_data user_data;
	struct doca_comch_client *comch_client;
	doca_error_t result;

	/* This argument is not in use */
	(void)event;
	comch_client = doca_comch_client_get_client_ctx(comch_connection);
	if (comch_client == NULL) {
		DOCA_LOG_ERR("Failed to get comch server from connection");
		return;
	}

	result = doca_ctx_get_user_data(doca_comch_client_as_ctx(comch_client), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	DOCA_LOG_INFO("Got a new remote consumer with ID = [%d]", id);
}

/**
 * Callback for expired consumer arrival event
 *
 * @event [in]: Expired remote consumer event object
 * @comch_connection [in]: The connection related to the consumer
 * @id [in]: The ID of the expired remote consumer
 */
void expired_consumer_callback(struct doca_comch_event_consumer *event,
			       struct doca_comch_connection *comch_connection,
			       uint32_t id)
{
	/* These arguments are not in use */
	(void)event;
	(void)comch_connection;
	(void)id;
}

static doca_error_t init_comch_consumer(struct doca_comch_connection *connection,
				 struct doca_mmap *user_mmap,
				 struct comch_consumer_cb_config *cfg,
				 struct doca_comch_consumer **consumer,
				 struct doca_pe **pe)
{
	doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;

	if (*pe == NULL) {
		DOCA_LOG_INFO("Creating new PE for consumer");
		result = doca_pe_create(pe);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
			return result;
		}
	}

	result = doca_comch_consumer_create(connection, user_mmap, consumer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create consumer with error = %s", doca_error_get_name(result));
		goto destroy_pe;
	}

	uint32_t id;
	doca_comch_consumer_get_id(*consumer, &id);
	DOCA_LOG_INFO("Created consumer with ID = [%d]", id);

	ctx = doca_comch_consumer_as_ctx(*consumer);

	result = doca_pe_connect_ctx(*pe, ctx);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
		goto destroy_consumer;
	}

	result = doca_ctx_set_state_changed_cb(ctx, cfg->ctx_state_changed_cb);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed setting state change callback with error = %s", doca_error_get_name(result));
		goto destroy_consumer;
	}

	result = doca_comch_consumer_task_post_recv_set_conf(*consumer,
							     cfg->recv_task_comp_cb,
							     cfg->recv_task_comp_err_cb,
							     CC_DATA_PATH_TASK_NUM);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed setting consumer recv task cbs with error = %s", doca_error_get_name(result));
		goto destroy_consumer;
	}

	user_data.ptr = cfg->ctx_user_data;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_consumer;
	}

	result = doca_ctx_start(ctx);
	if (result != DOCA_ERROR_IN_PROGRESS) {
		DOCA_LOG_ERR("Failed to start consumer context with error = %s", doca_error_get_name(result));
		goto destroy_consumer;
	}

	return DOCA_SUCCESS;

destroy_consumer:
	doca_comch_consumer_destroy(*consumer);
	*consumer = NULL;
destroy_pe:
	doca_pe_destroy(*pe);
	*pe = NULL;
	return result;
}

/**
 * Callback for consumer post recv task successful completion
 *
 * @task [in]: Recv task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void consumer_recv_task_comp_cb(struct doca_comch_consumer_task_post_recv *task,
						   union doca_data task_user_data,
						   union doca_data ctx_user_data)
{
	struct objects *objs;
	size_t recv_msg_len;
	void *recv_msg;
	struct doca_buf *buf;
	doca_error_t result;

	(void)task_user_data;

	objs = (struct objects *)(ctx_user_data.ptr);

	/* Task just completed → it's leaving DOCA's in-flight pool (we are about
	 * to resubmit it, which re-enters the pool under capacity check). */
	doca_pool_release(&objs->recv_tasks_in_flight);

	buf = doca_comch_consumer_task_post_recv_get_buf(task);

	objs->consumer_result = doca_buf_get_data(buf, &recv_msg);
	if (objs->consumer_result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get data address from DOCA buf with error = %s",
			     doca_error_get_name(objs->consumer_result));
		goto err_out;
	}

	objs->consumer_result = doca_buf_get_data_len(buf, &recv_msg_len);
	if (objs->consumer_result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get data length from DOCA buf with error = %s",
			     doca_error_get_name(objs->consumer_result));
		goto err_out;
	}

	if (recv_msg_len > 0 && objs->rx_data_hook != NULL) {
		objs->rx_data_hook(objs->rx_hook_ctx, (const uint8_t *)recv_msg, (uint32_t)recv_msg_len);
	}

	doca_buf_reset_data_len(buf);
	struct doca_task *t = doca_comch_consumer_task_post_recv_as_task(task);

	/* Gate resubmit on our own counter; on failure stash the task in
	 * consumer_retry[] for the main PE loop to drain. The _exact variant is
	 * safe because this callback and drain_consumer_retry run only on the PE
	 * thread (single submitter, no concurrent-submitter race). */
	if (!doca_pool_try_acquire_exact(&objs->recv_tasks_in_flight, objs->recv_tasks_max))
		goto stash;
	result = doca_task_submit(t);
	if (result == DOCA_SUCCESS)
		return;

	/* Capacity had room but submit still failed: release count and stash. */
	doca_pool_release(&objs->recv_tasks_in_flight);
	DOCA_LOG_WARN("Recv resubmit gated-submit failed: %s; stashing",
	              doca_error_get_name(result));

stash:
	pthread_mutex_lock(&objs->consumer_retry_lock);
	if (objs->num_consumer_retry < MAX_CONSUMER_RETRY) {
		objs->consumer_retry[objs->num_consumer_retry++] = t;
		pthread_mutex_unlock(&objs->consumer_retry_lock);
		return;
	}
	pthread_mutex_unlock(&objs->consumer_retry_lock);
	DOCA_LOG_ERR("consumer_retry full (%d); dropping recv task — capacity bug",
	             MAX_CONSUMER_RETRY);
	/* fall through to err_out; this really shouldn't happen */

err_out:
	(void)doca_buf_dec_refcount(buf, NULL);
	doca_task_free(doca_comch_consumer_task_post_recv_as_task(task));
}

/**
 * Callback for consumer post recv task completion with error
 *
 * @task [in]: Recv task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void consumer_recv_task_comp_err_cb(struct doca_comch_consumer_task_post_recv *task,
						       union doca_data task_user_data,
						       union doca_data ctx_user_data)
{
	struct objects *objs;
	struct doca_buf *buf;

	(void)task_user_data;

	objs = (struct objects *)(ctx_user_data.ptr);
	/* Task is leaving the pool regardless of error status. */
	doca_pool_release(&objs->recv_tasks_in_flight);

	objs->consumer_result = doca_task_get_status(doca_comch_consumer_task_post_recv_as_task(task));
	DOCA_LOG_ERR("Consumer failed to recv message with error = %s (non-fatal, continuing)",
		     doca_error_get_name(objs->consumer_result));

	buf = doca_comch_consumer_task_post_recv_get_buf(task);
	(void)doca_buf_dec_refcount(buf, NULL);
	doca_task_free(doca_comch_consumer_task_post_recv_as_task(task));
	/* Do NOT stop consumer context — allow other pods to continue working */
}

/**
 * Post the initial pool of recv tasks on the consumer.
 *
 * @objs [in]: transport objects
 * @consumer [in]: comch consumer to post recv tasks on
 * @cmem [in]: consumer-side buf pool
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t prepare_consumer_tasks(struct objects *objs, struct doca_comch_consumer *consumer, struct local_mem_bufs *cmem)
{
	struct doca_comch_consumer_task_post_recv *consumer_task;
	struct doca_buf *buf;
	struct doca_task *task_obj;
	doca_error_t result;
	int i;

	for (i = 0; i < CC_DATA_PATH_TASK_NUM; i++) {
		result = doca_buf_pool_buf_alloc(cmem->bpool, &buf);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to allocate buf from bpool with error = %s", doca_error_get_name(result));
			return result;
		}
		result = doca_comch_consumer_task_post_recv_alloc_init(consumer, buf, &consumer_task);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to allocate task for consumer with error = %s", doca_error_get_name(result));
			return result;
		}
		task_obj = doca_comch_consumer_task_post_recv_as_task(consumer_task);

		/* Bootstrap: fill the pool up to max (single-threaded init loop). */
		atomic_fetch_add(&objs->recv_tasks_in_flight, 1);
		result = doca_task_submit(task_obj);
		if (result != DOCA_SUCCESS) {
			atomic_fetch_sub(&objs->recv_tasks_in_flight, 1);
			(void)doca_buf_dec_refcount(buf, NULL);
			doca_task_free(task_obj);
			DOCA_LOG_ERR("Failed submitting recv task with error = %s", doca_error_get_name(result));
			return result;
		}
	}

	return DOCA_SUCCESS;
}

/**
 * Callback triggered whenever CC consumer context state changes
 *
 * @user_data [in]: User data associated with the CC consumer context
 * @ctx [in]: The CC consumer context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void consumer_state_changed_cb(const union doca_data user_data,
					    struct doca_ctx *ctx,
					    enum doca_ctx_states prev_state,
					    enum doca_ctx_states next_state)
{
	(void)ctx;

	struct objects *objs = (struct objects *)user_data.ptr;
	
	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("CC consumer context has been stopped");

		/* A move to stop from non running/stopping state means there's been an error */
		if ((prev_state != DOCA_CTX_STATE_RUNNING) && (prev_state != DOCA_CTX_STATE_STOPPING))
			objs->consumer_result = DOCA_ERROR_UNEXPECTED;

		break;
	case DOCA_CTX_STATE_STARTING:
		/**
		 * The context is in starting state.
		 */
		DOCA_LOG_INFO("CC consumer context entered into starting state. Waiting consumer producer negotiation finish");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("CC consumer context is running, pref_state:%d, Receiving message from producer, waiting finish", prev_state);
		objs->consumer_result = prepare_consumer_tasks(objs, objs->consumer, objs->consumer_mem);
		if (objs->consumer_result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to submit consumer recv task with error = %s",
				     doca_error_get_name(objs->consumer_result));
			(void)doca_ctx_stop(doca_comch_consumer_as_ctx(objs->consumer));
		}
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * The context is in stopping, this can happen when fatal error encountered or when stopping context.
		 * doca_pe_progress() will cause all tasks to be flushed, and finally transition state to idle
		 */
		DOCA_LOG_INFO("CC consumer context entered into stopping state");
		break;
	default:
		break;
	}
}


doca_error_t
init_comch_datapath_consumer(struct objects *objs)
{
    doca_error_t result;
    struct local_mem_bufs *cmem;
    struct comch_consumer_cb_config consumer_cb_cfg = {
        .recv_task_comp_cb = consumer_recv_task_comp_cb,
        .recv_task_comp_err_cb = consumer_recv_task_comp_err_cb,
        .ctx_user_data = objs,
        .ctx_state_changed_cb = consumer_state_changed_cb
    };

    /* Record the recv pool size for the capacity-gated submit path. */
    objs->recv_tasks_max = CC_DATA_PATH_TASK_NUM;

    objs->consumer_mem = calloc(1, sizeof(struct local_mem_bufs));
    if (!objs->consumer_mem) {
        DOCA_LOG_ERR("Failed to allocate memory for consumer mem buffers");
        return DOCA_ERROR_NO_MEMORY;
    }
    cmem = objs->consumer_mem;

    /* setup connsumer's mmap and doca_buf infrastructure */
    cmem->need_alloc_mem = true;
    result = init_local_mem_bufs(cmem, objs->dev, BUF_INV_TYPE_POOL, CC_DATA_PATH_MSG_SIZE, CC_DATA_PATH_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init consumer memory with error = %s", doca_error_get_name(result));
        return result;
    }
	uint32_t max_consumers;
	doca_comch_consumer_cap_get_max_consumers(doca_dev_as_devinfo(objs->dev), &max_consumers);
	DOCA_LOG_INFO("Device supports max %u concurrent consumers", max_consumers);

	uint32_t consumer_max_buf_size;
	result = doca_comch_consumer_cap_get_max_buf_size(doca_dev_as_devinfo(objs->dev), &consumer_max_buf_size);
	if (result == DOCA_SUCCESS) {
		DOCA_LOG_INFO("Consumer HW max_buf_size: %u, configured: %u",
			      consumer_max_buf_size, (unsigned int)CC_DATA_PATH_MSG_SIZE);
		if (consumer_max_buf_size < CC_DATA_PATH_MSG_SIZE) {
			DOCA_LOG_WARN("Consumer HW max_buf_size(%u) < CC_DATA_PATH_MSG_SIZE(%u)",
				      consumer_max_buf_size, (unsigned int)CC_DATA_PATH_MSG_SIZE);
		}
	}

	uint32_t consumer_max_tasks;
	result = doca_comch_consumer_cap_get_max_num_tasks(doca_dev_as_devinfo(objs->dev), &consumer_max_tasks);
	if (result == DOCA_SUCCESS) {
		DOCA_LOG_INFO("Consumer max_num_tasks: %u, configured: %u",
			      consumer_max_tasks, (unsigned int)CC_DATA_PATH_TASK_NUM);
		if (consumer_max_tasks < CC_DATA_PATH_TASK_NUM) {
			DOCA_LOG_WARN("Consumer HW max_num_tasks(%u) < CC_DATA_PATH_TASK_NUM(%u)",
				      consumer_max_tasks, (unsigned int)CC_DATA_PATH_TASK_NUM);
		}
	}

	result = init_comch_consumer(objs->connection,
					cmem->mmap,
					&consumer_cb_cfg,
					&(objs->consumer),
					&(objs->consumer_pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init a consumer with error = %s", doca_error_get_name(result));
        clean_local_mem_bufs(cmem); 
        free(cmem);
        objs->consumer_mem = NULL;
        return result;
    }

	enum doca_ctx_states state;
	do {
		doca_pe_progress(objs->consumer_pe);
		doca_pe_progress(objs->pe);

		doca_ctx_get_state(doca_comch_consumer_as_ctx(objs->consumer), &state);
	} while (state != DOCA_CTX_STATE_RUNNING);

	return DOCA_SUCCESS;
}