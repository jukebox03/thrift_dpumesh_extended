#include "comch_client.h"

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include "comch_consumer.h"
#include "object.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(COMCH_CLIENT);

#ifndef SLEEP_IN_NANOS
#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */
#endif

/**
 * Callback for client send task successful completion
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void client_send_task_completion_callback(struct doca_comch_task_send *task,
						 union doca_data task_user_data,
						 union doca_data ctx_user_data)
{
	struct objects *objs;
	void *payload_copy = task_user_data.ptr;

	objs = (struct objects *)(ctx_user_data.ptr);
	doca_pool_release(&objs->send_tasks_in_flight);

	if (payload_copy != NULL)
		free(payload_copy);
	doca_task_free(doca_comch_task_send_as_task(task));
}

/**
 * Callback for client send task completion with error
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void client_send_task_completion_err_callback(struct doca_comch_task_send *task,
						     union doca_data task_user_data,
						     union doca_data ctx_user_data)
{
	struct objects *objs;
	void *payload_copy = task_user_data.ptr;

	objs = (struct objects *)(ctx_user_data.ptr);
	doca_pool_release(&objs->send_tasks_in_flight);
	if (payload_copy != NULL)
		free(payload_copy);
	doca_task_free(doca_comch_task_send_as_task(task));
	(void)doca_ctx_stop(doca_comch_client_as_ctx(objs->cc_client));
}

/**
 * Callback for client message recv event
 *
 * @event [in]: Recv event object
 * @recv_buffer [in]: Message buffer
 * @msg_len [in]: Message len
 * @comch_connection [in]: Connection the message was received on
 */
static void client_message_recv_callback(struct doca_comch_event_msg_recv *event,
					 uint8_t *recv_buffer,
					 uint32_t msg_len,
					 struct doca_comch_connection *comch_connection)
{
	union doca_data user_data;
	struct doca_comch_client *comch_client;
	doca_error_t result;
	struct objects *objs;

	(void)event;

	comch_client = doca_comch_client_get_client_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_client_as_ctx(comch_client), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;

	/* Dispatch on the 1-byte type at offset 0. */
	switch (recv_buffer[0])
	{
	case DMESH_MSG_BATCH_FWD_ACK:
		/* Batched TX_ACK — coalesced free of K (port,seq) TX slots. */
		if (objs->rx_data_hook)
			objs->rx_data_hook(objs->rx_hook_ctx, recv_buffer, msg_len);
		break;

	case DMESH_MSG_BATCH_REV_DONE:
		/* Batched reverse-DMA completion — coalesced delivery of K responses. */
		if (objs->rx_data_hook)
			objs->rx_data_hook(objs->rx_hook_ctx, recv_buffer, msg_len);
		break;

	case DMESH_MSG_POD_ASSIGNED:
		/* DPU assigned this node's pod_id at registration. Stash it for the
		 * init-time register wait loop to pick up. NOT via rx_data_hook (the
		 * PE thread / hook aren't wired until the end of dpumesh_init). */
		if (msg_len >= sizeof(struct dmesh_pod_assigned_msg)) {
			const struct dmesh_pod_assigned_msg *am =
				(const struct dmesh_pod_assigned_msg *)recv_buffer;
			__atomic_store_n(&objs->assigned_pod_id, am->pod_id, __ATOMIC_RELEASE);
		}
		break;

	default:
		DOCA_LOG_INFO("Received unknown message type from server: %u", recv_buffer[0]);
		break;
	}
}

/**
 * Client sends a message to server
 *
 * @objs [in]: Transport objects context
 * @msg [in]: The msg to send
 * @len [in]: The msg length
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t client_send_msg(struct objects *objs, const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;
	void *msg_copy;
	union doca_data task_user_data;
	struct doca_task *task_obj;

	/* Capacity check: gate on our mirror of DOCA's send pool, progressing
	 * the PE while waiting for room. */
	int acq_retry = 0;
	while (!doca_pool_try_acquire(&objs->send_tasks_in_flight, objs->send_tasks_max)) {
		if (objs->pe)
			doca_pe_progress(objs->pe);
		if (++acq_retry > 10000) {
			DOCA_LOG_ERR("client_send_msg: send pool full after %d PE progresses", acq_retry);
			return DOCA_ERROR_AGAIN;
		}
	}

	msg_copy = malloc(len);
	if (msg_copy == NULL) {
		DOCA_LOG_ERR("Failed to allocate client payload copy");
		doca_pool_release(&objs->send_tasks_in_flight);
		return DOCA_ERROR_NO_MEMORY;
	}
	memcpy(msg_copy, msg, len);

	result = doca_comch_client_task_send_alloc_init(objs->cc_client,
							objs->connection,
								msg_copy,
							len,
							&task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate client task with error = %s", doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		return result;
	}

	task_obj = doca_comch_task_send_as_task(task);
	task_user_data.ptr = msg_copy;
	doca_task_set_user_data(task_obj, task_user_data);

	result = doca_task_submit(task_obj);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send client task with error = %s", doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		doca_task_free(task_obj);
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t init_comch_ctrl_path_client(const char *server_name,
                    struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;
	uint32_t max_msg_size, max_rq_size;
	enum doca_ctx_states state;
	struct timespec ts = {
		.tv_nsec = SLEEP_IN_NANOS,
	};

    /* Prime task-pool counters before anything that can submit. */
    objects_init_task_pools(objs);

    result = doca_pe_create(&(objs->pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
        return result;
    }

    result = doca_comch_client_create(objs->dev, server_name, &(objs->cc_client));
    if (result != DOCA_SUCCESS) {   
        DOCA_LOG_ERR("Failed to create client with error = %s", doca_error_get_name(result));
        goto destroy_pe;
    }

    ctx = doca_comch_client_as_ctx(objs->cc_client);

    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {   
        DOCA_LOG_ERR("Failed adding pe context to client with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    result = doca_comch_client_task_send_set_conf(objs->cc_client,
                                                  client_send_task_completion_callback,
                                                  client_send_task_completion_err_callback,
                                                  CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {   
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    result = doca_comch_client_event_msg_recv_register(objs->cc_client, 
                                                    client_message_recv_callback);
    if (result != DOCA_SUCCESS) {   
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

	/* register event callback for new comsumer and expired consumer */
	if (is_fast_path) {
		result = doca_comch_client_event_consumer_register(objs->cc_client,
									client_new_consumer_callback, expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_client;
		}
	}

    /* Set client properties */
	result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

     result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    DOCA_LOG_INFO("CC client max msg size: %u B, max rq size: %u", max_msg_size, max_rq_size);

	result = doca_comch_client_set_max_msg_size(objs->cc_client, max_msg_size);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set msg size property with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	{
		uint32_t desired_rq = max_rq_size;
		if (desired_rq < CC_CLIENT_RECV_QUEUE_SIZE) desired_rq = CC_CLIENT_RECV_QUEUE_SIZE;
		result = doca_comch_client_set_recv_queue_size(objs->cc_client, desired_rq);
		if (result == DOCA_SUCCESS) {
			DOCA_LOG_INFO("CC client recv queue size set to %u (cap=%u)", desired_rq, max_rq_size);
		}
	}
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set recv queue size property with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	user_data.ptr = (void *)objs;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	/* Client is not started until connection is finished, so getting connection in progress */
	result = doca_ctx_start(ctx);
	if (result != DOCA_ERROR_IN_PROGRESS) {
		DOCA_LOG_ERR("Failed to start client context with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	(void)doca_ctx_get_state(ctx, &state);
	while (state != DOCA_CTX_STATE_RUNNING) {
		(void)doca_pe_progress(objs->pe);
		nanosleep(&ts, &ts);
		(void)doca_ctx_get_state(ctx, &state);
	}

	(void)doca_comch_client_get_connection(objs->cc_client, &objs->connection);
	doca_comch_connection_set_user_data(objs->connection, user_data);
	DOCA_LOG_INFO("CC client connection established successfully");

    return DOCA_SUCCESS;

destroy_client:
    doca_comch_client_destroy(objs->cc_client);
    objs->cc_client = NULL;    
destroy_pe:
    doca_pe_destroy(objs->pe);
    objs->pe = NULL;
    return result;
}