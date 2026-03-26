
#include "comch_server.h"

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "../dpumesh.h"

#include "common.h"
#include "object.h"
#include "dpa.h"
#include "dpa_common.h"
#include "comch_common.h"
#include "comch_consumer.h"

#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_comch_consumer.h>
#include <doca_log.h>
#include <doca_comch_producer.h>

/* Forward declaration — defined below server_message_recv_callback */
static doca_error_t
server_send_msg_to(struct objects *objs, struct doca_comch_connection *conn,
                   const char *msg, size_t len);


DOCA_LOG_REGISTER(COMCH_SERVER);

static void server_send_task_completion_callback(struct doca_comch_task_send *task,
						 union doca_data task_user_data,
						 union doca_data ctx_user_data)
{
	struct objects *objs;

	(void)task_user_data;

	objs = (struct objects *)ctx_user_data.ptr;
	(void)objs;
	DOCA_LOG_INFO("Server task sent successfully");
	doca_task_free(doca_comch_task_send_as_task(task));
}

static void server_send_task_completion_err_callback(struct doca_comch_task_send *task,
						     union doca_data task_user_data,
						     union doca_data ctx_user_data)
{
	struct objects *objs;

	(void)task_user_data;

	objs = (struct objects *)ctx_user_data.ptr;
	doca_task_free(doca_comch_task_send_as_task(task));
	(void)doca_ctx_stop(doca_comch_server_as_ctx(objs->cc_server));
}

/**
 * Server sends a message to client
 *
 * @sample_objects [in]: The sample object to use
 * @msg [in]: The msg to send
 * @len [in]: The msg length
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
server_send_msg(struct objects *objs, const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, objs->connection,
							(void *)msg, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate server task with error = %s", doca_error_get_name(result));
		return result;
	}

	result = doca_task_submit(doca_comch_task_send_as_task(task));
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send server task with error = %s", doca_error_get_name(result));
		doca_task_free(doca_comch_task_send_as_task(task));
		return result;
	}

	return DOCA_SUCCESS;
}

/**
 * Callback for server message recv event
 *
 * @event [in]: Recv event object
 * @recv_buffer [in]: Message buffer
 * @msg_len [in]: Message len
 * @comch_connection [in]: Connection the message was received on
 */
static void server_message_recv_callback(struct doca_comch_event_msg_recv *event,
					 uint8_t *recv_buffer,
					 uint32_t msg_len,
					 struct doca_comch_connection *comch_connection)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;
	struct dmesh_comch_msg *comch_msg;

	(void)event;

	// DOCA_LOG_INFO("Message received: '%.*s', size: %u", (int)msg_len, recv_buffer, msg_len);

	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;

	/* Update connection for primary (first) client — backward compat */
	if (objs->connection == NULL)
		objs->connection = comch_connection;

	comch_msg = (struct dmesh_comch_msg *)recv_buffer;

	DOCA_LOG_INFO("Received message from client with type = %u", comch_msg->type);
	switch (comch_msg->type) {
	case DMESH_MSG_EXPORT_DESC:

		if (msg_len <= sizeof(struct dmesh_mmap_msg)) {
			DOCA_LOG_ERR("Received invalid MMAP message from client");
			return;
		}
		result = process_mmap_msg(objs, comch_connection, (struct dmesh_mmap_msg *)recv_buffer);
		break;

	case DMESH_MSG_REGISTER: {
		struct dmesh_register_msg *reg = (struct dmesh_register_msg *)recv_buffer;
		if (msg_len < sizeof(struct dmesh_register_msg)) {
			DOCA_LOG_ERR("Received invalid REGISTER message");
			return;
		}
		pods_register(objs, comch_connection, reg->pod_id, reg->app_name);
		DOCA_LOG_INFO("Pod registered: pod_id=%d, app=%s", reg->pod_id, reg->app_name);

		/* Reply with consumer ID so the host can create its producer.
		 * This is needed because the "new consumer" event only fires once
		 * (when the consumer is first created), so the second+ pod never
		 * gets the event and would block forever. */
		if (objs->consumer != NULL) {
			uint32_t cid;
			doca_comch_consumer_get_id(objs->consumer, &cid);
			struct dmesh_consumer_id_msg reply;
			reply.type = DMESH_MSG_CONSUMER_ID;
			reply.consumer_id = cid;
			server_send_msg_to(objs, comch_connection,
			                   (const char *)&reply, sizeof(reply));
			DOCA_LOG_INFO("Sent CONSUMER_ID=%u to pod_id=%d", cid, reg->pod_id);
		}
		break;
	}

	default:

		DOCA_LOG_ERR("Received unknown message type from client: %u", comch_msg->type);
		break;
	}
}

/**
 * Callback for connection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the connection was successful or not
 */
static void server_connection_event_callback(struct doca_comch_event_connection_status_changed *event,
					     struct doca_comch_connection *comch_connection,
					     uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	if (change_success == 0) {
		DOCA_LOG_ERR("Failed connection received");
		return;
	}

	(void)event;

	comch_server = doca_comch_server_get_server_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;

	/* First connection is the primary (backward compatible) */
	if (objs->connection == NULL)
		objs->connection = comch_connection;

	/* Add to pods table */
	pods_add_connection(objs, comch_connection);

	DOCA_LOG_INFO("New connection established (total pods: %d)", objs->num_pods);
}

/**
 * Callback for disconnection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the disconnection was successful or not
 */
static void server_disconnection_event_callback(struct doca_comch_event_connection_status_changed *event,
						struct doca_comch_connection *comch_connection,
						uint8_t change_success)
{
	(void)event;
	(void)comch_connection;

	if (change_success == 0)
		DOCA_LOG_ERR("Failed disconnection received");
}

static void server_state_changed_callback(const union doca_data user_data,
					  struct doca_ctx *ctx,
					  enum doca_ctx_states prev_state,
					  enum doca_ctx_states next_state)
{
	(void)ctx;
	(void)prev_state;
	struct objects *objs = (struct objects *)user_data.ptr;
	(void)objs;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("CC server context is idle");
		break;
	case DOCA_CTX_STATE_STARTING:
		DOCA_LOG_INFO("CC server context is starting");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("CC server context is running. Waiting for clients to connect");
		break;
	case DOCA_CTX_STATE_STOPPING:
		DOCA_LOG_INFO("CC server context is stopping");
		break;
	default:
		break;
	}
}

doca_error_t
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
    struct doca_ctx *ctx;
    union doca_data user_data;
    uint32_t max_msg_size, max_rq_size;
	struct timespec ts = {
		.tv_nsec = SLEEP_IN_NANOS,
	};

	/* create a progress engine */
    result = doca_pe_create(&(objs->pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
        return result;
    }
	
    result = doca_comch_server_create(objs->dev, objs->rep_dev,
                server_name, &objs->cc_server);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create server with error = %s", doca_error_get_name(result));
        goto destroy_pe;
    }

    ctx = doca_comch_server_as_ctx(objs->cc_server);

    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_ctx_set_state_changed_cb(ctx, server_state_changed_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting state change callback with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_task_send_set_conf(objs->cc_server,
                server_send_task_completion_callback,
                server_send_task_completion_err_callback,
                CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_msg_recv_register(objs->cc_server, server_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_connection_status_changed_register(objs->cc_server,
                                        server_connection_event_callback,
                                        server_disconnection_event_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding connection status changed event cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }                                        

    /* Config the data_path related events */
	if (is_fast_path) {
		result = doca_comch_server_event_consumer_register(objs->cc_server,
									server_new_consumer_callback,
									expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_server;
		}
	}

    result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    } 

    result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }
    
    result = doca_comch_server_set_max_msg_size(objs->cc_server, max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_set_recv_queue_size(objs->cc_server, CC_RECV_QUEUE_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    user_data.ptr = (void *)objs;
    result = doca_ctx_set_user_data(ctx, user_data);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_ctx_start(ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start server context with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	while (objs->connection == NULL) {
		if (doca_pe_progress(objs->pe) == 0)
			nanosleep(&ts, &ts);
	}

	DOCA_LOG_INFO("Server connection established");

    return DOCA_SUCCESS;

destroy_server:
    doca_comch_server_destroy(objs->cc_server);
    objs->cc_server = NULL;
destroy_pe:
    doca_pe_destroy(objs->pe);
    objs->pe = NULL;
    return result;
}

doca_error_t
export_dpa_comp_to_host(struct objects *objs)
{
	doca_error_t result;
	struct dmesh_dpa_comp_msg dpa_comp_msg;
	dpa_comp_msg.type = DMESH_MSG_EXPORT_DPA_COMP;

	result = doca_comch_consumer_completion_get_dpa_handle(objs->dpa_comch->consumer_comp,
									&dpa_comp_msg.dpa_consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_dpa_completion_get_dpa_handle(objs->dpa_comch->producer_comp,
									&dpa_comp_msg.dpa_producer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_producer_get_dpa_handle(objs->dpa_comch->recv.producer,
									&dpa_comp_msg.dpa_producer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_consumer_get_dpa_handle(objs->dpa_comch->send.consumer,
									&dpa_comp_msg.dpa_consumer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer handle - %s",
				doca_error_get_name(result));
		return result;
	}

	DOCA_LOG_INFO("dpa_consumer_comp: 0x%lx, dpa_producer_comp: 0x%lx, dpa_producer: 0x%lx, dpa_consumer: 0x%lx",
			dpa_comp_msg.dpa_consumer_comp,
			dpa_comp_msg.dpa_producer_comp,
			dpa_comp_msg.dpa_producer,
			dpa_comp_msg.dpa_consumer);

	return server_send_msg(objs, (const char *)&dpa_comp_msg, sizeof(dpa_comp_msg));
}

doca_error_t
server_send_rx_data(struct objects *objs,
                    const void *desc, uint32_t desc_len,
                    const void *body, uint32_t body_len)
{
	size_t total = sizeof(struct dmesh_rx_data_msg) + body_len;

	/* desc must be exactly sw_descriptor_t size (64 bytes) */
	if (desc_len != 64) {
		DOCA_LOG_ERR("server_send_rx_data: bad desc_len=%u (expected 64)", desc_len);
		return DOCA_ERROR_INVALID_VALUE;
	}

	uint8_t *buf = (uint8_t *)malloc(total);
	if (!buf) {
		DOCA_LOG_ERR("server_send_rx_data: malloc failed for %zu bytes", total);
		return DOCA_ERROR_NO_MEMORY;
	}

	struct dmesh_rx_data_msg *msg = (struct dmesh_rx_data_msg *)buf;
	msg->type = DMESH_MSG_RX_DATA;
	memcpy(msg->desc, desc, 64);
	msg->body_len = body_len;
	if (body_len > 0)
		memcpy(msg->body, body, body_len);

	doca_error_t result = server_send_msg(objs, (const char *)buf, total);
	free(buf);
	return result;
}

/* ====================================================================
 * Send RX data to a specific connection (for multi-pod routing)
 * ==================================================================== */

static doca_error_t
server_send_msg_to(struct objects *objs, struct doca_comch_connection *conn,
                   const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, conn,
							(void *)msg, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("server_send_msg_to: alloc failed: %s", doca_error_get_name(result));
		return result;
	}

	result = doca_task_submit(doca_comch_task_send_as_task(task));
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("server_send_msg_to: submit failed: %s", doca_error_get_name(result));
		doca_task_free(doca_comch_task_send_as_task(task));
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t
server_send_rx_data_to(struct objects *objs,
                       struct doca_comch_connection *conn,
                       const void *desc, uint32_t desc_len,
                       const void *body, uint32_t body_len)
{
	size_t total = sizeof(struct dmesh_rx_data_msg) + body_len;

	if (desc_len != 64) {
		DOCA_LOG_ERR("server_send_rx_data_to: bad desc_len=%u", desc_len);
		return DOCA_ERROR_INVALID_VALUE;
	}

	uint8_t *buf = (uint8_t *)malloc(total);
	if (!buf)
		return DOCA_ERROR_NO_MEMORY;

	struct dmesh_rx_data_msg *msg = (struct dmesh_rx_data_msg *)buf;
	msg->type = DMESH_MSG_RX_DATA;
	memcpy(msg->desc, desc, 64);
	msg->body_len = body_len;
	if (body_len > 0)
		memcpy(msg->body, body, body_len);

	doca_error_t result = server_send_msg_to(objs, conn, (const char *)buf, total);
	free(buf);
	return result;
}

doca_error_t
server_send_tx_ack_to(struct objects *objs,
                      struct doca_comch_connection *conn,
                      uint32_t req_id,
                      int32_t dst_pod_id)
{
	struct dmesh_tx_ack_msg ack;
	ack.type = DMESH_MSG_TX_ACK;
	ack.req_id = req_id;
	ack.dst_pod_id = dst_pod_id;
	return server_send_msg_to(objs, conn, (const char *)&ack, sizeof(ack));
}

/* ====================================================================
 * Pod connection management
 * ==================================================================== */

int
pods_add_connection(struct objects *objs, struct doca_comch_connection *conn)
{
	pthread_mutex_lock(&objs->pods_lock);
	if (objs->num_pods >= MAX_PODS) {
		pthread_mutex_unlock(&objs->pods_lock);
		DOCA_LOG_ERR("pods_add_connection: table full (%d)", MAX_PODS);
		return -1;
	}

	int idx = objs->num_pods;
	objs->pods[idx].connection = conn;
	objs->pods[idx].pod_id = -1;  /* not yet registered */
	objs->pods[idx].app_name[0] = '\0';
	objs->pods[idx].registered = 0;
	objs->num_pods++;
	pthread_mutex_unlock(&objs->pods_lock);

	DOCA_LOG_INFO("pods_add_connection: slot %d", idx);
	return 0;
}

int
pods_register(struct objects *objs, struct doca_comch_connection *conn,
              int32_t pod_id, const char *app_name)
{
	pthread_mutex_lock(&objs->pods_lock);
	for (int i = 0; i < objs->num_pods; i++) {
		if (objs->pods[i].connection == conn) {
			objs->pods[i].pod_id = pod_id;
			snprintf(objs->pods[i].app_name, sizeof(objs->pods[i].app_name),
			         "%s", app_name);
			objs->pods[i].registered = 1;
			pthread_mutex_unlock(&objs->pods_lock);
			DOCA_LOG_INFO("pods_register: slot %d → pod_id=%d app=%s",
			              i, pod_id, app_name);
			return 0;
		}
	}
	pthread_mutex_unlock(&objs->pods_lock);
	DOCA_LOG_ERR("pods_register: connection not found for pod_id=%d", pod_id);
	return -1;
}

struct pod_state *
find_pod_by_id(struct objects *objs, int32_t pod_id)
{
	pthread_mutex_lock(&objs->pods_lock);
	for (int i = 0; i < objs->num_pods; i++) {
		if (objs->pods[i].registered && objs->pods[i].pod_id == pod_id) {
			pthread_mutex_unlock(&objs->pods_lock);
			return &objs->pods[i];
		}
	}
	pthread_mutex_unlock(&objs->pods_lock);
	return NULL;
}

struct pod_state *
find_pod_by_connection(struct objects *objs, struct doca_comch_connection *conn)
{
	pthread_mutex_lock(&objs->pods_lock);
	for (int i = 0; i < objs->num_pods; i++) {
		if (objs->pods[i].connection == conn) {
			pthread_mutex_unlock(&objs->pods_lock);
			return &objs->pods[i];
		}
	}
	pthread_mutex_unlock(&objs->pods_lock);
	return NULL;
}