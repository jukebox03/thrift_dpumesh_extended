
#include "comch_server.h"

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "object.h"
#include "comch_common.h"
#include "comch_consumer.h"

#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_log.h>

DOCA_LOG_REGISTER(COMCH_SERVER);

static void server_send_task_completion_callback(struct doca_comch_task_send *task,
						 union doca_data task_user_data,
						 union doca_data ctx_user_data)
{
	struct objects *objs;
	void *payload_copy = task_user_data.ptr;

	objs = (struct objects *)ctx_user_data.ptr;
	doca_pool_release(&objs->send_tasks_in_flight);
	if (payload_copy != NULL)
		free(payload_copy);
	doca_task_free(doca_comch_task_send_as_task(task));
}

static void server_send_task_completion_err_callback(struct doca_comch_task_send *task,
						     union doca_data task_user_data,
						     union doca_data ctx_user_data)
{
	struct objects *objs = (struct objects *)ctx_user_data.ptr;
	void *payload_copy = task_user_data.ptr;

	doca_pool_release(&objs->send_tasks_in_flight);
	DOCA_LOG_ERR("Server send task failed: %s",
		     doca_error_get_name(doca_task_get_status(doca_comch_task_send_as_task(task))));
	if (payload_copy != NULL)
		free(payload_copy);
	doca_task_free(doca_comch_task_send_as_task(task));
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
	void *msg_copy;
	union doca_data task_user_data;
	struct doca_task *task_obj;

	/* Capacity gate on our mirror of DOCA's send pool so we never trigger
	 * DOCA_ERROR_AGAIN. This path is init-only (mmap/handle export), so we
	 * progress PE while waiting to make room. */
	int acq_retry = 0;
	while (!doca_pool_try_acquire(&objs->send_tasks_in_flight, objs->send_tasks_max)) {
		progress_all_pes(objs);
		if (++acq_retry > 10000) {
			DOCA_LOG_ERR("server_send_msg: send pool full after %d PE progresses", acq_retry);
			return DOCA_ERROR_AGAIN;
		}
	}

	msg_copy = malloc(len);
	if (msg_copy == NULL) {
		DOCA_LOG_ERR("Failed to allocate server payload copy");
		doca_pool_release(&objs->send_tasks_in_flight);
		return DOCA_ERROR_NO_MEMORY;
	}
	memcpy(msg_copy, msg, len);

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, objs->connection,
								msg_copy, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to allocate server task with error = %s", doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		return result;
	}

	task_obj = doca_comch_task_send_as_task(task);
	task_user_data.ptr = msg_copy;
	doca_task_set_user_data(task_obj, task_user_data);

	result = doca_task_submit(task_obj);
	if (result != DOCA_SUCCESS) {
		/* Capacity gated ahead of time; handle defensively. */
		DOCA_LOG_ERR("Failed to send server task with error = %s",
		             doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		doca_task_free(task_obj);
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

	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;

	/* Track the primary (first) client's connection */
	if (objs->connection == NULL)
		objs->connection = comch_connection;

	comch_msg = (struct dmesh_comch_msg *)recv_buffer;

	switch (comch_msg->type) {
	case DMESH_MSG_MMAP_EXPORT:

		if (msg_len <= sizeof(struct dmesh_mmap_msg)) {
			DOCA_LOG_ERR("Received invalid MMAP message from client");
			return;
		}
		result = process_mmap_msg(objs, comch_connection, (struct dmesh_mmap_msg *)recv_buffer);
		break;

	case DMESH_MSG_POD_REGISTER: {
		struct dmesh_register_msg *reg = (struct dmesh_register_msg *)recv_buffer;
		if (msg_len < sizeof(struct dmesh_register_msg)) {
			DOCA_LOG_ERR("Received invalid REGISTER message");
			return;
		}
		int assigned = pods_register(objs, comch_connection, reg->pod_id, reg->service_id);
		if (assigned >= 0) {
			/* Reply with the assigned pod_id so the host can address itself.
			 * Non-blocking send (no PE re-entry from this callback). */
			struct dmesh_pod_assigned_msg am = { .type = DMESH_MSG_POD_ASSIGNED };
			am.pod_id = assigned;
			doca_error_t sr = server_send_msg_to_conn(objs, comch_connection,
			                                          (const char *)&am, sizeof(am));
			if (sr != DOCA_SUCCESS)
				DOCA_LOG_ERR("POD_ASSIGNED send failed (pod_id=%d): %s",
				             assigned, doca_error_get_name(sr));
		}
		DOCA_LOG_INFO("Pod registered: pod_id=%d service_id=%d", assigned, reg->service_id);
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

	/* First connection is the primary */
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
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	(void)event;

	if (change_success == 0)
		DOCA_LOG_ERR("Disconnection reported as failed; cleaning up anyway");

	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("disconnect: failed to get user data from ctx: %s",
			     doca_error_get_name(result));
		return;
	}
	objs = (struct objects *)user_data.ptr;

	if (objs->connection == comch_connection)
		objs->connection = NULL;

	if (pods_remove_connection(objs, comch_connection) != 0)
		DOCA_LOG_WARN("disconnect: no pod slot matched the disconnected connection");
}

static void server_state_changed_callback(const union doca_data user_data,
					  struct doca_ctx *ctx,
					  enum doca_ctx_states prev_state,
					  enum doca_ctx_states next_state)
{
	(void)ctx;
	(void)prev_state;
	(void)user_data;

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

	/* Prime task-pool counters before anything that can submit. */
	objects_init_task_pools(objs);

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

    {
        uint32_t desired_rq = max_rq_size;
        if (desired_rq < CC_SERVER_RECV_QUEUE_SIZE) desired_rq = CC_SERVER_RECV_QUEUE_SIZE;
        result = doca_comch_server_set_recv_queue_size(objs->cc_server, desired_rq);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set recv queue size (%u) with error = %s",
                         desired_rq, doca_error_get_name(result));
            goto destroy_server;
        }
        DOCA_LOG_INFO("CC server recv queue size set to %u (cap=%u)", desired_rq, max_rq_size);
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

/* ====================================================================
 * Send a raw message to a specific connection (comch control path)
 * ==================================================================== */

doca_error_t
server_send_msg_to_conn(struct objects *objs, struct doca_comch_connection *conn,
                        const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;
	void *msg_copy;
	union doca_data task_user_data;
	struct doca_task *task_obj;

	/* Capacity gate on our mirror of DOCA's send pool. If full, return
	 * DOCA_ERROR_AGAIN so the caller can retain the work and retry later.
	 * No retry loop here: PE re-entry from a callback is unsafe. */
	if (!doca_pool_try_acquire(&objs->send_tasks_in_flight, objs->send_tasks_max))
		return DOCA_ERROR_AGAIN;

	msg_copy = malloc(len);
	if (msg_copy == NULL) {
		DOCA_LOG_ERR("server_send_msg_to_conn: payload copy allocation failed");
		doca_pool_release(&objs->send_tasks_in_flight);
		return DOCA_ERROR_NO_MEMORY;
	}
	memcpy(msg_copy, msg, len);

	result = doca_comch_server_task_send_alloc_init(objs->cc_server, conn,
							msg_copy, len, &task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("server_send_msg_to_conn: alloc failed: %s",
		             doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		return result;
	}

	task_obj = doca_comch_task_send_as_task(task);
	task_user_data.ptr = msg_copy;
	doca_task_set_user_data(task_obj, task_user_data);

	result = doca_task_submit(task_obj);
	if (result != DOCA_SUCCESS) {
		/* Capacity gated ahead of time — should not hit this, but be safe */
		DOCA_LOG_ERR("server_send_msg_to_conn: submit failed: %s",
		             doca_error_get_name(result));
		doca_pool_release(&objs->send_tasks_in_flight);
		free(msg_copy);
		doca_task_free(task_obj);
		return result;
	}

	return DOCA_SUCCESS;
}


/* Send a batched TX_ACK (n (port,seq) entries, 1..BATCH_TXACK_MAX) as one
 * message. Only the first 4 + 4*n bytes are transmitted. DOCA_ERROR_AGAIN if the
 * send pool is full (caller retains the batch). */
doca_error_t
server_send_batch_tx_ack_to(struct objects *objs,
                            struct doca_comch_connection *conn,
                            const struct dmesh_tx_ack_entry *acks, int n)
{
	if (n <= 0)
		return DOCA_SUCCESS;
	struct dmesh_batch_tx_ack_msg m;
	m.type = DMESH_MSG_BATCH_FWD_ACK;
	m.count = (uint8_t)n;
	m._pad[0] = m._pad[1] = 0;
	for (int i = 0; i < n; i++)
		m.acks[i] = acks[i];
	size_t wire = 4 + 4u * (size_t)n;   /* header + only the valid entries */
	return server_send_msg_to_conn(objs, conn, (const char *)&m, wire);
}

/* Send a batched REV_DONE (n completions, 1..BATCH_REVDONE_MAX) as one message.
 * Only 4 + 16*n bytes are transmitted. AGAIN if the send pool is full (caller
 * retains the batch). Mirrors server_send_batch_tx_ack_to. */
doca_error_t
server_send_batch_rev_done_to(struct objects *objs,
                              struct doca_comch_connection *conn,
                              const struct dmesh_rev_done_entry *entries, int n)
{
	if (n <= 0)
		return DOCA_SUCCESS;
	struct dmesh_batch_rev_done_msg m;
	m.type = DMESH_MSG_BATCH_REV_DONE;
	m.count = (uint8_t)n;
	m._pad[0] = m._pad[1] = 0;
	for (int i = 0; i < n; i++)
		m.entries[i] = entries[i];
	size_t wire = 4 + 16u * (size_t)n;   /* header + only the valid entries */
	return server_send_msg_to_conn(objs, conn, (const char *)&m, wire);
}

/* ====================================================================
 * Pod connection management
 * ==================================================================== */

int
pods_add_connection(struct objects *objs, struct doca_comch_connection *conn)
{
	/* See object.h pods[] concurrency model. Single writer (control PE
	 * callback); registered=0 so readers skip the slot until pods_register
	 * publishes it. The num_pods bump is the visibility gate for the slot's
	 * existence; do it last.
	 *
	 * Prefer REUSING a slot freed by pods_remove_connection (connection==NULL,
	 * registered==0) over always appending — otherwise the table exhausts after
	 * MAX_PODS cumulative connect/disconnect cycles even though ≤MAX_PODS are ever
	 * live. Slot indices stay stable; a freed slot is only reused on a later
	 * connect, by which point the disconnected pod is long gone (reconnect latency
	 * ≫ comp_queue drain), so no live comp_queue entry still references the index. */
	int n = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
	int idx = -1;
	for (int i = 0; i < n; i++) {
		if (objs->pods[i].connection == NULL &&
		    __atomic_load_n(&objs->pods[i].registered, __ATOMIC_ACQUIRE) == 0) {
			idx = i;   /* recycle a freed slot */
			break;
		}
	}
	if (idx < 0) {
		if (n >= MAX_PODS) {
			DOCA_LOG_ERR("pods_add_connection: table full (%d)", MAX_PODS);
			return -1;
		}
		idx = n;
	}

	objs->pods[idx].connection = conn;
	objs->pods[idx].pod_id = -1;  /* not yet registered */
	__atomic_store_n(&objs->pods[idx].registered, 0, __ATOMIC_RELEASE);
	if (idx == n)
		__atomic_store_n(&objs->num_pods, idx + 1, __ATOMIC_RELEASE);

	DOCA_LOG_INFO("pods_add_connection: slot %d%s", idx, (idx == n) ? "" : " (reused)");
	return 0;
}

int
pods_remove_connection(struct objects *objs, struct doca_comch_connection *conn)
{
	struct doca_mmap *ring_mmaps[MAX_EU_PER_POD] = {0};
	int ring_mmap_count = 0;
	struct doca_mmap *remote_mmap = NULL;
	struct doca_mmap *host_rx_mmap = NULL;
	int32_t pod_id = -1;
	int found_idx = -1;

	int n = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
	for (int i = 0; i < n; i++) {
		if (objs->pods[i].connection != conn)
			continue;
		found_idx = i;
		pod_id = objs->pods[i].pod_id;

		/* Capture the host-exported mmap views to destroy after tearing
		 * the slot down. These are DPU-side handles created via
		 * doca_mmap_create_from_export, so destroying them only releases
		 * the DPU-side handle. */
		ring_mmap_count = objs->pods[i].ring_mmap_count;
		for (int j = 0; j < ring_mmap_count && j < MAX_EU_PER_POD; j++)
			ring_mmaps[j] = objs->pods[i].ring_mmaps[j];
		remote_mmap  = objs->pods[i].remote_mmap;
		host_rx_mmap = objs->pods[i].host_rx_mmap;

		/* Mark slot dead in PUBLICATION-INVERSE order: store registered=0
		 * with RELEASE FIRST so any reader that observes registered=1 is
		 * guaranteed to also see a still-valid slot. The slot index is kept
		 * stable (not compacted) so any in-flight comp_queue entry with
		 * pod_idx == i hits the (connection == NULL) branch and ACKs the
		 * originator instead of dereferencing freed memory. */
		__atomic_store_n(&objs->pods[i].registered, 0, __ATOMIC_RELEASE);
		/* Clear the O(1) map so the freed pod_id no longer resolves to this
		 * dead slot and can be re-registered into a new slot. */
		if (pod_id >= 0 && pod_id < POD_ID_SPACE)
			__atomic_store_n(&objs->pod_id_to_slot[pod_id], -1, __ATOMIC_RELEASE);
		objs->pods[i].dma_ready       = 0;
		objs->pods[i].connection      = NULL;
		objs->pods[i].pod_id          = -1;
		for (int j = 0; j < ring_mmap_count && j < MAX_EU_PER_POD; j++)
			objs->pods[i].ring_mmaps[j] = NULL;
		objs->pods[i].ring_mmap_count = 0;
		objs->pods[i].remote_mmap     = NULL;
		objs->pods[i].host_rx_mmap    = NULL;
		break;
	}

	if (found_idx < 0)
		return -1;

	for (int j = 0; j < ring_mmap_count && j < MAX_EU_PER_POD; j++) {
		if (!ring_mmaps[j])
			continue;
		doca_error_t r = doca_mmap_destroy(ring_mmaps[j]);
		if (r != DOCA_SUCCESS)
			DOCA_LOG_WARN("disconnect: ring_mmap[%d] destroy failed: %s",
				      j, doca_error_get_name(r));
	}
	if (remote_mmap) {
		doca_error_t r = doca_mmap_destroy(remote_mmap);
		if (r != DOCA_SUCCESS)
			DOCA_LOG_WARN("disconnect: remote_mmap destroy failed: %s",
				      doca_error_get_name(r));
	}
	if (host_rx_mmap) {
		doca_error_t r = doca_mmap_destroy(host_rx_mmap);
		if (r != DOCA_SUCCESS)
			DOCA_LOG_WARN("disconnect: host_rx_mmap destroy failed: %s",
				      doca_error_get_name(r));
	}

	DOCA_LOG_INFO("pods_remove_connection: slot %d (pod_id=%d) invalidated",
		      found_idx, pod_id);
	return 0;
}

int
pods_register(struct objects *objs, struct doca_comch_connection *conn,
              int32_t pod_id, int32_t service_id)
{
	int n = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
	for (int i = 0; i < n; i++) {
		if (objs->pods[i].connection != conn)
			continue;

		/* DPU-assigned pod_id: the host no longer picks its own address. A
		 * register with pod_id < 0 gets the pods[] slot index — unique among
		 * live pods and always in [0, MAX_PODS) ⊂ [0, POD_ID_SPACE). */
		if (pod_id < 0)
			pod_id = i;

		/* Publication order: write all fields first, then the gate.
		 * Readers that observe registered=1 (ACQUIRE load) are guaranteed
		 * to see the prior pod_id write. */
		objs->pods[i].pod_id = pod_id;
		objs->pods[i].service_id = service_id;
		__atomic_store_n(&objs->pods[i].registered, 1, __ATOMIC_RELEASE);

		/* Publish the O(1) pod_id->slot map AFTER registered=1, so a reader
		 * that observes the map entry is guaranteed to also see registered=1.
		 * (find_pod_by_id re-validates registered + pod_id regardless.) */
		if (pod_id >= 0 && pod_id < POD_ID_SPACE)
			__atomic_store_n(&objs->pod_id_to_slot[pod_id], i, __ATOMIC_RELEASE);

		/* service_id -> pod resolution for dpu_route (first request of a
		 * connection). Multiple pods registering the same service_id are LB
		 * candidates; this mock keeps the last writer (future L7 picks). */
		if (service_id >= 0 && service_id < POD_ID_SPACE)
			__atomic_store_n(&objs->service_table[service_id], pod_id, __ATOMIC_RELEASE);

		DOCA_LOG_INFO("pods_register: slot %d → pod_id=%d service_id=%d",
		              i, pod_id, service_id);
		return pod_id;
	}
	DOCA_LOG_ERR("pods_register: connection not found");
	return -1;
}

struct pod_state *
find_pod_by_id(struct objects *objs, int32_t pod_id)
{
	/* Lock-free O(1) lookup via the pod_id->slot map. The map is only an
	 * accelerator: the registered ACQUIRE + pod_id re-check below remain the
	 * authority, so a stale/torn map entry can only yield a re-validated hit
	 * or NULL — never a wrong pod. See object.h pods[] concurrency model. */
	if (pod_id < 0 || pod_id >= POD_ID_SPACE)
		return NULL;
	int idx = __atomic_load_n(&objs->pod_id_to_slot[pod_id], __ATOMIC_ACQUIRE);
	if (idx < 0 || idx >= MAX_PODS)
		return NULL;
	struct pod_state *p = &objs->pods[idx];
	if (__atomic_load_n(&p->registered, __ATOMIC_ACQUIRE) && p->pod_id == pod_id)
		return p;
	return NULL;
}

struct pod_state *
find_pod_by_connection(struct objects *objs, struct doca_comch_connection *conn)
{
	/* Lock-free read keyed on the connection field (not the registered gate),
	 * used both pre-register and post-register. The connection pointer is
	 * NULL-ed in pods_remove_connection AFTER the registered=0 RELEASE store,
	 * so a non-NULL connection always corresponds to a valid slot. */
	int n = __atomic_load_n(&objs->num_pods, __ATOMIC_ACQUIRE);
	for (int i = 0; i < n; i++) {
		if (objs->pods[i].connection == conn)
			return &objs->pods[i];
	}
	return NULL;
}