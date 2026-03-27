#ifndef COMCH_PRODUCER_H
#define COMCH_PRODUCER_H

#include <doca_comch_producer.h>
#include <doca_comch.h>
#include <doca_ctx.h>

struct local_mem_bufs;

struct comch_producer_cb_config {
	/* User specified callback when task completed successfully */
	doca_comch_producer_task_send_completion_cb_t send_task_comp_cb;
	/* User specified callback when task completed with error */
	doca_comch_producer_task_send_completion_cb_t send_task_comp_err_cb;
	/* User specified context data */
	void *ctx_user_data;
	/* User specified PE context state changed event callback */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb;
};

struct objects;

doca_error_t
init_comch_datapath_producer(struct objects *objs);

doca_error_t
init_comch_datapath_producer_for_connection(struct objects *objs,
											struct doca_comch_connection *connection,
											struct local_mem_bufs **producer_mem,
											struct doca_comch_producer **producer,
											struct doca_pe **producer_pe);

doca_error_t
comch_datapath_send_payload(struct doca_comch_producer *producer,
							struct local_mem_bufs *producer_mem,
							uint32_t remote_consumer_id,
							const void *payload,
							uint32_t payload_len);

#endif /* COMCH_PRODUCER_H */