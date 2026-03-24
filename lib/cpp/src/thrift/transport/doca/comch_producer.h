#ifndef COMCH_PRODUCER_H
#define COMCH_PRODUCER_H

#include <doca_comch_producer.h>
#include <doca_comch.h>
#include <doca_ctx.h>

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

#endif /* COMCH_PRODUCER_H */