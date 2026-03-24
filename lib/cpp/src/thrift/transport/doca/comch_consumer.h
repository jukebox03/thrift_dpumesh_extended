#ifndef COMCH_CONSUMER_H
#define COMCH_CONSUMER_H

#include <doca_comch_consumer.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#define CC_DATA_PATH_MAX_MSG_SIZE (1024 * 1024) /* CC DATA PATH maximum message size */
#define CC_DATA_PATH_TASK_NUM       256
#define CC_DATA_PATH_MSG_SIZE		64
#define INVALID_CONSUMER_ID 0xffff

struct comch_consumer_cb_config {
	/* User specified callback when task completed successfully */
	doca_comch_consumer_task_post_recv_completion_cb_t recv_task_comp_cb;
	/* User specified callback when task completed with error */
	doca_comch_consumer_task_post_recv_completion_cb_t recv_task_comp_err_cb;
	/* User specified context data */
	void *ctx_user_data;
	/* User specified PE context state changed event callback */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb;
};

struct objects; /* Forward declaration */

doca_error_t
init_comch_datapath_consumer(struct objects *objs);

void 
server_new_consumer_callback(struct doca_comch_event_consumer *event,
				  struct doca_comch_connection *comch_connection,
				  uint32_t id);

void
client_new_consumer_callback(struct doca_comch_event_consumer *event,
				  struct doca_comch_connection *comch_connection,
				  uint32_t id);

void 
expired_consumer_callback(struct doca_comch_event_consumer *event,
			       struct doca_comch_connection *comch_connection,
			       uint32_t id);

#endif /* COMCH_CONSUMER_H */