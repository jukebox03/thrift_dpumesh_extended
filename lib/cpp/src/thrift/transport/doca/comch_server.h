#ifndef COMCH_SERVER_H
#define COMCH_SERVER_H

#include <stdbool.h>
#include <doca_comch.h>
#include <doca_ctx.h>

struct objects; /* Forward declaration */

#define CC_SEND_TASK_NUM 1024 /* Number of CC send tasks  */
#define CC_RECV_QUEUE_SIZE 1024 /* Size of CC receive queue */

#define STR_START_DATA_PATH_TEST "start_data_path_test" /* The negotiation message between client and server */
#define STR_STOP_DATA_PATH_TEST "stop_data_path_test"	/* The negotiation message between client and server */

#ifndef SLEEP_IN_NANOS
#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */
#endif

struct comch_ctrl_path_server_cb_config {
	/* User specified callback when task completed successfully */
	doca_comch_task_send_completion_cb_t send_task_comp_cb;
	/* User specified callback when task completed with error */
	doca_comch_task_send_completion_cb_t send_task_comp_err_cb;
	/* User specified callback when a message is received */
	doca_comch_event_msg_recv_cb_t msg_recv_cb;
	/* User specified callback when server receives a new connection */
	doca_comch_event_connection_status_changed_cb_t server_connection_event_cb;
	/* User specified callback when server finds a disconnected connection */
	doca_comch_event_connection_status_changed_cb_t server_disconnection_event_cb;
	/* Whether need to configure data_path related event callback */
	bool data_path_mode;
	/* User specified callback when a new consumer registered */
	doca_comch_event_consumer_cb_t new_consumer_cb;
	/* User specified callback when a consumer expired event occurs */
	doca_comch_event_consumer_cb_t expired_consumer_cb;
	/* User specified context data */
	void *ctx_user_data;
	/* User specified PE context state changed event callback */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb;
};

doca_error_t start_comch_data_path_server(const char *server_name,
							struct objects *objs);

doca_error_t 
init_comch_dpa_datapath(struct objects *objs);

doca_error_t 
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path);							

doca_error_t 
server_send_msg(struct objects *objs, const char *msg, size_t len);

doca_error_t
export_dpa_comp_to_host(struct objects *objs);

/* Send RX data from DPU to Host via comch control path (temporary path) */
doca_error_t
server_send_rx_data(struct objects *objs,
                    const void *desc, uint32_t desc_len,
                    const void *body, uint32_t body_len);

/* Send RX data to a specific connection (for multi-pod routing) */
doca_error_t
server_send_rx_data_to(struct objects *objs,
                       struct doca_comch_connection *conn,
                       const void *desc, uint32_t desc_len,
                       const void *body, uint32_t body_len);

/* Find a pod by pod_id. Returns NULL if not found. */
struct pod_state *
find_pod_by_id(struct objects *objs, int32_t pod_id);

/* Find a pod by connection. Returns NULL if not found. */
struct pod_state *
find_pod_by_connection(struct objects *objs, struct doca_comch_connection *conn);

/* Register a new connection in the pods table. Returns 0 on success. */
int
pods_add_connection(struct objects *objs, struct doca_comch_connection *conn);

/* Register pod_id for an existing connection. Returns 0 on success. */
int
pods_register(struct objects *objs, struct doca_comch_connection *conn,
              int32_t pod_id, const char *app_name);

#endif // COMCH_SERVER_H