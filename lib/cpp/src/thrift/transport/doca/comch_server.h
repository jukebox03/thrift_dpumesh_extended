#ifndef COMCH_SERVER_H
#define COMCH_SERVER_H

#include <stdbool.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#include "comch_common.h"  /* dmesh_rev_done_entry for server_send_batch_rev_done_to */

struct objects; /* Forward declaration */

#define CC_SEND_TASK_NUM 8192 /* Number of CC send tasks (HW max ~65536) */
#define CC_SERVER_RECV_QUEUE_SIZE 1024 /* Size of CC receive queue (server side) */

#ifndef SLEEP_IN_NANOS
#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */
#endif

doca_error_t 
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path);							

doca_error_t 
server_send_msg(struct objects *objs, const char *msg, size_t len);

/* Send a raw message to a specific connection (comch control path) */
doca_error_t
server_send_msg_to_conn(struct objects *objs, struct doca_comch_connection *conn,
                        const char *msg, size_t len);


/* Batched TX_ACK: coalesce n (port,seq) entries into one message. */
doca_error_t
server_send_batch_tx_ack_to(struct objects *objs,
							struct doca_comch_connection *conn,
							const struct dmesh_tx_ack_entry *acks, int n);

/* Batched REV_DONE: coalesce n reverse-DMA completions into one message. */
doca_error_t
server_send_batch_rev_done_to(struct objects *objs,
							  struct doca_comch_connection *conn,
							  const struct dmesh_rev_done_entry *entries, int n);

/* Find a pod by pod_id. Returns NULL if not found. */
struct pod_state *
find_pod_by_id(struct objects *objs, int32_t pod_id);

/* Find a pod by connection. Returns NULL if not found. */
struct pod_state *
find_pod_by_connection(struct objects *objs, struct doca_comch_connection *conn);

/* Register a new connection in the pods table. Returns 0 on success. */
int
pods_add_connection(struct objects *objs, struct doca_comch_connection *conn);

/* Invalidate the pod slot for the given connection on disconnect. Marks the
 * slot as not-registered, clears the connection pointer so future lookups
 * skip it, and destroys the host-exported mmap views held on the DPU side.
 * Local DPU buffers (dma_buffer, tx_ring) and the DPA-side ring registration
 * are not torn down here. Returns 0 if a slot was found and invalidated,
 * -1 if no slot matched. */
int
pods_remove_connection(struct objects *objs, struct doca_comch_connection *conn);

/* Register an existing connection. pod_id < 0 → the DPU assigns a free pod_id
 * (the pods[] slot index). Returns the assigned pod_id (>= 0), or -1 on error. */
int
pods_register(struct objects *objs, struct doca_comch_connection *conn,
              int32_t pod_id, int32_t service_id);

#endif // COMCH_SERVER_H