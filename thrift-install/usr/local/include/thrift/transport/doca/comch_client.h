#ifndef COMCH_CLIENT_H
#define COMCH_CLIENT_H

#include <doca_error.h>
#include <stdbool.h>
#include "common.h"

struct objects; /* Forward declaration */

#define CC_SEND_TASK_NUM 8192 /* Number of CC send tasks (HW max ~65536) */
#define CC_CLIENT_RECV_QUEUE_SIZE 8192 /* Size of CC receive queue (client side) */

doca_error_t init_comch_ctrl_path_client(const char *server_name,
                    struct objects *objs, bool is_fast_path);

doca_error_t
client_send_msg(struct objects *objs, const char *msg, size_t len);

#endif // COMCH_CLIENT_H