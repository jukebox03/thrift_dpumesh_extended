#ifndef COMCH_CLIENT_H
#define COMCH_CLIENT_H

#include <doca_error.h>
#include <doca_mmap.h>
#include <stdbool.h>
#include "common.h"

struct objects; /* Forward declaration */

#define CC_SEND_TASK_NUM 1024 /* Number of CC send tasks  */
#define CC_RECV_QUEUE_SIZE 1024 /* Size of CC receive queue */

#define STR_START_DATA_PATH_TEST "start_data_path_test" /* The negotiation message between client and server */
#define STR_STOP_DATA_PATH_TEST "stop_data_path_test"	/* The negotiation message between client and server */

doca_error_t 
start_comch_data_path_client(const char *server_name, struct objects *objs);

doca_error_t init_comch_ctrl_path_client(const char *server_name,
                    struct objects *objs, bool is_fast_path);

doca_error_t
client_send_msg(struct objects *objs, const char *msg, size_t len);

#endif // COMCH_CLIENT_H