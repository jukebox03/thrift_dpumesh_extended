#include "object.h"

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

#include "comch_server.h"  /* CC_SEND_TASK_NUM */

DOCA_LOG_REGISTER(OBJECT);

/* Call at startup (before any submit / after ctx created) to prime the
 * capacity model. Safe to call from init_comch_ctrl_path_{server,client}. */
void
objects_init_task_pools(struct objects *objs)
{
    atomic_store(&objs->send_tasks_in_flight, 0);
    objs->send_tasks_max = CC_SEND_TASK_NUM;
    atomic_store(&objs->recv_tasks_in_flight, 0);
    objs->recv_tasks_max = 0;   /* set later in init_comch_datapath_consumer */

    objs->num_consumer_retry = 0;
    pthread_mutex_init(&objs->consumer_retry_lock, NULL);
}

int
objects_drain_consumer_retry(struct objects *objs)
{
    int submitted = 0;
    pthread_mutex_lock(&objs->consumer_retry_lock);
    int remaining = 0;
    for (int i = 0; i < objs->num_consumer_retry; i++) {
        struct doca_task *t = objs->consumer_retry[i];
        /* _exact: single-PE-thread caller, no race — see comch_consumer.c */
        if (!doca_pool_try_acquire_exact(&objs->recv_tasks_in_flight, objs->recv_tasks_max)) {
            objs->consumer_retry[remaining++] = t;
            continue;
        }
        doca_error_t rc = doca_task_submit(t);
        if (rc == DOCA_SUCCESS) {
            submitted++;
        } else {
            doca_pool_release(&objs->recv_tasks_in_flight);
            objs->consumer_retry[remaining++] = t;
        }
    }
    objs->num_consumer_retry = remaining;
    pthread_mutex_unlock(&objs->consumer_retry_lock);
    return submitted;
}

void
cleanup_objects(struct objects *objs)
{
    doca_error_t result;
    
    if (objs->cc_server) {
        result = doca_comch_server_destroy(objs->cc_server);
        if(result != DOCA_SUCCESS) {
        }   
        objs->cc_server = NULL;
    }

    if (objs->pe) {
        result = doca_pe_destroy(objs->pe);
        if(result != DOCA_SUCCESS) {
        }
        objs->pe = NULL;
    }

    if (objs->rep_dev) {
        result = doca_dev_rep_close(objs->rep_dev);
        if (result != DOCA_SUCCESS) {
        }
        objs->rep_dev = NULL;
    }

    if (objs->dev) {
        result = doca_dev_close(objs->dev);
        if (result != DOCA_SUCCESS) {
        }
        objs->dev = NULL;
    }

    pthread_mutex_destroy(&objs->consumer_retry_lock);
}