#include "object.h"

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

DOCA_LOG_REGISTER(OBJECT);

void
cleanup_objects(struct objects *objs)
{
    doca_error_t result;
    
    if (objs->cc_server) {
        result = doca_comch_server_destroy(objs->cc_server);
        if(result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy cc server properly with error = %s", doca_error_get_name(result));
        }   
        objs->cc_server = NULL;
    }

    if (objs->pe) {
        result = doca_pe_destroy(objs->pe);
        if(result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy pe properly with error = %s", doca_error_get_name(result));
        }
        objs->pe = NULL;
    }

    if (objs->rep_dev) {
        result = doca_dev_rep_close(objs->rep_dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close rep device properly with error = %s", doca_error_get_name(result));
        }
        objs->rep_dev = NULL;
    }

    if (objs->dev) {
        result = doca_dev_close(objs->dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close device properly with error = %s", doca_error_get_name(result));
        }
        objs->dev = NULL;
    }
}