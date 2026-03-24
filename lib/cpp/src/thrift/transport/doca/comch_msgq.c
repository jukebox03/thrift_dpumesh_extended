#include "comch_msgq.h"

#include "dpa.h"
#include "object.h"
#include "dpa_common.h"
#include "comch_common.h"
#include <doca_log.h>
#include <doca_pe.h>
#include <doca_build_config.h>

DOCA_LOG_REGISTER(COMCH_MSGQ);

doca_error_t 
init_comch_dpa_msgq(struct objects *objs, struct doca_pe *pe)
{
	doca_error_t result;
	
	result = dmesh_doca_dpa_comch_create(objs);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA comch.");
		return result;
	}

	struct dmesh_doca_dpa_msgq_create_attr msgq_attr = {
		.dev = objs->dev,
		.dpa = objs->dpa_thread->dpa,
		.max_num_msg = CC_DPA_MAX_MSG_NUM,
		.consumer_comp = objs->dpa_comch->consumer_comp,
		.producer_comp = objs->dpa_comch->producer_comp,
		.pe = pe,
		.ctx_state_changed_cb = dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb,
		.ctx_user_data = objs,
	};

	msgq_attr.is_send = true;
	result = dmesh_doca_dpa_msgq_create(&msgq_attr, &objs->dpa_comch->send);
	if (result != DOCA_SUCCESS)
		return result;

	msgq_attr.is_send = false;
	result = dmesh_doca_dpa_msgq_create(&msgq_attr, &objs->dpa_comch->recv);
	if (result != DOCA_SUCCESS)
		return result;

	return DOCA_SUCCESS;
}