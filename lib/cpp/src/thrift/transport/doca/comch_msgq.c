#include "comch_msgq.h"

#ifdef DOCA_ARCH_DPU

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

	/* One 1c/1p comch channel per EU thread. Each channel's DPU-side ctx
	 * connects to the single consumer PE, drained by the worker loop. */
	for (int k = 0; k < objs->num_dpa_threads; k++) {
		result = dmesh_doca_dpa_comch_create(objs, k);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to create DPA comch for EU %d.", k);
			return result;
		}

		struct dmesh_doca_dpa_msgq_create_attr msgq_attr = {
			.dev = objs->dev,
			.dpa = objs->dpa,
			.max_num_msg = CC_DPA_MAX_MSG_NUM,
			.consumer_comp = objs->dpa_comches[k]->consumer_comp,
			.producer_comp = objs->dpa_comches[k]->producer_comp,
			.pe = pe,
			.ctx_state_changed_cb = dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb,
			.ctx_user_data = objs,
		};

		msgq_attr.is_send = true;
		result = dmesh_doca_dpa_msgq_create(&msgq_attr, &objs->dpa_comches[k]->send);
		if (result != DOCA_SUCCESS)
			return result;

		msgq_attr.is_send = false;
		result = dmesh_doca_dpa_msgq_create(&msgq_attr, &objs->dpa_comches[k]->recv);
		if (result != DOCA_SUCCESS)
			return result;
	}

	return DOCA_SUCCESS;
}
#endif /* DOCA_ARCH_DPU */
