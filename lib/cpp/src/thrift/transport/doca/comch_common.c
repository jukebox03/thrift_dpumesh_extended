#include "comch_common.h"

#include <doca_log.h>
#include <doca_error.h>
#include <doca_mmap.h>
#include <doca_dpa.h>

#include "object.h"
#include "comch_client.h"
#include "comch_server.h"
#include "dpa.h"
#include "dpumesh_common.h"
DOCA_LOG_REGISTER(COMCH_COMMON);


doca_error_t
export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction)
{
    doca_error_t result;
    struct dmesh_mmap_msg *msg;
    const void *export_desc;
	size_t export_desc_len;
    char export_msg[4096];

    result = doca_mmap_export_pci(mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export local mmap to DPU: %s", doca_error_get_descr(result));
        return result;
    }

    DOCA_LOG_INFO("Successfully exported local mmap to DPU, export descriptor length: %zu bytes",
                  export_desc_len);

    msg = (struct dmesh_mmap_msg *)export_msg;
    msg->type = DMESH_MSG_EXPORT_DESC;
    msg->mmap_type = mmap_type;
    msg->host_addr = (void *)htonq((uint64_t)buffer);
    msg->buf_size = htonq((uint64_t)buf_size);
    msg->export_desc_len = htonq(export_desc_len);
    memcpy(msg->export_desc, export_desc, export_desc_len);
    
    /* Send export descriptor to DPU via comch */
    if (direction == HOST_TO_DPU) {
        return client_send_msg(objs, (const char *)msg, sizeof(struct dmesh_mmap_msg) + export_desc_len);
    } else {
        return server_send_msg(objs, (const char *)msg, sizeof(struct dmesh_mmap_msg) + export_desc_len);
    }
}

/* Forward declaration — implemented in dpa.c */
doca_error_t setup_pod_dma(struct objects *objs, struct pod_state *pod);

doca_error_t
process_mmap_msg(struct objects *objs, struct doca_comch_connection *conn,
                 struct dmesh_mmap_msg *mmap_msg)
{
	doca_error_t result;
	struct doca_mmap **mmap;
	void *remote_addr = (void *)ntohq((uint64_t)mmap_msg->host_addr);
	size_t buf_size = ntohq(mmap_msg->buf_size);
	size_t export_desc_len = ntohq(mmap_msg->export_desc_len);

	DOCA_LOG_INFO("remote_addr: %p, buf_size: %zu, export_desc_len: %zu",
		      remote_addr, buf_size, export_desc_len);

#ifdef DOCA_ARCH_DPU
	/* DPU side: store per-pod */
	struct pod_state *pod = find_pod_by_connection(objs, conn);
	if (!pod) {
		DOCA_LOG_ERR("process_mmap_msg: no pod found for connection");
		return DOCA_ERROR_NOT_FOUND;
	}

	if (mmap_msg->mmap_type == DMA_RING) {
		mmap = &pod->ring_mmap;
	} else if (mmap_msg->mmap_type == DMA_BUFFER) {
		mmap = &pod->remote_mmap;
	} else if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER) {
		mmap = &pod->host_rx_mmap;
	} else {
		DOCA_LOG_ERR("Invalid mmap type received: %d", mmap_msg->mmap_type);
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = doca_mmap_create_from_export(NULL, mmap_msg->export_desc,
					      export_desc_len,
					      objs->dev,
					      mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create remote mmap from export desc: %s",
			     doca_error_get_name(result));
		return result;
	}

	if (remote_addr == NULL || buf_size == 0) {
		DOCA_LOG_ERR("Invalid remote mmap metadata: remote_addr=%p buf_size=%zu",
			     remote_addr, buf_size);
		return DOCA_ERROR_INVALID_VALUE;
	}

	if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER) {
		pod->host_rx_addr = remote_addr;
		pod->host_rx_buf_size = buf_size;
		/* rq_depth derived from host_rx buffer size: num_slots × slot_size. */
		pod->rq_depth = (uint32_t)(buf_size / DPUMESH_SLOT_SIZE);
		DOCA_LOG_INFO("Pod %d: Host RX buffer stored (addr=%p, size=%zu, rq_depth=%u)",
			      pod->pod_id, remote_addr, buf_size, pod->rq_depth);
	} else {
		pod->remote_addr = remote_addr;
		pod->remote_buf_size = buf_size;
	}

	DOCA_LOG_INFO("Pod %d: mmap_type=%d stored (ring_mmap=%p, remote_mmap=%p, host_rx_mmap=%p)",
		      pod->pod_id, mmap_msg->mmap_type,
		      (void *)pod->ring_mmap, (void *)pod->remote_mmap, (void *)pod->host_rx_mmap);

	/* Trigger per-pod DMA setup when both forward-direction mmaps have arrived */
	if (pod->ring_mmap && pod->remote_mmap && !pod->dma_ready) {
		result = setup_pod_dma(objs, pod);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("setup_pod_dma failed for pod %d: %s",
				     pod->pod_id, doca_error_get_descr(result));
			return result;
		}
	}

	/* If Host RX buffer arrived after DMA setup, update the DPA reverse ring info */
	if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER && pod->dma_ready) {
		result = update_rev_ring_host_rx(objs, pod);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_WARN("update_rev_ring_host_rx failed for pod %d: %s",
				      pod->pod_id, doca_error_get_descr(result));
		}
	}
#else
	/* Host side: store the imported mmap in objs. The host holds only its
	 * own ring/data buffers; the per-pod table lives on the DPU. */
	(void)conn;
	if (mmap_msg->mmap_type == DMA_BUFFER) {
		mmap = &objs->remote_mmap;
	} else if (mmap_msg->mmap_type == DMA_RING) {
		mmap = &objs->ring_mmap;
	} else {
		DOCA_LOG_ERR("Invalid mmap type received: %d", mmap_msg->mmap_type);
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = doca_mmap_create_from_export(NULL, mmap_msg->export_desc,
					      export_desc_len,
					      objs->dev,
					      mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create remote mmap from export desc: %s",
			     doca_error_get_name(result));
		return result;
	}

	if (remote_addr == NULL || buf_size == 0) {
		DOCA_LOG_ERR("Invalid remote mmap metadata: remote_addr=%p buf_size=%zu",
			     remote_addr, buf_size);
		return DOCA_ERROR_INVALID_VALUE;
	}

	objs->remote_addr = remote_addr;
	objs->remote_buf_size = buf_size;
#endif

	return DOCA_SUCCESS;
}
