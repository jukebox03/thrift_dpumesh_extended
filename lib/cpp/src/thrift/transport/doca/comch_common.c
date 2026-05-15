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
        return result;
    }


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

#ifdef DOCA_ARCH_DPU
/* Phase 4: broadcast peer topology. For every ordered pair (recv, peer)
 * of fully-registered pods, send recv the export descs of peer's
 * rx_dma_buffer and forward dma_ring. recv's host then mmap_create_from_export
 * both and stores them in its peer table for direct host→host chunk DMA. */
void broadcast_peer_topology(struct objects *objs)
{
	uint8_t buf[1024 + 1024 + sizeof(struct dmesh_peer_topology_msg) + 64];
	for (int i = 0; i < objs->num_pods; i++) {
		struct pod_state *recv = &objs->pods[i];
		if (!recv->registered || !recv->dma_ready || !recv->connection) continue;
		for (int j = 0; j < objs->num_pods; j++) {
			if (i == j) continue;
			struct pod_state *peer = &objs->pods[j];
			if (!peer->registered || !peer->dma_ready) continue;
			if (peer->host_rx_export_desc_len == 0 ||
			    peer->ring_export_desc_len == 0)
				continue;

			size_t msg_size = sizeof(struct dmesh_peer_topology_msg) +
			                  peer->host_rx_export_desc_len +
			                  peer->ring_export_desc_len;
			if (msg_size > sizeof(buf)) {
				continue;
			}
			struct dmesh_peer_topology_msg *m = (struct dmesh_peer_topology_msg *)buf;
			m->type = DMESH_MSG_PEER_TOPOLOGY;
			m->pod_id = peer->pod_id;
			m->src_id = (uint32_t)peer->pod_id;
			m->rx_addr = peer->host_rx_addr;
			m->rx_buf_size = peer->host_rx_buf_size;
			m->rq_depth = peer->rq_depth;
			m->slot_size = DPUMESH_SLOT_SIZE;
			m->rx_export_len = (uint32_t)peer->host_rx_export_desc_len;
			m->ring_export_len = (uint32_t)peer->ring_export_desc_len;
			memcpy(m->desc_data, peer->host_rx_export_desc,
			       peer->host_rx_export_desc_len);
			memcpy(m->desc_data + peer->host_rx_export_desc_len,
			       peer->ring_export_desc, peer->ring_export_desc_len);

			doca_error_t r = server_send_msg_to_conn(objs, recv->connection,
			                                          (const char *)m, msg_size);
		}
	}
}
#endif

doca_error_t
process_mmap_msg(struct objects *objs, struct doca_comch_connection *conn,
                 struct dmesh_mmap_msg *mmap_msg)
{
	doca_error_t result;
	struct doca_mmap **mmap;
	void *remote_addr = (void *)ntohq((uint64_t)mmap_msg->host_addr);
	size_t buf_size = ntohq(mmap_msg->buf_size);
	size_t export_desc_len = ntohq(mmap_msg->export_desc_len);


#ifdef DOCA_ARCH_DPU
	/* DPU side: store per-pod */
	struct pod_state *pod = find_pod_by_connection(objs, conn);
	if (!pod) {
		return DOCA_ERROR_NOT_FOUND;
	}

	if (mmap_msg->mmap_type == DMA_RING) {
		mmap = &pod->ring_mmap;
	} else if (mmap_msg->mmap_type == DMA_BUFFER) {
		mmap = &pod->remote_mmap;
	} else if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER) {
		mmap = &pod->host_rx_mmap;
	} else if (mmap_msg->mmap_type == DMA_HOST_TX_HDR_BUFFER) {
		mmap = &pod->remote_hdr_mmap;
	} else if (mmap_msg->mmap_type == DMA_HOST_RX_HDR_BUFFER) {
		mmap = &pod->host_hdr_rx_mmap;
	} else if (mmap_msg->mmap_type == DMA_HDR_RING) {
		mmap = &pod->hdr_ring_mmap;
	} else {
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = doca_mmap_create_from_export(NULL, mmap_msg->export_desc,
					      export_desc_len,
					      objs->dev,
					      mmap);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	if (remote_addr == NULL || buf_size == 0) {
		return DOCA_ERROR_INVALID_VALUE;
	}

	if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER) {
		pod->host_rx_addr = remote_addr;
		pod->host_rx_buf_size = buf_size;
		/* rq_depth derived from host_rx buffer size: num_slots × slot_size. */
		pod->rq_depth = (uint32_t)(buf_size / DPUMESH_SLOT_SIZE);
		/* Phase 4: cache the raw export desc for peer broadcast. */
		if (export_desc_len <= sizeof(pod->host_rx_export_desc)) {
			memcpy(pod->host_rx_export_desc, mmap_msg->export_desc, export_desc_len);
			pod->host_rx_export_desc_len = export_desc_len;
		} else {
		}
	} else if (mmap_msg->mmap_type == DMA_HOST_TX_HDR_BUFFER) {
		pod->remote_hdr_addr = remote_addr;
		pod->remote_hdr_buf_size = buf_size;
		/* Resolve DPA handle once now so the forward kernel can use it via
		 * desc->mmap override without doing a per-request lookup. */
		result = doca_mmap_dev_get_dpa_handle(pod->remote_hdr_mmap, objs->dev,
		                                      &pod->remote_hdr_dpa_handle);
		if (result != DOCA_SUCCESS) {
			return result;
		}
	} else if (mmap_msg->mmap_type == DMA_HOST_RX_HDR_BUFFER) {
		pod->host_hdr_rx_addr = remote_addr;
		pod->host_hdr_rx_buf_size = buf_size;
		/* Phase 2: DPA reverse kernel uses dst_mmap override via dma_desc.dst_mmap.
		 * Resolve the handle eagerly so DPU's hdr-flush path (Phase 3) just fills
		 * desc->dst_mmap = host_hdr_rx_dpa_handle without lookup. */
		result = doca_mmap_dev_get_dpa_handle(pod->host_hdr_rx_mmap, objs->dev,
		                                      &pod->host_hdr_rx_dpa_handle);
		if (result != DOCA_SUCCESS) {
			return result;
		}
	} else if (mmap_msg->mmap_type == DMA_HDR_RING) {
		pod->hdr_ring_addr = remote_addr;
		pod->hdr_ring_buf_size = buf_size;
	} else {
		pod->remote_addr = remote_addr;
		pod->remote_buf_size = buf_size;
		/* Phase 4: cache the dma_ring export desc too (DMA_RING case lands
		 * here too; mmap_type check below — we cache both BODY and RING but
		 * the broadcast only uses ring's). */
		if (mmap_msg->mmap_type == DMA_RING &&
		    export_desc_len <= sizeof(pod->ring_export_desc)) {
			memcpy(pod->ring_export_desc, mmap_msg->export_desc, export_desc_len);
			pod->ring_export_desc_len = export_desc_len;
		}
	}


	/* Trigger per-pod DMA setup when all forward-direction mmaps have arrived.
	 * Phase 3: include hdr_tx + host_hdr_rx so the ring info pushed to DPA
	 * carries the DPU-resolved hdr mmap handles. Without this, OP_HDR_BATCH
	 * forwards / reverses fall back to body mmap, which doesn't cover the
	 * hdr buffer VA range — DMAs fail silently.
	 * Phase 4: also wait for host_rx_mmap so broadcast_peer_topology can
	 * send the rx export desc to peers as soon as setup completes. */
	if (pod->ring_mmap && pod->remote_mmap && pod->remote_hdr_mmap &&
	    pod->host_hdr_rx_mmap && pod->host_rx_mmap && pod->hdr_ring_mmap &&
	    !pod->dma_ready) {
		result = setup_pod_dma(objs, pod);
		if (result != DOCA_SUCCESS) {
			return result;
		}
		/* Phase 4: now that this pod is fully set up, broadcast its
		 * topology to other ready pods AND send other pods' topology to
		 * this one. Direct host→host DMA needs each src to have the
		 * peer's rx_dma_buffer export desc + ring credit slot. */
		broadcast_peer_topology(objs);
	}

	/* If Host RX buffer arrived after DMA setup, update the DPA reverse ring info */
	if (mmap_msg->mmap_type == DMA_HOST_RX_BUFFER && pod->dma_ready) {
		result = update_rev_ring_host_rx(objs, pod);
		if (result != DOCA_SUCCESS) {
		}
	}
#else
	/* Host side: store in objs (backward compat, single client) */
	(void)conn;
	if (mmap_msg->mmap_type == DMA_BUFFER) {
		mmap = &objs->remote_mmap;
	} else if (mmap_msg->mmap_type == DMA_RING) {
		mmap = &objs->ring_mmap;
	} else {
		return DOCA_ERROR_INVALID_VALUE;
	}

	result = doca_mmap_create_from_export(NULL, mmap_msg->export_desc,
					      export_desc_len,
					      objs->dev,
					      mmap);
	if (result != DOCA_SUCCESS) {
		return result;
	}

	if (remote_addr == NULL || buf_size == 0) {
		return DOCA_ERROR_INVALID_VALUE;
	}

	objs->remote_addr = remote_addr;
	objs->remote_buf_size = buf_size;
#endif

	return DOCA_SUCCESS;
}

doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg)
{
    objs->remote_dpa_producer = dpa_comp_msg->dpa_producer;
    objs->remote_dpa_producer_comp = dpa_comp_msg->dpa_producer_comp;

    return DOCA_SUCCESS;
}
