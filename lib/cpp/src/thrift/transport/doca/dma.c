#include "dma.h"

#include <stdlib.h>

#include <doca_log.h>
#include <doca_dma.h>
#include <doca_mmap.h>
#include <doca_error.h>
#include <errno.h>
#include <arpa/inet.h>

#include "dpa_common.h"
#include "object.h"
#include "common.h"
#include "buffer.h"

DOCA_LOG_REGISTER(DMA);

#ifdef DOCA_ARCH_DPU
#include "dpa.h"

doca_error_t
init_dma_resources(struct objects *objs)
{
    doca_error_t result;

    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                                   &objs->dma_buffer, 1024 * 1024,
                                   DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    /* wait for remote mmap info from the host */
    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    return DOCA_SUCCESS;
}

doca_error_t
send_dma_request_to_dpa(struct objects *objs)
{
    doca_error_t result;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    struct comch_dma_req_msg dma_req_msg;

#ifdef DOCA_ARCH_DPU
    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    result = doca_mmap_dev_get_dpa_handle(objs->remote_mmap, objs->dev, &src_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }
#endif

    result = doca_mmap_dev_get_dpa_handle(objs->local_mmap, objs->dev, &dst_mmap);
    if (result != DOCA_SUCCESS) {
        return result;
    }

    dma_req_msg.type = COMCH_MSG_TYPE_DMA_REQ;
    dma_req_msg.dpa_producer = objs->remote_dpa_producer;
    doca_dpa_dev_completion_t tmp_comp;
    result = doca_dpa_completion_get_dpa_handle(objs->dpa_comch->producer_comp, &tmp_comp);
    if (result != DOCA_SUCCESS) {
        return result;
    }
    dma_req_msg.dpa_producer_comp = tmp_comp;
    dma_req_msg.src_mmap = src_mmap;
    dma_req_msg.dst_mmap = dst_mmap;
    dma_req_msg.src_addr = (uint64_t)objs->remote_addr;
    dma_req_msg.dst_addr = (uint64_t)objs->dma_buffer;
    dma_req_msg.length = 1024;

    result = dmesh_doca_dpa_msgq_send(&objs->dpa_comch->send,
                              &dma_req_msg,
                              sizeof(dma_req_msg));
    if (result != DOCA_SUCCESS) {
        return result;
    }

    return DOCA_SUCCESS;
}
#endif /* DOCA_ARCH_DPU */