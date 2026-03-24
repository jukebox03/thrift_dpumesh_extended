#include "ring.h"
#include <stdlib.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(RING);

int setup_dma_ring(struct objects *objs, size_t size)
{
    doca_error_t result;
    struct dma_ring *ring;

    if (objs->dma_ring == NULL) {
        objs->dma_ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    }

    ring = objs->dma_ring;
    ring->size = size;
    ring->head = 0;
    ring->tail = 0;
    ring->descs = NULL;

    /* allocate local buffer and set mmap for PCI export */
    result = alloc_buffer_and_set_mmap(&ring->mmap, objs->dev,
                           (void **)&ring->descs, 
                           ring->size * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        return result;
    }
    
    /* export mmap to DPU */
    result = export_mmap_to_remote(objs, ring->mmap, 
                                   ring->descs, 
                                   ring->size * sizeof(struct dma_desc), 
                                   DMA_RING, HOST_TO_DPU);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
        free(objs->dma_ring);
        destroy_mmap_and_free_buffer(ring->mmap, ring->descs);
        return result;
    }
    return 0;
}

struct dma_desc *get_next_dma_desc(struct dma_ring *ring)
{
    struct dma_desc *desc = ring->descs + ring->head;
    ring->head = (ring->head + 1) % ring->size;
    // DOCA_LOG_INFO("Get next DMA desc - head: %u, tail: %u, desc: %p", ring->head, ring->tail, desc);
    return desc;
}
