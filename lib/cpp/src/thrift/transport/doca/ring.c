#include "ring.h"
#include <stdlib.h>
#include <string.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "dma.h"
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

    /* Descriptors must start as invalid; otherwise DPA may consume garbage slots. */
    memset(ring->descs, 0, ring->size * sizeof(struct dma_desc));
    
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

int setup_dpu_tx_ring(struct doca_dev *dev, size_t size,
                      struct dma_ring **out_ring, struct doca_mmap **out_mmap)
{
    doca_error_t result;
    struct dma_ring *ring;

    ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    if (!ring) return DOCA_ERROR_NO_MEMORY;

    ring->size = size;
    ring->head = 0;
    ring->tail = 0;
    ring->descs = NULL;

    result = alloc_buffer_and_set_mmap(&ring->mmap, dev,
                           (void **)&ring->descs,
                           ring->size * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DPU TX ring: %s", doca_error_get_descr(result));
        free(ring);
        return result;
    }

    memset(ring->descs, 0, ring->size * sizeof(struct dma_desc));

    *out_ring = ring;
    *out_mmap = ring->mmap;
    return 0;
}

struct dma_desc *get_next_dma_desc(struct dma_ring *ring)
{
    /* Valid bit is owned by DPA consumer; if still set, producer must not overwrite. */
    struct dma_desc *desc = ring->descs + ring->head;

    if (desc->valid) {
        DOCA_LOG_WARN("DMA ring busy at head=%u (size=%u)", ring->head, ring->size);
        return NULL;
    }

    uint32_t next_head = (ring->head + 1) % ring->size;
    ring->head = next_head;
    DOCA_LOG_DBG("Get next DMA desc - head: %u, desc: %p", ring->head, desc);
    return desc;
}
