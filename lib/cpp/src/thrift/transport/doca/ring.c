#include "ring.h"
#include <stdlib.h>
#include <string.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(RING);

int setup_dma_ring(struct objects *objs, size_t size, struct dma_ring **out_ring)
{
    doca_error_t result;
    struct dma_ring *ring;

    ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    if (!ring)
        return DOCA_ERROR_NO_MEMORY;
    *out_ring = ring;
    ring->size = size;          /* logical ring size (host wraps at this) */
    ring->enq_pos = 0;
    ring->descs = NULL;
    ring->busy_probes = 0;

    /* Per-slot Vyukov cell sequence (host memory): seq[i]=i so slot i is first
     * writable by ticket i (generation 0). Lock-free MPSC producer state. */
    ring->seq = (uint64_t *)malloc((size_t)size * sizeof(uint64_t));
    if (!ring->seq) {
        free(ring);
        *out_ring = NULL;
        return DOCA_ERROR_NO_MEMORY;
    }
    for (size_t i = 0; i < (size_t)size; i++) ring->seq[i] = i;

    /* Allocate one EXTRA slot. Slots 0..size-1 are normal dma_desc entries;
     * slot `size` holds the RX credit counter. Host atomically bumps its first
     * 8 bytes on rx_free; DPA polls it via the same buf_arr (no separate mmap,
     * so no race with other PCIe reads). */
    size_t alloc_slots = ring->size + 1;
    result = alloc_buffer_and_set_mmap(&ring->mmap, objs->dev,
                           (void **)&ring->descs,
                           alloc_slots * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA resources: %s", doca_error_get_descr(result));
        free(ring->seq);
        free(ring);
        *out_ring = NULL;
        return result;
    }

    /* Descriptors must start as invalid; otherwise DPA may consume garbage slots. */
    memset(ring->descs, 0, alloc_slots * sizeof(struct dma_desc));

    /* export mmap to DPU (covers all alloc_slots) */
    result = export_mmap_to_remote(objs, ring->mmap,
                                   ring->descs,
                                   alloc_slots * sizeof(struct dma_desc),
                                   DMA_RING);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export mmap and buffer to DPU: %s", doca_error_get_descr(result));
        destroy_mmap_and_free_buffer(ring->mmap, ring->descs);
        free(ring->seq);
        free(ring);
        *out_ring = NULL;
        return result;
    }
    return 0;
}

