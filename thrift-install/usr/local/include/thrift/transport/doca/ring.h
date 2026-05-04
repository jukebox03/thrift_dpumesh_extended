#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>

#include "dpumesh_common.h"  /* DMA_RING_SIZE */

struct dma_desc;
struct doca_dev;
struct doca_mmap;
struct objects;

struct dma_ring {
    struct doca_mmap *mmap;
    uint32_t head;
    uint32_t tail;
    uint32_t size;
    struct dma_desc *descs;
};

int setup_dma_ring(struct objects *objs, size_t size);

/* Create a DPU-side DMA ring for reverse direction (DPU→CPU).
 * Allocates ring memory locally (PCI-accessible), does NOT export to remote. */
int setup_dpu_tx_ring(struct doca_dev *dev, size_t size,
                      struct dma_ring **out_ring, struct doca_mmap **out_mmap);

struct dma_desc *get_next_dma_desc(struct dma_ring *ring);
#endif /* RING_H */