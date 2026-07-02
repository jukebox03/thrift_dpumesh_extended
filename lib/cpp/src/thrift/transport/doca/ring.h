#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>

struct dma_desc;
struct doca_dev;
struct doca_mmap;
struct objects;

struct dma_ring {
    struct doca_mmap *mmap;
    uint32_t head;
    uint32_t size;
    struct dma_desc *descs;
    /* "ring busy" WARN rate-limit state, PER RING (get_next_dma_desc is called
     * under this ring's own lock, so these must NOT be function-static shared
     * across the K rings — that races + defeats the throttle when >1 ring stalls). */
    uint32_t busy_head;
    uint64_t busy_probes;
};

/* Create + export one host→DPU forward descriptor ring (with the +1 credit
 * slot). EU-sharding allocates K of these; each is exported as DMA_RING and the
 * DPU pairs them in arrival order. */
int setup_dma_ring(struct objects *objs, size_t size, struct dma_ring **out_ring);

/* Create a DPU-side DMA ring for reverse direction (DPU→CPU).
 * Allocates ring memory locally (PCI-accessible), does NOT export to remote. */
int setup_dpu_tx_ring(struct doca_dev *dev, size_t size,
                      struct dma_ring **out_ring, struct doca_mmap **out_mmap);

struct dma_desc *get_next_dma_desc(struct dma_ring *ring);
#endif /* RING_H */