#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>

#define DMA_RING_SIZE 1024

struct dma_desc;
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

struct dma_desc *get_next_dma_desc(struct dma_ring *ring);
#endif /* RING_H */