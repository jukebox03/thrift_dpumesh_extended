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
    uint32_t size;
    struct dma_desc *descs;
    /* Host forward ring — LOCK-FREE MPSC (Vyukov bounded queue). Producers claim a
     * monotonic ticket from enq_pos (fetch-add); seq[i] is the per-slot cell
     * sequence (host memory, NOT in the ring mmap; init seq[i]=i). A producer owns
     * slot t%size for generation t when seq==t, or when the previous occupant
     * published (seq==t-size+1) AND the DPA consumed it (desc.valid==0) → reclaim.
     * A stalled producer leaves seq unadvanced, so a lapping producer waits rather
     * than overwriting — generation-safe with just the existing `valid` flag, no
     * lock, DPA untouched. */
    uint64_t  enq_pos;
    uint64_t *seq;
    /* "ring busy" WARN rate-limit state (best-effort under lock-free contention;
     * a racy probe count only mis-throttles a diagnostic, never corrupts). */
    uint64_t busy_probes;
};

/* Create + export one host→DPU forward descriptor ring (with the +1 credit
 * slot). EU-sharding allocates K of these; each is exported as DMA_RING and the
 * DPU pairs them in arrival order. */
int setup_dma_ring(struct objects *objs, size_t size, struct dma_ring **out_ring);
#endif /* RING_H */