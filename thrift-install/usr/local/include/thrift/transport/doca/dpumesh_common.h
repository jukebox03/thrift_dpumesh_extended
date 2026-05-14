#ifndef DPUMESH_COMMON_H
#define DPUMESH_COMMON_H

/* ====== Common Flags (shared by Host, DPU ARM, and DPA) ====== */

/* CaseFlag (match Python CaseFlag) */
#define CASE_EXTERNAL  1
#define CASE_INGRESS   2
#define CASE_LOCAL     3

/* OpFlag (match Python OpFlag) */
#define OP_REQUEST     0x00
#define OP_RESPONSE    0x10

/* PoolType (match Python PoolType) */
#define POOL_NONE           0
#define POOL_HOST_TX_BODY   2
#define POOL_HOST_RX_BODY   4
#define POOL_DPU_TX_BODY    6
#define POOL_DPU_RX_BODY    7
/* Phase 1 (v2 plan): independent host TX pool for header batches. Tracked
 * in TX_ACK.pool_type so the host frees the correct slot. */
#define POOL_HOST_TX_HDR    8

/* ====== DOCA / DPA limits ====== */
#define MAX_DPA_RINGS       8
#define MAX_PODS            8

/* DPU-side DMA buffer size per pod (DPU's intermediate buffers used for
 * forward and reverse DMA staging). MUST equal
 * DPUMESH_NUM_SLOTS_DEFAULT × DPUMESH_SLOT_SIZE_DEFAULT so end-node
 * slot-based admission directly bounds in-flight bytes ≤ DPU buffer size.
 *
 * That invariant is the entire flow-control story for a single (src, dst)
 * pair: end-nodes hold a TX slot from enqueue until response (or TX_ACK),
 * so #live slots × slot_size is the worst-case per-source in-flight
 * footprint inside DPU. With this equality DPU never laps unconsumed
 * bytes, even though DPU/DPA do no FC of their own.
 *
 * Both directions carry per-entry payload = body (no in-band header), with
 * body_len ≤ slot_size. Per-request metadata travels via dma_desc /
 * comch_dma_comp_msg, NOT in the DMA payload — that is what keeps reverse
 * footprint = forward footprint = num_slots × slot_size.
 *
 * Caveat: a destination pod's reverse staging buffer aggregates entries
 * from ALL source pods that target it. With N concurrent sources targeting
 * one dst, worst-case dst staging occupancy is N × this size. Single-source
 * workloads fit exactly; multi-source needs a scaled DPU_BUFFER_SIZE. */
#define DPU_BUFFER_SIZE     (16 * 1024 * 1024)  /* 16MB = 2048 × 8KB */
#define DPUMESH_SLOT_SIZE   8192               /* matches DPUMESH_SLOT_SIZE_DEFAULT */
/* DMA descriptor ring depth (host→DPU forward). Mirrored from ring.h so
 * the DPA kernel — which can't include ring.h — knows the ring length.
 * Host's setup_dma_ring allocates this many slots PLUS 1 extra for the
 * credit counter at index DMA_RING_SIZE. */
#define DMA_RING_SIZE       2048

#endif /* DPUMESH_COMMON_H */
