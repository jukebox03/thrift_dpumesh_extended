#ifndef DPUMESH_COMMON_H
#define DPUMESH_COMMON_H

/* ====== Common Flags (shared by Host, DPU ARM, and DPA) ====== */

/* CaseFlag — case classification (wire protocol shared by Host, DPU ARM, DPA). */
#define CASE_EXTERNAL  1
#define CASE_INGRESS   2

/* OpFlag — request/response direction bit (OR'd into the descriptor flags byte) */
#define OP_REQUEST     0x00
#define OP_RESPONSE    0x10

/* ====== DOCA / DPA limits ====== */
#define MAX_DPA_RINGS       8   /* per-EU ring capacity (forward + reverse each) */
#define MAX_PODS            8

/* EU-sharding: a pod spreads its forward+reverse traffic across K rings, each on
 * a distinct EU, so a 2-pod pair can drive >2 EUs. K = DPUMESH_RINGS_PER_POD
 * (default 1 = legacy single-ring-per-pod). MAX_EU_PER_POD bounds the per-pod
 * ring arrays; a pod can shard across at most min(K, num_dpa_threads) EUs. */
#define MAX_EU_PER_POD      MAX_DPA_RINGS
#define DPUMESH_RINGS_PER_POD_DEFAULT 1

/* pod_id is int8 on the wire (valid ids [0,127]); this sizes the pod_id->slot
 * map to cover that id space. Always >= MAX_PODS. Widen together with the int8
 * wire fields if pod_id ever needs to exceed 127. */
#define POD_ID_SPACE        128

/* DPU-side DMA buffer size per pod (intermediate forward/reverse DMA staging).
 * MUST equal DPUMESH_NUM_SLOTS_DEFAULT × DPUMESH_SLOT_SIZE_DEFAULT so end-node
 * slot-based admission bounds in-flight bytes ≤ DPU buffer size.
 * Caveat: a dst pod's reverse staging aggregates entries from all sources that
 * target it; N concurrent sources need a DPU_BUFFER_SIZE scaled by N. */
#define DPU_BUFFER_SIZE     (32 * 1024 * 1024)  /* 32MB = 4096 × 8KB */
#define DPUMESH_SLOT_SIZE   8192               /* matches DPUMESH_SLOT_SIZE_DEFAULT */
/* DMA descriptor ring depth (host→DPU forward). Mirrored from ring.h so
 * the DPA kernel — which can't include ring.h — knows the ring length.
 * Host's setup_dma_ring allocates this many slots PLUS 1 extra for the
 * credit counter at index DMA_RING_SIZE. */
#define DMA_RING_SIZE       4096

#endif /* DPUMESH_COMMON_H */
