#ifndef DPUMESH_COMMON_H
#define DPUMESH_COMMON_H

/* ====== Common Flags (shared by Host, DPU ARM, and DPA) ====== */

/* CaseFlag — case classification (wire protocol shared by Host, DPU ARM, DPA).
 * NOTE: nothing in-tree READS these bits today (every flags consumer masks only
 * OP_RESPONSE); they are write-only metadata. CASE_EXTERNAL is still set by the
 * host senders (gateway.c, bench_dpumesh.c) and CASE_INGRESS by the DPU/client,
 * so both are kept. CASE_LOCAL had zero references and was removed. */
#define CASE_EXTERNAL  1
#define CASE_INGRESS   2

/* OpFlag — request/response direction bit (OR'd into the descriptor flags byte) */
#define OP_REQUEST     0x00
#define OP_RESPONSE    0x10

/* PoolType — buffer-pool identifiers */
#define POOL_NONE           0
#define POOL_HOST_TX_BODY   2

/* ====== DOCA / DPA limits ====== */
#define MAX_DPA_RINGS       8
#define MAX_PODS            8

/* pod_id is int8 on the wire (see dpa_common.h comch_dma_comp_msg.src/dst_pod),
 * so valid ids are [0,127]. find_pod_by_id uses an O(1) pod_id->slot map sized
 * to cover that whole id space — independent of MAX_PODS (how many pods are live
 * at once), so the lookup stays O(1) and correct as the pod count grows. Widen
 * this together with the int8 wire fields if pod_id ever needs to exceed 127.
 * (Always >= MAX_PODS; not asserted here because this header is also included
 * by C++ translation units via dpumesh.h.) */
#define POD_ID_SPACE        128

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
/* 16MB = 2048×8KB. §10.7 tried 32MB/4096 and got flat throughput — but that
 * was on fair/1-core BEFORE SKIP_REQ_TXACK, when the round-trip was binding
 * (depth didn't matter). Under SKIP+multicore the host TX slots ARE binding
 * (tx_inflight hits num_slots=2048 → ~137K plateau), so 32MB/4096 is re-tried
 * here (experiment A). Must equal DPUMESH_NUM_SLOTS × slot_size. */
#define DPU_BUFFER_SIZE     (32 * 1024 * 1024)  /* 32MB = 4096 × 8KB */
#define DPUMESH_SLOT_SIZE   8192               /* matches DPUMESH_SLOT_SIZE_DEFAULT */
/* DMA descriptor ring depth (host→DPU forward). Mirrored from ring.h so
 * the DPA kernel — which can't include ring.h — knows the ring length.
 * Host's setup_dma_ring allocates this many slots PLUS 1 extra for the
 * credit counter at index DMA_RING_SIZE. */
#define DMA_RING_SIZE       4096

#endif /* DPUMESH_COMMON_H */
