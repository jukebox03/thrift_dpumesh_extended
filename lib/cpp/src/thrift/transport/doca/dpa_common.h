#ifndef DPA_COMMON_H
#define DPA_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <doca_mmap.h>

#include "dpumesh_common.h"

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

/* ====== Multi-ring DPA thread arg ====== */

struct dpa_ring_info {
	doca_dpa_dev_buf_arr_t buf_arr;
	uint32_t buf_arr_size;
	doca_dpa_dev_mmap_t host_mmap;   /* Host DMA buffer mmap */
	uint64_t host_addr;              /* Host DMA buffer base address */
	uint64_t host_buf_size;          /* Host DMA buffer size */
	doca_dpa_dev_mmap_t dpu_mmap;    /* DPU local buffer mmap */
	uint64_t dpu_addr;               /* DPU local buffer addr */
	uint32_t dpu_buf_size;
	int32_t pod_id;
	/* Credit return (reverse rings only — admission gate on DPA side).
	 * Host atomically increments freed_cumulative in the credit block;
	 * DPA reads it before issuing reverse DMA via host_credit_buf_arr (a
	 * 1-element buf_arr over the host credit mmap, exactly like dma_ring).
	 * host_credit_buf_arr=0 means "skip admission check" (forward rings,
	 * or not yet wired). */
	doca_dpa_dev_buf_arr_t host_credit_buf_arr;
	uint32_t rq_depth;
	uint32_t _pad_credit;
} __attribute__((__packed__, aligned(8)));

struct dpa_thread_arg {
	/* Shared comch msgq handles (CPU→DPU direction: DPA sends completions to DPU) */
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	uint32_t dpu_consumer_id; /* DPU-side comch consumer ID for DPA->DPU sends */
	uint32_t _pad1; /* was producer_slots_inflight (M2-style lazy drain — SDK manages backpressure) */

	/* Forward rings (CPU→DPU, per-pod) */
	volatile uint32_t num_rings;
	uint32_t _pad2;
	struct dpa_ring_info rings[MAX_DPA_RINGS];
	uint32_t desc_idx[MAX_DPA_RINGS];
	uint32_t pos[MAX_DPA_RINGS];

	/* Reverse rings (DPU→CPU, per-pod) */
	volatile uint32_t num_rev_rings;
	uint32_t _pad3;
	struct dpa_ring_info rev_rings[MAX_DPA_RINGS];
	uint32_t rev_desc_idx[MAX_DPA_RINGS];
	uint32_t rev_pos[MAX_DPA_RINGS];
} __attribute__((__packed__, aligned(8)));

/* ====== Per-message payload layout ======
 * The DMA payload is the body itself — no in-band header.
 *   Forward path (Host→DPU): payload = body
 *   Reverse path (DPU→Host): payload = body
 * Per-request metadata (req_id / src_pod_id / dst_pod_id / flags / length)
 * is carried via dmesh_dma_completion_msg (and dma_desc on-DPU), keeping
 * reverse per-entry size = forward per-entry size = slot_size. That is
 * what makes num_slots × slot_size ≤ DPU_BUFFER_SIZE actually bound the
 * reverse buffer occupancy.
 *
 * Flow control is handled end-to-end at the application layer via slot-
 * based admission. DPU/DPA do not interpret any byte-position field. */

/* ====== Comch message types (DPU ↔ DPA) ====== */

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
	COMCH_MSG_TYPE_ADD_RING = 3,
	COMCH_MSG_TYPE_TRIGGER = 4,   /* DPU→DPA: wake up thread (no payload) */
	COMCH_MSG_TYPE_ADD_REV_RING = 6, /* DPU→DPA: add reverse (DPU→CPU) ring */
	COMCH_MSG_TYPE_REV_DMA_COMPLETED = 7, /* DPA→DPU: reverse DMA completed (DPU→CPU) */
};

/* Packed to exactly 16 bytes (one WQE BB) to minimize PCIe immediate-data cost
 * on dma_copy. Field widths chosen to preserve semantics:
 *   type        : 1B  — only 2 values used (DMA_COMPLETED, REV_DMA_COMPLETED)
 *   flags       : 1B  — OP_REQUEST/OP_RESPONSE + CASE_* (bit-flag set)
 *   src/dst_pod : 1B  — MAX_PODS=8 + -1 sentinel fits in int8
 *   pos         : 4B  — buffer offset (DPU buf / Host RX buf)
 *   length      : 4B  — DMA'd body length (≤ DPUMESH_SLOT_SIZE_DEFAULT)
 *   req_id      : 4B  — Thrift stream/request ID (wraparound counter)
 * All 4B fields land on their natural alignment so no __attribute__((packed)) is
 * needed and DPA accesses stay aligned. Originally 28B (4B enum + 5× uint32 +
 * int8 + 3B pad); §11.3 E5 showed 12B→24B = -8.6% throughput, so dropping the
 * struct from 32B HW quantum to 16B HW quantum is the inverse of that. */
struct comch_dma_comp_msg {
	uint8_t  type;        /* one of: COMCH_MSG_TYPE_DMA_COMPLETED, _REV_DMA_COMPLETED */
	int8_t   flags;       /* OP_REQUEST / OP_RESPONSE + CASE_* */
	int8_t   src_pod_id;  /* originating pod */
	int8_t   dst_pod_id;  /* destination pod */
	uint32_t pos;         /* buffer offset (forward: DPU dpu_buf; reverse: Host RX) */
	uint32_t length;      /* payload length */
	uint32_t req_id;      /* Thrift stream/request ID */
};
/* Sent as immediate data via doca_dpa_dev_comch_producer_dma_copy() — HW max 32 bytes */
_Static_assert(sizeof(struct comch_dma_comp_msg) == 16,
               "comch_dma_comp_msg must pack to exactly 16 bytes (one WQE BB)");
/* src/dst_pod_id travel as int8 on the wire (dst==-1 is the echo sentinel), so
 * pod_id must fit in int8. Fail-fast if MAX_PODS ever outgrows that. */
_Static_assert(MAX_PODS <= 127,
               "pod_id wire format is int8 in comch_dma_comp_msg; MAX_PODS must be <= 127");

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

struct comch_add_ring_msg {
	enum comch_msg_type type;
	uint32_t _pad;
	struct dpa_ring_info ring;
} __attribute__((__packed__, aligned(8)));

struct comch_add_rev_ring_msg {
	enum comch_msg_type type;
	uint32_t _pad;
	struct dpa_ring_info ring;
} __attribute__((__packed__, aligned(8)));

struct comch_msg {
	enum comch_msg_type type;
	union
	{
		struct comch_dma_comp_msg dma_comp_msg;
		struct comch_add_ring_msg add_ring_msg;
		struct comch_add_rev_ring_msg add_rev_ring_msg;
	};
} __attribute__((__packed__, aligned(4)));

/* ====== DMA ring descriptor ====== */

/* Exactly 64 bytes = one cache line per descriptor. This isolation is
 * load-bearing, not just padding: the DPA clears valid=0 and flushes via
 * __dpa_thread_window_writeback(), which operates at cache-line granularity.
 * If two descriptors shared a line, that writeback would read-modify-write the
 * whole line and clobber a neighbouring slot the host had concurrently filled
 * (valid=1) — breaking the lossless single-owner-per-slot handshake. A 32B
 * pack was tried and produced exactly this corruption (stuck slots → timeouts),
 * so keep one descriptor per cache line. */
struct dma_desc {
	doca_dpa_dev_mmap_t mmap;      /* 4B */
	uint64_t addr;                 /* 8B */
	uint32_t size;                 /* 4B (fixed width for Host/DPA ABI stability) */
	uint64_t idx;                  /* 8B (req_id) */
	int32_t dst_pod_id;            /* 4B (routing target) */
	int8_t flags;                  /* 1B (OP_REQUEST/OP_RESPONSE + CASE_*) */
	uint8_t pad0[3];               /* 3B alignment for src_pod_id */
	int32_t src_pod_id;            /* 4B (original forward sender; on reverse rings,
	                                * ring->pod_id is the receiver, so the source
	                                * must be carried in the descriptor itself.
	                                * Forward path: DPA derives src from ring->pod_id
	                                * and ignores this field. Reverse path: DPU sets
	                                * it in dpu_enqueue_reverse_dma; DPA copies into
	                                * comp.src_pod_id so the receiving host can
	                                * route OP_REQUEST/RESPONSE correctly without
	                                * an in-payload sw_descriptor. */
	uint8_t reserved[27];          /* 27B */
	volatile uint8_t valid;        /* 1B */
} __attribute__((__packed__, aligned(8)));

/* Keep Host/DPA descriptor ABI stable across toolchains. */
_Static_assert(sizeof(struct dma_desc) == 64, "dma_desc must be 64 bytes");
_Static_assert(offsetof(struct dma_desc, addr) == 4, "dma_desc.addr offset mismatch");
_Static_assert(offsetof(struct dma_desc, size) == 12, "dma_desc.size offset mismatch");
_Static_assert(offsetof(struct dma_desc, idx) == 16, "dma_desc.idx offset mismatch");
_Static_assert(offsetof(struct dma_desc, dst_pod_id) == 24, "dma_desc.dst_pod_id offset mismatch");
_Static_assert(offsetof(struct dma_desc, flags) == 28, "dma_desc.flags offset mismatch");
_Static_assert(offsetof(struct dma_desc, src_pod_id) == 32, "dma_desc.src_pod_id offset mismatch");
_Static_assert(offsetof(struct dma_desc, valid) == 63, "dma_desc.valid offset mismatch");

#endif
