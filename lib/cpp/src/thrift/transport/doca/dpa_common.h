#ifndef DPA_COMMON_H
#define DPA_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <doca_mmap.h>

#include "dpumesh_common.h"

/* OP flag bits (Phase 3) — shared between host, DPU, and the DPA kernel.
 * Mirrors mesh.h; duplicated here because the DPA kernel build doesn't
 * include mesh.h. Keep in sync with mesh.h. */
#ifndef OP_HDR_BATCH
#define OP_HDR_BATCH 0x40
#endif
#ifndef OP_CHUNK
#define OP_CHUNK     0x20
#endif
#ifndef CASE_DIRECT
#define CASE_DIRECT  4
#endif

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

/* ====== Multi-ring DPA thread arg ====== */

#define MAX_DPA_PEERS  16

/* Phase 4: peer info pushed from DPU to DPA. DPA forward kernel looks up
 * by dst_pod_id when desc.flags & CASE_DIRECT, then performs single-shot
 * dma_copy from src host's body mmap to peer host's rx_mmap (both DPU-
 * device DPA handles — no host→host PCIe peer access needed).
 *
 * Flow control (v1.0.0 pattern): peer's dma_ring buf_arr is re-used as a
 * credit pipe. Slot index DMA_RING_SIZE in that buf_arr is a uint64_t
 * "freed_cumulative" counter that the peer host increments on rx_free.
 * DPA lazily refreshes its cached copy and admits new direct DMAs only
 * when sent - cached_freed < rq_depth. */
struct dpa_peer_info {
	int32_t  pod_id;        /* -1 = empty slot */
	uint32_t _pad;
	doca_dpa_dev_mmap_t rx_mmap;
	uint32_t _pad2;
	uint64_t rx_addr;
	uint64_t rx_buf_size;
	doca_dpa_dev_buf_arr_t credit_buf_arr;  /* peer's dma_ring buf_arr (slot DMA_RING_SIZE = credit) */
	uint32_t rq_depth;                       /* peer's rx_dma_buffer num_slots */
	uint32_t _pad3;
} __attribute__((__packed__, aligned(8)));

struct dpa_ring_info {
	doca_dpa_dev_buf_arr_t buf_arr;
	uint32_t buf_arr_size;
	doca_dpa_dev_mmap_t host_mmap;   /* Host DMA buffer mmap (body pool) */
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
	/* Phase 3: hdr-side mmap handles resolved against the DPU device.
	 *   host_hdr_mmap    — forward rings: host's hdr_tx_buffer (DPA reads
	 *                      from it when desc.flags & OP_HDR_BATCH).
	 *   host_hdr_rx_mmap — reverse rings: host's hdr_rx_buffer (DPA writes
	 *                      to it when desc.flags & OP_HDR_BATCH).
	 * Both are 0 until the matching DMA_HOST_TX_HDR_BUFFER / RX_HDR_BUFFER
	 * mmap_msg lands; before that the forward/reverse path for hdr would
	 * fall back to body mmap (degraded, but legacy keeps working). */
	doca_dpa_dev_mmap_t host_hdr_mmap;
	doca_dpa_dev_mmap_t host_hdr_rx_mmap;
	uint64_t host_hdr_addr;          /* host_hdr_tx base VA (forward) or host_hdr_rx base VA (reverse) */
} __attribute__((__packed__, aligned(8)));

struct dpa_thread_arg {
	/* Shared comch msgq handles (CPU→DPU direction: DPA sends completions to DPU) */
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	uint32_t dpu_consumer_id; /* DPU-side comch consumer ID for DPA->DPU sends */
	uint32_t producer_slots_inflight; /* number of producer send slots currently in use */

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

	/* Phase 4: peer table — indexed by pod_id (sparse). Populated via
	 * COMCH_MSG_TYPE_ADD_PEER from DPU after setup_pod_dma completes.
	 * Forward kernel CASE_DIRECT looks up peers[dst_pod_id] and issues
	 * single-shot dma_copy directly to peer's rx_mmap. */
	struct dpa_peer_info peers[MAX_DPA_PEERS];
	uint64_t peer_write_cursor[MAX_DPA_PEERS];
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
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
	COMCH_MSG_TYPE_ADD_RING = 3,
	COMCH_MSG_TYPE_TRIGGER = 4,   /* DPU→DPA: wake up thread (no payload) */
	COMCH_MSG_TYPE_DMA_CHUNK = 5, /* DPA→DPU: intermediate DMA chunk landed (no action needed) */
	COMCH_MSG_TYPE_ADD_REV_RING = 6, /* DPU→DPA: add reverse (DPU→CPU) ring */
	COMCH_MSG_TYPE_REV_DMA_COMPLETED = 7, /* DPA→DPU: reverse DMA completed (DPU→CPU) */
	COMCH_MSG_TYPE_ADD_PEER = 8, /* DPU→DPA (Phase 4): register peer host_rx_mmap for CASE_DIRECT */
};

/* struct dpa_peer_info / MAX_DPA_PEERS moved above struct dpa_ring_info. */

struct comch_add_peer_msg {
	enum comch_msg_type type;
	uint32_t _pad;
	struct dpa_peer_info peer;
} __attribute__((__packed__, aligned(8)));

struct comch_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t pos;
	uint32_t length;
	uint32_t req_id;      /* Thrift stream/request ID */
	int32_t  src_pod_id;  /* originating pod */
	int32_t  dst_pod_id;  /* destination pod */
	/* Phase 4: paired chunk_tx slot for OP_HDR_BATCH completion. DPA copies
	 * from dma_desc.src_chunk_buf_slot/_len so the DPU knows where the
	 * body bytes live in src host's chunk_tx_buffer once it starts firing
	 * body DMA itself. Slot fits in int16 (DPUMESH_NUM_SLOTS_DEFAULT=4096,
	 * cap < 32768); len fits in uint16 (≤ DPUMESH_SLOT_SIZE_DEFAULT 8192).
	 * (-1, 0) when not present. */
	uint32_t src_chunk_buf_len; /* 0 if not present */
	int16_t  src_chunk_buf_slot; /* -1 if not present */
	int8_t   flags;       /* OP_REQUEST / OP_RESPONSE + CASE_* */
	int8_t   _pad;
};
/* Sent as immediate data via doca_dpa_dev_comch_producer_dma_copy() — max 32 bytes */
_Static_assert(sizeof(struct comch_dma_comp_msg) <= 32,
               "comch_dma_comp_msg must fit in 32-byte immediate data limit");

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

struct comch_dma_req_msg {
	enum comch_msg_type type;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_mmap_t dst_mmap;
	uint64_t src_addr;
	uint64_t dst_addr;
	uint32_t length;
} __attribute__((__packed__, aligned(8)));

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
		struct comch_dma_req_msg dma_req_msg;
		struct comch_dma_comp_msg dma_comp_msg;
		struct comch_add_ring_msg add_ring_msg;
		struct comch_add_rev_ring_msg add_rev_ring_msg;
	};
} __attribute__((__packed__, aligned(4)));

/* ====== DMA ring descriptor ====== */

struct dma_desc {
	doca_dpa_dev_mmap_t mmap;      /* 4B - forward: src override (Phase 1), reverse: src override */
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
	doca_dpa_dev_mmap_t dst_mmap;  /* 4B (Phase 2) - reverse DST override. Non-zero
	                                * means DPA reverse kernel writes into this mmap
	                                * (e.g. host_hdr_rx_dpa_handle) instead of
	                                * ring->host_mmap. Forward kernel ignores. */
	uint32_t dst_pos;              /* 4B (Phase 3) - offset within the dst buffer
	                                * for the host to read from. DPA echoes this
	                                * into comp.pos when dst_overridden so the
	                                * host knows where in hdr_rx_buffer the data
	                                * landed. (When dst_mmap == 0, comp.pos is
	                                * derived from rev_pos[r] as before.) */
	uint64_t dst_addr;             /* 8B (Phase 2) - absolute virtual address in
	                                * dst_mmap. Only used when dst_mmap != 0. */
	/* Phase 4: paired chunk_tx slot for OP_HDR_BATCH descriptors. Lets the
	 * DPU locate the body bytes in src host's chunk_tx_buffer so it can
	 * issue the body DMA itself (single src→dst dma_copy via DPA). DPA
	 * copies these into comch_dma_comp_msg so DPU sees them on completion.
	 * For non-hdr-batch descriptors these are (-1, 0) / 0 / 0 and ignored. */
	int32_t  src_chunk_buf_slot;   /* 4B (-1 if not present) */
	uint32_t src_chunk_buf_len;    /* 4B (0 if not present) */
	uint8_t reserved[3];           /* 3B */
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
_Static_assert(offsetof(struct dma_desc, dst_mmap) == 36, "dma_desc.dst_mmap offset mismatch");
_Static_assert(offsetof(struct dma_desc, dst_pos)  == 40, "dma_desc.dst_pos offset mismatch");
_Static_assert(offsetof(struct dma_desc, dst_addr) == 44, "dma_desc.dst_addr offset mismatch");
_Static_assert(offsetof(struct dma_desc, src_chunk_buf_slot) == 52, "dma_desc.src_chunk_buf_slot offset mismatch");
_Static_assert(offsetof(struct dma_desc, src_chunk_buf_len)  == 56, "dma_desc.src_chunk_buf_len offset mismatch");
_Static_assert(offsetof(struct dma_desc, valid) == 63, "dma_desc.valid offset mismatch");

#endif
