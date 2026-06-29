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
	/* EU-sharding: byte offset of THIS ring's region within the shared per-pod
	 * buffer (forward: DPU staging; reverse: host RX). The DPA adds it to its
	 * relative pos cursor so the reported completion pos is absolute. 0 for K=1
	 * (region == whole buffer) → completion pos == relative pos (legacy). */
	uint32_t region_off;
} __attribute__((__packed__, aligned(8)));

struct dpa_thread_arg {
	/* Shared comch msgq handles (CPU→DPU direction: DPA sends completions to DPU) */
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	uint32_t dpu_consumer_id; /* DPU-side comch consumer ID for DPA->DPU sends */
	uint32_t eu_index; /* which EU this thread is (0..N-1); indexes the per-EU
	                    * reverse-admission globals in dpa_kernel.c. */

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
	/* Reverse admission accounting (dpa_sent_count/dpa_cached_freed) is NOT
	 * stored here — it lives in file-scope globals in dpa_kernel.c indexed by
	 * [eu_index][ring], kept per-EU-isolated via eu_index. */
} __attribute__((__packed__, aligned(8)));

/* ====== Per-message payload layout ======
 * The DMA payload is the body itself — no in-band header.
 *   Forward path (Host→DPU): payload = body
 *   Reverse path (DPU→Host): payload = body
 * Per-request metadata (req_id / src_pod_id / dst_pod_id / flags / length)
 * is carried via comch_dma_comp_msg (and dma_desc on-DPU), keeping
 * reverse per-entry size = forward per-entry size = slot_size. That is
 * what makes num_slots × slot_size ≤ DPU_BUFFER_SIZE actually bound the
 * reverse buffer occupancy.
 *
 * Flow control is handled end-to-end at the application layer via slot-
 * based admission. DPU/DPA do not interpret any byte-position field. */

/* ====== Datapath message types (DPU ARM ↔ DPA) ======
 * Exchanged over the doca_comch_msgq between the DPU ARM and the DPA EU kernel.
 * Explicit values, contiguous from 1; 0 is reserved INVALID so a zeroed buffer
 * hits the default-reject arm (fail-safe). */
enum dpa_msg_type {
	DPA_MSG_INVALID      = 0, /* reserved: zeroed buffer hits default-reject */
	DPA_MSG_RING_ADD     = 1, /* DPU→DPA: add forward (CPU→DPU) ring */
	DPA_MSG_REV_RING_ADD = 2, /* DPU→DPA: add reverse (DPU→CPU) ring */
	DPA_MSG_WAKE         = 3, /* DPU→DPA: wake up the EU thread (no payload) */
	DPA_MSG_FWD_DONE     = 4, /* DPA→DPU: forward DMA completed (CPU→DPU) */
	DPA_MSG_REV_DONE     = 5, /* DPA→DPU: reverse DMA completed (DPU→CPU) */
};

/* DPA->DPU completion immediate — packed to EXACTLY 16 bytes (one WQE BB) to
 * minimize PCIe immediate-data cost on dma_copy. `type` MUST stay at offset 0 (the
 * recv callback peeks raw[0] to dispatch). Carries the endpoint tuple so the DPU
 * can route (dst_pod==BLANK -> resolve dst_service) and the host can demux by
 * dst_port; port/seq are OPAQUE passthrough.
 *   src_service is NOT on the wire (16B budget): the DPU derives the caller's
 *   service from src_pod's registration (assumes ONE service per pod — widen the
 *   wire if a pod ever hosts multiple services).
 * Layout is naturally aligned (uint16 on even offsets, pos@12) — no padding:
 *   type0 src_pod1 dst_pod2 dst_svc3 src_port4 dst_port6 seq8 length10 pos12 = 16B. */
struct comch_dma_comp_msg {
	uint8_t  type;        /* DPA_MSG_FWD_DONE / DPA_MSG_REV_DONE (offset 0 — peeked) */
	int8_t   src_pod_id;  /* originating pod (always concrete) */
	int8_t   dst_pod_id;  /* dest pod; DMESH_POD_BLANK -> DPU resolves dst_service */
	int8_t   dst_service; /* callee service id (routing input when dst_pod==BLANK) */
	uint16_t src_port;    /* sender port */
	uint16_t dst_port;    /* dest port; PORT_BLANK -> accept queue */
	uint16_t seq;         /* per-conn sequence (match key with port) */
	uint16_t length;      /* payload length (<= DPUMESH_SLOT_SIZE) */
	uint32_t pos;         /* buffer offset (forward: DPU dpu_buf; reverse: Host RX) */
};
/* Sent as immediate via doca_dpa_dev_comch_producer_dma_copy() — HW max 32 bytes. */
_Static_assert(sizeof(struct comch_dma_comp_msg) == 16,
               "comch_dma_comp_msg must be exactly 16 bytes (one WQE BB)");
_Static_assert(offsetof(struct comch_dma_comp_msg, type) == 0,
               "comch_dma_comp_msg.type must be at offset 0 (recv-cb peeks raw[0])");
/* src/dst_pod_id travel as int8 on the wire (dst==-1 is the echo sentinel), so
 * pod_id must fit in int8. Fail-fast if MAX_PODS ever outgrows that. */
_Static_assert(MAX_PODS <= 127,
               "pod_id wire format is int8 in comch_dma_comp_msg; MAX_PODS must be <= 127");

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

struct comch_add_ring_msg {
	enum dpa_msg_type type;
	uint32_t _pad;
	struct dpa_ring_info ring;
} __attribute__((__packed__, aligned(8)));

struct comch_add_rev_ring_msg {
	enum dpa_msg_type type;
	uint32_t _pad;
	struct dpa_ring_info ring;
} __attribute__((__packed__, aligned(8)));

struct comch_msg {
	enum dpa_msg_type type;
	union
	{
		struct comch_dma_comp_msg dma_comp_msg;
		struct comch_add_ring_msg add_ring_msg;
		struct comch_add_rev_ring_msg add_rev_ring_msg;
	};
} __attribute__((__packed__, aligned(4)));

/* These structs cross the host(x86) / DPU-ARM / DPA-EU toolchain boundary
 * (dpa_ring_info is the RING_ADD/REV_RING_ADD payload and is also h2d_memcpy'd
 * inside dpa_thread_arg; comch_msg is the configured msgq imm_data_len). Lock
 * their layout so any ABI drift between toolchains fails the build instead of
 * silently corrupting the wire. */
_Static_assert(sizeof(struct dpa_ring_info) == 72, "dpa_ring_info ABI drift");
_Static_assert(sizeof(struct comch_add_ring_msg) == 80, "comch_add_ring_msg ABI drift");
_Static_assert(sizeof(struct comch_add_rev_ring_msg) == 80, "comch_add_rev_ring_msg ABI drift");
_Static_assert(offsetof(struct comch_add_ring_msg, ring) == 8, "comch_add_ring_msg.ring offset drift");
_Static_assert(sizeof(struct comch_msg) == 84, "comch_msg ABI drift");

/* ====== DMA ring descriptor ====== */

/* Exactly 64 bytes = one cache line per descriptor. This isolation is
 * load-bearing, not just padding: the DPA clears valid=0 and flushes via
 * __dpa_thread_window_writeback(), which operates at cache-line granularity.
 * If two descriptors shared a line, that writeback would read-modify-write the
 * whole line and clobber a neighbouring slot the host had concurrently filled
 * (valid=1) — breaking the lossless single-owner-per-slot handshake. Keep one
 * descriptor per cache line. */
struct dma_desc {
	doca_dpa_dev_mmap_t mmap;      /* 4B */
	uint64_t addr;                 /* 8B */
	uint32_t size;                 /* 4B (fixed width for Host/DPA ABI stability) */
	/* 8B of endpoint-tuple metadata (replaces the old 8B `idx`/req_id, offset 16).
	 * DPA copies these verbatim into the completion (opaque passthrough). */
	uint16_t seq;                  /* 2B per-conn sequence (was req_id) */
	uint16_t src_port;             /* 2B sender port */
	uint16_t dst_port;             /* 2B dest port (PORT_BLANK=0 -> accept queue) */
	int8_t   src_service;          /* 1B caller service (SVC_NONE if none) */
	int8_t   dst_service;          /* 1B callee service (routing input when dst_pod==BLANK) */
	int32_t dst_pod_id;            /* 4B routing target; DMESH_POD_BLANK(-1) -> DPU resolves dst_service */
	int8_t flags;                  /* 1B (reserved/unused — was OP_/CASE_; kept for offset stability) */
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
_Static_assert(offsetof(struct dma_desc, seq) == 16, "dma_desc.seq offset mismatch");
_Static_assert(offsetof(struct dma_desc, dst_pod_id) == 24, "dma_desc.dst_pod_id offset mismatch");
_Static_assert(offsetof(struct dma_desc, flags) == 28, "dma_desc.flags offset mismatch");
_Static_assert(offsetof(struct dma_desc, src_pod_id) == 32, "dma_desc.src_pod_id offset mismatch");
_Static_assert(offsetof(struct dma_desc, valid) == 63, "dma_desc.valid offset mismatch");

#endif
