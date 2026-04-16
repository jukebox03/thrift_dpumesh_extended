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
} __attribute__((__packed__, aligned(8)));

/* ====== Flow-control message header ======
 * Prepended to every DMA payload by the sender.
 * DPA copies it verbatim; the receiver parses it. */
struct fc_header {
	uint32_t consumer_tail;  /* sender's RX buffer consumption position */
	uint32_t payload_len;    /* actual payload length after this header */
} __attribute__((__packed__));

/* ====== Comch message types (DPU ↔ DPA) ====== */

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
	COMCH_MSG_TYPE_ADD_RING = 3,
	COMCH_MSG_TYPE_TRIGGER = 4,   /* DPU→DPA: wake up thread (no payload) */
	COMCH_MSG_TYPE_DMA_CHUNK = 5, /* DPA→DPU: intermediate DMA chunk landed (no action needed) */
	COMCH_MSG_TYPE_ADD_REV_RING = 6, /* DPU→DPA: add reverse (DPU→CPU) ring */
	COMCH_MSG_TYPE_REV_DMA_COMPLETED = 7, /* DPA→DPU: reverse DMA completed (DPU→CPU) */
};

struct comch_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t pos;
	uint32_t length;
	uint32_t req_id;      /* Thrift stream/request ID */
	int32_t  src_pod_id;  /* originating pod */
	int32_t  dst_pod_id;  /* destination pod */
	int8_t   flags;       /* OP_REQUEST / OP_RESPONSE + CASE_* */
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
	doca_dpa_dev_mmap_t mmap;      /* 4B */
	uint64_t addr;                 /* 8B */
	uint32_t size;                 /* 4B (fixed width for Host/DPA ABI stability) */
	uint64_t idx;                  /* 8B (req_id) */
	int32_t dst_pod_id;            /* 4B (routing target) */
	int8_t flags;                  /* 1B (OP_REQUEST/OP_RESPONSE + CASE_*) */
	uint8_t reserved[34];          /* 34B */
	volatile uint8_t valid;        /* 1B */
} __attribute__((__packed__, aligned(8)));

/* Keep Host/DPA descriptor ABI stable across toolchains. */
_Static_assert(sizeof(struct dma_desc) == 64, "dma_desc must be 64 bytes");
_Static_assert(offsetof(struct dma_desc, addr) == 4, "dma_desc.addr offset mismatch");
_Static_assert(offsetof(struct dma_desc, size) == 12, "dma_desc.size offset mismatch");
_Static_assert(offsetof(struct dma_desc, idx) == 16, "dma_desc.idx offset mismatch");
_Static_assert(offsetof(struct dma_desc, dst_pod_id) == 24, "dma_desc.dst_pod_id offset mismatch");
_Static_assert(offsetof(struct dma_desc, flags) == 28, "dma_desc.flags offset mismatch");
_Static_assert(offsetof(struct dma_desc, valid) == 63, "dma_desc.valid offset mismatch");

#endif
