#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stdint.h>
#include <doca_mmap.h>

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

/* ====== Multi-ring DPA thread arg ====== */

#define MAX_DPA_RINGS 8

struct dpa_ring_info {
	doca_dpa_dev_buf_arr_t buf_arr;
	uint32_t buf_arr_size;
	doca_dpa_dev_mmap_t host_mmap;   /* Host DMA buffer mmap */
	doca_dpa_dev_mmap_t dpu_mmap;    /* DPU local buffer mmap */
	uint64_t dpu_addr;               /* DPU local buffer addr */
	uint32_t dpu_buf_size;
	int32_t pod_id;
} __attribute__((__packed__, aligned(8)));

struct dpa_thread_arg {
	/* Shared comch msgq handles */
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;

	/* Ring array (per-pod) */
	volatile uint32_t num_rings;
	uint32_t _pad;
	struct dpa_ring_info rings[MAX_DPA_RINGS];
} __attribute__((__packed__, aligned(8)));

/* ====== Comch message types (DPU ↔ DPA) ====== */

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
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

struct comch_msg {
	enum comch_msg_type type;
	union
	{
		struct comch_dma_req_msg dma_req_msg;
		struct comch_dma_comp_msg dma_comp_msg;
	};
} __attribute__((__packed__, aligned(4)));

/* ====== DMA ring descriptor ====== */

struct dma_desc {
	doca_dpa_dev_mmap_t mmap; 	// 4B
	uint64_t addr;			   // 8B
	size_t size;				   // 8B
	uint64_t idx;		   // 8B (req_id)
	int32_t dst_pod_id;    // 4B (routing target)
	int8_t flags;          // 1B (OP_REQUEST/OP_RESPONSE + CASE_*)
	uint8_t reserved[30];  // 30B
	volatile uint8_t valid;		   // 1B
} __attribute__((__packed__, aligned(8)));

#endif
