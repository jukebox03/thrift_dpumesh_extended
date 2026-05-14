/*
 * mesh.h — DPUmesh wire formats for header/body split pipeline (Phase 3)
 *
 * Shared by host (libthrift), DPU ARM (dpumesh_dpu), and indirectly the
 * DPA kernel (which reads dma_desc only — no mesh structs).
 *
 * All structs are packed and have a stable ABI. Do not reorder fields.
 */
#ifndef MESH_H
#define MESH_H

#include <stdint.h>

/* ====== req_id (Phase 3): 64bit globally unique ======
 * Pair (src_id, seq) — src_id distinguishes RPCs across pods,
 * seq is a monotonic per-src counter. dst uses src_id to demux its
 * expected[] table; collisions on req_id alone (same seq from different
 * srcs) cannot happen.
 *
 * Phase 3 wires src_id = src pod_id directly. Phase 4 will introduce a
 * DPU-assigned stable id and src_id-pod_id mapping. */
struct mesh_req_id {
    uint32_t src_id;
    uint32_t seq;
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_req_id) == 8, "mesh_req_id ABI");

#define MESH_REQ_ID_EQ(a, b) ((a).src_id == (b).src_id && (a).seq == (b).seq)

/* ====== mesh_hdr_req — src → DPU header batch entry ======
 * src writes one of these into hdr_tx_buffer slot per send_header() call.
 * hdr_builder flushes a batch (N × 80B) via forward DMA when full or on
 * timeout. DPU parses, resolves dst_service → dst_pod_id, and re-emits a
 * mesh_hdr_fwd per entry into the matching pod's hdr_outbound staging. */
struct mesh_hdr_req {
    struct mesh_req_id req_id;      /* 8B */
    char     dst_service[24];       /* NUL-terminated within 24B */
    int32_t  dst_pod_id_hint;       /* -1 if unknown (DPU resolves) */
    uint32_t body_size;
    uint64_t trace_id;
    uint64_t span_id;
    uint8_t  flags;                 /* OP_REQUEST | OP_RESPONSE */
    uint8_t  reserved[23];
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_hdr_req) == 80, "mesh_hdr_req ABI");

/* ====== mesh_hdr_fwd — DPU → dst header forward entry ======
 * DPU writes one of these into the dst's host_hdr_rx_buffer per resolved
 * hdr_req. dst rx hook enqueues into expected[src_id] ring. */
struct mesh_hdr_fwd {
    struct mesh_req_id req_id;      /* 8B (src's req_id, echoed) */
    int32_t  src_pod_id;            /* 4B */
    uint32_t body_size;             /* 4B */
    uint64_t trace_id;              /* 8B */
    uint64_t span_id;               /* 8B */
    uint8_t  flags;                 /* 1B */
    uint8_t  status;                /* 1B: 0=OK, 1=DROP */
    uint8_t  reserved[30];          /* 30B → total 64 */
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_hdr_fwd) == 64, "mesh_hdr_fwd ABI");

/* ====== mesh_chunk_header — src → dst body chunk header ======
 * 12B header prefixed to a chunk of N body payloads (no per-body size
 * inline — sizes come from dst's expected[src_id] ring populated by
 * hdr_fwd arrival). first_seq + num_bodies tell dst how many entries
 * to pop from the queue head; the queue (per-src FIFO) guarantees
 * matching seq ordering as long as DPU's per-dst outbound staging is
 * strict FIFO. */
struct mesh_chunk_header {
    uint32_t src_id;
    uint32_t first_seq;
    uint32_t num_bodies;
} __attribute__((packed));
_Static_assert(sizeof(struct mesh_chunk_header) == 12, "mesh_chunk_header ABI");

/* ====== Capacity limits ====== */
#define MESH_HDR_BATCH_MAX_ENTRIES   96             /* 8192 / 80 = 102, safe margin */
#define MESH_CHUNK_MAX_BODIES        64
#define MESH_CHUNK_BODY_BUDGET       (8192 - 12)    /* slot_size - chunk_header */
#define MESH_HDR_RX_BUDGET           8192           /* per hdr_fwd batch DMA */

/* ====== OP flags (extend dpumesh_common.h's bits) ======
 * Existing: OP_REQUEST=0x00, OP_RESPONSE=0x10.
 * Phase 3 adds hdr/chunk discriminators in the same flags byte so DPU
 * and host can branch on dma_desc.flags / rx_data_hook's flag without
 * a separate wire field. */
#define OP_HDR_BATCH    0x40
#define OP_CHUNK        0x20

#endif /* MESH_H */
