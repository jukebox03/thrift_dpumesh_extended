/*
 * dpumesh.h - DPUmesh transport public API for Thrift
 *
 * Backend-agnostic header. The actual implementation (SHM or DOCA)
 * is selected at library build time via -DWITH_DOCA=ON|OFF.
 * Application code should include only this header.
 */

#ifndef DPUMESH_H
#define DPUMESH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

/* ====== Default constants ====== */
#define DPUMESH_SLOT_SIZE_DEFAULT       (1024 * 1024)   /* 1MB */
#define DPUMESH_NUM_SLOTS_DEFAULT       64
#define DPUMESH_DESCRIPTOR_SIZE         64
#define DPUMESH_MAX_DESCRIPTORS_DEFAULT 512
#define DPUMESH_PREFIX_DEFAULT          "dpumesh"

/* ====== Configuration ====== */
typedef struct {
    int num_slots;        /* slots per pool (0 = use default 64) */
    int slot_size;        /* bytes per slot (0 = use default 1MB) */
    int max_descriptors;  /* descriptor ring capacity (0 = use default 512) */
} dpumesh_config_t;

#define DPUMESH_CONFIG_DEFAULT { 0, 0, 0 }

/* Flags (match Python CaseFlag, OpFlag) */
#define CASE_EXTERNAL  1
#define CASE_INGRESS   2
#define CASE_LOCAL     3
#define OP_REQUEST     0x00
#define OP_RESPONSE    0x10

/* PoolType (match Python PoolType) */
#define POOL_NONE           0
#define POOL_HOST_TX_BODY   2
#define POOL_HOST_RX_BODY   4
#define POOL_DPU_TX_BODY    6
#define POOL_DPU_RX_BODY    7

/* ====== SwDescriptor (64 bytes, packed, matches '<iIiIIIiibbBBiiii12x') ====== */
typedef struct __attribute__((packed)) {
    int32_t  header_buf_slot;       /* i  (always -1 for Thrift) */
    uint32_t header_len;            /* I  (always 0 for Thrift)  */
    int32_t  body_buf_slot;         /* i */
    uint32_t body_len;              /* I */
    uint32_t req_id;                /* I  (stream_id) */
    uint32_t step_id;               /* I */
    int32_t  dst_pod_id;            /* i */
    int32_t  src_pod_id;            /* i */
    int8_t   flags;                 /* b */
    int8_t   valid;                 /* b */
    uint8_t  src_body_pool_type;    /* B */
    uint8_t  src_header_pool_type;  /* B  (always 0 for Thrift) */
    int32_t  src_body_pod_id;       /* i */
    int32_t  src_header_pod_id;     /* i  (always 0 for Thrift) */
    int32_t  src_body_buf_slot;     /* i */
    int32_t  src_header_buf_slot;   /* i  (always -1 for Thrift) */
    uint8_t  _pad[12];             /* 12x */
} sw_descriptor_t;

/* ====== Opaque context ====== */
typedef struct dpumesh_ctx dpumesh_ctx_t;

/* ====== Lifecycle ====== */
int  dpumesh_init(dpumesh_ctx_t **ctx, const char *app_name, int worker_id,
                  const dpumesh_config_t *config);  /* NULL = use defaults */
void dpumesh_destroy(dpumesh_ctx_t *ctx);

/* ====== Query configured values ====== */
int dpumesh_get_slot_size(dpumesh_ctx_t *ctx);

/* ====== Info ====== */
int         dpumesh_get_notify_fd(dpumesh_ctx_t *ctx);
int         dpumesh_get_pod_id(dpumesh_ctx_t *ctx);
const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx);

/* ====== Raw Buffer API (for Thrift transport) ====== */

/*
 * Dequeue one descriptor from RX SQ.
 * Blocks up to timeout_ms (-1 = block forever, 0 = non-blocking).
 * Returns 0 on success, -1 on timeout/error.
 */
int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms);

/* Get pointer to RX buffer data for a slot (zero-copy read). */
uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot);

/* Free an RX buffer slot after reading. */
void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot);

/* Allocate a TX buffer slot. Returns slot index or -1. */
int dpumesh_tx_alloc(dpumesh_ctx_t *ctx);

/* Get pointer to TX buffer data for a slot (zero-copy write). */
uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot);

/* Free a TX buffer slot (on error path). */
void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot);

/* Enqueue a descriptor to TX SQ. Returns 0 on success, -1 on failure. */
int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc);

/* ====== Client-side API (request/response matching) ====== */

/* Allocate a unique request ID (atomic, thread-safe). */
uint32_t dpumesh_alloc_req_id(dpumesh_ctx_t *ctx);

/* Register a pending entry for req_id (must call before enqueue).
 * Returns 0 on success, -1 on failure. */
int dpumesh_register_pending(dpumesh_ctx_t *ctx, uint32_t req_id);

/* Wait for a response matching req_id.
 * Blocks up to timeout_ms (-1 = forever, 0 = non-blocking).
 * On success, fills resp and returns 0. On timeout, returns -1.
 * Caller must free resp->body_buf_slot via dpumesh_rx_free(). */
int dpumesh_wait_response(dpumesh_ctx_t *ctx, uint32_t req_id,
                          sw_descriptor_t *resp, int timeout_ms);

/* Cancel a pending entry (e.g. on error path). */
void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id);

#ifdef __cplusplus
}
#endif

#endif /* DPUMESH_H */
