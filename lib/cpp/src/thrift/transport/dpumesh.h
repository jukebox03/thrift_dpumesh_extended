/*
 * dpumesh.h - DPUmesh transport public API for Thrift
 *
 * Public C API for the NVIDIA DOCA (Comch + DMA) DPUmesh backend,
 * compiled into libthrift when configured with -DWITH_DOCA=ON.
 * Application code should include only this header.
 */

#ifndef DPUMESH_H
#define DPUMESH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

#include "doca/dpumesh_common.h"

/* ====== Default constants ====== */
#define DPUMESH_SLOT_SIZE_DEFAULT       8192            /* 8KB */
/* Slot pool size — used both for host TX (outgoing) and host RX (incoming
 * application data). num_slots × slot_size MUST equal DPU_BUFFER_SIZE
 * so slot-based admission directly bounds in-flight bytes inside DPU.
 * Bumped 1024 → 2048 (16 MB per buffer): provides 2× headroom which lets
 * the DPA-side admission gate's lazy-refresh tolerate larger cache lag
 * without false-positive defers, smoothing the latency curve at cap. */
#define DPUMESH_NUM_SLOTS_DEFAULT       2048
#define DPUMESH_MAX_DESCRIPTORS_DEFAULT 2048

/* ====== Configuration ====== */
typedef struct {
    int num_slots;        /* slots per pool (0 = use default 2048) */
    int slot_size;        /* bytes per slot (0 = use default 8192 = 8KB) */
    int max_descriptors;  /* descriptor ring capacity (0 = use default 2048) */
} dpumesh_config_t;

#define DPUMESH_CONFIG_DEFAULT { 0, 0, 0 }

/* ====== SwDescriptor (64 bytes, packed; little-endian field layout) ====== */
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

/* ====== Debug/localization stats (host bottleneck analysis) ======
 * rx_depth   = current rx_queue occupancy (requests delivered, awaiting a
 *              worker dequeue). High → this pod's RX consumers can't keep up.
 * tx_inflight= TX slots currently allocated (held across the RTT). Near
 *              num_slots → this pod is TX-slot starved (downstream not freeing). */
void dpumesh_debug_stats(dpumesh_ctx_t *ctx, int *rx_depth, int *tx_inflight);

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

/* Non-blocking poll for a response matching req_id (async-client model).
 * Returns:
 *    0 = response arrived (resp filled; TX already freed; caller must free the
 *        body via dpumesh_rx_free(resp->body_buf_slot)),
 *    1 = not ready yet — caller should poll again later,
 *   -1 = error/abandoned (no live pending for this req_id).
 * Pairs with DPUMESH_ASYNC_CLIENT: in that mode the PE thread stops the
 * per-request response wakeup (pthread_cond_signal), so polling is the only way
 * to observe completion. A given client process must use EITHER wait_response
 * (blocking) OR poll_response (async) consistently, never both. */
int dpumesh_poll_response(dpumesh_ctx_t *ctx, uint32_t req_id,
                          sw_descriptor_t *resp);

/* Associate a TX slot with a pending request (call after successful enqueue).
 * On timeout, the TX slot is deferred until DPA finishes processing. */
void dpumesh_pending_attach_tx(dpumesh_ctx_t *ctx, uint32_t req_id, int tx_slot);

/* Cancel a pending entry (e.g. on error path).
 * If TX is attached and DPA may still be using it, defers cleanup
 * until the response arrives (state -2 → rx_data_hook frees TX). */
void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id);

/* Asynchronously release a pending entry registered via dpumesh_register_pending.
 * Intended for responder-side use (e.g. server sending OP_RESPONSE) after
 * enqueue + attach_tx, when no response is expected on this req_id and the
 * caller does NOT want to call dpumesh_wait_response.
 *
 * Behavior (only acts on state == 0):
 *   - tx_slot still attached: transition to state -2; TX_ACK handler will
 *     free the TX slot and clear the entry (state -2 → -1).
 *   - tx_slot already released by an earlier TX_ACK: clear immediately
 *     (state 0 → -1) so the slot is reusable for future register_pending.
 *   - any other state: no-op (already managed by another path).
 *
 * Idempotent. Safe to call concurrently with TX_ACK arrival. */
void dpumesh_pending_release_async(dpumesh_ctx_t *ctx, uint32_t req_id);

#ifdef __cplusplus
}
#endif

#endif /* DPUMESH_H */
