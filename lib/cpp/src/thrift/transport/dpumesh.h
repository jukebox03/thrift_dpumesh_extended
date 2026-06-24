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
/* Slot pool size (host TX + host RX). num_slots × slot_size MUST equal
 * DPU_BUFFER_SIZE so slot-based admission bounds in-flight bytes inside DPU. */
#define DPUMESH_NUM_SLOTS_DEFAULT       4096
/* The host→DPU descriptor ring depth is NOT configurable: it is the wire-ABI
 * constant DMA_RING_SIZE (doca/dpumesh_common.h), which the host and the DPA
 * kernel must agree on at build time. */

/* ====== Configuration ====== */
typedef struct {
    int num_slots;        /* slots per pool (0 = default) */
    int slot_size;        /* bytes per slot (0 = default) */
} dpumesh_config_t;

#define DPUMESH_CONFIG_DEFAULT { 0, 0 }

/* ====== SwDescriptor (host-internal RX/TX descriptor, packed) ====== */
typedef struct __attribute__((packed)) {
    int32_t  header_buf_slot;       /* i  (always -1 for Thrift) */
    uint32_t header_len;            /* I  (always 0 for Thrift)  */
    int32_t  body_buf_slot;         /* i */
    uint32_t body_len;              /* I */
    uint32_t req_id;                /* I  (stream_id) */
    int32_t  dst_pod_id;            /* i */
    int32_t  src_pod_id;            /* i */
    int8_t   flags;                 /* b */
    int8_t   valid;                 /* b */
} sw_descriptor_t;

/* ====== Opaque context ====== */
typedef struct dpumesh_ctx dpumesh_ctx_t;

/* ====== Lifecycle ====== */
int  dpumesh_init(dpumesh_ctx_t **ctx, const char *app_name, int worker_id,
                  const dpumesh_config_t *config);  /* NULL = use defaults */
void dpumesh_destroy(dpumesh_ctx_t *ctx);

/* ====== Query configured values ====== */
int dpumesh_get_slot_size(dpumesh_ctx_t *ctx);

/* Enable + return a readiness eventfd: a real fd that becomes readable whenever an
 * inbound request/response is delivered. Wait on it with a VANILLA epoll/poll/select
 * (no busy-poll); on wakeup, drain it with one read() of a uint64_t, then collect
 * work via dpumesh_dequeue(0) / dpumesh_poll_response(). Returns -1 on failure.
 * Idempotent; the PE thread (DPUMESH_HOST_EPOLL=1) drives it notification-style. */
int dpumesh_get_event_fd(dpumesh_ctx_t *ctx);

/* ====== Info ====== */
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

/* Allocate a TX buffer slot. Returns a slot index (>=0). Under backpressure
 * (free-list empty) it BUSY-SPINS with capped backoff until a slot frees — it
 * does not fail/return -1. */
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

/* Non-blocking poll for a response matching req_id.
 * Returns:
 *    0 = response arrived (resp filled; TX already freed; caller must free the
 *        body via dpumesh_rx_free(resp->body_buf_slot)),
 *    1 = not ready yet — caller should poll again later,
 *   -1 = error/abandoned (no live pending for this req_id).
 * Polling is the only completion model (there is no blocking wait); the PE
 * thread delivers responses into the pending table + raises the readiness
 * eventfd (dpumesh_get_event_fd) for a native epoll_wait(). */
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
 * enqueue + attach_tx, when no response is expected on this req_id (so the
 * pending entry is released without polling for a response).
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
