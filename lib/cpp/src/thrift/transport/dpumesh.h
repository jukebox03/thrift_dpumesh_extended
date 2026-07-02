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
/* Host-internal descriptor (NOT a wire layout): the façade builds it for
 * dpumesh_enqueue (translated to dma_desc) and dpumesh_dequeue fills it from a
 * delivered completion. Carries the oriented endpoint tuple — see
 * design-endpoint-tuple.md §12.2. */
typedef struct {
    int32_t  body_buf_slot;         /* TX slot (send) | RX landing byte-offset (recv) */
    uint32_t body_len;
    /* ---- oriented endpoint tuple ---- */
    int16_t  src_pod;               /* sender pod (always concrete) */
    uint16_t src_port;              /* sender port */
    int16_t  src_service;           /* sender's own service (= ep service_id); SVC_NONE if none */
    int16_t  dst_service;           /* peer service (routing input when dst_pod==BLANK) */
    int16_t  dst_pod;               /* dest pod; DMESH_POD_BLANK(-1) -> DPU resolves dst_service */
    uint16_t dst_port;              /* dest port; DMESH_PORT_BLANK(0) -> accept queue */
    uint16_t seq;                   /* per-conn sequence (match key with port) */
    int8_t   valid;
    uint8_t  route_group;           /* route-affinity key (0 = normal per-message LB); the façade
                                     * SAR stamps all chunks of one large message with the same
                                     * value so the DPU routes them to ONE backend (reassembly). */
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

/* ====== Connection API (connection-oriented, full-duplex — no RPC matching) ======
 *
 * A "port" IS a connection (like a socket fd): it owns a peer, an inbound message
 * queue, and an optional per-conn readiness eventfd. Inbound is routed by dst_port
 * to the conn's inbox; there is NO request↔response matching. */

/* Endpoint roles for dpumesh_alloc_port. */
#define DMESH_ROLE_FREE          0
#define DMESH_ROLE_CLIENT        1
#define DMESH_ROLE_SERVER        2
/* Model B: a new server conn the PE created at message-1 delivery but the app has
 * not accepted yet. Inbound coalesces to its inbox (so pipelined messages 2..P
 * don't re-hit the accept queue); next_ready skips it; dmesh_accept promotes it
 * to DMESH_ROLE_SERVER. */
#define DMESH_ROLE_SERVER_PENDING 3

/* Allocate a host-unique conn port (>=1) as CLIENT or SERVER (allocates its inbound
 * ring); 0 on exhaustion. `user` is the app's conn handle, returned later by
 * dpumesh_next_ready (stored before the port goes live so a ready-list entry never
 * dereferences NULL). Release with dpumesh_free_port (reclaims undelivered inbound
 * credits). */
uint16_t dpumesh_alloc_port(dpumesh_ctx_t *ctx, int role, void *user);
/* Allocate a SPECIFIC port slot (model B accept): bind exactly the DPU-assigned
 * upstream id `port` (a uP in [DMESH_UPORT_BASE, 65535)). Returns `port` on
 * success, 0 if the slot is already live. */
uint16_t dpumesh_alloc_port_specific(dpumesh_ctx_t *ctx, uint16_t port, int role, void *user);
/* Promote a PE-created DMESH_ROLE_SERVER_PENDING slot to a live SERVER conn:
 * attach the app's conn handle `user`. Returns `port` on success, 0 if the slot
 * is not pending (already accepted / freed / race). Used by dmesh_accept. */
uint16_t dpumesh_accept_port(dpumesh_ctx_t *ctx, uint16_t port, void *user);
void     dpumesh_free_port(dpumesh_ctx_t *ctx, uint16_t port);

/* Pop the next inbound message descriptor for a conn (CLIENT or SERVER — one path).
 * Returns 1 + fills *out (body at *out->body_buf_slot in the shared RX mmap; free
 * via dpumesh_rx_free after reading), or 0 if the conn inbox is empty. */
int dpumesh_conn_recv(dpumesh_ctx_t *ctx, uint16_t port, sw_descriptor_t *out);

/* Pop the next READY conn (one whose inbox went empty→non-empty since you last
 * drained it) and return the `user` handle registered at alloc; NULL when drained.
 * The single endpoint fd (dpumesh_get_event_fd) wakes you; this names the conns to
 * service WITHOUT scanning every conn or holding a per-conn fd. Drain each returned
 * conn to EAGAIN (edge-triggered re-arm). Single-consumer (the event-loop thread). */
void *dpumesh_next_ready(dpumesh_ctx_t *ctx);

/* Record a SENT TX slot for (port,seq) so its DPU TX_ACK frees it (the BATCH_FWD_ACK
 * handler reclaims it). Call after a successful dpumesh_enqueue. A conn may have many
 * un-ACKed slots in flight (full-duplex / pipelined); never blocks. */
void dpumesh_tx_track(dpumesh_ctx_t *ctx, uint16_t port, uint16_t seq, int tx_slot);

#ifdef __cplusplus
}
#endif

#endif /* DPUMESH_H */
