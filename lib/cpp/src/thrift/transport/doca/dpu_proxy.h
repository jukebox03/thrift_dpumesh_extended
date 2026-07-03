#ifndef DPU_PROXY_H
#define DPU_PROXY_H

/* ===================================================================
 * DPU L7-proxy L4 engine (plan.md) — proxy_route interface + machinery
 * ===================================================================
 *
 * The DPU is an envoy-like L7 proxy: an L7 function (`proxy_route`, MOCK for
 * now) parses a CONNECTION's input bytes and returns routing segments
 * {off,len,dst}; this L4 engine executes them:
 *
 *   per-conn input window  →  proxy_route(segs)  →  per-destination SG-DMA
 *   (ordered bytes, cursor,    (L7 owns parsing      gather (ARM generic
 *    unconsumed tail kept)      + routing/LB)         doca_dma, one op + one
 *                                                     batched notify per dst)
 *
 * Requests AND replies both run through this machinery (symmetric — required
 * so a future envoy can process replies too). A request's dst is DECIDED by
 * the proxy; a reply's dst is already determined by the upstream it returns
 * on — the L4 conntrack table provides it and the proxy only CONFIRMS.
 *
 * Custody: the sender's TX slot is held END-TO-END — the SG-DMA reads the
 * message bytes IN PLACE from DPU staging, so TX_ACK fires only when the
 * egress DMA that read those bytes completes (batched). Early release would
 * let the host overwrite the staging bytes mid-read.
 *
 * Enabled by env DPUMESH_PROXY (unset = engine absent, legacy per-slot path
 * bit-identical):
 *   DPUMESH_PROXY=passthru|1  one seg per arrived message, dst deferred to
 *                             the L4 default route (service table + route-
 *                             affinity pins) — wire-identical message
 *                             boundaries and routing; parity/regression mock.
 *   DPUMESH_PROXY=frame       length-prefixed frame parser (the byte-stream
 *                             demo): [u32 len][u8 svc][payload]; waits for
 *                             whole frames (exercises the window/tail/seam),
 *                             routes each frame by its svc byte (gateway-
 *                             style), any length (a >8KB frame is delivered
 *                             as consecutive <=8KB byte-stream chunks).
 */

#include <stdint.h>

struct objects;

/* ---- proxy return format (plan.md — the deliverable) ---- */

struct dmesh_route_seg {
    uint32_t off;    /* start within this call's input window (buf) */
    uint32_t len;    /* segment length (> 0; a 0-length wire msg is the FIN) */
    int32_t  dst;    /* destination backend pod (LB already done by L7), or
                      * DMESH_SEG_DST_DEFER = let the L4 default route decide
                      * (service table + route-affinity pin of these bytes). */
};

/* Defer sentinel — let the L4 default route (service table + route-affinity) pick. */
#define DMESH_SEG_DST_DEFER (-2)

/* The proxy's view of one connection (one direction of one stream). The L4
 * engine owns the struct; the proxy reads it and may keep state in `user`. */
typedef struct dmesh_proxy_conn {
    int32_t  src_pod;      /* stream identity: sender pod ... */
    uint16_t src_port;     /* ... and sender port (client port, or uP) */
    int      is_reply;     /* 1 = upstream reply stream: dst is predetermined —
                            * peer_* below is the conntrack answer; the proxy
                            * confirms (returns peer_pod) rather than routing */
    int16_t  dst_service;  /* the service the client addressed (request streams) */
    int32_t  peer_pod;     /* reply streams: table-resolved destination (client) */
    uint16_t peer_port;    /* reply streams: the client's real conn port */
    void    *user;         /* proxy-owned per-conn state (L4 never touches it) */
} dmesh_proxy_conn;

/*
 * proxy_route — the L7 seam (MOCK now, envoy later). Called with the
 * connection's input bytes COLLECTED IN ORDER and shown contiguously.
 *
 *   buf/avail : the window's unconsumed bytes. `avail` may be less than the
 *               total buffered — returning *consumed==0 makes the L4 present
 *               a LARGER contiguous view next call (tail + later bytes are
 *               aligned into a seam buffer), up to the seam cap.
 *   segs/max  : out: up to `max` segments {off,len,dst} to ship.
 *               Contract: segments lie within [0, *consumed), do not overlap,
 *               and are emitted in ascending off (per-conn byte order is the
 *               delivery order). Consumed bytes NOT covered by any segment
 *               are dropped (filtered) by the L4.
 *   consumed  : out: bytes fully processed; the window cursor advances by
 *               this and the unconsumed tail is kept for the next call.
 *   return    : number of segments (>= 0), or < 0 = protocol error → the L4
 *               drops this connection's stream (poison).
 *
 * Runs on the single ARM worker thread — no locking, but do not block.
 */
typedef int (*dmesh_proxy_route_fn)(struct objects *objs, dmesh_proxy_conn *conn,
                                    const uint8_t *buf, uint32_t avail,
                                    struct dmesh_route_seg *segs, int max,
                                    uint32_t *consumed);

/* Max segments the engine requests per proxy_route call. */
#define DMESH_PROXY_SEG_MAX 32

/* ---- engine lifecycle / hooks (called from dpu_worker.c) ---- */

/* Create the engine if env DPUMESH_PROXY selects a mock (NULL env = engine
 * off, objs->proxy stays NULL and everything is bit-identical). Requires
 * objs->dev + objs->pe live. Returns DOCA_SUCCESS also when off. */
int px_init(struct objects *objs);

/* Ingest one COMP_ENTRY_FORWARD (data or FIN, request or reply — symmetric).
 * Returns 1 = consumed, 0 = retry later (transient resource exhaustion; the
 * caller keeps the completion queued), -1 = dropped (sender TX_ACKed). */
int px_ingest_forward(struct objects *objs, void *entry /* dpu_comp_entry_t* */);

/* One drain pass: submit per-destination SG-DMA batches, emit completed
 * batches' REV_DONE entries + custody TX_ACKs, kick credit refreshes.
 * Returns non-zero if any progress was made. */
int px_drain(struct objects *objs);

/* Idle flush (proc==0 in the main loop): nothing px-specific today — REV_DONE
 * and TX_ACK ride the per-pod batches flushed by dpu_worker's idle flush. */

#endif /* DPU_PROXY_H */
