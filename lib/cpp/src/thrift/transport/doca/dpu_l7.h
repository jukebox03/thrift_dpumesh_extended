#ifndef DPU_L7_H
#define DPU_L7_H

/* ===================================================================
 * dpu_l7.h — the L7 proxy hook (write your parser in dpu_l7.c)
 * ===================================================================
 *
 * This is the ONLY interface an L7 author needs. You implement ONE function,
 * `dmesh_l7_route`, in dpu_l7.c. Everything else (byte-stream reassembly,
 * scatter-gather DMA, custody, credits, the DPU/DOCA machinery) is handled for
 * you by the L4 engine — you never see it.
 *
 * Mental model: a connection is a byte stream. You are shown only the HEAD of
 * the message at the FRONT (up to a small bounded window) and asked two things —
 * "how long is this whole message, and where does it go?" You read the head
 * (length prefix / routing key) and answer. The engine then forwards the whole
 * message (head + body, even the parts you never saw) from staging via
 * scatter-gather DMA — the BODY is never copied — and calls you again for the
 * next message. This is why a large message costs no per-slot memcpy.
 *
 * You are called only for REQUEST streams. Replies are forwarded back to the
 * client automatically — you never handle them.
 * ===================================================================
 */

#include <stdint.h>

/* Read-only context for one routing decision (no DPU/DOCA types). */
struct dmesh_l7_ctx {
    int32_t  service;      /* the service id the client addressed (the default
                            * target; also your routing "base") */
    int32_t  client_pod;   /* who sent this stream (informational) */
    uint16_t client_port;  /* the client's port    (informational) */
};

/*
 * dmesh_l7_route — YOU IMPLEMENT THIS (in dpu_l7.c).
 *
 * buf[0 .. len) is the HEAD of the front message — NOT the whole message. `len`
 * is a small bounded window (the body usually is NOT here). Read the head only.
 *
 *   return  > 0  (N) : the front message is N bytes TOTAL (head + body). The
 *                      engine ships all N bytes from staging (even the body you
 *                      never saw) to the target, then calls you again at the
 *                      next message. N MAY be much larger than `len`.
 *
 *   return  == 0     : the HEAD is not fully in buf yet. The engine gives you a
 *                      few more contiguous head bytes and calls again. (Used when
 *                      a head straddles a slot boundary.) Do not set *target.
 *
 *   return  <  0     : malformed / protocol violation → the engine drops
 *                      (poisons) this connection.
 *
 * *target : pre-set to ctx->service. To route by content, overwrite it with a
 *           different SERVICE id (0..127) — NOT a pod. The engine resolves the
 *           service to a backend (and load-balances). An unknown service makes
 *           the engine drop that message.
 *
 * CONTRACT (keep these and you cannot corrupt the engine):
 *   1. Determine N from the HEAD (e.g. a length prefix). Do NOT wait for the
 *      body — you will not see it. Return 0 ONLY when the head itself is
 *      incomplete; return N>0 as soon as you know the total length.
 *   2. Read only within buf[0 .. len). Never read past `len`.
 *   3. Be STATELESS. On a 0-return you are re-invoked from the same buf[0] with
 *      more head bytes. Re-parse.
 *   4. The HEAD must fit the head window (<= 4 KB, PX_HEAD_MAX). A head that
 *      needs more than that poisons the conn (cf. Envoy max_request_headers_kb).
 *      The BODY has no such limit — it streams.
 *   5. Never block, never malloc/free, never log — this is the hot path.
 *   6. Runs on a single thread — no locking needed.
 */
int dmesh_l7_route(const uint8_t *buf, uint32_t len,
                   const struct dmesh_l7_ctx *ctx, int32_t *target);

#endif /* DPU_L7_H */
