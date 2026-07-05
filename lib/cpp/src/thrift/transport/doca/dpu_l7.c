/* ===================================================================
 * dpu_l7.c — WRITE YOUR L7 PROXY HERE
 * ===================================================================
 *
 * This is the only file you edit. Implement dmesh_l7_route(): given the HEAD of
 * the front message, say how long the WHOLE message is and (optionally) which
 * service it goes to. The engine ships the whole message (head + body) from
 * staging via scatter-gather — you never see or copy the body. Read dpu_l7.h
 * for the full contract.
 *
 * The default parses the SAME length-prefixed framing as the byte-stream
 * validator (bench/stream_sock.c):
 *     [u32 LE total_len (incl. this 5-byte header)][u8 svc][payload ...]
 * and routes each message to the service named by its `svc` byte. Because it
 * matches the validator, `./test-bench.sh stream` can drive this L7 path
 * directly (compare against DPUMESH_PROXY=frame — results must be identical).
 * Replace the body with your own protocol.
 * ===================================================================
 */

#include "dpu_l7.h"
#include <string.h>

#define L7_HDR      5u                     /* [u32 total_len][u8 svc] */
#define L7_MSG_MAX  (16u * 1024u * 1024u)  /* sanity cap on one message */

int dmesh_l7_route(const uint8_t *buf, uint32_t len,
                   const struct dmesh_l7_ctx *ctx, int32_t *target)
{
    (void)ctx;                             /* default routes by the svc byte, below */

    if (len < L7_HDR)
        return 0;                          /* 5-byte head not fully in the window yet */

    uint32_t total;
    memcpy(&total, buf, 4);                /* [u32 LE total_len], includes this 5-byte header */
    if (total < L7_HDR || total > L7_MSG_MAX)
        return -1;                         /* implausible → drop the connection */

    /* Route by the svc byte (content routing). 0xFF or out-of-range → keep the
     * client's addressed service (*target is pre-set to ctx->service). */
    uint8_t svc = buf[4];
    if (svc != 0xFF && svc < 128)
        *target = (int32_t)svc;

    /* We know the whole length from the head; the body need not be here — the
     * engine streams it from staging. Return the total message length. */
    return (int)total;
}
