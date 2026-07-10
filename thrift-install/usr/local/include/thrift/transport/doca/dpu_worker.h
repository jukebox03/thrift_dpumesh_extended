#ifndef DPU_WORKER_H
#define DPU_WORKER_H

#include "object.h"

/* ====== DPU Worker ====== */

void run_dpu_worker(struct objects *objs);

/* ====== Exposed to the SG-DMA egress engine (dpu_proxy.c) ======
 * These live in dpu_worker.c; the egress engine calls them so request/reply
 * egress reuses the SAME batched TX_ACK / REV_DONE accumulators and the SAME
 * L4 default route. */

/* L4 default route (service table + route-affinity pin). Used for a DEFERred
 * request seg. Returns a live pod_id or -1 (unroutable). */
int32_t dpu_route_l4(struct objects *objs, int16_t svc, uint8_t route_group);

/* Accumulate one TX_ACK into src_pod's batch (custody release to the sender). */
void batch_or_send_tx_ack(struct objects *objs, struct pod_state *src_pod,
                          uint16_t port, uint16_t seq);

/* Flush a pod's accumulated REV_DONE batch as one message (idle/full flush). */
void flush_rev_done_batch(struct objects *objs, struct pod_state *pod);

#endif /* DPU_WORKER_H */
