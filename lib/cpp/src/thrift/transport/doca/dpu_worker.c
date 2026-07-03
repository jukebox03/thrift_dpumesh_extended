#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "dpu_worker.h"

#include "comch_server.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "dpa.h"
#include "dpa_common.h"
#include "comch_msgq.h"
#include "buffer.h"
#include "ring.h"
#include "../dpumesh.h"

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/epoll.h>     /* event-driven DPU main loop (epoll on PE handles) */

DOCA_LOG_REGISTER(DPU_WORKER);

/* ====== TX_ACK send helper ====== */

/* Try to send TX_ACK; on EAGAIN park to the deferred queue. No-op when src_pod
 * is gone (its host — and the slot's custody list — died with it). */
static void
send_or_defer_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint16_t port, uint16_t seq)
{
    if (!src_pod || !src_pod->connection)
        return;

    struct dmesh_tx_ack_entry e = { .port = port, .seq = seq };
    doca_error_t r = server_send_batch_tx_ack_to(objs, src_pod->connection, &e, 1);
    if (r == DOCA_SUCCESS)
        return;

    if (r == DOCA_ERROR_AGAIN) {
        if (objs->num_deferred_tx_acks < MAX_DEFERRED_TX_ACK) {
            int n = objs->num_deferred_tx_acks++;
            objs->deferred_tx_acks[n].conn = src_pod->connection;
            objs->deferred_tx_acks[n].port = port;
            objs->deferred_tx_acks[n].seq  = seq;
        } else {
            DOCA_LOG_ERR("deferred TX_ACK queue full — dropping port=%u seq=%u (pod %d). "
                         "Host TX slot stays in custody until close.",
                         port, seq, src_pod->pod_id);
        }
        return;
    }

    DOCA_LOG_WARN("TX_ACK failed for port=%u seq=%u to pod %d: %s",
                  port, seq, src_pod->pod_id, doca_error_get_descr(r));
}

/* ====== Batched TX_ACK ====== */

/* Flush a pod's accumulated TX_ACK batch as one message. On AGAIN the batch is
 * retained (retried by the next flush, including the idle proc==0 flush). */
static void
flush_txack_batch(struct objects *objs, struct pod_state *pod)
{
    if (!pod || pod->txack_batch_n == 0)
        return;
    if (!pod->connection) { pod->txack_batch_n = 0; return; }
    doca_error_t r = server_send_batch_tx_ack_to(objs, pod->connection,
                                                 pod->txack_batch, pod->txack_batch_n);
    if (r != DOCA_ERROR_AGAIN)
        pod->txack_batch_n = 0;   /* sent (a hard error drops the batch) */
}

/* Accumulate one TX_ACK into the src pod's batch; flush when full. Falls back to
 * the single-send path when the pod is gone or the batch is already full and a
 * prior flush is still pending (AGAIN). */
static void
batch_or_send_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint16_t port, uint16_t seq)
{
    if (!src_pod || !src_pod->connection) {
        send_or_defer_tx_ack(objs, src_pod, port, seq);
        return;
    }
    if (src_pod->txack_batch_n >= BATCH_TXACK_MAX) {
        /* Batch full and not yet drained (send pool busy) — single-send this one
         * so no ack is lost; the full batch is retried by the idle proc==0 flush. */
        send_or_defer_tx_ack(objs, src_pod, port, seq);
        return;
    }
    src_pod->txack_batch[src_pod->txack_batch_n].port = port;
    src_pod->txack_batch[src_pod->txack_batch_n].seq  = seq;
    src_pod->txack_batch_n++;
    if (src_pod->txack_batch_n >= BATCH_TXACK_MAX)
        flush_txack_batch(objs, src_pod);
}

/* ====== Batched REV_DONE (mirror of batched TX_ACK) ======
 * The single host PE thread reaping one REV_DONE comch msg per response is the
 * 2-pod cap; coalescing K responses into one msg cuts the PE reap rate K-fold. */

/* Flush a pod's accumulated REV_DONE batch as one message. On AGAIN the batch is
 * retained (retried by the next flush, including the idle proc==0 flush). */
static void
flush_rev_done_batch(struct objects *objs, struct pod_state *pod)
{
    if (!pod || pod->rev_done_batch_n == 0)
        return;
    if (!pod->connection) { pod->rev_done_batch_n = 0; return; }
    doca_error_t r = server_send_batch_rev_done_to(objs, pod->connection,
                                                   pod->rev_done_batch, pod->rev_done_batch_n);
    if (r == DOCA_ERROR_AGAIN)
        return;  /* retain; retried */
    pod->rev_done_batch_n = 0;   /* sent (a hard error drops the batch) */
}

/* Accumulate one REV_DONE into the target pod's batch; flush when full. Returns
 * 1 = accumulated, 0 = batch full AND a prior flush is still AGAIN (caller must
 * retain the comp entry → ingest backpressure, exactly like the un-batched send). */
static int
batch_or_send_rev_done(struct objects *objs, struct pod_state *target_pod,
                       const dpu_comp_entry_t *ce)
{
    if (target_pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        flush_rev_done_batch(objs, target_pod);          /* try to make room */
    if (target_pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        return 0;                                        /* still full (send pool busy) */
    struct dmesh_rev_done_entry *e = &target_pod->rev_done_batch[target_pod->rev_done_batch_n++];
    e->src_pod_id  = (int8_t)ce->src_pod_id;
    e->src_service = (int8_t)ce->src_service;
    e->dst_service = (int8_t)ce->dst_service;
    e->_pad = 0;
    e->src_port = ce->src_port;
    e->dst_port = ce->dst_port;
    e->seq = ce->seq;
    e->length = (uint16_t)ce->length;
    e->pos = ce->buf_offset;                             /* landing pos in host RX buffer */
    if (target_pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        flush_rev_done_batch(objs, target_pod);
    return 1;
}

/* ====== Reverse DMA Enqueue (DPU→CPU) ====== */

/*
 * Enqueue a reverse-DMA descriptor for in-place forwarding.
 *
 * Source = src_pod->dma_buffer at src_buf_offset (where forward DMA landed);
 * no DPU-side staging copy. Each descriptor carries the source mmap handle and
 * full virtual address so the DPA reverse handler dispatches to the right pod's
 * buffer per request.
 *
 * Slot lifecycle: src's RX slot is held from forward-completion through
 * reverse-completion (full RTT). TX_ACK to src is therefore deferred to
 * process_rev_notify_entry, not sent here.
 *
 * Returns DOCA_SUCCESS or DOCA_ERROR_AGAIN (TX descriptor ring full).
 */
static doca_error_t
dpu_enqueue_reverse_dma(struct pod_state *src_pod,
                        struct pod_state *dst_pod, int ring_k,
                        const dpu_comp_entry_t *ce, int32_t resolved_dst_pod,
                        uint32_t src_buf_offset, uint32_t body_len,
                        uint16_t out_src_port, uint16_t out_dst_port)
{
    /* Post the reverse desc to dst_pod's ring ring_k (the EU-sharding ring whose
     * EU owns dst_pod's rev region ring_k). The single ARM thread is the sole
     * writer of every ring → each stays single-producer (lock-free). */
    if (!dst_pod->tx_rings[ring_k]) {
        DOCA_LOG_ERR("dpu_enqueue_reverse_dma: pod %d reverse ring %d not ready", dst_pod->pod_id, ring_k);
        return DOCA_ERROR_NOT_CONNECTED;
    }
    if (!src_pod->dma_buffer || src_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("dpu_enqueue_reverse_dma: src pod %d dma_buffer/handle not ready",
                     src_pod->pod_id);
        return DOCA_ERROR_NOT_CONNECTED;
    }

    /* Post descriptor to TX ring */
    struct dma_desc *dma = get_next_dma_desc(dst_pod->tx_rings[ring_k]);
    if (!dma) {
        DOCA_LOG_WARN("dpu_enqueue_reverse_dma: TX ring %d full for pod %d", ring_k, dst_pod->pod_id);
        return DOCA_ERROR_AGAIN;
    }

    /* Fill descriptor: DPA reverse handler reads the source from
     * (desc->mmap, desc->addr). desc->addr is the full virtual address inside
     * src_pod's local_mmap range (DOCA dma_copy expects raw VA). */
    dma->mmap = src_pod->local_mmap_dpa_handle;
    dma->addr = (uint64_t)src_pod->dma_buffer + src_buf_offset;
    dma->size = body_len;
    /* Full tuple, opaque passthrough → DPA copies into REV_DONE → host demux. */
    dma->seq         = ce->seq;
    dma->src_port    = out_src_port;       /* rewritten by the caller (model B) */
    dma->dst_port    = out_dst_port;       /* rewritten by the caller (model B) */
    dma->src_service = (int8_t)ce->src_service;
    dma->dst_service = (int8_t)ce->dst_service;
    dma->dst_pod_id  = resolved_dst_pod;   /* resolved target (direct, not BLANK) */
    dma->src_pod_id  = ce->src_pod_id;     /* forward sender (client for a request) */

    __sync_synchronize();
    dma->valid = 1;

    return DOCA_SUCCESS;
}

/* ====== L7 routing seam (MOCK) ======
 *
 * dpu_route() is the single point where the DPU decides the destination pod for a
 * forward completion. Production MOCK: service_table[svc] (one pod per service).
 * Two features layer on top: (a) ROUTE-AFFINITY — a non-zero route_group pins every
 * chunk of one large (SAR) message to ONE backend so they reassemble even under
 * per-message LB; (b) a TEST round-robin LB (DPUMESH_LB_RR) to exercise (a) under
 * real scatter. A real L7 proxy (body parse / LB / policy) plugs into lb_pick().
 * Single ARM thread → the group table + RR cursor need no lock.
 */
static inline int32_t
lb_pick(struct objects *objs, int16_t svc)
{
    if (objs->lb_rr_count > 0) {   /* TEST: round-robin across the configured backend SERVICES */
        for (int i = 0; i < objs->lb_rr_count; i++) {
            /* Entries are SERVICE ids (pod_ids are DPU-assigned, not knowable by
             * the test harness); resolve each to its pod via the service table. */
            int32_t s = objs->lb_rr_pods[(objs->lb_rr_cursor++) % (uint32_t)objs->lb_rr_count];
            int32_t p = (s >= 0 && s < POD_ID_SPACE) ? objs->service_table[s] : -1;
            if (p >= 0 && find_pod_by_id(objs, p))
                return p;
        }
        return -1;
    }
    if (svc >= 0 && svc < POD_ID_SPACE) {   /* production: single backend per service */
        int p = objs->service_table[svc];
        if (p >= 0)
            return p;
    }
    return -1;
}

static inline int32_t
dpu_route(struct objects *objs, const dpu_comp_entry_t *entry)
{
    /* Already resolved (established request or any response → dst_pod filled by the
     * client/server) → deliver direct, no re-routing (connection-level LB). */
    if (entry->dst_pod_id != DMESH_POD_BLANK)
        return entry->dst_pod_id;

    int16_t svc = entry->dst_service;
    uint8_t rg  = entry->route_group;        /* 0 = normal LB; 1..255 = affinity group */

    /* Route-affinity: an auto-chunked large message stamps EVERY chunk with the same
     * non-zero route_group (a per-channel rolling id, so one channel's concurrent
     * messages differ); a dmesh_pin_route'd conn stamps its one id on every message.
     * All messages sharing a key pin to ONE backend. Whichever message reaches this
     * single ARM thread FIRST LB-picks (lb_pick — RR under the DPUMESH_LB_RR test,
     * else the service table) + records the pin; the rest reuse it.
     * The table is keyed (dst_service, rg), NOT rg alone: id counters are per-channel
     * and only 255 wide, so unrelated channels/conns routinely reuse a byte — service
     * scoping confines a collision to same-service traffic (both pin to one backend:
     * balance skew, ordering/reassembly intact) and makes cross-service redirection
     * structurally impossible (an rg reused by another service gets its own entry).
     * OVERWRITE-ON-REUSE, no delete: ORDER-INDEPENDENT (chunks that round-robin across
     * EU-sharding rings can reach here out of order, yet all resolve to the one
     * recorded backend); a pin to a since-dead backend is re-picked. Single ARM
     * thread → no lock. (is_last-DELETE was rejected: it would free a pin mid-message
     * under either hazard → scatter. See scale_log 2026-07-02.) */
    if (rg != 0) {
        if (svc < 0 || svc >= POD_ID_SPACE)
            return -1;                       /* unroutable either way (caller drops) */
        int32_t pinned = objs->route_group_backend[svc][rg];
        if (pinned >= 0 && find_pod_by_id(objs, pinned))
            return pinned;
        int32_t b = lb_pick(objs, svc);
        if (b >= 0) objs->route_group_backend[svc][rg] = b;
        return b;
    }

    /* No affinity (route_group==0) → normal per-message routing via the service table.
     * The TEST RR-LB (lb_pick) is applied ONLY to route_group!=0 (SAR) traffic above,
     * so it never hijacks ordinary RPC / pipeline / loopback routing — one deploy can
     * validate those regressions AND SAR affinity together. Unknown service → -1
     * (caller drops + TX_ACKs the sender). */
    if (svc >= 0 && svc < POD_ID_SPACE) {
        int p = objs->service_table[svc];
        if (p >= 0)
            return p;
    }
    return -1;
}

/* ====== Deferred Completion Queue Drain ====== */

/*
 * Process a COMP_ENTRY_FORWARD entry: enqueue reverse DMA for in-place
 * forwarding. No TX_ACK on the success path — that fires from
 * process_rev_notify_entry once reverse DMA completes (the slot must stay held
 * in src's dma_buffer until DPA finishes reading it). Error paths do send
 * TX_ACK here, since reverse will not fire.
 *
 * Returns 1 if processed, 0 if should retry (TX desc ring full), -1 on error.
 */
static int
process_forward_entry(struct objects *objs, dpu_comp_entry_t *entry)
{
    struct dpu_conntrack *ct = objs->conntrack;

    /* Original sender identity for the ERROR-path TX_ACK, captured BEFORE any
     * rewrite. For a client request this is the client's real (pod,port); for a
     * backend reply it is the backend's (pod,uP). The entry itself is never
     * mutated (so a TX-ring-full retry re-runs cleanly). */
    int32_t  ack_pod  = entry->src_pod_id;
    uint16_t ack_port = entry->src_port;

    /* The forward DMA landed in pods[entry->pod_idx]->dma_buffer at
     * entry->buf_offset. That same offset is the source for reverse DMA. */
    struct pod_state *fwd_buf_pod = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods)
        fwd_buf_pod = &objs->pods[entry->pod_idx];
    if (!fwd_buf_pod || !pod_data_ready(fwd_buf_pod) || !fwd_buf_pod->dma_buffer ||
        fwd_buf_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("comp_queue: invalid pod_idx=%d for seq=%u", entry->pod_idx, entry->seq);
        send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
        return -1;
    }

    /* Rewritten tuple for the reverse desc (locals, not the entry). */
    int32_t  resolved_dst_pod;
    uint16_t out_src_port, out_dst_port;

    if (entry->dst_pod_id == DMESH_POD_BLANK && entry->length == 0) {
        /* ---- CLIENT FIN (0-length): fan-out teardown ----
         * Under per-message LB this downstream may have opened upstreams to
         * SEVERAL backends. Tear down EVERY one: scan the registered pods, find
         * each upstream (client_pod, client_port, backend), send it a FIN (0-len
         * reverse, best-effort) so that backend closes its server conn, and free
         * the DPU table entry unconditionally (this is what stops the uP leak).
         * Ack the client's FIN slot directly — the entry still carries the
         * client's REAL port, so no uP translation is needed here. */
        for (int i = 0; i < objs->num_pods; i++) {
            int32_t b = objs->pods[i].pod_id;
            uint16_t uP = dpu_upstream_find(objs->conntrack, entry->src_pod_id, entry->src_port, b);
            if (uP == 0) continue;
            struct pod_state *B = find_pod_by_id(objs, b);
            if (B && pod_data_ready(B) && B->tx_rings[0]) {
                int kr = B->k_rings > 0 ? B->k_rings : 1;
                (void)dpu_enqueue_reverse_dma(fwd_buf_pod, B,
                                              (int)((uint32_t)uP % (uint32_t)kr),
                                              entry, b, entry->buf_offset, 0 /*FIN*/, uP, uP);
            }
            dpu_upstream_free(objs->conntrack, uP);
        }
        send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
        return 1;
    }

    if (entry->dst_pod_id == DMESH_POD_BLANK) {
        /* ---- CLIENT REQUEST: resolve service -> backend, own an upstream ---- */
        int32_t B = dpu_route(objs, entry);
        if (B < 0) {
            DOCA_LOG_ERR("forward: unresolved service=%d seq=%u", entry->dst_service, entry->seq);
            send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
            return -1;
        }
        uint16_t uP = dpu_upstream_find(ct, entry->src_pod_id, entry->src_port, B);
        if (uP == 0) uP = dpu_upstream_create(ct, entry->src_pod_id, entry->src_port, B);
        if (uP == 0) {
            DOCA_LOG_ERR("forward: upstream space full (client %d:%u -> pod %d)",
                         entry->src_pod_id, entry->src_port, B);
            send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
            return -1;
        }
        resolved_dst_pod = B;
        out_src_port = uP;   /* backend sees the sender as (client_pod, uP) ... */
        out_dst_port = uP;   /* ... and demuxes its server conn by dst_port=uP  */
    } else {
        /* ---- BACKEND REPLY: dst_pod concrete, dst_port == uP → map to client ---- */
        struct dpu_upstream *u = (entry->dst_port >= DMESH_UPORT_BASE)
                                     ? &ct->upstream[entry->dst_port] : NULL;
        if (!u || !u->in_use) {
            DOCA_LOG_ERR("reply: stale upstream uP=%u (pod %d) seq=%u",
                         entry->dst_port, entry->dst_pod_id, entry->seq);
            /* backend's data was read out → its slot is free; still ack it */
            send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
            return -1;
        }
        resolved_dst_pod = u->client_pod;
        out_src_port = entry->src_port;   /* backend's (pod,uP) — client ignores src */
        out_dst_port = u->client_port;    /* client demuxes to its real conn */
    }

    struct pod_state *target_pod = find_pod_by_id(objs, resolved_dst_pod);
    if (!target_pod || !pod_data_ready(target_pod) || !target_pod->tx_rings[0]) {
        DOCA_LOG_ERR("forward: dst pod %d not ready seq=%u", resolved_dst_pod, entry->seq);
        send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
        return -1;
    }

    /* conn-sharding (reverse): shard by the DESTINATION conn's (rewritten) port so
     * a conn's inbound stays FIFO on ONE EU. Single ARM writer → no lock. */
    int kr = target_pod->k_rings > 0 ? target_pod->k_rings : 1;
    int ring_k = (int)((uint32_t)out_dst_port % (uint32_t)kr);

    doca_error_t fwd_result = dpu_enqueue_reverse_dma(
        fwd_buf_pod, target_pod, ring_k, entry, resolved_dst_pod,
        entry->buf_offset, entry->length, out_src_port, out_dst_port);

    if (fwd_result == DOCA_ERROR_AGAIN)
        return 0;  /* TX descriptor ring full — preserve entry, retry next iter */
    if (fwd_result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Forward failed seq=%u dst_pod=%d: %s",
                     entry->seq, resolved_dst_pod, doca_error_get_descr(fwd_result));
        send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
        return -1;
    }

    /* Success: TX_ACK fires from process_rev_notify_entry on reverse completion. */
    return 1;
}

/*
 * Drain the deferred TX_ACK queue. Called every main-loop iteration after
 * doca_pe_progress(pe), which releases comch send-pool slots as send
 * completions fire. Returns the number of ACKs successfully sent; the rest
 * stay in the queue for the next iteration.
 */
static int
drain_deferred_tx_acks(struct objects *objs)
{
    if (objs->num_deferred_tx_acks == 0)
        return 0;

    int sent = 0;
    int kept = 0;
    int total = objs->num_deferred_tx_acks;
    for (int i = 0; i < total; i++) {
        deferred_tx_ack_t *d = &objs->deferred_tx_acks[i];
        struct dmesh_tx_ack_entry e = { .port = d->port, .seq = d->seq };
        doca_error_t rc = server_send_batch_tx_ack_to(objs, d->conn, &e, 1);
        if (rc == DOCA_SUCCESS) {
            sent++;
            continue;
        }
        if (rc == DOCA_ERROR_AGAIN) {
            /* Pool still full — keep entry for next iteration. Compact in
             * place so retained entries stay contiguous + FIFO ordered. */
            if (kept != i)
                objs->deferred_tx_acks[kept] = *d;
            kept++;
            continue;
        }
        /* Hard error — log and drop (the slot stays in the host's custody
         * list until that conn closes; only these once-in-a-blue-moon hard
         * failures ever leak one). */
        DOCA_LOG_WARN("deferred TX_ACK fatal for port=%u seq=%u: %s",
                      d->port, d->seq, doca_error_get_descr(rc));
        sent++;  /* count as "removed from queue" */
    }
    objs->num_deferred_tx_acks = kept;
    return sent;
}

/*
 * Process a COMP_ENTRY_REV_NOTIFY entry: reverse DMA completed (DPU→CPU).
 * Send DMA_COMPLETION to the destination host pod, then TX_ACK to the src pod.
 * The src's dma_buffer slot is held through the whole RTT — releasing it earlier
 * would let the host overwrite it before DPA's reverse DMA finished reading.
 *
 * If DMA_COMPLETION returns AGAIN, retry the whole entry next iter (return 0
 * leaves the comp_queue entry in place). TX_ACK has its own deferred queue for
 * the partial case where DMA_COMPLETION succeeded but TX_ACK hits EAGAIN.
 *
 * Returns 1 if processed, 0 if should retry, -1 on error.
 */
static int
process_rev_notify_entry(struct objects *objs, dpu_comp_entry_t *entry)
{
    struct dpu_conntrack *ct = objs->conntrack;
    int32_t target_id = entry->dst_pod_id;
    struct pod_state *target_pod = find_pod_by_id(objs, target_id);

    /* TX_ACK target = the ORIGINAL sender. On the client-request (forward-to-backend)
     * leg the reverse carries dst_port = uP (a DPU upstream id) and src_port was
     * rewritten to uP; translate uP -> the client's real (pod,port) so the CLIENT frees
     * its slot. On the reply leg (dst_port = a real client port) no translation — the
     * sender is the backend at (pod, uP = its real local port). Upstream TEARDOWN
     * happens in the client-FIN fan-out (process_forward_entry), not here. */
    int32_t  ack_pod  = entry->src_pod_id;
    uint16_t ack_port = entry->src_port;
    if (entry->dst_port >= DMESH_UPORT_BASE && ct->upstream[entry->dst_port].in_use) {
        struct dpu_upstream *u = &ct->upstream[entry->dst_port];
        ack_pod  = u->client_pod;
        ack_port = u->client_port;
    }

    if (!target_pod || !target_pod->connection) {
        DOCA_LOG_ERR("REV_NOTIFY: target pod %d not found or no connection", target_id);
        send_or_defer_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
        return -1;
    }

    /* Deliver REV_DONE to the destination host, BATCHED so the host PE reaps 1
     * comch msg per K responses. */
    if (batch_or_send_rev_done(objs, target_pod, entry) == 0)
        return 0;  /* batch full + send pool busy → retain entry (backpressure) */

    /* Release the original sender's TX slot (translated for the client leg). */
    batch_or_send_tx_ack(objs, find_pod_by_id(objs, ack_pod), ack_port, entry->seq);
    return 1;
}

/*
 * Process up to max_batch entries from the deferred completion queue.
 * Called from the main loop to avoid blocking inside consumer callbacks.
 * All comch sends (TX_ACK, REV_DMA notify) happen here — never inside
 * consumer callbacks, which would risk re-entrant doca_pe_progress.
 * Returns number of entries processed.
 */
static int
process_completion_queue(struct objects *objs, int max_batch)
{
    int processed = 0;

    /* PE progress is per-batch, not per-entry: the send pool (CC_SEND_TASK_NUM)
     * and consumer recv pool (CC_DATA_PATH_TASK_NUM) are both ≫ max_batch, so a
     * batch (≤128 entries × 2 sends) cannot exhaust either pool mid-batch. */
    while (processed < max_batch) {
        dpu_comp_entry_t *entry = comp_queue_peek(&objs->comp_queue);
        if (!entry)
            break;

        int result;
        if (entry->entry_type == COMP_ENTRY_REV_NOTIFY) {
            result = process_rev_notify_entry(objs, entry);
            if (result == 0)
                break;  /* comch send busy, retry next iteration */
        } else {
            result = process_forward_entry(objs, entry);
            if (result == 0)
                break;  /* TX buffer full, retry next iteration */
        }

        comp_queue_dequeue(&objs->comp_queue);
        processed++;
    }

    return processed;
}

/* ====== DPU Worker ====== */

/* One pass of the DPU worker's drain work: progress both PEs, retry deferred
 * TX_ACKs, drain the completion queue, release backpressure, drain consumer
 * retries. Returns non-zero if any progress was made (a PE advanced or a
 * completion was processed). The event-driven driver uses this to detect the
 * idle point (when it is safe to arm + block); the busy-poll driver calls it
 * once per iteration. Shared by both drivers so they cannot drift. */
static int
dpu_drain_iteration(struct objects *objs)
{
    uint8_t did_consumer = doca_pe_progress(objs->consumer_pe);
    uint8_t did_ctrl     = doca_pe_progress(objs->pe);  /* new conns, REGISTER, TX_DATA */

    /* Retry deferred TX_ACKs right after pe_progress so just-released send-pool
     * slots are available. */
    drain_deferred_tx_acks(objs);

    /* Drain deferred completion queue (reverse DMA enqueue). 128/pass — safe
     * because consumer_pe is progressed above, keeping DPA recv tasks recycled. */
    int proc = process_completion_queue(objs, 128);
    /* Idle (no completions this pass) → flush partial TX_ACK + REV_DONE batches so
     * low-load latency is not held by coalescing. This is the only batch-flush
     * site (the periodic keepalive no longer flushes here). */
    if (proc == 0)
        for (int i = 0; i < objs->num_pods; i++) {
            flush_txack_batch(objs, &objs->pods[i]);
            flush_rev_done_batch(objs, &objs->pods[i]);
        }

    /* Backpressure release: resubmit deferred recv tasks once the ingest
     * hand-off (comp_queue) drains below BP_LOW. */
    if (objs->num_deferred_recv > 0 &&
        ingest_usage(objs) < COMP_QUEUE_BP_LOW) {
        int remaining = 0, resubmitted = 0, original = objs->num_deferred_recv;
        for (int i = 0; i < original; i++) {
            struct doca_task *t = objs->deferred_recv[i];
            doca_error_t rs = doca_task_submit(t);
            if (rs == DOCA_SUCCESS) {
                resubmitted++;
            } else {
                objs->deferred_recv[remaining++] = t;
                DOCA_LOG_WARN("Deferred recv resubmit failed: %s; retaining",
                              doca_error_get_descr(rs));
            }
        }
        objs->num_deferred_recv = remaining;
        if (resubmitted > 0)
            DOCA_LOG_INFO("Backpressure release: resubmitted %d/%d deferred recv tasks (retained %d)",
                          resubmitted, original, remaining);
    }

    /* Drain consumer_retry tasks stashed by the consumer completion callback. */
    objects_drain_consumer_retry(objs);

    return (did_consumer || did_ctrl || proc > 0);
}

/* Send DPA_MSG_WAKE to every running EU (the ~1 ms keepalive). A parked EU is not
 * woken by a silent forward-ring desc->valid=1 store (no completion), so the ARM
 * pokes it on cadence; it re-scans its rings on the WAKE. No-op under load. */
static void
dpu_send_wake(struct objects *objs)
{
    if (!objs->dpa_thread_running_any)
        return;
    struct comch_msg trigger;
    memset(&trigger, 0, sizeof(trigger));
    trigger.type = DPA_MSG_WAKE;
    for (int k = 0; k < objs->num_dpa_threads; k++)
        if (objs->dpa_thread_running[k])
            (void)dmesh_doca_dpa_msgq_send_try(&objs->dpa_comches[k]->send,
                                                &trigger, sizeof(trigger));
}

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;

    /* Driver selection. DPUMESH_EVENT_LOOP=1 (default) runs the event-driven main
     * loop: the ARM SLEEPS on epoll over the two PE notification handles, waking on
     * a real DPA→DPU completion (FWD_DONE/REV_DONE), a host control message, OR a
     * ~1 ms epoll timeout. On each tick it sends the 1 ms DPU→DPA WAKE keepalive
     * (the DPA EU parks when idle and a silent desc->valid=1 store can't wake it,
     * so the ARM pokes it ~1 kHz). This is NOT busy-poll — the ARM sleeps between
     * ticks, so idle CPU is a few % (≈1 kHz wakeups), vs a full core for busy-poll.
     * EVENT_LOOP=0 = the legacy busy-poll loop (burns a full ARM core), fallback. */
    const double keepalive_sec = 0.001;   /* 1 ms DPU→DPA WAKE cadence */
    struct timespec now, last_kick;
    double kick_elapsed;
    int event_loop = 1;
    { const char *e = getenv("DPUMESH_EVENT_LOOP"); if (e) event_loop = (atoi(e) != 0); }

    DOCA_LOG_INFO("Starting DPU worker");

    /* Init pods table. See object.h pods[] concurrency model — lock-free
     * with atomic publication on `registered`; no mutex needed. */
    memset(objs->pods, 0, sizeof(objs->pods));
    objs->num_pods = 0;

    /* find_pod_by_id's O(1) pod_id->slot map + dpu_route's service_id->pod map
     * start empty (-1 = no live pod / unresolved service). Done before any
     * PE/thread starts, so plain stores are race-free here. */
    for (int i = 0; i < POD_ID_SPACE; i++) {
        objs->pod_id_to_slot[i] = -1;
        objs->service_table[i]  = -1;
    }

    /* Route-affinity table ((service, route_group) -> pinned backend) starts empty;
     * TEST RR-LB off. 0xFF bytes == -1 for two's-complement int32. */
    memset(objs->route_group_backend, 0xFF, sizeof(objs->route_group_backend));
    objs->lb_rr_count = 0;
    objs->lb_rr_cursor = 0;
    { const char *e = getenv("DPUMESH_LB_RR");   /* TEST: "11,13,14" → per-message RR-LB */
      if (e && *e) {
          char tmp[64]; snprintf(tmp, sizeof tmp, "%s", e);
          char *save = NULL, *tok = strtok_r(tmp, ",", &save);
          while (tok && objs->lb_rr_count < 8) {
              objs->lb_rr_pods[objs->lb_rr_count++] = atoi(tok);
              tok = strtok_r(NULL, ",", &save);
          }
          DOCA_LOG_INFO("DPUMESH_LB_RR: per-message round-robin LB across %d backends",
                        objs->lb_rr_count);
      }
    }

    /* Init DPU connection tracking (model B). Heap-allocated — too large to embed
     * on the stack. calloc zeroes every in_use flag; next_uport starts at BASE. */
    objs->conntrack = calloc(1, sizeof(struct dpu_conntrack));
    if (!objs->conntrack) {
        DOCA_LOG_ERR("Failed to allocate DPU connection tracking table");
        cleanup_objects(objs);
        return;
    }
    objs->conntrack->next_uport = DMESH_UPORT_BASE;

    /* Init deferred completion queue + backpressure state */
    objs->comp_queue.head = 0;
    objs->comp_queue.tail = 0;
    objs->num_deferred_recv = 0;

    /* Not ready until DPA + msgq init completes (below). Gates process_mmap_msg's
     * setup_pod_dma so a fast host's early mmaps don't set up before the msgq. */
    objs->dpu_ready = 0;

    /* Resolve N (DPA EU threads) + K (rings/pod) from env NOW — BEFORE the comch
     * server starts accepting pods. Otherwise a host that connects and sends its
     * DMA_RING exports before init_dpa_objects (step 3) sets k_rings makes
     * process_mmap_msg see k_rings=0 -> kmax=1, so it rejects the pod's 2nd forward
     * ring ("extra DMA_RING k=1 ignored") and that pod never reaches dma_ready (a
     * startup race that a fast host — e.g. one that no longer allocates the datapath
     * consumer — hits deterministically). init_dpa_objects re-checks `<= 0`, so it
     * now skips re-resolving and uses these. Mirrors init_dpa_objects' clamps. */
    {
        const char *env = getenv("DPUMESH_DPA_THREADS");
        int n = (env && *env) ? atoi(env) : 4;
        if (n < 1) n = 1;
        if (n > MAX_DPA_RINGS) n = MAX_DPA_RINGS;
        objs->num_dpa_threads = n;

        const char *kenv = getenv("DPUMESH_RINGS_PER_POD");
        int k = (kenv && *kenv) ? atoi(kenv) : DPUMESH_RINGS_PER_POD_DEFAULT;
        if (k < 1) k = 1;
        if (k > objs->num_dpa_threads) k = objs->num_dpa_threads;
        if (k > MAX_EU_PER_POD) k = MAX_EU_PER_POD;
        objs->k_rings = k;
    }

    /* 1. comch control path server (waits for first connection) */
    result = init_comch_ctrl_path_server("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path server: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 2. comch datapath consumer (for DPA → DPU messages) */
    result = init_comch_datapath_consumer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch datapath consumer: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 3. DPA app init (shared) */
    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init DPA objects: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 4. DPA threads create (one per EU on the shared device; not run yet —
     *    each EU is started on its first assigned pod in setup_pod_dma). EU
     *    thread k is pinned to absolute EU k (partition exposes abs_EUs 0-63). */
    for (int k = 0; k < objs->num_dpa_threads; k++) {
        result = dmesh_doca_dpa_thread_create(objs->dpa_threads[k], k);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create DPA thread EU %d: %s",
                         k, doca_error_get_descr(result));
            cleanup_objects(objs);
            return;
        }
    }

    /* 5. comch DPA message queue (channels bind to consumer_pe) */
    result = init_comch_dpa_msgq(objs, objs->consumer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch DPA msgq: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 6. Publish the DPU as ready, then run per-pod DMA setup for any pod whose
     *    mmaps ALREADY arrived during init (a fast host can connect + export before
     *    the msgq is up; process_mmap_msg stored them but deferred setup_pod_dma via
     *    the dpu_ready gate). Later pods set up inline via process_mmap_msg. */
    objs->dpu_ready = 1;
    {
        int kmax = objs->k_rings > 0 ? objs->k_rings : 1;
        for (int i = 0; i < objs->num_pods; i++) {
            struct pod_state *pod = &objs->pods[i];
            if (!__atomic_load_n(&pod->registered, __ATOMIC_ACQUIRE))
                continue;
            if (pod->ring_mmap_count >= kmax && pod->remote_mmap && !pod->dma_ready) {
                result = setup_pod_dma(objs, pod);
                if (result != DOCA_SUCCESS) {
                    DOCA_LOG_ERR("deferred setup_pod_dma failed for pod %d: %s",
                                 pod->pod_id, doca_error_get_descr(result));
                    continue;
                }
                if (pod->host_rx_mmap) {   /* host RX already exported → wire reverse ring */
                    result = update_rev_ring_host_rx(objs, pod);
                    if (result != DOCA_SUCCESS)
                        DOCA_LOG_WARN("deferred update_rev_ring_host_rx failed for pod %d: %s",
                                      pod->pod_id, doca_error_get_descr(result));
                }
            }
        }
    }

    DOCA_LOG_INFO("DPU worker initialized (event-based), entering main loop");

    if (!event_loop) {
        /* ===== Legacy busy-poll driver (fallback) =====
         * Spin dpu_drain_iteration() continuously (burns a full ARM core) + the
         * 1 ms keepalive WAKE to poke the parked EU, same as the event loop. */
        clock_gettime(CLOCK_MONOTONIC, &last_kick);
        while (true) {
            dpu_drain_iteration(objs);
            clock_gettime(CLOCK_MONOTONIC, &now);
            kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                           (now.tv_nsec - last_kick.tv_nsec) / 1e9;
            if (kick_elapsed >= keepalive_sec) {
                dpu_send_wake(objs);
                last_kick = now;
            }
        }
        return;  /* not reached */
    }

    /* ===== Event-driven driver (default) =====
     * epoll over {consumer_pe fd, ctrl pe fd}. Each pass: drain → send the 1 ms
     * keepalive WAKE on cadence → if idle, arm → re-poll → block on epoll with a
     * 1 ms timeout (so we wake to re-send the keepalive even with no completion).
     * The ARM SLEEPS between ticks (epoll), so idle CPU is a few % (~1 kHz wakeups)
     * — not the full core busy-poll burns. Falls back to busy-poll if setup fails. */
    doca_notification_handle_t cfd = 0, pfd = 0;
    doca_error_t hc = doca_pe_get_notification_handle(objs->consumer_pe, &cfd);
    doca_error_t hp = doca_pe_get_notification_handle(objs->pe, &pfd);
    int ep  = (hc == DOCA_SUCCESS && hp == DOCA_SUCCESS) ? epoll_create1(0) : -1;
    int setup_ok = (ep >= 0);
    if (setup_ok) {
        struct epoll_event ec = { .events = EPOLLIN, .data = { .u32 = 0 } };  /* consumer_pe */
        struct epoll_event ept = { .events = EPOLLIN, .data = { .u32 = 1 } }; /* ctrl pe    */
        if (epoll_ctl(ep, EPOLL_CTL_ADD, (int)cfd, &ec) != 0 ||
            epoll_ctl(ep, EPOLL_CTL_ADD, (int)pfd, &ept) != 0)
            setup_ok = 0;
    }
    if (!setup_ok) {
        DOCA_LOG_WARN("Event-loop setup failed (consumer_pe=%s ctrl_pe=%s ep=%d) → falling back to busy-poll",
                      doca_error_get_name(hc), doca_error_get_name(hp), ep);
        if (ep >= 0) close(ep);
        clock_gettime(CLOCK_MONOTONIC, &last_kick);
        while (true) {
            dpu_drain_iteration(objs);
            clock_gettime(CLOCK_MONOTONIC, &now);
            kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                           (now.tv_nsec - last_kick.tv_nsec) / 1e9;
            if (kick_elapsed >= keepalive_sec) {
                dpu_send_wake(objs);
                last_kick = now;
            }
        }
        return;  /* not reached */
    }

    DOCA_LOG_INFO("DPU worker: EVENT-DRIVEN main loop armed (consumer_pe fd=%d, ctrl_pe fd=%d)",
                  (int)cfd, (int)pfd);

    clock_gettime(CLOCK_MONOTONIC, &last_kick);
    while (true) {
        int progressed = dpu_drain_iteration(objs);   /* ONE pass */

        /* 1 ms DPU→DPA keepalive WAKE, gated to ~1 kHz (checked every pass, not
         * only when sleeping, so a sustained-load stretch still pokes a briefly-
         * parked reverse EU). At idle the 1 ms epoll timeout below brings us back
         * here to re-send it. */
        clock_gettime(CLOCK_MONOTONIC, &now);
        kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                       (now.tv_nsec - last_kick.tv_nsec) / 1e9;
        if (kick_elapsed >= keepalive_sec) {
            dpu_send_wake(objs);
            last_kick = now;
        }

        if (progressed)
            continue;   /* work this pass → poll again (no sleep under load) */

        /* Idle: arm both PEs, then RE-POLL once before blocking (arm→re-check→
         * block) to close the drain→arm race — a completion landing between the
         * last drain and the arm must not be stranded. */
        (void)doca_pe_request_notification(objs->consumer_pe);
        (void)doca_pe_request_notification(objs->pe);

        if (dpu_drain_iteration(objs)) {
            /* Work arrived during/just before arm — handle it, don't sleep. */
            (void)doca_pe_clear_notification(objs->consumer_pe, cfd);
            (void)doca_pe_clear_notification(objs->pe, pfd);
            continue;
        }

        /* SLEEP on epoll with a 1 ms timeout: wake on a real completion (FWD_DONE/
         * REV_DONE/ctrl) OR after ≤1 ms to re-send the keepalive WAKE. The ARM
         * sleeps between ticks → idle CPU a few % (~1 kHz wakeups), NOT a busy-poll
         * full core. The timeout also backstops any missed PE notification. */
        struct epoll_event evs[2];
        (void)epoll_wait(ep, evs, 2, 1);

        /* Clear PE notifications so they can be re-armed (SELECTIVE contract). */
        (void)doca_pe_clear_notification(objs->consumer_pe, cfd);
        (void)doca_pe_clear_notification(objs->pe, pfd);
    }
}
