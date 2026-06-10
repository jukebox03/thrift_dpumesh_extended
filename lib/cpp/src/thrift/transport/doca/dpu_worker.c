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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

DOCA_LOG_REGISTER(DPU_WORKER);

/* ====== TX_ACK send helper ====== */

/* Try to send TX_ACK; on EAGAIN park to the deferred queue. No-op when src_pod
 * is gone (host's 2-second collision-wait reclaim is the safety net). */
static void
send_or_defer_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint32_t req_id, int32_t dst_pod_id)
{
    if (!src_pod || !src_pod->connection)
        return;

    doca_error_t r = server_send_tx_ack_to(objs, src_pod->connection, req_id, dst_pod_id);
    if (r == DOCA_SUCCESS)
        return;

    if (r == DOCA_ERROR_AGAIN) {
        if (objs->num_deferred_tx_acks < MAX_DEFERRED_TX_ACK) {
            int n = objs->num_deferred_tx_acks++;
            objs->deferred_tx_acks[n].conn        = src_pod->connection;
            objs->deferred_tx_acks[n].req_id      = req_id;
            objs->deferred_tx_acks[n].dst_pod_id  = dst_pod_id;
        } else {
            DOCA_LOG_ERR("deferred TX_ACK queue full — dropping req_id=%u (pod %d). "
                         "Host slot will reclaim at 2s.",
                         req_id, src_pod->pod_id);
        }
        return;
    }

    DOCA_LOG_WARN("TX_ACK failed for req_id=%u to pod %d: %s",
                  req_id, src_pod->pod_id, doca_error_get_descr(r));
}

/* ====== Batched TX_ACK ====== */

/* Flush a pod's accumulated TX_ACK batch as one message. On AGAIN the batch is
 * retained (retried by the next flush, including the 1 kHz tail flush). */
static void
flush_txack_batch(struct objects *objs, struct pod_state *pod)
{
    if (!pod || pod->txack_batch_n == 0)
        return;
    if (!pod->connection) { pod->txack_batch_n = 0; return; }
    doca_error_t r = server_send_batch_tx_ack_to(objs, pod->connection,
                                                 pod->txack_batch, pod->txack_batch_n);
    if (r != DOCA_ERROR_AGAIN)
        pod->txack_batch_n = 0;   /* sent (or hard error → host 2s reclaim) */
}

/* Accumulate one TX_ACK into the src pod's batch; flush when full. Falls back to
 * the single-send path when the pod is gone or the batch is already full and a
 * prior flush is still pending (AGAIN). */
static void
batch_or_send_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint32_t req_id, int32_t dst_pod_id)
{
    if (!src_pod || !src_pod->connection) {
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return;
    }
    if (src_pod->txack_batch_n >= BATCH_TXACK_MAX) {
        /* Batch full and not yet drained (send pool busy) — single-send this one
         * so no ack is lost; the full batch is retried by the tail flush. */
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return;
    }
    src_pod->txack_batch[src_pod->txack_batch_n++] = req_id;
    (void)dst_pod_id;
    if (src_pod->txack_batch_n >= BATCH_TXACK_MAX)
        flush_txack_batch(objs, src_pod);
}

/* ====== Batched REV_DONE (mirror of batched TX_ACK) ======
 * The single host PE thread reaping one REV_DONE comch msg per response is the
 * 2-pod cap; coalescing K responses into one msg cuts the PE reap rate K-fold. */

/* Flush a pod's accumulated REV_DONE batch as one message. On AGAIN the batch is
 * retained (retried by the next flush, including the proc==0 + 1 kHz tail flush). */
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
    pod->rev_done_batch_n = 0;   /* sent (or hard error → host 2s reclaim) */
}

/* Accumulate one REV_DONE into the target pod's batch; flush when full. Returns
 * 1 = accumulated, 0 = batch full AND a prior flush is still AGAIN (caller must
 * retain the comp entry → ingest backpressure, exactly like the un-batched send). */
static int
batch_or_send_rev_done(struct objects *objs, struct pod_state *target_pod,
                       uint32_t req_id, int32_t src_pod_id, int32_t dst_pod_id,
                       uint32_t pos, uint32_t length, int8_t flags)
{
    if (target_pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        flush_rev_done_batch(objs, target_pod);          /* try to make room */
    if (target_pod->rev_done_batch_n >= BATCH_REVDONE_MAX)
        return 0;                                        /* still full (send pool busy) */
    struct dmesh_rev_done_entry *e = &target_pod->rev_done_batch[target_pod->rev_done_batch_n++];
    e->flags = flags;
    e->src_pod_id = (int8_t)src_pod_id;
    e->dst_pod_id = (int8_t)dst_pod_id;
    e->_pad = 0;
    e->pos = pos;
    e->length = length;
    e->req_id = req_id;
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
dpu_enqueue_reverse_dma(struct objects *objs, struct pod_state *src_pod,
                        struct pod_state *dst_pod, int ring_k,
                        uint32_t req_id, int32_t dst_pod_id, int32_t src_pod_id,
                        int8_t flags, uint32_t src_buf_offset, uint32_t body_len)
{
    (void)objs;
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
    dma->idx = req_id;
    dma->dst_pod_id = dst_pod_id;
    dma->src_pod_id = src_pod_id;
    dma->flags = flags;

    __sync_synchronize();
    dma->valid = 1;

    return DOCA_SUCCESS;
}

/* ====== L7 routing seam (MOCK) ======
 *
 * dpu_route() is the single point where the DPU decides the destination pod for
 * a forward completion. The mock returns the host-provided desc.dst_pod_id
 * verbatim (identity routing); a routing table or body parsing plugs in here.
 */
static inline int32_t
dpu_route(struct objects *objs, const dpu_comp_entry_t *entry)
{
    (void)objs;
    /* MOCK: return the host-provided routing input unchanged. */
    return entry->dst_pod_id;
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
    int32_t src_pod_id = entry->src_pod_id;
    int32_t dst_pod_id = dpu_route(objs, entry);   /* L7 seam (mock = passthrough) */
    uint32_t req_id = entry->req_id;
    uint32_t payload_len = entry->length;

    /* Resolve src_pod for TX_ACK on error paths. */
    struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);

    /* The forward DMA landed in pods[entry->pod_idx]->dma_buffer at
     * entry->buf_offset. That same offset is the source for reverse DMA. */
    struct pod_state *fwd_buf_pod = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods)
        fwd_buf_pod = &objs->pods[entry->pod_idx];

    /* pod_data_ready ACQUIRE-loads dma_ready so the dma_buffer/handle/tx_ring
     * reads below see the RELEASE-published setup fields. */
    if (!fwd_buf_pod || !pod_data_ready(fwd_buf_pod) || !fwd_buf_pod->dma_buffer ||
        fwd_buf_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("comp_queue: invalid pod_idx=%d for req_id=%u", entry->pod_idx, req_id);
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return -1;
    }

    int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);
    struct pod_state *target_pod = echo_mode ? src_pod
                                             : find_pod_by_id(objs, dst_pod_id);

    if (!target_pod || !pod_data_ready(target_pod) || !target_pod->tx_rings[0]) {
        DOCA_LOG_ERR("DMA completed: target_pod=%d not found or TX ring not ready",
                     echo_mode ? src_pod_id : dst_pod_id);
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return -1;
    }

    /* EU-sharding: round-robin the reverse DMA across target_pod's K rings so it
     * is issued by K different EUs. Single ARM writer → rev_rr needs no lock. */
    int kr = target_pod->k_rings > 0 ? target_pod->k_rings : 1;
    int ring_k = (int)(target_pod->rev_rr++ % (uint32_t)kr);

    int8_t fwd_flags = echo_mode ? OP_RESPONSE
                                 : ((entry->flags & OP_RESPONSE) | CASE_INGRESS);
    doca_error_t fwd_result = dpu_enqueue_reverse_dma(
        objs, fwd_buf_pod, target_pod, ring_k, req_id, dst_pod_id, src_pod_id, fwd_flags,
        entry->buf_offset, payload_len);

    if (fwd_result == DOCA_ERROR_AGAIN) {
        return 0;  /* TX descriptor ring full — preserve entry, retry next iter */
    }
    if (fwd_result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("%s failed for req_id=%u dst_pod=%d: %s",
                     echo_mode ? "Echo" : "Forward",
                     req_id, dst_pod_id, doca_error_get_descr(fwd_result));
        /* Reverse will not fire — release src host's TX slot now so the
         * caller doesn't stall on 2s reclaim. */
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return -1;
    }

    /* Success: TX_ACK will fire from process_rev_notify_entry on reverse
     * completion. The src dma_buffer slot stays held until then. */
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
        doca_error_t rc = server_send_tx_ack_to(objs, d->conn, d->req_id,
                                                 d->dst_pod_id);
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
        /* Hard error — log and drop (host's 2s reclaim is the safety net
         * for these once-in-a-blue-moon hard failures). */
        DOCA_LOG_WARN("deferred TX_ACK fatal for req_id=%u: %s",
                      d->req_id, doca_error_get_descr(rc));
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
    /* Determine destination pod — for echo (dst=-1 or dst==src), send to source */
    int echo_mode = (entry->dst_pod_id == -1 || entry->dst_pod_id == entry->src_pod_id);
    int32_t target_id = echo_mode ? entry->src_pod_id : entry->dst_pod_id;
    struct pod_state *target_pod = find_pod_by_id(objs, target_id);

    if (!target_pod || !target_pod->connection) {
        DOCA_LOG_ERR("REV_NOTIFY: target pod %d not found or no connection", target_id);
        /* Still try to release the src's TX slot — reverse DMA already
         * read the data out of src's dma_buffer, so the slot is logically
         * free regardless of whether the dst notification lands. */
        struct pod_state *src_pod = find_pod_by_id(objs, entry->src_pod_id);
        send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id);
        return -1;
    }

    /* Deliver REV_DONE to the destination host. Cross-pod (the common case incl.
     * the bench): BATCH it so the host PE reaps 1 comch msg per K responses — the
     * 2-pod cap. Echo (target == src below, same conn): send UN-batched so REV_DONE
     * strictly precedes the same-conn TX_ACK (load-bearing ordering). */
    if (echo_mode) {
        struct dmesh_dma_completion_msg comp_msg;
        comp_msg.type = DMESH_MSG_REV_DONE;
        comp_msg.pos = entry->buf_offset;
        comp_msg.length = entry->length;
        comp_msg.req_id = entry->req_id;
        comp_msg.src_pod_id = entry->src_pod_id;
        comp_msg.dst_pod_id = entry->dst_pod_id;
        comp_msg.flags = entry->flags;
        doca_error_t result = server_send_msg_to_conn(objs, target_pod->connection,
                                                       (const char *)&comp_msg, sizeof(comp_msg));
        if (result == DOCA_ERROR_AGAIN) {
            return 0;  /* retain entry → ingest backpressure */
        }
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("REV_NOTIFY: send DMA_COMPLETION to pod %d failed: %s",
                         target_id, doca_error_get_descr(result));
            struct pod_state *src_pod = find_pod_by_id(objs, entry->src_pod_id);
            send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id);
            return -1;
        }
    } else {
        if (batch_or_send_rev_done(objs, target_pod, entry->req_id, entry->src_pod_id,
                                   entry->dst_pod_id, entry->buf_offset, entry->length,
                                   entry->flags) == 0)
            return 0;  /* batch full + send pool busy → retain entry (backpressure) */
    }

    /* DMA_COMPLETION sent; now release src's TX slot — the only place it is
     * released on the success path. For echo src==dst this goes to the same pod
     * as the DMA_COMPLETION above; comch handles the second send (or defers). */
    struct pod_state *src_pod = echo_mode ? target_pod
                                          : find_pod_by_id(objs, entry->src_pod_id);
    /* Request forwards skip TX_ACK — the client frees its TX slot on response
     * arrival (REV_DONE). Response forwards (OP_RESPONSE) get no reply, so they
     * still ACK their src to free its slot; echo (src==dst) keeps the ACK too. */
    if (echo_mode || (entry->flags & OP_RESPONSE))
        batch_or_send_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id);

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

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec now, last_kick;
    double kick_elapsed = 0.0;

    /* Keepalive interval (DPU→DPA WAKE). A yielded EU re-checks its reverse
     * tx_ring only when woken by a msgq message; the ARM's reverse-desc post is
     * a silent memory write, so this interval bounds the worst-case
     * forward→reverse handoff latency when the EU out-runs the ARM and yields. */
    const double keepalive_sec = 0.001;

    DOCA_LOG_INFO("Starting DPU worker");

    /* Init pods table. See object.h pods[] concurrency model — lock-free
     * with atomic publication on `registered`; no mutex needed. */
    memset(objs->pods, 0, sizeof(objs->pods));
    objs->num_pods = 0;

    /* find_pod_by_id's O(1) pod_id->slot map starts empty (-1 = no live pod).
     * Done before any PE/thread starts, so a plain store is race-free here. */
    for (int i = 0; i < POD_ID_SPACE; i++)
        objs->pod_id_to_slot[i] = -1;

    /* Init deferred completion queue + backpressure state */
    objs->comp_queue.head = 0;
    objs->comp_queue.tail = 0;
    objs->num_deferred_recv = 0;

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

    /* 6. No more blocking waits — per-pod DMA setup is event-driven.
     *    When a pod's ring_mmap + remote_mmap arrive (via process_mmap_msg),
     *    setup_pod_dma() is called automatically, which sets up buf_arr,
     *    local DMA buffer, DPA ring info, and starts DPA thread on first pod. */

    DOCA_LOG_INFO("DPU worker initialized (event-based), entering main loop");

    /* Main loop: poll consumer PE + ctrl path PE + per-pod producer PE */
    clock_gettime(CLOCK_MONOTONIC, &last_kick);
    while (true) {
        doca_pe_progress(objs->consumer_pe);
        doca_pe_progress(objs->pe);  /* handle new connections, REGISTER, TX_DATA */

        /* Retry any TX_ACKs that were deferred when the comch send pool was
         * full. Done right after pe_progress so the just-released send-pool
         * slots are available. */
        drain_deferred_tx_acks(objs);

        /* Drain deferred completion queue (reverse DMA enqueue). 128 entries per
         * batch — safe because consumer_pe is progressed inside the loop, keeping
         * DPA recv tasks recycled. */
        int proc = process_completion_queue(objs, 128);
        /* Idle (no completions this iter) → flush partial REV_DONE batches so
         * low-load latency is not held by coalescing. Under steady load proc>0
         * keeps batches accumulating to BATCH_REVDONE_MAX (full coalescing). */
        if (proc == 0)
            for (int i = 0; i < objs->num_pods; i++)
                flush_rev_done_batch(objs, &objs->pods[i]);

        /* Backpressure release: resubmit deferred recv tasks when the ingest
         * hand-off (comp_queue) drains below BP_LOW. This resumes DPA→DPU
         * message flow. */
        if (objs->num_deferred_recv > 0 &&
            ingest_usage(objs) < COMP_QUEUE_BP_LOW) {
            int remaining = 0;
            int resubmitted = 0;
            int original = objs->num_deferred_recv;
            for (int i = 0; i < original; i++) {
                struct doca_task *t = objs->deferred_recv[i];
                /* These are per-EU DPA→DPU msgq recv tasks — a FIXED per-channel
                 * allocation (CC_DPA_MAX_MSG_NUM each), NOT the standalone
                 * consumer's recv_tasks_in_flight pool. Resubmit merely recycles
                 * an existing task, so it must not gate on that pool.
                 * Backpressure is governed entirely by ingest_usage (deferred at
                 * BP_HIGH, resumed here below BP_LOW). */
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

        /* Drain any consumer_retry tasks that were stashed by the consumer
         * completion callback when capacity was full. */
        objects_drain_consumer_retry(objs);

        clock_gettime(CLOCK_MONOTONIC, &now);

        /* Keepalive trigger: bounds the idle→active wake-up latency for DPA to
         * the keepalive interval when no other event arrives on its
         * consumer_comp. During a busy burst DPA keeps spinning so this does
         * nothing; during idle (DPA reschedules) the next kick pulls it back to
         * drain. Fire-and-forget: a missed kick is recovered by the next one. */
        kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                       (now.tv_nsec - last_kick.tv_nsec) / 1e9;
        if (kick_elapsed >= keepalive_sec) {
            if (objs->dpa_thread_running_any) {
                struct comch_msg trigger;
                memset(&trigger, 0, sizeof(trigger));
                trigger.type = DPA_MSG_WAKE;
                /* Each running EU has its own channel and reschedules
                 * independently when idle, so every started EU needs its own
                 * keepalive to be woken within ~1 ms. */
                for (int k = 0; k < objs->num_dpa_threads; k++) {
                    if (objs->dpa_thread_running[k])
                        (void)dmesh_doca_dpa_msgq_send_try(&objs->dpa_comches[k]->send,
                                                            &trigger, sizeof(trigger));
                }
            }
            /* Tail flush partial TX_ACK + REV_DONE batches. */
            for (int i = 0; i < objs->num_pods; i++) {
                flush_txack_batch(objs, &objs->pods[i]);
                flush_rev_done_batch(objs, &objs->pods[i]);
            }
            last_kick = now;
        }
    }
}
