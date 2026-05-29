#include "dpu_worker.h"

#include "comch_server.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "dpa.h"
#include "dpa_common.h"
#include "comch_msgq.h"
#include "buffer.h"
#include "ring.h"
#include "dma.h"
#include "../dpumesh.h"

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_dpa.h>

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stddef.h>

DOCA_LOG_REGISTER(DPU_WORKER);

/* ====== TX_ACK send helper ====== */

/*
 * Try to send TX_ACK; on EAGAIN park to deferred queue. Used by:
 *   (a) process_forward_entry error paths where reverse DMA will not fire,
 *   (b) process_rev_notify_entry after reverse-DMA completes (normal path).
 * No-op when src_pod is gone — host's 2-second collision-wait reclaim is
 * the safety net.
 */
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

/* ====== Reverse DMA Enqueue (DPU→CPU) ====== */

/*
 * Enqueue a reverse-DMA descriptor for in-place forwarding.
 *
 * Source = src_pod->dma_buffer at src_buf_offset (where forward DMA
 * landed). NO DPU-side staging memcpy: dst_pod's old tx_buffer is bypassed
 * entirely. Each descriptor carries the source mmap handle and full
 * virtual address so DPA reverse handler can dispatch to the right pod's
 * buffer per request.
 *
 * Per-request metadata (req_id, src_pod_id, dst_pod_id, flags) is carried
 * via dma_desc on-DPU and propagated to the receiving host through
 * comch_dma_comp_msg → dmesh_dma_completion_msg.
 *
 * Slot lifecycle: src's RX slot is held from forward-completion through
 * reverse-completion (full RTT). TX_ACK to src is therefore deferred to
 * process_rev_notify_entry, NOT sent here. End-node host TX-slot
 * accounting (num_slots × slot_size ≤ DPU_BUFFER_SIZE) is what bounds
 * the unified-buffer occupancy.
 *
 * Returns DOCA_SUCCESS or DOCA_ERROR_AGAIN (TX descriptor ring full).
 */
static doca_error_t
dpu_enqueue_reverse_dma(struct objects *objs, struct pod_state *src_pod,
                        struct pod_state *dst_pod,
                        const sw_descriptor_t *desc, uint32_t src_buf_offset,
                        uint32_t body_len)
{
    if (!dst_pod->tx_ring) {
        DOCA_LOG_ERR("dpu_enqueue_reverse_dma: pod %d reverse ring not ready", dst_pod->pod_id);
        return DOCA_ERROR_NOT_CONNECTED;
    }
    if (!src_pod->dma_buffer || src_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("dpu_enqueue_reverse_dma: src pod %d dma_buffer/handle not ready",
                     src_pod->pod_id);
        return DOCA_ERROR_NOT_CONNECTED;
    }

    /* Post descriptor to TX ring */
    struct dma_desc *dma = get_next_dma_desc(dst_pod->tx_ring);
    if (!dma) {
        DOCA_LOG_WARN("dpu_enqueue_reverse_dma: TX ring full for pod %d", dst_pod->pod_id);
        return DOCA_ERROR_AGAIN;
    }

    /* Fill descriptor: DPA reverse handler reads the source from
     * (desc->mmap, desc->addr). desc->addr is full virtual address inside
     * src_pod's local_mmap range (DOCA dma_copy expects raw VA, see
     * forward path's ring->dpu_addr usage in dpa_kernel.c). */
    dma->mmap = src_pod->local_mmap_dpa_handle;
    dma->addr = (uint64_t)src_pod->dma_buffer + src_buf_offset;
    dma->size = body_len;
    dma->idx = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->src_pod_id = desc->src_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    return DOCA_SUCCESS;
}

/* ====== Deferred Completion Queue Drain ====== */

/*
 * Process a COMP_ENTRY_FORWARD entry: enqueue reverse DMA for in-place
 * forwarding. NO TX_ACK on the success path — that fires from
 * process_rev_notify_entry once reverse DMA completes (slot must remain
 * held in src's dma_buffer until DPA finishes reading it). Error paths
 * (no src buffer, no target pod, hard reverse-enqueue failure) DO send
 * TX_ACK here, since reverse will not fire and the host would otherwise
 * sit on the 2-second collision-wait reclaim cliff.
 *
 * Returns 1 if processed, 0 if should retry (TX desc ring full), -1 on error.
 */
static int
process_forward_entry(struct objects *objs, dpu_comp_entry_t *entry)
{
    int32_t src_pod_id = entry->src_pod_id;
    int32_t dst_pod_id = entry->dst_pod_id;
    uint32_t req_id = entry->req_id;
    uint32_t payload_len = entry->length;

    /* Resolve src_pod for TX_ACK on error paths. */
    struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);

    /* The forward DMA landed in pods[entry->pod_idx]->dma_buffer at
     * entry->buf_offset. That same offset is the source for reverse DMA. */
    struct pod_state *fwd_buf_pod = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods)
        fwd_buf_pod = &objs->pods[entry->pod_idx];

    if (!fwd_buf_pod || !fwd_buf_pod->dma_buffer ||
        fwd_buf_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("comp_queue: invalid pod_idx=%d for req_id=%u", entry->pod_idx, req_id);
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return -1;
    }

    int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);
    struct pod_state *target_pod = echo_mode ? src_pod
                                             : find_pod_by_id(objs, dst_pod_id);

    if (!target_pod || !target_pod->tx_ring) {
        DOCA_LOG_ERR("DMA completed: target_pod=%d not found or TX ring not ready",
                     echo_mode ? src_pod_id : dst_pod_id);
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
        return -1;
    }

    sw_descriptor_t fwd_desc;
    memset(&fwd_desc, 0, sizeof(fwd_desc));
    fwd_desc.header_buf_slot = -1;
    fwd_desc.body_buf_slot = -1;
    fwd_desc.body_len = payload_len;
    fwd_desc.req_id = req_id;
    fwd_desc.src_pod_id = src_pod_id;
    fwd_desc.dst_pod_id = dst_pod_id;
    fwd_desc.flags = echo_mode ? OP_RESPONSE
                               : ((entry->flags & OP_RESPONSE) | CASE_INGRESS);
    fwd_desc.valid = 1;

    doca_error_t fwd_result = dpu_enqueue_reverse_dma(
        objs, fwd_buf_pod, target_pod, &fwd_desc, entry->buf_offset, payload_len);

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
 * Send DMA_COMPLETION to the destination Host pod, then TX_ACK to the
 * src pod (under in-place forwarding the src's dma_buffer slot was held
 * through the whole RTT — releasing it earlier would let host overwrite
 * the slot before DPA's reverse DMA finished reading it).
 *
 * If DMA_COMPLETION returns AGAIN we retry the whole entry next iter and
 * defer TX_ACK along with it (returning 0 leaves the comp_queue entry in
 * place). TX_ACK has its own deferred queue for partial-progress cases
 * where DMA_COMPLETION succeeded but TX_ACK hits an EAGAIN.
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

    /* Send DMA_COMPLETION to destination Host pod via comch control path */
    struct dmesh_dma_completion_msg comp_msg;
    comp_msg.type = DMESH_MSG_DMA_COMPLETION;
    comp_msg.pos = entry->buf_offset;
    comp_msg.length = entry->length;
    comp_msg.req_id = entry->req_id;
    comp_msg.src_pod_id = entry->src_pod_id;
    comp_msg.dst_pod_id = entry->dst_pod_id;
    comp_msg.flags = entry->flags;

    doca_error_t result = server_send_msg_to_conn(objs, target_pod->connection,
                                                   (const char *)&comp_msg,
                                                   sizeof(comp_msg));
    if (result == DOCA_ERROR_AGAIN) {
        return 0;  /* retry next iteration — TX_ACK deferred too */
    }
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("REV_NOTIFY: send DMA_COMPLETION to pod %d failed: %s",
                     target_id, doca_error_get_descr(result));
        /* Hard error on dst notify; still release src's TX slot — its data
         * has been DMA'd out and the slot is logically free. */
        struct pod_state *src_pod = find_pod_by_id(objs, entry->src_pod_id);
        send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id);
        return -1;
    }

    /* DMA_COMPLETION sent. Now release src's TX slot (under in-place
     * forwarding this is the only place it gets released on the success
     * path — process_forward_entry no longer ACKs on success). For echo
     * src==dst this goes to the same pod as the DMA_COMPLETION above;
     * comch handles the second send (or defers via TX_ACK queue). */
    struct pod_state *src_pod = echo_mode ? target_pod
                                          : find_pod_by_id(objs, entry->src_pod_id);
    send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id);

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

    /* Per-batch (not per-entry) PE progress. Rationale:
     *   Send pool (CC_SEND_TASK_NUM=8192) and consumer recv pool
     *   (CC_DATA_PATH_TASK_NUM=8192) are both ≫ max_batch=128. A batch
     *   consumes at most 128 entries × 2 sends (DMA_COMPLETION + TX_ACK)
     *   = 256 send slots, well inside the pool. Likewise DPA cannot
     *   exhaust the 8192 consumer recv pool within one batch.
     *
     *   The earlier per-entry progress was a leftover from when these
     *   pools were 1024 slots — at that depth a 128-entry batch could
     *   plausibly drain the pool mid-batch. With 8192-slot pools the
     *   cost (256 extra doca_pe_progress calls per main-loop iter,
     *   dominating the flame in §6.5) outweighs the safety margin. */
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
    struct timespec last, now, last_kick;
    double elapsed = 0.0;
    double kick_elapsed = 0.0;

    DOCA_LOG_INFO("Starting DPU worker");

    /* Init pods table. See object.h pods[] concurrency model — lock-free
     * with atomic publication on `registered`; no mutex needed. */
    memset(objs->pods, 0, sizeof(objs->pods));
    objs->num_pods = 0;

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

    /* 4. DPA thread create (shared, not run yet — started on first pod) */
    result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DPA thread: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 5. comch DPA message queue (shared) */
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
    clock_gettime(CLOCK_MONOTONIC, &last);
    last_kick = last;
    while (true) {
        doca_pe_progress(objs->consumer_pe);
        doca_pe_progress(objs->pe);  /* handle new connections, REGISTER, TX_DATA */

        /* Retry any TX_ACKs that were deferred when the comch send pool was
         * full. Done right after pe_progress so the just-released send-pool
         * slots are available. */
        drain_deferred_tx_acks(objs);

        /* Drain deferred completion queue (reverse DMA enqueue).
         * 128 entries per batch — safe because consumer_pe is progressed
         * inside the loop, keeping DPA recv tasks recycled. */
        process_completion_queue(objs, 128);

        /* Backpressure release: resubmit deferred recv tasks when queue
         * drains below BP_LOW. This resumes DPA→DPU message flow.
         * Gate each submit on recv task pool capacity; preserve un-submitted
         * tasks by shifting them to the front instead of zeroing count. */
        if (objs->num_deferred_recv > 0 &&
            comp_queue_usage(&objs->comp_queue) < COMP_QUEUE_BP_LOW) {
            int remaining = 0;
            int resubmitted = 0;
            int original = objs->num_deferred_recv;
            for (int i = 0; i < original; i++) {
                struct doca_task *t = objs->deferred_recv[i];
                /* _exact: main loop is the only resubmitter — no race */
                if (!doca_pool_try_acquire_exact(&objs->recv_tasks_in_flight, objs->recv_tasks_max)) {
                    objs->deferred_recv[remaining++] = t;
                    continue;
                }
                doca_error_t rs = doca_task_submit(t);
                if (rs == DOCA_SUCCESS) {
                    resubmitted++;
                } else {
                    doca_pool_release(&objs->recv_tasks_in_flight);
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

        /* 1 kHz keepalive trigger: bounds the idle→active wake-up latency
         * for DPA to ~1 ms when no other event arrives on its consumer_comp.
         * During a busy burst, drain_all_rings keeps DPA spinning (chunks > 0
         * never reschedules), so the keepalive does nothing.  During idle
         * (chunks == 0 → DPA reschedules), the next keepalive fires within
         * 1 ms and pulls DPA back to drain whatever the host posted. Send
         * fire-and-forget: a missed kick is recovered by the next one. The
         * cost is 1000 × 68 B = 68 KB/s on the DPU→DPA msgq — negligible. */
        kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                       (now.tv_nsec - last_kick.tv_nsec) / 1e9;
        if (kick_elapsed >= 0.001) {
            if (objs->dpa_thread_running && objs->dpa_comch) {
                struct comch_msg trigger;
                memset(&trigger, 0, sizeof(trigger));
                trigger.type = COMCH_MSG_TYPE_TRIGGER;
                (void)dmesh_doca_dpa_msgq_send_try(&objs->dpa_comch->send,
                                                    &trigger, sizeof(trigger));
            }
            last_kick = now;
        }

        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            uint32_t cq_depth = comp_queue_usage(&objs->comp_queue);
            /* Only emit the periodic stat line when there is something to report.
             * On an idle DPU this fired every second, growing the log file
             * indefinitely with no useful information. */
            if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0 ||
                cq_depth > 0 || objs->num_deferred_recv > 0) {
                DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s, pods: %d, cq_depth: %u, deferred: %d",
                              elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt, objs->num_pods,
                              cq_depth, objs->num_deferred_recv);
            }

            /* === Diagnostic: read DPA polling counters via d2h_memcpy ===
             * Computes polls/dma_copy ratio to validate whether dpumesh's
             * 4-ring pattern actually amortizes polling (~1 read/dma_copy)
             * or pays full 4 reads per dma_copy. */
            if (objs->dpa_thread_running && objs->dpa_thread &&
                objs->dpa_thread->arg && objs->sent_msg_cnt + objs->recv_msg_cnt > 1000) {
                static uint64_t prev_iters = 0, prev_polls = 0, prev_copies = 0;
                struct { uint64_t iters; uint64_t polls; uint64_t copies; } s;
                doca_dpa_dev_uintptr_t addr = objs->dpa_thread->arg +
                    offsetof(struct dpa_thread_arg, stat_inner_iters);
                doca_error_t rc = doca_dpa_d2h_memcpy(objs->dpa_thread->dpa,
                                                     &s, addr, sizeof(s));
                if (rc == DOCA_SUCCESS) {
                    uint64_t d_iters = s.iters - prev_iters;
                    uint64_t d_polls = s.polls - prev_polls;
                    uint64_t d_copies = s.copies - prev_copies;
                    double ratio = d_copies > 0
                        ? (double)d_polls / (double)d_copies : 0.0;
                    DOCA_LOG_INFO("DPA stat: iters=%lu polls=%lu copies=%lu poll/copy=%.3f",
                                  d_iters, d_polls, d_copies, ratio);
                    prev_iters  = s.iters;
                    prev_polls  = s.polls;
                    prev_copies = s.copies;
                }
            }

            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;

            /* No keepalive: DPA wake-up is driven entirely by per-request
             * triggers (host WAKE_DPA on dpumesh_enqueue, DPU TRIGGER on
             * dpu_enqueue_reverse_dma). The 1Hz tick had two roles —
             * timer reset against the 12 s max kernel runtime, and idle
             * fallback wake — neither needed once DPA reschedules every
             * iteration and every desc post is paired with an explicit
             * trigger. */
        }
    }

}
