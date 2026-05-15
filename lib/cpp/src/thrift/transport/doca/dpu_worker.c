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
#include "mesh.h"
#include "../dpumesh.h"

/* DPA size alignment — must match dpa_kernel.c ALIGN_UP_128 macro. */
#ifndef DMA_COPY_SIZE_ALIGN
#define DMA_COPY_SIZE_ALIGN 128
#endif
#define DPU_ALIGN_UP_128(x) (((x) + (DMA_COPY_SIZE_ALIGN - 1)) & ~(uint32_t)(DMA_COPY_SIZE_ALIGN - 1))

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

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
                     uint32_t req_id, int32_t dst_pod_id, uint8_t pool_type)
{
    if (!src_pod || !src_pod->connection)
        return;

    doca_error_t r = server_send_tx_ack_to(objs, src_pod->connection, req_id,
                                           dst_pod_id, pool_type);
    if (r == DOCA_SUCCESS)
        return;

    if (r == DOCA_ERROR_AGAIN) {
        if (objs->num_deferred_tx_acks < MAX_DEFERRED_TX_ACK) {
            int n = objs->num_deferred_tx_acks++;
            objs->deferred_tx_acks[n].conn        = src_pod->connection;
            objs->deferred_tx_acks[n].req_id      = req_id;
            objs->deferred_tx_acks[n].dst_pod_id  = dst_pod_id;
            objs->deferred_tx_acks[n].pool_type   = pool_type;
        }
        /* Silently drop if deferred queue is also full — host's 2s reclaim is
         * the safety net. Logging per-request here floods the DPU log and
         * kills dpumesh_dpu (see plan §9.12 / memory rule). */
        return;
    }
    /* Other hard errors: silent. Host-side stat counters detect persistent
     * loss; per-request log on DPU floods. */
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
    /* Hot path under repeated misconfig fires per request; suppress logs
     * (host-side timeout / stat counters will surface persistent issues). */
    if (!dst_pod->tx_ring) return DOCA_ERROR_NOT_CONNECTED;
    if (!src_pod->dma_buffer || src_pod->local_mmap_dpa_handle == 0)
        return DOCA_ERROR_NOT_CONNECTED;

    /* Post descriptor to TX ring */
    struct dma_desc *dma = get_next_dma_desc(dst_pod->tx_ring);
    if (!dma) {
        /* Hot path under backpressure: ring is full this iteration. Caller
         * retries next iter (EAGAIN). NO per-request log on DPU — at high
         * RPS this fires every chunk and floods /tmp on the DPU. */
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
    /* Phase 3: OP_HDR_BATCH overrides destination to dst's host_hdr_rx_buffer.
     * Cursor managed per-dst (no credit return; Phase 4 will add a grant
     * scheme). Body forwards (OP_CHUNK and legacy) keep dst override unset
     * so DPA falls back to ring->host_mmap (= dst's body RX). */
    if ((desc->flags & OP_HDR_BATCH) && dst_pod->host_hdr_rx_addr) {
        /* Phase 3: cursor + absolute VA. DPA picks dst_mmap from
         * ring->host_hdr_rx_mmap (resolved against DPU device), so we leave
         * dma->dst_mmap = 0. dst_addr is the host VA — the DPA-side mmap
         * exports the same VA range. dst_pos echoes the offset to host. */
        uint32_t aligned = DPU_ALIGN_UP_128(body_len);
        if (dst_pod->hdr_rx_cursor + aligned > dst_pod->host_hdr_rx_buf_size)
            dst_pod->hdr_rx_cursor = 0;
        dma->dst_mmap = 0;
        dma->dst_pos  = dst_pod->hdr_rx_cursor;
        dma->dst_addr = (uint64_t)dst_pod->host_hdr_rx_addr + dst_pod->hdr_rx_cursor;
        dst_pod->hdr_rx_cursor += aligned;
    } else {
        dma->dst_mmap = 0;
        dma->dst_pos  = 0;
        dma->dst_addr = 0;
    }

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

    /* Phase 4: CASE_DIRECT — DPA already wrote into dst host's rx_dma_buffer
     * bypassing DPU staging. No reverse DMA to enqueue. Just notify dst
     * (rx_data_hook drains) and ack src. EAGAIN on DMA_COMPLETION leaves
     * the comp_queue entry in place (return 0) so we retry next iter —
     * this matches the pattern used by REV_NOTIFY and prevents tail-latency
     * cliffs at saturation. */
    if (entry->flags & CASE_DIRECT) {
        struct pod_state *dst_pod = find_pod_by_id(objs, dst_pod_id);
        if (dst_pod && dst_pod->connection) {
            struct dmesh_dma_completion_msg comp;
            comp.type = DMESH_MSG_DMA_COMPLETION;
            comp.pos = entry->buf_offset;
            comp.length = entry->length;
            comp.req_id = entry->req_id;
            comp.src_pod_id = entry->src_pod_id;
            comp.dst_pod_id = entry->dst_pod_id;
            comp.flags = entry->flags;
            doca_error_t r = server_send_msg_to_conn(objs, dst_pod->connection,
                                                      (const char *)&comp, sizeof(comp));
            if (r == DOCA_ERROR_AGAIN) {
                return 0;  /* retry next iteration */
            }
            /* Other errors: silent. Host-side timeout is the backstop. */
        }
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id, POOL_HOST_TX_BODY);
        return 1;
    }

    /* The forward DMA landed in pods[entry->pod_idx]->dma_buffer at
     * entry->buf_offset. That same offset is the source for reverse DMA. */
    struct pod_state *fwd_buf_pod = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods)
        fwd_buf_pod = &objs->pods[entry->pod_idx];

    /* Hot path errors below fire per-request under misconfig; suppress logs.
     * Host's wait_response_v2 timeout is the persistent-failure detector. */
    if (!fwd_buf_pod || !fwd_buf_pod->dma_buffer ||
        fwd_buf_pod->local_mmap_dpa_handle == 0) {
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id, POOL_HOST_TX_BODY);
        return -1;
    }

    int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);
    struct pod_state *target_pod = echo_mode ? src_pod
                                             : find_pod_by_id(objs, dst_pod_id);

    if (!target_pod || !target_pod->tx_ring) {
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id, POOL_HOST_TX_BODY);
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
    /* Phase 3: preserve OP_HDR_BATCH / OP_CHUNK discriminator bits so the
     * dst host can demux to the correct buffer + parser. */
    fwd_desc.flags |= (entry->flags & (OP_HDR_BATCH | OP_CHUNK));
    fwd_desc.valid = 1;

    doca_error_t fwd_result = dpu_enqueue_reverse_dma(
        objs, fwd_buf_pod, target_pod, &fwd_desc, entry->buf_offset, payload_len);

    if (fwd_result == DOCA_ERROR_AGAIN) {
        return 0;  /* TX descriptor ring full — preserve entry, retry next iter */
    }
    if (fwd_result != DOCA_SUCCESS) {
        /* Reverse will not fire — release src host's TX slot now so the
         * caller doesn't stall on 2s reclaim. Silent: host timeout reports. */
        send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id, POOL_HOST_TX_BODY);
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
                                                 d->dst_pod_id, d->pool_type);
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
        /* Hard error — silently drop. Host's 2s reclaim is the safety net;
         * per-request log here floods DPU /tmp under sustained backpressure. */
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

    /* Phase 3: pool_type for TX_ACK is the SOURCE pool that fed the forward DMA.
     * For OP_HDR_BATCH, the source was the sender's hdr_tx_buffer; for everything
     * else (body chunks via legacy reverse path) it's the body TX pool. Without
     * this, the sender's TX_ACK handler frees the wrong slot (body instead of
     * hdr) and the hdr_tx pool leaks one slot per hdr_batch — exhausting the
     * 1024-slot hdr pool within ~5s at 10k RPS and stalling all subsequent
     * send_response calls (echo side) since they can't allocate a hdr slot. */
    uint8_t ack_pool = (entry->flags & OP_HDR_BATCH) ? POOL_HOST_TX_HDR
                                                     : POOL_HOST_TX_BODY;

    if (!target_pod || !target_pod->connection) {
        /* Still try to release src's TX slot — silent (per-req log floods). */
        struct pod_state *src_pod = find_pod_by_id(objs, entry->src_pod_id);
        send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id, ack_pool);
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
        /* Hard error: release src's TX slot. Silent (per-req log floods). */
        struct pod_state *src_pod = find_pod_by_id(objs, entry->src_pod_id);
        send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id, ack_pool);
        return -1;
    }

    /* DMA_COMPLETION sent. Now release src's TX slot (under in-place
     * forwarding this is the only place it gets released on the success
     * path — process_forward_entry no longer ACKs on success). For echo
     * src==dst this goes to the same pod as the DMA_COMPLETION above;
     * comch handles the second send (or defers via TX_ACK queue). */
    struct pod_state *src_pod = echo_mode ? target_pod
                                          : find_pod_by_id(objs, entry->src_pod_id);
    send_or_defer_tx_ack(objs, src_pod, entry->req_id, entry->dst_pod_id, ack_pool);

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

    while (processed < max_batch) {
        dpu_comp_entry_t *entry = comp_queue_peek(&objs->comp_queue);
        if (!entry)
            break;

        /* Progress both PEs between entries:
         * - pe: process send task completions (frees send pool slots)
         * - consumer_pe: resubmit DPA→DPU recv tasks so DPA doesn't stall
         *   waiting for consumer availability. Critical at high load where
         *   forward+reverse share one producer/consumer (20000 dma_copy/s
         *   at 5000 RPS 8K). Without this, DPA exhausts 1024 recv tasks
         *   during the batch and stalls. */
        doca_pe_progress(objs->pe);
        doca_pe_progress(objs->consumer_pe);

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


    /* Init pods table */
    memset(objs->pods, 0, sizeof(objs->pods));
    objs->num_pods = 0;
    pthread_mutex_init(&objs->pods_lock, NULL);

    /* Init deferred completion queue + backpressure state */
    objs->comp_queue.head = 0;
    objs->comp_queue.tail = 0;
    objs->num_deferred_recv = 0;

    /* 1. comch control path server (waits for first connection) */
    result = init_comch_ctrl_path_server("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        cleanup_objects(objs);
        return;
    }

    /* 2. comch datapath consumer (for DPA → DPU messages) */
    result = init_comch_datapath_consumer(objs);
    if (result != DOCA_SUCCESS) {
        cleanup_objects(objs);
        return;
    }

    /* 3. DPA app init (shared) */
    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        cleanup_objects(objs);
        return;
    }

    /* 4. DPA thread create (shared, not run yet — started on first pod) */
    result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    if (result != DOCA_SUCCESS) {
        cleanup_objects(objs);
        return;
    }

    /* 5. comch DPA message queue (shared) */
    result = init_comch_dpa_msgq(objs, objs->consumer_pe);
    if (result != DOCA_SUCCESS) {
        cleanup_objects(objs);
        return;
    }

    /* 6. No more blocking waits — per-pod DMA setup is event-driven.
     *    When a pod's ring_mmap + remote_mmap arrive (via process_mmap_msg),
     *    setup_pod_dma() is called automatically, which sets up buf_arr,
     *    local DMA buffer, DPA ring info, and starts DPA thread on first pod. */


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
                }
            }
            objs->num_deferred_recv = remaining;
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
