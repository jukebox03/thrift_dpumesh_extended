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

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

DOCA_LOG_REGISTER(DPU_WORKER);

/* ====== Reverse DMA Enqueue (DPU→CPU) ====== */

/*
 * Enqueue data for DPU→CPU DMA via the destination pod's TX ring.
 *
 * Layout in TX buffer:
 *   [fc_header (8B)] [sw_descriptor_t (64B)] [payload (body_len B)]
 *   Total padded to 128B alignment for DMA.
 *
 * Flow control: checks available space in dst pod's TX buffer.
 * Returns DOCA_SUCCESS, DOCA_ERROR_AGAIN (TX buffer full), or error.
 */
static doca_error_t
dpu_enqueue_reverse_dma(struct objects *objs, struct pod_state *dst_pod,
                        const sw_descriptor_t *desc,
                        const uint8_t *body, uint32_t body_len,
                        uint32_t rx_consumer_tail_to_piggyback)
{
    if (!dst_pod->tx_ring || !dst_pod->tx_buffer) {
        DOCA_LOG_ERR("dpu_enqueue_reverse_dma: pod %d reverse DMA not ready", dst_pod->pod_id);
        return DOCA_ERROR_NOT_CONNECTED;
    }

    /* Total payload = fc_header + sw_descriptor + body */
    uint32_t total_len = sizeof(struct fc_header) + sizeof(sw_descriptor_t) + body_len;
    uint32_t padded_len = (total_len + 127) & ~(uint32_t)127;

    /* Flow control: check available space in TX buffer */
    uint32_t head = dst_pod->tx_producer_head;
    uint32_t tail = dst_pod->tx_last_consumer_tail;
    uint32_t buf_size = (uint32_t)dst_pod->tx_buf_size;
    uint32_t used = (head >= tail) ? (head - tail) : (buf_size - tail + head);
    uint32_t available = buf_size - used;

    if (padded_len > available) {
        DOCA_LOG_WARN("dpu_enqueue_reverse_dma: TX buffer full for pod %d "
                      "(need=%u, avail=%u, head=%u, tail=%u)",
                      dst_pod->pod_id, padded_len, available, head, tail);
        return DOCA_ERROR_AGAIN;
    }

    /* Check wrap-around: if data won't fit contiguously, wrap to beginning */
    uint32_t write_pos = head;
    if (write_pos + padded_len > buf_size) {
        /* Wrap to beginning — requires space from 0 */
        if (padded_len > tail) {
            DOCA_LOG_WARN("dpu_enqueue_reverse_dma: TX buffer wrap-around insufficient for pod %d",
                          dst_pod->pod_id);
            return DOCA_ERROR_AGAIN;
        }
        write_pos = 0;
    }

    /* Write fc_header + descriptor + body into TX buffer */
    uint8_t *dst = (uint8_t *)dst_pod->tx_buffer + write_pos;

    struct fc_header *hdr = (struct fc_header *)dst;
    hdr->consumer_tail = rx_consumer_tail_to_piggyback;
    hdr->payload_len = sizeof(sw_descriptor_t) + body_len;

    memcpy(dst + sizeof(struct fc_header), desc, sizeof(sw_descriptor_t));
    if (body_len > 0)
        memcpy(dst + sizeof(struct fc_header) + sizeof(sw_descriptor_t), body, body_len);

    /* Post descriptor to TX ring */
    struct dma_desc *dma = get_next_dma_desc(dst_pod->tx_ring);
    if (!dma) {
        DOCA_LOG_WARN("dpu_enqueue_reverse_dma: TX ring full for pod %d", dst_pod->pod_id);
        return DOCA_ERROR_AGAIN;
    }

    /* Fill descriptor: DPA DMAs from DPU TX buffer to Host RX buffer.
     * addr is offset within TX buffer — DPA adds ring->dpu_addr base. */
    dma->addr = write_pos;
    dma->size = total_len;
    dma->idx = desc->req_id;
    dma->dst_pod_id = desc->dst_pod_id;
    dma->flags = desc->flags;

    __sync_synchronize();
    dma->valid = 1;

    /* Advance TX producer head */
    dst_pod->tx_producer_head = write_pos + padded_len;
    if (dst_pod->tx_producer_head >= buf_size)
        dst_pod->tx_producer_head = 0;

    DOCA_LOG_INFO("dpu_enqueue_reverse_dma: pod=%d write_pos=%u total_len=%u padded=%u new_head=%u",
                  dst_pod->pod_id, write_pos, total_len, padded_len, dst_pod->tx_producer_head);
    return DOCA_SUCCESS;
}

/* ====== Deferred Completion Queue Drain ====== */

/*
 * Process a COMP_ENTRY_FORWARD entry: TX_ACK + reverse DMA routing.
 * Returns 1 if processed, 0 if should retry (TX buffer full), -1 on error.
 */
static int
process_forward_entry(struct objects *objs, dpu_comp_entry_t *entry)
{
    int32_t src_pod_id = entry->src_pod_id;
    int32_t dst_pod_id = entry->dst_pod_id;
    uint32_t req_id = entry->req_id;
    uint32_t payload_len = entry->length;

    /* Zero-copy: resolve data pointer from pod's RX DMA buffer + offset */
    uint8_t *data = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods) {
        struct pod_state *p = &objs->pods[entry->pod_idx];
        if (p->dma_buffer)
            data = (uint8_t *)p->dma_buffer + entry->buf_offset;
    }
    if (!data) {
        DOCA_LOG_ERR("comp_queue: invalid pod_idx=%d for req_id=%u", entry->pod_idx, req_id);
        return -1;
    }

    struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);

    /* Route data to destination pod via reverse DMA (DPU→CPU) */
    int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);
    struct pod_state *target_pod = echo_mode ? src_pod
                                             : find_pod_by_id(objs, dst_pod_id);

    /* Advance consumer_tail FIRST so both reverse DMA and TX_ACK can
     * piggyback the up-to-date value. Host uses this to refresh its
     * fc_tx_last_consumer_tail window; under an idle load the TX_ACK
     * may be the only channel carrying the update (no reverse DMA). */
    uint32_t new_tail = 0;
    int tail_advanced = 0;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods) {
        struct pod_state *p = &objs->pods[entry->pod_idx];
        uint32_t dma_start = entry->buf_offset - (uint32_t)sizeof(struct fc_header);
        uint32_t total_dma_len = (uint32_t)sizeof(struct fc_header) + payload_len;
        uint32_t padded = (total_dma_len + 127) & ~(uint32_t)127;
        new_tail = dma_start + padded;
        if (new_tail >= p->dma_buf_size)
            new_tail = 0;
        p->rx_consumer_tail = new_tail;
        tail_advanced = 1;
    }

    if (target_pod && target_pod->tx_ring) {
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

        uint32_t fc_tail = 0;
        if (src_pod)
            fc_tail = src_pod->rx_consumer_tail;

        doca_error_t fwd_result = dpu_enqueue_reverse_dma(
            objs, target_pod, &fwd_desc, data, payload_len, fc_tail);
        if (fwd_result == DOCA_ERROR_AGAIN) {
            return 0;  /* preserve entry, retry next iteration */
        }
        if (fwd_result != DOCA_SUCCESS)
            DOCA_LOG_ERR("%s failed for req_id=%u dst_pod=%d: %s",
                         echo_mode ? "Echo" : "Forward",
                         req_id, dst_pod_id, doca_error_get_descr(fwd_result));
        else
            DOCA_LOG_INFO("%s %u bytes to pod %d for req_id=%u via reverse DMA",
                          echo_mode ? "Echo" : "Forwarded",
                          payload_len, echo_mode ? src_pod_id : dst_pod_id, req_id);
    } else {
        DOCA_LOG_ERR("DMA completed: target_pod=%d not found or TX ring not ready",
                     echo_mode ? src_pod_id : dst_pod_id);
    }

    /* Send TX_ACK with the advanced tail so the Host refreshes its
     * flow-control window even on an idle connection (no reverse DMA). */
    if (src_pod && src_pod->connection) {
        uint32_t ack_tail = tail_advanced ? new_tail : (src_pod ? src_pod->rx_consumer_tail : 0);
        doca_error_t ack_result = server_send_tx_ack_to(objs, src_pod->connection,
                                                         req_id, dst_pod_id, ack_tail);
        if (ack_result != DOCA_SUCCESS)
            DOCA_LOG_WARN("TX_ACK failed for req_id=%u to pod %d: %s",
                          req_id, src_pod_id, doca_error_get_descr(ack_result));
    }

    return 1;
}

/*
 * Process a COMP_ENTRY_REV_NOTIFY entry: reverse DMA completed (DPU→CPU).
 * Send DMA_COMPLETION notification to the destination Host pod via comch.
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
        return 0;  /* retry next iteration */
    }
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("REV_NOTIFY: send DMA_COMPLETION to pod %d failed: %s",
                     target_id, doca_error_get_descr(result));
        return -1;
    }

    DOCA_LOG_INFO("REV_NOTIFY: sent DMA_COMPLETION to pod %d (req_id=%u pos=%u len=%u)",
                  target_id, entry->req_id, entry->buf_offset, entry->length);
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
    struct timespec last, now;
    double elapsed = 0.0;

    DOCA_LOG_INFO("Starting DPU worker");

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
    while (true) {
        doca_pe_progress(objs->consumer_pe);
        doca_pe_progress(objs->pe);  /* handle new connections, REGISTER, TX_DATA */

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
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            uint32_t cq_depth = comp_queue_usage(&objs->comp_queue);
            DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s, pods: %d, cq_depth: %u, deferred: %d",
                          elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt, objs->num_pods,
                          cq_depth, objs->num_deferred_recv);

            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;
        }
    }

}
