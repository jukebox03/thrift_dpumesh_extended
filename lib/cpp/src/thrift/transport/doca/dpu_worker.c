#include "dpu_worker.h"

#include "comch_server.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "comch_producer.h"
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

#define DPU_BUFFER_SIZE (1024 * 1024)

/* ====== Deferred Completion Queue Drain ====== */

/*
 * Process up to max_batch entries from the deferred completion queue.
 * Called from the main loop to avoid blocking inside consumer callbacks.
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

        int32_t src_pod_id = entry->src_pod_id;
        int32_t dst_pod_id = entry->dst_pod_id;
        uint32_t req_id = entry->req_id;
        uint32_t payload_len = entry->length;
        uint8_t *data = entry->data;

        /* 1. Send TX ACK back to source pod */
        struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);
        if (src_pod && src_pod->connection) {
            doca_error_t ack_result = server_send_tx_ack_to(objs,
                src_pod->connection, req_id, dst_pod_id);
            if (ack_result == DOCA_ERROR_AGAIN) {
                /* Can't send yet — leave in queue, try next iteration */
                break;
            }
            if (ack_result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("TX_ACK send failed to src_pod=%d req_id=%u dst_pod=%d: %s",
                             src_pod_id, req_id, dst_pod_id,
                             doca_error_get_descr(ack_result));
            }
        }

        /* 2. Route data to destination pod */
        int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);

        if (!echo_mode) {
            struct pod_state *dst = find_pod_by_id(objs, dst_pod_id);
            if (dst && dst->connection) {
                sw_descriptor_t fwd_desc;
                memset(&fwd_desc, 0, sizeof(fwd_desc));
                fwd_desc.header_buf_slot = -1;
                fwd_desc.body_buf_slot = -1;
                fwd_desc.body_len = payload_len;
                fwd_desc.req_id = req_id;
                fwd_desc.src_pod_id = src_pod_id;
                fwd_desc.dst_pod_id = dst_pod_id;
                fwd_desc.flags = (entry->flags & OP_RESPONSE) | CASE_INGRESS;
                fwd_desc.valid = 1;

                doca_error_t fwd_result = server_send_rx_data_to(
                    objs, dst->connection,
                    &fwd_desc, sizeof(sw_descriptor_t),
                    data, payload_len);
                if (fwd_result == DOCA_ERROR_AGAIN) {
                    /* TX_ACK already sent but data forward blocked — drop from queue,
                     * the ACK ensures host frees its slot even if forward fails. */
                    DOCA_LOG_WARN("Forward to pod %d blocked for req_id=%u, will retry next iter",
                                  dst_pod_id, req_id);
                    break;
                }
                if (fwd_result != DOCA_SUCCESS)
                    DOCA_LOG_ERR("Forward to pod %d failed for req_id=%u: %s",
                                 dst_pod_id, req_id, doca_error_get_descr(fwd_result));
                else
                    DOCA_LOG_INFO("Forwarded %u bytes to pod %d for req_id=%u",
                                  payload_len, dst_pod_id, req_id);
            } else {
                DOCA_LOG_ERR("DMA completed: dst_pod=%d not found", dst_pod_id);
            }
        } else {
            /* Echo mode */
            sw_descriptor_t echo_desc;
            memset(&echo_desc, 0, sizeof(echo_desc));
            echo_desc.header_buf_slot = -1;
            echo_desc.body_buf_slot = -1;
            echo_desc.body_len = payload_len;
            echo_desc.req_id = req_id;
            echo_desc.flags = OP_RESPONSE;
            echo_desc.valid = 1;

            doca_error_t echo_result = DOCA_ERROR_NOT_FOUND;
            if (src_pod && src_pod->connection) {
                echo_result = server_send_rx_data_to(objs,
                    src_pod->connection,
                    &echo_desc, sizeof(sw_descriptor_t),
                    data, payload_len);
            }
            if (echo_result == DOCA_ERROR_AGAIN) {
                DOCA_LOG_WARN("Echo back blocked for req_id=%u, will retry next iter", req_id);
                break;
            }
            if (echo_result != DOCA_SUCCESS)
                DOCA_LOG_ERR("Echo back failed for req_id=%u: %s",
                             req_id, doca_error_get_descr(echo_result));
        }

        /* Done with this entry — free heap data and advance */
        free(data);
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

    /* Init deferred completion queue */
    objs->comp_queue.head = 0;
    objs->comp_queue.tail = 0;

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

        /* per-pod producer PE progress (응답 전송 완료 처리) */
        for (int i = 0; i < objs->num_pods; i++) {
            if (objs->pods[i].producer_pe)
                doca_pe_progress(objs->pods[i].producer_pe);
        }

        /* Drain deferred completion queue (TX_ACK + data forward) */
        process_completion_queue(objs, 32);

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            uint32_t cq_depth = (objs->comp_queue.tail >= objs->comp_queue.head)
                ? (objs->comp_queue.tail - objs->comp_queue.head)
                : (DPU_COMP_QUEUE_SIZE - objs->comp_queue.head + objs->comp_queue.tail);
            DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s, pods: %d, cq_depth: %u",
                          elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt, objs->num_pods, cq_depth);

            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;
        }
    }

}
