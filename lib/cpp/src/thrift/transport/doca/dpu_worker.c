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

#define DPU_BUFFER_SIZE (1024 * 1024)

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

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s, pods: %d",
                          elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt, objs->num_pods);
            /* DEBUG: check if DMA data arrived in each pod's dma_buffer at multiple offsets */
            for (int i = 0; i < objs->num_pods; i++) {
                uint8_t *buf = (uint8_t *)objs->pods[i].dma_buffer;
                if (buf) {
                    /* Check offset 0, 59, 118, 177 (assuming 59-byte requests) */
                    static const uint32_t dbg_offsets[] = {0, 59, 118, 177, 236};
                    for (int oi = 0; oi < 5; oi++) {
                        uint32_t off = dbg_offsets[oi];
                        DOCA_LOG_INFO("DEBUG dma_buffer pod=%d off=%u: "
                                      "%02x %02x %02x %02x %02x %02x %02x %02x",
                                      objs->pods[i].pod_id, off,
                                      buf[off+0], buf[off+1], buf[off+2], buf[off+3],
                                      buf[off+4], buf[off+5], buf[off+6], buf[off+7]);
                    }
                }
            }
            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;
        }
    }

}
