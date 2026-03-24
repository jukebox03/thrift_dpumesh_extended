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

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>

#include <time.h>

DOCA_LOG_REGISTER(DPU_WORKER);

#define DPU_BUFFER_SIZE (1024 * 1024)

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec last, now;
    double elapsed = 0.0;

    DOCA_LOG_INFO("Starting DPU worker");

    /* 1. comch control path server */
    result = init_comch_ctrl_path_server("DPUMesh", objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path server: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 2. comch datapath consumer */
    result = init_comch_datapath_consumer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch datapath consumer: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 3. DPA objects */
    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init DPA objects: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 4. DPA thread */
    result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DPA thread: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 5. comch DPA message queue */
    result = init_comch_dpa_msgq(objs, objs->consumer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch DPA msgq: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 6. Wait for Host ring mmap */
    while (objs->ring_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    /* 7. Setup DPA buffer array with remote mmap */
    result = setup_dpa_buf_array(objs, DMA_RING_SIZE, objs->ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DPA buffer array: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 8. Allocate local DMA buffer + PCI export */
    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                                       &objs->dma_buffer, DPU_BUFFER_SIZE,
                                       DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate DMA buffer: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 9. Wait for Host remote mmap */
    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    /* 10. Run DPA thread */
    result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to run DPA thread: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* 11. Send initial DMA request to DPA */
    result = send_dma_request_to_dpa(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA request to DPA: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    DOCA_LOG_INFO("DPU worker initialized, entering main loop");

    /* Main loop: poll consumer PE */
    clock_gettime(CLOCK_MONOTONIC, &last);
    while (true) {
        doca_pe_progress(objs->consumer_pe);

        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed = (now.tv_sec - last.tv_sec) +
                  (now.tv_nsec - last.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            if (objs->sent_msg_cnt > 0 || objs->recv_msg_cnt > 0)
                DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s",
                              elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt);
            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;
        }
    }
}
