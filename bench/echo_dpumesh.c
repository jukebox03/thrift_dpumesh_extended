/*
 * echo_dpumesh.c — DPUmesh echo server daemon (A안)
 *
 * Lifecycle:
 *   1. dpumesh_init() once at startup, register as pod_id=11.
 *   2. Loop: dequeue request descriptor → enqueue OP_RESPONSE back to
 *      desc.src_pod_id with the same body.
 *
 * No Thrift parsing. Pure transport echo for fair latency measurement.
 *
 * Response uses the same TX-slot pending pattern the server transport uses
 * (register_pending + attach_tx + release_async), so the host's TX slot is
 * released by the TX_ACK from DPU.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>

#include <thrift/transport/dpumesh.h>
#include <thrift/transport/doca/mesh.h>

#define ECHO_THREADS_DEFAULT 32

static dpumesh_ctx_t *g_ctx = NULL;
static int            g_split = 0;

static void process_one(const sw_descriptor_t *req) {
    /* Pull request body */
    if (req->body_buf_slot < 0 || req->body_len == 0) {
        if (req->body_buf_slot >= 0)
            dpumesh_rx_free(g_ctx, req->body_buf_slot);
        return;
    }
    uint8_t *rx_buf = dpumesh_rx_buf(g_ctx, req->body_buf_slot);
    if (!rx_buf) {
        dpumesh_rx_free(g_ctx, req->body_buf_slot);
        return;
    }

    /* Phase 3 split path: echo via dpumesh_send_response. Mesh layer
     * handles req_id, dst_pod, and the dual hdr/chunk write. */
    if (g_split) {
        /* Copy body locally so we can release rx slot ASAP. */
        uint8_t body[MESH_CHUNK_BODY_BUDGET];
        uint32_t blen = req->body_len > sizeof(body) ? (uint32_t)sizeof(body) : req->body_len;
        if (blen > 0) memcpy(body, rx_buf, blen);
        dpumesh_rx_free(g_ctx, req->body_buf_slot);

        struct mesh_req_id rid = { .src_id = (uint32_t)req->src_pod_id,
                                   .seq    = req->req_id };
        uint8_t flags = (req->flags & ~OP_REQUEST) | OP_RESPONSE;
        (void)dpumesh_send_response(g_ctx, req->src_pod_id, rid,
                                    body, blen, flags);
        return;
    }

    /* Allocate TX slot for response */
    int tx_slot = dpumesh_tx_alloc(g_ctx);
    if (tx_slot < 0) {
        dpumesh_rx_free(g_ctx, req->body_buf_slot);
        return;
    }

    uint8_t *tx_buf = dpumesh_tx_buf(g_ctx, tx_slot);
    memcpy(tx_buf, rx_buf, req->body_len);

    /* Done with RX — free now (data copied into TX slot) */
    dpumesh_rx_free(g_ctx, req->body_buf_slot);

    /* Server-side pending lifecycle (mirror of TDpumeshTransport flush) */
    if (dpumesh_register_pending(g_ctx, req->req_id) < 0) {
        dpumesh_tx_free(g_ctx, tx_slot);
        return;
    }
    dpumesh_pending_attach_tx(g_ctx, req->req_id, tx_slot);

    sw_descriptor_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.header_buf_slot     = -1;
    resp.body_buf_slot       = tx_slot;
    resp.body_len            = req->body_len;
    resp.req_id              = req->req_id;
    resp.dst_pod_id          = req->src_pod_id;        /* back to sender */
    resp.src_pod_id          = dpumesh_get_pod_id(g_ctx);
    resp.flags               = (req->flags & ~OP_REQUEST) | OP_RESPONSE;
    resp.valid               = 1;
    resp.src_body_pool_type  = POOL_HOST_TX_BODY;
    resp.src_body_pod_id     = dpumesh_get_pod_id(g_ctx);
    resp.src_body_buf_slot   = tx_slot;
    resp.src_header_buf_slot = -1;

    if (dpumesh_enqueue(g_ctx, &resp) < 0) {
        dpumesh_cancel_pending(g_ctx, req->req_id);
        return;
    }
    /* Fire-and-forget: TX_ACK from DPU will free the TX slot */
    dpumesh_pending_release_async(g_ctx, req->req_id);
}

/* Worker thread: dequeue + echo loop. Multiple of these run concurrently —
 * dpumesh_dequeue serializes on rx_lock briefly to grab one descriptor,
 * then process_one runs outside the lock so workers parallelize. dpumesh
 * tx/rx/pending APIs are all internally locked (proven thread-safe by the
 * multi-threaded gateway). Without this, a single dequeue thread caps
 * throughput at 1 / per_msg_cost — observed as ~25K RPS at 8KB payloads
 * with 3+ms client-side queueing. */
static void *worker(void *arg) {
    (void)arg;
    while (1) {
        sw_descriptor_t req;
        if (dpumesh_dequeue(g_ctx, &req, -1) < 0)
            continue;
        if (!req.valid) continue;
        process_one(&req);
    }
    return NULL;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    signal(SIGPIPE, SIG_IGN);

    int worker_id = 11;
    if (getenv("BENCH_WORKER_ID"))
        worker_id = atoi(getenv("BENCH_WORKER_ID"));

    int n_threads = ECHO_THREADS_DEFAULT;
    if (getenv("ECHO_THREADS")) {
        n_threads = atoi(getenv("ECHO_THREADS"));
        if (n_threads < 1) n_threads = 1;
    }
    if (getenv("BENCH_SPLIT"))
        g_split = atoi(getenv("BENCH_SPLIT")) ? 1 : 0;
    fprintf(stderr, "[echo] path: %s\n", g_split ? "SPLIT (Phase 3)" : "LEGACY");

    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int rc = dpumesh_init(&g_ctx, "echo-dpumesh", worker_id, &cfg);
    if (rc != 0 || !g_ctx) {
        fprintf(stderr, "[echo] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    fprintf(stderr, "[echo] ready: pod_id=%d, threads=%d\n",
            dpumesh_get_pod_id(g_ctx), n_threads);

    pthread_t *tids = calloc((size_t)n_threads, sizeof(pthread_t));
    if (!tids) return 1;
    for (int i = 0; i < n_threads; i++) {
        if (pthread_create(&tids[i], NULL, worker, NULL) != 0) {
            fprintf(stderr, "[echo] pthread_create %d failed\n", i);
            return 1;
        }
    }
    /* Block forever */
    for (int i = 0; i < n_threads; i++)
        pthread_join(tids[i], NULL);
    return 0;
}
