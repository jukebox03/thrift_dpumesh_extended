/*
 * echo_dpumesh.c — Single-thread echo server (v5).
 *
 * Why single-thread:
 *   Old echo ran 32 worker threads to serve 200k RPS. Flame analysis
 *   (plan.md §3) showed echo-side CPU was 45% kernel + 44% scheduler,
 *   only 2.5% in dpumesh-code. The thread pool was the bottleneck, not
 *   the transport. With 1 worker + 1 PE thread on a shared core, CFS
 *   alternates between just two threads — no wake/sleep cycle storm.
 *
 *   Per-request server work in the split path is small (builder lock +
 *   memcpy + atomic release_async). One thread can sustain >200k RPS
 *   for small bodies; only large-body memcpy might push us to add a
 *   second thread (ECHO_THREADS env still honored as escape hatch).
 *
 * Always uses the Phase 3 split path. Legacy g_split=0 mode is no
 * longer exercised; transport-side code stays per Hard Rule #2.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <stdint.h>

#include <thrift/transport/dpumesh.h>
#include <thrift/transport/doca/mesh.h>

#define ECHO_THREADS_DEFAULT 1

static dpumesh_ctx_t *g_ctx = NULL;

static void process_one(const sw_descriptor_t *req) {
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

    uint8_t body[MESH_CHUNK_BODY_BUDGET];
    uint32_t blen = req->body_len > sizeof(body)
                    ? (uint32_t)sizeof(body) : req->body_len;
    if (blen > 0) memcpy(body, rx_buf, blen);
    dpumesh_rx_free(g_ctx, req->body_buf_slot);

    struct mesh_req_id rid = { .src_id = (uint32_t)req->src_pod_id,
                               .seq    = req->req_id };
    uint8_t flags = (req->flags & ~OP_REQUEST) | OP_RESPONSE;
    (void)dpumesh_send_response(g_ctx, req->src_pod_id, rid,
                                body, blen, flags);
}

/* Worker: block on dequeue (cond_wait under empty), then echo. */
static void *worker(void *arg) {
    (void)arg;
    while (1) {
        sw_descriptor_t req;
        if (dpumesh_dequeue(g_ctx, &req, -1) < 0) continue;
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
    fprintf(stderr, "[echo] mode=single-thread split, threads=%d\n", n_threads);

    dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
    int rc = dpumesh_init(&g_ctx, "echo-dpumesh", worker_id, &cfg);
    if (rc != 0 || !g_ctx) {
        fprintf(stderr, "[echo] dpumesh_init failed: %d\n", rc);
        return 1;
    }
    fprintf(stderr, "[echo] ready: pod_id=%d\n", dpumesh_get_pod_id(g_ctx));

    pthread_t *tids = calloc((size_t)n_threads, sizeof(pthread_t));
    if (!tids) return 1;
    for (int i = 0; i < n_threads; i++) {
        if (pthread_create(&tids[i], NULL, worker, NULL) != 0) {
            fprintf(stderr, "[echo] pthread_create %d failed\n", i);
            return 1;
        }
    }
    for (int i = 0; i < n_threads; i++)
        pthread_join(tids[i], NULL);
    return 0;
}
