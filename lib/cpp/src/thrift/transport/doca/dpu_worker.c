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
#include "dpu_proxy.h"
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
 * is gone (its host died with it). */
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
                         "Host TX byte-ring bytes stall until close.",
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
 * prior flush is still pending (AGAIN).
 * Non-static: the proxy engine (dpu_proxy.c) releases custody through this. */
void
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
 * retained (retried by the next flush, including the idle proc==0 flush).
 * Non-static: the proxy engine (dpu_proxy.c) shares this accumulator. */
void
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

/* ====== L4 routing ======
 *
 * dpu_route_l4() is the single point where the DPU picks the destination pod for a
 * forward segment: service_table[svc] (one backend per service), with ROUTE-
 * AFFINITY layered on top — a non-zero route_group pins every chunk of one large
 * (SAR) message to ONE backend so they reassemble. Single ARM thread → the group
 * table needs no lock. Called by the SG-DMA egress engine (dpu_proxy.c) for
 * DEFERred request segs; a body-parsing L7 proxy is the future step (api.md §8).
 */
static inline int32_t
lb_pick(struct objects *objs, int16_t svc)
{
    if (svc >= 0 && svc < POD_ID_SPACE) {   /* single backend per service */
        int p = objs->service_table[svc];
        if (p >= 0)
            return p;
    }
    return -1;
}

/* L4 default route: service table + route-affinity pin. The single point that
 * resolves a DEFERred request seg's backend for the SG-DMA egress engine
 * (dpu_proxy.c → dpu_route_l4). Single ARM thread → the pin table needs no lock. */
int32_t
dpu_route_l4(struct objects *objs, int16_t svc, uint8_t rg)
{
    /* Route-affinity: an auto-chunked large message stamps EVERY chunk with the same
     * non-zero route_group (a per-channel rolling id, so one channel's concurrent
     * messages differ); a dmesh_pin_route'd conn stamps its one id on every message.
     * All messages sharing a key pin to ONE backend. Whichever message reaches this
     * single ARM thread FIRST LB-picks (lb_pick → the service table) + records the
     * pin; the rest reuse it.
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
     * Unknown service → -1 (caller drops + TX_ACKs the sender). */
    if (svc >= 0 && svc < POD_ID_SPACE) {
        int p = objs->service_table[svc];
        if (p >= 0)
            return p;
    }
    return -1;
}

/* ====== Deferred Completion Queue Drain ====== */

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
        /* Hard error — log and drop (the host's TX byte-ring bytes stay
         * unreclaimed until that conn closes; only these once-in-a-blue-moon
         * hard failures ever leak one). */
        DOCA_LOG_WARN("deferred TX_ACK fatal for port=%u seq=%u: %s",
                      d->port, d->seq, doca_error_get_descr(rc));
        sent++;  /* count as "removed from queue" */
    }
    objs->num_deferred_tx_acks = kept;
    return sent;
}

/*
 * Process up to max_batch entries from the deferred completion queue.
 * Called from the main loop to avoid blocking inside consumer callbacks.
 * Every forward completion (request AND reply) feeds the per-conn input window
 * → proxy_route (mock) → per-dst SG-DMA egress engine (dpu_proxy.c). All comch
 * sends happen here — never inside consumer callbacks, which would risk
 * re-entrant doca_pe_progress. Returns number of entries processed.
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

        /* Returns 1 consumed, 0 retry (alloc/pool pressure — retain entry),
         * -1 dropped (sender already TX_ACKed). */
        int result = px_ingest_forward(objs, entry);
        if (result == 0)
            break;  /* engine backpressure, retry next iteration */

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

    /* Drain deferred completion queue (reverse DMA enqueue, or proxy ingest).
     * 128/pass — safe because consumer_pe is progressed above, keeping DPA recv
     * tasks recycled. */
    int proc = process_completion_queue(objs, 128);

    /* SG-DMA egress: submit queued per-dst batches + emit completed batches'
     * REV_DONE entries and custody TX_ACKs (the unified DPU→host reverse path). */
    int px_progressed = px_drain(objs);

    /* Idle (no completions this pass) → flush partial TX_ACK + REV_DONE batches so
     * low-load latency is not held by coalescing. This is the only batch-flush
     * site (the periodic keepalive no longer flushes here). The SG engine emits
     * into these same accumulators, so this flushes its tail too. */
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

    return (did_consumer || did_ctrl || proc > 0 || px_progressed);
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

    /* Event-driven main loop: the ARM SLEEPS on epoll over the two PE notification
     * handles, waking on a real DPA→DPU completion (FWD_DONE/REV_DONE), a host
     * control message, OR a ~1 ms epoll timeout. On each tick it sends the 1 ms
     * DPU→DPA WAKE keepalive (the DPA EU parks when idle and a silent desc->valid=1
     * store can't wake it, so the ARM pokes it ~1 kHz). This is NOT busy-poll — the
     * ARM sleeps between ticks, so idle CPU is a few % (≈1 kHz wakeups), vs a full
     * core for busy-poll. If the epoll setup fails, it falls back to busy-poll. */
    const double keepalive_sec = 0.001;   /* 1 ms DPU→DPA WAKE cadence */
    struct timespec now, last_kick;
    double kick_elapsed;

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

    /* Route-affinity table ((service, route_group) -> pinned backend) starts empty.
     * 0xFF bytes == -1 for two's-complement int32. */
    memset(objs->route_group_backend, 0xFF, sizeof(objs->route_group_backend));

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

    /* N (DPA EU threads) + K (forward rings/pod) — baked to the measured config
     * (N=4 EUs, K=2 rings, so a 2-pod pair spreads across 4 distinct EUs; K=2 is
     * the sweet spot, K=4 is flat — the DPA op-rate caps ~810K dma_copy/s). Set
     * BEFORE the comch server accepts pods so process_mmap_msg sees the right K
     * (else it rejects a pod's 2nd forward ring and that pod never reaches
     * dma_ready). init_dpa_objects re-checks `<= 0`, so it reuses these. */
    objs->num_dpa_threads = 4;
    /* K = forward rings per pod (EU-sharding). Deploy option DPUMESH_RINGS_PER_POD
     * (default 2); clamped to [1, min(N, MAX_EU_PER_POD)]. The HOST reads the SAME
     * env (dpumesh_doca.c) and MUST land on the same K — forward rings pair 1:1, a
     * mismatch makes a pod never reach dma_ready. A conn pins to ONE ring
     * (src_port % K); different conns spread across the K rings. K≤N (=4). */
    objs->k_rings = DPUMESH_RINGS_PER_POD_DEFAULT;   /* = 2 */
    { const char *ke = getenv("DPUMESH_RINGS_PER_POD");
      if (ke && *ke) {
          int v = atoi(ke);
          if (v >= 1) {
              if (v > objs->num_dpa_threads) v = objs->num_dpa_threads;
              if (v > MAX_EU_PER_POD) v = MAX_EU_PER_POD;
              objs->k_rings = v;
          }
      } }
    DOCA_LOG_WARN("K forward rings/pod = %d (DPUMESH_RINGS_PER_POD; N=%d)",
                  objs->k_rings, objs->num_dpa_threads);

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
            }
        }
    }

    /* 7. SG-DMA egress engine (dpu_proxy.c) — the UNIFIED DPU→host reverse path,
     *    ALWAYS on. Requires objs->dev (opened in dpu_main before run_dpu_worker)
     *    + objs->pe (step 1) live. DPUMESH_PROXY only selects the request parser
     *    (passthru default = metadata L4 route; frame; L7). */
    result = px_init(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init L7-proxy L4 engine: %s",
                     doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    DOCA_LOG_INFO("DPU worker initialized (event-based), entering main loop");

    /* ===== Event-driven driver =====
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
