#ifndef _GNU_SOURCE
#define _GNU_SOURCE   /* pthread_setaffinity_np, cpu_set_t */
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
#include <pthread.h>
#include <sched.h>

DOCA_LOG_REGISTER(DPU_WORKER);

/* ===== DPU-side hop-latency trace (DPUMESH_TRACE=1, split-off only) =====
 * Measures the DPU's contribution to one request hop: from forward-completion
 * dequeue (request landed in DPU buffer) to reverse-completion dequeue (data
 * DMA'd to dst host) for the same req_id. = ARM route + reverse-desc post +
 * EU pickup + reverse DMA + completion delivery. Decides whether the chain's
 * ms-scale RTT is DPU-side or host-side. Avg written once/sec to a SEPARATE
 * file (not the -l-filtered log, so it never grows the DPU log). Off by default. */
#define TRACE_SLOTS 65536            /* power of two; ≥ max in-flight req_ids */
#define TRACE_FILE  "/tmp/dpumesh_trace.txt"
static int      g_trace_on = 0;
/* DPUMESH_SKIP_REQ_TXACK: when 1, the DPU does NOT send TX_ACK for request
 * forwards (flags lack OP_RESPONSE). The client frees its TX slot on response
 * arrival instead — halving the client host PE-thread's RX message load.
 * Response forwards still ACK their src. Split-OFF path only. */
static int      g_skip_req_txack = 0;
/* DPUMESH_BATCH_TXACK: when 1, response-forward TX_ACKs are coalesced per-dst
 * pod into one dmesh_batch_tx_ack_msg (flushed when full or on the 1 kHz tail
 * flush) instead of one message per request. Reduces the host PE thread's
 * remaining per-request message rate. Split-OFF only (single ARM owns the
 * per-pod batch). */
static int      g_batch_txack = 0;
static uint64_t g_trace_fwd_ns[TRACE_SLOTS];   /* fwd-deq timestamp per req_id */
static uint64_t g_trace_hop_sum_ns = 0;        /* Σ (rev-deq − fwd-deq) */
static uint64_t g_trace_hop_cnt = 0;
static uint64_t g_trace_hop_max_ns = 0;

static inline uint64_t dmesh_now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

/* Pin a thread to a single ARM core. core < 0 = leave placement relaxed. */
static void
dmesh_pin_thread(pthread_t t, int core)
{
    if (core < 0)
        return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    int rc = pthread_setaffinity_np(t, sizeof(set), &set);
    if (rc != 0)
        DOCA_LOG_WARN("pthread_setaffinity_np(core %d) failed: %d", core, rc);
}

/* Defined below; forward-declared so the SPLIT_SEND helpers can fall back to it
 * in the non-split path. */
static void
send_or_defer_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint32_t req_id, int32_t dst_pod_id);

/* SPLIT_SHARD: each worker thread sets this to its own &shard_send[k] so the
 * egress send-handoff helpers below push to the worker's PRIVATE 1p1c SPSC.
 * NULL on thread A / the single worker (those modes use objs->send_spsc). */
static __thread send_spsc_t *t_cur_send_spsc = NULL;

/* The egress-send SPSC the CURRENT thread must push to. SPLIT_SHARD → the
 * calling worker's shard_send[k] (via thread-local); SPLIT_SENDS → the single
 * shared send_spsc. (Only ever called when send_via_spsc(objs) is true.) */
static inline send_spsc_t *cur_send_spsc(struct objects *objs)
{
    return (objs->split_send == SPLIT_SHARD) ? t_cur_send_spsc : &objs->send_spsc;
}

/* ====== A-side egress send hand-off (SPLIT_SEND) ======
 * Thread A pushes a TX_ACK onto the A→B SPSC. Returns 1 if pushed (or nothing
 * to send), 0 if the SPSC is full (caller must retain the comp_queue entry so
 * backpressure flows through comp_queue, NOT a side buffer). */
static int
a_spsc_tx_ack(struct objects *objs, struct pod_state *src_pod,
              uint32_t req_id, int32_t dst_pod_id)
{
    if (!src_pod || !src_pod->connection)
        return 1;   /* originator gone — host's 2s reclaim is the safety net */
    send_spsc_t *sq = cur_send_spsc(objs);
    if (send_spsc_free(sq) < 1)
        return 0;
    send_req_t a;
    a.conn       = src_pod->connection;
    a.kind       = SEND_REQ_TX_ACK;
    a.flags      = 0;
    a.src_pod_id = src_pod->pod_id;
    a.dst_pod_id = dst_pod_id;
    a.pos        = 0;
    a.length     = 0;
    a.req_id     = req_id;
    send_spsc_push(sq, &a);
    return 1;
}

/* Forward-path error ACK on the correct thread. Returns 1 if dispatched (caller
 * drops the entry, returns -1); 0 if it could not be dispatched now (SPSC full)
 * and the caller must retain the entry (return 0). */
static int
forward_error_ack(struct objects *objs, struct pod_state *src_pod,
                  uint32_t req_id, int32_t dst_pod_id)
{
    /* SPLIT_SENDS / SPLIT_SHARD: the routing thread must not touch cc_server →
     * hand the ACK to the SENDER via an SPSC (shared, or the worker's private
     * shard_send[k]). SPLIT_REBAL/OFF: the caller IS the send-owning thread, so
     * send inline. */
    if (send_via_spsc(objs))
        return a_spsc_tx_ack(objs, src_pod, req_id, dst_pod_id);
    send_or_defer_tx_ack(objs, src_pod, req_id, dst_pod_id);
    return 1;
}

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

/* ====== Batched TX_ACK (DPUMESH_BATCH_TXACK, split-OFF) ====== */

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
 * the single-send path when batching is off, the pod is gone, or the batch is
 * already full and a prior flush is still pending (AGAIN). */
static void
batch_or_send_tx_ack(struct objects *objs, struct pod_state *src_pod,
                     uint32_t req_id, int32_t dst_pod_id)
{
    if (!g_batch_txack || !src_pod || !src_pod->connection) {
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

/* ====== L7 routing seam (MOCK) ======
 *
 * dpu_route() is the single point where the DPU decides the destination pod
 * for a forward completion. The TARGET architecture (architecture.md §6 step ④)
 * has the DPU parse the request body to COMPUTE dst (true L7). For now this is
 * MOCKED: the host still stamps a routing input in desc.dst_pod_id and the mock
 * returns it verbatim, so behaviour is byte-identical to the host-relay path.
 *
 * This is the seam where a fixed routing table or real body parsing plugs in
 * later (a one-function change). Deliberately we keep the host-provided value
 * as the routing INPUT rather than foreclosing it: the mock reads NO body, so
 * nothing yet forces DPU staging, and host->host direct DMA stays on the table.
 *
 * Echo benchmark note: a *literal constant* dst would break the 2-pod cross-pod
 * workload (pod 10->11, 11->10), so the mock is a verbatim passthrough, which is
 * the identity routing the bench already relies on.
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
    int32_t dst_pod_id = dpu_route(objs, entry);   /* L7 seam (mock = passthrough) */
    uint32_t req_id = entry->req_id;
    uint32_t payload_len = entry->length;

    /* TRACE: stamp when this hop's request landed in the DPU (fwd-completion
     * dequeue). Paired with rev-completion dequeue in process_rev_notify_entry. */
    if (g_trace_on)
        g_trace_fwd_ns[req_id & (TRACE_SLOTS - 1)] = dmesh_now_ns();

    /* Resolve src_pod for TX_ACK on error paths. */
    struct pod_state *src_pod = find_pod_by_id(objs, src_pod_id);

    /* The forward DMA landed in pods[entry->pod_idx]->dma_buffer at
     * entry->buf_offset. That same offset is the source for reverse DMA. */
    struct pod_state *fwd_buf_pod = NULL;
    if (entry->pod_idx >= 0 && entry->pod_idx < objs->num_pods)
        fwd_buf_pod = &objs->pods[entry->pod_idx];

    /* pod_data_ready ACQUIRE-loads dma_ready so the dma_buffer/handle/tx_ring
     * reads below see the RELEASE-published setup fields under SPLIT_SEND. */
    if (!fwd_buf_pod || !pod_data_ready(fwd_buf_pod) || !fwd_buf_pod->dma_buffer ||
        fwd_buf_pod->local_mmap_dpa_handle == 0) {
        DOCA_LOG_ERR("comp_queue: invalid pod_idx=%d for req_id=%u", entry->pod_idx, req_id);
        if (!forward_error_ack(objs, src_pod, req_id, dst_pod_id))
            return 0;   /* SPSC full — retain entry, retry next iter */
        return -1;
    }

    int echo_mode = (dst_pod_id == -1 || dst_pod_id == src_pod_id);
    struct pod_state *target_pod = echo_mode ? src_pod
                                             : find_pod_by_id(objs, dst_pod_id);

    if (!target_pod || !pod_data_ready(target_pod) || !target_pod->tx_ring) {
        DOCA_LOG_ERR("DMA completed: target_pod=%d not found or TX ring not ready",
                     echo_mode ? src_pod_id : dst_pod_id);
        if (!forward_error_ack(objs, src_pod, req_id, dst_pod_id))
            return 0;
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
        if (!forward_error_ack(objs, src_pod, req_id, dst_pod_id))
            return 0;
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
    /* TRACE: reverse-completion dequeue — close the hop started at fwd-deq.
     * gap = full DPU-side hop latency (route + reverse-post + EU pickup +
     * reverse DMA + completion delivery). */
    if (g_trace_on) {
        uint64_t t0 = g_trace_fwd_ns[entry->req_id & (TRACE_SLOTS - 1)];
        if (t0) {
            uint64_t d = dmesh_now_ns() - t0;
            g_trace_hop_sum_ns += d;
            g_trace_hop_cnt++;
            if (d > g_trace_hop_max_ns) g_trace_hop_max_ns = d;
            g_trace_fwd_ns[entry->req_id & (TRACE_SLOTS - 1)] = 0;
        }
    }

    /* Determine destination pod — for echo (dst=-1 or dst==src), send to source */
    int echo_mode = (entry->dst_pod_id == -1 || entry->dst_pod_id == entry->src_pod_id);
    int32_t target_id = echo_mode ? entry->src_pod_id : entry->dst_pod_id;
    struct pod_state *target_pod = find_pod_by_id(objs, target_id);

    if (send_via_spsc(objs)) {
        /* Hand both egress sends to the SENDER. DMA_COMPLETION (→dst) and TX_ACK
         * (→src) are pushed as an all-or-nothing PAIR so a partial enqueue
         * can't split a request and desync src TX-slot accounting. SPSC-full →
         * return 0 so the entry is RETAINED and backpressure flows through the
         * ingest queue (BP_HIGH → deferred_recv → DPA stall), NOT a side buffer.
         * Order DMA_COMPLETION-before-TX_ACK is preserved (single SPSC, FIFO
         * drain) — load-bearing only for the echo same-conn case. SPLIT_SHARD:
         * this worker's private shard_send[k] (via cur_send_spsc) — still a
         * strict 1-producer SPSC, FIFO order intact. */
        send_spsc_t *sq = cur_send_spsc(objs);
        struct pod_state *src_pod = echo_mode ? target_pod
                                              : find_pod_by_id(objs, entry->src_pod_id);
        int have_dst = (target_pod && target_pod->connection);
        int have_src = (src_pod && src_pod->connection);
        int need = (have_dst ? 1 : 0) + (have_src ? 1 : 0);
        if (need == 0) {
            DOCA_LOG_ERR("REV_NOTIFY: target pod %d not found or no connection", target_id);
            return -1;   /* nothing deliverable — drop (host 2s reclaim) */
        }
        if (send_spsc_free(sq) < (uint32_t)need)
            return 0;    /* retain entry → ingest backpressure */
        if (have_dst) {
            send_req_t c;
            c.conn       = target_pod->connection;
            c.kind       = SEND_REQ_DMA_COMPLETION;
            c.flags      = entry->flags;
            c.src_pod_id = entry->src_pod_id;
            c.dst_pod_id = entry->dst_pod_id;
            c.pos        = entry->buf_offset;
            c.length     = entry->length;
            c.req_id     = entry->req_id;
            send_spsc_push(sq, &c);
        } else {
            DOCA_LOG_ERR("REV_NOTIFY: target pod %d not found or no connection", target_id);
        }
        if (have_src) {
            send_req_t a;
            a.conn       = src_pod->connection;
            a.kind       = SEND_REQ_TX_ACK;
            a.flags      = 0;
            a.src_pod_id = entry->src_pod_id;
            a.dst_pod_id = entry->dst_pod_id;
            a.pos        = 0;
            a.length     = 0;
            a.req_id     = entry->req_id;
            send_spsc_push(sq, &a);
        }
        return 1;
    }

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
    comp_msg.type = DMESH_MSG_REV_DONE;
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
    /* Skip TX_ACK for request forwards when enabled — the client frees its TX
     * slot on response arrival (REV_DONE). Response forwards (OP_RESPONSE) still
     * ACK their src, which gets no reply and needs the ACK to free its slot.
     * Echo same-conn (echo_mode) keeps the ACK too (src==dst, has a waiter). */
    if (!g_skip_req_txack || echo_mode || (entry->flags & OP_RESPONSE))
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

/* ====== Thread B drain — rebalanced (SPLIT_REBAL) ======
 * B drains the raw-completion work_spsc and does the FULL per-RTT work: route
 * (find_pod_by_id), reverse-DMA enqueue (process_forward_entry → dst tx_ring),
 * and the inline comch sends (process_rev_notify_entry, mode != SPLIT_SENDS).
 * Same peek/retain contract as process_completion_queue: result 0 = retain
 * (TX-ring or send pool busy) → stop draining → work_spsc fills → A's recv-cb
 * trips backpressure (ingest_usage ≥ BP_HIGH) → DPA stall. Single producer of
 * every tx_ring here is thread B (A only feeds work_spsc), so the reverse
 * desc post stays single-writer. */
static int
process_work_spsc(struct objects *objs, int max_batch)
{
    int processed = 0;
    while (processed < max_batch) {
        dpu_comp_entry_t *entry = work_spsc_peek(&objs->work_spsc);
        if (!entry)
            break;

        int result;
        if (entry->entry_type == COMP_ENTRY_REV_NOTIFY)
            result = process_rev_notify_entry(objs, entry);
        else
            result = process_forward_entry(objs, entry);
        if (result == 0)
            break;   /* busy — retain entry, retry next iter */

        work_spsc_pop(&objs->work_spsc);
        processed++;
    }
    return processed;
}

/* Submit up to `budget` queued egress sends from one SPSC into the single
 * cc_server. Stops early when the SPSC drains or the send pool returns AGAIN
 * (the caller re-progresses objs->pe to reap completions, then retries). Shared
 * by the SPLIT_SENDS single SENDER and the SPLIT_SHARD SENDER (one shard_send[k]
 * per call). Returns the number submitted. */
static int
drain_send_spsc(struct objects *objs, send_spsc_t *sq, int budget)
{
    int sent = 0;
    while (budget-- > 0) {
        send_req_t *r = send_spsc_peek(sq);
        if (!r)
            break;
        doca_error_t rc;
        if (r->kind == SEND_REQ_DMA_COMPLETION) {
            struct dmesh_dma_completion_msg m;
            m.type       = DMESH_MSG_REV_DONE;
            m.pos        = r->pos;
            m.length     = r->length;
            m.req_id     = r->req_id;
            m.src_pod_id = r->src_pod_id;
            m.dst_pod_id = r->dst_pod_id;
            m.flags      = r->flags;
            rc = server_send_msg_to_conn(objs, r->conn, (const char *)&m, sizeof(m));
        } else {
            rc = server_send_tx_ack_to(objs, r->conn, r->req_id, r->dst_pod_id);
        }
        if (rc == DOCA_ERROR_AGAIN)
            break;   /* pool full — retry next iter after objs->pe progress */
        /* success or hard error: consume. Hard error falls back to the host's
         * 2s slot reclaim, same as the single-thread path. */
        send_spsc_pop(sq);
        sent++;
    }
    return sent;
}

/* ====== SPLIT_SHARD worker (controller k) ======
 * One per active EU. Drains its private shard_work[k] (thread A is the sole
 * producer) and does the full per-RTT route + reverse-DMA post + egress-send
 * hand-off. Because thread A routes by effdst%N, worker k is the SOLE writer of
 * every tx_ring whose pod_id%N==k, so the reverse-desc post stays single-writer
 * with no lock. Egress sends are pushed to this worker's private shard_send[k]
 * (via t_cur_send_spsc) for the SENDER to submit — workers never touch cc_server.
 * On a busy entry (tx ring full or shard_send full) process_shard_work stops,
 * shard_work[k] fills, and thread A's ingest_usage (max over workers) trips
 * BP_HIGH → DPA stall: same single rate-matcher as the other modes. */
static int
process_shard_work(struct objects *objs, int k, int max_batch)
{
    int processed = 0;
    while (processed < max_batch) {
        dpu_comp_entry_t *entry = work_spsc_peek(&objs->shard_work[k]);
        if (!entry)
            break;

        int result;
        if (entry->entry_type == COMP_ENTRY_REV_NOTIFY)
            result = process_rev_notify_entry(objs, entry);
        else
            result = process_forward_entry(objs, entry);
        if (result == 0)
            break;   /* busy — retain entry, retry next iter */

        work_spsc_pop(&objs->shard_work[k]);
        processed++;
    }
    return processed;
}

struct shard_worker_arg { struct objects *objs; int k; };

static void *
run_shard_worker(void *arg)
{
    struct shard_worker_arg *wa = (struct shard_worker_arg *)arg;
    struct objects *objs = wa->objs;
    int k = wa->k;
    /* Bind this thread's egress sends to its own SPSC (read by cur_send_spsc). */
    t_cur_send_spsc = &objs->shard_send[k];
    int core = (objs->shard_worker_core_base >= 0)
                   ? objs->shard_worker_core_base + k : -1;
    dmesh_pin_thread(pthread_self(), core);
    DOCA_LOG_INFO("DPU shard worker %d running (core=%d)", k, core);

    while (true)
        process_shard_work(objs, k, 128);

    return NULL;
}

/* ====== Thread B — egress SENDER (SPLIT_SEND) ======
 * Owns objs->pe (comch control PE) + the comch send pool. Drains the route
 * thread(s)' SPSC(s) and submits the DMA_COMPLETION / TX_ACK they produced.
 * Peek-and-retry on DOCA_ERROR_AGAIN: a full send pool stops the drain, the
 * SPSC then fills, the routing side retains entries, and the ingest→DPA
 * backpressure engages. objs->pe is progressed every loop so send completions
 * are reaped (freeing the pool) and control messages (connection / REGISTER /
 * EXPORT_DESC) are serviced — all on this thread, so cc_server has exactly one
 * submitter+progressor (no concurrent-submit hazard) under every split mode. */
static void *
run_send_thread(void *arg)
{
    struct objects *objs = (struct objects *)arg;
    dmesh_pin_thread(pthread_self(), objs->arm_core_b);
    DOCA_LOG_INFO("DPU send thread (B) running (core=%d)", objs->arm_core_b);

    while (true) {
        doca_pe_progress(objs->pe);

        if (objs->split_send == SPLIT_REBAL) {
            /* Rebalanced: B does the whole route+reverse+send half off work_spsc. */
            process_work_spsc(objs, 128);
            drain_deferred_tx_acks(objs);
            continue;
        }

        if (objs->split_send == SPLIT_SHARD) {
            /* SENDER: drain every worker's shard_send[k] into the one cc_server.
             * TX_ACKs travel via shard_send (send_via_spsc), so deferred_tx_acks
             * is never populated in this mode — no drain needed (as SPLIT_SENDS). */
            for (int k = 0; k < objs->num_dpa_threads; k++)
                drain_send_spsc(objs, &objs->shard_send[k], 256);
            continue;
        }

        /* SPLIT_SENDS: B only submits the sends A queued. */
        drain_send_spsc(objs, &objs->send_spsc, 256);
    }
    return NULL;
}

/* ====== SPLIT_SHARD drain shard (DPUMESH_DRAIN_SHARDS > 1) ======
 * One drain thread per EU-channel group g, each progressing its OWN
 * consumer_pe_shard[g] (one-PE-per-thread). The recv-cb pushes completions to
 * shard_work[effdst%N]; for group-affine traffic effdst%N stays in group g, so
 * each shard_work[k] keeps a single producer (drain g). drain[0] runs on the
 * main thread (also emits the 1 Hz stat); drain[1..M-1] are spawned. Workers +
 * SENDER are unchanged. consumer_lock_shard[g] serializes this drain's pe
 * progress + keepalive sends vs the rare setup path (which locks all groups). */
static void
run_drain_shard_loop(struct objects *objs, int g)
{
    struct timespec last, now, last_kick;
    clock_gettime(CLOCK_MONOTONIC, &last);
    last_kick = last;
    while (true) {
        pthread_mutex_lock(&objs->consumer_lock_shard[g]);
        doca_pe_progress(objs->consumer_pe_shard[g]);
        pthread_mutex_unlock(&objs->consumer_lock_shard[g]);

        objects_drain_consumer_retry(objs);

        clock_gettime(CLOCK_MONOTONIC, &now);
        double kick_elapsed = (now.tv_sec - last_kick.tv_sec) +
                              (now.tv_nsec - last_kick.tv_nsec) / 1e9;
        if (kick_elapsed >= 0.001) {
            if (objs->dpa_thread_running_any) {
                struct comch_msg trigger;
                memset(&trigger, 0, sizeof(trigger));
                trigger.type = DPA_MSG_WAKE;
                pthread_mutex_lock(&objs->consumer_lock_shard[g]);
                for (int k = 0; k < objs->num_dpa_threads; k++) {
                    if (drain_group_of_eu(objs, k) == g && objs->dpa_thread_running[k])
                        (void)dmesh_doca_dpa_msgq_send_try(&objs->dpa_comches[k]->send,
                                                            &trigger, sizeof(trigger));
                }
                pthread_mutex_unlock(&objs->consumer_lock_shard[g]);
            }
            last_kick = now;
        }

        if (g == 0) {
            double elapsed = (now.tv_sec - last.tv_sec) +
                             (now.tv_nsec - last.tv_nsec) / 1e9;
            if (elapsed >= 1.0) {
                if (objs->recv_msg_cnt > 0 || objs->sent_msg_cnt > 0)
                    DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s, pods: %d, drains: %d",
                                  elapsed, objs->sent_msg_cnt, objs->recv_msg_cnt,
                                  objs->num_pods, objs->num_drain_shards);
                objs->sent_msg_cnt = 0;
                objs->recv_msg_cnt = 0;
                last = now;
            }
        }
    }
}

static void *
run_drain_shard(void *arg)
{
    struct shard_worker_arg *wa = (struct shard_worker_arg *)arg;
    struct objects *objs = wa->objs;
    int g = wa->k;
    /* Pin drain g to arm_core_a + g (drains 0..M-1 on consecutive cores). */
    int core = (objs->arm_core_a >= 0) ? objs->arm_core_a + g : -1;
    dmesh_pin_thread(pthread_self(), core);
    DOCA_LOG_INFO("DPU drain shard %d running (core=%d)", g, core);
    run_drain_shard_loop(objs, g);
    return NULL;
}

/* ====== DPU Worker ====== */

void
run_dpu_worker(struct objects *objs)
{
    doca_error_t result;
    struct timespec last, now, last_kick;
    double elapsed = 0.0;
    double kick_elapsed = 0.0;

    /* Keepalive interval (DPU→DPA WAKE). A yielded EU re-checks its reverse
     * tx_ring only when woken by a msgq message; the ARM's reverse-desc post is
     * a silent memory write, so this interval bounds the worst-case
     * forward→reverse handoff latency whenever the EU out-runs the ARM and
     * yields. Default 1 ms (legacy). Lower via DPUMESH_KEEPALIVE_US to shrink
     * that handoff wait (E3). */
    double keepalive_sec = 0.001;
    {
        const char *ke = getenv("DPUMESH_KEEPALIVE_US");
        if (ke) { double us = atof(ke); if (us >= 1.0) keepalive_sec = us / 1e6; }
    }
    DOCA_LOG_INFO("keepalive interval = %.0f us", keepalive_sec * 1e6);

    { const char *tr = getenv("DPUMESH_TRACE"); g_trace_on = tr ? atoi(tr) : 0; }
    { const char *st = getenv("DPUMESH_SKIP_REQ_TXACK"); g_skip_req_txack = st ? atoi(st) : 0;
      if (g_skip_req_txack) DOCA_LOG_INFO("SKIP_REQ_TXACK on: client frees TX on response, no request TX_ACK"); }
    { const char *bt = getenv("DPUMESH_BATCH_TXACK"); g_batch_txack = bt ? atoi(bt) : 0;
      if (g_batch_txack) DOCA_LOG_INFO("BATCH_TXACK on: coalesce up to %d TX_ACKs per message", BATCH_TXACK_MAX); }
    if (g_trace_on) DOCA_LOG_INFO("DPU-side hop-latency trace ON → " TRACE_FILE);

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

    /* Functional pipeline split (DPUMESH_SPLIT_SEND): 0=off, 1=sends-only,
     * 2=rebalanced, 3=sharded (N workers + SENDER). Non-zero runs extra ARM
     * cores. Default off → byte-identical single loop. */
    {
        const char *se = getenv("DPUMESH_SPLIT_SEND");
        int mode = se ? atoi(se) : 0;
        if (mode < SPLIT_OFF || mode > SPLIT_SHARD) mode = SPLIT_OFF;
        objs->split_send = mode;
        const char *ca = getenv("DPUMESH_ARM_CORE_A");
        const char *cb = getenv("DPUMESH_ARM_CORE_B");
        const char *wb = getenv("DPUMESH_SHARD_CORE_BASE");
        objs->arm_core_a = ca ? atoi(ca) : 2;
        objs->arm_core_b = cb ? atoi(cb) : 3;
        objs->shard_worker_core_base = wb ? atoi(wb) : 4;
        objs->send_spsc.head = 0;
        objs->send_spsc.tail = 0;
        objs->work_spsc.head = 0;
        objs->work_spsc.tail = 0;
        /* Per-EU SPSCs for SPLIT_SHARD (init all MAX_DPA_RINGS — cheap). */
        for (int k = 0; k < MAX_DPA_RINGS; k++) {
            objs->shard_work[k].head = 0;
            objs->shard_work[k].tail = 0;
            objs->shard_send[k].head = 0;
            objs->shard_send[k].tail = 0;
            objs->consumer_pe_shard[k] = NULL;
            pthread_mutex_init(&objs->consumer_lock_shard[k], NULL);
        }
        /* Drain sharding (SPLIT_SHARD only): M independent consumer_pe drains.
         * Raw env now; clamped to [1, num_dpa_threads] after init_dpa_objects. */
        const char *ds = getenv("DPUMESH_DRAIN_SHARDS");
        objs->num_drain_shards = ds ? atoi(ds) : 1;
        if (objs->num_drain_shards < 1) objs->num_drain_shards = 1;
        pthread_mutex_init(&objs->consumer_lock, NULL);
        DOCA_LOG_INFO("DPUMESH_SPLIT_SEND=%d (%s) (A=core %d, B=core %d, shard_base=%d)",
                      objs->split_send,
                      objs->split_send == SPLIT_SHARD ? "sharded" :
                      objs->split_send == SPLIT_REBAL ? "rebalanced" :
                      objs->split_send == SPLIT_SENDS ? "sends-only" : "off",
                      objs->arm_core_a, objs->arm_core_b, objs->shard_worker_core_base);
    }

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
     *    each EU is started on its first assigned pod in setup_pod_dma). */
    for (int k = 0; k < objs->num_dpa_threads; k++) {
        /* Pin EU thread k to absolute EU k (partition exposes abs_EUs 0-63)
         * when affinity is enabled; eu_id<0 leaves placement relaxed. */
        int eu_id = objs->dpa_affinity ? k : -1;
        result = dmesh_doca_dpa_thread_create(objs->dpa_threads[k], eu_id);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create DPA thread EU %d: %s",
                         k, doca_error_get_descr(result));
            cleanup_objects(objs);
            return;
        }
    }

    /* 4b. Drain sharding: clamp M to [1, num_dpa_threads] (SPLIT_SHARD only) and
     *     create the M consumer PEs the msgq channels will bind to (one PE per
     *     drain group). consumer_pe_shard[0] reuses the existing consumer_pe so
     *     M=1 is byte-identical to the single-drain path. */
    if (objs->split_send != SPLIT_SHARD)
        objs->num_drain_shards = 1;
    if (objs->num_drain_shards > objs->num_dpa_threads)
        objs->num_drain_shards = objs->num_dpa_threads;
    objs->consumer_pe_shard[0] = objs->consumer_pe;
    for (int g = 1; g < objs->num_drain_shards; g++) {
        result = doca_pe_create(&objs->consumer_pe_shard[g]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create drain PE %d: %s", g, doca_error_get_descr(result));
            cleanup_objects(objs);
            return;
        }
    }
    if (objs->num_drain_shards > 1)
        DOCA_LOG_INFO("Drain sharding: %d drains over %d EUs (group = eu*M/N)",
                      objs->num_drain_shards, objs->num_dpa_threads);

    /* 5. comch DPA message queue (channels bind to consumer_pe_shard[group(k)]) */
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

    /* SPLIT_SEND: pin this thread (A = consumer_pe / route) and spawn thread B
     * (egress sends, owns objs->pe). All single-threaded init above is done, so
     * objs->pe is now handed exclusively to B; A stops progressing it below. */
    if (objs->split_send) {
        dmesh_pin_thread(pthread_self(), objs->arm_core_a);
        int rc = pthread_create(&objs->send_thread, NULL, run_send_thread, objs);
        if (rc != 0) {
            DOCA_LOG_ERR("SPLIT_SEND: pthread_create(SENDER) failed (%d) — falling back to single thread", rc);
            objs->split_send = 0;
        } else if (objs->split_send == SPLIT_SHARD) {
            /* Spawn one route worker per EU (controller k). Args are static so
             * they outlive this function for the threads' lifetime. */
            static struct shard_worker_arg wargs[MAX_DPA_RINGS];
            int spawned = 0;
            for (int k = 0; k < objs->num_dpa_threads; k++) {
                wargs[k].objs = objs;
                wargs[k].k = k;
                int wrc = pthread_create(&objs->shard_workers[k], NULL,
                                         run_shard_worker, &wargs[k]);
                if (wrc != 0)
                    DOCA_LOG_ERR("SPLIT_SHARD: pthread_create(worker %d) failed (%d) "
                                 "— EU %d completions will stall", k, wrc, k);
                else
                    spawned++;
            }
            /* Drain sharding: spawn drains 1..M-1 (drain 0 = this main thread,
             * entered below). Each owns consumer_pe_shard[g] for EU group g. */
            static struct shard_worker_arg drargs[MAX_DPA_RINGS];
            int drains = 1;
            for (int g = 1; g < objs->num_drain_shards; g++) {
                drargs[g].objs = objs;
                drargs[g].k = g;   /* group index */
                int drc = pthread_create(&objs->drain_threads[g], NULL,
                                         run_drain_shard, &drargs[g]);
                if (drc != 0)
                    DOCA_LOG_ERR("SPLIT_SHARD: pthread_create(drain %d) failed (%d) "
                                 "— EU group %d completions will stall", g, drc, g);
                else
                    drains++;
            }
            DOCA_LOG_INFO("SPLIT_SHARD active: %d drain(s) (core base %d), SENDER=core %d, "
                          "%d/%d workers (core base %d)",
                          drains, objs->arm_core_a, objs->arm_core_b, spawned,
                          objs->num_dpa_threads, objs->shard_worker_core_base);
        } else {
            DOCA_LOG_INFO("SPLIT_SEND active: A(route)=core %d, B(send)=core %d",
                          objs->arm_core_a, objs->arm_core_b);
        }
    }

    /* SPLIT_SHARD + drain sharding: this main thread IS drain 0. The legacy
     * single-drain main loop below is only for M==1 / non-SHARD modes. */
    if (objs->split_send == SPLIT_SHARD && objs->num_drain_shards > 1) {
        run_drain_shard_loop(objs, 0);   /* never returns */
        return;
    }

    /* Main loop: poll consumer PE + ctrl path PE + per-pod producer PE */
    clock_gettime(CLOCK_MONOTONIC, &last);
    last_kick = last;
    while (true) {
        if (objs->split_send) {
            /* Thread A owns consumer_pe; thread B owns objs->pe + sends. Hold
             * consumer_lock around consumer_pe progress so the rare pod-setup
             * path on B (which progresses consumer_pe via DPU→DPA msgq sends)
             * never runs concurrently. Uncontended on the steady path. */
            pthread_mutex_lock(&objs->consumer_lock);
            doca_pe_progress(objs->consumer_pe);
            pthread_mutex_unlock(&objs->consumer_lock);
            /* objs->pe progress + drain_deferred_tx_acks are B's job now. */
        } else {
            doca_pe_progress(objs->consumer_pe);
            doca_pe_progress(objs->pe);  /* handle new connections, REGISTER, TX_DATA */

            /* Retry any TX_ACKs that were deferred when the comch send pool was
             * full. Done right after pe_progress so the just-released send-pool
             * slots are available. */
            drain_deferred_tx_acks(objs);
        }

        /* Drain deferred completion queue (reverse DMA enqueue).
         * SPLIT_REBAL: thread B drains work_spsc instead. SPLIT_SHARD: the N
         * shard workers drain shard_work[k] instead. In both, thread A is
         * consumer_pe drain ONLY here. SPLIT_OFF/SENDS: A routes + posts reverse
         * DMA. 128 entries per batch — safe because consumer_pe is progressed
         * inside the loop, keeping DPA recv tasks recycled. */
        if (objs->split_send != SPLIT_REBAL && objs->split_send != SPLIT_SHARD)
            process_completion_queue(objs, 128);

        /* Backpressure release: resubmit deferred recv tasks when the ingest
         * hand-off (comp_queue, or work_spsc under SPLIT_REBAL) drains below
         * BP_LOW. This resumes DPA→DPU message flow. Gate each submit on recv
         * task pool capacity; preserve un-submitted tasks by shifting them to
         * the front instead of zeroing count. */
        if (objs->num_deferred_recv > 0 &&
            ingest_usage(objs) < COMP_QUEUE_BP_LOW) {
            int remaining = 0;
            int resubmitted = 0;
            int original = objs->num_deferred_recv;
            for (int i = 0; i < original; i++) {
                struct doca_task *t = objs->deferred_recv[i];
                /* These are per-EU DPA→DPU msgq recv tasks — a FIXED per-channel
                 * allocation (CC_DPA_MAX_MSG_NUM each), NOT the standalone
                 * consumer's recv_tasks_in_flight pool. They were submitted in
                 * the DPA recv-cb without acquiring that pool, so resubmitting
                 * (which merely recycles an existing task) must not gate on it.
                 * The old try_acquire_exact gate was a permanent no-op: the pool
                 * is pinned at max by the vestigial consumer bootstrap, so the
                 * gate always failed and deferred tasks could NEVER be resubmitted
                 * → DPA stall at ≥4 active EUs. Backpressure is governed entirely
                 * by ingest_usage (deferred at BP_HIGH, resumed here below BP_LOW). */
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
        if (kick_elapsed >= keepalive_sec) {
            if (objs->dpa_thread_running_any) {
                struct comch_msg trigger;
                memset(&trigger, 0, sizeof(trigger));
                trigger.type = DPA_MSG_WAKE;
                /* dpa_comches[k]->send is a consumer_pe-bound producer; under
                 * SPLIT_SEND take consumer_lock so this never races B's
                 * setup-time ADD_RING sends on the same producer. */
                if (objs->split_send) pthread_mutex_lock(&objs->consumer_lock);
                /* Each running EU has its own channel and reschedules
                 * independently when idle, so every started EU needs its own
                 * keepalive to be woken within ~1 ms. */
                for (int k = 0; k < objs->num_dpa_threads; k++) {
                    if (objs->dpa_thread_running[k])
                        (void)dmesh_doca_dpa_msgq_send_try(&objs->dpa_comches[k]->send,
                                                            &trigger, sizeof(trigger));
                }
                if (objs->split_send) pthread_mutex_unlock(&objs->consumer_lock);
            }
            /* Tail flush: send any partially-filled TX_ACK batches so a pod's
             * last few acks (below BATCH_TXACK_MAX) don't wait indefinitely at
             * low load. At high load batches flush on full long before this. */
            if (g_batch_txack)
                for (int i = 0; i < objs->num_pods; i++)
                    flush_txack_batch(objs, &objs->pods[i]);
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

            objs->sent_msg_cnt = 0;
            objs->recv_msg_cnt = 0;
            last = now;

            /* TRACE: write DPU-side hop latency avg/max to a separate file
             * (overwrite — never grows; independent of -l filter). */
            if (g_trace_on && g_trace_hop_cnt > 0) {
                FILE *tf = fopen(TRACE_FILE, "w");
                if (tf) {
                    fprintf(tf, "dpu_hop_avg_us=%.2f dpu_hop_max_us=%.2f samples=%lu\n",
                            (double)g_trace_hop_sum_ns / g_trace_hop_cnt / 1000.0,
                            (double)g_trace_hop_max_ns / 1000.0,
                            (unsigned long)g_trace_hop_cnt);
                    fclose(tf);
                }
                g_trace_hop_sum_ns = 0;
                g_trace_hop_cnt = 0;
                g_trace_hop_max_ns = 0;
            }

            /* The 1 kHz keepalive above bounds idle DPA wake-up latency;
             * this 1 Hz tick only resets the per-second stat counters and
             * emits the stat line above. */
        }
    }

}
