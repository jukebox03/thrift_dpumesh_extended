#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

/* Spin iterations waiting for DPU consumer to resubmit recv tasks before
 * dropping the descriptor (consumer stalled). */
#define DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS  0x800000

/* Max DMA size for doca_dpa_dev_comch_producer_dma_copy.
 * HW supports up to 8KB per single call with 128B-aligned addresses. */
#define DPA_DMA_COPY_MAX  8192
/* The host RX staging slot is DPUMESH_SLOT_SIZE bytes; each reverse DMA is
 * capped at DPA_DMA_COPY_MAX. If the cap exceeded the canonical slot size a
 * reverse DMA could overflow the host RX slot, so couple them at compile time.
 * (A host configured with a SMALLER slot_size is caught at runtime in
 * process_rx_dma_entry.) */
_Static_assert(DPA_DMA_COPY_MAX <= DPUMESH_SLOT_SIZE,
               "DPA_DMA_COPY_MAX must not exceed DPUMESH_SLOT_SIZE");

/* Alignment requirements for doca_dpa_dev_comch_producer_dma_copy:
 * - Source and destination addresses: 64B aligned
 *   (ensured by CACHE_ALIGN=128 buffer allocation + 128B-aligned offsets)
 * - Transfer size per call: 128B aligned, max 8KB */
#define DMA_COPY_SIZE_ALIGN  128
#define ALIGN_UP_128(x)  (((x) + (DMA_COPY_SIZE_ALIGN - 1)) & ~(uint32_t)(DMA_COPY_SIZE_ALIGN - 1))

/* Max consecutive descriptors drained from one ring per process_*_desc call.
 * Bounds per-ring work so a busy ring can't starve the others within a single
 * drain_all_rings inner iter. */
#define RING_BATCH_CAP  32

/* Forward declarations */
static void drain_producer_completions(struct dpa_thread_arg *thread_arg);

/* Reverse admission accounting — per-EU file-scope globals (fast DPA memory),
 * indexed by [eu_index][ring]. Each EU owns its own row (thread_arg->eu_index)
 * → single-writer per row, no atomics.
 *   dpa_cached_freed[e][r] = host's freed_cumulative cached for EU e, ring r
 *   dpa_sent_count[e][r]   = reverse DMAs EU e has issued for ring r */
uint64_t dpa_cached_freed[MAX_DPA_RINGS][MAX_DPA_RINGS] = {{0}};
uint64_t dpa_sent_count[MAX_DPA_RINGS][MAX_DPA_RINGS] = {{0}};

/* Lazy-refresh margin: refresh credit when computed inflight is within
 * this many slots of the cap. */
#define CREDIT_REFRESH_MARGIN 64

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @num_msg [in]: Number of consumer recv credits to acknowledge
 * @return: always returns 0
 */

__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

	return 0;
}

static void handle_dpu_msg(struct dpa_thread_arg *thread_arg, const struct comch_msg *msg)
{
    switch(msg->type) {
        case DPA_MSG_RING_ADD: {
            struct comch_add_ring_msg *add_msg = (struct comch_add_ring_msg *)msg;
            if (thread_arg->num_rings < MAX_DPA_RINGS) {
                thread_arg->rings[thread_arg->num_rings] = add_msg->ring;
                DOCA_DPA_DEV_LOG_INFO("ADD_RING received: pod_id=%d, buf_arr_size=%u\n",
                                     add_msg->ring.pod_id, add_msg->ring.buf_arr_size);
                thread_arg->num_rings++;
                DOCA_DPA_DEV_LOG_INFO("Added ring: pod_id=%d, num_rings=%u\n",
                                     add_msg->ring.pod_id, thread_arg->num_rings);
            } else {
                DOCA_DPA_DEV_LOG_INFO("Ring add failed: too many rings=%u\n", thread_arg->num_rings);
            }
            break;
        }
        case DPA_MSG_REV_RING_ADD: {
            struct comch_add_rev_ring_msg *add_msg = (struct comch_add_rev_ring_msg *)msg;
            /* Check if ring for this pod_id already exists (update case) */
            int found = 0;
            for (uint32_t ri = 0; ri < thread_arg->num_rev_rings; ri++) {
                if (thread_arg->rev_rings[ri].pod_id == add_msg->ring.pod_id) {
                    thread_arg->rev_rings[ri] = add_msg->ring;
                    DOCA_DPA_DEV_LOG_INFO("ADD_REV_RING updated: pod_id=%d host_mmap=0x%lx\n",
                                         add_msg->ring.pod_id, add_msg->ring.host_mmap);
                    found = 1;
                    break;
                }
            }
            if (!found) {
                if (thread_arg->num_rev_rings < MAX_DPA_RINGS) {
                    thread_arg->rev_rings[thread_arg->num_rev_rings] = add_msg->ring;
                    DOCA_DPA_DEV_LOG_INFO("ADD_REV_RING received: pod_id=%d, buf_arr_size=%u\n",
                                         add_msg->ring.pod_id, add_msg->ring.buf_arr_size);
                    thread_arg->num_rev_rings++;
                } else {
                    DOCA_DPA_DEV_LOG_INFO("Rev ring add failed: too many=%u\n", thread_arg->num_rev_rings);
                }
            }
            break;
        }
        case DPA_MSG_WAKE:
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
            break;
    }
}

static void handle_msgs(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    struct comch_msg *msg;
    uint32_t msg_size;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
    doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    uint32_t num_msgs = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        if (msg == NULL)
            continue;
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
        doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
}

/*
 * Lazy drain: no per-op inflight tracking. The SDK + producer completion queue
 * absorb in-flight ops; we only ack drained completions periodically so the
 * queue does not fill.
 */
static void drain_producer_completions(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_completion_t producer_comp = thread_arg->dpa_producer_comp;
    doca_dpa_dev_completion_element_t elem;
    uint32_t count = 0;

    while (doca_dpa_dev_get_completion(producer_comp, &elem) != 0) {
        count++;
    }

    if (count > 0)
        doca_dpa_dev_completion_ack(producer_comp, count);
}

/*
 * Drain forward descriptors (Host→DPU) from ring r: up to RING_BATCH_CAP
 * consecutive valid descs per call. Returns the number of dma_copy ops issued
 * (0 if ring idle). The window is fresh from the single read_inv in
 * drain_all_rings, so each new desc address reads correctly on first access.
 */
static int process_fwd_ring(struct dpa_thread_arg *thread_arg, uint32_t r)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    struct dpa_ring_info *ring = &thread_arg->rings[r];
    struct comch_dma_comp_msg comp;
    int total_chunks = 0;

    for (int b = 0; b < RING_BATCH_CAP; b++) {
        doca_dpa_dev_buf_t buf =
            doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->desc_idx[r]);
        struct dma_desc *desc =
            (struct dma_desc *)doca_dpa_dev_buf_get_external_ptr(buf);

        if (!desc->valid)
            break;                       /* ring drained */

        /* Host enforces desc->size <= DPUMESH_SLOT_SIZE_DEFAULT (= DPA_DMA_COPY_MAX
         * = 8KB) via check_slot_size(). Anything larger is a caller bug; drop. */
        if (desc->size > DPA_DMA_COPY_MAX) {
            DOCA_DPA_DEV_LOG_INFO("FWD: desc size %u > %u (slot cap); dropping ring=%u slot=%u\n",
                                  desc->size, DPA_DMA_COPY_MAX, r, thread_arg->desc_idx[r]);
            desc->valid = 0;   /* flushed by the batched writeback in drain_all_rings */
            thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
            total_chunks += 1;
            continue;
        }

        /* Wait for DPU consumer recv availability. On timeout the consumer is
         * stalled — clear this desc and stop the batch (no point continuing). */
        int aborted = 0;
        {
            uint32_t wait = 0;
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                if (++wait >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) { aborted = 1; break; }
            }
        }
        if (aborted) {
            DOCA_DPA_DEV_LOG_INFO("FWD: consumer timeout (ring=%u slot=%u). Dropping.\n",
                                  r, thread_arg->desc_idx[r]);
            desc->valid = 0;
            thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
            total_chunks += 1;
            break;
        }

        /* dma_copy requires 128B-aligned size; DPU buffer pos advances by it. */
        uint32_t chunk = ALIGN_UP_128(desc->size);
        if (thread_arg->pos[r] + chunk > ring->dpu_buf_size)
            thread_arg->pos[r] = 0;

        comp.type = DPA_MSG_FWD_DONE;
        comp.pos = ring->region_off + thread_arg->pos[r];  /* absolute (EU-sharding) */
        comp.length = desc->size;
        comp.req_id = (uint32_t)desc->idx;
        comp.src_pod_id = ring->pod_id;
        comp.dst_pod_id = desc->dst_pod_id;
        comp.flags = desc->flags;

        doca_dpa_dev_comch_producer_dma_copy(producer,
                                    dpu_consumer_id,
                                    ring->dpu_mmap,
                                    ring->dpu_addr + thread_arg->pos[r],
                                    ring->host_mmap,
                                    desc->addr,
                                    chunk,
                                    (uint8_t *)&comp,
                                    sizeof(struct comch_dma_comp_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH |
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);

        thread_arg->pos[r] += chunk;
        if (thread_arg->pos[r] >= ring->dpu_buf_size)
            thread_arg->pos[r] = 0;

        /* Flip valid=0; the actual host-visible writeback is batched once per
         * drain_all_rings inner iter. Each desc owns its own 64B cache line, so
         * deferring the flush cannot clobber a neighbour. */
        desc->valid = 0;

        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        total_chunks += 1;
    }

    return total_chunks;
}

/*
 * Drain reverse descriptors (DPU→CPU) from ring r: up to RING_BATCH_CAP
 * consecutive valid descs per call.
 *   Source      = DPU TX buffer (or desc->mmap under in-place forwarding)
 *   Destination = Host RX buffer (host_mmap/host_addr in ring info)
 * Completion is sent to the DPU ARM, which forwards it to the Host.
 * Returns the number of dma_copy ops issued (0 if ring idle).
 */
static int process_rev_ring(struct dpa_thread_arg *thread_arg, uint32_t r)
{
    struct dpa_ring_info *ring = &thread_arg->rev_rings[r];
    /* Same msgq producer as forward direction — DPU ARM receives all
     * completions and forwards to Host via the comch control path. */
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    uint32_t e = thread_arg->eu_index;   /* this EU's row in the per-EU admission globals */
    struct comch_dma_comp_msg comp;
    int total_chunks = 0;

    /* Window is fresh from the single read_inv in drain_all_rings. */
    for (int b = 0; b < RING_BATCH_CAP; b++) {
        doca_dpa_dev_buf_t buf =
            doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->rev_desc_idx[r]);
        struct dma_desc *desc =
            (struct dma_desc *)doca_dpa_dev_buf_get_external_ptr(buf);

        if (!desc->valid)
            break;                       /* ring drained */

        /* Reverse body size inherited from forward DMA, host-capped at 8KB. */
        if (desc->size > DPA_DMA_COPY_MAX) {
            DOCA_DPA_DEV_LOG_INFO("REV: desc size %u > %u (slot cap); dropping ring=%u slot=%u\n",
                                  desc->size, DPA_DMA_COPY_MAX, r, thread_arg->rev_desc_idx[r]);
            desc->valid = 0;   /* flushed by the batched writeback in drain_all_rings */
            thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
            total_chunks += 1;
            continue;
        }

        /* Under in-place forwarding desc->mmap is the SOURCE of reverse DMA
         * (DPU ARM set it to the original sender's local_mmap DPA handle).
         * dpu_enqueue_reverse_dma always sets a nonzero mmap and refuses to
         * post a reverse desc when local_mmap_dpa_handle==0 (dpu_worker.c),
         * so a live reverse desc can never carry mmap==0 — the old
         * ring->dpu_mmap/dpu_addr fallback was unreachable and is removed. */
        doca_dpa_dev_mmap_t src_mmap = desc->mmap;
        uint64_t src_base = desc->addr;

        /* === Admission gate using cached_freed[] (refreshed in drain_all_rings) ===
         * If inflight reverse DMAs >= rq_depth, host's RX RQ is at capacity —
         * stop the batch (desc stays valid, retried next iter after cache refresh). */
        if (ring->host_credit_buf_arr != 0 && ring->rq_depth != 0) {
            if (dpa_sent_count[e][r] - dpa_cached_freed[e][r] >= ring->rq_depth) {
                break;
            }
        }

        /* Wait for DPU consumer availability; on timeout clear + stop batch. */
        int aborted = 0;
        {
            uint32_t wait = 0;
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
                if (++wait >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) { aborted = 1; break; }
            }
        }
        if (aborted) {
            desc->valid = 0;
            thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
            total_chunks += 1;
            break;
        }

        uint32_t chunk = ALIGN_UP_128(desc->size);
        if (thread_arg->rev_pos[r] + chunk > ring->host_buf_size)
            thread_arg->rev_pos[r] = 0;

        /* src_pod_id is the ORIGINAL forward sender (set by DPU on the desc).
         * The reverse ring's ring->pod_id is the RECEIVER, so the source
         * identity must come from the descriptor. */
        comp.type = DPA_MSG_REV_DONE;
        comp.pos = ring->region_off + thread_arg->rev_pos[r];  /* absolute (EU-sharding) */
        comp.length = desc->size;
        comp.req_id = (uint32_t)desc->idx;
        comp.src_pod_id = desc->src_pod_id;
        comp.dst_pod_id = desc->dst_pod_id;
        comp.flags = desc->flags;

        doca_dpa_dev_comch_producer_dma_copy(producer,
                                    dpu_consumer_id,
                                    ring->host_mmap,
                                    ring->host_addr + thread_arg->rev_pos[r],
                                    src_mmap,
                                    src_base,
                                    chunk,
                                    (uint8_t *)&comp,
                                    sizeof(struct comch_dma_comp_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH |
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);

        thread_arg->rev_pos[r] += chunk;
        if (thread_arg->rev_pos[r] >= ring->host_buf_size)
            thread_arg->rev_pos[r] = 0;
        /* Successfully consumed one host RX slot (admission accounting) */
        dpa_sent_count[e][r]++;

        /* valid=0 flushed by the batched writeback in drain_all_rings. */
        desc->valid = 0;

        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        total_chunks += 1;
    }

    return total_chunks;
}

/*
 * Drain all valid descriptors across all rings (both directions).
 * Returns total number of dma_copy calls issued. Producer-side backpressure
 * is handled by the SDK; we only need to ack producer completions periodically
 * (drain_producer_completions throttle below) so the completion queue cycles.
 *
 * Throttled actions inside the inner iter:
 *  - handle_msgs: PCIe-touching consumer-completion poll. Steady-state messages
 *    are only wake triggers and deploy-time ADD_RING / ADD_REV_RING, so every-iter
 *    polling would waste EU cycles on empty completions.
 *  - drain_producer_completions: acks producer send completions to free queue slots.
 *
 * The outer run_dma_manager loop calls handle_msgs and drain_producer_completions
 * once per wake, so any setup message / pending completion is not delayed beyond
 * a single yield cycle. */
#define HANDLE_MSGS_EVERY 32
#define DRAIN_COMPLETIONS_EVERY 8

static int drain_all_rings(struct dpa_thread_arg *thread_arg)
{
    int total_dma_calls = 0;
    int found;
    uint32_t iter_counter = 0;
    uint32_t e = thread_arg->eu_index;   /* this EU's row in the per-EU admission globals */

    do {
        found = 0;
        if ((iter_counter & (HANDLE_MSGS_EVERY - 1)) == 0)
            handle_msgs(thread_arg);
        if ((iter_counter & (DRAIN_COMPLETIONS_EVERY - 1)) == 0)
            drain_producer_completions(thread_arg);
        iter_counter++;

        /* One window invalidation per iteration covers every ring's desc reads
         * plus the credit slot below: read_inv is a window-wide read fence
         * (__DPA_MMIO, R, R), so the first read of each address this iter is
         * fresh. */
        __dpa_thread_window_read_inv();

        /* Forward rings (Host→DPU) */
        uint32_t nr = thread_arg->num_rings;
        for (uint32_t r = 0; r < nr; r++) {
            int chunks = process_fwd_ring(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }

        /* Lazy credit refresh: only PCIe-read the credit slot when computed
         * inflight is approaching the cap. At low/medium load, inflight is
         * tiny relative to rq_depth so this never fires — zero overhead.
         * At cap, refreshes once every ~(rq_depth - MARGIN) sends. The
         * credit slot lives at index DMA_RING_SIZE within each pod's
         * forward buf_arr (host's dma_ring extended by 1 slot). */
        uint32_t nr_rev = thread_arg->num_rev_rings;
        for (uint32_t r = 0; r < nr_rev; r++) {
            struct dpa_ring_info *rev = &thread_arg->rev_rings[r];
            if (rev->host_credit_buf_arr == 0 || rev->rq_depth == 0)
                continue;
            uint64_t inflight = dpa_sent_count[e][r] - dpa_cached_freed[e][r];
            if (inflight + CREDIT_REFRESH_MARGIN < (uint64_t)rev->rq_depth)
                continue;  /* still plenty of headroom — skip PCIe read */
            /* Credit slot sits one past the DMA_RING_SIZE descriptors in the
             * forward dma_ring, so it lives at index DMA_RING_SIZE. */
            doca_dpa_dev_buf_t cbuf =
                doca_dpa_dev_buf_array_get_buf(rev->host_credit_buf_arr,
                                               DMA_RING_SIZE);
            doca_dpa_dev_uintptr_t cptr = doca_dpa_dev_buf_get_external_ptr(cbuf);
            volatile uint64_t *fp = (volatile uint64_t *)cptr;
            dpa_cached_freed[e][r] = *fp;
        }

        /* Reverse rings (DPU→Host) */
        for (uint32_t r = 0; r < nr_rev; r++) {
            int chunks = process_rev_ring(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }

        /* Batched writeback: process_fwd_ring/process_rev_ring only store
         * desc->valid=0 in DPA cache; this single window-wide fence flushes all
         * of this iteration's frees to host memory at once. Each desc owns its
         * own 64B cache line, so the batched flush never touches a neighbouring
         * slot the host is concurrently filling. */
        if (found)
            __dpa_thread_window_writeback();
    } while (found > 0);

    return total_dma_calls;
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    /* Yield when no work: each ring poll is a host-memory PCIe read of
     * desc->valid, so a pure busy-spin would contend with DMA traffic. The
     * yield throttles polling when all rings are idle. */
    while (1) {
        handle_msgs(thread_arg);
        int chunks = drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);
        if (chunks == 0)
            doca_dpa_dev_thread_reschedule();
    }
}