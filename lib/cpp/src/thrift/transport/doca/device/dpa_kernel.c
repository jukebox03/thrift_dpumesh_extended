#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

#define DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS  0x800000

/* Spin iterations waiting for DPU consumer to resubmit recv tasks */
#define DPA_CONSUMER_WAIT_LOOPS  0x200000

/* Max DMA size for doca_dpa_dev_comch_producer_dma_copy.
 * HW supports up to 8KB per single call with 128B-aligned addresses.
 * Verified via DPUMesh_doca byte-level verification test. */
#define DPA_DMA_COPY_MAX  8192

/* Alignment requirements for doca_dpa_dev_comch_producer_dma_copy:
 * - Source and destination addresses: 64B aligned
 *   (ensured by CACHE_ALIGN=128 buffer allocation + 128B-aligned offsets)
 * - Transfer size per call: 128B aligned, max 8KB */
#define DMA_COPY_SIZE_ALIGN  128
#define ALIGN_UP_128(x)  (((x) + (DMA_COPY_SIZE_ALIGN - 1)) & ~(uint32_t)(DMA_COPY_SIZE_ALIGN - 1))

/* Producer completion queue depth is configured to CC_DPA_MAX_MSG_NUM=1024
 * (see dpa.h + doca_comch_producer_set_dev_max_num_send). We rely on periodic
 * drain (every DRAIN_COMPLETIONS_EVERY inner iters in drain_all_rings) to ack
 * completions and keep the queue cycling. SDK handles producer-side backpressure
 * internally; we no longer track per-op inflight in DPA-local state. */

/* Forward declarations */
static void drain_producer_completions(struct dpa_thread_arg *thread_arg);

/* DPA-local cache of host's freed_cumulative, refreshed lazily.
 * process_one_rev_desc reads from this cache (no PCIe access in reverse
 * path) to avoid the window/cache race seen with reverse-path reads. */
uint64_t dpa_cached_freed[MAX_DPA_RINGS] = {0};

/* Per-ring count of reverse DMAs DPA has issued (admission accounting).
 * Was a static local in process_one_rev_desc; promoted to file scope so
 * drain_all_rings can use it for lazy-refresh decisions. */
uint64_t dpa_sent_count[MAX_DPA_RINGS] = {0};

/* Lazy-refresh margin: refresh credit when computed inflight is within
 * this many slots of the cap. Smaller = fewer PCIe reads but tighter
 * margin; larger = more reads, more headroom. 64 leaves enough buffer
 * to refresh before false-positive defer at the cap. */
#define CREDIT_REFRESH_MARGIN 64

/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
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
        case COMCH_MSG_TYPE_ADD_RING: {
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
        case COMCH_MSG_TYPE_ADD_REV_RING: {
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
        case COMCH_MSG_TYPE_TRIGGER:
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
 * M2-style lazy drain: we do not track per-op inflight here. SDK + producer
 * completion queue (CC_DPA_MAX_MSG_NUM=1024) absorbs the in-flight ops; we
 * just have to keep acking so the queue does not fill. Per wake burst is
 * ~280 dma_copies (§10.1), drain runs every DRAIN_COMPLETIONS_EVERY inner
 * iters in drain_all_rings, and we never let more than that build up.
 *
 * Counter-and-CAP-with-abort pattern (the previous design) was incompatible
 * with the SDK's OPTIMIZE_REPORTS flag: deferred completions caused our
 * inflight counter to look saturated and trigger spurious abort. M2 baseline
 * proves the SDK handles producer-side backpressure internally as long as
 * completions are drained and acked periodically.
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
 * Process one descriptor from the given ring.
 * Returns 1 if a descriptor was processed, 0 if ring was idle.
 */
static int process_one_desc(struct dpa_thread_arg *thread_arg,
                            uint32_t r)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    struct dpa_ring_info *ring = &thread_arg->rings[r];
    struct comch_dma_comp_msg comp;
    doca_dpa_dev_buf_t buf;
    doca_dpa_dev_uintptr_t dev_ptr;
    struct dma_desc *desc;

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    /* Host enforces desc->size <= DPUMESH_SLOT_SIZE_DEFAULT (= DPA_DMA_COPY_MAX
     * = 8KB) via check_slot_size() at enqueue time. Anything larger is a
     * caller bug; drop the slot and surface it in the log. The chunking
     * fallback that used to handle this case was unreachable in practice. */
    if (desc->size > DPA_DMA_COPY_MAX) {
        DOCA_DPA_DEV_LOG_INFO("FWD: desc size %u > %u (slot cap); dropping ring=%u slot=%u\n",
                              desc->size, DPA_DMA_COPY_MAX, r, thread_arg->desc_idx[r]);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    {
        uint32_t empty_wait_loops = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            empty_wait_loops++;
            if (empty_wait_loops >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
                DOCA_DPA_DEV_LOG_INFO("DMA DIAG [Receiver Not Ready]: timeout waiting consumer credits (ring=%u ring_pod=%d slot=%u req_id=%u consumer_id=%u loops=%u). Dropping descriptor.\n",
                                     r,
                                     ring->pod_id,
                                     thread_arg->desc_idx[r],
                                     (uint32_t)desc->idx,
                                     dpu_consumer_id,
                                     empty_wait_loops);
                /* Clear descriptor to unblock ring slot */
                desc->valid = 0;
                __dpa_thread_window_writeback();
                thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
                return 1;
            }
        }
        (void)empty_wait_loops;
    }

    /* dma_copy requires 128B-aligned transfer size; the DPU buffer position
     * advances by the rounded-up amount. */
    uint32_t chunk = ALIGN_UP_128(desc->size);

    /* Wrap around if padded DMA would exceed DPU buffer boundary */
    if (thread_arg->pos[r] + chunk > ring->dpu_buf_size)
        thread_arg->pos[r] = 0;

    /* Build completion message with routing info.
     * Use comch_dma_comp_msg directly instead of comch_msg union to stay
     * within the 32-byte immediate data limit of dma_copy. */
    comp.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    comp.pos = thread_arg->pos[r];
    comp.length = desc->size;
    comp.req_id = (uint32_t)desc->idx;
    comp.src_pod_id = ring->pod_id;
    comp.dst_pod_id = desc->dst_pod_id;
    comp.flags = desc->flags;

    int num_chunks = 0;
    int aborted = 0;
    {
        uint32_t wait = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            if (++wait >= DPA_CONSUMER_WAIT_LOOPS) { aborted = 1; break; }
        }
        if (!aborted) {
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
            num_chunks = 1;
        }
    }

    /* Clear descriptor and advance ring index.
     * On abort: don't advance pos (partial DMA data is abandoned in DPU buffer).
     * No final completion sent, so host will timeout — better than corrupt data.
     * Advance pos by the 128B-aligned chunk size to keep DPU buffer positions
     * aligned for subsequent dma_copy calls. */
    if (!aborted) {
        thread_arg->pos[r] += chunk;
        if (thread_arg->pos[r] >= ring->dpu_buf_size)
            thread_arg->pos[r] = 0;
    }

    /* Only flip valid=0. Host always overwrites mmap/addr/size/idx/dst_pod_id/
     * flags on next post, so pre-clearing them is cosmetic. */
    desc->valid = 0;
    __dpa_thread_window_writeback();

    thread_arg->desc_idx[r] = (thread_arg->desc_idx[r] + 1) % ring->buf_arr_size;
    return num_chunks;  /* count all chunks sent (including before abort) for batch tracking */
}

/* drain_rev_producer_completions removed: reverse DMA completions now use
 * the same msgq producer as forward direction (DPA → DPU ARM). */

/*
 * Process one reverse descriptor (DPU→CPU) from ring r.
 * Source = DPU TX buffer (dpu_mmap/dpu_addr in ring info).
 * Destination = Host RX buffer (host_mmap/host_addr in ring info).
 * Completion sent to Host via rev_dpa_producer.
 * Returns number of dma_copy chunks issued, 0 if ring idle.
 */
static int process_one_rev_desc(struct dpa_thread_arg *thread_arg, uint32_t r)
{
    struct dpa_ring_info *ring = &thread_arg->rev_rings[r];
    /* Use same msgq producer as forward direction — DPU ARM receives all
     * completions and forwards to Host via comch control path. */
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    uint32_t dpu_consumer_id = thread_arg->dpu_consumer_id;
    struct comch_dma_comp_msg comp;
    doca_dpa_dev_buf_t buf;
    doca_dpa_dev_uintptr_t dev_ptr;
    struct dma_desc *desc;

    /* sent_count promoted to file scope as dpa_sent_count so drain_all_rings
     * can use it for lazy credit refresh decisions. */

    buf = doca_dpa_dev_buf_array_get_buf(ring->buf_arr, thread_arg->rev_desc_idx[r]);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    desc = (struct dma_desc *)dev_ptr;

    __dpa_thread_window_read_inv();
    if (!desc->valid)
        return 0;

    /* Reverse body size is inherited from the original forward DMA, which is
     * host-capped at DPA_DMA_COPY_MAX (= 8KB) via check_slot_size(). Larger
     * values are a caller bug; surface and drop. */
    if (desc->size > DPA_DMA_COPY_MAX) {
        DOCA_DPA_DEV_LOG_INFO("REV: desc size %u > %u (slot cap); dropping ring=%u slot=%u\n",
                              desc->size, DPA_DMA_COPY_MAX, r, thread_arg->rev_desc_idx[r]);
        desc->valid = 0;
        __dpa_thread_window_writeback();
        thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
        return 1;
    }

    /* Under in-place forwarding desc->mmap is the SOURCE of reverse DMA
     * (set by DPU ARM in dpu_enqueue_reverse_dma to the original sender's
     * local_mmap DPA handle). Fall back to ring->dpu_mmap if a legacy
     * desc arrives with mmap=0 — keeps the path working during a partial
     * deploy. */
    doca_dpa_dev_mmap_t src_mmap = desc->mmap ? desc->mmap : ring->dpu_mmap;
    uint64_t src_base = desc->mmap ? desc->addr : (ring->dpu_addr + desc->addr);

    /* === Admission gate using cached_freed[] (refreshed in drain_all_rings) ===
     * dpa_sent_count[r] - dpa_cached_freed[r] = inflight reverse DMAs.
     * If >= rq_depth, host's RX RQ is at capacity — defer (return 0; desc
     * stays valid, retry next iter when cache is refreshed). */
    if (ring->host_credit_buf_arr != 0 && ring->rq_depth != 0) {
        if (dpa_sent_count[r] - dpa_cached_freed[r] >= ring->rq_depth) {
            return 0;
        }
    }

    /* Wait for DPU consumer availability */
    {
        uint32_t wait = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            wait++;
            if (wait >= DMA_DIAG_EMPTY_WAIT_FAIL_LOOPS) {
                desc->valid = 0;
                __dpa_thread_window_writeback();
                thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
                return 1;
            }
        }
    }

    uint32_t chunk = ALIGN_UP_128(desc->size);

    /* Wrap around if padded DMA would exceed Host RX buffer boundary */
    if (thread_arg->rev_pos[r] + chunk > ring->host_buf_size)
        thread_arg->rev_pos[r] = 0;

    /* Build completion message for DPU ARM (which forwards to Host).
     * Reverse direction: src=DPU TX buffer, dst=Host RX buffer.
     *
     * src_pod_id is the ORIGINAL forward sender (DPU set it on the desc
     * during dpu_enqueue_reverse_dma). The reverse ring's ring->pod_id
     * is the RECEIVER pod, not the source — using it here would lose
     * the original sender identity, which the receiving host needs for
     * routing. */
    comp.type = COMCH_MSG_TYPE_REV_DMA_COMPLETED;
    comp.pos = thread_arg->rev_pos[r];
    comp.length = desc->size;
    comp.req_id = (uint32_t)desc->idx;
    comp.src_pod_id = desc->src_pod_id;
    comp.dst_pod_id = desc->dst_pod_id;
    comp.flags = desc->flags;

    int num_chunks = 0;
    int aborted = 0;
    {
        uint32_t wait = 0;
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, dpu_consumer_id) == 1) {
            if (++wait >= DPA_CONSUMER_WAIT_LOOPS) { aborted = 1; break; }
        }
        if (!aborted) {
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
            num_chunks = 1;
        }
    }

    if (!aborted) {
        thread_arg->rev_pos[r] += chunk;
        if (thread_arg->rev_pos[r] >= ring->host_buf_size)
            thread_arg->rev_pos[r] = 0;
        /* Successfully consumed one host RX slot (admission accounting) */
        dpa_sent_count[r]++;
    }

    desc->valid = 0;
    __dpa_thread_window_writeback();

    thread_arg->rev_desc_idx[r] = (thread_arg->rev_desc_idx[r] + 1) % ring->buf_arr_size;
    return num_chunks;
}

/*
 * Drain all valid descriptors across all rings (both directions).
 * Returns total number of dma_copy calls issued. Producer-side backpressure
 * is handled by the SDK; we only need to ack producer completions periodically
 * (drain_producer_completions throttle below) so the completion queue cycles.
 *
 * Throttled actions inside the inner iter:
 *  - handle_msgs: PCIe-touching consumer-completion poll. Steady-state messages
 *    are only TRIGGER (~1 kHz) and deploy-time ADD_RING / ADD_REV_RING. Every-iter
 *    polling at ~50K iters/s wastes EU cycles on empty completions.
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

    do {
        found = 0;
        if ((iter_counter & (HANDLE_MSGS_EVERY - 1)) == 0)
            handle_msgs(thread_arg);
        if ((iter_counter & (DRAIN_COMPLETIONS_EVERY - 1)) == 0)
            drain_producer_completions(thread_arg);
        iter_counter++;

        /* Forward rings (CPU→DPU) */
        uint32_t nr = thread_arg->num_rings;
        for (uint32_t r = 0; r < nr; r++) {
            int chunks = process_one_desc(thread_arg, r);
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
            uint64_t inflight = dpa_sent_count[r] - dpa_cached_freed[r];
            if (inflight + CREDIT_REFRESH_MARGIN < (uint64_t)rev->rq_depth)
                continue;  /* still plenty of headroom — skip PCIe read */
            /* Credit slot is at the END of forward dma_ring (one past
             * DMA_RING_SIZE descriptors). buf_arr_size is forward ring's
             * buf_arr size; index = forward ring count. The forward
             * ring_info_buf_arr_size used in process_one_desc is its own
             * (DMA_RING_SIZE), so credit at index = DMA_RING_SIZE. */
            doca_dpa_dev_buf_t cbuf =
                doca_dpa_dev_buf_array_get_buf(rev->host_credit_buf_arr,
                                               /* slot index */ DMA_RING_SIZE);
            doca_dpa_dev_uintptr_t cptr = doca_dpa_dev_buf_get_external_ptr(cbuf);
            volatile uint64_t *fp = (volatile uint64_t *)cptr;
            __dpa_thread_window_read_inv();
            dpa_cached_freed[r] = *fp;
        }

        /* Reverse rings (DPU→CPU) */
        for (uint32_t r = 0; r < nr_rev; r++) {
            int chunks = process_one_rev_desc(thread_arg, r);
            if (chunks > 0) {
                found++;
                total_dma_calls += chunks;
            }
        }
    } while (found > 0);

    return total_dma_calls;
}

__dpa_global__ void hello_world(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    DOCA_DPA_DEV_LOG_INFO("hello_world: num_rings=%u\n", thread_arg->num_rings);

    doca_dpa_dev_thread_reschedule();
}

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;

    /* Yield when no work. Empirically necessary even though dpumesh has
     * the same busy-spin freedom as M2 baseline. Why: dpumesh DPA polls
     * num_pods × 2 (forward + reverse) rings each iter, each ring poll is
     * a host-memory PCIe read of desc->valid. Removing the yield caused
     * 55K p99 to jump 91 → 224 ms (2.5×) — most plausibly because the
     * busy-spin's PCIe polling rate contended with actual DMA traffic.
     * The 1.37 ms idle gap from yield is acting as a throttle, not as
     * pure wake latency. (M2 has only 1 ring, so busy-spin is fine there.) */
    while (1) {
        handle_msgs(thread_arg);
        int chunks = drain_all_rings(thread_arg);
        drain_producer_completions(thread_arg);
        if (chunks == 0)
            doca_dpa_dev_thread_reschedule();
    }
}