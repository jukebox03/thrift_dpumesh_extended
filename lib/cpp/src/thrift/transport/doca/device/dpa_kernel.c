#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

/* Spin iterations waiting for DPU consumer to resubmit recv tasks before
 * dropping the descriptor (consumer stalled). */
#define DMA_CONSUMER_EMPTY_WAIT_LOOPS  0x800000

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
        case DPA_MSG_WAKE:
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
            break;
    }
}

/* Returns the number of consumer-completion messages drained this call (WAKE,
 * RING_ADD, REV_RING_ADD). The pre-park re-scan in run_dma_manager uses this:
 * a nonzero return after arming means a signal landed in the arm→reschedule
 * race window, so the EU must NOT park (it would do so with a consumed
 * one-shot notification → lost wakeup). */
static uint32_t handle_msgs(struct dpa_thread_arg *thread_arg)
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

    return num_msgs;
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

        /* Host enforces desc->size <= slot_size (= DPA_DMA_COPY_MAX = 8KB) in
         * dpumesh_enqueue. Anything larger is a caller bug; drop. */
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
                if (++wait >= DMA_CONSUMER_EMPTY_WAIT_LOOPS) { aborted = 1; break; }
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

        /* dma_copy requires 128B-aligned size. A FIN carries desc->size==0
         * (comp.length stays 0 → receiver reads EOF); still issue one min 128B
         * transfer so the DMA engine never sees a zero-length descriptor. */
        uint32_t chunk = ALIGN_UP_128(desc->size);
        if (chunk == 0) chunk = DMA_COPY_SIZE_ALIGN;

        /* PER-CONN CONTIGUOUS STAGING (mirror of the host TX byte-ring). Land each
         * chunk at the SAME offset it occupies in the host TX buffer, so a conn's
         * bytes are contiguous in staging (a mirror of its per-conn TX region) and
         * the L7 parser sees a contiguous per-conn byte stream — no arrival-boundary
         * seam. moff = desc->addr - host_addr is the host TX byte offset; the pod's
         * staging base = dpu_addr - region_off (dpu_addr points at this ring's old
         * EU-shard region). Occupancy mirrors the host TX byte-ring (bounded by
         * tx_w - tx_f <= conn_bytes) so staging never overflows — no ring wrap. The
         * pod staging buffer carries a small tail slack (dpa.c) so a final message's
         * ALIGN_UP_128 rounding cannot write past the buffer end. */
        uint32_t moff = (uint32_t)(desc->addr - ring->host_addr);
        uint64_t staging_base = ring->dpu_addr - (uint64_t)ring->region_off;

        comp.type = DPA_MSG_FWD_DONE;
        comp.pos = moff;                         /* staging offset == host TX offset */
        comp.length = (uint16_t)desc->size;
        /* Endpoint tuple — opaque passthrough from the host-posted desc. src_service
         * is NOT carried (16B budget); the DPU derives it from src_pod. */
        comp.seq = desc->seq;
        comp.src_port = desc->src_port;
        comp.dst_port = desc->dst_port;
        comp.dst_service = desc->dst_service;
        comp.src_pod_id = ring->pod_id;          /* forward: sender = this ring's pod */
        comp.dst_pod_id = desc->dst_pod_id;      /* may be DMESH_POD_BLANK → DPU resolves */
        comp.route_group = desc->route_group;    /* opaque passthrough → ARM dpu_route pins the group */

        doca_dpa_dev_comch_producer_dma_copy(producer,
                                    dpu_consumer_id,
                                    ring->dpu_mmap,
                                    staging_base + moff,
                                    ring->host_mmap,
                                    desc->addr,
                                    chunk,
                                    (uint8_t *)&comp,
                                    sizeof(struct comch_dma_comp_msg),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH |
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);
        /* no pos[r] advance — the staging offset is host-driven (mirror) */

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
 * Drain all valid forward descriptors across all rings.
 * Returns total number of dma_copy calls issued. Producer-side backpressure
 * is handled by the SDK; we only need to ack producer completions periodically
 * (drain_producer_completions throttle below) so the completion queue cycles.
 *
 * Throttled actions inside the inner iter:
 *  - handle_msgs: PCIe-touching consumer-completion poll. Steady-state messages
 *    are only wake triggers and deploy-time ADD_RING, so every-iter polling would
 *    waste EU cycles on empty completions.
 *  - drain_producer_completions: acks producer send completions to free queue slots.
 *
 * The outer run_dma_manager loop calls handle_msgs and drain_producer_completions
 * once per wake, so any setup message / pending completion is not delayed beyond
 * a single yield cycle. */
#define HANDLE_MSGS_EVERY 32
#define DRAIN_COMPLETIONS_EVERY 8

/* poll_msgs: when 0, this scan does NOT touch the consumer-completion queue
 * (no handle_msgs). The pre-park re-scan uses poll_msgs=0 so it cannot silently
 * consume a WAKE that lands in the arm→reschedule window — the caller drains and
 * COUNTS messages explicitly so a racing wake is detected, not lost. */
static int drain_all_rings(struct dpa_thread_arg *thread_arg, int poll_msgs)
{
    int total_dma_calls = 0;
    int found;
    uint32_t iter_counter = 0;

    do {
        found = 0;
        if (poll_msgs && (iter_counter & (HANDLE_MSGS_EVERY - 1)) == 0)
            handle_msgs(thread_arg);
        if ((iter_counter & (DRAIN_COMPLETIONS_EVERY - 1)) == 0)
            drain_producer_completions(thread_arg);
        iter_counter++;

        /* One window invalidation per iteration covers every ring's desc reads:
         * read_inv is a window-wide read fence (__DPA_MMIO, R, R), so the first
         * read of each address this iter is fresh. */
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

        /* Batched writeback: process_fwd_ring only stores desc->valid=0 in DPA
         * cache; this single window-wide fence flushes all of this iteration's
         * frees to host memory at once. Each desc owns its own 64B cache line, so
         * the batched flush never touches a neighbouring slot the host is
         * concurrently filling. */
        if (found)
            __dpa_thread_window_writeback();
    } while (found > 0);

    return total_dma_calls;
}

/* Consecutive empty drains before the EU parks. The EU runs on a dedicated FlexIO
 * EU (zero ARM/host CPU), so it should POLL as continuously as possible for the
 * lowest latency — it parks only on SUSTAINED idle, purely to satisfy the FlexIO
 * watchdog (a never-yielding thread is descheduled after ~120s; measured). Each
 * empty drain is ~µs (a few PCIe desc->valid reads), so this grace window is
 * ~tens-to-hundreds of ms — far longer than any real inter-request gap (so under
 * load the counter resets every request and the EU NEVER parks → no park/WAKE
 * latency on the hot path) yet far shorter than the ~120s watchdog. */
#define IDLE_SPINS_BEFORE_PARK  262144u

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;
    uint32_t idle_spins = 0;

    /* Poll continuously while there is work OR within the grace window; PARK
     * (reschedule) only after IDLE_SPINS_BEFORE_PARK consecutive empty drains.
     * reschedule releases the EU and needs a completion trigger (the ARM's 1 ms
     * DPA_MSG_WAKE) — it does NOT self-sustain (measured). Before parking we
     * RE-ARM both attached completion contexts (DOCA contract: without
     * request_notification a newly-arrived completion is not populated/triggered,
     * so the WAKE would not re-activate the parked EU), then RE-SCAN once — both
     * the rings (closes the silent host desc->valid=1 store race) AND the consumer
     * completion queue. We park ONLY if BOTH come up empty. A WAKE/RING_ADD drained
     * by the re-scan's handle_msgs means a signal landed in the arm→reschedule
     * window: parking then would consume the one-shot notification yet still sleep
     * → lost wakeup → the EU is parked+disarmed and subsequent WAKEs never
     * re-activate it (they fill the consumer queue until recv credits exhaust →
     * permanent wedge needing redeploy). Counting drained messages here turns that
     * into "loop, re-arm next cycle" — the canonical arm→re-poll→don't-sleep-if-
     * found idiom applied to BOTH wake sources. */
    while (1) {
        handle_msgs(thread_arg);
        int chunks = drain_all_rings(thread_arg, 1);
        drain_producer_completions(thread_arg);
        if (chunks > 0) {
            idle_spins = 0;                       /* work found → keep polling HOT */
        } else if (++idle_spins >= IDLE_SPINS_BEFORE_PARK) {
            doca_dpa_dev_comch_consumer_completion_request_notification(thread_arg->dpa_consumer_comp);
            doca_dpa_dev_completion_request_notification(thread_arg->dpa_producer_comp);
            /* Re-scan after arming. handle_msgs() drains+COUNTS the consumer queue
             * so a WAKE/RING_ADD that landed in the arm window is DETECTED; the ring
             * re-scan runs with poll_msgs=0 so it cannot silently consume a WAKE.
             * Park only if BOTH sources came up empty — otherwise loop and re-arm
             * next cycle (never park with a consumed one-shot notification). */
            uint32_t msgs = handle_msgs(thread_arg);
            int rescan = drain_all_rings(thread_arg, 0);   /* rings only (silent host writes) */
            if (msgs == 0 && rescan == 0)
                doca_dpa_dev_thread_reschedule();   /* park; woken by WAKE completion */
            idle_spins = 0;                       /* reset after wake */
        }
        /* else: empty but within grace window → loop again (keep polling) */
    }
}