/*
 * TDpumeshTransport.cpp - Thrift transport over DPUmesh (server side)
 *
 *   1. read() transparently fetches the next dpumesh request from the
 *      rx_queue once flush() has been called for the current one, so one
 *      runner thread processes many requests sequentially. read() returns 0
 *      when the internal dequeue idles out, ending the runner thread cleanly.
 *
 *   2. write() lazy-allocates a TX slot on first call after each request
 *      boundary and writes directly into it (no write_buf_ vector).
 *
 *   3. flush() uses dpumesh_register_pending + attach_tx + release_async:
 *      TX_ACK frees the slot once the DPU confirms forward DMA consumption.
 */

#include <thrift/transport/TDpumeshTransport.h>
#include <cstring>
#include <string>

namespace apache {
namespace thrift {
namespace transport {

TDpumeshTransport::TDpumeshTransport(dpumesh_ctx_t *ctx, const sw_descriptor_t &desc)
    : TDpumeshTransportBase(ctx),
      flushed_(false),
      shutdown_(false),
      stream_id_(desc.req_id),
      src_pod_id_(desc.src_pod_id),
      flags_(desc.flags),
      tx_buf_ptr_(nullptr),
      tx_pos_(0),
      tx_slot_size_(dpumesh_get_slot_size(ctx)) {
    rx_slot_ = desc.body_buf_slot;
    read_len_ = desc.body_len;
    read_pos_ = 0;

    if (rx_slot_ >= 0 && read_len_ > 0) {
        read_buf_ = dpumesh_rx_buf(ctx_, rx_slot_);
    }
}

TDpumeshTransport::~TDpumeshTransport() {
    close();
}

void TDpumeshTransport::close() {
    if (rx_slot_ >= 0) {
        dpumesh_rx_free(ctx_, rx_slot_);
        rx_slot_ = -1;
    }
    /* If the handler started writing but never flushed, the TX slot is
     * still ours — release it directly (no DPA in flight, since ownership
     * is handed to the pending entry by attach_tx inside flush()). */
    if (tx_slot_ >= 0) {
        dpumesh_tx_free(ctx_, tx_slot_);
        tx_slot_ = -1;
    }
    read_buf_ = nullptr;
    tx_buf_ptr_ = nullptr;
    tx_pos_ = 0;
}

bool TDpumeshTransport::fetch_next_request() {
    /* Free the previous request's RX slot before blocking on dequeue —
     * holding it across the wait would needlessly pin a slot in the pool. */
    if (rx_slot_ >= 0) {
        dpumesh_rx_free(ctx_, rx_slot_);
        rx_slot_ = -1;
        read_buf_ = nullptr;
    }

    sw_descriptor_t desc;
    int rc = dpumesh_dequeue(ctx_, &desc, IDLE_TIMEOUT_MS);
    if (rc != 0) {
        /* Idle timeout — let the runner thread wind down cleanly. */
        shutdown_ = true;
        return false;
    }

    stream_id_  = desc.req_id;
    src_pod_id_ = desc.src_pod_id;
    flags_      = desc.flags;
    rx_slot_    = desc.body_buf_slot;
    read_len_   = desc.body_len;
    read_pos_   = 0;
    if (rx_slot_ >= 0 && read_len_ > 0) {
        read_buf_ = dpumesh_rx_buf(ctx_, rx_slot_);
    } else {
        read_buf_ = nullptr;
    }
    flushed_ = false;
    return true;
}

uint32_t TDpumeshTransport::read(uint8_t *buf, uint32_t len) {
    if (shutdown_) return 0;

    /* Loop because fetch_next_request might land on a 0-length descriptor
     * (defensive — body_len > 0 normally) — fall through to the next one. */
    while (read_pos_ >= read_len_) {
        if (!flushed_) {
            /* Handler has not produced a response for the current request
             * yet, so we cannot move on to the next. Returning 0 here
             * surfaces as TTransportException::END_OF_FILE inside the
             * processor → TConnectedClient::run breaks → thread exits.
             * (Thrift treats this as "client disconnect", which is the
             * right outcome when the handler aborted mid-message.) */
            return 0;
        }
        if (!fetch_next_request()) {
            return 0;
        }
    }
    return read_base(buf, len);
}

void TDpumeshTransport::write(const uint8_t *buf, uint32_t len) {
    /* Lazy-allocate TX slot on first write after each request boundary, then
     * write directly into the slot (no vector temporary). */
    if (tx_slot_ < 0) {
        tx_slot_ = dpumesh_tx_alloc(ctx_);
        if (tx_slot_ < 0) {
            throw_exception("DPUmesh TX pool full", -1);
        }
        tx_buf_ptr_ = dpumesh_tx_buf(ctx_, tx_slot_);
        if (!tx_buf_ptr_) {
            dpumesh_tx_free(ctx_, tx_slot_);
            tx_slot_ = -1;
            throw_exception("DPUmesh TX buf null");
        }
        tx_pos_ = 0;
    }

    if (tx_pos_ + len > static_cast<uint32_t>(tx_slot_size_)) {
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh response exceeds slot size ("
                                  + std::to_string(tx_pos_ + len) + " > "
                                  + std::to_string(tx_slot_size_) + ")");
    }
    std::memcpy(tx_buf_ptr_ + tx_pos_, buf, len);
    tx_pos_ += len;
}

void TDpumeshTransport::flush() {
    if (tx_slot_ < 0) {
        /* Nothing was written — nothing to flush. */
        flushed_ = true;
        return;
    }

    /* Use the pending machinery so TX_ACK frees the TX slot once the DPU
     * confirms forward DMA consumption. */
    if (dpumesh_register_pending(ctx_, stream_id_) < 0) {
        dpumesh_tx_free(ctx_, tx_slot_);
        tx_slot_ = -1;
        tx_buf_ptr_ = nullptr;
        tx_pos_ = 0;
        throw_exception("DPUmesh pending register failed (collision)", -1);
    }

    /* Invariant: attach the TX slot to the pending entry BEFORE enqueue. A
     * TX_ACK can never precede enqueue (the DPU has nothing to consume yet),
     * so attaching first guarantees the TX_ACK handler observes
     * (state == 0 && tx_slot >= 0); attaching after enqueue would race a fast
     * TX_ACK and strand the slot/entry. */
    dpumesh_pending_attach_tx(ctx_, stream_id_, tx_slot_);
    /* Ownership of tx_slot_ now lives in the pending entry — clean up
     * cancel paths via dpumesh_cancel_pending, not direct dpumesh_tx_free. */
    int local_tx_slot = tx_slot_;  /* keep for fill_descriptor_base */
    tx_slot_           = -1;
    tx_buf_ptr_        = nullptr;

    sw_descriptor_t desc;
    fill_descriptor_base(desc, local_tx_slot, stream_id_, src_pod_id_,
                         (flags_ & ~OP_REQUEST) | OP_RESPONSE);
    /* This transport writes directly into the TX slot (no write_buf_ vector),
     * so set the real body length here. */
    desc.body_len = tx_pos_;

    if (dpumesh_enqueue(ctx_, &desc) < 0) {
        /* enqueue failed — pending entry still owns local_tx_slot. cancel
         * frees it (state==0 path inside cancel_pending). */
        dpumesh_cancel_pending(ctx_, stream_id_);
        tx_pos_ = 0;
        throw_exception("DPUmesh TX SQ full", -1);
    }

    /* Mark fire-and-forget so TX_ACK arrival completes the lifecycle
     * without a wait_response. release_async is idempotent w.r.t. an
     * already-arrived TX_ACK (state still 0 + tx_slot already cleared
     * → it transitions state directly to -1). */
    dpumesh_pending_release_async(ctx_, stream_id_);

    tx_pos_  = 0;
    flushed_ = true;
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
