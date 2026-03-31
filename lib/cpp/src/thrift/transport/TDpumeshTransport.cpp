/*
 * TDpumeshTransport.cpp - Thrift transport over DPUmesh
 */

#include <thrift/transport/TDpumeshTransport.h>
#include <cstring>
#include <algorithm>
#include <string>
#include <vector>

namespace apache {
namespace thrift {
namespace transport {

TDpumeshTransport::TDpumeshTransport(dpumesh_ctx_t *ctx, const sw_descriptor_t &desc)
    : TDpumeshTransportBase(ctx),
      read_done_(false),
      flushed_(false),
      stream_id_(desc.req_id),
      src_pod_id_(desc.src_pod_id),
      flags_(desc.flags) {
    rx_slot_ = desc.body_buf_slot;
    read_len_ = desc.body_len;

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
    if (tx_slot_ >= 0) {
        /* TX slot is released by DMESH_MSG_TX_ACK handler in dpumesh_doca.c. */
        tx_slot_ = -1;
    }
    read_buf_ = nullptr;
}

uint32_t TDpumeshTransport::read(uint8_t *buf, uint32_t len) {
    return read_base(buf, len);
}

void TDpumeshTransport::write(const uint8_t *buf, uint32_t len) {
    write_base(buf, len);
}

void TDpumeshTransport::flush() {
    if (write_buf_.empty()) return;

    /* Check slot size limit */
    check_slot_size(write_buf_.size());

    /* Allocate TX slot */
    int tx_slot = dpumesh_tx_alloc(ctx_);
    if (tx_slot < 0) {
        throw_exception("DPUmesh TX pool full", -1);
    }

    /* Copy write buffer to TX slot */
    uint8_t *tx_ptr = dpumesh_tx_buf(ctx_, tx_slot);
    if (!tx_ptr) {
        dpumesh_tx_free(ctx_, tx_slot);
        throw_exception("DPUmesh TX buf null");
    }
    std::memcpy(tx_ptr, write_buf_.data(), write_buf_.size());

    /* Build response descriptor echoing stream_id back */
    sw_descriptor_t desc;
    fill_descriptor_base(desc, tx_slot, stream_id_, src_pod_id_, (flags_ & ~OP_REQUEST) | OP_RESPONSE);

    if (dpumesh_enqueue(ctx_, &desc) < 0) {
        dpumesh_tx_free(ctx_, tx_slot);
        throw_exception("DPUmesh TX SQ full", -1);
    }

    tx_slot_ = tx_slot;  /* ownership transferred to ACK-based inflight table */
    write_buf_.clear();
    flushed_ = true;
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
