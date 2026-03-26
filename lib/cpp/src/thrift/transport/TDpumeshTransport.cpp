/*
 * TDpumeshTransport.cpp - Thrift transport over DPUmesh
 */

#include <thrift/transport/TDpumeshTransport.h>
#include <thrift/transport/TTransportException.h>
#include <cstring>
#include <algorithm>
#include <string>
#include <vector>

namespace {
inline bool hasFramedPrefix(const std::vector<uint8_t> &buf) {
    if (buf.size() < 4) {
        return false;
    }
    uint32_t frame_len = (static_cast<uint32_t>(buf[0]) << 24) |
                         (static_cast<uint32_t>(buf[1]) << 16) |
                         (static_cast<uint32_t>(buf[2]) << 8) |
                         static_cast<uint32_t>(buf[3]);
    return frame_len == (buf.size() - 4);
}
}

namespace apache {
namespace thrift {
namespace transport {

TDpumeshTransport::TDpumeshTransport(dpumesh_ctx_t *ctx, const sw_descriptor_t &desc)
    : ctx_(ctx),
      read_buf_(nullptr),
      read_len_(0),
      read_pos_(0),
      read_done_(false),
      rx_slot_(-1),
      flushed_(false),
      tx_slot_(-1),
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
    if (!read_buf_ || read_pos_ >= read_len_) {
        return 0;
    }

    uint32_t avail = read_len_ - read_pos_;
    uint32_t to_read = std::min(len, avail);
    std::memcpy(buf, read_buf_ + read_pos_, to_read);
    read_pos_ += to_read;

    return to_read;
}

void TDpumeshTransport::write(const uint8_t *buf, uint32_t len) {
    write_buf_.insert(write_buf_.end(), buf, buf + len);
}

void TDpumeshTransport::flush() {
    if (write_buf_.empty()) return;

    /* Thrift framed transports may prepend 4-byte frame length (big-endian).
     * We forward the buffer exactly as provided so both framed and raw modes work. */
    const bool framed = hasFramedPrefix(write_buf_);
    const uint32_t payload_len = static_cast<uint32_t>(write_buf_.size());
    (void)framed;

    /* Check slot size limit */
    int slot_size = dpumesh_get_slot_size(ctx_);
    if (static_cast<int>(write_buf_.size()) > slot_size) {
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh write exceeds slot size ("
                                  + std::to_string(write_buf_.size()) + " > "
                                  + std::to_string(slot_size) + ")");
    }

    /* Allocate TX slot */
    int tx_slot = dpumesh_tx_alloc(ctx_);
    if (tx_slot < 0) {
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "DPUmesh TX pool full");
    }

    /* Copy write buffer to TX slot */
    uint8_t *tx_ptr = dpumesh_tx_buf(ctx_, tx_slot);
    if (!tx_ptr) {
        dpumesh_tx_free(ctx_, tx_slot);
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh TX buf null");
    }
    std::memcpy(tx_ptr, write_buf_.data(), write_buf_.size());

    /* Build response descriptor echoing stream_id back */
    sw_descriptor_t desc;
    std::memset(&desc, 0, sizeof(desc));
    desc.header_buf_slot = -1;
    desc.header_len = 0;
    desc.body_buf_slot = tx_slot;
    desc.body_len = payload_len;
    desc.req_id = stream_id_;
    desc.step_id = 0;
    desc.dst_pod_id = src_pod_id_;
    desc.src_pod_id = dpumesh_get_pod_id(ctx_);
    desc.flags = (flags_ & ~OP_REQUEST) | OP_RESPONSE;  /* clear request flag, set response flag */
    desc.valid = 1;
    desc.src_body_pool_type = POOL_HOST_TX_BODY;
    desc.src_body_pod_id = dpumesh_get_pod_id(ctx_);
    desc.src_body_buf_slot = tx_slot;
    desc.src_header_pool_type = POOL_NONE;
    desc.src_header_pod_id = 0;
    desc.src_header_buf_slot = -1;

    if (dpumesh_enqueue(ctx_, &desc) < 0) {
        dpumesh_tx_free(ctx_, tx_slot);
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "DPUmesh TX SQ full");
    }

    tx_slot_ = tx_slot;  /* ownership transferred to ACK-based inflight table */
    write_buf_.clear();
    flushed_ = true;
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
