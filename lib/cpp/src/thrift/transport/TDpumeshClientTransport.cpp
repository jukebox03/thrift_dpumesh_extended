/*
 * TDpumeshClientTransport.cpp - Thrift client transport over DPUmesh
 */

#include <thrift/transport/TDpumeshClientTransport.h>
#include <thrift/transport/TTransportException.h>
#include <cstring>
#include <algorithm>
#include <string>

namespace apache {
namespace thrift {
namespace transport {

TDpumeshClientTransport::TDpumeshClientTransport(dpumesh_ctx_t *ctx,
                                                   int32_t dst_pod_id,
                                                   int timeout_ms)
    : ctx_(ctx),
      dst_pod_id_(dst_pod_id),
      timeout_ms_(timeout_ms),
      req_id_(0),
      read_buf_(nullptr),
      read_len_(0),
      read_pos_(0),
      rx_slot_(-1),
      tx_slot_(-1),
      response_ready_(false),
      pending_registered_(false) {
}

TDpumeshClientTransport::~TDpumeshClientTransport() {
    close();
}

void TDpumeshClientTransport::cleanup_response() {
    if (rx_slot_ >= 0) {
        dpumesh_rx_free(ctx_, rx_slot_);
        rx_slot_ = -1;
    }
    if (tx_slot_ >= 0) {
        dpumesh_tx_free(ctx_, tx_slot_);
        tx_slot_ = -1;
    }
    read_buf_ = nullptr;
    read_len_ = 0;
    read_pos_ = 0;
    response_ready_ = false;
}

void TDpumeshClientTransport::close() {
    cleanup_response();

    if (pending_registered_) {
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
    }
}

void TDpumeshClientTransport::write(const uint8_t *buf, uint32_t len) {
    write_buf_.insert(write_buf_.end(), buf, buf + len);
}

void TDpumeshClientTransport::flush() {
    if (write_buf_.empty()) return;

    /* Clean up any previous response state */
    cleanup_response();

    /* Check slot size limit */
    int slot_size = dpumesh_get_slot_size(ctx_);
    if (static_cast<int>(write_buf_.size()) > slot_size) {
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh client write exceeds slot size ("
                                  + std::to_string(write_buf_.size()) + " > "
                                  + std::to_string(slot_size) + ")");
    }

    /* Allocate unique req_id */
    req_id_ = dpumesh_alloc_req_id(ctx_);

    /* Register pending entry BEFORE enqueue (so response can't beat us) */
    if (dpumesh_register_pending(ctx_, req_id_) < 0) {
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh pending slot collision for req_id="
                                  + std::to_string(req_id_));
    }
    pending_registered_ = true;

    /* Allocate TX slot */
    int tx_slot = dpumesh_tx_alloc(ctx_);
    if (tx_slot < 0) {
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "DPUmesh client TX pool full");
    }

    /* Copy write buffer to TX slot */
    uint8_t *tx_ptr = dpumesh_tx_buf(ctx_, tx_slot);
    if (!tx_ptr) {
        dpumesh_tx_free(ctx_, tx_slot);
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw TTransportException(TTransportException::INTERNAL_ERROR,
                                  "DPUmesh client TX buf null");
    }
    std::memcpy(tx_ptr, write_buf_.data(), write_buf_.size());

    /* Build request descriptor */
    sw_descriptor_t desc;
    std::memset(&desc, 0, sizeof(desc));
    desc.header_buf_slot = -1;
    desc.header_len = 0;
    desc.body_buf_slot = tx_slot;
    desc.body_len = static_cast<uint32_t>(write_buf_.size());
    desc.req_id = req_id_;
    desc.step_id = 0;
    desc.dst_pod_id = dst_pod_id_;
    desc.src_pod_id = dpumesh_get_pod_id(ctx_);
    desc.flags = OP_REQUEST | CASE_INGRESS;
    desc.valid = 1;
    desc.src_body_pool_type = POOL_HOST_TX_BODY;
    desc.src_body_pod_id = dpumesh_get_pod_id(ctx_);
    desc.src_body_buf_slot = tx_slot;
    desc.src_header_pool_type = POOL_NONE;
    desc.src_header_pod_id = 0;
    desc.src_header_buf_slot = -1;

    /* Enqueue to DMA ring */
    if (dpumesh_enqueue(ctx_, &desc) < 0) {
        dpumesh_tx_free(ctx_, tx_slot);
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "DPUmesh client TX SQ full");
    }

    tx_slot_ = tx_slot;  /* freed in cleanup_response() after DMA completes */
    write_buf_.clear();
    /* Response will be fetched on first read() call */
}

uint32_t TDpumeshClientTransport::read(uint8_t *buf, uint32_t len) {
    if (!response_ready_) {
        if (!pending_registered_) {
            return 0;  /* No request in flight */
        }

        /* Wait for response matching our req_id */
        sw_descriptor_t resp;
        int rc = dpumesh_wait_response(ctx_, req_id_, &resp, timeout_ms_);
        pending_registered_ = false;

        if (rc != 0) {
            throw TTransportException(TTransportException::TIMED_OUT,
                                      "DPUmesh client response timeout for req_id="
                                      + std::to_string(req_id_));
        }

        /* Setup read state from response descriptor */
        rx_slot_ = resp.body_buf_slot;
        read_len_ = resp.body_len;
        read_pos_ = 0;

        if (rx_slot_ >= 0 && read_len_ > 0) {
            read_buf_ = dpumesh_rx_buf(ctx_, rx_slot_);
            if (!read_buf_) {
                throw TTransportException(TTransportException::INTERNAL_ERROR,
                                          "DPUmesh client RX buf null for slot="
                                          + std::to_string(rx_slot_));
            }
        } else {
            read_buf_ = nullptr;
            read_len_ = 0;
        }

        response_ready_ = true;
    }

    /* Serve data from response buffer */
    if (!read_buf_ || read_pos_ >= read_len_) {
        return 0;
    }

    uint32_t avail = read_len_ - read_pos_;
    uint32_t to_read = std::min(len, avail);
    std::memcpy(buf, read_buf_ + read_pos_, to_read);
    read_pos_ += to_read;

    return to_read;
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
