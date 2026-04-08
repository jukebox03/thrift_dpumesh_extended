/*
 * TDpumeshClientTransport.cpp - Thrift client transport over DPUmesh
 */

#include <thrift/transport/TDpumeshClientTransport.h>
#include <cstring>
#include <algorithm>
#include <string>

namespace apache {
namespace thrift {
namespace transport {

TDpumeshClientTransport::TDpumeshClientTransport(dpumesh_ctx_t *ctx,
                                                   int32_t dst_pod_id,
                                                   int timeout_ms)
    : TDpumeshTransportBase(ctx),
      dst_pod_id_(dst_pod_id),
      timeout_ms_(timeout_ms),
      req_id_(0),
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
    write_base(buf, len);
}

void TDpumeshClientTransport::flush() {
    if (write_buf_.empty()) return;

    /* Clean up any previous response state */
    cleanup_response();

    /* Check slot size limit */
    check_slot_size(write_buf_.size());

    /* Allocate unique req_id */
    req_id_ = dpumesh_alloc_req_id(ctx_);

    /* Register pending entry BEFORE enqueue (so response can't beat us) */
    if (dpumesh_register_pending(ctx_, req_id_) < 0) {
        throw_exception("DPUmesh pending slot collision for req_id=" + std::to_string(req_id_));
    }
    pending_registered_ = true;

    /* Allocate TX slot */
    int tx_slot = dpumesh_tx_alloc(ctx_);
    if (tx_slot < 0) {
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw_exception("DPUmesh client TX pool full", -1);
    }

    /* Copy write buffer to TX slot */
    uint8_t *tx_ptr = dpumesh_tx_buf(ctx_, tx_slot);
    if (!tx_ptr) {
        dpumesh_tx_free(ctx_, tx_slot);
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw_exception("DPUmesh client TX buf null");
    }
    std::memcpy(tx_ptr, write_buf_.data(), write_buf_.size());

    /* Build request descriptor */
    sw_descriptor_t desc;
    fill_descriptor_base(desc, tx_slot, req_id_, dst_pod_id_, OP_REQUEST | CASE_INGRESS);

    /* Enqueue to DMA ring */
    if (dpumesh_enqueue(ctx_, &desc) < 0) {
        dpumesh_tx_free(ctx_, tx_slot);
        dpumesh_cancel_pending(ctx_, req_id_);
        pending_registered_ = false;
        throw_exception("DPUmesh client TX SQ full", -1);
    }

    tx_slot_ = tx_slot;  /* freed in cleanup_response() after DMA completes */
    write_buf_.clear();
    /* Response will be fetched on first read() call */
}

/* 
 * Overriding read() because we need to fetch the response if not ready.
 */
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
            throw_exception("DPUmesh client response timeout for req_id=" + std::to_string(req_id_), rc);
        }

        /* Setup read state from response descriptor */
        rx_slot_ = resp.body_buf_slot;
        read_len_ = resp.body_len;
        read_pos_ = 0;

        if (rx_slot_ >= 0 && read_len_ > 0) {
            read_buf_ = dpumesh_rx_buf(ctx_, rx_slot_);
            if (!read_buf_) {
                throw_exception("DPUmesh client RX buf null for slot=" + std::to_string(rx_slot_));
            }
        } else {
            read_buf_ = nullptr;
            read_len_ = 0;
        }

        response_ready_ = true;
    }

    return read_base(buf, len);
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
