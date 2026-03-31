/*
 * TDpumeshTransportBase.h - Common logic for DPUmesh transports
 */

#ifndef _THRIFT_TRANSPORT_TDPUMESHTRANSPORTBASE_H_
#define _THRIFT_TRANSPORT_TDPUMESHTRANSPORTBASE_H_

#include <thrift/transport/TTransportException.h>
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <string>

extern "C" {
#include <thrift/transport/dpumesh.h>
}

namespace apache {
namespace thrift {
namespace transport {

class TDpumeshTransportBase {
protected:
    dpumesh_ctx_t* ctx_;

    /* Write buffer: accumulates data before flush */
    std::vector<uint8_t> write_buf_;

    /* Read buffer: serves data from RX slot */
    uint8_t* read_buf_;
    uint32_t read_len_;
    uint32_t read_pos_;
    int rx_slot_;

    /* Last allocated TX slot (managed by subclasses) */
    int tx_slot_;

    TDpumeshTransportBase(dpumesh_ctx_t* ctx)
        : ctx_(ctx),
          read_buf_(nullptr),
          read_len_(0),
          read_pos_(0),
          rx_slot_(-1),
          tx_slot_(-1) {}

    virtual ~TDpumeshTransportBase() = default;

    void write_base(const uint8_t* buf, uint32_t len) {
        write_buf_.insert(write_buf_.end(), buf, buf + len);
    }

    uint32_t read_base(uint8_t* buf, uint32_t len) {
        if (!read_buf_ || read_pos_ >= read_len_) {
            return 0;
        }
        uint32_t avail = read_len_ - read_pos_;
        uint32_t to_read = std::min(len, avail);
        std::memcpy(buf, read_buf_ + read_pos_, to_read);
        read_pos_ += to_read;
        return to_read;
    }

    void fill_descriptor_base(sw_descriptor_t& desc, int tx_slot, uint32_t req_id, int32_t dst_pod_id, int8_t flags) {
        std::memset(&desc, 0, sizeof(desc));
        desc.header_buf_slot = -1;
        desc.header_len = 0;
        desc.body_buf_slot = tx_slot;
        desc.body_len = static_cast<uint32_t>(write_buf_.size());
        desc.req_id = req_id;
        desc.step_id = 0;
        desc.dst_pod_id = dst_pod_id;
        desc.src_pod_id = dpumesh_get_pod_id(ctx_);
        desc.flags = flags;
        desc.valid = 1;
        desc.src_body_pool_type = POOL_HOST_TX_BODY;
        desc.src_body_pod_id = dpumesh_get_pod_id(ctx_);
        desc.src_body_buf_slot = tx_slot;
        desc.src_header_pool_type = POOL_NONE;
        desc.src_header_pod_id = 0;
        desc.src_header_buf_slot = -1;
    }

    void check_slot_size(size_t len) {
        int slot_size = dpumesh_get_slot_size(ctx_);
        if (static_cast<int>(len) > slot_size) {
            throw TTransportException(TTransportException::INTERNAL_ERROR,
                                      "DPUmesh write exceeds slot size ("
                                      + std::to_string(len) + " > "
                                      + std::to_string(slot_size) + ")");
        }
    }

    static void throw_exception(const std::string& msg, int err_code = 0) {
        TTransportException::TTransportExceptionType type = TTransportException::INTERNAL_ERROR;
        if (err_code == -1) { /* common error return for dpumesh_init/enqueue/etc */
            type = TTransportException::NOT_OPEN;
        }
        throw TTransportException(type, msg + (err_code != 0 ? " (rc=" + std::to_string(err_code) + ")" : ""));
    }
};

}  // namespace transport
}  // namespace thrift
}  // namespace apache

#endif  // _THRIFT_TRANSPORT_TDPUMESHTRANSPORTBASE_H_
