/*
 * TDpumeshClientTransport.h - Thrift client transport over DPUmesh
 *
 * Replaces TSocket for inter-service calls via DPUmesh.
 * Designed to be wrapped with TFramedTransport for compatibility
 * with servers using TFramedTransportFactory.
 *
 * Usage:
 *   auto dpumesh = std::make_shared<TDpumeshClientTransport>(ctx);
 *   auto framed  = std::make_shared<TFramedTransport>(dpumesh);
 *   auto proto   = std::make_shared<TBinaryProtocol>(framed);
 *   auto client  = new MyServiceClient(proto);
 */

#ifndef _THRIFT_TRANSPORT_TDPUMESHCLIENTTRANSPORT_H_
#define _THRIFT_TRANSPORT_TDPUMESHCLIENTTRANSPORT_H_

#include <thrift/transport/TVirtualTransport.h>
#include <vector>
#include <cstdint>

extern "C" {
#include <thrift/transport/dpumesh.h>
}

namespace apache {
namespace thrift {
namespace transport {

class TDpumeshClientTransport : public TVirtualTransport<TDpumeshClientTransport> {
public:
    /**
     * @param ctx        Shared dpumesh context (must be initialized)
     * @param dst_pod_id Target service pod ID (default 0)
     * @param timeout_ms Response wait timeout in ms (default 30s)
     */
    TDpumeshClientTransport(dpumesh_ctx_t *ctx, int32_t dst_pod_id = 0,
                            int timeout_ms = 30000);

    ~TDpumeshClientTransport() override;

    bool isOpen() override { return ctx_ != nullptr; }
    void open() override {}
    void close() override;

    uint32_t read(uint8_t *buf, uint32_t len);
    void write(const uint8_t *buf, uint32_t len);
    void flush() override;

private:
    void cleanup_response();

    dpumesh_ctx_t *ctx_;
    int32_t dst_pod_id_;
    int timeout_ms_;

    /* Write buffer: accumulates request data before flush */
    std::vector<uint8_t> write_buf_;

    /* Response state: populated after flush, consumed by read */
    uint32_t req_id_;
    uint8_t *read_buf_;
    uint32_t read_len_;
    uint32_t read_pos_;
    int rx_slot_;
    int tx_slot_;  /* TX slot allocated in flush(), freed in cleanup_response() */
    bool response_ready_;
    bool pending_registered_;
};

}  // namespace transport
}  // namespace thrift
}  // namespace apache

#endif  // _THRIFT_TRANSPORT_TDPUMESHCLIENTTRANSPORT_H_
