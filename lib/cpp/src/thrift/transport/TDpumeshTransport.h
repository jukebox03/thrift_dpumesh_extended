/*
 * TDpumeshTransport.h - Thrift transport over DPUmesh
 *
 * Per-request transport: reads from RX buffer, writes to TX buffer.
 * stream_id (req_id) maps request to response via the sidecar.
 */

#ifndef _THRIFT_TRANSPORT_TDPUMESHTRANSPORT_H_
#define _THRIFT_TRANSPORT_TDPUMESHTRANSPORT_H_

#include <thrift/transport/TVirtualTransport.h>
#include <thrift/transport/TDpumeshTransportBase.h>

namespace apache {
namespace thrift {
namespace transport {

class TDpumeshTransport : public TVirtualTransport<TDpumeshTransport>,
                          public TDpumeshTransportBase {
public:
    /**
     * Construct a per-request transport from a dequeued descriptor.
     * Takes ownership of the RX slot (freed on close).
     */
    TDpumeshTransport(dpumesh_ctx_t *ctx, const sw_descriptor_t &desc);

    ~TDpumeshTransport() override;

    bool isOpen() override { return !read_done_ || !flushed_; }
    void open() override {}

    void close() override;

    uint32_t read(uint8_t *buf, uint32_t len);
    void write(const uint8_t *buf, uint32_t len);
    void flush() override;

    uint32_t getStreamId() const { return stream_id_; }
    int32_t getSrcPodId() const { return src_pod_id_; }

private:
    bool read_done_;
    bool flushed_;

    /* Request metadata for response routing */
    uint32_t stream_id_;
    int32_t src_pod_id_;
    int8_t flags_;
};

}  // namespace transport
}  // namespace thrift
}  // namespace apache

#endif  // _THRIFT_TRANSPORT_TDPUMESHTRANSPORT_H_
