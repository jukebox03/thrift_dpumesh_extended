/*
 * TDpumeshTransport.h - Thrift transport over DPUmesh (server side)
 *
 * Hands a single dequeued descriptor to a Thrift connected-client thread on
 * construction, then transparently fetches subsequent descriptors from the
 * same thread on each follow-up read once the previous response has been
 * flushed. Lets a runner thread amortise its pthread-create cost across
 * many dpumesh requests, and gives the rx_queue multiple concurrent
 * consumers (one per runner thread) so the single-threaded accept loop is
 * no longer the bottleneck.
 *
 * Write path mirrors the gateway's raw-API layout: the TX slot is
 * lazy-allocated on the first write and written into directly, eliminating
 * the write_buf_ vector + flush memcpy pair. flush() then uses the same
 * register_pending + attach_tx + release_async sequence the gateway uses
 * (pending mechanism owns TX-slot lifetime; TX_ACK frees early).
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
     * Construct from a freshly dequeued descriptor — the runner thread will
     * read this request first, then loop back into read() to fetch more.
     */
    TDpumeshTransport(dpumesh_ctx_t *ctx, const sw_descriptor_t &desc);

    ~TDpumeshTransport() override;

    bool isOpen() override { return ctx_ != nullptr && !shutdown_; }
    void open() override {}

    void close() override;

    uint32_t read(uint8_t *buf, uint32_t len);
    void write(const uint8_t *buf, uint32_t len);
    void flush() override;

    uint32_t getStreamId() const { return stream_id_; }
    int32_t getSrcPodId() const { return src_pod_id_; }

    /* Idle timeout (ms) for the internal dequeue between requests. After
     * this many ms with no incoming work the transport returns 0 from
     * read(), which causes the Thrift processor loop to exit cleanly so
     * the runner thread can wind down. Tunable via env in the future. */
    static constexpr int IDLE_TIMEOUT_MS = 30000;

private:
    /* Free current rx_slot_ + dequeue the next request, repopulating
     * stream_id_/src_pod_id_/flags_/rx_slot_/read_buf_/read_len_/read_pos_.
     * Returns true on success, false on idle timeout / shutdown. */
    bool fetch_next_request();

    bool flushed_;          /* current request already responded? */
    bool shutdown_;         /* internal dequeue has timed out — read() returns 0 */

    /* Request metadata — refreshed by fetch_next_request() between requests. */
    uint32_t stream_id_;
    int32_t  src_pod_id_;
    int8_t   flags_;

    /* Direct-write TX state (no write_buf_ vector). tx_slot_/tx_pos_/
     * tx_buf_ptr_ are lazily initialised on the first write() call after
     * each request boundary; flush() hands ownership to the pending entry
     * and resets these to the unbound state. */
    uint8_t *tx_buf_ptr_;
    uint32_t tx_pos_;
    int      tx_slot_size_;
};

}  // namespace transport
}  // namespace thrift
}  // namespace apache

#endif  // _THRIFT_TRANSPORT_TDPUMESHTRANSPORT_H_
