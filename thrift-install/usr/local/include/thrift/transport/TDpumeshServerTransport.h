/*
 * TDpumeshServerTransport.h - Thrift server transport over DPUmesh
 *
 * Replaces TServerSocket: initializes DPUmesh and accepts connections
 * by dequeuing descriptors from the RX queue.
 */

#ifndef _THRIFT_TRANSPORT_TDPUMESHSERVERTRANSPORT_H_
#define _THRIFT_TRANSPORT_TDPUMESHSERVERTRANSPORT_H_

#include <thrift/stdcxx.h>
#include <thrift/transport/TServerTransport.h>
#include <thrift/transport/TDpumeshTransport.h>
#include <string>

extern "C" {
#include <thrift/transport/dpumesh.h>
}

namespace apache {
namespace thrift {
namespace transport {

class TDpumeshServerTransport : public TServerTransport {
public:
    /**
     * @param app_name  Service name (e.g. "unique-id-service")
     * @param worker_id Worker number for pod registration
     * @param config    (second overload only) DPUmesh configuration; zero-valued
     *                  fields fall back to DPUMESH_*_DEFAULT (8192-byte slots,
     *                  2048 slots, 2048 descriptors). The first overload uses
     *                  DPUMESH_CONFIG_DEFAULT.
     */
    TDpumeshServerTransport(const std::string &app_name, int worker_id);
    TDpumeshServerTransport(const std::string &app_name, int worker_id,
                            const dpumesh_config_t &config);

    ~TDpumeshServerTransport() override;

    void listen() override;
    void close() override;
    void interrupt() override;
    void interruptChildren() override;

    int getPort() const { return 0; }  /* no TCP port */

protected:
    stdcxx::shared_ptr<TTransport> acceptImpl() override;

private:
    std::string app_name_;
    int worker_id_;
    dpumesh_config_t config_;
    dpumesh_ctx_t *ctx_;
    volatile bool listening_;
};

}  // namespace transport
}  // namespace thrift
}  // namespace apache

#endif  // _THRIFT_TRANSPORT_TDPUMESHSERVERTRANSPORT_H_
