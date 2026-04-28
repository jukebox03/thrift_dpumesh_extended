/*
 * TDpumeshServerTransport.cpp - Thrift server transport over DPUmesh
 */

#include <thrift/stdcxx.h>
#include <thrift/transport/TDpumeshServerTransport.h>
#include <thrift/transport/TTransportException.h>
#include <cstdio>

namespace apache {
namespace thrift {
namespace transport {

TDpumeshServerTransport::TDpumeshServerTransport(const std::string &app_name, int worker_id)
    : app_name_(app_name),
      worker_id_(worker_id),
      ctx_(nullptr),
      listening_(false) {
    dpumesh_config_t def = DPUMESH_CONFIG_DEFAULT;
    config_ = def;
}

TDpumeshServerTransport::TDpumeshServerTransport(const std::string &app_name, int worker_id,
                                                   const dpumesh_config_t &config)
    : app_name_(app_name),
      worker_id_(worker_id),
      config_(config),
      ctx_(nullptr),
      listening_(false) {
}

TDpumeshServerTransport::~TDpumeshServerTransport() {
    close();
}

void TDpumeshServerTransport::listen() {
    if (ctx_) return;  /* already initialized */

    int rc = dpumesh_init(&ctx_, app_name_.c_str(), worker_id_, &config_);
    if (rc < 0) {
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "Failed to initialize DPUmesh for " + app_name_);
    }
    listening_ = true;
    printf("[TDpumeshServerTransport] listening: app=%s pod_id=%d\n",
           app_name_.c_str(), dpumesh_get_pod_id(ctx_));
}

stdcxx::shared_ptr<TTransport> TDpumeshServerTransport::acceptImpl() {
    if (!ctx_ || !listening_) {
        throw TTransportException(TTransportException::NOT_OPEN,
                                  "DPUmesh server transport not listening");
    }

    sw_descriptor_t desc;
    while (listening_) {
        int rc = dpumesh_dequeue(ctx_, &desc, 1000);  /* 1 second timeout */
        if (rc == 0) {
            /* HOT PATH — no logging here. Per-accept printf was a major
             * throughput bottleneck for TThreadedServer-based services
             * (e.g. unique-id-service): the accept loop is single-threaded,
             * and printf to a pipe (kubectl logs) takes the stdio mutex +
             * blocks on pipe writes, capping accept rate at ~1k/s and
             * defeating the per-request thread parallelism. */
            return stdcxx::make_shared<TDpumeshTransport>(ctx_, desc);
        }
        /* timeout: loop and check listening_ flag */
    }

    throw TTransportException(TTransportException::INTERRUPTED,
                              "DPUmesh server transport interrupted");
}

void TDpumeshServerTransport::close() {
    listening_ = false;
    if (ctx_) {
        dpumesh_destroy(ctx_);
        ctx_ = nullptr;
    }
}

void TDpumeshServerTransport::interrupt() {
    listening_ = false;
}

void TDpumeshServerTransport::interruptChildren() {
    /* TDpumeshTransport instances are self-contained; nothing to interrupt */
}

}  // namespace transport
}  // namespace thrift
}  // namespace apache
