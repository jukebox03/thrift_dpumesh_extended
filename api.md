# DPUmesh API — Whitepaper

A service-mesh **data plane** built on NVIDIA DOCA (Comch + DMA). The transport
runs on the **BlueField DPU/DPA** instead of host CPU, so the application keeps
its full host core (no in-host sidecar tax). Two pods exchange messages
**host → DPU → host**: the DPU routes on `dst_pod_id` (metadata only, never reads
the body) and the DPA EU performs the DMA copies.

This document is the complete API reference for the current code
(`lib/cpp/src/thrift/transport/`). It covers two layers:

- **Layer 1 — Low-level C API** (`dpumesh.h`): zero-copy slot/descriptor API. Use directly for custom clients/servers.
- **Layer 2 — Thrift C++ transports**: drop-in replacements for `TServerSocket` / `TSocket`.

---

## 1. Model in one paragraph

Each node owns two fixed **slot pools** (TX and RX), `num_slots × slot_size`
bytes each. To send: allocate a TX slot, write the body into it (zero-copy),
fill a `sw_descriptor_t`, and `enqueue`. The DPA EU DMAs the body to the
destination's RX pool and the destination `dequeue`s a descriptor pointing at an
RX slot. Requests carry a `req_id`; clients match responses to requests via the
**pending** API (blocking `wait_response` or async `poll_response`). Slot-based
admission bounds in-flight bytes: `num_slots × slot_size` **must equal**
`DPU_BUFFER_SIZE` (32 MB = 4096 × 8 KB).

---

## 2. Data types & constants

### `dpumesh_config_t`
| Field | Type | Meaning | 0 / default |
|---|---|---|---|
| `num_slots` | `int` | Slots per pool (TX and RX each) | 0 → env `DPUMESH_NUM_SLOTS` → **4096** |
| `slot_size` | `int` | Bytes per slot (max body per message) | 0 → env `DPUMESH_SLOT_SIZE` → **8192** |
| `max_descriptors` | `int` | Descriptor ring capacity | 0 → env `DPUMESH_MAX_DESCRIPTORS` → **2048** |
| `poll_rx` | `int` | `1` = `dequeue` spin-polls the RX ring (no cond wakeup). For echo/server pools. | `0` = cond-wait |
| `async_client` | `int` | `1` = response delivered via `poll_response` (PE thread does **not** signal a per-request cond). For async clients. | `0` = `wait_response` |

`#define DPUMESH_CONFIG_DEFAULT { 0, 0, 0, 0, 0 }` — pass `NULL` to `dpumesh_init` for the same effect.

**Precedence for sizing fields:** explicit `config` field (if `> 0`) → environment variable → compile-time default.

### `sw_descriptor_t` (packed)
| Field | Type | Meaning |
|---|---|---|
| `header_buf_slot` | `int32_t` | Header slot; **always `-1`** (Thrift has no separate header) |
| `header_len` | `uint32_t` | **always `0`** |
| `body_buf_slot` | `int32_t` | TX slot (send) / RX slot (receive) holding the body |
| `body_len` | `uint32_t` | Body length in bytes (**≤ `slot_size`**) |
| `req_id` | `uint32_t` | Request/stream id (for response matching) |
| `dst_pod_id` | `int32_t` | Destination pod id `[0,127]` |
| `src_pod_id` | `int32_t` | Source pod id (set to `dpumesh_get_pod_id(ctx)`) |
| `flags` | `int8_t` | `OpFlag | CaseFlag` (see below) |
| `valid` | `int8_t` | Must be `1` for a live descriptor |

### Flag constants (`dpumesh_common.h`)
| Constant | Value | Meaning |
|---|---|---|
| `OP_REQUEST` | `0x00` | Request direction (OR into `flags`) |
| `OP_RESPONSE` | `0x10` | Response direction |
| `CASE_EXTERNAL` | `1` | Client-originated traffic |
| `CASE_INGRESS` | `2` | DPU-injected ingress |

Compose: request = `OP_REQUEST | CASE_EXTERNAL`; reply = `(req.flags & ~OP_REQUEST) | OP_RESPONSE`.

### Limits / defaults
| Constant | Value |
|---|---|
| `DPUMESH_SLOT_SIZE_DEFAULT` | `8192` (8 KB; also the per-`dma_copy` HW max) |
| `DPUMESH_NUM_SLOTS_DEFAULT` | `4096` |
| `DPUMESH_MAX_DESCRIPTORS_DEFAULT` | `2048` |
| `MAX_PODS` | `8` (pod_id wire range `[0,127]`) |
| `DPU_BUFFER_SIZE` | `32 MB` (= `4096 × 8192`; the `num_slots × slot_size` invariant) |

---

## 3. Layer 1 — C API reference (`dpumesh.h`)

Opaque handle: `typedef struct dpumesh_ctx dpumesh_ctx_t;`. All `tx`/`rx`/`pending`
calls are internally locked and thread-safe.

### Lifecycle
**`int dpumesh_init(dpumesh_ctx_t **ctx, const char *app_name, int worker_id, const dpumesh_config_t *config)`**
| Param | Dir | Meaning |
|---|---|---|
| `ctx` | out | Receives the allocated context pointer |
| `app_name` | in | Service name (used in the worker-id string and pod registration) |
| `worker_id` | in | Worker number; becomes this node's `pod_id` unless env `DPUMESH_POD_ID` overrides |
| `config` | in | Configuration, or `NULL` for all defaults |
| **Returns** | | `0` on success (`*ctx` set); non-zero on failure |

**`void dpumesh_destroy(dpumesh_ctx_t *ctx)`** — Tear down the context and release all DOCA resources.

### Query / info
| Signature | Returns |
|---|---|
| `int dpumesh_get_slot_size(dpumesh_ctx_t *ctx)` | Configured `slot_size` (bytes) — the max body per message |
| `int dpumesh_get_pod_id(dpumesh_ctx_t *ctx)` | This node's `pod_id` |
| `const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx)` | Worker-id string `"<app_name>-worker-<n>"` (lifetime = `ctx`) |

### Raw buffer / queue API
**`int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms)`** — Receive one descriptor.
| Param | Dir | Meaning |
|---|---|---|
| `desc` | out | Filled on success; `desc->body_buf_slot` is an **RX slot the caller must `rx_free`** after reading |
| `timeout_ms` | in | `-1` = block forever, `0` = non-blocking, `>0` = milliseconds |
| **Returns** | | `0` on success; `-1` on timeout/error |

| Signature | Param / Returns |
|---|---|
| `uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot)` | Pointer to RX slot body (zero-copy read); `NULL` if invalid |
| `void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot)` | Release an RX slot after reading |
| `int dpumesh_tx_alloc(dpumesh_ctx_t *ctx)` | Allocate a TX slot → slot index `≥0`, or **`-1` = pool exhausted (backpressure, not an error)** |
| `uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot)` | Pointer to TX slot body (zero-copy write up to `slot_size`) |
| `void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot)` | Free a TX slot on the **error path** (before ownership is handed to `attach_tx`) |
| `int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc)` | Submit a filled descriptor to TX → `0` success, `-1` failure |

### Client / responder request-response API
| Signature | Params / Returns |
|---|---|
| `uint32_t dpumesh_alloc_req_id(dpumesh_ctx_t *ctx)` | Atomic, thread-safe unique request id |
| `int dpumesh_register_pending(dpumesh_ctx_t *ctx, uint32_t req_id)` | Register a pending entry **before** `enqueue` → `0` / `-1` |
| `void dpumesh_pending_attach_tx(dpumesh_ctx_t *ctx, uint32_t req_id, int tx_slot)` | Bind the TX slot to the pending entry **after a successful `enqueue`**; TX-slot lifetime now owned by the pending/`TX_ACK` path |
| `void dpumesh_cancel_pending(dpumesh_ctx_t *ctx, uint32_t req_id)` | Cancel a pending entry (error path); defers TX cleanup if the DPA may still be using the slot |

**`int dpumesh_wait_response(dpumesh_ctx_t *ctx, uint32_t req_id, sw_descriptor_t *resp, int timeout_ms)`** — Blocking response wait (blocking-client model).
| Param | Dir | Meaning |
|---|---|---|
| `resp` | out | Response descriptor on success; caller must `rx_free(resp->body_buf_slot)` |
| `timeout_ms` | in | `-1` forever, `0` non-blocking, `>0` ms |
| **Returns** | | `0` on success (`resp` filled); `-1` on timeout |

**`int dpumesh_poll_response(dpumesh_ctx_t *ctx, uint32_t req_id, sw_descriptor_t *resp)`** — Non-blocking poll (async-client model).
| Returns | Meaning |
|---|---|
| `0` | Response arrived — `resp` filled, **TX already freed**, caller must `rx_free(resp->body_buf_slot)` |
| `1` | Not ready — poll again later |
| `-1` | Error / abandoned (no live pending for `req_id`) |

> A client process must use **either** `wait_response` (set `async_client=0`) **or** `poll_response` (set `async_client=1`) consistently — never both.

**`void dpumesh_pending_release_async(dpumesh_ctx_t *ctx, uint32_t req_id)`** — Responder-side fire-and-forget release after `enqueue`+`attach_tx` when **no response is expected** (e.g. a server sending `OP_RESPONSE`). Idempotent; safe to race with `TX_ACK`. Acts only on the unmodified state:
- TX still attached → mark for release; `TX_ACK` frees the slot and clears the entry.
- TX already freed by an earlier `TX_ACK` → clear immediately (slot reusable).
- any other state → no-op.

---

## 4. Ownership & lifecycle rules (correctness-critical)

**Send (request or response):**
```
req_id = alloc_req_id(ctx)                 // client only; responder reuses req->req_id
tx = tx_alloc(ctx)                         // -1 → backpressure, retry later
write into tx_buf(ctx, tx)                 // ≤ slot_size bytes
register_pending(ctx, req_id)              // BEFORE enqueue; -1 → tx_free + fail
fill sw_descriptor_t (valid=1, slots/ids/flags)
enqueue(ctx, &desc)                        // -1 → tx_free + cancel_pending + fail
attach_tx(ctx, req_id, tx)                 // success → pending owns the TX slot
   client : wait_response | poll_response  // frees TX on completion
   responder: pending_release_async        // TX_ACK frees TX (fire-and-forget)
```
- After `attach_tx`, **never** call `tx_free` — `TX_ACK` (or `cancel_pending`'s deferred path) owns it.
- `tx_free` is only for the error path **before** `attach_tx`.

**Receive:** `dequeue` (or a response from `wait`/`poll`) yields `body_buf_slot`; read via `rx_buf`; **always** `rx_free` it.

---

## 5. Layer 2 — Thrift C++ transports

Drop-in for the Apache Thrift transport stack. All in `apache::thrift::transport`.

### `TDpumeshServerTransport` — replaces `TServerSocket`
```cpp
TDpumeshServerTransport(const std::string& app_name, int worker_id);
TDpumeshServerTransport(const std::string& app_name, int worker_id,
                        const dpumesh_config_t& config);
```
| Param | Meaning |
|---|---|
| `app_name` | Service name (pod registration) |
| `worker_id` | Worker number → pod id |
| `config` | (2nd overload) sizing/mode; zero fields fall back to defaults. 1st overload uses `DPUMESH_CONFIG_DEFAULT`. |

Methods (override `TServerTransport`): `listen()`, `close()`, `interrupt()`, `interruptChildren()`, `getPort()` → `0` (no TCP port), `acceptImpl()` → a `TDpumeshTransport` per dequeued request.

### `TDpumeshClientTransport` — replaces `TSocket`
```cpp
TDpumeshClientTransport(dpumesh_ctx_t* ctx, int32_t dst_pod_id = 0,
                        int timeout_ms = 30000);
```
| Param | Default | Meaning |
|---|---|---|
| `ctx` | — | Shared, already-initialized context |
| `dst_pod_id` | `0` | Target service pod id |
| `timeout_ms` | `30000` | Per-call response timeout (ms) |

Methods: `read`, `write`, `flush`, `isOpen` (true while `ctx != nullptr`), `open` (no-op), `close`. **Wrap in `TFramedTransport`.**

### `TDpumeshTransport` — per-connection server-side transport
`TDpumeshTransport(dpumesh_ctx_t* ctx, const sw_descriptor_t& desc)` — constructed by `acceptImpl()` from a freshly dequeued request; one runner thread serves many requests (loops back into `read()` to fetch the next). `static constexpr int IDLE_TIMEOUT_MS = 30000;` — after this idle gap `read()` returns 0 so the processor loop exits cleanly.

---

## 6. Environment variables

**Host library** (`dpumesh_init`), precedence `config field (>0) → env → default`:
| Variable | Effect | Default |
|---|---|---|
| `DPUMESH_PCI_ADDR` | DOCA device PCI address | `94:00.0` |
| `DPUMESH_POD_ID` | Override pod id (else = `worker_id`) | `worker_id` |
| `DPUMESH_NUM_SLOTS` | Slots per pool | `4096` |
| `DPUMESH_SLOT_SIZE` | Bytes per slot | `8192` |
| `DPUMESH_MAX_DESCRIPTORS` | Descriptor ring capacity | `2048` |
| `DPUMESH_RINGS_PER_POD` | `K` = EU-sharding rings/pod; **must match the DPU process** | `1` (clamped ≤ 8) |
| `DPUMESH_HOST_EPOLL` | `1` = host PE-progress thread sleeps on epoll; `0` = busy-poll | `0` (test-bench sets `1`) |

**DPU process** (set when launching `dpumesh_dpu`, not the host app): `DPUMESH_DPA_THREADS` (EU count, default 1; test-bench 4), `DPUMESH_RINGS_PER_POD` (K, must match host), `DPUMESH_EVENT_LOOP` (1 = epoll-at-idle ARM loop, default).

---

## 7. Examples

### 7a. Client — async (poll) model
```c
dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
cfg.async_client = 1;                        // use poll_response, not wait_response
dpumesh_ctx_t *ctx;
if (dpumesh_init(&ctx, "bench", /*worker_id*/10, &cfg) != 0) return 1;
int my_pod = dpumesh_get_pod_id(ctx);

// --- send ---
uint32_t req_id = dpumesh_alloc_req_id(ctx);
int tx = dpumesh_tx_alloc(ctx);              // -1 => backpressure, retry later
if (tx < 0) { /* retry */ }
uint8_t *buf = dpumesh_tx_buf(ctx, tx);
memcpy(buf, payload, payload_len);           // <= slot_size

if (dpumesh_register_pending(ctx, req_id) < 0) { dpumesh_tx_free(ctx, tx); /* fail */ }

sw_descriptor_t d; memset(&d, 0, sizeof d);
d.header_buf_slot = -1;
d.body_buf_slot   = tx;
d.body_len        = payload_len;
d.req_id          = req_id;
d.dst_pod_id      = 11;                       // target service
d.src_pod_id      = my_pod;
d.flags           = OP_REQUEST | CASE_EXTERNAL;
d.valid           = 1;
if (dpumesh_enqueue(ctx, &d) < 0) { dpumesh_tx_free(ctx, tx); dpumesh_cancel_pending(ctx, req_id); /* fail */ }
dpumesh_pending_attach_tx(ctx, req_id, tx);   // pending now owns the TX slot

// --- harvest (non-blocking) ---
sw_descriptor_t resp;
for (;;) {
    int r = dpumesh_poll_response(ctx, req_id, &resp);
    if (r == 1) { /* not ready: do other work, or time out */ continue; }
    if (r == 0) {
        const uint8_t *rb = dpumesh_rx_buf(ctx, resp.body_buf_slot);
        /* use rb[0..resp.body_len-1] */
        dpumesh_rx_free(ctx, resp.body_buf_slot);   // TX already freed by the lib
    } else { /* r == -1: abandoned */ dpumesh_cancel_pending(ctx, req_id); }
    break;
}
```
*(Blocking model: set `cfg.async_client = 0` and replace the harvest loop with
`dpumesh_wait_response(ctx, req_id, &resp, /*timeout_ms*/30000)` → `0`/`-1`,
then `rx_free(resp.body_buf_slot)`.)*

### 7b. Server / echo — recv → reply
```c
dpumesh_config_t cfg = DPUMESH_CONFIG_DEFAULT;
cfg.poll_rx = 1;                              // lean RX: spin-poll, no per-req cond
dpumesh_ctx_t *ctx;
dpumesh_init(&ctx, "echo", /*worker_id*/11, &cfg);

for (;;) {
    sw_descriptor_t req;
    if (dpumesh_dequeue(ctx, &req, -1) < 0 || !req.valid) continue;

    const uint8_t *in = dpumesh_rx_buf(ctx, req.body_buf_slot);
    int tx = dpumesh_tx_alloc(ctx);
    if (tx < 0) { dpumesh_rx_free(ctx, req.body_buf_slot); continue; }
    uint8_t *out = dpumesh_tx_buf(ctx, tx);
    memcpy(out, in, req.body_len);            // produce the response body
    dpumesh_rx_free(ctx, req.body_buf_slot);  // done with RX

    if (dpumesh_register_pending(ctx, req.req_id) < 0) { dpumesh_tx_free(ctx, tx); continue; }
    dpumesh_pending_attach_tx(ctx, req.req_id, tx);

    sw_descriptor_t resp; memset(&resp, 0, sizeof resp);
    resp.header_buf_slot = -1;
    resp.body_buf_slot   = tx;
    resp.body_len        = req.body_len;
    resp.req_id          = req.req_id;
    resp.dst_pod_id      = req.src_pod_id;                       // back to sender
    resp.src_pod_id      = dpumesh_get_pod_id(ctx);
    resp.flags           = (req.flags & ~OP_REQUEST) | OP_RESPONSE;
    resp.valid           = 1;
    if (dpumesh_enqueue(ctx, &resp) < 0) { dpumesh_cancel_pending(ctx, req.req_id); continue; }
    dpumesh_pending_release_async(ctx, req.req_id);             // TX_ACK frees the slot
}
```

### 7c. Thrift server (replaces `TServerSocket`)
```cpp
auto transport = std::make_shared<TDpumeshServerTransport>("unique-id-service", /*worker_id*/11);
auto tf = std::make_shared<TFramedTransportFactory>();
auto pf = std::make_shared<TBinaryProtocolFactory>();
TThreadedServer server(processor, transport, tf, pf);   // handler/processor unchanged
server.serve();
```

### 7d. Thrift client (replaces `TSocket`)
```cpp
auto dpumesh = std::make_shared<TDpumeshClientTransport>(ctx, /*dst_pod_id*/11, /*timeout_ms*/30000);
auto framed  = std::make_shared<TFramedTransport>(dpumesh);
auto proto   = std::make_shared<TBinaryProtocol>(framed);
MyServiceClient client(proto);
framed->open();
client.someRpc(...);                          // routed over DPUmesh to pod 11
```

---

## 8. Notes & invariants
- **Sizing invariant:** `num_slots × slot_size == DPU_BUFFER_SIZE` (32 MB). The defaults (4096 × 8 KB) satisfy it; change both together.
- **Body cap:** `body_len ≤ slot_size`; the Thrift base layer throws `TTransportException` if a write exceeds it.
- **`K` must match:** host `DPUMESH_RINGS_PER_POD` and the DPU process value must be equal (host TX rings pair 1:1 with DPU per-pod rings).
- **No host→host:** the body always traverses host → DPU → host (the DPU is the routing rendezvous); the DPU routes on `dst_pod_id` and never reads the body.
- **Thread-safety:** `tx`/`rx`/`pending` calls are internally locked; many worker threads may share one `ctx`.
