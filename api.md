# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on
the **BlueField DPU/DPA**, not host CPU, so the application keeps its full host
core (no in-host sidecar tax). Two pods exchange messages **host → DPU → host**;
the DPU routes on `dst_pod_id` (metadata only — it never reads the body) and the
DPA EU performs the DMA copies.

The public API is shaped like **BSD sockets + epoll**, so an ordinary
non-blocking epoll server/client ports by swapping each call for its `_dpumesh`
twin (`read` → `read_dpumesh`, …). Header: `thrift/transport/dpumesh_sock.h`
(header-only, built on the C core `dpumesh.h`).

## 0. Read this first — how DPUmesh differs from sockets

| Property | DPUmesh | Why |
|---|---|---|
| **Non-blocking RX** | `accept`/`read` + epoll readiness never block; "not ready" → sentinel + `errno=EAGAIN`. **TX** (`write`/`sendfile`/`send`) never blocks on the peer but **busy-spins** (capped ~50µs backoff) while the local TX-slot pool / DMA ring is saturated — it self-throttles, never fails with backpressure. `send`'s `register_pending` can stall up to ~2s on a rare `req_id` collision. | RX is poll-based; TX uses slot-admission with spin backoff. No cond-blocking mode. |
| **Message-oriented** | One **request ↔ one response**; not a byte stream. | DMA delivers whole messages. |
| **Atomic delivery** | `accept_dpumesh()` already holds the **entire** request body. No partial-read loop, no `EPOLLOUT` body dance. | The message arrives in one RX slot. |
| **Single-shot conn** | A `dpmconn_t` carries **one** request/response; after `send_dpumesh()` it cannot be reused. | `close_dpumesh()` then a new `connect`/`accept`. |
| **8 KB body cap** | `body_len ≤ slot_size`; **8192 B is the DPA `dma_copy` hard limit**. `slot_size` is **not** host-clamped, so setting it > 8192 silently **drops** messages at the DPA — keep `slot_size ≤ 8192`. `write`/`sendfile` past `slot_size` → `EMSGSIZE`. | Larger payloads must be chunked at the app layer. |
| **Address = `pod_id`** | A small integer `[0,127]`, not IP:port. | Pods register with the DPU by id. |
| **`write` buffers** | `write_dpumesh()` accumulates; `send_dpumesh()` transmits. | A message is sent as a unit. |
| **Per-request "connection"** | A `dpmconn_t` is one conversation, not a persistent stream. | "keep-alive" = simply accept the next request. |

> **No cond-blocking calls** (by design). RX (`accept`/`read`) polls; TX spins under local
> saturation. To SLEEP until work is ready, wait on `dpumesh_event_fd(s)` with **native**
> kernel epoll/poll/select — it is notification-driven (the PE thread sleeps on the DOCA
> notification fd and signals this fd; idle CPU stays ~0). A `dpmconn_t` is **single-shot**.

---

## 1. Types

```c
typedef struct dpm_endpoint dpm_t;     // a bound, listening endpoint (one per process)
typedef struct dpm_conn     dpmconn_t; // one request/response conversation (≈ an fd)
```
Only two handles. Event-loop multiplexing uses **native** kernel epoll/poll/select on
`dpumesh_event_fd(s)` — there are no DPUmesh-specific epoll types or functions.

Accessors: `int dpm_pod_id(dpm_t*)`, `int dpm_msg_max(dpm_t*)` (= `slot_size`),
`int dpm_is_server(dpmconn_t*)`, `int32_t dpm_peer(dpmconn_t*)`,
`void dpm_set_data(dpmconn_t*, void*)` / `void *dpm_get_data(dpmconn_t*)` (mirrors `epoll_event.data.ptr`).

---

## 2. API reference

All calls are **non-blocking**. "would-block" = the listed sentinel **with `errno=EAGAIN`**.

### Endpoint (socket + bind + listen, folded)
| Function | Returns / errno |
|---|---|
| `dpm_t *socket_dpumesh(const char *app_name, int pod_id)` | Endpoint handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address (overridden by env `DPUMESH_POD_ID`). |
| `void destroy_dpumesh(dpm_t *s)` | — (releases all DOCA resources; safe on `NULL`). |

### Accept / connect
| Function | Returns / errno |
|---|---|
| `dpmconn_t *accept_dpumesh(dpm_t *s)` | New **server** conn holding the next request (body ready), or `NULL`+`EAGAIN` if none pending. **Non-blocking.** |
| `dpmconn_t *connect_dpumesh(dpm_t *s, int dst_pod_id)` | New **client** conn targeting `dst_pod_id`. No round-trip (just binds the target). `NULL` on OOM. |

### Read / write / send
| Function | Returns / errno |
|---|---|
| `ssize_t read_dpumesh(dpmconn_t *c, void *buf, size_t len)` | `>0` bytes copied from the inbound body; `0` = end of message; `-1` = would-block (client response not in yet, `EAGAIN`) or abandoned (`ECONNRESET`). |
| `ssize_t write_dpumesh(dpmconn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len`; `-1` = would exceed `slot_size` (`EMSGSIZE`) or conn already sent (`EINVAL`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t sendfile_dpumesh(dpmconn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the body (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. Returns bytes appended (`0` = EOF), `-1` on read error / already-sent (`EINVAL`). |
| `int send_dpumesh(dpmconn_t *c)` | **Transmits** the buffered message (client → request; server → response matched to the inbound `req_id`). `0` sent; `-1` = already sent (`EINVAL`), a rare `req_id` pending-table collision (`EAGAIN`, ~2 s hard timeout — retry), or an enqueue validation error. Buffered body retained on `-1`. |
| `int close_dpumesh(dpmconn_t *c)` | Frees the conn's slots/pending. Always `0`. Safe on `NULL`. |

**Lifecycles (the only correct orderings):**
```
server:  c = accept_dpumesh(s);  read_dpumesh(c,…)…(EOF);  write_dpumesh(c,…)…;  send_dpumesh(c);  close_dpumesh(c);
client:  c = connect_dpumesh(s,dst);  write_dpumesh(c,…)…;  send_dpumesh(c);  // then later:
         while (read_dpumesh(c,buf,len) < 0 && errno==EAGAIN) {/* poll / epoll_wait */}  …;  close_dpumesh(c);
```

### Event-loop readiness fd (for NATIVE epoll/poll/select)
| Function | Returns |
|---|---|
| `int dpumesh_event_fd(dpm_t *s)` | A real fd that becomes **readable** whenever an inbound request/response is delivered, or `-1` if unavailable. Register it in your own `epoll`/`poll`/`select` like a listen socket. |

> **Usage:** register `dpumesh_event_fd(s)` with `EPOLLIN`. On a readiness event,
> **drain** it (`while (read(dfd,&u64,8) > 0) {}`), then `accept_dpumesh(s)` in a loop
> until it returns `NULL` (a server) or `read_dpumesh()` your client conns. The fd is
> raised per delivery and is **notification-driven** (no busy-poll; the PE thread sleeps
> on the DOCA notification fd). It mixes freely with real sockets in one epoll set.

---

## 3. Examples

### 3a. Echo server — the natural accept-loop (no epoll needed)
Because a request arrives atomically, the kernel server's READ/SEND_HDR/SEND_BODY
state machine collapses into accept → handle → reply.
```c
#include <thrift/transport/dpumesh_sock.h>

dpm_t *s = socket_dpumesh("echo", /*pod_id*/11);
for (;;) {
    dpmconn_t *c = accept_dpumesh(s);          // non-blocking
    if (!c) { /* errno==EAGAIN: idle — sched_yield() or do other work */ continue; }

    char buf[8192]; ssize_t n, off = 0;
    while ((n = read_dpumesh(c, buf+off, sizeof buf-off)) > 0) off += n;   // whole request
    write_dpumesh(c, buf, off);                 // echo it back (<= 8 KB)
    while (send_dpumesh(c) < 0 && errno == EAGAIN) sched_yield();   // reply exactly once
    close_dpumesh(c);                            // frees the request RX slot
}
```
*(For throughput, run this loop on N threads — the endpoint is shared and thread-safe.)*

### 3b. Client (request → response)
```c
dpm_t *s = socket_dpumesh("client", /*pod_id*/10);
dpmconn_t *c = connect_dpumesh(s, /*dst_pod_id*/11);

write_dpumesh(c, "ping", 4);
while (send_dpumesh(c) < 0 && errno == EAGAIN) sched_yield();   // request out

char resp[8192]; ssize_t n;
for (;;) {                                   // poll for the response (non-blocking)
    n = read_dpumesh(c, resp, sizeof resp);
    if (n >= 0) break;                       // got it (n bytes; 0 = empty reply)
    if (errno != EAGAIN) { /* ECONNRESET: lost */ break; }
    sched_yield();
}
close_dpumesh(c);   // REQUIRED on every path — also reclaims the pending entry on ECONNRESET
```

### 3c. NATIVE epoll server — a normal epoll loop, only the data calls suffixed
This is `bench/echo_sock.c` (validated: 240K RPS, 0-fail, idle CPU ~1%). The epoll
machinery is **stock kernel epoll**; `dpumesh_event_fd(s)` is the "listen socket".
```c
dpm_t *s = socket_dpumesh("echo", 11);
int dfd  = dpumesh_event_fd(s);                          // the DPUmesh readiness fd

int epfd = epoll_create1(0);                             // ── vanilla kernel epoll ──
struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

struct epoll_event events[64];
for (;;) {
    int nfds = epoll_wait(epfd, events, 64, -1);         // SLEEPS until activity (no busy-poll)
    for (int n = 0; n < nfds; n++) {
        if (events[n].data.fd != dfd) continue;          // (real sockets can share this loop)
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}   // drain readiness fd

        dpmconn_t *c;
        while ((c = accept_dpumesh(s)) != NULL) {         // accept every queued request
            char b[8192]; ssize_t off = 0, r;
            while ((r = read_dpumesh(c, b+off, sizeof b-off)) > 0) off += r;   // full request
            if (off > 0) write_dpumesh(c, b, off);        // echo it back
            while (send_dpumesh(c) < 0 && errno == EAGAIN) sched_yield();
            close_dpumesh(c);
        }
    }
}
```
> The diff from an ordinary TCP epoll echo server is **only** the `_dpumesh` suffix on
> `socket/accept/read/write/send/close` and using `dpumesh_event_fd(s)` as the listen fd.
> The epoll loop is unchanged.

### 3d. Porting an ordinary epoll HTTP server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost
1:1 — and gets **simpler**, because the message is atomic (no `STATE_SEND_HDR`/
`STATE_SEND_BODY`, no partial-write retry). The full request is in hand at accept;
build the whole response and `send_dpumesh` once:
```c
dpm_t *s = socket_dpumesh("httpd", pod_id);
for (;;) {
    dpmconn_t *c = accept_dpumesh(s);
    if (!c) { sched_yield(); continue; }

    char req[8192]; ssize_t off = 0, n;
    while ((n = read_dpumesh(c, req+off, sizeof req-off)) > 0) off += n;   // full request header

    if (parse_request(req, off) < 0) {
        write_dpumesh(c, ERR_400, strlen(ERR_400));
    } else {
        int fd = open_file(url);
        if (fd < 0) write_dpumesh(c, ERR_404, strlen(ERR_404));
        else if (flen + 256 > (long)dpm_msg_max(s))      // header + body won't fit one ≤8KB msg
            write_dpumesh(c, ERR_500, strlen(ERR_500));   // (or chunk across round-trips)
        else {
            char hdr[256];
            int hl = snprintf(hdr, sizeof hdr, "HTTP/1.0 200 OK\r\nContent-Length: %ld\r\n\r\n", flen);
            write_dpumesh(c, hdr, hl);
            sendfile_dpumesh(c, fd, NULL, flen);   // header + body in ONE message
            close(fd);
        }
    }
    while (send_dpumesh(c) < 0 && errno == EAGAIN) sched_yield();   // one atomic response
    close_dpumesh(c);
}
```
> **Caveat (8 KB cap):** header + file body must fit in one `slot_size` (≤ 8 KB)
> message. For larger files you must chunk at the app layer (multiple
> request/response round-trips) — DPUmesh has no multi-segment streaming.

---

## 4. Limitations & rules (explicit)

- **No cond-blocking calls.** RX (`accept`/`read`) polls; the TX side (`write`/`sendfile`/`send`) **busy-spins** under local TX-slot/DMA-ring saturation (it never fails with backpressure), and `send`'s `register_pending` can stall ~2 s on a rare collision. To sleep until work is ready, use **native epoll on `dpumesh_event_fd(s)`** (notification-driven).
- **Single-shot conn.** A `dpmconn_t` carries one request/response; after `send_dpumesh()`, `write`/`sendfile`/`send` on it return `-1`+`EINVAL`. Reuse = `close_dpumesh()` + a new `connect`/`accept`.
- **Message ≤ `slot_size` (≤ 8 KB).** `write`/`sendfile` past `slot_size` → `EMSGSIZE`. 8192 B is the DPA hard limit; `slot_size` is **not** host-clamped, so `> 8192` silently drops at the DPA. No streaming/segmentation.
- **Ownership:** every `accept`/successful response `read` owns an RX slot freed by `close_dpumesh`; `write` owns a TX slot handed to the transport by `send` (freed by the DPU), or freed by `close` if never sent. **Pair every conn with exactly one `close_dpumesh` — including the abandoned/`ECONNRESET` path, where `close` reclaims the still-live pending entry.**
- **`send` then read (client):** poll `read_dpumesh` (or wait on `dpumesh_event_fd`) only **after** `send_dpumesh`.
- **Addressing:** `dst_pod_id` must be a live, registered pod `[0,127]`; there is no name resolution.
- **Thread-safety:** the endpoint is shared/thread-safe; a single `dpmconn_t` is single-thread. The native-epoll reactor is single-threaded (validated at 240K, 0-fail).

---

## Appendix A — Low-level C primitives (`dpumesh.h`)

The façade is a thin wrapper over these; use them directly only for custom paths.
All are thread-safe.

| Function | Purpose / returns |
|---|---|
| `int dpumesh_init(dpumesh_ctx_t **ctx, const char *app_name, int worker_id, const dpumesh_config_t *config)` | Create context (`NULL` config = defaults). `0`/non-zero. |
| `void dpumesh_destroy(dpumesh_ctx_t *ctx)` | Tear down. |
| `int dpumesh_get_slot_size(ctx)` / `int dpumesh_get_pod_id(ctx)` / `const char *dpumesh_get_worker_id(ctx)` | Configured slot size / this pod id / `"<app>-worker-<n>"`. |
| `int dpumesh_get_event_fd(ctx)` | Enable + return the readiness eventfd (readable on inbound delivery) for native epoll/poll/select; `-1` if unavailable. (`dpumesh_event_fd(s)` in the façade wraps this.) |
| `int dpumesh_dequeue(ctx, sw_descriptor_t *desc, int timeout_ms)` | RX one message. `-1`=block forever, `0`=non-blocking, `>0`=ms. `0`/`-1`. `desc->body_buf_slot` must be `rx_free`'d. |
| `uint8_t *dpumesh_rx_buf(ctx, int slot)` / `void dpumesh_rx_free(ctx, int slot)` | RX body pointer (zero-copy) / release it. |
| `int dpumesh_tx_alloc(ctx)` | TX slot index `≥0`; under backpressure BUSY-SPINS (capped backoff) until a slot frees — never returns `-1`. (`dpumesh_enqueue` likewise blocks on a full DMA ring; it returns `-1` only on descriptor validation: NULL / bad slot / `body_len > slot_size`.) |
| `uint8_t *dpumesh_tx_buf(ctx, int slot)` / `void dpumesh_tx_free(ctx, int slot)` | TX body pointer / free on error before handoff. |
| `int dpumesh_enqueue(ctx, const sw_descriptor_t *desc)` | Submit a filled descriptor. `0`/`-1`. |
| `uint32_t dpumesh_alloc_req_id(ctx)` | Atomic unique request id (starts at 1). |
| `int dpumesh_register_pending(ctx, uint32_t req_id)` | Register **before** enqueue. `0`/`-1`. |
| `int dpumesh_wait_response(ctx, req_id, sw_descriptor_t *resp, int timeout_ms)` | **Blocking** match (`-1`/`0`/`>0`). `0`=`resp` filled (free its `body_buf_slot`), `-1`=timeout. |
| `int dpumesh_poll_response(ctx, req_id, sw_descriptor_t *resp)` | Non-blocking: `0`=arrived (TX already freed; free body), `1`=not ready, `-1`=abandoned. |
| `void dpumesh_pending_attach_tx(ctx, req_id, int tx_slot)` | Bind TX slot **after** a successful enqueue (TX_ACK owns it now). |
| `void dpumesh_cancel_pending(ctx, req_id)` | Cancel (error path); defers TX cleanup if in flight. |
| `void dpumesh_pending_release_async(ctx, req_id)` | Responder fire-and-forget after enqueue+attach_tx (no response expected). Idempotent. |

> A process uses **either** `wait_response` (set `config.async_client=0`) **or**
> `poll_response` (set `config.async_client=1`) — never both. The façade selects
> the non-blocking (`poll_response`) model.

### `dpumesh_config_t`
| Field | Meaning | 0 / default |
|---|---|---|
| `num_slots` | slots per pool | env `DPUMESH_NUM_SLOTS` → **4096** |
| `slot_size` | bytes per slot (≤ 8192 effective) | env `DPUMESH_SLOT_SIZE` → **8192** |
| `max_descriptors` | descriptor ring capacity | env `DPUMESH_MAX_DESCRIPTORS` → **2048** |
| `poll_rx` | `1` = `dequeue` spin-polls (lean server) | `0` |
| `async_client` | `1` = `poll_response` model | `0` |

Precedence: explicit field (`> 0`) → env var → default. Invariant: `num_slots × slot_size == 32 MB` (`DPU_BUFFER_SIZE`).

### `sw_descriptor_t` (fill for `dpumesh_enqueue`)
`header_buf_slot=-1`, `header_len=0`, `body_buf_slot` (TX/RX slot), `body_len`
(≤ slot_size), `req_id`, `dst_pod_id`, `src_pod_id` (= `dpumesh_get_pod_id`),
`flags` = `OpFlag|CaseFlag`, `valid=1`.

Flags: `OP_REQUEST=0x00`, `OP_RESPONSE=0x10`, `CASE_EXTERNAL=1`, `CASE_INGRESS=2`.
Reply flags = `(req.flags & ~OP_REQUEST) | OP_RESPONSE`.

---

## Appendix B — Environment variables (host)

Precedence `config field (>0) → env → default`:

| Variable | Effect | Default |
|---|---|---|
| `DPUMESH_PCI_ADDR` | DOCA device PCI address | `94:00.0` |
| `DPUMESH_POD_ID` | Override this node's pod id | = `worker_id` arg |
| `DPUMESH_NUM_SLOTS` | Slots per pool | `4096` |
| `DPUMESH_SLOT_SIZE` | Bytes per slot | `8192` |
| `DPUMESH_MAX_DESCRIPTORS` | Descriptor ring capacity | `2048` |
| `DPUMESH_RINGS_PER_POD` | EU-sharding rings/pod `K` (must match the DPU process) | `1` |
| `DPUMESH_HOST_EPOLL` | `1` = host PE-progress thread sleeps on epoll; `0` = busy-poll | `0` |
