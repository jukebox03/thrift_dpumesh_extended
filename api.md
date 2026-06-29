# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on
the **BlueField DPU/DPA**, not the host CPU, so your application keeps its full host
core (no in-host sidecar tax). Pods exchange messages **host → DPU → host**; you address
a **`service_id`** and the DPU resolves it to a backend pod (metadata only — it never
reads the body), then the DPA EU performs the DMA copies. Routing is **connection-level**:
the backend is chosen on a connection's first request and stays sticky.

The API wears a **BSD-sockets + epoll shape** (`dmesh_create_channel`/`dmesh_accept`/`dmesh_read`/
`dmesh_write`/`dmesh_close` + a native-epoll readiness fd), but it is **not a drop-in socket
replacement**. Under the hood it is a **message / RPC channel**, not a byte stream: each
call is one whole ≤ 8 KB request→response matched by an internal id — no connection, no
handshake, no stream ordering.

- **What ports cleanly:** the *shape* of a **non-blocking request/response epoll loop**.
  And it often gets **simpler** — a message is atomic, so the partial-read loop and the
  `EPOLLOUT` send dance disappear.
- **What does *not* port:** socket *semantics* — persistent connections, `read()==0` as
  EOF (here it just means end-of-message), per-fd epoll readiness (the readiness fd is
  per-endpoint → you scan), pipelining, > 8 KB messages, `connect()` liveness, IP/DNS
  addressing.

In short: the call *vocabulary* and the simple loop *shape* carry over; socket
*semantics* do not. See **§2** for the exact differences. Header:
`thrift/transport/dpm.h` (header-only, built on the C core `dpumesh.h`).

---

## 1. Concepts — two handles

You touch only **two handles** (a third, the engine, is hidden inside the first):

| Handle | What it is | Lifetime / count |
|---|---|---|
| **`dmesh_channel_t`** (endpoint) | The bound endpoint. Wraps the whole DPU backend — DOCA device, the RX poller thread, the slot pool, the link to the DPU. Created by `dmesh_create_channel()`. | **one per process**, lives the whole run; **heavy** to create |
| **`dmesh_conn_t`** (conn) | A handle bound to a **peer** — what an IP:port used to be. Created by `dmesh_accept()` (server, an incoming exchange) or `dmesh_connect()` (client, outgoing to a **service**). | **cheap**; a client conn is **reusable**, a server conn handles one accepted request |
| `dpumesh_ctx_t` (engine) | The low-level DOCA context that moves the bytes. A `dmesh_channel_t` holds one internally; **you never touch it.** | one per `dmesh_channel_t` |

> You only ever hold `dmesh_channel_t` and `dmesh_conn_t`. `dpumesh_ctx_t` is the engine the façade
> hides inside `dmesh_channel_t` (the low-level `dpumesh.h` core — the façade is a thin wrapper).

### A `dmesh_conn_t` is a **peer handle**, used one exchange at a time

It is closer to an **RPC channel** than a TCP byte stream — **no persistent byte
stream, no handshake**:

- `dmesh_connect()` does **no round-trip** — it just binds "this conn talks to service 11."
- One **exchange** = a request fired, its response matched back by an **internal
  `(port, seq)` id** (not by arrival order).
- A **client conn is reusable**: read the response, then `write` again for the next
  request. One conn carries **one outstanding exchange at a time** (use more conns for
  concurrency — they may complete out of order, each matched by id).
- **One-way** is first-class: `write` then `close` *without reading* = fire-and-forget.

```c
// Reusable RPC client: connect once, loop, close at the end.
dmesh_channel_t *s = dmesh_create_channel("myapp", 10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);
for (int i = 0; i < N; i++) {
    dmesh_write(c, req, len);              // buffer the request
    dmesh_flush(c);                        // ship it (read does NOT auto-send)
    char resp[8192];
    ssize_t n = dmesh_read(c, resp, sizeof resp);   // wait for + return the reply
    /* ... use resp (n bytes); the next write starts a fresh request ... */
}
dmesh_close(c);
```

---

## 2. How it differs from BSD sockets

The API *shape* ports; the *semantics* in this table do not. DPUmesh trades the socket's
generality for a lean DPU data path. Versus `read`/`write`/`epoll`:

| | BSD socket | DPUmesh | What it means for you |
|---|---|---|---|
| **Channel** | a **connection** — persistent bidirectional byte stream | a **peer handle**, one exchange at a time | a client conn is reusable (read the reply, then `write` the next request); server conns are per-accepted-request |
| **Setup** | 3-way **handshake** | **none** — `dmesh_connect` is local, no round-trip | like UDP/RPC: fire-and-match; peer liveness is *not* checked at connect |
| **Framing** | byte stream (you frame it) | **message** — one request, one response, delivered **atomically** | no partial-read loop, no `EPOLLOUT` body dance — the whole body is in hand at `accept`/`read` |
| **Size** | unbounded stream | **≤ 8 KB per message** (the DPA `dma_copy` hard limit) | larger payloads must be chunked across round-trips |
| **Ordering** | in-order within a connection | each exchange independent, **matched by request-id** | out-of-order completion across conns is fine; no cross-message stream order |
| **Address** | IP:port | **`service_id`** integer — the DPU resolves it to a backend pod (connection-sticky) | name→backend routing on the DPU; self-routing OK; no DNS |
| **Blocking** | blocking *or* non-blocking | **non-blocking only** — RX polls; TX busy-spins under local saturation (never blocks on the peer) | to *sleep* until ready, use native epoll on `dmesh_event_fd` (below) |
| **`write`** | sends immediately | **buffers only**; `dmesh_flush` ships it — read/close do **not** auto-send | always `write`→`flush`→`read` (RPC) or `write`→`flush`→`close` (one-way) |
| **One-way** | first-class (`write`-only, `shutdown`) | **send-only is first-class** (`write`→`flush`→`close`, no read) | fire-and-forget: the TX slot is freed by the DPU ACK (no leak); any reply the peer sends is silently dropped |
| **epoll** | the fd itself is pollable | **native epoll on `dmesh_event_fd(s)`** — a real fd you register like a listen socket | no `dmesh_epoll_*` wrappers; it mixes with real sockets in one epoll set |

**Must-follow rules**
- **Exactly one `dmesh_close()` per conn** — every `accept`/`connect` conn, including the
  error/`ECONNRESET` path (close reclaims the pending entry and the held slots).
- **One conn = one outstanding exchange.** A client `write` while a request is still
  in flight (response not yet read) fails `EINVAL`. For concurrency use more conns.
- **`service_id` must be a live, registered service.** A dead/unregistered service is
  *not* detected at `connect` — the request is dropped at the DPU and no response
  arrives, so `dmesh_read` stays `EAGAIN`. **Apply your own wall-clock timeout.**
  (Self-routing is allowed: a service may resolve to your own pod.)
- **Thread-safety:** the `dmesh_channel_t` endpoint is shared/thread-safe (run the accept loop on
  N threads); a single `dmesh_conn_t` is single-thread.
- **`slot_size` ≤ 8192.** The 8 KB cap is the DPA `dma_copy` limit. A value > 8192 set
  via `DPUMESH_SLOT_SIZE` is **not** host-clamped — `write` would accept it but the DPA
  silently drops the excess, and it breaks the `num_slots × slot_size = DPU_BUFFER_SIZE`
  admission invariant. Leave it at the default.

---

## 3. API reference

All calls are **non-blocking**. "would-block" = the listed sentinel **with `errno=EAGAIN`**.

### Endpoint (socket + bind + listen, folded)
| Function | Returns / errno |
|---|---|
| `dmesh_channel_t *dmesh_create_channel(const char *app_name, int pod_id)` | Endpoint handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address. |
| `void dmesh_destroy_channel(dmesh_channel_t *s)` | — (releases all DOCA resources; safe on `NULL`). |
| `int dmesh_event_fd(dmesh_channel_t *s)` | A real fd that becomes **readable** when an inbound request/response is delivered, for NATIVE epoll/poll/select; `-1` if unavailable. |
| `int dmesh_pod_id(dmesh_channel_t *s)` / `int dmesh_msg_max(dmesh_channel_t *s)` | This node's `pod_id` / the max body size (`slot_size`). |

> **Env:** `DPUMESH_POD_ID` overrides `pod_id`; `DPUMESH_PCI_ADDR` selects the DOCA
> device (default `94:00.0`). `DPUMESH_HOST_EPOLL=1` makes the library's internal
> RX-progress thread **sleep on the DOCA notification fd** (idle CPU ~0); the default
> (`0`) is an adaptive busy-poll (low but non-zero idle CPU).

### Accept / connect
| Function | Returns / errno |
|---|---|
| `dmesh_conn_t *dmesh_accept(dmesh_channel_t *s)` | New **server** conn holding the next request (body ready), or `NULL`+`EAGAIN` if none pending. **Non-blocking.** (A `NULL` with `errno=ENOMEM` is a rare conn-alloc failure: the request was dropped, its RX slot reclaimed; an accept-until-NULL loop treats it as drained — safe, it just skips one.) |
| `dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int service_id)` | New **client** conn bound to a **`service_id`**. No round-trip — the DPU resolves the service to a backend pod on the first request (connection-sticky thereafter); a dead/unregistered service is **not** detected here (no response → `dmesh_read` stays `EAGAIN`; apply your own timeout). `NULL`+`ENOMEM` on OOM. |

### Read / write / sendfile / flush
| Function | Returns / errno |
|---|---|
| `ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len)` | `>0` bytes copied from the inbound body; `0` = end of message; `-1` = would-block (reply not in yet, or request not yet flushed, `EAGAIN`) or abandoned (`ECONNRESET`). **NO implicit send** — `dmesh_flush` the request before reading the reply. |
| `ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len`. On a **reused** client conn (its response already read) the first `write` auto-starts a NEW request. `-1` = would exceed `slot_size` (`EMSGSIZE`), or a `write` while a request is still outstanding / a 2nd reply on a server conn (`EINVAL`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the body (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. Returns bytes appended (`0` = EOF), `-1` on read error / already-outstanding (`EINVAL`) / no slot room (`EMSGSIZE`). |
| `int dmesh_flush(dmesh_conn_t *c)` | **The explicit ship — REQUIRED to send** (write only buffers; read/close do NOT auto-send). `0` sent; `-1` **`EINVAL`** = already sent (not retryable); **`EAGAIN`** = rare `(port,seq)` pending-table collision (conn reused before its prior TX_ACK landed), retryable (body kept), returns after up to a ~2 s wait; **`EBADMSG`** = permanent descriptor fault (not retryable; close the conn). |
| `int dmesh_close(dmesh_conn_t *c)` | Frees the conn's slots/pending. **Does NOT send** — a buffered-but-unflushed message is discarded (flush first). If you flushed but never read the reply (one-way / abandoned), close cancels the pending wait; the TX slot is still freed by the DPU's TX_ACK (no leak) and any reply is silently dropped. Returns `0`. Safe on `NULL`. **ONE-WAY:** `write → flush → close` (no read). |

**Lifecycles:**
```
server:  c = dmesh_accept(s);  dmesh_read(c,…)…(EOF);  dmesh_write(c,…)…;  dmesh_flush(c);  dmesh_close(c);
client:  c = dmesh_connect(s,dst);
  RPC (reusable):  for each call: dmesh_write(c,…)…;  dmesh_flush(c);  dmesh_read(c,…)…;   // flush ships, read waits
                   …; dmesh_close(c);                                       // once, at the end
  one-way:         dmesh_write(c,…)…;  dmesh_flush(c);  dmesh_close(c);     // fire-and-forget (no read)
```

### Accessors
`int32_t dmesh_peer(dmesh_conn_t*)` — the other pod in this exchange (role-neutral: the
sender of a message you received, or the target of one you initiated).

---

## 4. Examples

### 4a. Echo server — the natural accept-loop (no epoll needed)
Because a request arrives atomically, the kernel server's READ/SEND_HDR/SEND_BODY state
machine collapses into accept → handle → reply.
```c
#include <thrift/transport/dpm.h>

dmesh_channel_t *s = dmesh_create_channel("echo", /*pod_id*/11);
for (;;) {
    dmesh_conn_t *c = dmesh_accept(s);          // non-blocking
    if (!c) { /* errno==EAGAIN: idle — sched_yield() or do other work */ continue; }

    char buf[8192];                          // a request is one whole <= 8 KB message...
    ssize_t n = dmesh_read(c, buf, sizeof buf);// ...so ONE read returns it in full (no loop)
    if (n > 0) { dmesh_write(c, buf, n); dmesh_flush(c); }   // echo it back + SHIP it
    dmesh_close(c);                            // frees the RX slot (does NOT send)
}
```
*(For throughput, run this loop on N threads — the endpoint is shared and thread-safe.)*

> **Delivery:** `dmesh_flush` is what ships the reply (`dmesh_close` does not send). It
> returns `-1` on a rare `(port,seq)` collision; if losing a reply is unacceptable, retry:
> `while (dmesh_flush(c) < 0 && errno == EAGAIN) sched_yield();` then `dmesh_close(c)`.
> (`EAGAIN` here already includes a ~2 s wait, so the loop is not a busy-spin.)

### 4b. Client — reusable RPC conn (request → response, repeated)
Connect once, then loop `write → flush → read`; the conn auto-starts a new request each time.
Apply a wall-clock timeout so a dead pod (no response ever) can't hang you.
```c
#include <thrift/transport/dpm.h>
#include <time.h>

dmesh_channel_t *s = dmesh_create_channel("client", /*pod_id*/10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);

for (int i = 0; i < N; i++) {
    dmesh_write(c, req[i], req_len[i]);            // buffer the request
    dmesh_flush(c);                                // ship it (read does NOT auto-send)
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    char resp[8192]; ssize_t n;
    for (;;) {
        n = dmesh_read(c, resp, sizeof resp);
        if (n >= 0) break;                       // got it (n bytes; 0 = empty reply)
        if (errno != EAGAIN) break;              // ECONNRESET: pending reclaimed (not peer death)
        struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - t0.tv_sec >= 5) break;  // dead/unregistered pod → EAGAIN forever; give up
        sched_yield();
    }
    /* ... handle resp; the next write starts a fresh request on this same conn ... */
}
dmesh_close(c);   // once, at the end
```

### 4c. Client — one-way (fire-and-forget)
`write → flush → close` with no read. flush ships the request; the TX slot is freed by the
DPU's TX_ACK (no leak); any reply the peer sends is silently dropped. (A one-way conn can't
be reused — close it; `dmesh_connect` is free.)
```c
for (int i = 0; i < N; i++) {
    dmesh_conn_t *c = dmesh_connect(s, /*service*/11);
    dmesh_write(c, msg[i], msg_len[i]);
    dmesh_flush(c);                           // ship it
    dmesh_close(c);                           // fire-and-forget (close does NOT send)
}
```

### 4d. Client — many in-flight via native epoll (pipelined window)
Fire a window of W requests (each its own conn) and harvest via native epoll on
`dmesh_event_fd(s)`. Two things matter: **(1) flush explicitly** — you sleep on
`epoll_wait` *before* reading, so you can't rely on read-triggered implicit send;
**(2)** `dmesh_event_fd` is **per-endpoint**, so a readiness event doesn't say *which*
conn — keep your own array of in-flight conns and scan it on wakeup.
```c
#include <thrift/transport/dpm.h>
#include <sys/epoll.h>
#include <time.h>
#define W 64                                        // up to W requests in flight

dmesh_channel_t *s = dmesh_create_channel("client", /*pod_id*/10);
int dfd  = dmesh_event_fd(s);
int epfd = epoll_create1(0);
struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

dmesh_conn_t *inflight[W] = {0};
time_t launched[W] = {0};
int pending = 0;

for (int i = 0; i < W; i++) {                        // ── launch the window ──
    dmesh_conn_t *c = dmesh_connect(s, /*service*/11);
    dmesh_write(c, req[i], req_len[i]);
    while (dmesh_flush(c) < 0 && errno == EAGAIN) sched_yield();   // ship NOW (we'll sleep on epoll)
    inflight[i] = c; launched[i] = time(NULL); pending++;
}

struct epoll_event events[8];
while (pending > 0) {                                 // ── harvest via epoll ──
    int nfds = epoll_wait(epfd, events, 8, 1000);    // SLEEP, ≤1s tick so dead pods can time out
    for (int e = 0; e < nfds; e++) {
        if (events[e].data.fd != dfd) continue;
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}   // drain readiness fd

        for (int i = 0; i < W; i++) {                // per-endpoint fd → scan in-flight conns
            dmesh_conn_t *c = inflight[i];
            if (!c) continue;
            char resp[8192];
            ssize_t n = dmesh_read(c, resp, sizeof resp);
            if (n < 0 && errno == EAGAIN) continue;  // this one's reply not in yet
            handle_response(i, resp, n);             // i = the in-flight slot (= request index)
            dmesh_close(c); inflight[i] = NULL; pending--;
        }
    }
    for (int i = 0; i < W; i++)                       // abandon dead-pod conns (EAGAIN never clears)
        if (inflight[i] && time(NULL) - launched[i] >= 5) {
            dmesh_close(inflight[i]); inflight[i] = NULL; pending--;   // count as a timeout failure
        }
}
```
> A reply is a whole ≤8 KB message, so one `dmesh_read` returns it in full. For a large
> window, replace the linear scan with your own bookkeeping — the readiness fd only says
> "something arrived," not *which* conn.

### 4e. NATIVE epoll server — a normal epoll loop, only the data calls suffixed
This is `bench/echo_sock.c`. **Validated 0-fail across a warm-ramped 30K→220K** (the
endpoint-tuple redesign re-validated this — perf-neutral vs the prior baseline; op-rate
ceiling ~257K — always ramp up; cold-jumping to/over the knee can transiently wedge the
load path). **Idle CPU ~1.2% requires `DPUMESH_HOST_EPOLL=1`** (the default
adaptive-busy-polls the internal RX thread). The epoll machinery is **stock kernel
epoll**; `dmesh_event_fd(s)` is the "listen socket".
```c
dmesh_channel_t *s = dmesh_create_channel("echo", 11);
int dfd  = dmesh_event_fd(s);                              // the DPUmesh readiness fd

int epfd = epoll_create1(0);                             // ── vanilla kernel epoll ──
struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

struct epoll_event events[64];
for (;;) {
    int nfds = epoll_wait(epfd, events, 64, -1);         // SLEEPS until activity (no busy-poll)
    for (int n = 0; n < nfds; n++) {
        if (events[n].data.fd != dfd) continue;          // (real sockets can share this loop)
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}   // drain readiness fd

        dmesh_conn_t *c;
        while ((c = dmesh_accept(s)) != NULL) {            // accept every queued request
            char b[8192];                                 // one whole <= 8 KB request...
            ssize_t n = dmesh_read(c, b, sizeof b);         // ...so ONE read gets it (no loop)
            if (n > 0) { dmesh_write(c, b, n); dmesh_flush(c); }   // echo it back + ship
            dmesh_close(c);                                 // frees the slot (does NOT send)
        }
    }
}
```
> Versus an ordinary TCP epoll echo server: the epoll loop is unchanged and the data
> calls just take the `dmesh_` prefix — but it is also **simpler**, not byte-for-byte
> identical. A request is one whole message, so the read loop collapses to a single
> `dmesh_read`. Sending is explicit: `dmesh_write` buffers, `dmesh_flush` ships (close
> does NOT send). `dmesh_event_fd(s)` is the listen fd.

### 4f. Porting an ordinary epoll HTTP server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost 1:1 — and
gets **simpler**, because the message is atomic (no `STATE_SEND_HDR`/`STATE_SEND_BODY`,
no partial-write retry). The full request is in hand at accept; build the whole response
with `dmesh_write`/`dmesh_sendfile`, then `dmesh_flush()` ships it:
```c
dmesh_channel_t *s = dmesh_create_channel("httpd", pod_id);
for (;;) {
    dmesh_conn_t *c = dmesh_accept(s);
    if (!c) { sched_yield(); continue; }

    char req[8192];                                       // the whole request is one atomic message...
    ssize_t off = dmesh_read(c, req, sizeof req);           // ...so ONE read gets it (no accumulation loop)

    if (parse_request(req, off) < 0) {
        dmesh_write(c, ERR_400, strlen(ERR_400));
    } else {
        int fd = open_file(url);
        if (fd < 0) dmesh_write(c, ERR_404, strlen(ERR_404));
        else if (flen + 256 > (long)dmesh_msg_max(s))      // header + body won't fit one ≤8KB msg
            dmesh_write(c, ERR_500, strlen(ERR_500));      // (or chunk across round-trips)
        else {
            char hdr[256];
            int hl = snprintf(hdr, sizeof hdr, "HTTP/1.0 200 OK\r\nContent-Length: %ld\r\n\r\n", flen);
            dmesh_write(c, hdr, hl);
            ssize_t w = dmesh_sendfile(c, fd, NULL, flen);   // capped at slot_size → may be SHORT
            close(fd);
            if (w < (ssize_t)flen) { /* body didn't fit one message — chunk or send an error */ }
        }
    }
    dmesh_flush(c);   // ship the one atomic response
    dmesh_close(c);
}
```
> **Caveat (8 KB cap):** header + file body must fit in one `slot_size` (≤ 8 KB) message.
> For larger files chunk at the app layer (multiple round-trips) — DPUmesh has no
> multi-segment streaming.

---

## 5. Addressing & routing model (design summary)

DPUmesh addresses **services**, not hosts, and demultiplexes the way TCP does — by an
**oriented endpoint tuple**, never by a per-message "request vs response" flag.

- **Oriented endpoint tuple (TCP-faithful).** Every message carries `src = (pod, port)`,
  `dst = (service, pod, port)`, and a per-connection `seq`. The receiver demuxes purely
  by the local **`dst_port`**: a *client* port ⇒ this is a reply; a *listening* endpoint
  ⇒ a fresh request. "Request vs response" is **not** a wire bit — it falls out of *which
  local socket* the tuple resolves to (exactly like a TCP 4-tuple). That is also why
  **self-routing / loopback works**: a request and its reply on the same host land on
  different ports, so they never alias.

- **`service_id` + DPU routing (connection-level, sticky).** `dmesh_connect(s, service_id)`
  binds a logical service. On a connection's **first** request the DPU resolves
  `service_id → backend pod` (this is the L7 / load-balancing seam); the connection then
  sticks to that backend and later requests go straight there. A service may resolve to
  the caller's own pod — fully supported.

- **Matching is internal `(port, seq)`.** There is no user-visible request id. A client
  connection owns a stable **port** (like a socket/fd) and stamps each request with a
  monotonic **seq**; the reply is matched back by `(port, seq)`. One connection carries
  one outstanding exchange at a time (use more connections for concurrency).

- **Lifecycle — each side closes its own conn.** There is no FIN/handshake. A client
  connection is persistent and sticky; a server handles each request and closes
  (stateless per request). TX slots and RX landing credits are released per leg, so
  there is no slot leak. Apply your own timeout for a service that never answers.

> **Bottom line:** the call *vocabulary* and the non-blocking epoll *loop shape* are
> socket-like; the *addressing* is a service name the DPU routes, and the *demux* is the
> oriented tuple — giving transparent service routing and loopback without a direction flag.
