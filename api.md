# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on
the **BlueField DPU/DPA**, not the host CPU, so your application keeps its full host
core (no in-host sidecar tax). Two pods exchange messages **host → DPU → host**; the
DPU routes on `dst_pod_id` (metadata only — it never reads the body) and the DPA EU
performs the DMA copies.

The public API is shaped like **BSD sockets + epoll**: an ordinary non-blocking
epoll server/client ports by swapping each call for its `_dpm` twin
(`read` → `read_dpm`, …). Header: `thrift/transport/dpm.h` (header-only, built on the
C core `dpumesh.h`).

---

## 1. Concepts — two handles

You touch only **two handles** (a third, the engine, is hidden inside the first):

| Handle | What it is | Lifetime / count |
|---|---|---|
| **`dpm_t`** (endpoint) | The bound endpoint. Wraps the whole DPU backend — DOCA device, the RX poller thread, the slot pool, the link to the DPU. Created by `socket_dpm()`. | **one per process**, lives the whole run; **heavy** to create |
| **`dpmconn_t`** (conn) | A handle bound to a **peer** — what an IP:port used to be. Created by `accept_dpm()` (server, an incoming exchange) or `connect_dpm()` (client, outgoing to a pod). | **cheap**; a client conn is **reusable**, a server conn handles one accepted request |
| `dpumesh_ctx_t` (engine) | The low-level DOCA context that moves the bytes. A `dpm_t` holds one internally; **you never touch it.** | one per `dpm_t` |

> You only ever hold `dpm_t` and `dpmconn_t`. `dpumesh_ctx_t` is the engine the façade
> hides inside `dpm_t` (the low-level `dpumesh.h` core — the façade is a thin wrapper).

### A `dpmconn_t` is a **peer handle**, used one exchange at a time

It is closer to an **RPC channel** than a TCP byte stream — **no persistent byte
stream, no handshake**:

- `connect_dpm()` does **no round-trip** — it just binds "this conn talks to pod 11."
- One **exchange** = a request fired, its response matched back by an **internal
  request-id** (not by arrival order).
- A **client conn is reusable**: read the response, then `write` again for the next
  request. One conn carries **one outstanding exchange at a time** (use more conns for
  concurrency — they may complete out of order, each matched by id).
- **One-way** is first-class: `write` then `close` *without reading* = fire-and-forget.

```c
// Reusable RPC client: connect once, loop, close at the end.
dpm_t *s = socket_dpm("myapp", 10);
dpmconn_t *c = connect_dpm(s, /*peer*/11);
for (int i = 0; i < N; i++) {
    write_dpm(c, req, len);              // buffer the request
    char resp[8192];
    ssize_t n = read_dpm(c, resp, sizeof resp);   // ships it, then returns the reply
    /* ... use resp (n bytes); the next write starts a fresh request ... */
}
close_dpm(c);
```

---

## 2. How it differs from BSD sockets

DPUmesh trades the socket's generality for a lean DPU data path. Versus `read`/`write`/`epoll`:

| | BSD socket | DPUmesh | What it means for you |
|---|---|---|---|
| **Channel** | a **connection** — persistent bidirectional byte stream | a **peer handle**, one exchange at a time | a client conn is reusable (read the reply, then `write` the next request); server conns are per-accepted-request |
| **Setup** | 3-way **handshake** | **none** — `connect_dpm` is local, no round-trip | like UDP/RPC: fire-and-match; peer liveness is *not* checked at connect |
| **Framing** | byte stream (you frame it) | **message** — one request, one response, delivered **atomically** | no partial-read loop, no `EPOLLOUT` body dance — the whole body is in hand at `accept`/`read` |
| **Size** | unbounded stream | **≤ 8 KB per message** (the DPA `dma_copy` hard limit) | larger payloads must be chunked across round-trips |
| **Ordering** | in-order within a connection | each exchange independent, **matched by request-id** | out-of-order completion across conns is fine; no cross-message stream order |
| **Address** | IP:port | **`pod_id`** integer `[0,127]`, registered with the DPU | no DNS / name resolution |
| **Blocking** | blocking *or* non-blocking | **non-blocking only** — RX polls; TX busy-spins under local saturation (never blocks on the peer) | to *sleep* until ready, use native epoll on `event_fd_dpm` (below) |
| **`write`** | sends immediately | **buffers**; auto-flushed by the next `read_dpm` (client) / `close_dpm` (server) | socket-style `write`→`read` / `write`→`close`; `flush_dpm` is optional (only the pipelined epoll client needs it) |
| **One-way** | first-class (`write`-only, `shutdown`) | **send-only is first-class** (`write`→`close`, no read) | fire-and-forget: the TX slot is freed by the DPU ACK (no leak); any reply the peer sends is silently dropped |
| **epoll** | the fd itself is pollable | **native epoll on `event_fd_dpm(s)`** — a real fd you register like a listen socket | no `epoll_*_dpm` wrappers; it mixes with real sockets in one epoll set |

**Must-follow rules**
- **Exactly one `close_dpm()` per conn** — every `accept`/`connect` conn, including the
  error/`ECONNRESET` path (close reclaims the pending entry and the held slots).
- **One conn = one outstanding exchange.** A client `write` while a request is still
  in flight (response not yet read) fails `EINVAL`. For concurrency use more conns.
- **`dst_pod_id` must be a live, registered pod** `[0,127]`. A dead/unregistered target
  is *not* detected at `connect` — the request is dropped at the DPU and no response
  arrives, so `read_dpm` stays `EAGAIN`. **Apply your own wall-clock timeout.**
- **Thread-safety:** the `dpm_t` endpoint is shared/thread-safe (run the accept loop on
  N threads); a single `dpmconn_t` is single-thread.
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
| `dpm_t *socket_dpm(const char *app_name, int pod_id)` | Endpoint handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address. |
| `void destroy_dpm(dpm_t *s)` | — (releases all DOCA resources; safe on `NULL`). |
| `int event_fd_dpm(dpm_t *s)` | A real fd that becomes **readable** when an inbound request/response is delivered, for NATIVE epoll/poll/select; `-1` if unavailable. |
| `int pod_id_dpm(dpm_t *s)` / `int msg_max_dpm(dpm_t *s)` | This node's `pod_id` / the max body size (`slot_size`). |

> **Env:** `DPUMESH_POD_ID` overrides `pod_id`; `DPUMESH_PCI_ADDR` selects the DOCA
> device (default `94:00.0`). `DPUMESH_HOST_EPOLL=1` makes the library's internal
> RX-progress thread **sleep on the DOCA notification fd** (idle CPU ~0); the default
> (`0`) is an adaptive busy-poll (low but non-zero idle CPU).

### Accept / connect
| Function | Returns / errno |
|---|---|
| `dpmconn_t *accept_dpm(dpm_t *s)` | New **server** conn holding the next request (body ready), or `NULL`+`EAGAIN` if none pending. **Non-blocking.** (A `NULL` with `errno=ENOMEM` is a rare conn-alloc failure: the request was dropped, its RX slot reclaimed; an accept-until-NULL loop treats it as drained — safe, it just skips one.) |
| `dpmconn_t *connect_dpm(dpm_t *s, int dst_pod_id)` | New **client** conn bound to `dst_pod_id`. No round-trip — a dead/unregistered target is **not** detected here (its response never arrives → `read_dpm` stays `EAGAIN`; apply your own timeout). `NULL`+`ENOMEM` on OOM. |

### Read / write / sendfile / flush
| Function | Returns / errno |
|---|---|
| `ssize_t read_dpm(dpmconn_t *c, void *buf, size_t len)` | `>0` bytes copied from the inbound body; `0` = end of message; `-1` = would-block (client response not in yet, `EAGAIN`) or abandoned (`ECONNRESET`). On a **client** conn the first `read` auto-flushes the buffered request (implicit send). |
| `ssize_t write_dpm(dpmconn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len`. On a **reused** client conn (its response already read) the first `write` auto-starts a NEW request. `-1` = would exceed `slot_size` (`EMSGSIZE`), or a `write` while a request is still outstanding / a 2nd reply on a server conn (`EINVAL`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t sendfile_dpm(dpmconn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the body (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. Returns bytes appended (`0` = EOF), `-1` on read error / already-outstanding (`EINVAL`) / no slot room (`EMSGSIZE`). |
| `int flush_dpm(dpmconn_t *c)` | **Optional on the happy path** (read/close auto-flush). It is the **explicit ship**: needed only when you ship *before* reading — a pipelined client that fires a window, then harvests via epoll. `0` sent; `-1` with **`EINVAL`** = already sent (not retryable); **`EAGAIN`** = rare `req_id`-table collision, retryable (body kept) but it returns only after a ~2 s internal wait, so a retry is not a fast spin; **`EBADMSG`** = permanent descriptor fault (not retryable; close the conn — unreachable via the façade). |
| `int close_dpm(dpmconn_t *c)` | Ships any buffered-but-unsent message, then frees the conn's slots/pending. `0` ok; **`-1` if that final ship FAILED** (rare `req_id` collision) — the conn is freed either way, so `-1` is *not* retryable. Safe on `NULL`. **ONE-WAY:** a client `write → close` *without a read* is fire-and-forget — the TX slot is freed by the DPU's TX_ACK (no leak) and any reply is silently dropped. For a server reply you must not lose, `flush_dpm()` (retry on `EAGAIN`) before `close_dpm()`. |

**Lifecycles:**
```
server:  c = accept_dpm(s);   read_dpm(c,…)…(EOF);  write_dpm(c,…)…;  close_dpm(c);   // close ships the reply
client:  c = connect_dpm(s,dst);
  RPC (reusable):  for each call: write_dpm(c,…)…;  read_dpm(c,…)…;     // read ships + returns the reply
                   …; close_dpm(c);                                     // once, at the end
  one-way:         write_dpm(c,…)…;  close_dpm(c);                      // fire-and-forget (no read)
```

### Accessors
`int is_server_dpm(dpmconn_t*)`, `int32_t peer_dpm(dpmconn_t*)` (the conn's peer pod),
`void set_data_dpm(dpmconn_t*, void*)` / `void *get_data_dpm(dpmconn_t*)` (mirrors
`epoll_event.data.ptr` — handy to find "which conn" in an epoll loop).

---

## 4. Examples

### 4a. Echo server — the natural accept-loop (no epoll needed)
Because a request arrives atomically, the kernel server's READ/SEND_HDR/SEND_BODY state
machine collapses into accept → handle → reply.
```c
#include <thrift/transport/dpm.h>

dpm_t *s = socket_dpm("echo", /*pod_id*/11);
for (;;) {
    dpmconn_t *c = accept_dpm(s);          // non-blocking
    if (!c) { /* errno==EAGAIN: idle — sched_yield() or do other work */ continue; }

    char buf[8192]; ssize_t n, off = 0;
    while ((n = read_dpm(c, buf + off, sizeof buf - off)) > 0) off += n;   // whole request
    write_dpm(c, buf, off);                 // echo it back (<= 8 KB)
    close_dpm(c);                            // ships the reply + frees the RX slot
}
```
*(For throughput, run this loop on N threads — the endpoint is shared and thread-safe.)*

> **Delivery:** `close_dpm` auto-ships the reply but can only *report* a ship failure
> via its return (`-1`), not retry it. If losing a reply is unacceptable, ship first:
> `while (flush_dpm(c) < 0 && errno == EAGAIN) sched_yield();` then `close_dpm(c)`.
> (`EAGAIN` here is the rare `req_id` collision and already includes a ~2 s wait, so
> the loop is not a busy-spin.)

### 4b. Client — reusable RPC conn (request → response, repeated)
Connect once, then loop `write → read`; the conn auto-starts a new request each time.
Apply a wall-clock timeout so a dead pod (no response ever) can't hang you.
```c
#include <thrift/transport/dpm.h>
#include <time.h>

dpm_t *s = socket_dpm("client", /*pod_id*/10);
dpmconn_t *c = connect_dpm(s, /*dst_pod_id*/11);

for (int i = 0; i < N; i++) {
    write_dpm(c, req[i], req_len[i]);            // buffer; the read below ships it
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    char resp[8192]; ssize_t n;
    for (;;) {
        n = read_dpm(c, resp, sizeof resp);
        if (n >= 0) break;                       // got it (n bytes; 0 = empty reply)
        if (errno != EAGAIN) break;              // ECONNRESET: pending reclaimed (not peer death)
        struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - t0.tv_sec >= 5) break;  // dead/unregistered pod → EAGAIN forever; give up
        sched_yield();
    }
    /* ... handle resp; the next write starts a fresh request on this same conn ... */
}
close_dpm(c);   // once, at the end
```

### 4c. Client — one-way (fire-and-forget)
`write → close` with no read. The request is shipped; the TX slot is freed by the DPU's
TX_ACK (no leak); any reply the peer sends is silently dropped. (A one-way conn can't be
reused — close it; `connect_dpm` is free.)
```c
for (int i = 0; i < N; i++) {
    dpmconn_t *c = connect_dpm(s, /*dst_pod_id*/11);
    write_dpm(c, msg[i], msg_len[i]);
    close_dpm(c);                           // fire-and-forget
}
```

### 4d. Client — many in-flight via native epoll (pipelined window)
Fire a window of W requests (each its own conn) and harvest via native epoll on
`event_fd_dpm(s)`. Two things matter: **(1) flush explicitly** — you sleep on
`epoll_wait` *before* reading, so you can't rely on read-triggered implicit send;
**(2)** `event_fd_dpm` is **per-endpoint**, so a readiness event doesn't say *which*
conn — tag each with `set_data_dpm` and scan your in-flight conns on wakeup.
```c
#include <thrift/transport/dpm.h>
#include <sys/epoll.h>
#include <time.h>
#define W 64                                        // up to W requests in flight

dpm_t *s = socket_dpm("client", /*pod_id*/10);
int dfd  = event_fd_dpm(s);
int epfd = epoll_create1(0);
struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

dpmconn_t *inflight[W] = {0};
time_t launched[W] = {0};
int pending = 0;

for (int i = 0; i < W; i++) {                        // ── launch the window ──
    dpmconn_t *c = connect_dpm(s, /*dst_pod_id*/11);
    set_data_dpm(c, (void *)(intptr_t)i);            // tag it (e.g. the request index)
    write_dpm(c, req[i], req_len[i]);
    while (flush_dpm(c) < 0 && errno == EAGAIN) sched_yield();   // ship NOW (we'll sleep on epoll)
    inflight[i] = c; launched[i] = time(NULL); pending++;
}

struct epoll_event events[8];
while (pending > 0) {                                 // ── harvest via epoll ──
    int nfds = epoll_wait(epfd, events, 8, 1000);    // SLEEP, ≤1s tick so dead pods can time out
    for (int e = 0; e < nfds; e++) {
        if (events[e].data.fd != dfd) continue;
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}   // drain readiness fd

        for (int i = 0; i < W; i++) {                // per-endpoint fd → scan in-flight conns
            dpmconn_t *c = inflight[i];
            if (!c) continue;
            char resp[8192];
            ssize_t n = read_dpm(c, resp, sizeof resp);
            if (n < 0 && errno == EAGAIN) continue;  // this one's reply not in yet
            int idx = (int)(intptr_t)get_data_dpm(c);
            handle_response(idx, resp, n);           // n>=0 = reply (0 = empty); n<0 = ECONNRESET
            close_dpm(c); inflight[i] = NULL; pending--;
        }
    }
    for (int i = 0; i < W; i++)                       // abandon dead-pod conns (EAGAIN never clears)
        if (inflight[i] && time(NULL) - launched[i] >= 5) {
            close_dpm(inflight[i]); inflight[i] = NULL; pending--;   // count as a timeout failure
        }
}
```
> A reply is a whole ≤8 KB message, so one `read_dpm` returns it in full. For a large
> window, replace the linear scan with your own bookkeeping — the readiness fd only says
> "something arrived," not *which* conn.

### 4e. NATIVE epoll server — a normal epoll loop, only the data calls suffixed
This is `bench/echo_sock.c`. **Validated 0-fail across a warm-ramped 30K→240K**; the
stable operating point is **≤235K** (240K is the warm-only saturation knee, op-rate
ceiling ~257K — always ramp up; cold-jumping to/over the knee can transiently wedge the
load path). **Idle CPU ~1.2% requires `DPUMESH_HOST_EPOLL=1`** (the default
adaptive-busy-polls the internal RX thread). The epoll machinery is **stock kernel
epoll**; `event_fd_dpm(s)` is the "listen socket".
```c
dpm_t *s = socket_dpm("echo", 11);
int dfd  = event_fd_dpm(s);                              // the DPUmesh readiness fd

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
        while ((c = accept_dpm(s)) != NULL) {            // accept every queued request
            char b[8192]; ssize_t off = 0, r;
            while ((r = read_dpm(c, b + off, sizeof b - off)) > 0) off += r;   // full request
            if (off > 0) write_dpm(c, b, off);           // echo it back
            close_dpm(c);                                 // ships it (implicit send)
        }
    }
}
```
> The diff from an ordinary TCP epoll echo server is **only** the `_dpm` suffix on
> `socket/accept/read/write/close` (no extra send — `close` auto-flushes the reply) and
> using `event_fd_dpm(s)` as the listen fd. The epoll loop is unchanged.

### 4f. Porting an ordinary epoll HTTP server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost 1:1 — and
gets **simpler**, because the message is atomic (no `STATE_SEND_HDR`/`STATE_SEND_BODY`,
no partial-write retry). The full request is in hand at accept; build the whole response
with `write_dpm`/`sendfile_dpm`, and `close_dpm()` ships it:
```c
dpm_t *s = socket_dpm("httpd", pod_id);
for (;;) {
    dpmconn_t *c = accept_dpm(s);
    if (!c) { sched_yield(); continue; }

    char req[8192]; ssize_t off = 0, n;
    while ((n = read_dpm(c, req + off, sizeof req - off)) > 0) off += n;   // full request header

    if (parse_request(req, off) < 0) {
        write_dpm(c, ERR_400, strlen(ERR_400));
    } else {
        int fd = open_file(url);
        if (fd < 0) write_dpm(c, ERR_404, strlen(ERR_404));
        else if (flen + 256 > (long)msg_max_dpm(s))      // header + body won't fit one ≤8KB msg
            write_dpm(c, ERR_500, strlen(ERR_500));      // (or chunk across round-trips)
        else {
            char hdr[256];
            int hl = snprintf(hdr, sizeof hdr, "HTTP/1.0 200 OK\r\nContent-Length: %ld\r\n\r\n", flen);
            write_dpm(c, hdr, hl);
            ssize_t w = sendfile_dpm(c, fd, NULL, flen);   // capped at slot_size → may be SHORT
            close(fd);
            if (w < (ssize_t)flen) { /* body didn't fit one message — chunk or send an error */ }
        }
    }
    close_dpm(c);   // ships the one atomic response (implicit send)
}
```
> **Caveat (8 KB cap):** header + file body must fit in one `slot_size` (≤ 8 KB) message.
> For larger files chunk at the app layer (multiple round-trips) — DPUmesh has no
> multi-segment streaming.
