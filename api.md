# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on
the **BlueField DPU/DPA**, not host CPU, so the application keeps its full host
core (no in-host sidecar tax). Two pods exchange messages **host → DPU → host**;
the DPU routes on `dst_pod_id` (metadata only — it never reads the body) and the
DPA EU performs the DMA copies.

The public API is shaped like **BSD sockets + epoll**, so an ordinary non-blocking
epoll server/client ports by swapping each call for its `_dpm` twin
(`read` → `read_dpm`, …). Header: `thrift/transport/dpm.h` (header-only, built on
the C core `dpumesh.h`).

---

## 1. Concepts — the handles, and "a pair, not a connection"

You touch only **two handles** (a third, the engine, is hidden inside the first):

| Handle | What it is | Analogy | Lifetime / count |
|---|---|---|---|
| **`dpm_t`** (endpoint) | The bound, listening endpoint. Wraps the whole DPU backend — DOCA device, the RX poller thread, the slot pool, the link to the DPU. Created by `socket_dpm()`. | a **phone installed in your house** | **one per process**, lives the whole run; **heavy** to create |
| **`dpmconn_t`** (conn) | **One request ↔ one response** conversation. Created by `accept_dpm()` (server, incoming) or `connect_dpm()` (client, outgoing). | **one phone call** | **one per request**, single-shot — use once, `close_dpm()`, throw away; **cheap** |
| `dpumesh_ctx_t` (engine) | The low-level DOCA context that actually moves the bytes. A `dpm_t` holds one internally; **you normally never touch it.** | the **phone line / exchange** behind the wall | one per `dpm_t` |

> You only ever hold `dpm_t` and `dpmconn_t`. `dpumesh_ctx_t` is the engine the
> façade hides inside `dpm_t` (it's the low-level `dpumesh.h` core — the façade is
> a thin wrapper over it).

### A `dpmconn_t` is **not a connection** — it is one request↔response *pair*

This is the single most important difference from sockets. There is **no persistent
channel and no handshake**:

- `connect_dpm()` does **no round-trip** — it just labels "this call targets pod 11."
- A request is fired; the matching response comes back, **correlated by an internal
  request-id** (not by arrival order); then the conn is done.
- So it behaves like an **RPC call**, not a TCP stream. Many pairs can be in flight at
  once and **arrive out of order** — each is matched by its id, so order doesn't matter.

```c
dpm_t *s = socket_dpm("myapp", 11);   // ① install the phone (once per process)
for (;;) {
    dpmconn_t *c = accept_dpm(s);     // ② one incoming call = a request↔response pair
    read_dpm(c, ...);                 //    read the request
    write_dpm(c, ...);                //    write the reply
    close_dpm(c);                     // ③ hang up (this c is done) — s lives on
}
```

---

## 2. How it differs from BSD sockets (the limits)

DPUmesh trades the socket's generality for a lean DPU data path. Versus `read`/`write`/`epoll`:

| | BSD socket | DPUmesh | What it means for you |
|---|---|---|---|
| **Channel** | a **connection** — persistent, bidirectional byte stream | a **request↔response pair**, single-shot | reuse = `close_dpm()` + a new `connect`/`accept`; no keep-alive on one conn |
| **Setup** | 3-way **handshake** | **none** — `connect_dpm` is local, no round-trip | like UDP/RPC: fire-and-match; peer liveness is *not* checked at connect |
| **Framing** | byte stream (you frame it yourself) | **message** — one request, one response, delivered **atomically** | no partial-read loop, no `EPOLLOUT` body dance — the whole body is in hand at `accept`/`read` |
| **Size** | unbounded stream | **≤ 8 KB per message** (the DPA `dma_copy` hard limit) | larger payloads must be chunked across round-trips |
| **Ordering** | in-order within a connection | each pair independent, **matched by request-id** | out-of-order arrival across pairs is fine; there is no cross-message stream order |
| **Address** | IP:port | **`pod_id`** integer `[0,127]`, registered with the DPU | no DNS / name resolution |
| **Blocking** | blocking *or* non-blocking | **non-blocking only** — RX polls; TX busy-spins under local saturation (never blocks on the peer) | to *sleep* until ready, use native epoll on `event_fd_dpm` (below) |
| **`write`** | sends immediately | **buffers**, auto-flushed by the next `read_dpm` (client) / `close_dpm` (server) | socket-style `write`→`read` / `write`→`close`; `send_dpm` is optional |
| **One-way** | first-class (`write`-only, `read`-only, `shutdown`) | **shaped for the pair** — pure send-only / receive-only is **not first-class** | a clean one-way send needs a dedicated path (ask if you need it); today a peer's reply to a non-waiting sender is generated and dropped |
| **epoll** | the fd itself is pollable | **native epoll on `event_fd_dpm(s)`** — a real fd you register like a listen socket | no `epoll_*_dpm` wrappers; it mixes with real sockets in one epoll set |

**Must-follow rules**
- **Exactly one `close_dpm()` per conn** — every `accept`/`connect` conn, including the error/`ECONNRESET` path (close reclaims the pending entry).
- **`dst_pod_id` must be a live, registered pod** `[0,127]`.
- **Thread-safety:** the `dpm_t` endpoint is shared/thread-safe (run the accept loop on N threads); a single `dpmconn_t` is single-thread.
- **`slot_size` ≤ 8192** — it is *not* host-clamped; a larger value silently drops messages at the DPA.

---

## 3. API reference

All calls are **non-blocking**. "would-block" = the listed sentinel **with `errno=EAGAIN`**.

### Endpoint (socket + bind + listen, folded)
| Function | Returns / errno |
|---|---|
| `dpm_t *socket_dpm(const char *app_name, int pod_id)` | Endpoint handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address. |
| `void destroy_dpm(dpm_t *s)` | — (releases all DOCA resources; safe on `NULL`). |

> **Env:** `DPUMESH_POD_ID` overrides `pod_id`; `DPUMESH_PCI_ADDR` selects the DOCA device (default `94:00.0`).
> `DPUMESH_HOST_EPOLL=1` makes the library's internal RX-progress thread **sleep on the DOCA notification fd**
> (idle CPU ~0). **The default (`0`) is an adaptive busy-poll** — low but non-zero idle CPU.

### Accept / connect
| Function | Returns / errno |
|---|---|
| `dpmconn_t *accept_dpm(dpm_t *s)` | New **server** conn holding the next request (body ready), or `NULL`+`EAGAIN` if none pending. **Non-blocking.** |
| `dpmconn_t *connect_dpm(dpm_t *s, int dst_pod_id)` | New **client** conn targeting `dst_pod_id`. No round-trip (just binds the target) — a **dead/unregistered** `dst_pod_id` is *not* detected here: the request is later dropped at the DPU and no response arrives, so `read_dpm` stays `EAGAIN` (apply your own timeout). `NULL` on OOM. |

### Read / write / send
| Function | Returns / errno |
|---|---|
| `ssize_t read_dpm(dpmconn_t *c, void *buf, size_t len)` | `>0` bytes copied from the inbound body; `0` = end of message; `-1` = would-block (client response not in yet, `EAGAIN`) or abandoned (`ECONNRESET`). On a **client** conn, the first `read` auto-flushes the buffered request (implicit send). |
| `ssize_t write_dpm(dpmconn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len` (auto-flushed later by `read`/`close`); `-1` = would exceed `slot_size` (`EMSGSIZE`) or conn already sent (`EINVAL`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t sendfile_dpm(dpmconn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the body (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. Returns bytes appended (`0` = EOF), `-1` on read error, already-sent (`EINVAL`), or no remaining slot room (`EMSGSIZE`). |
| `int send_dpm(dpmconn_t *c)` | **Optional on the happy path** (read/close auto-flush), but it is the **only way to CONFIRM / RETRY delivery**: a transient `-1` (`EAGAIN`) can be retried with the body still buffered, whereas `close_dpm`'s auto-flush can only *report* a failure (conn already freed). Also flushes early (pipelined client). `0` sent; `-1` = already sent (`EINVAL`), a rare `req_id` collision (`EAGAIN`, ~2 s — retry), or an enqueue error (body retained). |
| `int close_dpm(dpmconn_t *c)` | Ships any buffered-but-unsent message, then frees the conn's slots/pending. `0` ok; **`-1` if that final ship FAILED** (rare `req_id` collision) — the conn is freed either way, so `-1` is *not* retryable. Safe on `NULL`. **For a server response you must not lose, call `send_dpm()` (retry on `EAGAIN`) before `close_dpm()`.** |

**Lifecycles (socket-style — no explicit send needed):**
```
server:  c = accept_dpm(s);  read_dpm(c,…)…(EOF);  write_dpm(c,…)…;  close_dpm(c);  // close ships it
client:  c = connect_dpm(s,dst);  write_dpm(c,…)…;  // then later:
         while (read_dpm(c,buf,len) < 0 && errno==EAGAIN) {/* poll / epoll_wait */}  …;  close_dpm(c);
         // the first read_dpm() ships the buffered request (implicit send); send_dpm(c) is optional.
```

### Event-loop readiness fd (for NATIVE epoll/poll/select)
| Function | Returns |
|---|---|
| `int event_fd_dpm(dpm_t *s)` | A real fd that becomes **readable** whenever an inbound request/response is delivered, or `-1` if unavailable. Register it in your own `epoll`/`poll`/`select` like a listen socket. |

> **Usage:** register `event_fd_dpm(s)` with `EPOLLIN`. On a readiness event, **drain**
> it (`while (read(dfd,&u64,8) > 0) {}`), then `accept_dpm(s)` in a loop until it returns
> `NULL` (server), or `read_dpm()` your client conns. The fd is raised per delivery and is
> **notification-driven**, so *your* `epoll_wait` sleeps until activity. It mixes freely with
> real sockets in one epoll set. (The library's *internal* RX-progress thread adaptive-busy-polls
> **by default**; set `DPUMESH_HOST_EPOLL=1` to make it sleep on the DOCA notification fd → idle CPU ~0.)
>
> **Multiple client conns:** `event_fd_dpm(s)` is **per-endpoint** (one fd for all conns), so a
> readiness event does not say *which* conn — keep your in-flight conns in a list (tag each with
> `set_data_dpm`) and `read_dpm()` each on wakeup; the ready one returns data, the rest `EAGAIN`.

Accessors: `int pod_id_dpm(dpm_t*)`, `int msg_max_dpm(dpm_t*)` (= `slot_size`),
`int is_server_dpm(dpmconn_t*)`, `int32_t peer_dpm(dpmconn_t*)` (the conn's peer pod),
`void set_data_dpm(dpmconn_t*, void*)` / `void *get_data_dpm(dpmconn_t*)`
(mirrors `epoll_event.data.ptr` — handy to find "which conn" in an epoll loop).

---

## 4. Examples

### 4a. Echo server — the natural accept-loop (no epoll needed)
Because a request arrives atomically, the kernel server's READ/SEND_HDR/SEND_BODY
state machine collapses into accept → handle → reply.
```c
#include <thrift/transport/dpm.h>

dpm_t *s = socket_dpm("echo", /*pod_id*/11);
for (;;) {
    dpmconn_t *c = accept_dpm(s);          // non-blocking
    if (!c) { /* errno==EAGAIN: idle — sched_yield() or do other work */ continue; }

    char buf[8192]; ssize_t n, off = 0;
    while ((n = read_dpm(c, buf+off, sizeof buf-off)) > 0) off += n;   // whole request
    write_dpm(c, buf, off);                 // echo it back (<= 8 KB)
    close_dpm(c);                            // ships the reply + frees the RX slot (no explicit send)
}
```
*(For throughput, run this loop on N threads — the endpoint is shared and thread-safe.)*

> **Delivery:** `close_dpm` auto-ships the reply but can only *report* a ship failure via its
> return (`-1`), not retry it. If losing a reply is unacceptable, ship explicitly first:
> `while (send_dpm(c) < 0 && errno == EAGAIN) sched_yield();` then `close_dpm(c)`.

### 4b. Client (request → response)
```c
dpm_t *s = socket_dpm("client", /*pod_id*/10);
dpmconn_t *c = connect_dpm(s, /*dst_pod_id*/11);

write_dpm(c, "ping", 4);                 // buffered; the first read ships it (implicit send)

char resp[8192]; ssize_t n;
for (;;) {                                   // poll for the response (non-blocking)
    n = read_dpm(c, resp, sizeof resp);
    if (n >= 0) break;                       // got it (n bytes; 0 = empty reply)
    if (errno != EAGAIN) { /* ECONNRESET: lost */ break; }
    sched_yield();
}
close_dpm(c);   // REQUIRED on every path — also reclaims the pending entry on ECONNRESET
```

### 4c. NATIVE epoll server — a normal epoll loop, only the data calls suffixed
This is `bench/echo_sock.c` (validated: 240K RPS, 0-fail; **idle CPU ~1% requires
`DPUMESH_HOST_EPOLL=1`** — the default adaptive-busy-polls the internal RX thread). The epoll
machinery is **stock kernel epoll**; `event_fd_dpm(s)` is the "listen socket".
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
            while ((r = read_dpm(c, b+off, sizeof b-off)) > 0) off += r;   // full request
            if (off > 0) write_dpm(c, b, off);           // echo it back
            close_dpm(c);                                 // ships it (implicit send)
        }
    }
}
```
> The diff from an ordinary TCP epoll echo server is **only** the `_dpm` suffix on
> `socket/accept/read/write/close` (no extra send — `close` auto-flushes the reply) and
> using `event_fd_dpm(s)` as the listen fd. The epoll loop is unchanged.

### 4d. Porting an ordinary epoll HTTP server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost
1:1 — and gets **simpler**, because the message is atomic (no `STATE_SEND_HDR`/
`STATE_SEND_BODY`, no partial-write retry). The full request is in hand at accept;
build the whole response with `write_dpm`/`sendfile_dpm`, and `close_dpm()` ships it:
```c
dpm_t *s = socket_dpm("httpd", pod_id);
for (;;) {
    dpmconn_t *c = accept_dpm(s);
    if (!c) { sched_yield(); continue; }

    char req[8192]; ssize_t off = 0, n;
    while ((n = read_dpm(c, req+off, sizeof req-off)) > 0) off += n;   // full request header

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
> **Caveat (8 KB cap):** header + file body must fit in one `slot_size` (≤ 8 KB)
> message. For larger files you must chunk at the app layer (multiple
> request/response round-trips) — DPUmesh has no multi-segment streaming.
