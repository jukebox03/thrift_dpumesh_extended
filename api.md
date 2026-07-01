# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on the
**BlueField DPU/DPA**, not the host CPU, so your application keeps its full host core (no
in-host sidecar tax). Pods exchange messages **host → DPU → host**: you address a
**`service_id`**, the DPU resolves it to a backend pod (metadata only — it never reads the
body), and the DPA EU performs the DMA copies.

It is a **connection-oriented, full-duplex, message transport** — close to TCP in shape,
but message-framed instead of a byte stream, and **NOT** request/response:

- A **`dmesh_conn_t`** is a persistent connection between a local endpoint and one peer,
  alive until either side closes. Once established, **both sides send and receive freely**;
  the transport does **no** request↔response matching (that is the app's job if it wants
  RPC).
- Each message is **one whole body ≤ 8 KB**, delivered **atomically** and **in send order
  per connection** (no partial-read loop, no `EPOLLOUT` dance).
- Readiness is a **single fd** per endpoint plus a **ready list**: the DPU-facing poller
  names exactly which connections have inbound, so you never scan all conns and never hold
  a per-conn fd.

The API wears a **BSD-sockets + epoll shape** (`dmesh_create_channel`/`dmesh_accept`/
`dmesh_connect`/`dmesh_read`/`dmesh_write`/`dmesh_close`), and most socket semantics now
**carry over**: persistent full-duplex connections, `read()==0` as EOF, in-order delivery.
What does **not** carry over: it is **message-framed** (atomic ≤ 8 KB, not a byte stream),
addressing is a **`service_id`** the DPU routes (no IP/DNS), and readiness is **one endpoint
fd + a ready list**, not one pollable fd per connection.

Header: `thrift/transport/dpm.h` (header-only, built on the C core `dpumesh.h`).

---

## 1. Concepts — two handles

You touch only **two handles** (a third, the engine, is hidden inside the first):

| Handle | What it is | Lifetime / count |
|---|---|---|
| **`dmesh_channel_t`** (endpoint) | The bound endpoint. Wraps the whole DPU backend — DOCA device, the RX poller (PE) thread, the slot pool, the link to the DPU. Created by `dmesh_create_channel()`. | **one per process**, lives the whole run; **heavy** to create |
| **`dmesh_conn_t`** (connection) | A **persistent full-duplex link to one peer** (a local port ↔ a peer `(pod,port)`). Created by `dmesh_accept()` (an inbound connection) or `dmesh_connect()` (outbound to a **service**). Alive until `dmesh_close()`. | **cheap**; reused for the whole conversation |
| `dpumesh_ctx_t` (engine) | The low-level DOCA context that moves the bytes. A `dmesh_channel_t` holds one internally; **you never touch it.** | one per `dmesh_channel_t` |

> You only ever hold `dmesh_channel_t` and `dmesh_conn_t`. `dpumesh_ctx_t` is the engine the
> façade hides inside `dmesh_channel_t` (the low-level `dpumesh.h` core — the façade is a thin
> wrapper).

### A `dmesh_conn_t` is a persistent, full-duplex connection

- `dmesh_connect(s, service)` is **local** — no round-trip. It binds "this conn talks to
  service 11." The **first** `write+flush` is the establishing message: it goes out with
  `dst_pod = BLANK`, and the DPU resolves `service → backend pod` (the L7/LB seam). The
  connection then **sticks** to that backend.
- The destination `dmesh_accept()`s the establishing message → a connection that has
  **learned the peer** `(pod, port)`. The connecting side learns the backend `(pod, port)`
  from its **first inbound** and addresses it directly thereafter.
- After that, **either side may `write`→`flush` any number of messages and `dmesh_read`
  whatever arrives**, in send order, until close. (The accept side cannot send before it has
  received the first message — it has no peer to address until then.)
- **Teardown:** `dmesh_close()` sends a **FIN** (a zero-length message that rides behind all
  prior data on the conn); the peer's `dmesh_read` then returns **0 (EOF)**, and it closes
  too, reclaiming its slot. `read()==0` ⇒ the peer closed ⇒ you close.

```c
// Persistent full-duplex conn: connect once, exchange freely, close at the end.
dmesh_channel_t *s = dmesh_create_channel("myapp", 10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);
for (int i = 0; i < N; i++) {
    dmesh_write(c, msg, len);              // buffer a message
    dmesh_flush(c);                        // ship it (read/close do NOT auto-send)
    char in[8192];
    ssize_t n = dmesh_read(c, in, sizeof in);   // -1/EAGAIN until something arrives; 0 = peer closed
    /* ... a whole message in hand (n bytes) ... */
}
dmesh_close(c);                            // sends a FIN so the peer reclaims its side
```

---

## 2. How it differs from BSD sockets

The API *shape* ports and most *semantics* now port too; the rows below are where DPUmesh
trades the socket's generality for a lean DPU data path.

| | BSD socket | DPUmesh | What it means for you |
|---|---|---|---|
| **Connection** | persistent, full-duplex byte stream | persistent, full-duplex **message** stream | same mental model; you frame nothing — messages are atomic |
| **Setup** | 3-way **handshake** | **none** — `dmesh_connect` is local; the first `write+flush` establishes (DPU routes the service) | peer liveness is *not* checked at connect; apply your own timeout |
| **Framing** | byte stream (you frame it) | **message** — one whole body, delivered **atomically** | no partial-read loop, no `EPOLLOUT` body dance — the whole body is in hand at `read` |
| **Size** | unbounded stream | **≤ 8 KB per message** (the DPA `dma_copy` hard limit) | larger payloads must be chunked at the app layer |
| **Ordering** | in-order within a connection | **in-order within a connection** (conn-sharding keeps a conn on one EU) | same as a socket; no cross-connection ordering |
| **EOF** | `read()==0` | **`read()==0`** — the peer sent a FIN (`dmesh_close`) | identical to a socket: `read()==0` ⇒ close your side |
| **Address** | IP:port | **`service_id`** integer — the DPU resolves it to a backend pod (connection-sticky) | name→backend routing on the DPU; self-routing OK; no DNS |
| **Blocking** | blocking *or* non-blocking | **non-blocking only** — RX delivers to a per-conn inbox; TX busy-spins under local saturation (never blocks on the peer) | to *sleep* until ready, native-epoll on `dmesh_event_fd` (below) |
| **`write`** | sends immediately | **buffers only**; `dmesh_flush` ships it | always `write`→`flush`; an empty flush is a no-op |
| **Readiness** | one pollable fd **per** connection | **ONE endpoint fd** + a **ready list** (`dmesh_next_ready`) | no per-conn fd, no scan — the DPU names the ready conns for you |

**Must-follow rules**
- **A conn is full-duplex and persistent.** Either side `write`→`flush`es freely (no single-
  outstanding restriction) and `dmesh_read`s whatever arrives, in order, until close. The
  transport does **no** request↔response matching — pair replies to requests yourself if you
  need RPC.
- **Establish (one round-trip) before pipelining.** A fresh client conn learns its peer only
  when it *reads* its first inbound, so its first `flush` goes out unrouted (`dst_pod=BLANK`,
  the DPU resolves the service). If you `flush` many messages **before** reading the first
  reply, every one ships `BLANK` and lands as a **new connection** on the server (accept-queue
  flood). Send one, read its reply, *then* pipeline as deep as you like on that conn.
- **`read()==0` is EOF.** It means the peer closed (sent a FIN). Respond by `dmesh_close()`.
  A *user* zero-length send is a no-op (zero length on the wire is reserved for the FIN), so
  `read()` never returns 0 except at EOF.
- **Exactly one `dmesh_close()` per conn** — it sends a FIN (if established) so the peer
  reclaims its slot, then frees the local slots/readiness state. Close what you
  `accept`/`connect`, including the abandoned path. Concurrent close is safe (a FIN landing
  on an already-freed conn is dropped).
- **Drain a ready conn to EAGAIN.** When `dmesh_next_ready` hands you a conn, `dmesh_read` it
  until `-1`/`EAGAIN` — the ready list re-arms a conn only on its inbox empty→non-empty edge,
  so a half-drained conn would stall until its next message.
- **`service_id` must be a live, registered service.** A dead/unregistered service is *not*
  detected at `connect` — the message is dropped at the DPU and nothing comes back, so
  `dmesh_read` stays `EAGAIN`. **Apply your own wall-clock timeout.** (Self-routing is allowed:
  a service may resolve to your own pod.)
- **Thread-safety:** the `dmesh_channel_t` endpoint is shared/thread-safe; a single
  `dmesh_conn_t` and the `dmesh_accept`/`dmesh_next_ready` event loop are **single-thread**
  (run one event loop; fan work out to worker threads by handing off whole conns).
- **`slot_size` ≤ 8192.** The 8 KB cap is the DPA `dma_copy` limit. A larger `DPUMESH_SLOT_SIZE`
  is **not** host-clamped — `write` would accept it but the DPA drops the excess and it breaks
  the `num_slots × slot_size = DPU_BUFFER_SIZE` admission invariant. Leave it at the default.

---

## 3. API reference

All calls are **non-blocking**. "would-block" = the listed sentinel **with `errno=EAGAIN`**.

### Endpoint (socket + bind + listen, folded)
| Function | Returns / errno |
|---|---|
| `dmesh_channel_t *dmesh_create_channel(const char *app_name, int pod_id)` | Endpoint handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address. |
| `void dmesh_destroy_channel(dmesh_channel_t *s)` | — (releases all DOCA resources; safe on `NULL`). |
| `int dmesh_event_fd(dmesh_channel_t *s)` | The **one** endpoint readiness fd, for NATIVE epoll/poll/select. Becomes readable when a new connection is pending **or** any connection has inbound. `-1` if unavailable. **Calling it enables readiness** — call it once at startup; a purely busy-polling app that never calls it must poll its conns itself. |
| `int dmesh_pod_id(dmesh_channel_t *s)` / `int dmesh_msg_max(dmesh_channel_t *s)` | This node's `pod_id` / the max body size (`slot_size`). |

> **Env:** `DPUMESH_POD_ID` overrides `pod_id`; `DPUMESH_PCI_ADDR` selects the DOCA device.
> `DPUMESH_HOST_EPOLL=1` makes the library's internal RX (PE) thread **sleep on the DOCA
> notification fd** (idle CPU ~0); the default adaptive busy-polls (low but non-zero idle CPU).

### Accept / connect / readiness
| Function | Returns / errno |
|---|---|
| `dmesh_conn_t *dmesh_accept(dmesh_channel_t *s)` | Next **incoming** connection, holding its first message (body ready) and having learned the peer `(pod,port)`; or `NULL`+`EAGAIN` if none pending. **Non-blocking.** (`NULL`+`ENOMEM` is a rare conn-alloc failure: the message is dropped, its RX credit reclaimed; an accept-until-NULL loop just skips it.) |
| `dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int service_id)` | New **outbound** connection bound to a **`service_id`**. No round-trip — the first `write+flush` establishes it (the DPU resolves the service to a backend pod, connection-sticky). A dead/unregistered service is **not** detected here. `NULL`+`ENOMEM` on OOM. |
| `dmesh_conn_t *dmesh_next_ready(dmesh_channel_t *s)` | Pop the next connection that **has inbound** (the DPU-facing poller named it) and return the **same** handle you created — or `NULL` when drained. **No scan, no per-conn fd.** After waking on `dmesh_event_fd`, loop this and `dmesh_read` each returned conn to EAGAIN. Single-consumer (your event loop). |

### Read / write / sendfile / flush / close
| Function | Returns / errno |
|---|---|
| `ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len)` | `>0` bytes from the next inbound message; **`0` = EOF** (the peer closed — sent a FIN; *sticky*, every later read also returns 0 → `dmesh_close(c)`); `-1` = would-block, nothing inbound yet (`EAGAIN`). One message is delivered whole — one read with a big-enough buffer returns it in full. **No implicit send.** |
| `ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len`. Consecutive writes accumulate into one message; the first write after a flush starts a new message. **No single-outstanding restriction** — flush many messages without reading. `-1` = would exceed `slot_size` (`EMSGSIZE`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the current message (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. Returns bytes appended (`0` = EOF on `in_fd`), `-1` on read error / no slot room (`EMSGSIZE`). |
| `int dmesh_flush(dmesh_conn_t *c)` | **The explicit ship — REQUIRED to send** (write only buffers). `0` = sent (or nothing buffered → no-op; a zero-length message is reserved for the FIN). `-1` `EAGAIN` = no TX slot right now (retryable); `EBADMSG` = descriptor fault (close the conn). |
| `int dmesh_close(dmesh_conn_t *c)` | **Graceful close.** If established (and you didn't just read its EOF), sends a **FIN** (zero-length, rides behind all prior data) so the peer's `dmesh_read` returns `0` and it reclaims its slot. Then frees this conn's slots; un-ACKed sent TX slots are freed by their own DPU ACKs. A buffered-but-unflushed message is discarded (flush first). Concurrent close is safe. Returns `0`. Safe on `NULL`. |

### Accessors
- `int32_t dmesh_peer(dmesh_conn_t *c)` — the peer pod once established, else the service id.
- `void *c->user_data` — **app-owned** (like epoll's `data.ptr`): set it after `accept`/`connect`,
  and `dmesh_next_ready` hands the conn back so you read your context off it. The transport
  never touches it.

**Lifecycles:**
```
server:  c = dmesh_accept(s);   loop: dmesh_read(c,…)…; (handle); dmesh_write(c,…); dmesh_flush(c);
         … on read()==0 (peer FIN): dmesh_close(c);
client:  c = dmesh_connect(s, service);
         loop: dmesh_write(c,…); dmesh_flush(c); … dmesh_read(c,…) when ready …;
         … dmesh_close(c);   // once, at the end (sends a FIN)
```

---

## 4. Examples

### 4a. Echo server — single channel fd + ready list (no conn table, no per-conn fd)
This is `bench/echo_sock.c`. One fd is registered; on wake you service **new** conns
(`dmesh_accept`) and conns **with inbound** (`dmesh_next_ready`) — the DPU names them, so
there is no scan. The app holds **no** conn table and does **no** per-conn `epoll_ctl`.
```c
#include <thrift/transport/dpm.h>
#include <sys/epoll.h>

static int serve(dmesh_conn_t *c) {                 // drain to EAGAIN; echo each message
    char b[8192]; ssize_t n;
    for (;;) {
        n = dmesh_read(c, b, sizeof b);
        if (n > 0)      { dmesh_write(c, b, n); dmesh_flush(c); }   // a whole message → echo + ship
        else if (n == 0) return 1;                  // EOF: peer sent FIN → caller closes
        else             return 0;                  // EAGAIN: nothing more now
    }
}

int main(void) {
    dmesh_channel_t *s = dmesh_create_channel("echo", /*pod_id*/11);
    int dfd = dmesh_event_fd(s);                     // the ONE channel fd

    int epfd = epoll_create1(0);                     // ── vanilla kernel epoll ──
    struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

    struct epoll_event events[8];
    for (;;) {
        epoll_wait(epfd, events, 8, -1);             // SLEEP until activity
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}    // drain the fd (level→edge)

        dmesh_conn_t *c;
        while ((c = dmesh_accept(s))     != NULL)     // new connections (first msg in hand)
            if (serve(c)) dmesh_close(c);
        while ((c = dmesh_next_ready(s)) != NULL)     // existing conns with inbound — no scan
            if (serve(c)) dmesh_close(c);             // read()==0 → peer FIN → reclaim
    }
}
```
> Versus a TCP epoll echo server: the loop is the same shape and **simpler** — a message is
> atomic (one `dmesh_read` per message, no accumulation loop), there is one fd instead of one
> per connection, and `dmesh_next_ready` replaces the readiness scan. Sending is explicit
> (`write` buffers, `flush` ships). Idle CPU ~0 with `DPUMESH_HOST_EPOLL=1`.

### 4b. Client — a persistent conn, request then response
Connect once, then `write → flush → read` in a loop on the **same** conn. Apply a wall-clock
timeout so a dead pod (no reply ever) can't hang you.
```c
#include <thrift/transport/dpm.h>
#include <time.h>

dmesh_channel_t *s = dmesh_create_channel("client", /*pod_id*/10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);

for (int i = 0; i < N; i++) {
    dmesh_write(c, req[i], req_len[i]);             // buffer
    dmesh_flush(c);                                 // ship (read does NOT auto-send)
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    char resp[8192]; ssize_t n;
    for (;;) {
        n = dmesh_read(c, resp, sizeof resp);
        if (n > 0)  break;                          // a whole reply
        if (n == 0) goto closed;                    // peer closed
        struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - t0.tv_sec >= 5) break;     // dead/unregistered pod → give up
        sched_yield();
    }
    /* ... handle resp (n bytes) ... */
}
closed:
dmesh_close(c);                                     // once, at the end (sends a FIN)
```

### 4c. Client — many connections, harvested via the ready list
Open W connections, set each one's `user_data` to your per-conn context, fire requests, then
sleep on the one channel fd and let `dmesh_next_ready` tell you which replies landed — no scan.
```c
#include <thrift/transport/dpm.h>
#include <sys/epoll.h>
#define W 256

typedef struct { int idx; struct timespec t_send; /* ... */ } slot_t;

dmesh_channel_t *s = dmesh_create_channel("client", /*pod_id*/10);
int dfd = dmesh_event_fd(s);
int epfd = epoll_create1(0);
struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

slot_t slot[W];
dmesh_conn_t *conn[W];
for (int i = 0; i < W; i++) {
    conn[i] = dmesh_connect(s, /*service*/11);
    conn[i]->user_data = &slot[i];                  // ← context handed back by next_ready
    slot[i].idx = i;
    send_request(conn[i], &slot[i]);                // write + flush; stamp t_send
}

struct epoll_event events[8];
for (;;) {
    epoll_wait(epfd, events, 8, -1);
    uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}

    dmesh_conn_t *c;
    while ((c = dmesh_next_ready(s)) != NULL) {      // exactly the conns with replies
        char resp[8192]; ssize_t n;
        while ((n = dmesh_read(c, resp, sizeof resp)) > 0) {
            slot_t *sl = c->user_data;               // O(1) correlation — no scan
            record_latency(sl);                      // now - sl->t_send
            send_request(c, sl);                     // pipeline the next request on this conn
        }
        if (n == 0) dmesh_close(c);                  // peer closed
    }
}
```
> (A purely throughput-driven client with a small fixed set of conns can also just busy-poll
> its own `conn[]` with `dmesh_read` — `dmesh_read` is a userspace inbox pop, not a syscall —
> and skip `dmesh_event_fd`/`dmesh_next_ready` entirely. Use the ready list when you want to
> *sleep* until something arrives without scanning many conns.)

### 4d. Porting an ordinary epoll server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost 1:1 and gets
**simpler**: the message is atomic (no `STATE_SEND_HDR`/`STATE_SEND_BODY`, no partial-write
retry), there is **one** fd, and `dmesh_next_ready` replaces the per-fd readiness scan. The
whole request is in hand at `dmesh_read`; build the response with `dmesh_write`/`dmesh_sendfile`,
then `dmesh_flush`. Header + body must fit one ≤ 8 KB message (chunk larger payloads across
messages at the app layer).

---

## 5. Addressing & routing model (design summary)

DPUmesh addresses **services**, not hosts, and demultiplexes the way TCP does — by an
**oriented endpoint tuple**, never by a per-message "request vs response" flag.

- **Oriented endpoint tuple (TCP-faithful).** Every message carries `src = (pod, port)`,
  `dst = (service, pod, port)`, and a per-connection `seq`. The receiver demuxes purely by the
  local **`dst_port`**: `BLANK` ⇒ a fresh connection (→ the accept queue); an allocated port
  ⇒ that connection's inbox. "Request vs response" is **not** a wire bit — it falls out of
  *which local connection* the tuple resolves to. That is also why **self-routing / loopback
  works**: the two directions land on different ports, so they never alias.

- **`service_id` + DPU routing (connection-level, sticky).** `dmesh_connect(s, service_id)`
  binds a logical service. On a connection's **first** message the DPU resolves
  `service_id → backend pod` (the L7 / load-balancing seam); the connection then sticks to
  that backend. A service may resolve to the caller's own pod — fully supported.

- **Delivery & readiness.** Bodies are DMA'd host→DPU→host straight into the receiver's RX
  buffer; only a small descriptor is queued per message. The DPU-facing poller (PE) delivers
  each message to its connection's inbox and, the moment a connection's inbox goes
  empty→non-empty, publishes that connection to a **ready list** and wakes the **one** endpoint
  eventfd. `dmesh_next_ready` drains the list — so the app learns exactly which connections to
  service without a per-connection fd and without scanning.

- **Teardown (FIN).** `dmesh_close` sends a zero-length **FIN** on the connection; it rides the
  same per-conn ordering as data (arrives after everything prior), and the peer's `dmesh_read`
  returns `0` (EOF) so it closes and reclaims its slot. A FIN landing on an already-closed
  connection is silently dropped (concurrent close is safe). A peer that crashes without a FIN
  leaves a slot until a future idle-reclaim backstop; apply your own timeout for a service that
  never answers.

> **Bottom line:** the call *vocabulary* and the non-blocking epoll *loop shape* are
> socket-like, and so are the *semantics* (persistent full-duplex connections, `read()==0`
> EOF, in-order delivery). What differs: messages are atomic (≤ 8 KB), addressing is a
> service name the DPU routes, the demux is the oriented tuple, and readiness is one endpoint
> fd plus a ready list instead of one fd per connection.
