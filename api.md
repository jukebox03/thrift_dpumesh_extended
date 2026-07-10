# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on the
**BlueField DPU/DPA**, not the host CPU, so your application keeps its full host core (no
in-host sidecar tax).

The DPU is an **L7-style proxy that owns every connection** (think Envoy): your app addresses a
**`service_id`** — never a pod — and the DPU routes **each message** to a backend pod
(**per-message load balancing**), owns the connection to that backend, and maps every reply back
to you. Bodies move by DMA **host → DPU → host**; by default the DPU touches only metadata —
it reads a body only when the **L7 proxy engine** is enabled (`DPUMESH_PROXY`, §8).

It is a **connection-oriented, full-duplex, byte-stream transport** — close to TCP in shape, but:

- **You address a service, not a peer.** `dmesh_connect(service)` opens a connection *to the DPU*
  tagged with a service. The DPU picks the backend **per message** and owns the upstream to it.
- **Byte stream, like TCP.** `write` **appends** to the connection's send buffer; `flush` ships the
  committed bytes, **coalescing** many small writes into few, large ≤ 8 KB DMAs (the throughput
  win). The receiver `read`s a byte stream and **frames its own length** — a whole write is not
  guaranteed to arrive as one `read`. Bytes are **in send order** on a connection to a given backend.
- **NOT request/response.** The transport does **no** request↔response matching. If you keep
  several requests outstanding, **you** correlate replies (a req-id in the body) — under
  per-message LB, replies on one connection can arrive **out of order**.
- **Non-blocking**, with **one endpoint fd** + a **ready list** (no per-conn fd, no scan).

Header: `thrift/transport/dpm.h` (header-only, built on the C core `dpumesh.h`).

---

## 1. Concepts — two handles

| Handle | What it is | Lifetime / count |
|---|---|---|
| **`dmesh_channel_t`** (channel) | Your process's one link to the DPU. Wraps the DOCA device, the RX poller (PE) thread, the TX/RX buffers, the connection (port) table, and the one event fd. Created by `dmesh_create_channel()`. | **one per process**, whole run; **heavy** |
| **`dmesh_conn_t`** (connection) | A logical stream. A **client** conn (`dmesh_connect(service)`) addresses a service; a **server** conn (`dmesh_accept`) is one the DPU created to your pod. | **cheap**; reused |

> `dpumesh_ctx_t` (the DOCA engine) is hidden inside the channel — **you never touch it.**

### A connection is host ↔ DPU, addressed to a service

- **`dmesh_connect(s, service)` is local** — no round-trip and **no pod is chosen here**. It just
  binds "this conn talks to service S." Every `write`→`flush` on it ships a message that the DPU
  routes to a backend (LB) — **different messages may go to different backends.**
- The **client never learns or pins a pod** — it keeps addressing the service. The DPU owns the
  connection to each backend and correlates replies back to you.
- On the backend, the DPU delivers the message as a **new connection**: `dmesh_accept()` returns a
  server conn holding the first message. The backend is a **plain server** — it reads, replies to
  its peer (which the DPU manages), and closes. It never sees or addresses the real client.
- **Full-duplex** once a message has been received: the backend replies freely; the client reads.
  (A server conn can only reply to its peer, so it must receive before it can send.)
- **Teardown:** `dmesh_close()` sends a **FIN** (a zero-length message riding behind all prior
  data); the peer's `dmesh_read` returns **0 (EOF)** and it closes; the DPU frees the upstream.
  `read()==0` ⇒ the peer closed ⇒ you close.

```c
// Address a service, exchange freely, close at the end. No pod is ever named.
dmesh_channel_t *s = dmesh_create_channel(DMESH_SVC_NONE);   // pure client: advertises no service
dmesh_conn_t *c = dmesh_connect(s, /*dst_service_id*/11);
for (int i = 0; i < N; i++) {
    dmesh_write(c, msg, len);                    // buffer a message
    dmesh_flush(c);                              // ship it (read/close do NOT auto-send)
    char in[8192];
    ssize_t n = dmesh_read(c, in, sizeof in);    // -1/EAGAIN until a reply; 0 = peer closed
    /* ... a whole reply in hand (n bytes) ... */
}
dmesh_close(c);                                  // sends a FIN so the DPU frees the upstream
```

---

## 2. How it differs from BSD sockets

| | BSD socket | DPUmesh | What it means for you |
|---|---|---|---|
| **Address** | IP:port (one peer) | **`service_id`** — the DPU routes it | you name a service, never a pod; no IP/DNS; self-routing OK |
| **Load balancing** | none (one peer) | **per message** — the DPU picks a backend for *each* message | one conn's messages may spread across backends |
| **Connection** | persistent, full-duplex byte stream | persistent, full-duplex **message** stream **owned by the DPU** | same mental model; the backend you talk to is the DPU's choice |
| **Setup** | 3-way **handshake** | **none** — `connect` is local; the DPU routes each message | peer liveness is *not* checked at connect; apply your own timeout |
| **Framing** | byte stream (you frame it) | **byte stream** (you frame it) — writes coalesce, reads are short at arrival boundaries | frame your own length + loop `read` to `EAGAIN`, exactly like TCP; no `EPOLLOUT` dance |
| **Size** | unbounded | **`write` ANY length** — buffered in the conn's byte-ring, shipped as ≤ 8 KB DMAs (the DPA `dma_copy` limit) | no `EMSGSIZE`; the transport chops the stream into ≤ 8 KB wire DMAs and the reader reassembles by its own framing (§3) |
| **Ordering** | in-order in a connection | in-order **on a connection to one backend**; **none** across backends | under LB, replies on one conn can arrive out of order |
| **RPC matching** | n/a | **none** — the transport delivers, you correlate | pipelining several outstanding? match replies by your own req-id |
| **EOF** | `read()==0` | **`read()==0`** — the peer sent a FIN (`dmesh_close`) | identical: `read()==0` ⇒ close your side |
| **Blocking** | blocking *or* non-blocking | **non-blocking only** — RX to a per-conn inbox; TX busy-spins under local saturation | to *sleep* until ready, native-epoll on `dmesh_event_fd` |
| **`write`** | sends immediately | **buffers**; `dmesh_flush` ships | always `write`→`flush`; an empty flush is a no-op |
| **Readiness** | one pollable fd **per** connection | **ONE endpoint fd** + a **ready list** (`dmesh_next_ready`) | no per-conn fd, no scan — the DPU names the ready conns |

**Must-follow rules**
- **Address a service; the DPU picks the backend per message.** You never choose or learn a pod.
- **Pipeline from the first message if you like** (there is no establish-before-pipeline dance —
  the DPU owns the upstream and the host coalesces). **But** the transport does **no** RPC matching
  and per-message LB can reorder replies on one conn, so if you keep several requests outstanding,
  put a **req-id in the body and match on it** (never on arrival order). Single-outstanding
  (one request, then its reply) needs no correlation — the pairing is structural.
- **`read()==0` is EOF** (the peer sent a FIN → `dmesh_close()`). A *user* zero-length send is a
  no-op (0-length on the wire is the FIN), so `read()` never returns 0 except at EOF.
- **Exactly one `dmesh_close()` per conn** — sends a FIN so the peer/DPU reclaim, then frees the
  local state. Close what you `accept`/`connect`. Concurrent close is safe.
- **Drain a ready conn to EAGAIN** — the ready list re-arms a conn only on its inbox
  empty→non-empty edge, so a half-drained conn stalls until its next message.
- **`service_id` must be a live, registered service** — a dead/unregistered service is *not*
  detected at `connect`; the message is dropped at the DPU and nothing comes back. Apply your own
  wall-clock timeout.
- **Thread-safety:** the `dmesh_channel_t` is shared/thread-safe; a single `dmesh_conn_t` and the
  `dmesh_accept`/`dmesh_next_ready` event loop are **single-thread** (run one event loop; fan work
  out by handing off whole conns).
- **`slot_size` ≤ 8192** — the DPA `dma_copy` limit; leave `DPUMESH_SLOT_SIZE` at its default.

---

## 3. API reference

All calls are **non-blocking**. "would-block" = the listed sentinel **with `errno=EAGAIN`**.

### Channel (socket + bind + listen, folded)
| Function | Returns / errno |
|---|---|
| `dmesh_channel_t *dmesh_create_channel(int service_id)` | Channel handle, or `NULL` on init failure. `service_id` = the service this node advertises (`DMESH_SVC_NONE` for a pure client). **The node's `pod_id` is assigned by the DPU at registration** — you never pick it; read it back with `dmesh_pod_id()`. Blocks briefly on the register round-trip. |
| `void dmesh_destroy_channel(dmesh_channel_t *s)` | — (releases all DOCA resources; safe on `NULL`). |
| `int dmesh_event_fd(dmesh_channel_t *s)` | The **one** readiness fd, for NATIVE epoll/poll/select. Readable when a new connection is pending **or** any connection has inbound. **Calling it enables readiness** — call once at startup; a purely busy-polling app that never calls it must poll its conns itself. `-1` if unavailable. |
| `int dmesh_pod_id(dmesh_channel_t *s)` / `int dmesh_msg_max(dmesh_channel_t *s)` | This node's `pod_id` / the max body size (`slot_size`). |

> **Env:** `DPUMESH_PCI_ADDR` selects the DOCA device; `DPUMESH_SERVICE_ID` overrides the
> `service_id` arg. (There is no `DPUMESH_POD_ID` — the DPU assigns `pod_id`.)
> `DPUMESH_CONN_POOL=N` sets the number of per-connection TX byte-rings = **max concurrent
> connections** (default 256); each ring is `num_slots*slot_size / N` bytes (default 128 KB), the
> connection's socket send buffer. Fewer, deeper connections (gRPC multiplexing) → smaller `N`.
> (Data-plane tuning — buffer slots, slot size, DPA EU count, EU-sharding, PE-sleep — is
> baked into the binaries; the DPU L7-proxy engine is the one runtime toggle, `DPUMESH_PROXY`, §8.)

### Connect / accept / readiness
| Function | Returns / errno |
|---|---|
| `dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int dst_service_id)` | New **client** connection bound to a **`dst_service_id`** (the same id the backend passed to `dmesh_create_channel`). Local, no round-trip — every `write+flush` is routed by the DPU (per-message LB); no pod is chosen or learned here. A dead/unregistered service is **not** detected. `NULL`+`ENOMEM` on OOM. |
| `dmesh_conn_t *dmesh_accept(dmesh_channel_t *s)` | Next **inbound** connection the DPU created to your pod, holding its first message (body ready) with the peer learned; or `NULL`+`EAGAIN` if none pending. **Non-blocking.** (`NULL`+`ENOMEM` = rare alloc failure: the message is dropped, its RX credit reclaimed; an accept-until-NULL loop just skips it.) |
| `dmesh_conn_t *dmesh_next_ready(dmesh_channel_t *s)` | Pop the next connection that **has inbound** (the DPU-facing poller named it) and return the **same** handle you created — or `NULL` when drained. **No scan, no per-conn fd.** After waking on `dmesh_event_fd`, loop this and `dmesh_read` each returned conn to EAGAIN. Single-consumer (your event loop). |
| `void dmesh_pin_route(dmesh_conn_t *c)` | **Pin this conn to ONE backend** (connection-level LB, like a TCP proxy): every subsequent message (and the FIN) carries one route-affinity key, so the DPU routes them all to the backend picked for the **first** message — replies then arrive in **send order**. Call right after `dmesh_connect`, before any write; idempotent; no-op on a server conn. Forgoes per-message LB **by design** — use when the app assumes socket-style total order (the LD_PRELOAD shim pins every conn, §7). Keys are a per-channel rolling 255-id space and the DPU scopes each pin **by destination service**: two pinned conns to the **same** service may share a backend (balance skew, never a correctness issue); a key reused by a different service's conn gets its own pin (cross-service redirection is impossible). |

### Read / write / sendfile / flush / close
| Function | Returns / errno |
|---|---|
| `ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len)` | Up to `len` bytes of the inbound **byte stream**; **`0` = EOF** (peer closed; sticky → `dmesh_close(c)`); `-1` = would-block (`EAGAIN`). Reads are a **byte stream, exactly like TCP** — a call returns whatever bytes have arrived (a short read at an arrival boundary is normal), so **frame your own length** and **loop until `EAGAIN`** to drain. Bytes arrive **in send order on a connection to one backend**; under per-message LB across backends that is **not** request order (correlate with a req-id, or `dmesh_pin_route` for socket order). **No implicit send.** |
| `ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len)` | **Appends** `len` bytes to the conn's outbound byte stream (its byte-ring) → returns `len`. **ANY length** — no `EMSGSIZE`. Bytes are buffered, not sent, until `dmesh_flush`; consecutive writes **coalesce** so `flush` ships them as few, large ≤`slot_size` DMAs (the throughput win). Busy-spins under saturation (never fails). Pipelining allowed from the first byte. |
| `ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` straight into the byte-ring (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. `0` = EOF on `in_fd`, `-1` on read error (`EMSGSIZE` if `count==0`). |
| `int dmesh_flush(dmesh_conn_t *c)` | **The explicit ship — REQUIRED to send.** Carves the conn's committed-but-unsent bytes into ≤`slot_size` descriptors (coalescing) and posts them. `0` = sent (or nothing buffered → no-op). `-1` `EBADMSG` = descriptor fault (close the conn). Never returns `EAGAIN` (backpressure is absorbed in `write`/`alloc`). |
| `int dmesh_close(dmesh_conn_t *c)` | **Graceful close.** Sends a **FIN** (zero-length, behind all prior data) so the peer's `dmesh_read` returns `0` and the DPU frees the upstream; then frees local state. A buffered-but-unflushed message is discarded (flush first). Concurrent close is safe. Returns `0`. Safe on `NULL`. |

### Zero-copy TX — `dmesh_alloc` / `dmesh_commit`
Fill transport DMA memory **directly** instead of memcpy'ing through `dmesh_write`. Every connection
owns a **contiguous byte-ring** (its socket send buffer); `dmesh_alloc` hands you a pointer straight
into it — of **any length**, not slot-granular — you fill it in place, `dmesh_commit` finalizes the
bytes, and `dmesh_flush` ships them. Same ring, same wire, no copy.
| Function | Returns / errno |
|---|---|
| `void *dmesh_alloc(dmesh_conn_t *c, size_t len)` | A pointer to **`len` CONTIGUOUS bytes** at this conn's write head — the DMA source; fill it directly. **Busy-spins** under backpressure (waits for the conn's own TX_ACKs to free ring space). `NULL` if `len==0`, `len` exceeds the per-conn ring (`DPUMESH_CONN_POOL` sets its size), or the conn is not established. |
| `int dmesh_commit(dmesh_conn_t *c, size_t len)` | Finalize `len` (**≤ the alloc'd len**) bytes as committed data, ready for `dmesh_flush`. Returns `0`. Commit *less* than you alloc'd to send a short message; the unused tail is reclaimed. |

```c
// zero-copy send: alloc a pointer into the ring → fill it → commit → flush (no memcpy)
void *p = dmesh_alloc(c, len);          // len bytes, contiguous, in transport DMA memory
if (p) { fill(p, len); dmesh_commit(c, len); dmesh_flush(c); }
```
> `dmesh_write(c, buf, len)` is exactly `dmesh_alloc`+`memcpy`+`dmesh_commit` fused — both feed the
> same byte-ring, so you can mix per-message (`write`) and zero-copy (`alloc`/`commit`) on one conn.

### Accessor
- `void *c->user_data` — **app-owned** (like epoll's `data.ptr`): set it after `accept`/`connect`,
  and `dmesh_next_ready` hands the conn back so you read your context off it. The transport never
  touches it.

### Large payloads (> `slot_size`) — transparent, **no special API**
There is **no** `write_large`/`read_large`. `dmesh_write` takes **any length** — the byte-ring
buffers it and `dmesh_flush` chops the stream into ≤ `slot_size` wire DMAs. Receive it with a
plain `dmesh_read` **loop**, concatenating until you have the length **your** protocol declares —
framing/completeness is the app's job, exactly like a TCP byte stream.

```c
// SEND any length — write buffers into the byte-ring; flush ships it as ≤ slot_size DMAs.
dmesh_write(c, big, big_len); dmesh_flush(c);
// RECEIVE — loop + concatenate until YOUR header's length is satisfied.
size_t got = 0; while (got < want) { ssize_t n = dmesh_read(c, buf+got, want-got);
    if (n > 0) got += n; else if (n == 0) break; else sched_yield(); }
```

> **Ordering across a multi-DMA payload.** The stream is chopped into independent ≤ 8 KB DMAs, each
> routed **per DMA** (per-message LB). To a service with **one** backend that is always in order.
> To a service that load-balances across **several** backends, `dmesh_pin_route(c)` first so the
> whole connection pins to one backend and the DMAs stay in send order (the LD_PRELOAD shim pins
> every conn, §7). Unpinned + multi-backend, a large payload's DMAs may land on different backends —
> use a connection-level pin, or frame at a granularity ≤ 8 KB. (A single `dmesh_write` larger than
> the per-conn ring, 128 KB by default, needs an intervening `dmesh_flush` to drain the ring.)

**Lifecycles:**
```
server:  c = dmesh_accept(s);   loop: dmesh_read(c,…); (handle); dmesh_write(c,…); dmesh_flush(c);
         … on read()==0 (peer FIN): dmesh_close(c);
client:  c = dmesh_connect(s, service);
         loop: dmesh_write(c,…); dmesh_flush(c); … dmesh_read(c,…) when ready …;
         … dmesh_close(c);   // once, at the end (sends a FIN)
```

---

## 4. Examples

### 4a. Backend server — a plain server behind the DPU proxy
`bench/echo_sock.c`. The DPU creates connections to you and routes clients' messages here; you
`accept`, `read`, reply to the **peer** (the DPU manages who that is), and `close`. One fd is
registered; on wake you service **new** conns (`dmesh_accept`) and conns **with inbound**
(`dmesh_next_ready`) — the DPU names them, so no scan, no conn table, no per-conn `epoll_ctl`.
```c
#include <thrift/transport/dpm.h>
#include <sys/epoll.h>

static int serve(dmesh_conn_t *c) {                 // drain to EAGAIN; reply to each message
    char b[8192]; ssize_t n;
    for (;;) {
        n = dmesh_read(c, b, sizeof b);
        if (n > 0)      { dmesh_write(c, b, n); dmesh_flush(c); }   // whole message → reply + ship
        else if (n == 0) return 1;                  // EOF: peer FIN → caller closes
        else             return 0;                  // EAGAIN: nothing more now
    }
}

int main(void) {
    dmesh_channel_t *s = dmesh_create_channel(/*service_id*/11);   // this backend serves service 11
    int dfd = dmesh_event_fd(s);                     // the ONE channel fd

    int epfd = epoll_create1(0);                     // ── vanilla kernel epoll ──
    struct epoll_event ev = { .events = EPOLLIN }; ev.data.fd = dfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, dfd, &ev);

    struct epoll_event events[8];
    for (;;) {
        epoll_wait(epfd, events, 8, -1);             // SLEEP until activity
        uint64_t cnt; while (read(dfd, &cnt, sizeof cnt) > 0) {}    // drain the fd (level→edge)

        dmesh_conn_t *c;
        while ((c = dmesh_accept(s))     != NULL)     // new conns (first message in hand)
            if (serve(c)) dmesh_close(c);
        while ((c = dmesh_next_ready(s)) != NULL)     // existing conns with inbound — no scan
            if (serve(c)) dmesh_close(c);             // read()==0 → peer FIN → reclaim
    }
}
```

### 4b. Client — request/response on a service (single-outstanding)
Connect to a **service**, then `write → flush → read` one at a time. With **one** request
outstanding, the reply that arrives IS this request's reply — **no correlation needed.**
```c
dmesh_channel_t *s = dmesh_create_channel(DMESH_SVC_NONE);   // pure client
dmesh_conn_t *c = dmesh_connect(s, /*dst_service_id*/11);   // address a service, not a pod

for (int i = 0; i < N; i++) {
    dmesh_write(c, req[i], req_len[i]); dmesh_flush(c);
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    char resp[8192]; ssize_t n;
    for (;;) {                                       // wait for the reply, with a wall-clock timeout
        n = dmesh_read(c, resp, sizeof resp);
        if (n > 0)  break;                           // this request's reply
        if (n == 0) goto closed;                     // peer closed
        struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - t0.tv_sec >= 5) break;      // dead/unregistered service → give up
        sched_yield();
    }
    /* ... handle resp (n bytes) ... */
}
closed:
dmesh_close(c);
```

### 4c. Client — pipelined requests, correlated by a req-id (the model B way)
Fire many requests without waiting. Because the DPU load-balances **per message**, replies on one
conn can arrive **out of order** — so put a **req-id in the body** and match on it, never on arrival
order. This is the transport's philosophy: *you address a service and pipeline freely; you own the
request↔response matching.*
```c
typedef struct { uint32_t req_id; uint8_t payload[BODY]; } msg_t;

dmesh_conn_t *c = dmesh_connect(s, /*dst_service_id*/11);

for (uint32_t id = 0; id < N; id++) {                // fire N requests, no waiting
    msg_t m; m.req_id = id; /* fill m.payload ... */
    dmesh_write(c, &m, sizeof m); dmesh_flush(c);     // buffer + ship each
}

unsigned got = 0;                                     // harvest replies, matched by req_id
while (got < N) {
    msg_t r; ssize_t n = dmesh_read(c, &r, sizeof r);
    if (n > 0)       { on_reply(r.req_id, &r); got++; }   // ← correlate by req_id (NOT arrival order)
    else if (n == 0) break;                           // peer closed
    else             sched_yield();                   // EAGAIN
}
dmesh_close(c);
```
> Many conns at once? Set each `conn->user_data` to its context, sleep on the one `dmesh_event_fd`,
> and let `dmesh_next_ready` hand back exactly the conns with replies — O(1), no scan. (A small,
> fixed set of conns can also just busy-poll its own `conn[]` with `dmesh_read` — a userspace inbox
> pop, not a syscall — and skip the event fd entirely.)

### 4d. Porting an ordinary epoll server
A standard `epoll_wait`/`accept`/`read`/`write`/`sendfile` server maps almost 1:1 and gets
**simpler**: the message is atomic (no `STATE_SEND_HDR`/`STATE_SEND_BODY`, no partial-write retry),
there is **one** fd, and `dmesh_next_ready` replaces the per-fd readiness scan. The whole request is
in hand at `dmesh_read`; build the reply with `dmesh_write`/`dmesh_sendfile`, then `dmesh_flush`.
Header + body must fit one ≤ 8 KB message (chunk larger payloads at the app layer).

---

## 5. Addressing & routing model

- **The DPU owns every connection.** A client addresses a **service**; the DPU picks a backend
  **per message** (per-message load balancing), owns the "upstream" connection to that backend, and
  maps the reply back. The client never sees or names a pod.
- **How it stays a connection.** The DPU assigns each upstream a private id `uP` and rewrites the
  message so the backend sees a connection from *the proxy* `(client_pod, uP)`, demuxes and replies
  by `uP`; the reply returns to the DPU, which maps `uP → (client, client_port)` and delivers it on
  your client conn. All of this is invisible to both apps — you just `write`/`read`.
- **Routing granularity is one whole message (one slot).** The DPU makes **one** routing decision
  per message and delivers it to **exactly one** backend — a message cannot be split across
  destinations. A message is atomic at **≤ 8 KB**; there is **no transport concept of a message that
  spans slots** at the wire level. But `dmesh_write` **auto-chunks** a larger payload into ≤ 8 KB
  wire messages for you (§3) and route-pins them, so the app just writes any length.
- **Route-affinity (keeps a large message's chunks together).** Each wire message carries a
  **route-affinity key** (`route_group`, one byte): the DPU pins every message sharing a non-zero
  key to the **same** backend (a small `(dst_service, key) → backend` table on the single ARM
  thread — no lock). Key `0` (a single-slot `dmesh_write`) = normal per-message LB. When a
  `dmesh_write` spans slots, it stamps ONE key (a **channel-wide** rolling id, so one channel's
  concurrent large messages get distinct keys) on all of that message's chunks, so — even when a
  service load-balances across several backends — every chunk lands on one backend and
  reassembles. In practice a message's chunks reach the ARM **in send order** (a connection's
  messages are conn-sharded onto ONE forward ring, `src_port % K`, so they stay FIFO — the
  **head chunk arrives first**, a property a future L7 parser relies on, see below); the table
  is nevertheless **overwrite-on-reuse** — whichever chunk reaches the ARM first records the pin
  and the rest reuse it — so correctness never depends on that ordering. It is **collision-safe**:
  pins are scoped by the message's **destination service**, so a key shared across channels/conns
  can only merge same-service traffic onto one backend (skew, still reassembling); it can never
  route a message to another service's backend. A grouped message forgoes per-message LB by
  design. (An `is_last`-DELETE that frees the pin per message was considered and **rejected** —
  under either hazard it can free a pin mid-message and scatter the chunks; the bounded
  self-healing table is used instead.)
- **L7 routing (body-aware).** The byte-stream proxy engine that ships every reply is **always on**
  (§8); its default parser (`passthru`) routes on **metadata only** — the service table +
  route-affinity above — and never reads a body (**bit-identical** to the legacy per-message LB). To
  make the DPU an Envoy-like L7 proxy that **parses the byte stream and content-routes**, select the
  L7 parser for a service (`DPUMESH_PROXY_L7_SVC`, **§8**). The real body-parsing hook is the future
  step — the mock reframers today validate the L4 substrate.
- **Response↔request matching is the app's job.** The transport does no matching, and this DPU
  proxy routes at the **connection** level, not the **request** level (it never parses the body). A
  protocol-parsing L7 proxy could correlate by stream-id; a metadata-driven DPU cannot. Carry a
  req-id and match on it — **especially** because per-message LB can reorder replies (§4c).
- **Delivery & readiness.** Bodies DMA host→DPU→host straight into the receiver's RX buffer; only a
  small descriptor + a 20-byte completion ride the control path. The PE thread delivers each message
  to its conn's inbox and, on the inbox's empty→non-empty edge, publishes the conn to a **ready
  list** and wakes the one endpoint eventfd. `dmesh_next_ready` drains it — no per-conn fd, no scan.
- **Teardown (FIN).** `dmesh_close` sends a zero-length **FIN** (behind all prior data on the conn);
  the peer's `dmesh_read` returns `0` and the DPU frees the upstream. A peer that crashes without a
  FIN leaves its conn + upstream allocated until that port/`uP` is reused (there is **no idle
  reaper**); apply your own wall-clock timeout for a service that never answers.

---

## 6. Architecture — three pods exchanging messages

Scenario: **pod 10** is a client of **service 11**, which has two backends, **pod 11** and **pod
13**. The DPU load-balances pod 10's messages across them and owns every connection.

```
   HOST pod 10  (client)                    BlueField DPU  (owns every connection)                    HOST pod 11  (service 11)
   ═════════════════════                    ═══════════════════════════════════════                   ══════════════════════════
   dmesh_channel_t  (1/process)             ┌── ARM control plane ──────────────────┐                  dmesh_channel_t
    • event_fd   (one readiness fd)         │  service_table[11] = { pod11, pod13 }  │                   • ports[uP1] = server conn
    • ports[] (the conn table)              │  dpu_route(11) → LB → pod11 | pod13     │                   •   peer = (pod10, uP1)
    │   [pC] = client conn → service 11     │                                        │                   • RX buffer  rx_dma_buffer
    • TX buffer  dma_buffer                 │  conntrack (the owned connections):    │                   • PE thread + event_fd
    │   ┌slot0┐┌slot1┐…  (8 KB each)        │    upstream[uP1] = {pod10, pC, pod11}  │
    • RX buffer  rx_dma_buffer              │    upstream[uP2] = {pod10, pC, pod13}  │                  HOST pod 13  (service 11)
    • PE thread  (delivers RX → inbox)      │    reuse (pod,port,backend) → uP        │                  ══════════════════════════
                                            └────────────────────────────────────────┘                   dmesh_channel_t
                                            ┌── DPA EUs (data plane) ───────────────┐                     • ports[uP2] = server conn
                                            │  fwd dma_copy: host → DPU-staging      │                     •   peer = (pod10, uP2)
                                            │  staging[pod]  (32 MB per pod)         │                     • RX buffer  rx_dma_buffer
                                            └────────────────────────────────────────┘                     • PE thread + event_fd

   descriptor posted per message (dma_desc, 64 B — the body is NOT in it, only a pointer + the tuple):
        { mmap, addr = &dma_buffer[slot], size,
          src = (pod, port),  dst = (service, pod, port),  seq,  valid }     ← the "oriented tuple"
   completion returned per DMA (20 B on the control path): { type, src/dst pod, ports, seq, len, pos, route_group }
```

**One message — `pod10:pC → service 11`, LB'd to `pod 11`, then its reply:**
```
  FORWARD  (client request → backend)
  (1) host10  write → dma_buffer[slot];  flush posts a dma_desc:
              src=(10, pC)   dst=(svc 11, BLANK, BLANK)   seq=s          ← dst_pod BLANK = "DPU, route me"
  (2) DPA EU  dma_copy  host10 dma_buffer[slot]  →  DPU staging[pod10]
  (3) ARM     dpu_route(11) → pod 11 ;  reuse or create upstream → uP1 ;  upstream[uP1]={10,pC,pod11}
              rewrite tuple → src=(10, uP1)  dst=(pod11, uP1)            ← backend will see the DPU id uP1
  (4) ARM     SG-DMA   DPU staging[pod10]  →  host11 rx_dma_buffer      (egress; in-place, no DPU CPU copy)
  (5) host11  PE: dst_port=uP1 not live → accept queue → dmesh_accept → server conn uP1, peer=(10,uP1)
              app dmesh_read → the message
      ARM     TX_ACK to (pod10, pC, s)   ← uP1 translated back to pC → frees host10's TX slot

  REPLY  (backend → its peer)
  (6) host11  write → flush     src=(11, uP1)   dst=(pod10, uP1)          ← concrete dst → no LB, direct
  (7) DPA     dma_copy  host11 → DPU staging[pod11] ;  ARM SG-DMA  staging → host10 rx_dma_buffer
  (8) ARM     dst_port=uP1 is an owned upstream → map uP1 → (pod10, pC) → rewrite dst → (pod10, pC)
              TX_ACK to (pod11, uP1)  → frees host11's TX slot
  (9) host10  PE: dst_port=pC → client conn pC inbox → ready list → dmesh_read → the reply
```
pod 10's **next** message may be routed to **pod 13** instead — a second upstream `uP2` is created,
its replies map back to the same client conn `pC`. That is why replies on `pC` can interleave across
backends and are matched by your **req-id**, not by order.

**Legend**
- **channel** — your process's single link to the DPU: DOCA device + PE thread + TX/RX buffers +
  the port (conn) table + the one event fd.
- **conn** — an entry in `ports[]`: a **client** conn (addresses a service) or a **server** conn
  (created by the DPU, bound to a `uP`). Each holds an inbox (arriving messages) and, for TX, a
  buffered slot.
- **buffer** — **TX** `dma_buffer` (`num_slots × 8 KB`, the DMA source) and **RX** `rx_dma_buffer`
  (where inbound bodies land) on each host; **DPU staging** per pod (the host→DPU→host hop; the body
  is read out of it in place, so the DPU never memcpy's).
- **descriptor** (`dma_desc`) — the small wire record posted per message: a pointer to the body
  slot + the **oriented tuple** `(src pod/port, dst service/pod/port, seq)`. The body travels by
  DMA; only the descriptor and a 20 B completion ride the control path.
- **uP** — the DPU-assigned upstream id: the backend's server-conn port, and the key the DPU maps
  ↔ `(client pod, client port, backend)` to route replies home.

---

## 7. Socket compatibility — `LD_PRELOAD` shim (`libdmesh_preload.so`)

Run an **unmodified, dynamically-linked POSIX socket application** over DPUmesh — no
recompile, no source change:

```sh
# backend: its listen(<port>) becomes a dmesh service listener
LD_PRELOAD=libdmesh_preload.so DMESH_PRELOAD_LISTEN=9095 DMESH_PRELOAD_SVC=15  ./my_server 9095
# client: connect()s to the mapped port are routed over DPUmesh
LD_PRELOAD=libdmesh_preload.so DMESH_PRELOAD_MAP=9095=15                       ./my_client host 9095
```

| env | meaning |
|---|---|
| `DMESH_PRELOAD_LISTEN=<port>` | `listen()` on this TCP port becomes the dmesh service listener |
| `DMESH_PRELOAD_SVC=<svc>` | the service this process advertises (required with `LISTEN`) |
| `DMESH_PRELOAD_MAP=<port>=<svc>[,…]` | `connect()`s to these TCP ports go over dmesh |
| `DMESH_PRELOAD_DEBUG=1` | per-connection diagnostics on stderr |

**How it works.** When a socket becomes dmesh-backed, the shim `dup2()`s a private
eventfd over the app's fd number — the fd stays a **real kernel fd**, so
`epoll`/`poll`/`select`/`close`/`dup` work natively (kernel-TCP fds and dmesh fds mix
freely in one epoll set). A dispatcher thread (the channel's single
`dmesh_accept`/`dmesh_next_ready` consumer) asserts per-fd readability. Reads are
byte-stream (short reads at message boundaries, exactly like TCP); each `send()`/
`write()` ships one message (`write`+`flush`); any length auto-chunks (§3). Blocking
sockets are emulated (`SO_RCVTIMEO` honored). Every client conn is
`dmesh_pin_route()`d, so one connection's traffic stays on **one backend** and replies
arrive **in order** — the socket contract; load is still balanced **across**
connections.

```
  UNMODIFIED app                     libdmesh_preload.so (interposes libc)          DPUmesh
  ══════════════                     ════════════════════════════════════          ═══════
  connect(fd, port)  ──intercept──▶  port ∈ MAP?  dmesh_connect(svc) + pin_route
  listen(port)       ──intercept──▶  port == LISTEN?  advertise service SVC
        fd  ◀───────── dup2() a private eventfd OVER the app's fd number ──────────┐
        │            (fd stays a REAL kernel fd → epoll/poll/select/close/dup work  │
        │             natively; kernel-TCP fds and dmesh fds share one epoll set)   │
  epoll_wait(fd)   ── native kernel ──▶ readable ⇐ eventfd asserted                 │
  read(fd)/write(fd) ──intercept──▶ dmesh_read (byte-stream, short reads) /         │
                                     dmesh_write+flush (one msg; >8 KB auto-chunks) │
                                                                                    │
   ┌── dispatcher thread: the channel's ONE dmesh_accept / dmesh_next_ready ────────┘
   │   consumer. Per delivered message it writes the conn's eventfd → the app's
   │   next epoll/read sees the fd ready (re-buys per-conn-fd readiness the native
   └── API avoids: one eventfd signal per message — the transparency tax).
```

**Cost.** The shim deliberately re-buys the per-conn-fd readiness model the native API
avoids (one eventfd signal per message). Measured (1 KB round trips, thread-per-conn
vanilla client, 2-core pod, scale_log 07-03): ~7K RPS per connection (closed-loop
RTT-bound, p50 ~130 µs), scaling near-linearly to 16 conns, then a **plateau of ~150K
RPS at 64 conns** (128 conns = same throughput at 2× latency). That is ~76% of the
native ceiling on the same deploy (~199K) — better than the ~½ originally projected
from the per-conn-fd experiment (scale_log 06-30). The price of transparency, paid
only by preloaded apps. Native `dmesh_*` callers are unaffected.

**Limits (v1).** AF_INET `SOCK_STREAM` only; most `SO_*` options are accepted no-ops;
`shutdown(SHUT_WR)` sends the FIN (no true half-close — late replies are
undeliverable); no fork-shared sockets (DOCA is not fork-safe); statically-linked or
raw-syscall binaries (e.g. Go) bypass `LD_PRELOAD` entirely, and so does **stdio**
(`FILE*` via `fdopen` — glibc stdio calls its internal `__read`/`__write`, not the
interposable symbols); use direct `read`/`write`/`send`/`recv` on shimmed sockets.

**Validation.** `bench/tcp_echo.c` (vanilla epoll echo) + `bench/tcp_client.c`
(vanilla **thread-per-conn** blocking client; sets `SO_RCVTIMEO=5s` so a lost message
is a counted failure, never a hang) run the SAME binaries over kernel TCP and over
DPUmesh (`./test-bench.sh preload <N> <SIZE> <CONNS>`): 0-fail across 1 KB–32 KB
(32 KB = auto-chunk + pin + stream reassembly) and 1–256 connections — including 23×
256-conn simultaneous-connect storms and idle→storm cycles — p50 ~120–150 µs,
back-to-back stable. One unreproduced 256-conn-storm message-loss incident is tracked
OPEN with tripwires armed (scale_log 07-03 session 3).

---

## 8. L7 proxy — byte-stream reframing (`DPUMESH_PROXY` = parser selector)

DPU→host egress is **always** this per-conn byte-stream engine — the sole reverse path. The forward
DMA lands a connection's bytes in DPU staging; an L7 function reframes the stream into **routing
segments**; the engine ships each to its backend by **scatter-gather DMA** (ARM). The backend
receives a byte stream and frames it **itself** (an ordinary server behind an envoy). `DPUMESH_PROXY`
does **not** toggle the engine — it selects the deploy-default **request parser**: `passthru`
(default; one segment per arrived message on the §5 metadata route — **bit-identical** to legacy
per-message LB) or `frame`. The real body-parsing L7 hook is a MOCK today; replies always pass through.

**The deliverable — what an L7 function returns:**
```c
struct dmesh_route_seg { uint32_t off; uint32_t len; int32_t dst; };  // one slice → one backend

// MOCK now, envoy later. Called with a connection's bytes, IN ORDER, shown contiguously.
int proxy_route(conn, const uint8_t *buf, uint32_t avail,
                dmesh_route_seg *segs, int max, uint32_t *consumed);
//  buf/avail : the window's unconsumed bytes (avail can grow across calls — see seam below)
//  segs/ret  : up to `max` segments {off,len,dst}; dst = a concrete backend pod, or DEFER to
//              the §5 L4 route. ret < 0 = protocol error → the L4 drops this stream.
//  consumed  : bytes fully processed; the cursor advances, the unconsumed tail is kept.
```

**The L4 engine (request AND reply run the SAME machinery):**
```
 forward DMA lands the body in DPU staging (in place)
   │
   ▼  per-conn INPUT WINDOW ── bytes in arrival order; zero-copy views over staging + a cursor.
   │   A SEAM buffer aligns the unconsumed tail into ONE contiguous run only when a parse stalls
   │   across staging extents (e.g. a >8 KB frame split across arrivals).
   ▼  proxy_route(MOCK) → segs {off,len,dst}          (consumed = how far the cursor advances)
   ▼  per-(dst pod, region) LANE ── segments to one backend gathered into ONE chained-buf SG-DMA
   │   (ARM generic doca_dma: staging → the backend's host RX buffer) + one batched notify
   ▼  BYTE-STREAM delivery ── the backend loops dmesh_read (≤8 KB chunks) and frames itself; per
   │   receiving conn, delivery order = segment order (lane FIFO).
   └─ CUSTODY ── the sender's TX slot is held until the egress DMA has READ its staging bytes,
      then a batched TX_ACK frees it (never released early — the host would overwrite mid-read).
```

**Full path — client bytes → DPU → backend → reply (the reply is symmetric):**
```
  HOST client                     BlueField DPU  (dpu_proxy.c)                        HOST backend
  ═══════════                     ════════════════════════════                       ════════════
  dmesh_write(bytes) ─fwd DMA─▶  window[conn]: [ …unparsed tail… ][ new bytes ]
    (any length;                     │  proxy_route(buf, avail) → seg{off,len,dst=B} seg{…}
     auto-chunks §3)                 ▼  lane[B]: gather segs → 1 chained SG-DMA ─host RX─▶ dmesh_read
                                     └─ TX_ACK client  (custody released at egress)      (≤8 KB chunks,
                                                                                          frame it yourself)
  dmesh_read(reply) ◀─host RX─  lane[client] ◀─ proxy_route ◀─ window[reply conn] ◀─fwd DMA─ dmesh_write
                                  reply dst is NOT re-decided: conntrack (uP→client, §5) gives it;
                                  proxy only CONFIRMS (so a future envoy can observe replies too)
```

**Key properties**
- **Byte stream both ends.** Message boundaries are the L7 parser's business, not `dmesh_write`'s —
  the app may pack several frames into one write, or let a big write auto-chunk, and the parser
  reframes from the content either way. (The receiver-side atomic-**message** contract of §3 is thus
  dropped for a proxied stream; the backend frames its own bytes, exactly like an envoy upstream.)
- **`dst` is a concrete pod** (the mock/envoy already did LB), or **`DEFER`** = fall through to the
  §5 default L4 route (service table + route-affinity).
- **Custody at egress**, not at receipt — the batched `TX_ACK` fires only when the SG-DMA has read
  the staging bytes, so the sender never overwrites a body mid-flight.
- **Ordering.** One receiving conn always lands in ONE lane (FIFO), so a conn's delivery order =
  segment order; one unit's chunks are never interleaved with another's.

**The parser is chosen per connection** from the addressed service — **not** a single global
choice. So a vanilla (shim) app, the frame demo, and a real L7 service **coexist in one deploy**,
independently.
| env | behavior | purpose |
|---|---|---|
| `DPUMESH_PROXY` *(unset)* | same as `passthru` — the engine is **always on** | production default, **bit-identical** to legacy per-message LB |
| `DPUMESH_PROXY=passthru` \| `1` | deploy default = **passthru**: one segment per arrived message; `dst` = the §5 L4 route. Works with **any** byte stream (no app framing). | parity / regression + the vanilla (shim) path |
| `DPUMESH_PROXY=frame` | deploy default = **frame** for every request stream (legacy all-frame) | the byte-stream demo |
| `DPUMESH_PROXY_FRAME_SVC=<csv>` | services whose **request** streams use the frame demo parser `[u32 len][u8 svc][payload]` (routes each whole frame by `svc`; a >8 KB frame ships as ≤8 KB chunks) | mix app kinds: `DPUMESH_PROXY=passthru DPUMESH_PROXY_FRAME_SVC=16` |
| `DPUMESH_PROXY_L7_SVC=<csv>` | services whose **request** streams run the **real L7 hook** — `dmesh_l7_route` in `dpu_l7.c` (checked before frame) | the production L7 slot |

**Writing an L7 parser.** Implement one function, `dmesh_l7_route`, in `dpu_l7.c` (contract in
`dpu_l7.h`): you are shown only the **head** of the front message (a bounded ≤`PX_HEAD_MAX`
window); return the whole message's **total length** (`>0`, may far exceed the head window), `0`
= "head not fully here", or `<0` = malformed; optionally set `*target` to route by content. The
engine **streams the body from staging via SG** — it is never linearized, so a large message
costs **no per-slot memcpy**; the only copy is the ≤`PX_HEAD_MAX` head, and only when a head
straddles a slot. The hook is **stateless, no malloc, no locks**. Assign its services with
`DPUMESH_PROXY_L7_SVC`; a head larger than `PX_HEAD_MAX` poisons the conn (cf. Envoy
`max_request_headers_kb`). Everything else is unaffected.

**Reply streams always pass through** regardless of the above: a reply's `dst` is the single
conntrack peer, so per-frame vs whole-arrival segmentation delivers a **byte-identical** stream —
framing a reply would only add seam cost.

Drive `frame` with `bench/stream_sock.c` (`./test-bench.sh stream <N> <SIZE> [<SVC_LIST>] [<FPW>]`):
it sends length-prefixed frames and checks **byte-exact** echo + a **served-byte exact-count**.
Deploy it two ways: legacy `DPUMESH_PROXY=frame`, or decoupled
`DPUMESH_PROXY=passthru DPUMESH_PROXY_FRAME_SVC=16` — the latter passes `./test-bench.sh preload`
(vanilla shim, svc 15) and `./test-bench.sh stream` (frame, svc 16) against the **same** DPU.
Validated (scale_log 07-04): byte-exact for self-loopback (1 KB), a >8 KB frame (seam), several
frames per write (`FPW`), and fan-out across backends (`SVC_LIST`) — 0 drops. **Status:** the L4
engine is validated and the L7 plug-in slot (`dpu_l7.c`) is wired; writing the real body parser
(parse + LB + policy) is the author's step and needs **no transport changes**.

---

## 9. Baked data-plane configuration (reference)

Most data-plane tuning is **compiled in** at the values measured as best; the runtime env knobs are
`DPUMESH_PROXY` + `DPUMESH_PROXY_FRAME_SVC` + `DPUMESH_PROXY_L7_SVC` (§8), `DPUMESH_ARM_EGRESS_THREADS`
+ `DPUMESH_RINGS_PER_POD` (table below), `DPUMESH_PCI_ADDR`, `DPUMESH_SERVICE_ID`, `DPUMESH_CONN_POOL`
(§1), and the `DMESH_PRELOAD_*` shim vars (§7). **To change a *baked* value, edit the named
constant/assignment and rebuild** (no env override).

| Setting | Baked value | Constant / assignment | File |
|---|---|---|---|
| TX/RX slots per pod | `4096` | `DPUMESH_NUM_SLOTS_DEFAULT` → `ctx->num_slots` | `dpumesh.h` / `dpumesh_doca.c` |
| Slot size (max wire DMA) | `8192` (8 KB — DPA `dma_copy` cap) | `DPUMESH_SLOT_SIZE_DEFAULT` → `ctx->slot_size` | `dpumesh.h` / `dpumesh_doca.c` |
| Per-conn TX rings (max concurrent conns) | `256` (128 KB byte-ring each) | `DPUMESH_CONN_POOL` env → `ctx->conn_pool` | `dpumesh_doca.c` |
| DPA EU threads (N) | `4` | `objs->num_dpa_threads = 4` | `doca/dpu_worker.c` |
| Forward rings per pod / EU-sharding (K) | `2` — env `DPUMESH_RINGS_PER_POD` | `DPUMESH_RINGS_PER_POD_DEFAULT` → `k_rings` | `doca/dpumesh_common.h` (DPU + host) |
| ARM SG-DMA egress workers (n_eng) | `1` — env `DPUMESH_ARM_EGRESS_THREADS` (use `2`) | `getenv` → `px->n_eng` | `doca/dpu_proxy.c` |
| DPU main loop | event-driven epoll (busy-poll auto-fallback on setup failure) | — (literal path) | `doca/dpu_worker.c` |
| Host RX (PE) thread | sleep on the notification fd | `want_epoll = 1` | `dpumesh_doca.c` |
| Proxy seam cap (max contiguous parser view) | `512 KB` | `PX_SEAM_MAX_DEFAULT` | `doca/dpu_proxy.c` |

Constraints when changing: `K ≤ N ≤ 8` (`MAX_DPA_RINGS`); the **host's K must equal the DPU's K**
(forward rings pair 1:1); `slot_size ≤ 8192` (the DPA limit). `num_slots × slot_size` is the per-pod
staging size (4096 × 8 KB = 32 MB). A programmatic `dpumesh_config` still overrides
`num_slots`/`slot_size` at `dmesh_create_channel` without a rebuild.
