# DPUmesh API — Whitepaper (user-facing)

A service-mesh **data plane** on NVIDIA DOCA (Comch + DMA). The transport runs on the
**BlueField DPU/DPA**, not the host CPU, so your application keeps its full host core (no
in-host sidecar tax).

The DPU is an **L7-style proxy that owns every connection** (think Envoy): your app addresses a
**`service_id`** — never a pod — and the DPU routes **each message** to a backend pod
(**per-message load balancing**), owns the connection to that backend, and maps every reply back
to you. Bodies move by DMA **host → DPU → host**; the DPU touches only metadata, never the body.

It is a **connection-oriented, full-duplex, message transport** — close to TCP in shape, but:

- **You address a service, not a peer.** `dmesh_connect(service)` opens a connection *to the DPU*
  tagged with a service. The DPU picks the backend **per message** and owns the upstream to it.
- **Message-framed:** each message is one whole body **≤ 8 KB**, delivered **atomically** and
  **in send order** on a connection *to a given backend*.
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
dmesh_channel_t *s = dmesh_create_channel("myapp", /*pod_id*/10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);
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
| **Framing** | byte stream (you frame it) | **message** — one whole body, atomic | no partial-read loop, no `EPOLLOUT` dance — the whole body is at `read` |
| **Size** | unbounded | **≤ 8 KB per message** (the DPA `dma_copy` limit) | chunk larger payloads into independent messages (§5) |
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
| `dmesh_channel_t *dmesh_create_channel(const char *app_name, int pod_id)` | Channel handle, or `NULL` on init failure. `app_name` = service identity (pod registration); `pod_id` = this node's address. |
| `void dmesh_destroy_channel(dmesh_channel_t *s)` | — (releases all DOCA resources; safe on `NULL`). |
| `int dmesh_event_fd(dmesh_channel_t *s)` | The **one** readiness fd, for NATIVE epoll/poll/select. Readable when a new connection is pending **or** any connection has inbound. **Calling it enables readiness** — call once at startup; a purely busy-polling app that never calls it must poll its conns itself. `-1` if unavailable. |
| `int dmesh_pod_id(dmesh_channel_t *s)` / `int dmesh_msg_max(dmesh_channel_t *s)` | This node's `pod_id` / the max body size (`slot_size`). |

> **Env:** `DPUMESH_POD_ID` overrides `pod_id`; `DPUMESH_PCI_ADDR` selects the DOCA device;
> `DPUMESH_SERVICE_ID` overrides the registered service (default = `pod_id`).
> `DPUMESH_HOST_EPOLL=1` makes the RX (PE) thread **sleep** on the DOCA notification fd (idle CPU ~0).

### Connect / accept / readiness
| Function | Returns / errno |
|---|---|
| `dmesh_conn_t *dmesh_connect(dmesh_channel_t *s, int service_id)` | New **client** connection bound to a **`service_id`**. Local, no round-trip — every `write+flush` is routed by the DPU (per-message LB); no pod is chosen or learned here. A dead/unregistered service is **not** detected. `NULL`+`ENOMEM` on OOM. |
| `dmesh_conn_t *dmesh_accept(dmesh_channel_t *s)` | Next **inbound** connection the DPU created to your pod, holding its first message (body ready) with the peer learned; or `NULL`+`EAGAIN` if none pending. **Non-blocking.** (`NULL`+`ENOMEM` = rare alloc failure: the message is dropped, its RX credit reclaimed; an accept-until-NULL loop just skips it.) |
| `dmesh_conn_t *dmesh_next_ready(dmesh_channel_t *s)` | Pop the next connection that **has inbound** (the DPU-facing poller named it) and return the **same** handle you created — or `NULL` when drained. **No scan, no per-conn fd.** After waking on `dmesh_event_fd`, loop this and `dmesh_read` each returned conn to EAGAIN. Single-consumer (your event loop). |

### Read / write / sendfile / flush / close
| Function | Returns / errno |
|---|---|
| `ssize_t dmesh_read(dmesh_conn_t *c, void *buf, size_t len)` | `>0` bytes of the next inbound message; **`0` = EOF** (peer closed; sticky → `dmesh_close(c)`); `-1` = would-block (`EAGAIN`). **One whole message per call** — if several are pending on the conn (you pipelined, or a burst coalesced), **loop until `EAGAIN`** to drain them all (there is no batch/multi-message read). Inbound is in **arrival** order — under per-message LB that is **not** request order (correlate yourself). **No implicit send.** |
| `ssize_t dmesh_write(dmesh_conn_t *c, const void *buf, size_t len)` | **Buffers** outbound body bytes → returns `len`. Consecutive writes accumulate into one message; the first write after a flush starts a new message. Flush many messages without reading (pipelining is allowed from the start). `-1` = would exceed `slot_size` (`EMSGSIZE`). Acquiring a TX slot busy-spins under saturation (never fails). |
| `ssize_t dmesh_sendfile(dmesh_conn_t *c, int in_fd, off_t *offset, size_t count)` | Appends ≤`count` bytes from `in_fd` into the current message (**capped at `slot_size` → may be SHORT; check the return**); advances `*offset` if non-NULL. `0` = EOF on `in_fd`, `-1` on read error / no room (`EMSGSIZE`). |
| `int dmesh_flush(dmesh_conn_t *c)` | **The explicit ship — REQUIRED to send.** `0` = sent (or nothing buffered → no-op; a zero-length message is reserved for the FIN). `-1` `EBADMSG` = descriptor fault (close the conn). (Acquiring the TX slot happened in `write` and busy-spins under saturation, so flush itself never returns `EAGAIN`.) |
| `int dmesh_close(dmesh_conn_t *c)` | **Graceful close.** Sends a **FIN** (zero-length, behind all prior data) so the peer's `dmesh_read` returns `0` and the DPU frees the upstream; then frees local state. A buffered-but-unflushed message is discarded (flush first). Concurrent close is safe. Returns `0`. Safe on `NULL`. |

### Accessor
- `void *c->user_data` — **app-owned** (like epoll's `data.ptr`): set it after `accept`/`connect`,
  and `dmesh_next_ready` hands the conn back so you read your context off it. The transport never
  touches it.

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
dmesh_channel_t *s = dmesh_create_channel("client", /*pod_id*/10);
dmesh_conn_t *c = dmesh_connect(s, /*service*/11);   // address a service, not a pod

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

dmesh_conn_t *c = dmesh_connect(s, /*service*/11);

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
  spans slots**. Chunk larger payloads into independent ≤ 8 KB messages at the app layer, and note
  that once a service load-balances across several backends the transport does **not** guarantee
  such chunks land on the same backend (keep a coherent unit to one slot).
- **Response↔request matching is the app's job.** The transport does no matching, and this DPU
  proxy routes at the **connection** level, not the **request** level (it never parses the body). A
  protocol-parsing L7 proxy could correlate by stream-id; a metadata-driven DPU cannot. Carry a
  req-id and match on it — **especially** because per-message LB can reorder replies (§4c).
- **Delivery & readiness.** Bodies DMA host→DPU→host straight into the receiver's RX buffer; only a
  small descriptor + a 16-byte completion ride the control path. The PE thread delivers each message
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
                                            │  dma_copy  host ↔ DPU-staging          │                     •   peer = (pod10, uP2)
                                            │  staging[pod]  (32 MB, host→DPU→host)  │                     • RX buffer  rx_dma_buffer
                                            └────────────────────────────────────────┘                     • PE thread + event_fd

   descriptor posted per message (dma_desc, 64 B — the body is NOT in it, only a pointer + the tuple):
        { mmap, addr = &dma_buffer[slot], size,
          src = (pod, port),  dst = (service, pod, port),  seq,  valid }     ← the "oriented tuple"
   completion returned per DMA (16 B on the control path): { type, src/dst pod, ports, seq, len, pos }
```

**One message — `pod10:pC → service 11`, LB'd to `pod 11`, then its reply:**
```
  FORWARD  (client request → backend)
  (1) host10  write → dma_buffer[slot];  flush posts a dma_desc:
              src=(10, pC)   dst=(svc 11, BLANK, BLANK)   seq=s          ← dst_pod BLANK = "DPU, route me"
  (2) DPA EU  dma_copy  host10 dma_buffer[slot]  →  DPU staging[pod10]
  (3) ARM     dpu_route(11) → pod 11 ;  reuse or create upstream → uP1 ;  upstream[uP1]={10,pC,pod11}
              rewrite tuple → src=(10, uP1)  dst=(pod11, uP1)            ← backend will see the DPU id uP1
  (4) DPA EU  dma_copy  DPU staging[pod10]  →  host11 rx_dma_buffer      (in-place; no DPU CPU copy)
  (5) host11  PE: dst_port=uP1 not live → accept queue → dmesh_accept → server conn uP1, peer=(10,uP1)
              app dmesh_read → the message
      ARM     TX_ACK to (pod10, pC, s)   ← uP1 translated back to pC → frees host10's TX slot

  REPLY  (backend → its peer)
  (6) host11  write → flush     src=(11, uP1)   dst=(pod10, uP1)          ← concrete dst → no LB, direct
  (7) DPA     dma_copy  host11 → DPU staging[pod11] → host10 rx_dma_buffer
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
  DMA; only the descriptor and a 16 B completion ride the control path.
- **uP** — the DPU-assigned upstream id: the backend's server-conn port, and the key the DPU maps
  ↔ `(client pod, client port, backend)` to route replies home.
