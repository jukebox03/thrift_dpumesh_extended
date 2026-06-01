# DPUmesh 시스템 분석

## 0. 5개 구성요소 한 줄 요약

| 구성요소 | 위치 | 역할 |
|---|---|---|
| **Gateway** | Host (x86), 별도 프로세스 | TCP 입구. 클라이언트 ↔ DPUmesh 간 brigde |
| **Unique-ID Service** | Host (x86), pod_id=0 | Thrift 비즈니스 로직 + `TDpumeshServerTransport` |
| **DPU** | BlueField ARM | comch 컨트롤 평면 + 라우팅 + DMA 스테이징 + **1kHz DPA keepalive** |
| **DPA** | BlueField RISC-V (HW DMA engine) | **hybrid spin/yield** DMA descriptor polling + `dma_copy` 실행 (idle 시 `thread_reschedule`) |
| **DMA Machine** | Host ↔ DPU shared mmap | 2048-slot ring (+ 1 credit slot) + body-pool buffer + 64B descriptor |

핵심 구조: 모든 host(gateway, unique-id-service)는 **동일한 transport 라이브러리** (`dpumesh_doca.c`)를 통해 DPU에 연결되며, `pod_id` 와 `OP_REQUEST/OP_RESPONSE` flag로 라우팅이 결정됨.

---

## 1. Component-level diagram

```
┌─────────────────────────────────  HOST (x86)  ─────────────────────────────────┐
│                                                                                │
│  ┌──── gateway (pod_id=1) ────┐         ┌──── unique-id-service (pod_id=0) ──┐ │
│  │  TCP :9091 (epoll, N work) │         │  TThreadedServer                   │ │
│  │  ↓ enqueue OP_REQUEST       │         │  ↑ TDpumeshServerTransport         │ │
│  │  ↑ wait_response            │         │  ↓ TDpumeshTransport (per req)     │ │
│  └────────────┬───────────────┘         └────────────┬───────────────────────┘ │
│               │ libthriftd (dpumesh_doca.c)         │                          │
│               │  - dma_buffer (16 MB tx body pool)  │                          │
│               │  - rx_buffer  (16 MB rx body pool)  │                          │
│               │  - rx_dma_buffer (DPU→Host DMA dst) │                          │
│               │  - dma_ring (2048 × 64B desc + 1     │                          │
│               │              credit slot @end)      │                          │
│               │  - pending[MAX_PENDING] (req↔resp)  │                          │
│               │  - PE thread (drives doca_pe)       │                          │
│               └────────────┬─────────────────────────┘                          │
│                            │                                                   │
│  ════════════ comch ctrl path ═════════════ DMA (PCIe) ═════════════════════   │
└────────────────────────────┼──────────────────────────────────────────────────┘
                             │
┌────────────────────────────┼──────────────────────────  BlueField DPU  ───────┐
│                            ▼                                                  │
│  ┌─────────────────────  DPU ARM (dpu_main + dpu_worker) ──────────────────┐  │
│  │                                                                        │  │
│  │  comch_server  ←→  per-pod connection (REGISTER/MMAP/TX_ACK/COMP)       │  │
│  │       │                                                                │  │
│  │       ▼                                                                │  │
│  │  pod_state[pod_id]:                                                    │  │
│  │    dma_buffer    ── DPU RX body pool (forward staging) ──┐             │  │
│  │    tx_buffer     ── DPU TX body pool (reverse staging) ──┤             │  │
│  │    forward ring  ── exported by host  (Host→DPU)         │             │  │
│  │    reverse ring  ── owned by DPU      (DPU→Host)         │             │  │
│  │                                                                        │  │
│  │  Main loop:                                                            │  │
│  │   doca_pe_progress(pe)         ← comch ctrl                            │  │
│  │   doca_pe_progress(consumer_pe)← DPA→DPU msgq (DMA_COMPLETED)          │  │
│  │   drain_deferred_tx_acks()                                             │  │
│  │   process_completion_queue() → process_forward / rev_notify            │  │
│  │   1kHz TRIGGER → DPA (max_run_time reset + idle wake bound 1ms)        │  │
│  └────────────────────────┬───────────────────────────────────────────────┘  │
│                           │ comch msgq (ADD_RING / DMA_REQ / TRIGGER)        │
│                           │      ▲ DMA_COMPLETED / DMA_CHUNK / REV_DMA_DONE  │
│                           ▼      │                                            │
│  ┌─────────────────────  DPA RISC-V (dpa_kernel.c::run_dma_manager) ─────┐   │
│  │                                                                       │   │
│  │  hybrid spin / yield loop:                                            │   │
│  │    while (1) {                                                        │   │
│  │      handle_msgs(thread_arg)   ← consumer_completion: TRIGGER/ADD_RING│   │
│  │      chunks = drain_all_rings():                                      │   │
│  │        for each forward ring r: process_one_desc(r)                   │   │
│  │             desc.valid==1 → dma_copy(host→DPU RX) chunked 8KB         │   │
│  │             final chunk → DMA_COMPLETED (immediate ≤32B)              │   │
│  │        for each reverse ring r: process_one_rev_desc(r)               │   │
│  │             dma_copy(DPU TX→host RX) chunked                          │   │
│  │             final chunk → REV_DMA_COMPLETED                           │   │
│  │      drain_producer_completions()  ← free 1024 producer slots         │   │
│  │      if (chunks == 0)                                                 │   │
│  │        doca_dpa_dev_thread_reschedule()  ← idle만 yield, timer reset  │   │
│  │    }                                                                  │   │
│  │                                                                       │   │
│  │  Why yield: doca_dpa_get_kernel_max_run_time = 12 s on BF-3.          │   │
│  │  A pure while(1) spin exceeds it → silent fatal termination →         │   │
│  │  recv: 0/s 영구 stuck (관측됨). chunks==0 reschedule이 timer를         │   │
│  │  자연스러운 idle gap마다 reset해서 재발 방지. 부하 중에는 chunks > 0   │   │
│  │  이 유지되므로 reschedule 호출 0회 → polling 처리량 그대로 유지.      │   │
│  │                                                                       │   │
│  │  Hardware DMA engine = doca_dpa_dev_comch_producer_dma_copy()         │   │
│  │    src 64B aligned, size 128B aligned, max 8 KB / call                │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 4종류의 메시지/디스크립터 (헤더)

이 시스템에는 4개의 별개 "메시지 / descriptor" 타입이 있다. 헷갈리면 path가 안 보임.

| 이름 | 크기 | 어디에 사는가 | 누가 쓰는가 → 누가 읽는가 |
|---|---|---|---|
| **`sw_descriptor_t`** (`dpumesh.h:43`) | 64 B | host RAM, transport API 인자 | host application ↔ host transport (gateway / Thrift transport). DPU/DPA는 보지 않음 |
| **`dma_desc`** (`dpa_common.h:133`) | 64 B (assert) | DMA ring (host export 또는 DPU local) | host transport 쓰기 / DPA 읽기 (forward), DPU 쓰기 / DPA 읽기 (reverse) |
| **`fc_header`** (`dpa_common.h:65`) | 4 B (`payload_len`) | DMA 페이로드 맨 앞 (slot 베이스) | sender가 prepend → receiver가 parse |
| **`comch_msg` family** (`dpa_common.h:71-129`) | ≤ 32 B (immediate data) | DPU↔DPA comch FIFO | DPU ARM ↔ DPA. `DMA_REQ`, `ADD_RING`, `ADD_REV_RING`, `TRIGGER`, `DMA_COMPLETED`, `DMA_CHUNK`, `REV_DMA_COMPLETED` |
| **`dmesh_*_msg`** (control path) | varies | DPU↔Host comch 컨트롤 채널 | `DMESH_MSG_REGISTER`, `MMAP`, `TX_ACK`, `DMA_COMPLETION`, `POD_CONSUMER_ID` |

핵심 invariant (`dpumesh_common.h:26-52`):
- `num_slots(2048) × slot_size(8 KB) = DPU_BUFFER_SIZE = 16 MB`
- `DMA_RING_SIZE = 2048` 디스크립터 슬롯 + **1개 추가 슬롯**(인덱스 = `DMA_RING_SIZE`)이 reverse-path RX credit 카운터로 사용됨 (`ring.c:30-46`).
- per-payload = `[fc_header(4B)][body]`. 메타데이터(req_id, src/dst_pod, flags)는 **payload 안에 절대 넣지 않음** — `dma_desc`와 `comch_dma_comp_msg`가 운반함. → reverse 쪽 staging 풀이 forward와 같은 footprint를 유지.

---

## 3. Forward path: Host → DPU (요청)

`gateway.c:212-281`(`process_request`) + `dpumesh_doca.c:740-839`(`dpumesh_enqueue`) + `dpa_kernel.c:262-481`(`process_one_desc`) + `dpu_worker.c:113-228`(`process_forward_entry`)

```
client TCP                 gateway worker(epoll)            host transport(libthriftd)
    │                           │                                │
    ├─[4B size][thrift frame]──►│                                │
    │                           │ admission_acquire (cap=900)    │
    │                           │ tx_slot = dpumesh_tx_alloc()   │
    │                           │ register_pending(req_id)       │
    │                           │ memcpy(req_buf → tx_buf)       │
    │                           │ desc.flags=OP_REQUEST|CASE_EXT │
    │                           │ desc.dst_pod_id=0(unique-id)  │
    │                           ├──dpumesh_enqueue(desc)────────►│
    │                           │                                │ ① tx_buf 시작에 fc_header.payload_len 기록
    │                           │                                │ ② get_next_dma_desc(dma_ring) → dma
    │                           │                                │ ③ dma->addr=tx_buf 절대주소
    │                           │                                │    dma->size=4+body_len
    │                           │                                │    dma->idx=req_id (DPA가 그대로 회송)
    │                           │                                │    dma->flags, dst_pod_id 복사
    │                           │                                │ ④ __sync_synchronize(); dma->valid=1
    │                           │                                │
    │                           │                                ▼  (HW: PCIe BAR mmap, no notify)
    │                           │                                │
                                                                 │
                                      DPA RISC-V (run_dma_manager: hybrid spin/yield)
                                                                 │
                                      while(1):
                                        handle_msgs()  ← TRIGGER/ADD_RING/ADD_REV_RING/DMA_REQ
                                        chunks = drain_all_rings():
                                          for each forward ring r:
                                            buf=buf_arr_get_buf(r, desc_idx[r])
                                            __dpa_thread_window_read_inv()
                                            if desc.valid==0: continue
                                            ──── process_one_desc ────
                                            chunked dma_copy (8 KB / call):
                                              src=ring->host_mmap @ desc.addr
                                              dst=ring->dpu_mmap  @ ring->dpu_addr+pos[r]
                                              comp_msg(immediate ≤32B):
                                                type=DMA_COMPLETED
                                                pos, length, req_id,
                                                src_pod_id=ring.pod_id,
                                                dst_pod_id, flags
                                              intermediate chunks  → DMA_CHUNK (consumer ignores)
                                              final chunk          → DMA_COMPLETED
                                            ensure_producer_slot(): drain 1024 slot pool
                                            pos[r] += ALIGN_UP_128(size)
                                            desc->valid=0; desc_idx[r]++
                                        drain_producer_completions()
                                        if (chunks == 0) thread_reschedule()
                                                  │
                                                  │  (DOCA HW: PCIe DMA + comch immediate)
                                                  ▼
                            ┌─────  DPU ARM (consumer_pe callback) ─────┐
                            │ on DMA_COMPLETED:                          │
                            │   comp_queue_enqueue(COMP_ENTRY_FORWARD)   │
                            │     {pod_idx, buf_offset=pos, length,     │
                            │      req_id, src_pod_id, dst_pod_id,      │
                            │      flags}                                │
                            │ on DMA_CHUNK: doca_task_resubmit only      │
                            └────────────────────────────────────────────┘
                                                  │
                            DPU main loop: process_completion_queue(128/iter)
                                                  │
                            process_forward_entry(entry):
                              data = pod[pod_idx].dma_buffer + buf_offset
                              ──► [fc_header][body]  (zero-copy from DPU RX pool)
                              fwd_desc.flags = (entry.flags & OP_RESPONSE)|CASE_INGRESS
                              dpu_enqueue_reverse_dma(target_pod, fwd_desc, body, len)
                                = (a) write [fc_header][body] into target_pod.tx_buffer
                                  (b) get_next_dma_desc(target_pod.tx_ring)
                                  (c) dma->src_pod_id = entry.src_pod_id (원래 발신자)
                                      dma->dst_pod_id = entry.dst_pod_id
                                  (d) dma->valid=1
                              server_send_tx_ack_to(src_pod, req_id)  ← src의 TX 슬롯 해제
                              if comch send pool full → defer to deferred_tx_acks queue
```

여기서 forward 경로의 두 가지 "메시지 시점":
- **DMA_COMPLETED** (DPA → DPU ARM, 32B immediate): DMA가 host→DPU buffer로 끝남.
- **TX_ACK** (DPU → src host, comch ctrl): host가 자기 TX 슬롯을 release할 수 있다는 신호. 이게 안 오면 host는 2초 collision-wait reclaim까지 기다림. dpu_worker.c:199-225 에서 "hard guarantee" 처리됨.

---

## 4. Service에 도착: DPUmesh → UniqueIdService

unique-id-service는 `TDpumeshServerTransport` 위에서 평소 `TThreadedServer`로 돈다. accept loop는 다음과 같다 (`TDpumeshServerTransport.cpp:49-72`):

```
TThreadedServer::serve()
  ├─ acceptImpl():
  │    desc = dpumesh_dequeue(ctx, 1000ms)   ← rx_queue (OP_REQUEST만)
  │    return new TDpumeshTransport(ctx, desc)
  │
  └─ TConnectedClient::run() loop:
       processor->process(iprot, oprot)
         ├─ TDpumeshTransport::read()  ← rx_buf[rx_slot] 직접 포인터, zero-copy
         │     read_pos < read_len → memcpy 한 번 (transport→thrift internal)
         │     read_pos == read_len & flushed → fetch_next_request()
         │       (dequeue 다음 desc, 같은 transport 재사용 → 스레드 1개로 N개 요청)
         │       → IDLE_TIMEOUT_MS 안에 안 오면 read()=0 ⇒ 스레드 종료
         │
         ├─ Generated::process_ComposeUniqueId()
         │     → UniqueIdHandler::ComposeUniqueId(...)  (machine_id|ts|counter)
         │
         └─ TDpumeshTransport::write() / flush():
              write: lazy tx_alloc, 직접 tx_buf에 복사 (vector bouncing 없음)
              flush:
                register_pending(stream_id)        ← 같은 req_id 재사용
                attach_tx(stream_id, tx_slot)      ← enqueue 전에 attach
                desc.flags = (req_flags & ~OP_REQUEST)|OP_RESPONSE
                desc.dst_pod_id = src_pod_id (원 발신자=gateway)
                dpumesh_enqueue(desc)
                pending_release_async(stream_id)   ← TX_ACK가 라이프사이클 종료
```

요점:
- **per-request transport**가 아니라 한 runner 스레드가 dequeue로 N requests를 직렬 처리. 이전에 connection-당 thread를 만들던 모델이 6만 RPS에서 thread thrashing 일으켜 이 구조로 바뀜.
- `flush`는 응답을 "register → attach → enqueue → release_async" 패턴으로 보낸다. wait_response 안 함 (응답에 응답이 올 일은 없으니까). TX_ACK가 `state -2 → -1` 시켜서 슬롯을 회수.

---

## 5. Reverse path: DPU → Host (응답)

응답 frame은 forward 경로와 똑같이 unique-id-service의 host transport에서 `dpumesh_enqueue`로 들어간다. 하지만 `flags=OP_RESPONSE | CASE_INGRESS` 이므로 DPU에서 처리가 다르다.

```
unique-id-service host transport
   │  desc.flags=OP_RESPONSE, dst_pod_id=gateway_pod_id(=1)
   │  dma_ring write → DPA forward ring (pod_id=0 ring)
   ▼
DPA: process_one_desc(forward ring r=0)
   │  dma_copy(host TX → DPU RX pod[0].dma_buffer)
   │  comp_msg.type=DMA_COMPLETED, src_pod_id=0, dst_pod_id=1, flags=OP_RESPONSE|CASE_EXT
   ▼
DPU: process_forward_entry(entry)
   │  data = pod[0].dma_buffer + buf_offset  ← payload 그대로
   │  dst_pod_id=1 (gateway) → target_pod = pod[1]
   │  dpu_enqueue_reverse_dma(pod[1], fwd_desc, body, len):
   │     (a) [fc_header][body] → pod[1].tx_buffer
   │     (b) post dma_desc onto pod[1].tx_ring (= reverse ring viewed from DPA)
   │     (c) src_pod_id=0 (원래 응답 발신자), dst_pod_id=1
   │  server_send_tx_ack_to(pod[0]_conn, req_id)  ← unique-id의 TX 슬롯 해제
   ▼
DPA: process_one_rev_desc(reverse ring of pod[1])
   │  ── admission gate (credit-return) ──
   │     inflight = dpa_sent_count[r] − dpa_cached_freed[r]
   │     if inflight ≥ pod[1].rq_depth → defer (return 0, retry next iter)
   │     dpa_cached_freed[r]는 drain_all_rings에서 lazy refresh:
   │       inflight + CREDIT_REFRESH_MARGIN(64) ≥ rq_depth 일 때만
   │       host의 credit slot(forward dma_ring 끝의 +1 슬롯) 1워드 PCIe read
   │  ──────────────────────────────────
   │  dma_copy(DPU TX pod[1].tx_buffer → Host pod[1].rx_dma_buffer)
   │  comp_msg.type=REV_DMA_COMPLETED, src=0, dst=1, req_id
   │  dpa_sent_count[r]++  (admission accounting)
   ▼
DPU: consumer_pe callback on REV_DMA_COMPLETED
   │  comp_queue_enqueue(COMP_ENTRY_REV_NOTIFY)
   ▼
DPU main loop: process_rev_notify_entry
   │  target = pod[1] (gateway)
   │  dmesh_dma_completion_msg{pos, length, req_id, src=0, dst=1, flags}
   │  server_send_msg_to_conn(pod[1].connection, ..., sizeof comp)
   ▼
Gateway host transport: rx_data_hook (dpumesh_doca.c:275-380)
   │  type==DMA_COMPLETION:
   │    process_rx_dma_entry(pos, len, req_id, src, dst, flags)
   │      buf = rx_dma_buffer + pos
   │      hdr = (fc_header*)buf
   │      slot = rx_slot_alloc()
   │      memcpy(rx_buffer[slot], buf+4, hdr->payload_len)
   │      desc.flags=OP_RESPONSE
   │    rx_deliver_desc:
   │      if OP_RESPONSE → pending[req_id % MAX_PENDING]
   │        state==0 → desc=*; state=1; cond_signal()
   │        state==-2 (timeout 후 cancel됐던 것) → tx free + rx free
   │
   │  (later) dpumesh_rx_free(slot):
   │    rx_slot_bitmap[slot] = 0
   │    __sync_add_and_fetch(&dma_ring->descs[size].first8B, 1)
   │      ← DPA가 lazy-poll하는 credit counter. ~10ns hot-path 비용.
   ▼
Gateway worker waiting in dpumesh_wait_response:
   resp slot=desc.body_buf_slot, len=desc.body_len
   tcp_send_all(client_fd, rx_buffer[slot], resp.body_len)
   dpumesh_rx_free(slot)
```

---

## 6. Completion / Polling / Message — 어디서 어떻게 일어나는가

| 종류 | 발생 위치 | 메커니즘 | 역할 |
|---|---|---|---|
| **DMA descriptor polling** | DPA RISC-V | `desc->valid` 비트 spin (`__dpa_thread_window_read_inv`); 부하 중에는 spin 유지 (chunks > 0 → no reschedule) | host가 enqueue한 forward 요청, DPU가 enqueue한 reverse 요청 picking |
| **Reverse-path admission gate** | DPA RISC-V | `dpa_sent_count[r] − dpa_cached_freed[r] ≥ rq_depth` 시 `process_one_rev_desc` defer. `dpa_cached_freed[]`는 `drain_all_rings`에서 lazy refresh — inflight가 cap 근처(`+ CREDIT_REFRESH_MARGIN=64`)일 때만 host credit slot 1워드 PCIe read. | DPU→Host reverse DMA가 host RX RQ를 overrun하는 것 방지. 저부하에서 PCIe read 0회 → 거의 zero overhead. |
| **DPA reschedule (idle yield)** | DPA RISC-V | drain_all_rings 결과 chunks==0이면 `doca_dpa_dev_thread_reschedule()` | 협력적 스케줄링 — work 없을 때 EU 양보. (이전 가설인 `max_run_time=12s` 초과 종료는 retest에서 재현 안 됨; `dpa_kernel.c:759-766` 주석. 한 활성화에서 30M+ iter spin 가능 확인.) 부하 중 호출 0회. |
| **DMA completion (DPA→DPU)** | DPA → DPU consumer comch | `dma_copy()`의 마지막 chunk에 immediate data로 `comch_dma_comp_msg` (≤32B) 첨부 | host→DPU 또는 DPU→host DMA가 끝났음을 DPU ARM에 알림 |
| **DMA completion (DPU→Host)** | DPU comch → Host comch | `DMESH_MSG_DMA_COMPLETION` (control msg) | DPU가 host RX buffer에 reverse DMA 끝낸 뒤 host에 위치/길이 알림 |
| **Producer slot completion** | DPA self | `doca_dpa_dev_get_completion(producer_comp)` poll | `dma_copy`마다 1슬롯 소모, 1024 slot pool, drain 안 하면 silently fail |
| **Consumer recv completion** | DPA self | `doca_dpa_dev_comch_consumer_get_completion(consumer_comp)` | DPU→DPA control msg (ADD_RING 등) picking |
| **DPU PE progress** | DPU ARM | `doca_pe_progress(pe)` (ctrl) + `doca_pe_progress(consumer_pe)` (DPA→DPU msgq) | comch 이벤트 디스패치, send-pool 슬롯 회수 |
| **TX_ACK** | DPU → Host comch | `DMESH_MSG_TX_ACK{req_id, dst_pod}` | host TX 슬롯 라이프사이클 종료 (pending state 0/-2 → -1) |
| **TRIGGER (keepalive)** | DPU → DPA comch (1 kHz) | `COMCH_MSG_TYPE_TRIGGER` empty msg, `dmesh_doca_dpa_msgq_send_try` (fire-and-forget) | DPA reschedule 후 host/DPU가 새로 post한 desc 발견 보장. idle→active wake 최대 1 ms. 부하 중에는 spin 상태라 사실상 no-op. |
| **Pending wait** | Host transport | `pending[req_id%MAX_PENDING]` mutex+cond, 30 s timeout | gateway worker가 응답 도착까지 block, OP_RESPONSE 도착 시 cond_signal |
| **RX credit return** | Host (`dpumesh_rx_free`) | `__sync_add_and_fetch(&dma_ring->descs[size].first8B, 1)` — `DMA_RING_SIZE`+1번째 슬롯이 credit 카운터. forward dma_ring buf_arr를 그대로 재활용 (별도 mmap/buf_arr 없음). | DPA의 reverse-path admission gate에 free 신호. ~10ns. |
| **Host TX wait backoff** | Host transport | `dpumesh_tx_alloc` cond_timedwait 50µs 백스톱 (이전 1ms), `dpumesh_enqueue` ring slot exponential backoff 1µs→50µs cap (이전 10µs→1ms cap) | cap-region에서 1ms wait이 ~44 req-worth latency 잡아먹는 문제 해결 |
| **PE thread** | Host (dedicated thread) | `pe_progress_fn` 별도 스레드 | comch ctrl msg 콜백 (`rx_data_hook`)을 driving |

---

## 7. 5개 구성요소의 책임 매트릭스

| | TCP | Frame parse | Pending table | Slot/Buf admission | DMA ring | DMA copy 실행 | Routing | Backpressure |
|---|---|---|---|---|---|---|---|---|
| Client (wrk) | ✓ | | | | | | | TCP window |
| **Gateway** | ✓ (epoll) | ✓ (4B size + thrift) | ✓ (req↔resp) | ✓ (tx_alloc/rx_alloc) | ✓ (ring write via libthriftd) | | dst_pod=0 (hardcoded) | admission cap (900<2048) + epoll pause + TCP backpressure |
| **Unique-ID Service** | | | ✓ (per response) | ✓ | ✓ (ring write via libthriftd) | | dst_pod=src (응답) | TX slot pool + RX credit-return on `rx_free` |
| **DPU ARM** | | | | (forwarder, no own slots) | reverse ring write | | by `pod_id`+flags | deferred TX_ACK queue + `comp_queue` 128/iter + `recv_tasks_in_flight` pool + **1 kHz DPA keepalive** |
| **DPA** | | | | producer 1024 slots + **reverse admission gate** (cached `freed_cumulative` vs `rq_depth`) | poll forward+reverse rings (idle 시 reschedule) | ✓ (`dma_copy` HW) | ring → comp_msg | reverse-path defer when `inflight ≥ rq_depth` |
| **DMA Machine** | | | | | shared mmap (+ 1 credit slot) | (passive) | | none |

Flow control은 **4-layer** 구조 (`dpa_kernel.c:520-548`, `dpumesh_doca.c:683-728/892-904` 등):
1. **End-node TX/RX slot admission**: host의 `slot_bitmap` / `rx_slot_bitmap` (`num_slots × slot_size = 16 MB = DPU_BUFFER_SIZE` 불변식이 DPU staging overflow 방지).
2. **Forward DMA ring slot** (host→DPU): `DMA_RING_SIZE=2048` slot 짜리 ring, 1µs→50µs exponential backoff.
3. **Reverse DMA admission gate** (DPA→host RX RQ): credit-return 카운터 한 워드. host `__sync_add_and_fetch` (~10ns), DPA lazy-poll (cap 근처에서만 PCIe read). forward `dma_ring`을 1슬롯 늘려서 마지막 슬롯을 credit으로 재활용 (별도 mmap 없음).
4. **DPU staging buffer 정적 sizing**: `DPU_BUFFER_SIZE=16 MB` per pod. N-source × dst worst-case fan-in 견디기 위해 8MB→16MB로 키움 (`dpumesh_common.h:42-45`).

**DPA kernel runtime 제약 (재검증됨)**: 초기 가설 — `doca_dpa_get_kernel_max_run_time()`이 BF-3에서 **12 s** 반환하므로 while(1) 무한 spin이 한도 초과 시 fatal 종료 — 은 `dpa_kernel.c:759-766` retest에서 **재현되지 않음**. 단일 활성화에서 30M+ iter, 수 초 spin이 정상 작동함 확인. 그래도 `if (chunks == 0) doca_dpa_dev_thread_reschedule()`은 **협력적 스케줄링** 차원에서 유지 — work 없을 때 EU 점유는 낭비. idle→active 재시동은 DPU의 1 kHz keepalive TRIGGER가 보장 (P50 latency 영향 ≤ 0.5 ms).

---

## 8. 한 요청의 라이프사이클 (numeric 예: ComposeUniqueId)

```
T0  client → TCP frame [size][thrift] → gateway:9091 (epoll-ET worker w0)
T1  w0: admission_acquire (in_flight++)
T2  w0: req_id=R, tx_slot=S0, register_pending(R), memcpy req → tx_buf[S0]
T3  w0: dpumesh_enqueue(desc{flags=REQUEST|EXT, dst=0, src=1, body=tx_buf[S0]})
        → fc_header.payload_len=req_len 기록
        → dma_ring[ring_slot] = {addr=&tx_buf[S0], size=4+req_len, idx=R, flags, dst=0, valid=1}
T4  DPA poll (forward ring, pod=1): desc.valid==1
        → chunked dma_copy → DPU pod[1].dma_buffer
        → final imm: DMA_COMPLETED{pos, len, R, src=1, dst=0, flags}
T5  DPU consumer cb: comp_queue.enq(FORWARD entry)
T6  DPU main loop: process_forward_entry
        → target=pod[0] (unique-id-service)
        → dpu_enqueue_reverse_dma(pod[0], desc, body)
            → write [fc_header][body] to pod[0].tx_buffer
            → post desc on pod[0].tx_ring (reverse ring as DPA sees it)
        → server_send_tx_ack_to(pod[1] gateway, R)
T7  gateway rx_data_hook(TX_ACK{R}): pending[R%]: tx_slot S0 free, in_flight--
T8  DPA poll (reverse ring of pod[0]): desc.valid==1
        → dma_copy DPU pod[0].tx_buffer → host pod[0].rx_dma_buffer @ pos
        → final imm: REV_DMA_COMPLETED{pos, len, R, src=1, dst=0}
T9  DPU consumer cb: comp_queue.enq(REV_NOTIFY entry)
T10 DPU main loop: process_rev_notify_entry → DMA_COMPLETION ctrl msg → pod[0]
T11 unique-id host rx_data_hook(DMA_COMPLETION):
        → process_rx_dma_entry: rx_slot=Su, memcpy fc body → rx_buffer[Su]
        → desc.flags has CASE_INGRESS bit (≠ OP_RESPONSE pure) → push to rx_queue
T12 unique-id TDpumeshServerTransport::acceptImpl returns TDpumeshTransport(desc=R)
        OR runner thread's fetch_next_request() picks it up
T13 runner thread: read body → UniqueIdHandler::ComposeUniqueId() → 8B post_id
T14 TDpumeshTransport::flush:
        register_pending(R), tx_slot=Sresp, write resp to tx_buf[Sresp]
        attach_tx(R, Sresp), enqueue(desc{flags=OP_RESPONSE, dst=1, src=0, body=Sresp})
        release_async(R)
T15 DPA forward ring(pod=0): dma_copy host→DPU pod[0].dma_buffer
        DMA_COMPLETED{R, src=0, dst=1, flags=OP_RESPONSE|...}
T16 DPU process_forward_entry → target=pod[1] (gateway, dst≠src):
        → dpu_enqueue_reverse_dma(pod[1])  // 응답을 gateway 쪽 reverse ring에
        → server_send_tx_ack_to(pod[0] unique-id, R)  → unique-id의 Sresp 해제
T17 DPA reverse ring(pod=1): dma_copy DPU pod[1].tx_buffer → gateway rx_dma_buffer
        REV_DMA_COMPLETED
T18 DPU → DMA_COMPLETION ctrl → gateway pod[1]
T19 gateway rx_data_hook: process_rx_dma_entry: rx_slot=Sg, body→rx_buffer[Sg]
        flags has OP_RESPONSE → rx_deliver_desc routes to pending[R%]
        pending[R].state=0 → desc=*, state=1, cond_signal
T20 gateway w0 unblocks from dpumesh_wait_response, gets resp.body_buf_slot=Sg
T21 w0: tcp_send_all(fd, rx_buffer[Sg], resp.body_len); rx_free(Sg)
T22 w0: admission_release; epoll_wait next
```

전체 한 요청 = host TX 슬롯 2개(S0, Sresp), host RX 슬롯 2개(Su, Sg), DMA ring 슬롯 2개(forward+reverse 양쪽), DPA producer 슬롯 = total chunks (size/8 KB ceil), comch 메시지 4개(DMA_COMPLETED×2, REV_DMA_COMPLETED×2, TX_ACK×2, DMA_COMPLETION×2).

---

## 9. 앞서 RFC와 무엇이 달라졌나 (`architecture/README.md` vs 실제 코드)

`architecture/README.md`는 이제 현재 DOCA 구현을 반영하도록 갱신되었다. 원래 RFC의 **Python 시뮬레이션 설계**(`dpumesh/dpa_daemon.py`, `dpumesh/dpu_daemon.py`, TCP bridge :5050) 대비 핵심 변경은:

- **DPU/DPA 실제 BlueField 하드웨어** 위에서 DOCA SDK로 구현 (`lib/cpp/src/thrift/transport/doca/`).
- **별도 gateway 프로세스**(`gateway.c`, port 9091)가 도입되어 외부 TCP 클라이언트를 받아 DPUmesh 라우팅으로 변환.
- DPU↔Host 컨트롤은 SHM/flock이 아니라 **DOCA comch ctrl path**.
- DPU↔DPA는 **DOCA comch msgq + DPA RISC-V 커널** (`run_dma_manager` hybrid spin/yield: idle 시 reschedule, 부하 중 spin 유지).
- `sw_descriptor_t`는 host transport API에만 남고, 실제 wire는 `dma_desc(64B)` + `fc_header(4B)` + `comch_dma_comp_msg(≤32B)`로 분리됨.

### DPA 사망/회복 (현재 설계의 출발점)

초기 구현은 `run_dma_manager`를 *순수* `while(1)` spin loop로 두고 thread_reschedule 호출하지 않았다. 배포 직후 DPA EU가 점유 0%로 떨어지면서 forward DMA 영구 무시 → host gateway worker가 wait_response 30 s 타임아웃 cycle 들어가는 증상이 관측됨:

- DPU/host 측 어느 누구도 감지 못 함 (우리 코드는 `doca_dpa_peek_at_last_error()` 호출 0회)
- DPU→DPA msgq의 1 Hz keepalive조차 drain 안 되어 ~17 분 후 producer task pool 1024 가득 → `DOCA_ERROR_AGAIN` 폭주
- `dpa-statistics collect --timeout 2000 --reset`이 `Cycles=0, Executions=0` 반환 → DPA EU 점유 0%로 확정

당시 추정 원인은 BlueField-3의 `doca_dpa_get_kernel_max_run_time()` = **12 초** 한도 초과로 인한 silent fatal kernel 종료였다. 그러나 이후 retest에서 (`dpa_kernel.c:759-766` 주석) 단일 활성화 시 30M+ iter, 수 초 spin이 정상적으로 동작함이 확인되어 **12s 한도 가설은 재현되지 않음**. 진짜 원인은 별도 — 가능성으로는 (a) producer task pool 고갈로 dma_copy 발행이 silent fail, (b) DPU send-pool / msgq 점유로 DPA가 통신 못 하는 상태 등이 있음.

어쨌든 현재 설계는 idle 시 yield하는 보수적 패턴으로 안정화됨:
1. `run_dma_manager`에 `if (chunks == 0) doca_dpa_dev_thread_reschedule();` 삽입. 부하 중 처리율 손실 0, idle 시 EU 양보(협력적 스케줄링).
2. DPU main loop 1 kHz keepalive TRIGGER로 reschedule된 DPA를 다음 1 ms 안에 wake.
3. `dmesh_doca_dpa_msgq_send_try` (fire-and-forget) 변형 추가 — 10000-step retry loop가 main thread를 점유하지 않도록.

검증: 40 k RPS × 30 s × 12 회 연속 100 % 성공 (14.4 M reqs, 0 failures). P50 0.41 ms, P99 2.1 ms.

### Reverse-path admission gate (commit 225a543a)

이전에는 DPA가 reverse DMA를 host RX RQ 가득 차도 멈출 수단이 없었다. 추가된 메커니즘:

- Host: `dpumesh_rx_free`에서 `dma_ring` 끝의 +1 슬롯에 `__sync_add_and_fetch(credit, 1)` (~10 ns)
- DPA: `dpa_sent_count[r] − dpa_cached_freed[r] ≥ rq_depth` 시 `process_one_rev_desc`가 0 리턴(defer)
- Lazy refresh: `inflight + CREDIT_REFRESH_MARGIN(64) < rq_depth` 이면 PCIe read 스킵 → 저부하에서 거의 0 overhead, cap 근처에서만 refresh
- 영리한 점: 별도 mmap/buf_arr 안 만들고 forward `dma_ring`을 1슬롯 늘려서 그 슬롯을 credit 카운터로 재활용 (`ring.c:30-46`, `dpa.c:1187` `DMA_RING_SIZE+1`로 buf_arr 생성)

---

## 10. 핵심 파일 색인

| 영역 | 파일 |
|---|---|
| Gateway entry | `gateway.c:1-518` |
| Host transport | `lib/cpp/src/thrift/transport/dpumesh_doca.c:1-1158` |
| Host transport API | `lib/cpp/src/thrift/transport/dpumesh.h` |
| 공용 헤더(상수/Pool/OP) | `lib/cpp/src/thrift/transport/doca/dpumesh_common.h` |
| Host↔DPA wire 헤더/desc | `lib/cpp/src/thrift/transport/doca/dpa_common.h` |
| DPU entry | `lib/cpp/src/thrift/transport/doca/dpu_main.c` |
| DPU main loop | `lib/cpp/src/thrift/transport/doca/dpu_worker.c:364-539` (1 kHz keepalive @ ~497) |
| DPU forward 핸들러 | `dpu_worker.c:113-228` (`process_forward_entry`) |
| DPU reverse 핸들러 | `dpu_worker.c:46-105` (`dpu_enqueue_reverse_dma`), `277-314` (`process_rev_notify_entry`) |
| DPA RISC-V kernel | `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c:700-733` (`run_dma_manager`, hybrid spin/yield with `chunks==0` reschedule) |
| DPA forward poll | `dpa_kernel.c:262-481` (`process_one_desc`) |
| DPA reverse poll | `dpa_kernel.c:493-650` (`process_one_rev_desc`) |
| DMA ring/buffer/mmap | `doca/dma.c`, `doca/buffer.c`, `doca/ring.c` |
| Thrift server transport | `TDpumeshServerTransport.cpp:36-72` |
| Thrift per-request transport | `TDpumeshTransport.cpp:77-220` |
