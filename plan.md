# DPUmesh — Phase 4 완료 (v4, 2026-05-15)

> v3 (2026-05-14) 폐기. v4는 Phase 4의 모든 fix가 적용된 안정 상태와
> flame graph 기반 최종 분석을 담는다.

---

## 0. TL;DR

**무엇**: Thrift 아래 깔리는 DPU 가속 mesh layer. App 코드 변경 0.
Mesh가 hdr (control) + body (data)를 분리해서 운반하며 DPU는 **L7 proxy
hook 자리** (현재 dummy passthrough — Phase 5에서 resolve / LB를 DPU에
이식 예정).

**최종 성능** (256B body, fair 1-core/pod):

| RPS | Achieved | p99 | Fails | 평가 |
|---|---:|---:|---:|---|
| 100k | 98.4k | 9.4 ms | 0 | 안정 |
| 130k | 127.6k | 14.3 ms | 0 | 안정 |
| 150k | 147.2k | 15.3 ms | 0 | 안정 |
| **170k** | **166.7k** | **16.7 ms** | **0** | **권장 한계** |
| 200k | 168.8k | 817 ms | 0 | plateau, latency 폭증 |

Plan v3 baseline (130k, p99 18.4 ms, back-to-back 10k cliff) 대비:
- Stable RPS **+30%**, p99 **−22%**, back-to-back 회귀 **해소**
- Chain HW 한계 17.35 Gbps 중 **2% 사용** (256B × 170k = 0.35 Gbps)

---

## 1. Architecture

### 1.1 두 파이프라인

```
                           ┌── DPU ARM ──┐
                           │ TX_ACK +    │   (control plane only —
                           │ DMA_COMP.   │    body는 ARM 통과 안 함)
                           └─▲─────▲────┘
                             │     │
              ┌──────────────┘     └──────────────┐
              │ hdr forward + reverse              │ forward complete
              │ (2-hop via DPU staging)            │ (CASE_DIRECT)
              │                                    │
   src host                                  dst host
   ┌─────────────┐                           ┌──────────────────┐
   │hdr_tx_buffer│──── DPA fwd DMA ──┐       │ hdr_rx_buffer    │
   │  (8 MB)     │                   │       │  (8 MB, wrap)    │
   ├─────────────┤            ┌──────▼──┐    ├──────────────────┤
   │ dma_buffer  │── DPA ─────│ DPU NIC │────► rx_dma_buffer    │
   │ (body 32MB) │ CASE_DIRECT│ engine  │    │ (body 32MB)      │
   └─────────────┘ (1 hop)    └─────────┘    └──────────────────┘
```

- **hdr path** (2-hop, DPU 매개): src `hdr_tx_buffer` → DPU staging →
  reverse DMA → dst `hdr_rx_buffer`. DPU L7 proxy hook 자리.
- **body path** (1-hop, CASE_DIRECT): DPA가 양쪽 host BAR을 직접 access.
  DPU staging 미사용. DPU ARM은 DMA_COMPLETION + TX_ACK 1회만 발사.

### 1.2 핵심 구성요소

| | 역할 |
|---|---|
| Thrift app | RPC 호출만 (`send_request` / `wait_response_v2`) |
| Mesh (host C) | 통합 builder (hdr+chunk pair, single lock, 8-way shard), peer table, expected ring + parked chunks |
| DPU ARM | hdr forward 매개, TX_ACK + DMA_COMPLETION 발사. **body 안 봄**. 미래 L7 proxy 자리 |
| DPA EU | DMA engine. forward (hdr: src host→DPU staging / body: src host→dst host 직접), reverse (hdr: DPU→dst hdr_rx). peer table + admission gate 보유 |

### 1.3 Per-RPC 메시지 흐름

```
bench worker (1 RPC):
  ├─ resolve(svc) → dst_pod_id
  ├─ atomic seq++
  ├─ register_pending_v2(rid) → state=0
  ├─ builder[dst][shard].lock              ← single lock; hdr+chunk atomic
  │   ├─ hdr_builder_append (mesh_hdr_req 80B)
  │   ├─ chunk_builder_append (body N bytes)
  │   └─ if cap: paired flush (hdr_tx 슬롯 + chunk_tx 슬롯 둘 다 enqueue)
  └─ wait_response_v2:
      ├─ futex_wait on p->state           ← spin 없음. 다이렉트 sleep.
      └─ on wake: read p->desc, return

PE thread (RX):
  doca_pe_progress → rx_data_hook
   ├─ if OP_HDR_BATCH: process_hdr_batch
   │   └─ for each entry: expected[src_id].push; touched bitmap
   │   └─ drain_parked_locked for touched src_ids
   └─ if OP_CHUNK:    process_chunk
       ├─ match: consume_chunk + drain_parked_locked (cross-shard 안전)
       └─ miss:  park (zero-copy)

consume_chunk → deliver_one_body × N:
  rx_slot_alloc + memcpy
  if OP_RESPONSE && pending matches:
     write p->desc; CAS state 0→1; if waiters>0 futex_wake_one
  else: push rx_queue (echo's worker_fn dequeues)

DPU ARM:
  CASE_DIRECT → DMA_COMPLETION to dst + TX_ACK to src (2 events/chunk)
  OP_HDR_BATCH → dpu_enqueue_reverse_dma → reverse DMA → REV_NOTIFY
```

---

## 2. 적용된 최적화 (v3 → v4)

| # | 변경 | 효과 |
|---|---|---|
| 1 | Dead routing flow 제거 (`mesh_lock`, `routing_cond`, `resolved_dst_pod_id`, `DMESH_MSG_ROUTING_INFO`) — plan v3 §9.3 폐기 잔재 | 메모리 + 코드 단순화 |
| 2 | **DPU TX_ACK pool_type fix** — `process_rev_notify_entry`가 OP_HDR_BATCH에도 `POOL_HOST_TX_BODY` 보내던 버그. hdr_tx 슬롯이 echo 측에서 영원히 누수 → 1024/1024 도달 후 back-to-back stall | **Back-to-back 10k cliff 해소** |
| 3 | Builder 통합 (hdr+chunk single lock) — 분리 락 race로 hdr append 순서 ≠ chunk body 순서 가능했음 | sharding 안전성 확보 |
| 4 | **Per-dst builder 8-way shard** — worker가 TLS-cached shard로 분산 | 1300 worker 직렬화 완화 |
| 5 | `process_chunk` match 후 `drain_parked_locked` 추가 — cross-shard interleave 안전 | 정확성 |
| 6 | **pthread_cond → futex direct + skip-wake-when-no-waiter** — `_Atomic state` + `_Atomic waiters` + atomic CAS 0→1 + waiters>0일 때만 futex_wake. spin 없음 (PE thread starve 방지) | PE thread wake 비용 감소 |
| 7 | `rx_slot_alloc` / `tx_alloc` round-robin cursor (O(N) → O(1) common) | 150k+ ops/sec에서 mutex hold 시간 단축 |
| 8 | **DPU hot-path 로그 8군데 silence** (TX_ACK queue full, reverse ring full, REV_NOTIFY 실패 등) — memory rule "No DPU log raise" 회귀 | DPU `/tmp` 폭증 → rsync 18 b/s hang 회귀 영구 차단 |
| 9 | test-bench.sh: DPU log truncate + `-l 30` (ERROR-only) | 안전망 |

---

## 3. Flame graph 기반 최종 분석

`bench/final_{bench,echo,dpu}_flame.svg` 참조 (150k load, 10초).

### 3.1 Bench (요청 발사 측)
대부분이 `libdoca_comch` 내부 polling. PE thread가 busy poll로 100% CPU.
나머지: `deliver_one_body` 135M, `futex_wake` 127M. 잘 정돈된 상태.

### 3.2 Echo (응답 측)
**Echo의 echo 로직 자체 (`process_one` + `dpumesh_rx_free` + memcpy +
`dpumesh_send_response`) = 92M samples = 전체의 <10%**. 즉 **echo 자체는
한계가 아님**.

나머지 echo CPU의 출처:
- `pthread_mutex_lock` 464M — `rx_queue` dequeue mutex
- `futex_wait_queue_me` 281M — worker가 `rx_cond` 대기
- `__schedule` 223M — context switch
- libdoca polling 14.6B — DOCA 내부

→ Echo bottleneck = **rx_queue 메커니즘 + scheduler overhead**. echo
연산 로직이 아님. 닫힌 루프 (closed-loop bench)에서 bench 측 wake 비용과
대칭이므로 양쪽이 동일 cap에 동시 도달.

### 3.3 DPU
거의 idle. `drain_deferred_tx_acks` + `doca_pe_progress`가 균등. DPA EU는
한가. **chain은 cap이 아님**.

### 3.4 결론
현재 cap = **host 측 PE thread CPU + worker scheduler overhead**.
DPA/DPU/comch 모두 여유. 향후 cap을 올리려면 host 측 wake-batch /
event-loop architecture 재설계가 필요.

---

## 4. 남은 최적화 후보

| # | 항목 | 예상 효과 | 비용 |
|---|---|---|---|
| 1 | **Echo `rx_queue` cond → futex batch wake**: chunk당 N번 `cond_signal` → 1번 `futex_wake(N)` | PE syscall 25분의 1 | 1-2일 |
| 2 | Bench 워커 모델: closed-loop → 파이프라이닝 (worker 적게 + in-flight 다수) | Scheduler 부하 큰 폭 감소 | bench 재설계, 3-5일 |
| 3 | Multi-core (현재 hw mode = catastrophic) — thread-level explicit pinning (PE는 한 코어 전용, worker는 다른 코어) | 잠재적으로 chain cap 근처 | 복잡, bench까지 손대야 함 |
| 4 | Phase 5: DPU L7 resolve / LB | 기능 추가 (성능 효과 없음 — RTT 추가) | 별도 |

권장 다음 단계: **#1 (echo rx_queue 배치 wake)**. PE thread만 바뀌고
wire format 변화 없음. 다른 후보는 모두 큰 재설계.

---

## 5. Hard Rules 점검

| # | 원칙 | 현재 |
|---|---|---|
| 1 | Thrift service code diff 0 | ✓ |
| 2 | Legacy (`BENCH_SPLIT=0`) 회귀 0 | ✓ |
| 3 | Back-to-back 안정 (10k×N, 50k×N, 170k×N) | ✓ |
| 4 | Deploy 한 줄 | ✓ `BENCH_SPLIT=1 ./test-bench.sh deploy` |
| 5 | **DPU log < 10MB** 어떤 부하에서도 | ✓ 전 sweep 통과 후 **283 byte 유지** |
| 6 | hdr / body pool 독립 (TX/RX/forward ring 모두) | ✓ |
| 7 | req_id = (src_id, seq) globally unique | ✓ |
| 8 | Spin 없음, event-driven | ✓ (futex_wait, no spin) |
| 9 | DPA-side handle device-locality (ADD_PEER 시 DPU device로 resolve) | ✓ |

---

## 6. File index

| File | 역할 |
|---|---|
| `lib/cpp/src/thrift/transport/dpumesh_doca.c` | Host: unified builder + sharding + futex pending + cursor alloc + sweep |
| `lib/cpp/src/thrift/transport/doca/mesh.h` | Wire formats (`mesh_req_id`, `mesh_hdr_req`, `mesh_chunk_header`) |
| `lib/cpp/src/thrift/transport/doca/dpu_worker.c` | DPU: forward + reverse + TX_ACK, hot-path logs silenced |
| `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c` | DPA forward (CASE_DIRECT peer lookup), reverse (hdr dst override) |
| `lib/cpp/src/thrift/transport/doca/dpa.c` | `setup_pod_dma`, ADD_PEER, broadcast peer topology |
| `bench/flame_capture.sh` | flame 자동 캡쳐 (bench + echo + DPU 동시) |
| `bench/final_*_flame.svg` | v4 최종 측정 (150k load) |
| `test-bench.sh` | deploy + DPU log 안전망 (`-l 30`, truncate) |

---

## 7. Glossary

| Term | Meaning |
|---|---|
| `mesh_req_id` | `{src_id, seq}` 64bit |
| **hdr path** | src → DPU → dst (L7 proxy hook 자리) |
| **body path / CASE_DIRECT** | src host → dst host 직접 DMA (DPU NIC engine 매개) |
| **builder shard** | per-dst 8-way shard. worker가 `pthread_self() % 8`로 hash |
| **paired flush** | 같은 send의 hdr append와 chunk append가 single lock 안에서 atomic |
| **expected ring** | per-src queue, hdr batch arrival로 채워짐, chunk가 head 매칭 |
| **parked chunk** | chunk가 hdr보다 일찍 도착할 때 임시 보관 (zero-copy) |
| **futex pending** | `_Atomic state` + `_Atomic waiters`. PE는 waiters>0일 때만 wake |

---

*문서 끝 (v4, 2026-05-15)*
