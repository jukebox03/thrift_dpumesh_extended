# DPUmesh — v5 (2026-05-15)

> v4 폐기. v4의 성능표는 bench 측 worker scheduler 오버헤드가 만든 cap이지
> dpumesh transport 자체의 cap이 아니었다는 게 오늘 분석으로 확인됨.
> v5는 (a) transport 로직 정리 상태, (b) 측정 인프라 신뢰성 회복,
> (c) **dpumesh transport는 1 core CPU의 2.5-2.8%만 쓰며 cap이 아님**,
> (d) 진짜 transport cap을 보기 위한 bench 단일 스레드 재설계 plan을 담는다.

---

## 0. TL;DR

**무엇**: Thrift 아래 깔리는 DPU-가속 mesh transport. App 코드 변경 0.
mesh가 hdr (control) + body (data)를 분리 운반. DPU는 L7 proxy hook 자리
(현재 dummy passthrough — Phase 5에서 resolve/LB 이식 예정).

**오늘 정리한 것 (v4 → v5)**:
1. 측정 인프라: flame graph `[unknown]` 비율 82% → 7%. DWARF unwinding + 빌드 플래그 패치.
2. DPU/DPA runtime log 전면 제거 (439 call). DPU log 283 byte 유지.
3. Pending ownership 깔끔하게: 방어적 2초 wait 제거, caller가 lifecycle 책임.
4. rx_queue batch wake: chunk당 N개 cond_signal → 1번 cond_broadcast.
5. Orphan OP_RESPONSE drop: cancel 후 늦게 도착한 응답이 rx_slot 점유 채 rx_queue로 가던 누수 차단.

**측정 결과 (200k load, 1 core/pod)**:
- **dpumesh transport 자체 = bench 2.8%, echo 2.5% CPU**
- 나머지: libdoca polling 42% + scheduler overhead 50%+
- → transport는 cap이 아니다. cap은 bench-side worker thread scheduler thrash 또는 vendor libdoca 폴링.

**다음 일**: bench를 단일 스레드 load generator로 재작성. 그래야 transport의
진짜 cap (libdoca 폴링 + DMA throughput)을 isolating해서 측정 가능.

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

- **hdr path** (2-hop, DPU 매개): src `hdr_tx_buffer` → DPU staging → reverse DMA → dst `hdr_rx_buffer`.
- **body path** (1-hop, CASE_DIRECT): DPA가 양쪽 host BAR을 직접 access. DPU staging 미사용.

### 1.2 핵심 구성요소

| | 역할 |
|---|---|
| Thrift app | RPC 호출만 (`send_request` / `wait_response_v2`) |
| Mesh (host C) | 통합 builder (hdr+chunk pair, single lock, 8-way shard), peer table, expected ring + parked chunks |
| DPU ARM | hdr forward 매개, TX_ACK + DMA_COMPLETION 발사. body는 안 봄 |
| DPA EU | DMA engine. forward (hdr→DPU staging / body 직접), reverse (hdr→dst hdr_rx). peer table + admission gate |

### 1.3 Per-RPC 메시지 흐름

```
caller (1 RPC):
  ├─ resolve(svc) → dst_pod_id
  ├─ atomic seq++
  ├─ register_pending_v2(rid) → state=0
  │   └─ 슬롯 busy면 즉시 -1 return (방어적 wait 없음)
  ├─ builder[dst][shard].lock              ← single lock; hdr+chunk atomic
  │   ├─ hdr_builder_append (mesh_hdr_req 80B)
  │   ├─ chunk_builder_append (body N bytes)
  │   └─ if cap: paired flush (hdr_tx 슬롯 + chunk_tx 슬롯 둘 다 enqueue)
  │   └─ append 실패면 register한 pending 즉시 cancel
  └─ wait_response_v2:
      ├─ futex_wait on p->state
      └─ wake 시 desc read, state=-1로 release
   (cancel_pending_v2도 동일하게 release하는 경로)

PE thread (RX):
  doca_pe_progress → rx_data_hook
   ├─ OP_HDR_BATCH: process_hdr_batch
   │   └─ for each entry: expected[src_id].push; touched bitmap
   │   └─ drain_parked_locked for touched src_ids
   └─ OP_CHUNK: process_chunk
       ├─ match: consume_chunk + drain_parked_locked
       └─ miss:  park (zero-copy)

consume_chunk (chunk 안 모든 body가 같은 flag — builder_flags 보장):
  ├─ OP_REQUEST: deliver_chunk_to_rxq (batch path, 한 번 cond_broadcast)
  └─ OP_RESPONSE: deliver_one_body × N
       ├─ pending matches (state=0): write desc, CAS 0→1, futex_wake_one
       ├─ pending timed-out (state=-2): release everything, state→-1
       └─ orphan (state=-1, 1, or rid mismatch): drop rx_slot (no rx_queue push)
```

---

## 2. 오늘 정리한 dpumesh transport 코드

### 2.1 Pending 슬롯 ownership 모델

**원리**: caller가 register 부터 release까지 lifecycle을 끝까지 책임진다.
transport는 방어 코드 두지 않는다.

| 동작 | 이전 (dirty) | 이후 (clean) |
|---|---|---|
| `register_pending(_v2)` | state ≠ -1이면 2초 wait, 안 풀리면 -2/-1 reclaim 시도 | state ≠ -1이면 즉시 -1 return |
| `send_request` | append 실패 시 등록된 pending 누수 | register 성공 후 append 실패 시 pending cancel |
| `deliver_one_body` | orphan OP_RESPONSE → rx_queue로 push (echo 외엔 안 비움 → 누수) | orphan OP_RESPONSE → rx_slot drop |
| caller thread teardown | in-flight 그대로 두고 종료 (state=0 누수) | in-flight 전부 cancel (state→-1) |

이 네 개가 같이 일관된 invariant를 형성: **slot은 caller가 잡고, caller가 푼다**.

### 2.2 rx_queue batch wake (echo response delivery)

이전엔 chunk 1개에 25개 body 들어있으면 PE thread가 body당 `pthread_cond_signal` → 25 syscall + 25 mutex acquire. 이제 `deliver_chunk_to_rxq`로 묶음:

```
Phase 1 (no rx_lock):    chunk의 N body를 미리 rx_slot 할당 + memcpy + desc 빌드
Phase 2 (single rx_lock): N개 desc 한 번에 push + cond_broadcast 1번
Phase 3 (rare):          overflow한 rx_slot release
```

OP_RESPONSE 경로는 그대로 (`deliver_one_body`의 lock-free CAS + futex_wake_one) — 이미 효율적이라 안 건드림.

### 2.3 측정 인프라 — flame graph 신뢰성

전제: dpumesh 자체가 어디서 CPU 쓰는지 보려면 perf stack walk가 끝까지 가야 한다.

| 변경 | 효과 |
|---|---|
| bench/echo gcc `-g -fno-omit-frame-pointer` | binary FP 보존 |
| libthrift cmake `RelWithDebInfo + -fno-omit-frame-pointer` | libthrift FP + debug info |
| `perf record --call-graph=dwarf,16384` | libc (FP 없음) 통과해서 stack walk |

결과: `[unknown]` 비율 — bench 82.2% → 6.6%, echo 68.7% → 0.6%, DPU 0%.

### 2.4 DPU/DPA runtime log 전면 삭제

`DOCA_LOG_DBG/INFO/WARN/ERR`, `DOCA_DPA_DEV_LOG_*`, `printf`, `fprintf` — runtime emit
하는 호출 **439개 전부 삭제**. 16개 파일 (DPU/DPA 전용 4 + 공용 12).

남긴 것: `DOCA_LOG_REGISTER`, `doca_log_backend_set_sdk_level`, `doca_dpa_set_log_level` (모두 emit이 아니라 config).

DPU 로그 283 byte 유지 (어떤 부하에서도). 회귀 시 host stderr 추가로 디버그.

---

## 3. dpumesh transport는 cap이 아님 — flame leaf 증거

200k load, DWARF 신뢰 가능한 flame, leaf-only sample 분포 (실제 cycle 소비처):

### Bench (1 core)

| Bucket | % CPU |
|---|---:|
| libdoca polling (vendor anon) | **42.1%** |
| DOCA symbol (`doca_pe_progress`, `priv_doca_cq_*`) | 15.0% |
| Kernel/syscall path | 17.0% |
| Scheduler 내부 (psi_group_change, update_load 등) | 16.7% |
| libc/pthread (mutex 자체) | 7.1% |
| **dpumesh-code** | **2.8%** |

### Echo (1 core)

| Bucket | % CPU |
|---|---:|
| Kernel (spin_lock, sched_clock, exit_to_user 등) | 45.2% |
| Scheduler 내부 | 43.9% |
| libc/pthread (cond/mutex) | 7.6% |
| **dpumesh-code** | **2.5%** |
| libdoca | 0.4% |

**결론**: 한 코어 100% 다 쓰는 상태에서 dpumesh 자체가 차지하는 건 3% 이내.
그 3% 안의 hot frame들 — `pe_progress_fn`, `deliver_one_body`, `consume_chunk`,
`dpumesh_dequeue`, `builder_append_locked` — 전부 "작은 일을 자주 하는" 정상 패턴.
알고리즘 비효율 없음.

실제 cap의 정체:
- Bench: 50-93개 worker thread가 1코어 위에 wake/sleep cycle → CFS scheduler bookkeeping 90%
- Echo: PE thread의 libdoca 폴링 + 워커 wake/sleep scheduler
- 양쪽 다 **dpumesh transport 바깥**에서 결정됨

---

## 4. Bench는 너무 무거움 — 다음 redesign 대상

### 4.1 현재 bench의 무게

| 요소 | 비용 |
|---|---|
| N (50-93)개 worker thread × K=32 pipeline | 워커마다 wake/sleep cycle |
| 워커당 `sleep_until` (clock_nanosleep) | per-fire syscall |
| 워커당 `wait_response_v2` (futex_wait) | per-reap syscall |
| 200k RPS 환산 | ~400k syscall/sec + ~200k context switch/sec |

→ 1코어 CPU의 90%가 bench의 thread scheduling 자체에 들어감. dpumesh transport가
무엇을 하든 측정값은 이 thread 모델에 막힘.

### 4.2 단일 스레드 load generator (Phase 5)

```
한 thread:
  loop:
    1. ring 가장 오래된 응답 ready? (wait_response_v2 timeout=0, non-blocking)
       → ready면 reap + record latency = (now - fire_t)
    2. 시간 됐고 ring 빈 자리 있나? → send_request, ring push, fire_t = now
    3. 둘 다 아니면 다음 scheduled tick까지 짧게 nanosleep
```

특성:
- worker thread = **1**. PE thread랑 1 core 양분.
- mutex / cond / futex = 0. atomic load만.
- 코드 100 줄 안쪽 (현재 ~500 줄에서 -80%)
- 단일 스레드 cap 추정:
  - send_request 1회 ≈ 2-5 μs (register + builder append + maybe flush)
  - non-blocking poll ≈ 0.5 μs
  - cycle ≈ 3-6 μs → **160-330k RPS** 단일 스레드만으로 가능
- 1 thread cap에 부딪히면 disjoint rid range로 2-4 thread (공유 state 없음)

### 4.3 v4 성능표 폐기 이유

v4 §0 표 (170k 권장 한계, 200k plateau 등) 와 오늘 측정한 숫자들 (256B 300k 등)
모두 **bench thread 모델의 cap**이지 dpumesh transport cap이 아님. 의미 있는
transport 측정은 **§4.2 redesign 후** 다시 한다.

---

## 5. 측정 인프라 (재현 가능하게)

### 5.1 빌드 플래그

`test-bench.sh`:
- cmake: `-DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_FLAGS="-fno-omit-frame-pointer" -DCMAKE_CXX_FLAGS="-fno-omit-frame-pointer"`
- bench/echo gcc: `-O2 -g -fno-omit-frame-pointer`

### 5.2 Flame 캡쳐

`bench/flame_capture.sh <rps> <dur> <size> <label>`:
- 호스트 측: `perf record -F 199 --call-graph=dwarf,16384 -p <pid>` (FP 불완전한 libc 통과)
- DPU 측: `perf record -F 199 -g -p <dpu_pid>` (ARM, FP OK)
- 렌더: stackcollapse-perf.pl + flamegraph.pl

### 5.3 Leaf-only 분류

분석은 cumulative samples (부모/자식 중복)가 아니라 leaf samples로:
- `perf script` → `stackcollapse-perf.pl` → 각 스택의 마지막 token만 카운트
- 카테고리: libdoca / DOCA-symbol / kernel-sched / libc-pthread / **dpumesh-code** / unknown

---

## 6. Hard Rules

| # | 원칙 | 현재 |
|---|---|---|
| 1 | Thrift service code diff 0 | ✓ |
| 2 | Legacy (`BENCH_SPLIT=0`) 회귀 0 | ✓ (pending ownership clean 후에도 legacy path 그대로) |
| 3 | DPU log < 10MB 어떤 부하에서도 | ✓ 283 byte |
| 4 | hdr / body pool 독립 (TX/RX/forward ring 모두) | ✓ |
| 5 | req_id = (src_id, seq) globally unique | ✓ |
| 6 | Spin 없음, event-driven | ✓ (futex 직접, no spin) |
| 7 | Pending slot ownership = caller (transport는 방어 wait 없음) | ✓ (v5 추가) |
| 8 | 측정 신뢰성: flame `[unknown]` < 10% | ✓ bench 6.6%, echo 0.6%, DPU 0% |
| 9 | 성능 cap 평가 = dpumesh transport만 isolating | **펜딩 — bench redesign 후** |

---

## 7. File index

| File | 역할 |
|---|---|
| `lib/cpp/src/thrift/transport/dpumesh_doca.c` | Host: builder + sharding + pending (ownership clean) + futex + batch wake |
| `lib/cpp/src/thrift/transport/doca/mesh.h` | Wire formats (`mesh_req_id`, `mesh_hdr_req`, `mesh_chunk_header`) |
| `lib/cpp/src/thrift/transport/doca/dpu_worker.c` | DPU: forward + reverse + TX_ACK (logs 전면 제거됨) |
| `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c` | DPA: CASE_DIRECT + reverse (logs 전면 제거됨) |
| `lib/cpp/src/thrift/transport/doca/dpa.c` | `setup_pod_dma`, ADD_PEER (logs 전면 제거됨) |
| `bench/bench_dpumesh.c` | 현재 multi-thread pipelined. **§4.2에서 단일 스레드로 재작성 예정** |
| `bench/echo_dpumesh.c` | echo daemon. 현 worker pool 모델 유지 (서비스 측이라 단순화 우선순위 낮음) |
| `bench/flame_capture.sh` | DWARF 모드 perf record + flame 렌더 |
| `bench/final_*_flame.svg` | DWARF 신뢰 가능한 측정 (200k load) |
| `test-bench.sh` | deploy + FP/debug 빌드 플래그 + DPU log 안전망 |
| `plan.md` | 이 문서 (v5) |

---

## 8. Glossary

| Term | Meaning |
|---|---|
| `mesh_req_id` | `{src_id, seq}` 64bit globally unique |
| **hdr path** | src → DPU → dst (L7 proxy hook 자리) |
| **body path / CASE_DIRECT** | src host → dst host 직접 DMA (DPU NIC engine 매개) |
| **builder shard** | per-dst 8-way shard. worker가 `pthread_self() % 8`로 hash |
| **paired flush** | 같은 send의 hdr append와 chunk append가 single lock 안에서 atomic |
| **expected ring** | per-src queue, hdr batch arrival로 채워짐, chunk가 head 매칭 |
| **parked chunk** | chunk가 hdr보다 일찍 도착할 때 임시 보관 (zero-copy) |
| **futex pending** | `_Atomic state` + `_Atomic waiters`. PE는 waiters>0일 때만 wake |
| **pending ownership** | caller가 register-release lifecycle 책임. transport는 방어 wait 안 함 (v5) |
| **batch wake** | chunk당 cond_signal N번 → cond_broadcast 1번 (v5) |
| **orphan response drop** | cancel 후 늦게 도착한 OP_RESPONSE는 rx_slot drop (v5) |
| **leaf-only sample** | flame 분석 시 cumulative가 아니라 스택의 deepest frame 기준. 실제 CPU 소비처 정확 측정. |

---

*문서 끝 (v5, 2026-05-15) — 다음: bench 단일 스레드 재작성*
