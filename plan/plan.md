# DPUmesh Thrift Transport — Plan

## 실행 아키텍처: CPU / DPU / DPA에서 무엇이 도는가

### 전체 구조

```
┌───────────────────────────────────────────────────────────────┐
│  Host (x86_64 CPU)                                            │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ Pod 0 (UniqueIdService 등)                            │     │
│  │   TDpumeshServerTransport / TDpumeshClientTransport   │     │
│  │     └─ dpumesh_doca.c (libthrift.so에 포함)           │     │
│  │          ├─ TX: DMA ring에 desc 쓰기 (valid=1)        │     │
│  │          ├─ RX: condvar 대기 (comch ctrl path 콜백)   │     │
│  │          ├─ PE progress thread (폴링)                 │     │
│  │          └─ Client API: req_id 매칭, condvar 응답 대기│     │
│  └──────────┬─────────────────────────┬──────────────────┘     │
│  ┌──────────┴────────┐  ┌─────────────┴──────────────────┐     │
│  │ Pod 1 (다른 서비스) │  │ Pod N ...                      │     │
│  └───────────────────┘  └────────────────────────────────┘     │
│              │ comch ctrl path       │ DMA ring                 │
│              │ (RX: DPU→Host)        │ (TX: Host→DPU)           │
│              ▼                       ▼                          │
│         PCI 94:00.0 (ConnectX-7/BF3, Host PF representor)      │
└───────────────────────────────────────────────────────────────┘
                       │ PCIe
┌───────────────────────────────────────────────────────────────┐
│  DPU (BlueField-3, ARM aarch64)                                │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ dpumesh_dpu (standalone binary)                       │     │
│  │   dpu_main.c → run_dpu_worker()                       │     │
│  │     ├─ comch server 시작 (multi-pod: 복수 Host 연결)  │     │
│  │     ├─ pod 등록: DMESH_MSG_REGISTER → pods[] 테이블   │     │
│  │     ├─ per-pod mmap 수신 → setup_pod_dma() 자동 호출  │     │
│  │     ├─ consumer datapath 초기화 (DPA → DPU)           │     │
│  │     ├─ DPA 커널/스레드 초기화 (공유, 1개)             │     │
│  │     ├─ TCP Gateway (포트 9091, 외부 클라이언트 수신)  │     │
│  │     └─ 메인 루프: consumer_pe + ctrl_pe 폴링          │     │
│  └──────────┬──────────────────────────────────────────┘     │
│              │ DPA thread 관리 + trigger msg                   │
│              ▼                                               │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ DPA (Data Path Accelerator, HW thread)                │     │
│  │   dpa_kernel.c → run_dma_manager()                    │     │
│  │     ├─ handle_msgs(): DPU trigger 메시지 처리         │     │
│  │     └─ poll_desc_rings(): multi-ring round-robin 폴링 │     │
│  │          ├─ per-pod ring의 dma_desc.valid 감시        │     │
│  │          ├─ DMA copy 실행 (Host→DPU 메모리)           │     │
│  │          └─ completion msg → DPU consumer로 전송       │     │
│  └──────────────────────────────────────────────────────┘     │
│                                                               │
│  PCI 03:00.0 (ConnectX-7, DPU측)                              │
└───────────────────────────────────────────────────────────────┘
```

### CPU (Host, x86_64)에서 도는 코드

| 프로세스 | 파일 | 역할 |
|----------|------|------|
| App | `TDpumeshServerTransport.cpp` | Thrift 서버: accept → read → process → write |
| (same) | `TDpumeshTransport.cpp` | 개별 연결 래퍼: read/write/flush |
| (same) | `TDpumeshClientTransport.cpp` | Thrift 클라이언트: req_id 매칭, condvar 응답 대기 |
| (same) | `dpumesh_doca.c` | DOCA 백엔드: TX(DMA ring), RX(comch condvar), PE thread, Client API |
| (same) | `doca/comch_client.c` | comch control path 클라이언트 (DPU 서버에 연결) |
| (same) | `doca/comch_producer.c` | comch datapath producer (DPA msgq로 DMA 요청 전달) |
| (same) | `doca/comch_consumer.c` | comch datapath consumer (DMA completion 수신) |
| (same) | `doca/dpa.c` | DPA 객체 초기화 (Host측, thread/app/buf_arr 생성) |
| (same) | `doca/dma.c` | DMA ring 설정, DMA 요청 메시지 구성 |
| PE thread | `dpumesh_doca.c:pe_progress_fn()` | `doca_pe_progress()` 1µs 폴링, comch 콜백 구동 |

빌드: `cmake -DWITH_DOCA=ON && make` → `libthrift.so`에 포함됨

### DPU (BlueField-3, ARM aarch64)에서 도는 코드

| 프로세스 | 파일 | 역할 |
|----------|------|------|
| dpumesh_dpu | `doca/dpu_main.c` | 진입점: 로깅, argp, 디바이스 open, rep open |
| (same) | `doca/dpu_worker.c` | 워커: comch server → consumer → DPA init → TCP Gateway → PE 폴링 루프 |
| (same) | `doca/config.c` | `-p`/`-r` PCI 주소 파싱 (doca_argp) |
| (same) | `doca/comch_server.c` | comch control path 서버 + multi-pod 관리 (pods[], register, routing) |
| (same) | `doca/comch_consumer.c` | DPA → DPU: DMA completion 메시지 수신 |
| (same) | `doca/comch_producer.c` | DPU → Host: completion/RX 데이터 전송 |
| (same) | `doca/dpa.c` | DPA 커널/스레드 생성, per-pod DMA 설정 (`setup_pod_dma`), trigger msg 전송 |
| (same) | `doca/comch_msgq.c` | DPU ↔ DPA message queue 관리 |

빌드: `cd lib/cpp/src/thrift/transport/doca && meson setup build && ninja -C build`
- **중요**: DPA 커널(`dpa_kernel.c`) 변경 시 반드시 `rm -rf build && meson setup build` (DPACC는 `meson setup` 시에만 실행됨, `ninja`로는 재빌드 안 됨)

실행: `sudo ./build/dpumesh_dpu -p 03:00.0 -r 94:00.0`

### DPA (Data Path Accelerator, HW thread)에서 도는 코드

| 함수 | 파일 | 역할 |
|------|------|------|
| `run_dma_manager()` | `doca/device/dpa_kernel.c` | 진입점: trigger msg 처리 → multi-ring 폴링 루프 (무한) |
| `thread_init_rpc()` | `doca/device/dpa_kernel.c` | DPA 스레드 초기화 RPC (consumer ack) |
| `poll_desc_rings()` | `doca/device/dpa_kernel.c` | 모든 pod의 ring을 round-robin 폴링, valid desc 발견 시 DMA copy + completion msg |
| `handle_msgs()` | `doca/device/dpa_kernel.c` | DPU→DPA comch 메시지 처리 (consumer completion에서 읽기) |
| `handle_dpu_msg()` | `doca/device/dpa_kernel.c` | 개별 메시지 타입별 처리 (DMA_REQ 등) |

**DPA 스레드 활성화 모델** (DOCA 핵심 개념):
- `doca_dpa_thread_run()`은 스레드를 "runnable" 상태로만 설정 → 실제 실행되지 않음
- 스레드에 부착된 completion context에 이벤트가 도착해야 실행됨
- DPU→DPA msgq를 통해 trigger 메시지를 보내면 consumer completion → 스레드 활성화
- `doca_dpa_dev_thread_reschedule()`은 스레드를 yield하고 다음 이벤트 대기 → 폴링 루프에서는 사용 금지

### 데이터 흐름

```
[TX: Host→DPU]  (DMA ring 기반, per-pod)
  App flush() → dpumesh_enqueue() → DMA ring에 dma_desc 쓰기 (valid=1, dst_pod_id 포함)
  → DPA poll_desc_rings()이 round-robin 감지
  → doca_dpa_dev_comch_producer_dma_copy() 실행 (Host mem → DPU local mem)
  → DMA 완료 → comch_dma_comp_msg (src_pod_id, dst_pod_id, req_id 포함) → DPU consumer 콜백

[RX: DPU→Host]  (comch control path, 임시)
  DPU에서 server_send_rx_data() / server_send_rx_data_to()
  → comch control path msg 전송 (multi-pod: connection별 라우팅)
  → Host comch client 콜백 → rx_data_hook() → RX buffer에 body 복사
  → descriptor queue push → condvar signal
  → App dpumesh_dequeue()에서 깨어남 → dpumesh_rx_buf()로 데이터 읽기

[Client API: 요청/응답 매칭]
  dpumesh_alloc_req_id() → dpumesh_register_pending(req_id)
  → dpumesh_enqueue(desc with OP_REQUEST) → TX 경로
  → DPU 처리 → RX 경로로 OP_RESPONSE 도착
  → poller thread가 req_id로 pending entry 찾기 → condvar signal
  → dpumesh_wait_response()에서 깨어남

[External TCP: 클라이언트 수정 없는 접근]
  TCP Client → DPU 9091 → tcp_listener_thread()
  → server_send_rx_data() → Host (comch ctrl path)
  → Host 처리 → DMA ring → DPA → DPU → TCP 응답
```
### Multi-Pod 아키텍처

```
Host Pod 0 ──┐                          ┌── DPA ring[0] (Pod 0)
Host Pod 1 ──┼── comch ctrl path ── DPU ┼── DPA ring[1] (Pod 1)
Host Pod N ──┘   (per-connection)        └── DPA ring[N] (Pod N)
```

- 각 Host Pod는 독립적으로 `dpumesh_init()` → comch 연결 → `DMESH_MSG_REGISTER` 전송
- DPU는 `pods[MAX_PODS]` 테이블로 연결별 상태 관리 (`pod_state` 구조체)
- per-pod: `ring_mmap`, `remote_mmap`, `buf_arr`, `local_mmap` (DPU DMA 작업 버퍼)
- DPA 스레드는 1개 (공유): `dpa_thread_arg.rings[]` 배열로 모든 pod의 ring 관리
- 첫 번째 pod 등록 시 DPA 스레 시작 + trigger 메시지 전송
- 이후 pod 등록 시: d2h_memcpy → ring 추가 → h2d_memcpy (DPA 스레드는 계속 실행)

### [알려진 문제점 & 해결책]

1. **응답 라우팅 오류 (Echo Routing)**:
   - **문제**: DPU에서 `server_send_rx_data()`가 첫 번째 연결(`objs->connection`)만 사용하여 모든 응답을 전송함. 이로 인해 여러 프로세스 실행 시 다른 프로세스가 응답을 가로채는 현상 발생.
   - **해결**: `src_pod_id`를 기반으로 `find_pod_by_id()`를 호출하여 해당 포드의 `connection`으로 `server_send_rx_data_to()`를 사용하도록 수정 필요.

2. **Pod ID 충돌**:
   - **문제**: `dpumesh_init()` 시 `worker_id`가 0으로 고정되면 모든 프로세스가 `pod_id=0`으로 등록됨. DPU는 `pod_id`로 포드를 구분하므로 라우팅이 불가능해짐.
   - **해결**: 각 프로세스 실행 시 `DPUMESH_POD_ID` 환경변수를 서로 다르게 설정해야 함. (예: `gateway`=1, `test_comch`=2)

---

## 1. 클라이언트 Transport 구현 — 🔄 진행 중

| 구조체 | 파일 | 역할 |
|--------|------|------|
| `struct objects` | `object.h` | 중앙 상태: dev, pe, comch, DPA, pods[], gateway |
| `struct pod_state` | `object.h` | per-pod: connection, mmap, buf_arr, dma_buffer |
| `struct dpa_thread_arg` | `dpa_common.h` | DPA 스레드 인자: comch handles + rings[MAX_DPA_RINGS] |
| `struct dpa_ring_info` | `dpa_common.h` | per-ring: buf_arr, host/dpu mmap, pod_id |
| `struct dma_desc` | `dpa_common.h` | DMA ring descriptor (64B packed): addr, size, idx(req_id), dst_pod_id, valid |
| `sw_descriptor_t` | `dpumesh.h` | 소프트웨어 descriptor (64B): req_id, pod_id, flags, slot 정보 |
| `struct tcp_gateway` | `dpu_worker.h` | TCP Gateway: listen_fd, conns[], next_req_id |
| `struct comch_dma_comp_msg` | `dpa_common.h` | DMA completion: pos, length, req_id, src/dst_pod_id |

---

## 1. 클라이언트 Transport 구현 — 🔄 진행 중

### 응답 매칭: condvar 방식 (구현됨)

- `req_id` 필드로 요청/응답 매칭 (HTTP/2의 stream_id와 같은 역할)
- `OP_REQUEST`/`OP_RESPONSE` flags로 요청/응답 구분

```
poller thread:  rx_sq에서 desc 꺼냄 (64B) → flags 확인
                  OP_REQUEST  → 기존 서버 처리 경로 (notify pipe)
                  OP_RESPONSE → pending_[req_id].desc에 복사 (64B) → cv.notify_one()

client thread:  요청 전송 → cv.wait() → 깨어남 → desc.body_buf_slot으로 SHM 직접 읽기 (zero-copy)
```

### 구현 상태

- ✅ `dpumesh.h`에 Client API 정의: `dpumesh_alloc_req_id()`, `dpumesh_register_pending()`, `dpumesh_wait_response()`, `dpumesh_cancel_pending()`
- ✅ `dpumesh_doca.c`에 condvar 기반 응답 매칭 구현
- ✅ `TDpumeshClientTransport.h/cpp` 작성됨
- ❌ E2E 테스트 (실제 서비스 간 호출 체인)


## 2. TNonblockingServer 호환 — 보류

### 결정: C. TNonblockingServer 지원 안 함

- dpumesh에서는 데이터가 한 번에 도착 → NonBlocking의 "조금씩 읽기" 패턴 불필요
- SHM slot 수(64개)가 동시 연결 상한 → 수천 연결 관리의 이점 없음
- TThreadedServer 사용으로 충분


---

## 3. DOCA 백엔드 통합

### 환경

- Host: Linux 5.15, x86_64, BF3 PCI `94:00.0`, DOCA SDK 3.1.0
- DPU: Ubuntu 22.04, aarch64, PCI `03:00.0`, DOCA SDK 3.1.0105
- DPU 접속: `ssh jukebox@192.168.100.2` (키 인증)
- DPU에 외부 인터넷 없음 (DNS resolve 불가, 패키지 설치 시 Host 경유 필요)
- DPU에 cmake 없음, meson 0.61.2 + ninja 1.10.1 사용
- 시간 동기화 안 됨 (clock skew 주의, `find . -exec touch {} +`로 우회)

### 완료된 작업

#### Part 1: DOCA 인프라 코드 통합 — ✅ 완료

- `DPUMesh_doca/DPUMesh/` → `lib/cpp/src/thrift/transport/doca/` 복사 + 수정
- `dpa_common.h` 수정: `comch_dma_comp_msg`에 `req_id`, `src_pod_id`, `dst_pod_id` 필드 추가

#### Part 2: 통합 공개 API (`dpumesh.h`) — ✅ 완료

- `dpumesh_doca.h`, `dpumesh_shm.h` → `dpumesh.h`로 통합
- 애플리케이션 코드는 `dpumesh.h`만 include (SHM/DOCA 모름)
- Client API 추가: `dpumesh_alloc_req_id()`, `dpumesh_register_pending()`, `dpumesh_wait_response()`, `dpumesh_cancel_pending()`

#### Part 3: dpumesh_doca.c 구현 (TX + RX + Client API) — ✅ 완료

- TX: DMA ring 기반 (Host→DPU), per-pod ring
- RX: comch control path 임시 구현 (DPU→Host), condvar 기반
- Client API: req_id 매칭, condvar 응답 대기

#### Part 4: comch RX 메시지 프로토콜 — ✅ 완료

- `DMESH_MSG_RX_DATA` 메시지 타입, `server_send_rx_data()`, `rx_data_hook()`
- `DMESH_MSG_REGISTER` 메시지 타입, multi-pod 등록 프로토콜
- `DMESH_MSG_EXPORT_DESC`, `DMESH_MSG_EXPORT_DPA_COMP`: mmap/DPA 핸들 교환

#### Part 5: DPU 바이너리 — ✅ 완료

- `dpu_main.c`, `dpu_worker.c`, `config.c` 작성
- meson.build 작성 (DPU cmake 없어서 meson 사용)
- `build_dpacc.sh`: DPACC 빌드 스크립트 (meson setup 시 `run_command()`로 실행)
- **DPU에서 빌드 및 실행 성공**

#### Part 6: Multi-Pod 아키텍처 — ✅ 완료

- `comch_server.c`: pod 관리 (`pods_add_connection`, `pods_register`, `find_pod_by_id`, `find_pod_by_connection`)
- `object.h`: `struct pod_state` (per-pod: connection, mmap, buf_arr, dma_buffer)
- `dpa.c`: `setup_pod_dma()` — per-pod DMA 설정 (mmap 도착 시 자동 호출)
- `dpa_common.h`: `struct dpa_thread_arg` multi-ring 지원 (`rings[MAX_DPA_RINGS]`)
- `dpa_kernel.c`: `poll_desc_rings()` multi-ring round-robin 폴링

#### Part 7: DPA 스레드 활성화 수정 — ✅ 완료 (2025-03-25)

**문제**: 단일 pod → multi-pod 전환 과정에서 DPA 스레드가 실행되지 않는 버그

**근본 원인**:
1. `doca_dpa_thread_run()`은 스레드를 "runnable"로만 설정 — 실행하려면 attached completion context에 이벤트 필요
2. 원래 트리거였던 `send_dma_request_to_dpa()`가 multi-pod 리팩토링에서 제거됨
3. `dpa_kernel.c`에서 `doca_dpa_dev_thread_reschedule()`이 스레드를 yield → 폴링 루프에서는 사용 불가

**수정**:
1. `dpa.c:setup_pod_dma()`: 첫 pod에서 `doca_dpa_thread_run()` 후 `dmesh_doca_dpa_msgq_send()`로 trigger 메시지 전송
2. `dpa_kernel.c:run_dma_manager()`: `reschedule()` 제거, `handle_msgs()` → `poll_desc_rings()` 직행 (무한 루프)

#### Part 8: CMakeLists.txt (Host) — ✅ 완료

- `WITH_DOCA` 옵션, 조건부 소스, DOCA pkg-config 링크
- Host 빌드 성공 확인

#### Part 9: TDpumeshClientTransport — ✅ 완료 (코드 작성)

- `TDpumeshClientTransport.h/cpp` 별도 클래스
- open(), write(), flush(), read(), close(), isOpen() 구현

#### Part 10: 배포 인프라 — ✅ 완료 (코드 작성)

- `Dockerfile.uniqueid`: `WITH_DOCA` arg
- `deploy-dpumesh.sh`: `--doca` 플래그
- `UniqueIdService/CMakeLists.txt`: thrift 라이브러리만 링크

#### DPU 환경 확인 — ✅ 완료

- DPU SSH 접속: `ssh jukebox@192.168.100.2` (키 인증)
- DOCA SDK 3.1.0105 설치됨
- DPACC (`/opt/mellanox/doca/tools/dpacc`) 존재
- PCI: `03:00.0` (ConnectX-7), Host PF representor = `pf0hpf` (PCI `94:00.0`)
- 빌드 도구: gcc 11.4.0, meson 0.61.2, ninja 1.10.1 (cmake 없음)
- 시간 동기화 안 됨 (clock skew 주의)

#### E2E 테스트 (test_comch) — ✅ 전체 통과 (2025-03-25)

- TEST 1: `dpumesh_init()` — comch 연결 + pod 등록 + mmap 교환 + DPA 시작
- TEST 2: Server path — enqueue → dequeue (OP_REQUEST echo)
- TEST 3: Client API — `register_pending` → `enqueue` → `wait_response`
- TEST 4: Multi-thread — 4 concurrent requests, 전부 성공
- **Results: 4 passed, 0 failed**

### 확인 필요 사항

1. **rep PCI 주소**: ✅ 해결
   - **올바른 값**: `-r 94:00.0` (Host PF representor `pf0hpf`에 바인딩)
   - `-r 03:00.0`으로 하면 firmware 거부 (syndrome=0xe5300)
2. **dpa_program.a**: ✅ DPACC로 DPU에서 재빌드 성공 (meson setup 시 자동)
3. **K8s PCI passthrough**: ❌ 미확인 (DOCA 디바이스를 컨테이너에서 접근하는 방식)

---

## 4. DPU TCP Gateway (Interception) 전략 — ✅ 코드 작성 완료

클라이언트 수정 없이 DPU가 TCP 요청을 가로채서 DPUmesh로 변환하는 전략입니다.

### 구현 상태

- ✅ `dpu_worker.h`: `struct tcp_gateway`, `struct tcp_conn` 정의
- ✅ `dpu_worker.c`: `gateway_init()`, `gateway_destroy()`, `gateway_add_conn()`, `gateway_find_conn()`, `gateway_remove_conn()`
- ✅ `dpu_worker.c`: `tcp_listener_thread()` — TCP 9091 수신 → Thrift framed transport 파싱 → `server_send_rx_data()` 호출
- ✅ `dpu_worker.c`: `start_tcp_gateway()` — socket/bind/listen + pthread 시작
- ❌ 응답 경로(TX) 연동: DPA DMA 완료 → DPU 워커 → TCP 응답 전송 (미완료)

### 데이터 흐름 (Interception 모드)

```
[Request]
  TCP Client → DPU:9091 → tcp_listener_thread()
  → tcp_recv_all() (Thrift framed: 4B length + body)
  → sw_descriptor_t 구성 (flags=OP_REQUEST|CASE_EXTERNAL, src_pod_id=-1)
  → server_send_rx_data() → comch ctrl path → Host
  → Host rx_data_hook() → dpumesh_dequeue() → Thrift 서비스 처리

[Response] (미완성)
  Host flush() → DMA ring → DPA → DMA copy → DPU
  → DMA completion msg (req_id, dst_pod_id=0)
  → gateway_find_conn(req_id) → TCP write → TCP Client
```

---

## 5. 남은 작업

### 우선순위 높음
- ❌ TCP Gateway 응답 경로 완성 (DMA completion → TCP write)
- ❌ E2E 서비스 간 호출 테스트 (TDpumeshClientTransport 사용)
- ❌ 빌드 경고 정리 (unused variables, DOCA_ARCH_DPU redefinition)

### 우선순위 중간
- ❌ UniqueIdService DOCA 빌드 → Thrift RPC 호출
- ❌ deploy-dpumesh.sh DOCA 모드 완성
- ❌ K8s PCI passthrough 설정

### 향후
- ❌ RX 경로 DMA 전환 (현재 comch ctrl path → DMA 기반으로 전환)
- ❌ DPA trigger 메시지 타입 전용화 (현재 `COMCH_MSG_TYPE_DMA_COMPLETED` 재사용 → `COMCH_MSG_TYPE_TRIGGER` 추가 고려)
- ❌ DPU host disconnect 시 consumer error 처리 개선

---

## 파일 구조

```
thrift_dpumesh_extended/lib/cpp/src/thrift/transport/
├── dpumesh.h                        ← 통합 공개 API (SHM/DOCA 공통, Client API 포함)
├── dpumesh_doca.c                   ← DOCA 백엔드 (Host, TX+RX+Client API)    [CPU]
├── dpumesh_shm.c                    ← SHM 백엔드 (시뮬레이션)                  [CPU]
├── TDpumeshTransport.cpp/h          ← Thrift C++ 래퍼 (서버 연결)             [CPU]
├── TDpumeshServerTransport.cpp/h    ← Thrift 서버 래퍼                        [CPU]
├── TDpumeshClientTransport.cpp/h    ← Thrift 클라이언트 래퍼                   [CPU]
└── doca/
    ├── meson.build                  ← DPU 바이너리 빌드 (meson + DPACC)
    ├── build_dpacc.sh               ← DPACC 빌드 스크립트
    ├── dpu_main.c                   ← DPU 바이너리 진입점                      [DPU]
    ├── dpu_worker.c/h               ← DPU 워커 + TCP Gateway                  [DPU]
    ├── config.c/h                   ← argp 파싱 (-p, -r PCI 주소)             [DPU]
    ├── common.c/h                   ← DOCA 디바이스 유틸                       [CPU+DPU]
    ├── object.c/h                   ← struct objects + pod_state (중앙 상태)   [CPU+DPU]
    ├── buffer.c/h                   ← mmap + 버퍼 관리                        [CPU+DPU]
    ├── ring.c/h                     ← DMA ring                               [CPU+DPU]
    ├── comch_common.c/h             ← 메시지 타입 정의 + mmap 교환 프로토콜    [CPU+DPU]
    ├── comch_client.c/h             ← comch 클라이언트                        [CPU]
    ├── comch_server.c/h             ← comch 서버 + multi-pod 관리             [DPU]
    ├── comch_consumer.c/h           ← comch datapath consumer                [CPU+DPU]
    ├── comch_producer.c/h           ← comch datapath producer                [CPU+DPU]
    ├── comch_msgq.c/h               ← DPU ↔ DPA message queue               [CPU+DPU]
    ├── dma.c/h                      ← DMA 유틸                               [CPU+DPU]
    ├── dpa.c/h                      ← DPA 초기화 + setup_pod_dma()           [CPU+DPU]
    ├── dpa_common.h                 ← DPA 공유 구조체 (multi-ring)            [CPU+DPU+DPA]
    └── device/
        ├── dpa_kernel.c             ← DPA 커널 (poll_desc_rings, handle_msgs)[DPA]
        └── dpa_program.a            ← DPA 커널 아카이브 (DPACC로 빌드)
```

### 빌드 방법

```bash
# Host (x86_64)
cd thrift_dpumesh_extended
mkdir build-doca && cd build-doca
cmake .. -DWITH_DOCA=ON
make -j$(nproc)

# DPU (aarch64, ssh jukebox@192.168.100.2)
cd ~/thrift_dpumesh_extended/lib/cpp/src/thrift/transport/doca
rm -rf build                    # DPA 커널 변경 시 반드시 clean build
meson setup build
ninja -C build

# 실행
sudo ./build/dpumesh_dpu -p 03:00.0 -r 94:00.0

# 테스트 (Host에서)
sudo ./test_comch
```
