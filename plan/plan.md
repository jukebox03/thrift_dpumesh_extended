# DPUmesh Thrift Transport — Plan

## 실행 아키텍처: CPU / DPU / DPA에서 무엇이 도는가

### 전체 구조

```
┌─────────────────────────────────────────────────────────┐
│  Host (x86_64 CPU)                                      │
│                                                         │
│  ┌─────────────────────────────────────────────┐        │
│  │ UniqueIdService (Thrift application)         │        │
│  │   TDpumeshServerTransport                    │        │
│  │     └─ dpumesh_doca.c (libthrift.so에 포함)  │        │
│  │          ├─ TX: DMA ring에 desc 쓰기         │        │
│  │          ├─ RX: condvar 대기 (comch 콜백)    │        │
│  │          └─ PE progress thread (폴링)        │        │
│  └─────────────┬───────────────────────┬────────┘        │
│                │ comch ctrl path       │ DMA              │
│                │ (RX: DPU→Host)        │ (TX: Host→DPU)   │
│                ▼                       ▼                  │
│           PCI 94:00.0 (ConnectX-7/BF3)                   │
└─────────────────────────────────────────────────────────┘
                         │ PCIe
┌─────────────────────────────────────────────────────────┐
│  DPU (BlueField-3, ARM aarch64)                         │
│                                                         │
│  ┌─────────────────────────────────────────────┐        │
│  │ dpumesh_dpu (standalone binary)              │        │
│  │   dpu_main.c → run_dpu_worker()              │        │
│  │     ├─ comch server 시작 (Host client 대기)  │        │
│  │     ├─ consumer datapath 초기화              │        │
│  │     ├─ DPA 커널/스레드 초기화                │        │
│  │     ├─ Host mmap export 수신 대기            │        │
│  │     └─ 메인 루프: doca_pe_progress() 폴링    │        │
│  └─────────────┬────────────────────────────────┘        │
│                │ DPA thread 관리                          │
│                ▼                                         │
│  ┌─────────────────────────────────────────────┐        │
│  │ DPA (Data Path Accelerator, HW thread)       │        │
│  │   dpa_kernel.c → run_dma_manager()           │        │
│  │     ├─ Host DMA ring 폴링 (desc.valid 감시)  │        │
│  │     ├─ DMA copy 실행 (Host→DPU 메모리)       │        │
│  │     └─ completion msg → Host consumer로 전송  │        │
│  └──────────────────────────────────────────────┘        │
│                                                         │
│  PCI 03:00.0 (ConnectX-7, DPU측)                        │
└─────────────────────────────────────────────────────────┘
```

### CPU (Host, x86_64)에서 도는 코드

| 프로세스 | 파일 | 역할 |
|----------|------|------|
| UniqueIdService | `TDpumeshServerTransport.cpp` | Thrift 서버: accept → read → process → write |
| (same) | `TDpumeshTransport.cpp` | 개별 연결 래퍼: read/write/flush |
| (same) | `dpumesh_doca.c` | DOCA 백엔드: TX(DMA ring), RX(comch condvar), PE thread |
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
| (same) | `doca/dpu_worker.c` | 워커: comch server → consumer → DPA init → PE 폴링 루프 |
| (same) | `doca/config.c` | `-p`/`-r` PCI 주소 파싱 (doca_argp) |
| (same) | `doca/comch_server.c` | comch control path 서버 (Host client 연결 수락) |
| (same) | `doca/comch_consumer.c` | Host producer 메시지 수신 |
| (same) | `doca/comch_producer.c` | Host consumer로 DMA completion 전송 |
| (same) | `doca/dpa.c` | DPA 커널/스레드 생성, buf_arr 설정, DPA 실행 |

빌드: `meson setup builddir && ninja -C builddir` (DPU에서 직접, DPACC 포함)
실행: `./dpumesh_dpu -p 03:00.0 -r 94:00.0`

### DPA (Data Path Accelerator, HW thread)에서 도는 코드

| 함수 | 파일 | 역할 |
|------|------|------|
| `run_dma_manager()` | `doca/device/dpa_kernel.c` | DMA ring 폴링 → DMA copy → completion msg 전송 |
| `thread_init_rpc()` | `doca/device/dpa_kernel.c` | DPA 스레드 초기화 RPC (consumer ack) |
| `poll_desc_ring()` | `doca/device/dpa_kernel.c` | Host 메모리의 dma_desc ring 감시, valid=1이면 DMA 실행 |
| `handle_msgs()` | `doca/device/dpa_kernel.c` | DPU→DPA comch 메시지 처리 (DMA 요청 등) |

### 데이터 흐름

```
[TX: Host→DPU]
  App flush() → dpumesh_enqueue() → DMA ring에 desc 쓰기 (valid=1)
  → DPA poll_desc_ring()이 감지 → doca_dpa_dev_comch_producer_dma_copy() 실행
  → DPU 메모리로 DMA 완료 → completion msg → Host consumer 콜백

[RX: DPU→Host] (임시, comch control path)
  DPU에서 server_send_rx_data() → comch control path msg 전송
  → Host comch client 콜백 → rx_data_hook() → RX buffer에 body 복사
  → descriptor queue push → condvar signal
  → App dpumesh_dequeue()에서 깨어남 → dpumesh_rx_buf()로 데이터 읽기
```


---

## 1. 클라이언트 Transport 구현 — ❌ 미완료

현재 서버 측(TDpumeshServerTransport + TDpumeshTransport)만 구현되어 있음.
서비스 간 호출 체인(ComposePost → UniqueId 등)은 여전히 TCP(TSocket) 사용 중.

### 응답 매칭: condvar 방식 (결정됨)

- `req_id` 필드로 요청/응답 매칭 (HTTP/2의 stream_id와 같은 역할)
- `OP_REQUEST`/`OP_RESPONSE` flags로 요청/응답 구분

```
poller thread:  rx_sq에서 desc 꺼냄 (64B) → flags 확인
                  OP_REQUEST  → 기존 서버 처리 경로 (notify pipe)
                  OP_RESPONSE → pending_[req_id].desc에 복사 (64B) → cv.notify_one()

client thread:  요청 전송 → cv.wait() → 깨어남 → desc.body_buf_slot으로 SHM 직접 읽기 (zero-copy)
```

### 구현 계획

- `TDpumeshClientTransport.h/cpp` 별도 클래스 (서버/클라이언트 read/write 동작이 다름)
  - 구현할 메서드: open(), write(), flush(), read(), close(), isOpen()
- `dpumesh_shm.c`에 condvar 기반 응답 매칭 추가 (순수 C 유지)
  - `pending_entry_t` 배열 (크기 = NUM_SLOTS, pthread_mutex/cond 사용)
  - poller thread에서 OP_REQUEST/OP_RESPONSE 분류 로직 추가


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
- DPU 접속: `ssh jukebox@192.168.100.2` (tmfifo_net0도 가능하나 192.168.100.2 사용)
- DPU에 외부 인터넷 없음 (DNS resolve 불가, 패키지 설치 시 Host 경유 필요)
- DPU에 cmake 없음, meson 0.61.2 + ninja 1.10.1 사용
- 시간 동기화 안 됨 (clock skew 주의, `find . -exec touch {} +`로 우회)

### 완료된 작업

#### Part 1: DOCA 인프라 코드 통합 — ✅ 완료

- `DPUMesh_doca/DPUMesh/` → `lib/cpp/src/thrift/transport/doca/` 복사 + 수정
- `dpa_common.h` 수정: `comch_dma_comp_msg`에 `req_id`, `src_pod_id` 필드 추가

#### Part 2: 통합 공개 API (`dpumesh.h`) — ✅ 완료

- `dpumesh_doca.h`, `dpumesh_shm.h` → `dpumesh.h`로 통합
- 애플리케이션 코드는 `dpumesh.h`만 include (SHM/DOCA 모름)

#### Part 3: dpumesh_doca.c 구현 (TX + RX) — ✅ 완료

- TX: DMA ring 기반 (Host→DPU)
- RX: comch control path 임시 구현 (DPU→Host), condvar 기반

#### Part 4: comch RX 메시지 프로토콜 — ✅ 완료

- `DMESH_MSG_RX_DATA` 메시지 타입, `server_send_rx_data()`, `rx_data_hook()`

#### Part 5: DPU 바이너리 — ✅ 완료 (코드 + 빌드)

- `dpu_main.c`, `dpu_worker.c`, `config.c` 작성
- CMakeLists.txt: `dpumesh_dpu` 타겟 (`EXCLUDE_FROM_ALL`)
- meson.build 작성 (DPU cmake 없어서 meson 사용)
- **DPU에서 빌드 성공** (meson + DPACC → `dpumesh_dpu` aarch64 ELF, 388K)
- **DPU에서 실행 확인** (`./dpumesh_dpu -p 03:00.0 -r 94:00.0` → DPU worker 시작 로그)

#### Part 6: CMakeLists.txt (Host) — ✅ 완료

- `WITH_DOCA` 옵션, 조건부 소스, DOCA pkg-config 링크
- Host 빌드 성공 확인

#### Part 7: 배포 인프라 — ✅ 완료 (코드 작성)

- `Dockerfile.uniqueid`: `WITH_DOCA` arg
- `deploy-dpumesh.sh`: `--doca` 플래그
- `UniqueIdService/CMakeLists.txt`: thrift 라이브러리만 링크

#### DPU 환경 확인 — ✅ 완료

- DPU SSH 접속: `ssh jukebox@192.168.100.2` (키 인증)
- DOCA SDK 3.1.0105 설치됨
- DPACC (`/opt/mellanox/doca/tools/dpacc`) 존재
- PCI: `03:00.0` (ConnectX-7), Host PF representor = `pf0hpf` (PCI `94:00.0`)
- 빌드 도구: gcc 11.4.0, meson 0.61.2, ninja 1.10.1 (cmake 없음)
- 시간 동기화 안 됨 (clock skew 주의, `find . -exec touch {} +`로 우회)

#### K8s 환경 정리 — ✅ 완료

- `social-network` namespace 삭제 (Terminating 상태에서 막힘 → 원인: metrics-server 죽음)
- metrics-server apiservice 삭제로 해결
- namespace finalizer 제거 → 삭제 완료

### 남은 작업 (Original)

- ❌ UniqueIdService DOCA 빌드 → Thrift RPC 호출
- ❌ deploy-dpumesh.sh DOCA 모드 완성
- ❌ RX 경로 DMA 전환 (향후)

### 확인 필요 사항

1. **rep PCI 주소**: ✅ 해결 (2026-03-24)
   - **잘못된 값**: `-r 03:00.0` (DPU의 SF representor `en3f0pf0sf0`에 바인딩됨)
   - **올바른 값**: `-r 94:00.0` (Host PF representor `pf0hpf`에 바인딩)
   - DPU의 representor 목록 (`doca_caps --list-rep-devs`):
     - `94:00.0` → `pf0hpf` (Host PF, **comch server가 바인딩해야 하는 대상**)
     - `03:00.0` → `en3f0pf0sf0` (SF, 잘못 사용하면 Host client가 syndrome 0xe5300으로 거부됨)
   - `-r 03:00.0`으로 서버 시작 시: 서버는 정상 시작되나 Host client의 devx object 생성을
     firmware가 거부 (EREMOTEIO, syndrome=0xe5300, DOCA_ERROR_CONNECTION_ABORTED)
   - `-r 94:00.0`으로 서버 시작 시: NVIDIA 공식 comch 샘플로 연결 성공 확인
2. **dpa_program.a**: DPACC로 DPU에서 재빌드 성공 (meson setup 시 자동)
3. **K8s PCI passthrough**: DOCA 디바이스를 컨테이너에서 접근하는 방식

---

## 4. DPU TCP Gateway (Interception) 전략 — 🔄 검토/진행

클라이언트 수정 없이 DPU가 TCP 요청을 가로채서 DPUmesh로 변환하는 전략입니다.

### 데이터 흐름 (Interception 모드)

1.  **Request**: Client (TCP) → DPU (Port 9091) → `server_send_rx_data()` → Host (DPUmesh RX) → UniqueIdService
2.  **Processing**: UniqueIdService가 `TDpumeshServerTransport`를 통해 데이터 수신 및 처리
3.  **Response**: UniqueIdService (Flush) → Host (DMA Ring) → DPA (DMA Copy) → DPU (TCP Write) → Client (TCP)

### 긴급 추가 작업 (Priority)

1.  **DPA 커널 루프 수정 (Bug Fix)**: `dpa_kernel.c`에서 링 디스크립터 전체를 순회하도록 수정 (현재 0번 슬롯 고정).
2.  **DPU TCP 리스너 추가**: `dpumesh_dpu`에 TCP 9091 리스닝 및 `server_send_rx_data` 연동.
3.  **응답 경로(TX) 연동**: DPA가 DMA 완료 후 DPU 워커에게 알리고, 워커가 TCP로 응답을 쏘는 로직 완성.

---

## 파일 구조

```
thrift_dpumesh_extended/lib/cpp/src/thrift/transport/
├── dpumesh.h                     ← 통합 공개 API (SHM/DOCA 공통)
├── dpumesh_doca.c                ← DOCA 백엔드 (Host, TX+RX)       [CPU]
├── dpumesh_shm.c                 ← SHM 백엔드 (시뮬레이션)         [CPU]
├── TDpumeshTransport.cpp/h       ← Thrift C++ 래퍼                [CPU]
├── TDpumeshServerTransport.cpp/h ← Thrift 서버 래퍼               [CPU]
└── doca/
    ├── meson.build               ← DPU 바이너리 빌드 (meson)
    ├── dpu_main.c                ← DPU 바이너리 진입점             [DPU]
    ├── dpu_worker.c/h            ← DPU 워커 및 TCP Gateway 예정    [DPU]
    ├── config.c/h                ← argp 파싱                      [DPU]
    ├── common.c/h                ← DOCA 디바이스 유틸              [CPU+DPU]
    ├── object.c/h                ← struct objects (중앙 상태)      [CPU+DPU]
    ├── buffer.c/h                ← mmap + 버퍼 관리               [CPU+DPU]
    ├── ring.c/h                  ← DMA ring                      [CPU+DPU]
    ├── comch_common.h            ← 메시지 타입 정의               [CPU+DPU]
    ├── comch_client.c/h          ← comch 클라이언트               [CPU]
    ├── comch_server.c/h          ← comch 서버                    [DPU]
    ├── comch_consumer.c/h        ← comch datapath consumer       [CPU+DPU]
    ├── comch_producer.c/h        ← comch datapath producer       [CPU+DPU]
    ├── comch_msgq.c/h            ← DPA message queue             [CPU+DPU]
    ├── dma.c/h                   ← DMA 유틸                      [CPU+DPU]
    ├── dpa.c/h                   ← DPA 초기화/실행               [CPU+DPU]
    ├── dpa_common.h              ← DPA 공유 구조체               [CPU+DPU+DPA]
    ├── build_dpacc.sh            ← DPACC 빌드 스크립트
    └── device/
        ├── dpa_kernel.c          ← DPA 커널 소스                 [DPA]
        └── dpa_program.a         ← DPA 커널 아카이브 (DPACC로 빌드)
```
