# DPUmesh Thrift Transport — Plan

## 실행 아키텍처: CPU / DPU / DPA에서 무엇이 도는가

### 전체 구조 (DPU TCP Gateway 모드)

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
│  │     ├─ TCP Listener (Port 9091) ◀─── Client  │ (NEW)  │
│  │     ├─ TCP-to-DPUmesh Relay (Interception)   │ (NEW)  │
│  │     ├─ comch server 시작 (Host client 대기)  │        │
│  │     ├─ consumer datapath 초기화              │        │
│  │     ├─ DPA 커널/스레드 초기화                │        │
│  │     └─ 메인 루프: doca_pe_progress() 폴링    │        │
│  └─────────────┬────────────────────────────────┘        │
│                │ DPA thread 관리                          │
│                ▼                                         │
│  ┌─────────────────────────────────────────────┐        │
│  │ DPA (Data Path Accelerator, HW thread)       │        │
│  │   dpa_kernel.c → run_dma_manager()           │        │
│  │     ├─ Host DMA ring 폴링 (desc.valid 감시)  │ (FIX)  │
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

### DPU (BlueField-3, ARM aarch64)에서 도는 코드

| 프로세스 | 파일 | 역할 |
|----------|------|------|
| dpumesh_dpu | `doca/dpu_main.c` | 진입점: 로깅, argp, 디바이스 open, rep open |
| (same) | `doca/dpu_worker.c` | **Thrift TCP Gateway**: 포트 9091 리스닝 및 데이터 릴레이 |
| (same) | `doca/config.c` | `-p`/`-r` PCI 주소 파싱 (doca_argp) |
| (same) | `doca/comch_server.c` | comch control path 서버 및 `server_send_rx_data()` |
| (same) | `doca/comch_consumer.c` | Host producer 메시지 수신 |
| (same) | `doca/comch_producer.c` | Host consumer로 DMA completion 전송 |
| (same) | `doca/dpa.c` | DPA 커널/스레드 생성, buf_arr 설정, DPA 실행 |

### DPA (Data Path Accelerator, HW thread)에서 도는 코드

| 함수 | 파일 | 역할 |
|------|------|------|
| `run_dma_manager()` | `doca/device/dpa_kernel.c` | DMA ring 폴링 → DMA copy → completion msg 전송 |
| `thread_init_rpc()` | `doca/device/dpa_kernel.c` | DPA 스레드 초기화 RPC (consumer ack) |
| `poll_desc_ring()` | `doca/device/dpa_kernel.c` | **FIX 필요**: 링 인덱스 증가 및 연속 폴링 로직 수정 |
| `handle_msgs()` | `doca/device/dpa_kernel.c` | DPU→DPA comch 메시지 처리 (DMA 요청 등) |

---

## 1. DPU TCP Gateway (Interception) 구현 — 🔄 진행 중

클라이언트(ComposePost 등)는 기존 TCP 방식을 유지하고, DPU가 이를 가로채서 DPUmesh로 변환합니다. `TDpumeshClientTransport` 구현은 제외합니다.

### 데이터 흐름 (Interception 모드)

1.  **Request**: Client (TCP) → DPU (Port 9091) → `server_send_rx_data()` → Host (DPUmesh RX) → UniqueIdService
2.  **Processing**: UniqueIdService가 `TDpumeshServerTransport`를 통해 데이터 수신 및 처리
3.  **Response**: UniqueIdService (Flush) → Host (DMA Ring) → DPA (DMA Copy) → DPU (TCP Write) → Client (TCP)

---

## 2. TNonblockingServer 호환 — 보류

- dpumesh에서는 데이터가 한 번에 도착 → NonBlocking의 "조금씩 읽기" 패턴 불필요
- SHM slot 수(64개)가 동시 연결 상한 → 수천 연결 관리의 이점 없음
- TThreadedServer 사용으로 충분

---

## 3. DOCA 백엔드 통합

### 환경
- Host: Linux 5.15, x86_64, BF3 PCI `94:00.0`, DOCA SDK 3.1.0
- DPU: Ubuntu 22.04, aarch64, PCI `03:00.0`, DOCA SDK 3.1.0105
- DPU 접속: `ssh jukebox@192.168.100.2`
- DPU 특징: 외부 인터넷 없음 (DNS 불가), cmake 없음, meson 0.61.2 + ninja 1.10.1 사용
- 시간 동기화 안 됨 (clock skew 이슈 → `find . -exec touch {} +`로 우회)

### 완료된 작업

#### Part 1: DOCA 인프라 코드 통합 — ✅ 완료
- `DPUMesh_doca/DPUMesh/` → `lib/cpp/src/thrift/transport/doca/` 복사 + 수정
- `dpa_common.h` 수정: `comch_dma_comp_msg`에 `req_id`, `src_pod_id` 필드 추가

#### Part 2: 통합 공개 API (`dpumesh.h`) — ✅ 완료
- `dpumesh_doca.h`, `dpumesh_shm.h` → `dpumesh.h`로 통합 완료.

#### Part 3: dpumesh_doca.c 구현 (TX + RX) — ✅ 완료
- TX: DMA ring 기반 (Host→DPU)
- RX: comch control path 임시 구현 (DPU→Host), condvar 기반

#### Part 4: comch RX 메시지 프로토콜 — ✅ 완료
- `DMESH_MSG_RX_DATA` 메시지 타입, `server_send_rx_data()`, `rx_data_hook()`

#### Part 5: DPU 바이너리 — ✅ 완료 (코드 + 빌드)
- `dpu_main.c`, `dpu_worker.c`, `config.c` 작성 및 DPU에서 빌드 성공.

#### Part 6: CMakeLists.txt (Host) — ✅ 완료
- `WITH_DOCA` 옵션 및 DOCA 라이브러리 링크 설정 완료.

#### Part 7: 배포 인프라 — ✅ 완료 (코드 작성)
- `Dockerfile.uniqueid`: `WITH_DOCA` 지원.
- `UniqueIdService/UniqueIdService.cpp`: `TDpumeshServerTransport` 적용 완료.

#### DPU 환경 확인 및 이슈 해결 — ✅ 완료
- **PCI Representor 이슈**: 
  - 잘못된 값: `-r 03:00.0` 사용 시 Host client 연결 거부 (syndrome `0xe5300`, `DOCA_ERROR_CONNECTION_ABORTED`).
  - 올바른 값: `-r 94:00.0` (Host PF representor `pf0hpf`) 바인딩으로 해결.
- **K8s 네임스페이스 삭제 이슈**:
  - `social-network` 네임스페이스가 Terminating에 갇힘 → `metrics-server` apiservice 삭제 및 finalizer 제거로 해결.

### 남은 작업 (Priority)

1.  **DPA 커널 루프 수정 (Bug Fix)**: `dpa_kernel.c`에서 링 디스크립터 전체를 순회하도록 수정 (현재 0번 슬롯 고정).
2.  **DPU TCP 리스너 추가**: `dpumesh_dpu`에 TCP 9091 리스닝 및 `server_send_rx_data` 연동.
3.  **응답 경로(TX) 연동**: DPA가 DMA 완료 후 DPU 워커에게 알리고, 워커가 TCP로 응답을 쏘는 로직 완성.
4.  **E2E 검증**: `ComposePostService`(TCP) → `DPU Gateway`(Intercept) → `UniqueIdService`(DPUmesh) 흐름 테스트.

---

## 파일 구조

```
thrift_dpumesh_extended/lib/cpp/src/thrift/transport/
├── dpumesh.h                     ← 통합 공개 API (SHM/DOCA 공통)
├── dpumesh_doca.c                ← DOCA 백엔드 (Host)
├── dpumesh_shm.c                 ← SHM 백엔드 (시뮬레이션)
├── TDpumeshTransport.cpp/h       ← Thrift C++ 래퍼
├── TDpumeshServerTransport.cpp/h ← Thrift 서버 래퍼
└── doca/
    ├── dpu_worker.c              ← DPU 워커 및 TCP Gateway 예정
    └── device/
        └── dpa_kernel.c          ← DPA 커널 (DMA manager)
```
