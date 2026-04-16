# DPU Mesh Transport 리디자인 — 진행 상황 및 남은 작업

## 완료된 Phase

### Phase 1: 데이터 구조 정의 [완료]
- `fc_header` (consumer_tail + payload_len) 추가
- `dpu_comp_entry_t` → heap pointer 대신 buffer offset (buf_offset, pod_idx)
- `pod_state` 확장: 양방향 ring, TX/RX buffer, flow control state
- `COMCH_MSG_TYPE_ADD_REV_RING`, `COMCH_MSG_TYPE_REV_DMA_COMPLETED` 추가
- `DMA_HOST_RX_BUFFER` mmap type 추가

### Phase 2: Ring/Buffer 인프라 [완료]
- `setup_dpu_tx_ring()`: DPU-side TX ring 생성 (PCI mmap, export 안함)
- Host RX DMA buffer 할당 및 DPU로 export (`DMA_HOST_RX_BUFFER` type)

### Phase 3: DPA 커널 양방향 처리 [완료]
- `process_one_rev_desc()`: 역방향 DMA (DPU TX → Host RX)
- `drain_rev_producer_completions()`: 역방향 producer completion 처리
- `COMCH_MSG_TYPE_ADD_REV_RING` 핸들러 (update 지원 포함)
- `drain_all_rings()`: forward + reverse 모두 처리

### Phase 4: DPU 측 변경 [완료]
- `setup_pod_dma()` 역방향 셋업: TX buffer, TX ring, buf_arr, ADD_REV_RING
- `dpu_enqueue_reverse_dma()`: DPU→CPU DMA enqueue (fc_header + sw_descriptor + body)
- `process_completion_queue()`: `server_send_rx_data_to()` → `dpu_enqueue_reverse_dma()` 교체
- `process_mmap_msg()`: `DMA_HOST_RX_BUFFER` type 처리
- `update_rev_ring_host_rx()`: 늦게 도착한 Host RX mmap → DPA ring 업데이트
- `COMCH_MSG_TYPE_REV_DMA_COMPLETED` 핸들러: DPU ARM이 Host에 comch 알림
- fc_header 파싱 (CPU→DPU): DPU가 Host의 RX consumer_tail 업데이트
- TX_ACK를 DMA completion 시점에 전송 (기존: 데이터 라우팅 후 전송)
- `rev_dpa_producer` = forward producer 재사용 (DPA→DPU ARM 경로)
- `server_send_msg_to_conn()` 공개 (static → public)

### Phase 5: Host 측 변경 [완료]
- `DMESH_MSG_DMA_COMPLETION` 핸들러 (comch_client.c)
- `rx_data_hook()`: DMA_COMPLETION 처리 — RX DMA buffer에서 fc_header + desc + body 파싱
- `rx_deliver_desc()`: 공통 전달 함수 (pending table 또는 RX queue)
- flow control 업데이트: `fc_tx_last_consumer_tail` (DPU의 consumer_tail 수신)
- `dpumesh_tx_buf()`: fc_header 공간 예약 (8B headroom)
- `dpumesh_enqueue()`: fc_header 작성 (consumer_tail + payload_len)
- `dpumesh_enqueue()`: descriptor size에 fc_header 크기 포함
- `dpumesh_enqueue()`: body_len 검증 — `slot_size - sizeof(fc_header)` 기준으로 수정

### Phase 6: comch data path 코드 제거 [완료]
- `comch_datapath_send_payload()` 제거 (comch_producer.c)
- `init_comch_datapath_producer_for_connection()` 제거 (comch_producer.c)
- `init_comch_datapath_producer()` 제거 (comch_producer.c) — 호출처 없음
- pod별 `producer/producer_pe/producer_mem` 필드 제거 (object.h pod_state)
- `ensure_pod_datapath_sender()` 제거 (comch_server.c)
- `server_send_rx_data()` 제거 (comch_server.c)
- `server_send_rx_data_to()` 제거 (comch_server.c)
- `progress_all_pes()` — per-pod producer PE 순회 제거 (object.h)
- DPU worker main loop — per-pod producer PE progress 제거 (dpu_worker.c)
- Host PE thread — producer_pe progress 제거 (dpumesh_doca.c)
- Host `init_datapath()` — `init_comch_datapath_producer()` 호출 제거

### 128B 정렬 검증 [완료]
- **결론: 정렬 깨지지 않음**
  - DMA source/dest base address: page-aligned (mmap 할당)
  - 모든 offset (pos, write_pos, slot_offset): 128B aligned
  - DPA가 chunk size를 항상 `ALIGN_UP_128()` 패딩
  - fc_header (8B)는 aligned block 시작 부분 → address alignment 불변
  - `dpumesh_enqueue()` body_len 검증 수정: `slot_size - sizeof(fc_header)` 기준

## 남은 작업

### 런타임 테스트 필요
- **E2E 테스트**: `test-dpumesh.sh deploy` 후 `test-dpumesh.sh test` 실행하여 전체 경로 검증
- **역방향 DMA 검증**: DPU→CPU 데이터가 Host RX DMA buffer에 정확히 도착하는지 확인
- **flow control 검증**: 고부하에서 stall이 발생하지 않는지 확인
- **edge case**: echo mode, 다중 pod, 큰 메시지 (>8KB chunking)

### 잠재적 이슈
1. **Host RX buffer mmap 타이밍**: `host_rx_mmap`이 `setup_pod_dma` 이후에 도착할 수 있음.
   현재 `update_rev_ring_host_rx`로 처리하지만, 첫 pod 시에는 h2d_memcpy로 
   thread_arg를 썼기 때문에 reverse ring이 host_mmap=0으로 들어감.
   → DPA가 host_mmap==0인 descriptor를 skip하므로 데이터 loss는 없지만,
     Host RX mmap 도착 전에 DPU→CPU 전송 시도하면 실패함.

2. **zero-copy 미완성**: Host RX path에서 `rx_data_hook`이 아직 `memcpy`로 
   body를 RX slot에 복사함. 진정한 zero-copy를 위해서는 RX DMA buffer의 
   offset을 직접 참조하도록 변경 필요.

3. **consumer_tail wrap-around**: 원형 버퍼 wrap-around 시 consumer_tail 계산이 
   정확한지 edge case 검증 필요 (특히 DPU TX buffer에서 write_pos가 끝에 
   도달했을 때).

4. **REV_DMA_COMPLETED 알림**: DPU ARM이 Host에 comch control path로 알림을 보냄.
   이 자체가 bottleneck이 될 수 있음 (comch control path 대역폭 제한).
   장기적으로는 DPA가 Host consumer에 직접 completion을 보내는 구조로 개선 필요.

5. **comch datapath consumer 정리 가능**: Host-side `init_comch_datapath_consumer()`는 
   DMA 경로로 대체되어 더 이상 데이터 수신에 사용되지 않음. 그러나 DPU-side consumer는 
   DPA message queue에 필요. Host-side consumer만 선택적으로 제거 가능.

## 수정된 파일 목록
- `lib/cpp/src/thrift/transport/doca/dpa_common.h` — fc_header, message types, rev ring
- `lib/cpp/src/thrift/transport/doca/object.h` — pod_state 확장 (per-pod producer 제거), dpu_comp_entry_t 변경, progress_all_pes 정리
- `lib/cpp/src/thrift/transport/doca/comch_common.h` — DMA_HOST_RX_BUFFER, DMESH_MSG_DMA_COMPLETION
- `lib/cpp/src/thrift/transport/doca/comch_common.c` — process_mmap_msg 확장, update_rev_ring
- `lib/cpp/src/thrift/transport/doca/comch_server.h` — server_send_msg_to_conn 선언, dead code 선언 제거
- `lib/cpp/src/thrift/transport/doca/comch_server.c` — dead code 제거 (ensure_pod_datapath_sender, server_send_rx_data_to 등)
- `lib/cpp/src/thrift/transport/doca/comch_client.c` — DMESH_MSG_DMA_COMPLETION 핸들러
- `lib/cpp/src/thrift/transport/doca/comch_producer.h` — dead code 선언 제거
- `lib/cpp/src/thrift/transport/doca/comch_producer.c` — dead code 제거 (init_comch_datapath_producer, comch_datapath_send_payload 등)
- `lib/cpp/src/thrift/transport/doca/dpa.h` — update_rev_ring_host_rx 선언
- `lib/cpp/src/thrift/transport/doca/dpa.c` — 역방향 셋업, fc_header 파싱, REV_DMA_COMPLETED, TX_ACK 이동
- `lib/cpp/src/thrift/transport/doca/dpu_worker.c` — dpu_enqueue_reverse_dma, process_completion_queue 변경, per-pod producer 제거
- `lib/cpp/src/thrift/transport/doca/ring.h` — setup_dpu_tx_ring 선언
- `lib/cpp/src/thrift/transport/doca/ring.c` — setup_dpu_tx_ring 구현
- `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c` — process_one_rev_desc, ADD_REV_RING update, REV type
- `lib/cpp/src/thrift/transport/dpumesh_doca.c` — Host RX DMA, fc_header TX, rx_deliver_desc, body_len 검증 수정, producer 제거
