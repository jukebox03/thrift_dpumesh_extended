# DPUMesh DMA 실험 결과

## 1. doca_dpa_dev_comch_producer_dma_copy 데이터 무결성 테스트 (DPUMesh_doca)

Host가 전체 DMA 버퍼를 `byte[i] = (i * 0x9E + 0x37) & 0xFF` 패턴으로 채우고,
DPU recv callback에서 매 바이트를 검증.

| msg_size | 결과 | recv rate | 비고 |
|----------|------|-----------|------|
| 64B | CORRUPT | - | offset 0 mismatch (got=0x37 expected=0xb7, pos=192) |
| 96B | CORRUPT | - | offset 0 mismatch |
| 112B | CORRUPT | - | offset 0 mismatch |
| 120B | CORRUPT | - | offset 0 mismatch |
| 124B | CORRUPT | - | offset 0 mismatch |
| 126B | CORRUPT | - | offset 0 mismatch |
| 127B | CORRUPT | - | offset 0 mismatch |
| **128B** | **CLEAN** | 818,630/s | 모든 바이트 정확 |
| 129B | NO DATA | - | DPA stuck (consumer empty) |
| 130B | NO DATA | - | DPA stuck |
| 132B | NO DATA | - | DPA stuck |
| 136B | NO DATA | - | DPA stuck |
| 144B | NO DATA | - | DPA stuck |
| 160B | NO DATA | - | DPA stuck |
| 192B | NO DATA | - | DPA stuck |
| **256B** | **CLEAN** | 912,780/s | 모든 바이트 정확 |
| **512B** | **CLEAN** | 821,046/s | 모든 바이트 정확 |
| 1024B | NO DATA | - | DPA stuck |
| 4096B | NO DATA | - | DPA stuck |
| 8192B | NO DATA | - | DPA stuck |

**발견**: `dma_copy`는 128B 배수일 때만 정상 작동. 128B 미만 또는 128B 배수가 아닌 크기에서 corruption 또는 DPA stuck 발생.

### 원인 분석: 64B에서도 corruption이 발생하는 이유

64B 테스트의 mismatch: `got=0x37 expected=0xb7 (pos=192)`
- Host 패턴: offset 0 = `0x37`, offset 192 = `0xB7`
- DPU에서 pos=192 위치의 데이터가 `0x37` (offset 0의 값)

Host에서 `desc->addr = buffer + pos`로 설정. msg_size=64일 때 pos는 0, 64, 128, 192...로 증가.
pos=192일 때 `desc->addr = buffer + 192` (128B 비정렬 주소).

**`dma_copy` HW가 소스 주소를 128B 경계로 round down하여 DMA를 수행한다.**
- pos=192 → round down → pos=128 → offset 128의 데이터 복사
- 실제로 offset 128의 패턴값: `(128 * 0x9E + 0x37) & 0xFF = 0x37` ← 일치!

128B, 256B, 512B가 성공하는 이유: pos가 해당 크기 단위로 증가하므로 항상 128B 정렬.
129~192B가 DPA stuck인 이유: 비정렬 DMA 실패로 consumer 응답 없음 → 무한 대기.

**결론: `doca_dpa_dev_comch_producer_dma_copy`는 소스/목적지 주소의 128B alignment가 필수.**
크기(size) 제한이 아니라 **주소 정렬(address alignment)** 제약이었다.

## 2. thrift_dpumesh_extended: dma-copy 버전 (DPA_DMA_COPY_MAX=128)

dpa_kernel.c에서 `doca_dpa_dev_comch_producer_dma_copy`를 128B 청크로 분할하여 전송.
consumer timeout 시 abort 로직 + 주기적 producer drain 추가.

### 직접 gateway 테스트 (test_thrift.py)

| 테스트 | 결과 |
|--------|------|
| 59B 기본 요청 | PASS |
| 140B single-entry (padding) | PASS |
| 140B 2-entry carrier map | PASS |
| 256B ~ 128KB (padding) | 전부 PASS |

### wrk2 부하 테스트 (compose-post through social network)

| Rate | Requests | Errors | Req/s | Avg Latency |
|------|----------|--------|-------|-------------|
| R=100 | 991 | 0 | 99 | - |
| R=1000 Run 1 | 9,992 | 0 | 999 | 2.71ms |
| R=1000 Run 2 | 9,991 | 0 | 999 | 2.58ms |
| R=1000 Run 3 | 9,992 | 0 | 999 | 2.66ms |
| R=2000 | 1 | 40 timeout | 0.1 | DPA stuck |

## 3. thrift_dpumesh_extended: memcpy 버전 (cleanup commit, post_memcpy + post_send_imm_only)

dpa_kernel.c에서 `doca_dpa_dev_post_memcpy`로 DMA 후 `post_send_imm_only`로 notification 분리.
DMA 크기 제한 없음 (DPA_MEMCPY_CHUNK_MAX=128KB).

### wrk2 부하 테스트 (compose-post through social network)

| Rate | Errors | Avg Latency |
|------|----------|--------|
| R=1000 | 0 | 2.75ms |
| R=2000 | 0 | 2.60ms |
| R=5000 | 0 | 847ms |

| Rate | Errors |Avg Latency |
|------|----------|--------|
| R=1000 | 0 | 2.00ms |
| R=2000 | 0 | 3.60ms |
| R=3000 | 0 | 734ms |
| R=4000 | 0 | 3798ms |
| R=5000 | 0 | 8988ms |

## 4. 비교 요약

| 항목 | dma-copy (128B chunk) | memcpy (cleanup) |
|------|----------------------|-----------------|
| R=1000 | 0% 에러 | 0% 에러 |
| R=2000 | DPA stuck | 0% 에러, 1998 req/s |
| R=5000 | 테스트 불가 | 0% 에러, 4127 req/s |
| 오버로드 후 복원 | 재배포 필요 | 즉시 복원 |
| DMA 방식 | dma_copy (128B 제한) | post_memcpy (128KB) + send_imm_only |
| chunk 수 (128KB) | 1,024 | 1 |
| consumer task 소모 | chunk당 1개 | transfer당 1개 |
