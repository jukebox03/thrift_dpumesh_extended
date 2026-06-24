# DMA Chain Bench 측정 (2026-05-04)

`/home/jukebox/test_dma`의 micro-bench로 DPA + comch + DPU 체인의 raw throughput
한계를 측정. 우리 dpumesh의 cap이 어디서 결정되는지 isolate하기 위함.

## 측정 환경

- bench는 **k8s pod 없음, TCP 없음, Thrift 없음**. host process가 dma_ring에
  desc 직접 post → DPA polling → DMA → DPU process가 PE polling
- 측정값: DPU(또는 host)의 comch consumer recv rate × msg_size
- 메시지 크기: 8 KB (single-DMA 한도, 우리 dpumesh와 동일)
- 방향: H2D (host → DPU)
- duration: 10s (warmup 3s)

## 측정 모드

| 모드 | DMA mechanism | DPA→DPU comch | DPU→Host comch |
|---|---|---|---|
| **Method 0** | `dma_copy` (atomic DMA + comch) | imm 자동 송신 | ✗ |
| **Method 1 (원본)** | `post_memcpy` + wait + `post_send_imm_only` | 별도 송신 | ✗ |
| **Method 1 (mod, fire-and-forget)** | `post_memcpy` + drain (no wait) + `post_send_imm_only` | 별도 송신 | ✗ |
| **Method 1 (mod, dma_copy 사용)** | `dma_copy` 통합 호출 | imm 자동 | ✗ |
| **Method 2 (신규, mode 3)** | `dma_copy` + DPU CPU가 host로 forward | imm 자동 | `server_send_msg` 추가 |

## 결과

| 측정 | recv/sec | Throughput | vs Method 0 |
|---|---:|---:|---:|
| **Method 0** (HW max, dma_copy + comch atomic) | 297,004 ~ 302,617 | **19.46 ~ 19.83 Gbps** | 100% (baseline) |
| Method 1 원본 (stop-and-wait) | 137,756 | 9.02 Gbps | 46% |
| Method 1 mod (fire-and-forget post_memcpy) | 172,076 | 11.27 Gbps | 57% |
| Method 1 mod (dma_copy with explicit imm) | 305,944 | 20.05 Gbps | ≈ Method 0 |
| **Method 2 (DPA→DPU→Host)** | 264,869 | **17.35 Gbps** | **87%** |

## 분리된 비용

| 추가 layer | throughput 손실 |
|---|---|
| `wait` for completion 추가 | -25% (11.27 → 9.02 Gbps) |
| `post_memcpy` + 별도 `imm` (vs `dma_copy` atomic) | -42% (19.83 → 11.27 Gbps) |
| **DPU CPU의 host로 `server_send_msg` 추가** | **-13%** (19.83 → 17.35 Gbps) |

→ `dma_copy`의 atomic combined call이 가장 효율적. `wait` 보다 분리된 API call의 비용이 더 큼.

## 우리 dpumesh와 비교

| 지표 | bench Method 2 (DPA→DPU→Host) | dpumesh @ 45K RPS |
|---|---:|---:|
| chain ops/sec | 264,869 | 45,000 × 4 = 180,000 |
| chain capacity 활용도 | 100% (HW chain max) | **68%** |
| 이론 max req/s (chain bound) | — | 264,869 / 4 = **66,217** |
| 실측 cap | — | **45,000 (68%)** |

**dpumesh의 32% 갭 = chain 외부 작업** (Thrift 처리, k8s overhead, TCP, 라우팅 등). chain 자체는 cap 아님.

## 측정 데이터 (raw)

CSV 파일들 (in `/home/jukebox/test_dma/`):
- `bench_results_20260504_180640.csv` — Method 0
- `bench_results_20260504_180806.csv` — Method 1 원본 (wait)
- `bench_results_20260504_181308.csv` — Method 1 mod (fire-and-forget)
- `bench_results_20260504_181812.csv` — Method 1 mod (dma_copy)
- `bench_results_20260504_184624.csv` — Method 0 baseline 재측정
- `bench_results_20260504_184758.csv` — **Method 2 (DPA→DPU→Host)**

## 결론

1. **HW DMA chain의 한계는 19~20 Gbps** (dma_copy atomic).
2. **DPA→DPU→Host 풀 chain은 17.35 Gbps** (HW의 87%).
3. 우리 dpumesh의 2.5 Gbps (per-direction TX from gateway view) 또는
   180K chain ops/sec는 chain의 68%만 사용. **chain은 우리 cap 아님**.
4. dpumesh의 cap (45K RPS)은 **per-request CPU 작업 + k8s/TCP/Thrift overhead**가 결정.
