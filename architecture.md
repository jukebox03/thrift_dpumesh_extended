# DPUmesh — 계획(Target) 멀티스레드 아키텍처

> **상태: 계획(PLAN), 미구현.** 현재 코드는 단일 ARM funnel + host가 dst 지정(relay)이다.
> 이 문서는 "DPU가 dst를 계산하는 진짜 L7 라우터 + lock-free 멀티스레드"로 가는 목표 구조다.
> 작성: 2026-06-08. 기존 현행 분석은 `architecture/dpumesh_architecture.md` 참고.
>
> ⚠️ **착수 전 측정 필수.** 천장 원인이 (a2) 단일 ARM 직렬화인지 (b) 왕복 latency인지 아직
> 미측정이다. 이전 판들이 측정 없이 단정했다 3번 뒤집혔다. 이 구조는 (a2)·(b) 둘 다 건드리지만,
> 효과 검증은 `comp_queue dwell + ARM 점유 측정` 후에 한다(§10).

---

## 0. 목표

1. **DPU를 진짜 L7 라우터로**: 목적지 pod을 host가 박지 않고 **DPU가 요청 body/헤더를 파싱해 계산**한다.
2. **천장 돌파**: 현재 2-pod 104K RPS(=416K dma_copy/s, EU 37%만 사용)의 단일 ARM funnel을
   **lock-free 파이프라인**으로 분해해 제어평면 병렬화.
3. **zero-copy 유지**: bulk body는 DPU/DPA에서 복사 없이 dma_copy + in-place만.

---

## 1. 현재 vs 계획 (델타)

| 항목 | 현재 | 계획 |
|---|---|---|
| dst 결정 | host가 desc에 박음 (relay) | **DPU가 body로 계산 (L7)** |
| 제어평면(DPU ARM) | 단일 worker funnel | **ROUTER(per-src) → N² SPSC → WORKER(per-dst) → SENDER** |
| reverse ring writer | 단일 ARM | **WORKER(dst) 단독** (single-writer 유지) |
| 데이터평면(DPA EU) | pod%N, 양방향 | **pod%N (동일, 변경 없음)** |
| host→host direct DMA | 보류 | **포기** (L7가 DPU에서 body를 봐야 하므로) |
| body memcpy | host TX 1회 | host TX 1회 (동일), DPU/DPA 0 |

---

## 2. 핵심 설계 원칙 — "shuffle 문제"

```
입력은 src로 갈려 들어오고(HW: comch 채널이 EU/src별, 1c/1p)
   ↓ 그런데 dst는 요청 내용으로 DPU가 *계산*해야 알 수 있다 (src로는 모름)
출력은 dst로 갈려 나가야 한다(reverse ring을 dst별 single-writer로 두려면)
```

→ **입력 분할축(src) ≠ 출력 분할축(dst)**, 중간에 dst가 정해짐 → **src→dst 재분배(shuffle)가 필수**.
이 shuffle을 **(src,dst) 쌍마다 SPSC 1개 = N² 매트릭스**로 깔면 모든 핸드오프가 단일생산자·단일소비자가
되어 **lock-free**가 된다. 유일하게 안 쪼개지는 곳은 host 전송(egress, 단일 cc_server) 하나뿐.

핵심 규칙:
- **lock-free 핸드오프 = SPSC** (양끝이 각각 1개). 한쪽이라도 여럿이면 락/CAS 필요.
- reverse ring을 **dst별 single-writer**로 유지하려면 → 쓰는 주체가 dst별 WORKER 하나여야 함.
- comch 채널은 N²로 못 만든다(HW recv-task 한계) → 채널은 src별 N개, **shuffle은 DPU 메모리 SPSC로**.

---

## 3. Thread 배정

### DPA (데이터평면) — **변경 없음**
- EU N개 (`DPUMESH_DPA_THREADS`). pod P의 forward·reverse 링 둘 다 **EU = `P % N`**.
- EU(k) = "pod k 전담 DMA 엔진": pod k가 **보낼 때 forward DMA**, **받을 때 reverse DMA**.
- 2-pod → 활성 EU 2개. (활성 EU = 서로 다른 pod 수, ≤ MAX_DPA_RINGS=8.)

### DPU ARM (제어평면) — **단일 funnel → 파이프라인**
- **controller(k) × N** (pod별). 각자 router+worker 겸함, SPSC로 분리되어 lock-free:
  - **[ROUTER 역할]** EU k 채널(consumer_pe_k) drain → `FWD_DONE`의 body를 src DPU 버퍼에서 읽어
    **★dst 계산(L7, 무거움)★** → `SPSC[k][dst]`에 push.
  - **[WORKER 역할]** `SPSC[*][k]`(자기 열) drain → `find_pod(k)` → pod k의 **tx_ring에 reverse desc
    post**(단독 writer, in-place 소스 지정).
  - EU k 채널의 `REV_DONE`(dst=k) → SENDER로 핸드오프.
- **sender × 1**: `cc_server` 단독 submit (DMA_COMPLETION → dst host, TX_ACK → src host).
  모든 controller가 send-SPSC로 던짐.

> 변형: ROUTER와 WORKER를 **별도 스레드(2N개)**로 분리하면 무거운 라우팅이 posting을 굶기지 않음.
> 단 2N > 8코어가 되면 oversubscribe. 기본은 controller 겸합(N개) + sender(1). **§10에서 측정으로 결정.**

스레드 수: 2-pod 기준 controller 2 + sender 1 = **ARM 3스레드**.

---

## 4. Ring / Buffer 구조

| 자원 | 위치 | 생산자 → 소비자 | 계획 변화 |
|---|---|---|---|
| forward ring (`dma_ring`, 2048 desc + 1 credit) | **host 메모리** | host → EU(src%N) | desc에 **최종 pod 대신 라우팅 입력(service_id 등)** |
| forward 데이터 버퍼 (`dma_buffer`/`local_mmap`) | **DPU 메모리** | forward DMA 적재; **ROUTER가 body 읽음** | 변경 없음 |
| reverse ring (`tx_ring`, 2048 desc) | **DPU 메모리** | **WORKER(dst) → EU(dst%N)** | writer가 단일 ARM → **WORKER(dst) 단독** |
| host RX 버퍼 (`host_rx`) | **host 메모리** | reverse DMA 적재 → 앱 in-place read | 변경 없음 |
| **N² SPSC 매트릭스** (신규) | **DPU 메모리** | ROUTER(src) → WORKER(dst) | **신규** |
| send SPSC (신규) | **DPU 메모리** | controller → SENDER | **신규** |
| ~~DPU TX 데이터 버퍼~~ | — | — | **제거됨**(in-place, 이미 삭제) |

흐름제어(유지): end-to-end 슬롯 기반. host가 TX 슬롯을 RTT 내내 점유. DPA reverse **admission gate**
(`dpa_sent_count[e][r] − cached_freed ≥ dst rq_depth`)로 dst host RX RQ 포화 방지.

---

## 5. N² SPSC shuffle

```
                       (DPU가 계산한) dst0   dst1   dst2
   ROUTER(src0) ──►       [SPSC 00]  [SPSC 01]  [SPSC 02]
   ROUTER(src1) ──►       [SPSC 10]  [SPSC 11]  [SPSC 12]
   ROUTER(src2) ──►       [SPSC 20]  [SPSC 21]  [SPSC 22]
                              │          │          │
                          WORKER0     WORKER1    WORKER2   (dst별)
```
- `SPSC[src][dst]`: **생산자 1개**(ROUTER src) + **소비자 1개**(WORKER dst) → 락 없음.
- ROUTER(src)는 **자기 행**(`SPSC[src][*]`)에만 씀; 계산된 dst에 따라 행 안의 칸을 고름.
- WORKER(dst)는 **자기 열**(`SPSC[*][dst]`)만 읽음.
- N²인 이유: 양끝을 각각 1개로 고정하려면 (src,dst) 모든 쌍에 SPSC가 필요. pod ≤8 → ≤64칸(메모리상 작음).
- comch 채널은 N²로 못 함(HW) → 채널은 src별 N개로 두고, 위 N²는 **DPU 내부 메모리 ring**.

---

## 6. 데이터 흐름 (src → dst 1회 전달)

```
① 앱 body → src host TX 버퍼                         (memcpy ★유일한 body 복사)
② src host가 forward ring에 desc 게시                (라우팅 입력 service_id 등; ※최종 dst 없음)
③ EU(src%N): forward DMA  [src host TX → src DPU 버퍼]  → FWD_DONE(dst 없음) on src 채널
④ controller(src)=ROUTER: src DPU 버퍼에서 body 읽어 ★dst 계산(L7)★ → SPSC[src][dst] push
⑤ controller(dst)=WORKER: SPSC[*][dst] pop → find_pod(dst)
                          → dst tx_ring에 reverse desc post (desc.mmap = src DPU 버퍼 = in-place 소스)
⑥ EU(dst%N): reverse DMA  [src DPU 버퍼 → dst host RX]  → REV_DONE
⑦ SENDER: DMA_COMPLETION → dst host,  TX_ACK → src host
⑧ dst 앱: RX 버퍼에서 body in-place read (복사 없음)
```
- echo 벤치 1 RTT = ②~⑧ **두 번**(요청+응답) = dma_copy 4번, DPU/DPA memcpy 0.
- ④에서 dst가 **처음 알려짐**(그 전엔 누구도 모름). 그래서 분배(shuffle)는 라우팅 *뒤*.

---

## 7. 전체 구조 (한 장)

```
   HOST (pods)                 DPU ARM (제어평면)                      DPA (데이터평면)
 ┌──────────┐  forward ring   ┌───────────────────────────┐        ┌──────────────┐
 │ src app  │───(service_id)──┼──────────────────────────────────►│ EU(src%N)    │ fwd DMA
 │ TX buf   │                 │                              완료  │  host→DPU buf │
 └──────────┘                 │  ROUTER(src): body→dst 계산 ◄──────┤              │
                              │      │ SPSC[src][dst]              └──────────────┘
                              │      ▼ (N² shuffle, lock-free)
                              │  WORKER(dst): reverse desc post ──► dst tx_ring ──►┌──────────────┐
                              │      │                                             │ EU(dst%N)    │ rev DMA
                              │      ▼ send SPSC                          REV_DONE │  DPU buf→host│
 ┌──────────┐                │  SENDER(1): cc_server  ──完료/ACK──────────────────┤  (in-place)  │
 │ dst app  │◄───host RX 버퍼─┼──────────────────────────◄ reverse DMA ───────────└──────────────┘
 │ RX buf   │ (in-place read) └───────────────────────────┘
 └──────────┘
   controller(k) = ROUTER(k) + WORKER(k) 겸함.  락은 egress(cc_server, SENDER 단독)에만 직렬.
```

---

## 8. memcpy / zero-copy 속성

- **DPU/DPA: body memcpy 0.** dma_copy 4번 + in-place(reverse가 src DPU 버퍼 직접 읽음).
  ROUTER는 body를 **읽기만**(파싱) → 복사 아님.
- **host TX 1 copy** (앱 body → 핀 슬롯, `dpumesh_doca.c:248`). 수신은 in-place read(복사 0).
- ⚠️ 라우팅이 **헤더 rewrite**까지 하면 → DPU 버퍼 **in-place 쓰기** 추가(전체 copy 아님). 순수 dst 결정이면 copy 0.
- **in-place가 유지되는 이유 = host→host 포기.** L7는 DPU가 body를 봐야 하므로 DPU-staging 필수,
  그게 곧 reverse in-place(zero-copy)를 성립시킴.

---

## 9. 제약 / foreclosure

| 제약 | 내용 |
|---|---|
| **host→host 포기** | L7 라우팅이 DPU에서 body를 읽어야 함 → DPU-staging 필수 → host→host와 양립 불가 |
| **comch 1c/1p** (`dpa.c:470,477`) | 채널을 N²로 못 만듦(HW recv-task 한계) → 채널 src별 N개 + 내부 N² 메모리 shuffle |
| **egress 단일** | `cc_server` ctx 1개 → 여러 스레드 동시 submit 불가 → SENDER 1개로 모음(락 회피). send는 비병목 |
| **활성 EU = pod 수 (≤8)** | `pod%N`, MAX_DPA_RINGS=8. >8 pod이면 EU 공유(불균형) |
| **comp_queue / recv 풀** | 단일 funnel을 쪼개면 **per-controller comp_queue**로 만들어야 ≥4 EU overflow·recv-pool 결합(B1) 동시 해소 (`project_recv_pool_coupling`) |
| **reverse fan-in** | 한 dst에 여러 src 수렴 → dst 버퍼는 src 합산(N×) 필요; dst ring은 WORKER 단독 writer로 락 회피 |

---

## 10. 미해결 / 측정 필요 (착수 전)

1. **병목 판정 (최우선):** `comp_queue dwell`(enqueue ts `dpa.c:122` vs dequeue ts `dpu_worker.c:208`, 둘 다
   host MONOTONIC) + ARM 점유율 + EU reschedule 카운터를 104K knee에서 1회 측정.
   - 단일 ARM이 포화/큐 적체 → **(a2) 직렬화** → 이 파이프라인이 직접 이득.
   - ARM 한가 + EU만 idle → **(b) 왕복 latency** → 파이프라인은 handoff 대기만 일부 줄임(이득 제한적).
   - 참고: `-O2` 중립으로 "ARM 연산 부족"은 이미 배제됨.
2. **라우팅 입력 포맷:** DPU가 무엇을 보고 dst 계산? (host가 주는 service_id 필드 + LB 테이블 vs 진짜
   body/L7 헤더 파싱). → ROUTER 비용·desc 포맷 결정. (현재 가정: 무거운 body 파싱.)
3. **controller 겸합 vs 분리:** ROUTER+WORKER 한 스레드(N) vs 분리(2N). 라우팅 무게/코어 수로 결정.
4. **헤더 rewrite 여부:** 있으면 in-place write 추가.
5. **per-controller comp_queue / recv 풀 재설계** (B1·overflow 동시 해결).

---

## 11. 변경 대상 코드 (착수 시)

| 영역 | 파일:위치 | 변경 |
|---|---|---|
| dst 결정 이동 | host: `dpumesh_doca.c`·`TDpumeshClientTransport.cpp` / DPU: `dpu_worker.c` `process_forward_entry` | host가 최종 pod 대신 라우팅 입력 전송; DPU가 body 읽어 dst 계산 |
| 제어평면 파이프라인 | `dpu_worker.c` `run_dpu_worker`/`process_completion_queue` | 단일 loop → controller(N) + sender(1) |
| consumer_pe 샤딩 | `comch_msgq.c:37`, `dpa.c` | 단일 consumer_pe → per-EU consumer_pe ("consumer k → ARM thread k", `object.h:341` 주석) |
| N² SPSC | `object.h` (신규 타입) | `send_spsc_t`/`work_spsc_t` 패턴 재사용해 N² 매트릭스 |
| reverse post writer | `dpu_worker.c` `dpu_enqueue_reverse_dma` | 단일 ARM → WORKER(dst) 단독 |
| comp_queue/recv 풀 | `object.h`·`comch_consumer.c`·`dpu_worker.c:737` | per-controller 분리 (`project_recv_pool_coupling`) |
| DPA EU | (변경 없음) | `pod%N` 유지 (`dpa.c:1084`) |

---

## 부록 — 한 줄 요약

**DPA(EU)는 그대로(pod%N). DPU(ARM)만 "단일 funnel" → "ROUTER(per-src, body로 dst 계산) → N² SPSC
shuffle → WORKER(per-dst, reverse post) → SENDER(단일 egress)" lock-free 파이프라인으로 바꾼다.
body 복사는 host TX 1번뿐(DPU/DPA zero-copy). L7 때문에 host→host는 포기. 착수 전 병목(직렬화 vs
latency)을 측정으로 먼저 확정한다.**
