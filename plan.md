# DPUmesh — DPU 위의 L7 프록시 (envoy) 계획 (2026-07-03)

> 목표: **DPU를 envoy 같은 L7 프록시로 만든다.**
> - **L7 (파싱 + 라우팅/LB)** = 나중에 DPU에 구현. 지금은 **mock**.
> - **L4 (dpumesh, 지금 구현)** = L7이 준 라우팅 결과대로 **scatter-gather DMA로 실어나르기.**
> 지금 정할 핵심 = **proxy 함수가 돌려줄 라우팅 결과 형식** + 그 결과로 동작하는 **나머지 전부(L4)**.
> 성능 근거는 `bench/scale_log.md`, 현재 전송 API는 `api.md`.

## 큰 그림

```
client --(byte-stream)--> DPU[ L7 파싱+라우팅(mock) → L4 SG-DMA ] --(byte-stream)--> backend
                                                                    <--(응답 되돌림)--
```

- client는 **byte-stream**을 보낸다 (메시지 경계는 전송이 모름).
- DPU의 **L7 로직(envoy, 나중·mock)** 이 그 바이트를 파싱해서 "어느 조각을 어느 backend로" 결정한다.
- **L4(dpumesh)** 는 그 결정대로 조각들을 목적지별로 모아 **SG-DMA**로 보낸다.
- backend는 그냥 byte-stream을 받아 **스스로 파싱**한다 (envoy 뒤의 평범한 서버). 응답은 반대로 되돌린다.
- 파싱은 L7의 책임, 전송은 L4의 책임 — 명확히 분리.
- **요청도 응답도 둘 다 proxy를 거친다 (대칭 — 필수).** 요청은 proxy가 목적지를 **정하고**, 응답은 목적지가 이미 정해져 있어(그 요청이 만든 upstream) proxy가 **확인만** 하고 전달한다. (응답이 proxy를 거쳐야 나중에 envoy가 응답도 처리 가능.)

## proxy 인터페이스 (지금 정할 것)

L7 로직(=proxy 함수)이 한 연결의 입력 바이트를 보고 라우팅 결과를 돌려준다:

```c
struct dmesh_route_seg {
    uint32_t off;    // 이 연결 입력 바이트 안에서의 시작 위치
    uint32_t len;    // 조각 길이
    int32_t  dst;    // 목적지 backend (envoy가 LB까지 끝낸 결과)
};

// 지금은 mock, 나중에 envoy가 채움. **요청·응답 양방향 모두 이 함수를 거친다.**
int proxy_route(dmesh_conn *conn, const uint8_t *buf, uint32_t avail,
                dmesh_route_seg *segs, int max, uint32_t *consumed);
//  conn      : 자기 역할/peer를 앎 (요청 conn이냐, 응답이 오는 upstream이냐)
//  buf/avail : 그 연결의 (순서대로 모인) 입력 바이트
//  segs/리턴 : 보낼 조각 {off,len,dst} 리스트 + 개수  (음수=에러/드롭)
//  consumed  : 파서가 완전히 처리한 바이트 수 (나머지 = 미완성 → 다음 호출에)
```

L4의 실행: 각 seg를 `buf+off`에서 `len`만큼 모아 `dst`의 upstream으로 SG-DMA.
연결 커서를 `consumed`만큼 전진, 안 쓴 꼬리는 보관.

**형식 세부 3개 (결정됨):**
1. `dst` = **구체 destination pod** (DPU에선 항상 구체 pod). ✔
2. 위치 = **창 기준 `off`** 기본. `slot_id`는 **구현하면서 필요하면 추가**, 아니면 생략 (구현 의존).
3. **응답도 proxy를 거친다 (대칭 — 필수).** 단 응답의 dst는 이미 정해져 있음(그 요청이 만든 upstream이 어느 client 건지 앎) → proxy는 **확인하고 그대로 전달.** 지금 mock은 pass-through(dst=peer)지만 **경로는 반드시 proxy 통과** — 나중에 envoy가 응답도 처리할 수 있어야 하므로.

## L4가 하는 일 (dpumesh, 이번에 구현)

1. **연결별 입력 창** — 한 연결의 바이트를 순서대로 모아 proxy에 연속으로 보여준다. proxy가 `consumed`로 소비한 만큼 커서 전진, 안 쓴 꼬리는 다음까지 보관 (staging이 감길 때 꼬리만 정렬).
2. **seg 실행 (SG-DMA)** — 목적지 upstream(uP)별로 조각들을 모아 **SG-DMA 한 방 + 알림 한 번.** (측정됨: DPU→host SG-DMA 정상, 64조각까지.)
3. **바이트 붙잡기 / 반납** — SG-DMA가 소스 바이트를 다 읽을 때까지 붙잡고, 완료 시 모아서 송신자에게 반납(TX_ACK). **먼저 반납 금지 — 소스가 덮어써져 깨짐.**
4. **응답 경로 (요청과 대칭)** — backend 응답도 **같은 machinery**(입력 창 → `proxy_route` → seg 실행 → SG-DMA)를 탄다. 단 dst는 새로 정하는 게 아니라 **DPU 연결 테이블(conntrack)에서 lookup**: 요청 라우팅 때 만든 `uP → (client_pod, client_port)` 항목을 응답이 돌아온 uP로 조회 → 그 client로. proxy는 응답 바이트를 보되(대칭·나중 처리용) 목적지는 **테이블이 준다**(proxy는 확인만). 테이블은 **L4 소유.**
5. **받는 쪽 전달** — backend에 **byte-stream**(있는 만큼 read)으로 준다. 메시지 단위 아님 (backend가 스스로 파싱).

## 지금 있는 것 (기반 — 재사용)

- **전송 코어** (`dpm.h` + `dpumesh_doca.c`): 프로세스당 채널 1개 + 연결(conn), non-blocking, 단일 event fd + ready-list, per-conn inbox, write/flush.
- **Model B 라우팅** (`dpu_worker.c`): DPU가 모든 연결 소유. client는 service만 지정, DPU가 backend 결정 + upstream(uP) 소유 + 응답 역매핑. 현재 `route_fn` 훅은 slot 단위(→ `proxy_route`로 대체 예정).
- **SG-DMA (측정 완료)**: ARM generic doca_dma의 chained-buf gather. DPU→host ~41 GB/s, 64조각. 이게 seg 실행의 엔진. (주의: 하드웨어 gotcha — dst 버퍼는 재사용 전 `doca_buf_reset_data_len`, submit 사이 `doca_pe_progress` 인터리브.)
- **DPU/DPA**: 단일 ARM + DPA EU×4. forward(host→staging)는 DPA가 담당. staging = ARM이 파싱용으로 읽는 곳.
- **LD_PRELOAD shim**, 연결 테이블, 알림(comch immediate), custody 구조 — 재사용.

## 이번에 새로 만드는 것 (신규)

- 연결별 **입력 창 + 커서** (순서대로 모으기, 미완성 꼬리 보관).
- **`proxy_route` 인터페이스** (현재 per-slot `route_fn` 대체) + 그 return 형식.
- 목적지(uP)별 **SG-DMA gather** + **완료-시-배치 반납** (붙잡기 유지, 먼저 반납 금지).
- **byte-stream 전달/응답** (받는 쪽 read = 있는 만큼).
- **mock `proxy_route`** + **byte-stream 테스트 앱**.

## 구현 상태 (2026-07-04 — frame 경로 HW 검증 통과)

**작성됨 (host `gcc -fsyntax-only` 통과 + frame 경로 HW 검증):**
- `doca/dpu_proxy.h` — `proxy_route` 인터페이스: `dmesh_route_seg{off,len,dst}` + `consumed` 계약, `DMESH_SEG_DST_DEFER`(L4 기본 라우팅 위임), conn 뷰(역할/peer/user).
- `doca/dpu_proxy.c` (~950줄) — L4 엔진 본체:
  - 연결별 입력 창 (staging 위 zero-copy view + 커서; 파서가 조각 경계에서 막힐 때만 꼬리를 seam 버퍼로 정렬)
  - seg 실행: 목적지(pod,region)별 lane → ARM generic doca_dma **chained-buf SG 1 op** + 배치 알림(REV_DONE ≤8KB 단위), 수신 conn별 순서 = lane FIFO + 제출순 회수
  - custody: 도착분별 바이트 카운트다운, **egress 완료 시 배치 TX_ACK** (먼저 반납 없음)
  - 응답 대칭 경로: 같은 machinery, dst는 conntrack(uP→client) lookup — proxy는 확인만
  - FIN: 창 소진 후 lane에 실려 데이터 뒤에 도착; client FIN = upstream fan-out 해체(기존 의미 유지)
  - admission: host RX 크레딧(host freed 카운터)을 ARM이 lazy DMA-read (DPA 방식의 ARM판)
  - mock 2개: `passthru`(도착 메시지당 seg 1개, 기존 라우팅/경계와 동일 — 회귀용), `frame`(길이-접두 프레이밍 + svc 바이트로 목적지 — plan 테스트 절 그대로)
- `object.h` — `objs->proxy` 포인터, pod별 `ring_host_addrs[]`(크레딧 슬롯 host VA).
- 활성화: env `DPUMESH_PROXY=passthru|frame`. **미설정 = 기존 경로 bit-identical** (엔진 미생성).

**배선 완료 (2026-07-03):**
1. ✅ **배선** — `dpu_worker.c`: `px_init` 호출(init step 7, dev+pe live 후), `process_completion_queue`의 forward 완료 → `objs->proxy`면 `px_ingest_forward` 분기, `dpu_drain_iteration`에 `px_drain` 훅(반환값·idle flush에 반영), `dpu_route_l4` 분리(dpu_route는 이걸 호출), `batch_or_send_tx_ack`/`flush_rev_done_batch` un-static + `dpu_worker.h` export. `comch_common.c`: DMA_RING import 시 `ring_host_addrs[idx]=remote_addr` 저장(egress admission의 host freed 카운터 주소). `meson.build`: `doca-dma` 의존성 + `dpu_proxy.c`.
2. ✅ **문법 체크** — `dpu_proxy.c`·`comch_common.c` 0 warning, `dpu_worker.c` 신규 warning 0(기존 dpa.h implicit-decl 5개는 standalone-gcc 아티팩트, meson 빌드엔 없음).
3. ✅ **byte-stream 테스트 앱** — `bench/stream_sock.c`(loopback형 self-service; `[u32 len][u8 svc][payload]` 프레임 송신 → DPU frame mock이 reframe+라우팅, 바이트-정확 echo + served-byte exact-count; `RUN <N> <SIZE> [<SVC_LIST>] [<FPW>]` — SVC_LIST로 fan-out, FPW로 multi-frame-per-window, 큰 프레임으로 seam 자극), `Dockerfile.stream_dpumesh`, `test-bench.sh` 배선(`DPUMESH_PROXY` env 전달, `stream` 커맨드+`run_stream`, k8s deploy(svc 16, replicas 0, on-demand scale), 이미지/빌드/logs/pin).

**HW 검증 — frame 경로 통과 (2026-07-04):** `DPUMESH_PROXY=frame` 배포 후 `./test-bench.sh stream`으로 4종 전부 **바이트-정확(ok==N, fail==0), DPU 로그 drop/poison 0**:
- self 1KB: 30/0, served 30870=30×1029, p50 114µs
- seam >8KB: `stream 200 20000` → 200/0, served Δ=200×20005 정확 (다중 arrival 창+seam+다중 piece SG-DMA), p50 169µs
- multi-frame/write: `stream 2000 512 self 4` → 2000/0, served Δ=2000×4×517 정확, p50 114µs
- fan-out: `stream 1000 512 11,13,14` → 1000/0, served FLAT(백엔드 11/13/14에서 echo — per-backend upstream + reply 매핑), p50 202µs

검증 중 하네스 버그 2개 수정: (1) 빈 SVC_LIST가 sscanf에서 접혀 fpw→svc 밀림 → bash "self" placeholder + 데몬 "self" 매핑 + 연속실패 early-abort. (2) `run_stream`이 매번 pod 재시작 → 재시작 반복이 DPU EU-0 msgq wedge(기존 teardown 취약성, proxy 아님) → `run_stream` 멱등화(떠 있는 pod 재사용). fresh 재배포로 회복.

**남은 일 (사용자):**
4. 잔여 검증: ① proxy off 회귀 = baseline ② `DPUMESH_PROXY=passthru`로 기존 스위트 parity ③ fan-out 목적지-정확을 echo recv_total로 exact-count(현재는 byte-정확+served-flat으로 간접 확인) ④ 리스크 ①(L4 CPU)·notify 비용 측정.
5. **문서 갱신** (검증 후): api.md §5 proxy seam, 본 plan 정리, scale_log 기록.

**열린 결정 (사용자):** 입력 창 방식 — **zero-copy**(현재 코드: staging에서 직접 gather, ACK-at-egress, seam 필요) vs **연결별 복사**(단순하지만 ACK 시점이 복사로 당겨지고 창-가득참의 head-of-line 처리가 새로 필요). 현재 코드는 zero-copy.

## 테스트 (proxy는 mock)

- mock `proxy_route`가 결정적 규칙으로 조각을 나눠 라우팅 (예: 길이-접두 프레이밍을 mock이 이해, 또는 첫 바이트로 목적지 선택).
- byte-stream 클라이언트/서버로 왕복 → **byte-정확 + 목적지-정확** 검증 (현 loopback served-counter exact-count 방식 재사용).
- 부하는 여러 backend로 흩어진(high fan-out) 경우도 포함.

## 정할 것 / 열린 리스크

- **결정됨:** `dst`=구체 pod / 위치=`off`(slot_id는 구현 의존) / 응답도 proxy 경유(대칭, dst는 확인만).
- **측정할 리스크 ①:** L4 자기 몫 CPU(모으기 + DMA 발행)가 가벼운지. (무거운 파싱은 L7/envoy 몫이라 L4 걱정 아님.)
- **측정할 리스크 ②:** 순서 — 한 연결은 지금도 링 1개로 가서 순서 보장됨. 그대로 유지하면 파서 입력이 순서대로 온다.
- **주의:** 이건 처리량(200K)을 올리는 작업이 아니다 (그 천장은 host 수신 쪽). 목적은 **L7 프록시 기능 + DPU CPU 절약 + 지연 감소.**

---

## 이전 전송 계층의 미구현/열린 항목 (참고, 별개)

- Thrift 트랜스포트 래퍼(`TDpumesh*`) 재작성 — 옛 API 참조로 死+빌드제외.
- DPU 죽은-연결 pod-slot 회수 — 비정상 종료 시 회수 안 됨 (MAX_PODS=8), full deploy로만 초기화.
- 256-conn 폭주 1회성 유실 — 미재현, tripwire 장착, OPEN.
- recv-pool 커플링(잠재), upstream-churn 천장 저하 — pre-existing.
