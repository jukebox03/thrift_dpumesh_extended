# DPUmesh — 현재 구현 상태 (2026-07-03)

> 무엇이 구현되어 있는지의 스냅샷. 성능 수치의 근거는 `bench/scale_log.md`,
> API 사용법은 `api.md`, 미래 L7 파이프라인 청사진은 `prev/architecture.md`.
> 해결 방식이 미정인 항목은 사실만 기록한다 (설계는 별도 결정).

## 전송 코어 (host: dpm.h + dpumesh_doca.c)

- 핸들 2개: **channel**(프로세스당 1, DPU-assigned pod_id, service_id 광고) +
  **conn**(cheap, client=service 주소 / server=accept).
- **non-blocking 전용**: 단일 event fd + PE-published **ready list** (per-conn fd/스캔
  없음), conn별 inbox SPSC(256), `write`(버퍼)→`flush`(발사) 분리.
- 메시지 = slot ≤8KB 원자 단위. **>8KB는 auto-chunk** + route-affinity로 재조립.
  앱이 작은 논리 메시지들을 한 slot에 패킹(write×k + flush 1회) 가능 — 수신은
  rx 커서로 잘라 읽고 앱이 프레이밍.
- FIN = 0-len graceful close (peer read()==0; DPU upstream 회수). 동시 close 안전.
- zero-copy TX: `dmesh_alloc`/`dmesh_slot_register` (contiguous arena, 기본 0 =
  bit-identical). lock-free TX slot pool + per-conn custody(TX_ACK 해제).
- **K=2 forward ring conn-sharding** (`src_port % K`) → conn별 FIFO (한 메시지의
  청크는 head-first로 ARM 도착).
- 이벤트 드리븐: HOST_EPOLL=1 (PE 스레드 sleep), idle CPU ~0.

## 라우팅 (Model B: DPU가 모든 연결 소유)

- 클라이언트는 **service만 주소 지정** (모든 메시지 dst_pod=BLANK) → DPU가
  **slot(=wire 메시지) 단위로** 라우팅 결정 1회, upstream `uP` 소유, 응답 역매핑.
  라우팅 granularity = **slot(flush 단위)** — 패킹된 slot은 한 단위로 라우팅.
- 운영 정책 = mock: `service_table[svc]` 서비스당 backend 1개. 다중-backend는
  test knob `DPUMESH_LB_RR`(rg≠0 트래픽 한정 RR)로만 행사.
- **route-affinity**: `(dst_service, route_group)`→backend pin 테이블(ARM, 무락,
  overwrite-on-reuse). ① >8KB 청크 재조립, ② `dmesh_pin_route`(연결 pin — preload가
  사용). 서비스 스코프라 id 재사용이 cross-service로 새지 않음 (07-03 픽스,
  exact-count 검증).

## DPU

- 단일 ARM worker + DPA EU×4 (K=2 rings/pod). `dma_copy` ≤8KB/op, **in-place
  forwarding — DPU/DPA body memcpy 0** (staging이 reverse 소스). 제어경로 배칭:
  BATCH_FWD_ACK / BATCH_REV_DONE. EVENT_LOOP=1 (epoll 기반 ARM 루프).
- **L7-readiness 훅 (07-03)**: `objs->route_fn` — BLANK-dst 데이터 메시지에서 기본
  L4 라우팅 **전에** 디스패치. body를 staging에서 **read-only**로 읽을 수 있음
  (완료-후-데이터 순서 성립, byte-정확 실증). 반환 = 임의 live pod(cross-service
  허용) / DROP(-1, drop+TX_ACK) / DEFER(-2, L4 폴스루). **NULL(운영 기본) =
  bit-identical.** FIN/응답은 훅 미경유. test knob `DPUMESH_L7_DEMO="svc[,svc…]"`
  (body[0]%n → 서비스). **L7 proxy 본체(파서/정책)는 미구현.**

## LD_PRELOAD shim (dmesh_preload.c)

- 무수정 POSIX socket 앱을 DPUmesh 위에서 실행. **fd 실체화**: 비공개 eventfd를
  `dup2`로 앱 fd에 덮음 → epoll/poll/select/close/dup 인터포즈 불필요.
- **dispatcher 스레드** = 채널의 단일 accept/next_ready 소비자 + **유일한
  dmesh_close 실행자** (앱 close는 큐잉). accept 시 conn efd 선-assert.
- byte-stream 시맨틱(short read), send 호출당 1 메시지(writev/sendmsg는 gather로
  1 메시지), blocking 에뮬레이션(SO_RCVTIMEO = 호출 전체 데드라인), 모든 client
  conn을 pin → 연결 단위 LB(전순서).
- env: `DMESH_PRELOAD_LISTEN/_SVC/_MAP/_DEBUG`. 한계(v1): AF_INET SOCK_STREAM,
  fork 불가, stdio(FILE*)/raw-syscall(Go) 우회, half-close 근사.

## 성능 (실측; config: DPA=4 K=2 EVENT_LOOP=1 HOST_EPOLL=1 NUM_SLOTS=4096 ARENA=512)

- native RPC 8KB: **200K 램프 198.9K 0-fail** (warm ramp 필수 — cold-jump는 링 wedge).
- preload 1KB: **plateau ~150K @64conns** (native의 ~76%), 128c 클린.
- large 16/32/64KB: 0-fail, ~124-149MB/s (호스트↔DPU DMA-BW 영역).
- 천장 = **DPA dma_copy op-rate** (~1.6M ops/s 공유; 8KB 기준 257K/pair). op당
  고정 비용 → 작은 메시지는 8KB 패킹이 경제적 (그래서 라우팅 단위=slot 긴장 존재).

## 테스트 하네스

- `test-bench.sh` 단일 진입: deploy / dpumesh(-large,-pipeline,-oneway,-hw*) /
  loopback / preload / tcp / dpulog / pin.
- tcp_client = vanilla thread-per-conn + SO_RCVTIMEO 5s (유실 = 카운트되는 실패,
  RUN 행 불가 — tripwire).
- discriminator 기법: loopback served 카운터로 exact-count 검증 (echo-마스킹 우회).

## 미구현 / 열린 항목 (사실만; 해결 방식은 추후 결정)

- **L7 proxy 본체** — 파서/정책/멀티스레드 파이프라인 (청사진 `prev/architecture.md`).
- **slot 내 sub-message 라우팅·집계** — scatter-gather DMA 방향, 설계는 사용자 결정.
- **DPU 죽은-연결 pod-slot 회수** — 비정상 종료 시 comch disconnect 이벤트 미발생
  → 슬롯 미회수 (MAX_PODS=8), full deploy로만 초기화.
- thread-per-conn **서버** 심화 검증 미완 (클라이언트는 256스레드까지 검증).
- 실제 앱(nginx/redis류) 포팅 시험 미실시.
- 256-conn 폭주 1회성 유실 — 미재현(×23), tripwire 장착, OPEN.
- upstream-churn 천장 저하 — pre-existing, 재배포/저부하로 회복.
