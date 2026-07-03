# LD_PRELOAD socket 호환층 — 계획 (2026-07-03)

> **STATUS (2026-07-03): 구현 + HW 검증 완료.** vanilla TCP 앱(tcp_echo/tcp_client,
> dmesh 헤더 0줄)이 shim으로 DPUmesh 위에서 0-fail 동작: 1KB×5000/8conns,
> 8KB×20000/16conns, 32KB×3000(auto-chunk+pin), back-to-back 안정, p50 ~120–150µs.
> native 회귀 무영향(30K/100K/200K 램프 + loopback 전부 0-fail, 200K=198.9K 베이스라인).
> 상세 수치/버그 이력은 bench/scale_log.md 2026-07-03 섹션, API 문서는 api.md §7.
> 잔여 후속 항목: ①DPU 죽은-연결 슬롯 회수(아래 리스크), ②thread-per-conn **서버**
> 심화 검증(blocking + thread-per-conn **클라이언트**는 256스레드×256conn까지 검증
> 완료 — scale_log 07-03 session 3), ③실제 앱(nginx/redis류) 포팅 시험,
> ④256-conn 폭주 1회성 메시지 유실(미재현 ×23, tripwire 장착 — 아래 리스크).
> (완료) 한계 실측 — 1KB plateau ~150K RPS@64c = native의 ~76% ("절반" 예상 상회);
> tcp_client는 thread-per-conn + SO_RCVTIMEO 5s(유실=카운트되는 실패, 행 불가) +
> RESULT에 RPS 필드. 상세 scale_log 07-03 session 3.
> (완료) 2026-07-03 코드리뷰 수정분 배포+검증 — route_group pin 테이블
> (service, id) 스코프(**DPU 변경**; 종전 전역 테이블은 id 재사용 시 타 서비스
> backend로 오배송 가능, echo-only 검증이라 미검출 → served-카운터 discriminator로
> 양성 검증) + shim 하드닝(SOCK_STREAM 게이트, fd 상한, 이중 FIN 억제, listener
> close 정리, RCVTIMEO 데드라인, writev 병합, fcntl64/sendfile64). 전 테스트
> 0-fail, 상세 scale_log 07-03 session 2.

목표: **기존 최적화 API(`dmesh_*`)는 동결**한 채, 무수정 socket 앱이 `LD_PRELOAD`로
DPUmesh 위에서 돌게 하는 호환층을 얹는다. 두 API는 하나가 될 수 없다(readiness 모델,
전송 시점, 순서 보장, blocking — 특히 per-message LB는 byte-stream 전순서와 양립 불가).
따라서 업계 표준(libvma/XLIO, F-Stack)과 같은 **2층 구조**로 간다.

```
 무수정 socket 앱                     직접 작성 앱
      │ LD_PRELOAD                        │
 ┌────▼─────────────────┐                 │
 │ libdmesh_preload.so  │  ← 신규 (호환층)│
 └────┬─────────────────┘                 │
 ┌────▼─────────────────────────────────  ▼──┐
 │ dmesh_* (dpm.h — 동결, pin 1개만 추가)    │  ← 성능층
 └────────────────────────────────────────────┘
 DPU / DPA / wire ABI : 변경 없음
```

## 비목표 (v1에서 하지 않는 것)
- per-conn mmap / mmap pool — 기각 (mmap은 등록 경계지 할당 경계가 아님; 연속성이
  필요해지면 단일 mmap 안의 extent 할당자로 푼다. 분석은 대화 기록 참조)
- scatter-gather DMA / 메시지 단위 descriptor — 보류 (LB "결정"은 이미 메시지 단위;
  8KB는 `doca_dpa_dev_comch_producer_dma_copy`의 HW 한계라 DMA op 수는 안 줄고,
  이득은 >8KB 트래픽의 제어경로뿐. preload 트래픽이 대형 write를 실제로 만들면 재평가)
- POSIX 소켓 표면 전체 호환 — fork-후-소켓공유, `MSG_OOB`, 대부분의 `SO_*`는 no-op/문서화
- Go 바이너리 — Go는 libc를 우회(raw syscall)하므로 LD_PRELOAD 대상이 아님

## Phase A — 연결 단위 backend 고정 (host 전용, DPU 무변경)

socket 앱은 한 연결의 byte-stream 전순서를 가정하므로, preload 연결은 backend를
고정해야 한다(per-message LB 포기 = 연결 간 LB만). **이미 있는 route-affinity 경로를
그대로 쓴다**: `dpu_route()`는 `route_group != 0`이면 그 그룹을 최초 라우팅된 backend에
pin한다 (`dpu_worker.c` — 원래 >8KB auto-chunk 재조립용). conn이 고정 그룹 id를 *모든*
메시지에 찍으면 그 conn 전체가 한 backend에 붙는다.

- `dpm.h`: `dmesh_conn_t.pin_group` 필드 + `dmesh_pin_route(c)` (채널의 global rolling
  id에서 1..255 하나 청구). ship/flush/auto-chunk/FIN 경로가 `pin_group`을 우선 사용.
- 한계(수용): 그룹 id 공간이 255라 pinned conn끼리 그룹을 공유할 수 있음 → 같은
  backend로 몰릴 뿐 정합성 문제 없음 (dpu_route는 collision-safe). 운영 기본값
  (단일 backend service_table, LB_RR 미설정)에서는 어차피 no-op.
- DPU/DPA/wire: **변경 없음** (completion은 이미 route_group을 나름).

## Phase B — `libdmesh_preload.so` (신규: `lib/cpp/src/thrift/transport/dmesh_preload.c`)

### 활성화 (env; 그 외 소켓은 전부 커널 pass-through)
| env | 의미 |
|---|---|
| `DMESH_PRELOAD_LISTEN=<port>` | 이 TCP 포트의 listen이 dmesh 서비스 리스너가 됨 |
| `DMESH_PRELOAD_SVC=<svc>` | 이 프로세스가 광고할 service_id (LISTEN과 함께) |
| `DMESH_PRELOAD_MAP=<port>=<svc>[,...]` | 이 포트로의 connect()가 dmesh로 전환 |

### fd 실체화 (핵심 설계)
- `socket()`은 일단 진짜 TCP 소켓을 만든다(pass-through). `connect()`가 매핑된 포트로
  향하거나 매핑된 리스너의 `listen()`/`accept()`가 불리는 순간, **conn당 kernel
  eventfd를 만들어 `dup2(efd, fd)`로 fd 번호를 유지한 채 eventfd로 교체**하고
  fd→conn 테이블에 등록한다.
- 효과: `epoll_ctl`/`epoll_wait`/`poll`/`select`/`close`/`dup`은 **인터포즈 불필요**
  (진짜 fd라 커널이 처리; kernel fd와 dmesh fd가 한 epoll set에 섞여도 동작).
- 비용: per-conn-fd 세금 부활 — 이미 측정된 값 (clean ceiling ~100K vs ready-list
  ~200K; scale_log 06-30 RESULT 3/4). **호환층에 가두는 대가로 수용.**

### dispatcher 스레드 (채널당 1개; api.md의 "accept/next_ready는 단일 스레드" 계약 준수)
- 채널 event fd + 내부 wake fd를 poll → 깨어나면:
  1. `dmesh_accept()` 루프: 새 conn마다 eventfd 생성 + 내부 accept 큐에 push +
     리스너 eventfd에 신호
  2. `dmesh_next_ready()` 루프: 해당 conn의 eventfd에 write(1)
  3. close 큐 처리: `dmesh_close()`는 **dispatcher만** 수행 (ready-list pop과
     use-after-free 경합 차단; 앱의 `close()`는 표시+큐잉+실제 fd close만)

### 데이터 경로 (byte-stream 시맨틱)
- `read`/`recv`: `dmesh_read`가 이미 rx_pos 커서로 부분 읽기를 지원 → 그대로 short
  read로 노출(=TCP와 동일). 성공 read마다 eventfd 재-assert(과잉 신호는 안전),
  EAGAIN 경로에서만 eventfd drain 후 **재시도 1회**(drain 사이 도착 경합 봉합) 후
  비차단이면 EAGAIN, 차단이면 eventfd poll 후 루프. `MSG_PEEK`/`MSG_WAITALL`/
  `MSG_DONTWAIT` 지원, `SO_RCVTIMEO` poll timeout 반영.
- `write`/`send`: `dmesh_write(전체) + dmesh_flush` = write 호출당 1 메시지(>8KB는
  auto-chunk + Phase A pin으로 순서 보존). 수신측은 메시지 경계를 못 보므로(short
  read 허용) byte-stream 성립. `writev`/`readv`/`sendfile`은 루프/스테이징으로 위임.
- EOF: peer FIN → `dmesh_read()==0` → `read()==0` (TCP와 동일). `shutdown(SHUT_WR)`
  → FIN 송신 + write 차단(half-close 근사; 문서화).
- 차단 소켓: eventfd poll로 에뮬레이션. `connect`는 local이라 즉시 성공(비차단
  connect의 EINPROGRESS 댄스도 즉시성공으로 합법).
- 기타: `getpeername`/`getsockname` 합성 주소, `setsockopt` 주요 옵션 no-op 성공,
  `SO_ERROR`=0, `fcntl(O_NONBLOCK)` 자체 추적, `FIONREAD` best-effort, `dup*`는
  테이블 별칭+refcount.

### 인터포즈 심볼 (전부)
`socket connect bind listen accept accept4 read recv recvfrom recvmsg readv write
send sendto sendmsg writev close shutdown fcntl(+fcntl64) ioctl getsockopt setsockopt
getsockname getpeername dup dup2 dup3 sendfile(+sendfile64)`
— 비추적 fd는 첫 줄에서 real 함수로 즉시 위임(배열 인덱스 1회).

## Phase C — 검증 (진짜 vanilla TCP 앱으로)

새 파일 (bench/):
- `tcp_echo.c` — **순수 POSIX socket epoll echo 서버** (dmesh 헤더 include 없음).
  커널 TCP로도 그대로 도는 것이 "vanilla" 증명.
- `tcp_client.c` — 순수 blocking TCP 클라이언트 데몬(**thread-per-conn**): stdin에서
  `RUN <N> <SIZE> <CONNS>` 를 받아 fresh 연결 CONNS개를 **각자의 스레드**로 열고 N을
  분할해 단건 왕복(내용 검증), `RESULT <ok> <fail> <p50us> <p99us> <rps>` 출력.
  `SO_RCVTIMEO=5s`로 유실 = TIMEOUT stderr + 실패 카운트(행 불가; POSIX라 vanilla성
  유지). (프로세스를 유지하는 이유: 채널 1개 = DPU pod 등록 1개. RUN마다 재등록하면
  MAX_PODS=8 슬롯이 소진됨. 연결 churn은 RUN마다 발생하므로 FIN/accept/close 경로는
  매 RUN 검증됨)
- `preload_runner.c` — pod entrypoint: 부팅 시 tcp_echo(preload)와
  tcp_client(preload)를 기동해 두고, 제어 포트 9092로 `RUN N SIZE CONNS` 수신 →
  client stdin에 전달 → 결과를 `OK ...`로 응답 (loopback pod와 같은 프로토콜 모양).

배선 (test-bench.sh + Dockerfile):
- `build_bench_binaries`: libdmesh_preload.so + tcp_echo + tcp_client + preload_runner 빌드
- `Dockerfile.preload_dpumesh` + `preload-dpumesh` Deployment(service_id **15**,
  echo 프로세스 `DMESH_PRELOAD_LISTEN=9095`/`SVC=15`, client `MAP=9095=15`)
- `./test-bench.sh preload <N> <SIZE> <CONNS>` 명령 추가; pin/logs 목록에 pod 추가

검증 순서:
1. **로컬(배포 전)**: 전 파일 컴파일/링크 + tcp_echo↔tcp_client를 **커널 TCP로**
   구동(수 만 왕복, 내용검증 0-fail) → 테스트 앱 자체의 vanilla성/정확성 확정
2. 배포(deploy) 후: `preload 5000 1024 8` 스모크 → `preload 50000 8192 32` →
   대형 메시지(`SIZE=32768`, auto-chunk+pin 경로) → 반복 RUN(슬롯/uP 누수 없음 확인,
   back-to-back 정책)
3. 기존 회귀: `dpumesh 100K/200K` 래더 0-fail 유지 (dpm.h 변경이 비-pin 경로에
   영향 없음을 확인; pin_group=0이면 기존과 bit-identical 경로)

## 리스크 / 주의
- **blocking 에뮬레이션 wakeup 경합** — 위 "drain 후 재시도 1회" 패턴과 "성공 read마다
  재-assert"로 lost-wakeup을 구조적으로 차단 (과잉 wake는 허용, 소실은 불가 방향으로).
- **close vs dispatcher 경합** — dmesh_close를 dispatcher로 일원화 (위).
- **등록 슬롯**: preload pod = 프로세스 2개 = 등록 2 → 총 7/8. 여유 1.
  **HW에서 실측 확인된 공백 (2026-07-03)**: DPU는 host 프로세스가 죽어도 pod 등록
  슬롯을 회수하지 않는다 (`pods_add_connection: table full (8)` 재현 — preload pod
  재시작 1회로 즉시 소진). deploy가 DPU를 항상 함께 재시작해서 지금까지 가려져
  있던 pre-existing 라이프사이클 공백. v1 대응 = pod 재시작 회피(runner는 자식이
  죽어도 exit하지 않음) + clean deploy로 초기화. (정정: `pods_remove_connection`
  슬롯 재사용 로직은 이미 존재 — 문제는 비정상 종료 시 comch 연결 해제 이벤트가
  안 오거나 늦어 슬롯이 안 풀리는 경우. **후속 항목: 죽은 연결 감지/회수 경로
  점검 + DPA ring 배열 회수**.)
- **uP churn**: RUN마다 연결을 새로 열므로 대량 churn 시 기존의 upstream-churn 한계가
  보일 수 있음 (기지 사항; 테스트 크기 조절).
- **256-conn 폭주 1회성 유실 (OPEN, 07-03)**: 256 동시 connect 폭주 꼬리에서 ~21개
  conn의 첫 교환이 무음 유실(conn은 양쪽 성립, 전 로그/카운터 클린) → 당시 타임아웃
  없던 클라이언트가 영구 행. 이후 23×256c 폭주(원 이력 재연·back-to-back churn·유휴→
  폭주 소크) 전부 0-fail로 **미재현**. tripwire 상시 장착: client SO_RCVTIMEO(유실=
  카운트+RUN 완료) + shim accept-드롭 로그/드레인 지속. 재발 시 즉시 증거 확보됨.
  상세 scale_log 07-03 session 3.
- 성능 실측 (07-03; 예상 대체): 1KB **plateau ~150K RPS@64c** = 같은 배포 native
  199K의 ~76% ("절반 ~100K" 예상 상회). conn당 ~7K(RTT-bound), knee 32-64c, 128c는
  같은 스루풋에 지연 2배. 이 갭은 결함이 아니라 per-conn-fd 모델의 설계된 대가.

## 파일 목록 (변경/신규)
| 파일 | 종류 | 내용 |
|---|---|---|
| `lib/cpp/src/thrift/transport/dpm.h` | 수정 | `pin_group` + `dmesh_pin_route()` + ship/FIN 경로 반영 |
| `lib/cpp/src/thrift/transport/dmesh_preload.c` | 신규 | LD_PRELOAD shim 본체 |
| `bench/tcp_echo.c` | 신규 | vanilla epoll TCP echo |
| `bench/tcp_client.c` | 신규 | vanilla blocking TCP client (stdin RUN 프로토콜) |
| `bench/preload_runner.c` | 신규 | preload pod entrypoint/제어 데몬 |
| `bench/Dockerfile.preload_dpumesh` | 신규 | preload pod 이미지 |
| `test-bench.sh` | 수정 | 빌드/이미지/manifest/pin/`preload` 명령 |
| `api.md` | 수정(후속) | `dmesh_pin_route` + preload 층 문서화 (HW 검증 후) |
