# DPUmesh Thrift Transport — Plan

## 1. 클라이언트 Transport 구현

현재 서버 측(TDpumeshServerTransport + TDpumeshTransport)만 구현되어 있음.
서비스 간 호출 체인(ComposePost → UniqueId 등)은 여전히 TCP(TSocket) 사용 중.

### 해결해야 할 핵심 문제: 응답 매칭

요청을 보낸 뒤 그에 대한 응답을 어떻게 매칭해서 받을 것인가.

- `req_id` 필드가 이미 descriptor에 존재 (HTTP/2의 stream_id와 같은 역할)
- `OP_REQUEST`/`OP_RESPONSE` flags로 요청/응답 구분 가능
- 부족한 것: req_id → 대기 스레드 전달 메커니즘

### 후보 방식

| 방식 | 설명 | 장단점 |
|---|---|---|
| condvar (HTTP/2 스타일) | `map<req_id, {desc, cond, ready}>`, poller가 응답 도착 시 `cond_signal` | fd 불필요, 가벼움, thrift blocking 모델에 적합 |
| pipe (mini_ms 방식) | per-request pipe fd, poller가 pipe에 write | libevent 통합 가능, fd 자원 소모 |

dpu_daemon.py에서 이미 `threading.Event` 기반으로 동일 패턴 구현 검증됨.

### 구현 시 고려사항

- poller thread에서 OP_REQUEST/OP_RESPONSE 분류 로직 추가 필요
  - 현재는 rx_sq에 오는 것이 전부 요청이라고 가정
  - 클라이언트 역할도 하면 응답도 섞여 들어옴
- 별도 클래스(TDpumeshClientTransport) vs 기존 클래스에 생성자 추가
  - 서버/클라이언트의 read()/write() 동작이 상당히 다름
  - 서버: 데이터가 이미 SHM에 있어서 즉시 읽기
  - 클라이언트: 요청 보낸 뒤 응답 대기 필요
- 구현할 메서드: open(), write(), flush(), read(), close(), isOpen()


## 2. TNonblockingServer 호환

### 문제

TNonblockingServer는 TServerFramework를 상속하지 않고 TServer를 직접 상속.
별도 인터페이스(TNonblockingServerTransport)를 사용하며, TSocket을 강제함.

```
TServerTransport::acceptImpl()              → shared_ptr<TTransport>  (범용)
TNonblockingServerTransport::acceptImpl()   → shared_ptr<TSocket>     (TSocket 강제)
```

TNonblockingServer 내부가 TSocket에 직접 의존하는 지점 3곳:
1. `serverSocket_ = serverTransport_->getSocketFD()` — listen fd를 libevent에 등록
2. `event_set(&event_, tSocket_->getSocketFD(), ...)` — 개별 연결 fd를 libevent에 등록
3. `tSocket_->read()/write()` — workSocket()에서 fd 기반 recv/send

### 선택지

#### A. Pipe 브릿지 (Thrift 코어 수정 없음)

- SHM 데이터를 socketpair으로 복사 → TSocket(fd)에 감싸서 반환
- 장점: Thrift 코어 수정 없음
- 단점: zero-copy 장점 사라짐, 브릿지 스레드 복잡도

#### B. 인터페이스 일반화 (Thrift 코어 수정)

- `acceptImpl()` 반환 타입을 TSocket → TTransport로 변경
- `workSocket()`에서 tSocket_->read() → tTransport_->read()로 변경
- 장점: zero-copy 유지, 올바른 추상화
- 단점: TNonblockingServer.h/.cpp, TNonblockingServerTransport.h, TTransport.h 수정 필요
- 가장 어려운 지점: libevent는 fd를 감시하는 구조인데 dpumesh 개별 연결은 fd가 없음

#### C. TNonblockingServer 지원 안 함

- dpumesh에서는 데이터가 한 번에 도착 → NonBlocking의 "조금씩 읽기" 패턴 불필요
- SHM slot 수(64개)가 동시 연결 상한 → 수천 연결 관리의 이점 없음
- 장점: 수정 없음
- 단점: transport layer 모듈로서 범용성 부족

### 결정 시 고려할 질문

1. TNonblockingServer를 dpumesh와 함께 사용할 실제 시나리오가 있는가?
2. B를 선택할 경우, 개별 연결의 libevent 등록을 어떻게 처리할 것인가?
3. Thrift 코어 수정의 유지보수 부담을 감수할 것인가?


## 3. Python common.py 상수 동기화

C 코드(dpumesh_shm.h)에서 configurable하게 바꿨지만, Python 쪽(common.py)은 하드코딩:

```python
SLOT_SIZE = 1024 * 1024      # 고정
NUM_SLOTS = 64               # 고정
MAX_DESCRIPTORS = 512        # 고정
```

C에서 config로 기본값이 아닌 값을 넣으면 Python과 SHM 레이아웃이 불일치.

### 후보 방식

- 환경변수로 Python/C 양쪽에서 읽기
- SHM 메타데이터 영역에 config 값을 써놓고 양쪽에서 참조
- 기본값만 사용하도록 제한하고 문서화
