# UniqueIdService DPUmesh Integration — Architecture

## 목표

UniqueIdService의 network I/O를 dpumesh로 교체하여 DPU 가속 통신을 적용한다.

### UniqueIdService를 선택한 이유

- Leaf service (다른 서비스를 호출하지 않음)
- DB/cache 미사용 - 순수 연산 서비스 (timestamp + counter + machine_id → 64-bit ID)
- Network 구조가 단순하여 dpumesh 적용 검증에 적합

---

## Thrift 개요

### Thrift란?

서비스 A가 서비스 B의 함수를 네트워크를 통해 호출할 수 있게 해주는 RPC 프레임워크.
Facebook이 만들었고, IDL로 서비스를 정의하면 여러 언어의 클라이언트/서버 코드를 자동 생성해준다.

ComposePostService와 UniqueIdService는 별도 Pod에서 실행되므로 직접 함수 호출이 불가능하다.
Thrift가 이 "네트워크를 통한 함수 호출" 전체 과정을 자동화해준다:
1. 함수 파라미터를 bytes로 변환 (직렬화)
2. TCP로 전송
3. 상대방이 bytes를 다시 파라미터로 변환 (역직렬화)
4. 함수 실행
5. 결과를 다시 bytes로 변환해서 돌려보냄

### 사용 방법 (3단계)

**1단계: 인터페이스를 정의** (`social_network.thrift:75-81`)

```thrift
service UniqueIdService {
  i64 ComposeUniqueId(
      1: i64 req_id,
      2: PostType post_type,
      3: map<string, string> carrier
  ) throws (1: ServiceException se)
}
```

**2단계: 코드 자동 생성** (`thrift --gen cpp social_network.thrift`)

`gen-cpp/` 디렉토리에 두 가지가 생김:
- `UniqueIdServiceClient` - 호출하는 쪽 (ComposePostService가 사용)
- `UniqueIdServiceProcessor` - 받는 쪽 (UniqueIdService가 사용)

**3단계: 비즈니스 로직만 작성** (`src/UniqueIdService/UniqueIdHandler.h`)

```cpp
class UniqueIdHandler : public UniqueIdServiceIf {
  int64_t ComposeUniqueId(int64_t req_id, PostType::type post_type,
                          const map<string,string>& carrier) override {
    // 여기에 실제 로직만 작성. 네트워크 코드는 한 줄도 없다.
    return post_id;
  }
};
```

### Thrift 레이어 구조

```
호출 코드:  client->ComposeUniqueId(42, POST, {})
               |
① Generated Client:  파라미터를 pargs 구조체로 묶고 args.write(oprot) 호출
               |
② Protocol (TBinaryProtocol):  구조체를 bytes로 변환 (직렬화)
               |
③ Transport (TFramedTransport):  bytes 앞에 길이 4바이트를 붙임 (프레이밍)
               |
④ Socket (TSocket):  TCP로 전송
               |
         ===== 네트워크 =====
               |
④ Socket (TServerSocket):  TCP로 수신
               |
③ Transport (TFramedTransport):  길이 읽고, 그만큼 데이터 읽기
               |
② Protocol (TBinaryProtocol):  bytes를 구조체로 복원 (역직렬화)
               |
① Generated Processor:  구조체에서 파라미터 꺼내서 Handler 호출
               |
비즈니스 로직:  UniqueIdHandler::ComposeUniqueId(42, POST, {})
```

---

## Thrift 상세 - 레이어별 설명

### ① Generated Code (자동 생성 코드)

`gen-cpp/UniqueIdService.h`, `gen-cpp/UniqueIdService.cpp`

**요청 args 구조체** (`gen-cpp/UniqueIdService.h:67-107`):
```cpp
class UniqueIdService_ComposeUniqueId_args {
  int64_t req_id;                              // field 1
  PostType::type post_type;                    // field 2
  std::map<std::string, std::string> carrier;  // field 3

  uint32_t read(TProtocol* iprot);   // 역직렬화
  uint32_t write(TProtocol* oprot);  // 직렬화
};
```

**응답 result 구조체** (`gen-cpp/UniqueIdService.h:129-164`):
```cpp
class UniqueIdService_ComposeUniqueId_result {
  int64_t success;       // field 0: 정상 반환값
  ServiceException se;   // field 1: 예외

  uint32_t read(TProtocol* iprot);
  uint32_t write(TProtocol* oprot);
};
```

**클라이언트 send** (`gen-cpp/UniqueIdService.cpp:291-305`):
```cpp
void UniqueIdServiceClient::send_ComposeUniqueId(...) {
  oprot_->writeMessageBegin("ComposeUniqueId", T_CALL, 0);  // 메시지 헤더

  UniqueIdService_ComposeUniqueId_pargs args;  // 파라미터를 구조체로 묶음
  args.req_id = &req_id;
  args.post_type = &post_type;
  args.carrier = &carrier;
  args.write(oprot_);              // 구조체 통째로 직렬화

  oprot_->writeMessageEnd();
  oprot_->getTransport()->writeEnd();
  oprot_->getTransport()->flush(); // ★ 여기서 [4B size][payload] TCP 전송
}
```

**클라이언트 recv** (`gen-cpp/UniqueIdService.cpp:307-346`):
```cpp
int64_t UniqueIdServiceClient::recv_ComposeUniqueId() {
  iprot_->readMessageBegin(fname, mtype, rseqid);  // ★ TCP에서 frame 읽기 (blocking)
  if (mtype == T_EXCEPTION) { /* 에러 처리 */ }

  UniqueIdService_ComposeUniqueId_presult result;
  result.success = &_return;
  result.read(iprot_);             // 응답 역직렬화

  if (result.__isset.success) return _return;  // i64 반환
  if (result.__isset.se) throw result.se;      // 예외 던지기
}
```

**서버 dispatcher** (`gen-cpp/UniqueIdService.cpp:348-365`):
```cpp
bool UniqueIdServiceProcessor::dispatchCall(..., const string& fname, ...) {
  // processMap_["ComposeUniqueId"] = &process_ComposeUniqueId (생성자에서 등록)
  ProcessMap::iterator pfn = processMap_.find(fname);
  (this->*(pfn->second))(seqid, iprot, oprot, callContext);
}
```

**서버 process** (`gen-cpp/UniqueIdService.cpp:367-422`):
```cpp
void UniqueIdServiceProcessor::process_ComposeUniqueId(...) {
  // ① 요청 역직렬화
  UniqueIdService_ComposeUniqueId_args args;
  args.read(iprot);

  // ② 비즈니스 로직 호출
  result.success = iface_->ComposeUniqueId(args.req_id, args.post_type, args.carrier);
  result.__isset.success = true;

  // ③ 응답 직렬화 + 전송
  oprot->writeMessageBegin("ComposeUniqueId", T_REPLY, seqid);
  result.write(oprot);
  oprot->getTransport()->flush();  // ★ [4B size][payload] TCP 전송
}
```

### ② Protocol (TBinaryProtocol) - 직렬화

"C++ 구조체 ↔ bytes" 변환 담당. Thrift Docker 이미지 내 `/usr/local/include/thrift/protocol/TBinaryProtocol.tcc`.

```cpp
// 메시지 헤더 (strict mode)
writeMessageBegin(name, type, seqid):
  writeI32(VERSION_1 | type)  // [4B: 80 01 00 01] (version + T_CALL)
  writeString(name)           // [4B: 길이][NB: "ComposeUniqueId"]
  writeI32(seqid)             // [4B: 00 00 00 00]

// 필드 헤더
writeFieldBegin(name, type, id):
  writeByte(type)  // [1B: 0x0a = T_I64]
  writeI16(id)     // [2B: 00 01 = field 1]

// 데이터 타입
writeI64(i64):  big-endian 변환 후 trans_->write(8 bytes)
writeI32(i32):  big-endian 변환 후 trans_->write(4 bytes)
writeString(s): writeI32(size) + trans_->write(data)

// writeMessageEnd(): 아무것도 안 함 (no-op). 논리적 표시일 뿐
```

args::write() (`gen-cpp/UniqueIdService.cpp:90-119`) 가 직렬화하면:
```
[0A] [00 01]                          <- "field 1은 i64 타입이다"
[00 00 00 00 00 00 00 2A]            <- req_id = 42
[08] [00 02]                          <- "field 2는 i32 타입이다"
[00 00 00 00]                        <- post_type = 0 (POST)
[0D] [00 03] [0B 0B 00 00 00 00]    <- "field 3은 빈 map이다"
[00]                                  <- T_STOP (끝)
```

### ③ Transport (TFramedTransport) - 프레이밍

Thrift Docker 이미지 내 `/usr/local/include/thrift/transport/TBufferTransports.h`.

TCP는 stream 프로토콜이라 "메시지의 경계"가 없다.
프레이밍 = 메시지 앞에 길이를 붙여서 경계를 표시하는 것.

```
프레이밍 적용 후:
[00 00 00 2F][47바이트 payload][00 00 00 20][32바이트 payload]
 ↑ "47" (4바이트 big-endian)    ↑ "32"
```

받는 쪽: 먼저 4바이트 읽기(=크기) → 그만큼 데이터 읽기 → 메시지 완성.

```cpp
class TFramedTransport {
  // write(): 내부 wBuf_에 memcpy (아직 네트워크로 안 감)
  // flush(): wBuf_ 앞 4바이트에 size 기록 → TSocket::write()로 전체 전송
  // readFrame(): 4바이트 읽기(size) → size만큼 데이터 읽기 → rBuf_에 저장
};
```

**writeEnd()와 flush()의 차이:**
- `writeEnd()`: 쓴 바이트 수만 반환. 실제 전송 안 함.
- `flush()`: ★ 실제 TCP 전송이 여기서 발생.

### ④ Socket (TSocket / TServerSocket) - TCP 전송

실제 `send()`/`recv()` 시스템콜을 호출하는 레이어. **dpumesh로 교체한 지점**.

### gen-cpp/UniqueIdService.cpp의 함수들을 레이어로 구분

**TBinaryProtocol 함수들** (iprot-> / oprot-> 로 호출):
```
메시지:   writeMessageBegin, readMessageBegin, writeMessageEnd, readMessageEnd
구조체:   writeStructBegin, readStructBegin, writeStructEnd, readStructEnd
필드:     writeFieldBegin, readFieldBegin, writeFieldEnd, readFieldEnd, writeFieldStop
데이터:   writeI64/readI64, writeI32/readI32, writeString/readString
Map:      writeMapBegin/readMapBegin, writeMapEnd/readMapEnd
기타:     skip (알 수 없는 필드 건너뛰기)
```

**TFramedTransport 함수들** (getTransport()-> 로 호출):
```
flush()    <- ★ 실제 TCP 전송 트리거
writeEnd() <- 쓴 바이트 수 반환 (전송 안 함)
readEnd()  <- 읽은 바이트 수 반환
```

---

## 실제 코드의 대응 관계

### 생성된 코드 ↔ 실제 사용 위치

| 생성된 코드 | 실제 사용 위치 | 역할 |
|------------|--------------|------|
| `UniqueIdServiceClient` | `ComposePostHandler.h:244` (GetClient() 반환값) | ComposePostService가 RPC 호출할 때 |
| `UniqueIdServiceProcessor` | `UniqueIdService.cpp:54` (TThreadedServer 생성자) | UniqueIdService가 RPC 수신할 때 |
| `UniqueIdServiceIf` | `UniqueIdHandler.h:42` (class 상속) | Handler가 구현하는 인터페이스 |

---

## 클라이언트 측 상세 (ComposePostService)

### Transport 스택 조립

`src/ThriftClient.h:59-70` - 포인터 체인이 만들어지는 과정:

```cpp
// 1단계: TCP socket 생성 (아직 연결 안 됨)
_socket = make_shared<TSocket>(addr, port);
_socket->setKeepAlive(true);

// 2단계: TSocket을 TFramedTransport로 감싼다
_transport = make_shared<TFramedTransport>(_socket);
// write → 내부 버퍼에 누적, flush → [4B size][payload]를 _socket->write()로 전송

// 3단계: TFramedTransport를 TBinaryProtocol로 감싼다
_protocol = make_shared<TBinaryProtocol>(_transport);
// writeI64(42) → 42를 big-endian 8바이트로 → _transport->write(8 bytes)

// 4단계: protocol을 UniqueIdServiceClient에 연결
_client = new UniqueIdServiceClient(_protocol);
```

결과 포인터 체인:
```
_client (UniqueIdServiceClient)
  -> oprot_ (_protocol: TBinaryProtocol)
    -> trans_ (_transport: TFramedTransport)
      -> transport_ (_socket: TSocket)
        -> socket fd (OS TCP socket)
```

### TCP 연결 시점

`src/ThriftClient.h:127-135`:
```cpp
void ThriftClient::Connect() {
  if (!IsConnected()) {
    _transport->open();
    // → TFramedTransport::open() → TSocket::open()
    // → socket(), connect() 시스템콜 → TCP 3-way handshake
  }
}
```

이 Connect()는 `ClientPool::Pop()` 안에서 호출됨 (`src/ClientPool.h:110-117`):
```cpp
TClient* ClientPool::Pop() {
  // pool에서 client 꺼내거나 새로 만들고
  client->Connect();  // <- TCP 연결 보장
  return client;
}
```

### RPC 호출 전체 흐름

`src/ComposePostService/ComposePostHandler.h:223-258`:
```cpp
int64_t ComposePostHandler::_ComposeUniqueIdHelper(...) {
  // 1. ClientPool에서 client 획득 (TCP 연결 포함)
  auto unique_id_client_wrapper = _unique_id_service_client_pool->Pop();
  auto unique_id_client = unique_id_client_wrapper->GetClient();

  // 2. RPC 호출 (내부적으로 send + recv)
  _return_unique_id = unique_id_client->ComposeUniqueId(req_id, post_type, writer_text_map);

  // 3. 연결 풀에 반환 (keepalive 시간 초과시 폐기)
  _unique_id_service_client_pool->Keepalive(unique_id_client_wrapper);
  return _return_unique_id;
}
```

이것이 ComposePost()에서 std::async로 병렬 호출됨 (`ComposePostHandler.h:383-395`):
```cpp
auto unique_id_future = std::async(std::launch::async,
    &ComposePostHandler::_ComposeUniqueIdHelper, this,
    req_id, post_type, writer_text_map);
// ... 다른 서비스들도 병렬 호출 ...
post.post_id = unique_id_future.get();  // 결과 대기
```

---

## 서버 측 상세 (UniqueIdService)

### 서버 시작 (원본 TCP 코드)

`src/UniqueIdService/UniqueIdService.cpp:52-61`:
```cpp
// TServerSocket: socket(), bind(0.0.0.0:9090), listen()
std::shared_ptr<TServerSocket> server_socket = get_server_socket(config_json, "0.0.0.0", port);

TThreadedServer server(
    // Processor: 요청 처리기 (UniqueIdHandler를 감싸고 있음)
    make_shared<UniqueIdServiceProcessor>(
        make_shared<UniqueIdHandler>(&thread_lock, machine_id)),

    server_socket,                               // 어디서 연결을 받을지

    make_shared<TFramedTransportFactory>(),       // 새 연결마다 TFramedTransport 생성
    make_shared<TBinaryProtocolFactory>());       // 새 연결마다 TBinaryProtocol 생성

server.serve();
```

Factory 패턴을 쓰는 이유: 클라이언트가 여러 개 연결되므로, 연결마다 새 Transport/Protocol 인스턴스가 필요하다.

### 서버 동작 모델 (TThreadedServer)

`TThreadedServer::serve()` → `TServerFramework::serve()` 호출 체인:

```cpp
// TServerFramework::serve() (TServerFramework.cpp:109-188)
void TServerFramework::serve() {
  serverTransport_->listen();           // ① TCP listen

  for (;;) {
    // 동시 접속 제한 대기
    { Synchronized sync(mon_);
      while (clients_ >= limit_) mon_.wait();
    }

    client = serverTransport_->accept();                        // ② 클라이언트 연결 대기

    inputTransport = inputTransportFactory_->getTransport(client);   // ③ TFramedTransport 생성
    outputTransport = outputTransportFactory_->getTransport(client);
    inputProtocol = inputProtocolFactory_->getProtocol(inputTransport);  // ④ TBinaryProtocol 생성

    newlyConnectedClient(new TConnectedClient(processor, inputProtocol, outputProtocol, client));
    // → onClientConnected() 호출
  }
}
```

```cpp
// TThreadedServer::onClientConnected() (TThreadedServer.cpp:116-123)
void TThreadedServer::onClientConnected(const shared_ptr<TConnectedClient>& pClient) {
  Synchronized sync(clientMonitor_);
  shared_ptr<TConnectedClientRunner> pRunnable = make_shared<TConnectedClientRunner>(pClient);
  shared_ptr<Thread> pThread = threadFactory_->newThread(pRunnable);
  activeClientMap_.insert({pClient.get(), pThread});
  pThread->start();    // ⑤ 새 스레드에서 pClient->run() 실행
}
```

`pClient->run()` 안에서 `processor->process(iprot, oprot)`가 반복 실행된다.

### 서버 측 포인터 체인 (연결당)

```
TConnectedClient::run()
  └→ processor->process(iprot, oprot)        // 반복 호출
       │
       ├→ iprot (TBinaryProtocol)            ← 요청 역직렬화
       │    └→ trans_ (TFramedTransport)     ← 프레임 경계 파싱 (4바이트 길이 prefix)
       │         └→ transport_ (TSocket)     ← accept()이 반환한 소켓 fd에서 recv()
       │
       ├→ oprot (TBinaryProtocol)            ← 응답 직렬화
       │    └→ trans_ (TFramedTransport)     ← 프레임 길이 prefix 붙이기
       │         └→ transport_ (TSocket)     ← 같은 소켓 fd로 send()
       │
       └→ iface_ (UniqueIdHandler)           ← 비즈니스 로직 (ID 생성)
```

---

## 비즈니스 로직 상세 (UniqueIdHandler)

`src/UniqueIdService/UniqueIdHandler.h:61-109`

(.h에 구현하는 이유: 이 프로젝트는 header-only 스타일. ClientPool.h, ThriftClient.h, ComposePostHandler.h 전부 .h에 구현이 있다. template 코드와의 호환성 + 파일 하나에 선언/구현이 있어 편의성.)

```cpp
int64_t UniqueIdHandler::ComposeUniqueId(
    int64_t req_id, PostType::type post_type,
    const map<string, string> &carrier) {
```

파라미터:
- `req_id`: 요청 추적용 ID (Jaeger tracing에 쓰임, ID 생성 자체에는 미사용)
- `post_type`: POST/REPOST/REPLY/DM (이 함수에서는 미사용)
- `carrier`: Jaeger trace context 전파용 key-value map

```cpp
  // ===== Jaeger Tracing (성능 측정용, 비즈니스 로직 아님) =====
  // 부모 span에서 trace context 추출 → 새 span 생성
  // Jaeger UI에서 "compose_unique_id_server"로 보임

  // ===== 핵심 로직 =====
  _thread_lock->lock();
  // mutex lock - 여러 thread가 동시에 같은 ID를 만들지 않도록

  int64_t timestamp = 현재시간(ms) - 2018년1월1일0시(ms);
  int idx = GetCounter(timestamp);
  // 같은 ms 내에서 몇 번째 요청인지 카운트
  // 같은 timestamp면 counter++, 새 timestamp면 counter=0

  _thread_lock->unlock();

  // timestamp → 10자리 hex (40-bit), counter → 3자리 hex (12-bit)으로 변환
  // machine_id(3 hex) + timestamp(10 hex) + counter(3 hex) = 16자리 hex

  string post_id_str = _machine_id + timestamp_hex + counter_hex;
  int64_t post_id = stoul(post_id_str, nullptr, 16) & 0x7FFFFFFFFFFFFFFF;
  // 16진수 문자열 → 정수 변환, 최상위 비트 0으로 (양수 보장)

  // 결과 비트 구조:
  // |0| 11-bit machine ID | 40-bit timestamp | 12-bit counter |
  return post_id;
}
```

machine_id 생성 (`UniqueIdHandler.h:126-156`):
- `/sys/class/net/eth0/address`에서 MAC 주소 읽기
- MAC + PID를 해시 → 하위 3 hex digits (11-bit)

---

## Wire에서 실제로 오가는 바이트 (예시)

`ComposeUniqueId(req_id=42, post_type=POST(0), carrier={})` 호출 시:

### 요청 (Client → Server)
```
[00 00 00 2F]                                    <- frame size (47 bytes)
[80 01 00 01]                                    <- version | T_CALL
[00 00 00 0F]                                    <- method name length (15)
[43 6F 6D 70 6F 73 65 55 6E 69 71 75 65 49 64]  <- "ComposeUniqueId"
[00 00 00 00]                                    <- sequence ID (0)
[0A 00 01]                                       <- field: T_I64, id=1
[00 00 00 00 00 00 00 2A]                        <- req_id = 42
[08 00 02]                                       <- field: T_I32, id=2
[00 00 00 00]                                    <- post_type = 0 (POST)
[0D 00 03]                                       <- field: T_MAP, id=3
[0B 0B 00 00 00 00]                              <- map<STRING,STRING>, size=0
[00]                                             <- T_STOP
```

### 응답 (Server → Client)
```
[00 00 00 1E]                                    <- frame size (30 bytes)
[80 01 00 02]                                    <- version | T_REPLY
[00 00 00 0F]                                    <- method name length (15)
[43 6F 6D 70 6F 73 65 55 6E 69 71 75 65 49 64]  <- "ComposeUniqueId"
[00 00 00 00]                                    <- sequence ID (0)
[0A 00 00]                                       <- field: T_I64, id=0 (success)
[XX XX XX XX XX XX XX XX]                        <- 생성된 post_id (8 bytes)
[00]                                             <- T_STOP
```

---

## ComposePost 호출 체인

```
wrk2 -> nginx(8080) -> ComposePostService -> [병렬 호출, std::async]
                                              +-> UniqueIdService  (leaf, 순수 연산)
                                              +-> UserService
                                              +-> TextService
                                              +-> MediaService
                                            -> [후속 처리]
                                              +-> PostStorageService
                                              +-> UserTimelineService (deferred)
                                              +-> HomeTimelineService (deferred)
```

---

## 핵심 파일 목록

### socialNetwork 프로젝트 파일

| 파일 | 역할 |
|------|------|
| `social_network.thrift` | IDL 정의 (서비스 인터페이스) |
| `gen-cpp/UniqueIdService.h` | 생성된 코드: args/result 구조체, Client stub, Processor |
| `gen-cpp/UniqueIdService.cpp` | 생성된 코드: 직렬화/역직렬화, send/recv, dispatch |
| `src/UniqueIdService/UniqueIdService.cpp` | 서버 진입점: TThreadedServer 구성 |
| `src/UniqueIdService/UniqueIdHandler.h` | 비즈니스 로직: unique ID 생성 |
| `src/ComposePostService/ComposePostService.cpp` | 클라이언트 측 서버: ClientPool 구성 |
| `src/ComposePostService/ComposePostHandler.h` | 클라이언트 측: `_ComposeUniqueIdHelper()` RPC 호출 |
| `src/ThriftClient.h` | transport 스택 조립: TSocket→TFramedTransport→TBinaryProtocol |
| `src/ClientPool.h` | 연결 풀: Pop/Push/Keepalive/Remove |
| `src/GenericClient.h` | Client 추상 인터페이스 |
| `src/utils_thrift.h` | 서버 소켓 생성 유틸 |

### Thrift 라이브러리 파일 (Docker 이미지 내)

| 파일 | 역할 |
|------|------|
| `thrift/protocol/TBinaryProtocol.h`, `.tcc` | binary 직렬화 구현 |
| `thrift/transport/TBufferTransports.h` | TFramedTransport, TMemoryBuffer |
| `thrift/server/TThreadedServer.h`, `.cpp` | connection당 thread 서버 |
| `thrift/server/TServerFramework.h`, `.cpp` | 서버 accept loop (핵심 `serve()` 구현) |
| `thrift/server/TConnectedClient.h` | per-connection processing loop |

---

## DPUmesh 실제 구현

### 구현 방식

별도 Transport 클래스(`TDpumeshTransport`, `TDpumeshServerTransport`)를 Thrift 라이브러리에 추가하고, 서버 코드에서 `TServerSocket` → `TDpumeshServerTransport`로 교체하는 방식을 택했다.

이유:
- TSocket 내부 수정은 dpumesh 비활성 시에도 코드 경로가 복잡해짐
- 별도 클래스가 디버깅/유지보수에 유리
- 실제 변경은 UniqueIdService.cpp 2줄뿐이므로 충분히 최소

### 변경 파일 요약

> **백엔드 = NVIDIA DOCA (실 BlueField-3 하드웨어).** 과거 RFC의 Python+SHM
> 시뮬레이션(`dpumesh/*.py`, `dpumesh_shm.c`, TCP bridge 5050)은 제거되었다.
> 현재 dpumesh transport는 `-DWITH_DOCA=ON`으로 libthrift에 빌드되며 DOCA
> Comch(컨트롤) + DPA `dma_copy`(데이터)로 동작한다. 최신 설계 상세는
> `architecture/dpumesh_architecture.md` 참조.

세 개의 별도 빌드로 나뉜다: ① host `libthrift.so`, ② DPU(BF3 ARM)
`dpumesh_dpu`, ③ DPA(FlexIO EU) 프로그램 `dpa_program.a`.

#### ① Thrift 라이브러리 (host, libthrift.so)

| 파일 | 역할 |
|------|------|
| `transport/dpumesh.h` | 공개 C API: 64B `sw_descriptor_t`, TX/RX 슬롯 풀, SQ enqueue/dequeue, 클라이언트 req/resp 매칭 |
| `transport/dpumesh_doca.c` | 호스트 DOCA 백엔드: 슬롯 풀·DMA 디스크립터 링·RX 큐·pending 테이블·PE progress 스레드·`rx_data_hook`(TX_ACK / DMA_COMPLETION 처리) |
| `transport/doca/common.c, object.c, buffer.c, ring.c` | DOCA 디바이스/리소스/버퍼/링 헬퍼 |
| `transport/doca/comch_common.c, comch_client.c, comch_server.c, comch_consumer.c, comch_msgq.c` | DOCA Comch 컨트롤·데이터 경로 (host = comch client) |
| `transport/doca/dpa.c` | 호스트측 DPA 셋업 (DPA thread, msgq, per-pod buffer array) |
| `transport/TDpumeshTransport.h/.cpp` | 요청당 transport: RX 슬롯 read, TX 슬롯 direct-write + flush |
| `transport/TDpumeshServerTransport.h/.cpp` | 서버 transport: `dpumesh_init()`, `acceptImpl()`이 RX 큐에서 dequeue → `TDpumeshTransport` 반환 |
| `transport/TDpumeshClientTransport.h/.cpp` | 클라이언트 transport: `register_pending` + `enqueue` + `wait_response` |

`TSocket.h/cpp`, `TServerSocket.h/cpp`는 **수정하지 않았다.** dpumesh transport는
`-DWITH_DOCA=ON`일 때만 libthrift에 포함된다.

#### ② DPU / ③ DPA (별도 빌드, `transport/doca/`)

| 파일 | 역할 |
|------|------|
| `doca/dpu_main.c, dpu_worker.c, config.c` | DPU(BF3 ARM) 바이너리 `dpumesh_dpu`: comch 컨트롤 PE + consumer PE + 라우팅/ACK 워커 루프 |
| `doca/device/dpa_kernel.c` | DPA(FlexIO EU) 프로그램: `drain_all_rings`로 forward/reverse `dma_copy` 수행 |

#### 앱 코드 (UniqueIdService만 변경)

```cpp
// UniqueIdService.cpp
+#include <thrift/transport/TDpumeshServerTransport.h>
+using apache::thrift::transport::TDpumeshServerTransport;

// TServerSocket → TDpumeshServerTransport
-auto server_socket = get_server_socket(config_json, "0.0.0.0", port);
+auto server_socket = std::make_shared<TDpumeshServerTransport>("unique-id-service", 0);
```

UniqueIdHandler.h, ComposePostHandler.h, ThriftClient.h, ClientPool.h 등은 **변경 없음.**

### 데이터 경로 (1 RTT = 4 dma_copy)

```
ComposePostService (client, libthrift + DOCA)
  → host TX 슬롯 direct-write → DMA 디스크립터 링 enqueue (host→DPU)
  → DPA: host TX → DPU buf 로 dma_copy (forward)
  → DPU worker: pod 라우팅 + TX_ACK 송신 + reverse 디스크립터 작성
  → DPA: DPU buf → 목적지 host RX 로 dma_copy (reverse)
  → DMA_COMPLETION(16B comch_dma_comp_msg) → host rx_data_hook → RX 큐
  → UniqueIdService (TDpumeshServerTransport): RX 큐 dequeue → TDpumeshTransport 처리
```

페이로드는 body-only(인밴드 헤더 없음). 메타데이터(req_id, src/dst pod, flags,
length)는 DMA 페이로드가 아니라 `comch_dma_comp_msg`로 전달된다.

### TCP vs DPUmesh 비교

| 항목 | TCP (원본) | DPUmesh (DOCA) |
|------|-----------|---------------|
| listen | `socket()` → `bind()` → `listen()` | `dpumesh_init()` — DOCA 풀/링/comch 초기화 |
| accept | 커널 `accept()` → 새 fd | `dpumesh_dequeue()` → RX 디스크립터 |
| read | `recv(fd)` — 커널 버퍼 → 유저 복사 | `dpumesh_rx_buf()` — RX 슬롯 직접 포인터 (zero-copy) |
| write | `send(fd)` — 유저 → 커널 → NIC | TX 슬롯 direct-write, `dpumesh_enqueue()` → DPA `dma_copy` |
| 전송 위치 | host CPU (커널 TCP) | DPU ARM + DPA EU (host CPU 외부) |
| 연결 단위 | 클라이언트당 소켓 fd (persistent) | 요청당 디스크립터 (one-shot) |
| 라우팅 | IP:port | pod_id + req_id(stream_id) |
