#!/bin/bash
# test-dpumesh.sh — DPUmesh 통합 배포/테스트 스크립트
#
# 사용법:
#   ./test-dpumesh.sh deploy    # 전체: sync + build + DPU시작 + pods 순서대로 시작 + test
#   ./test-dpumesh.sh dpu       # DPU만: sync + build + restart dpumesh_dpu
#   ./test-dpumesh.sh host      # Host만: libthrift.so 재빌드
#   ./test-dpumesh.sh restart   # Pods만: 순서대로 재시작 (DPU→Gateway→Service)
#   ./test-dpumesh.sh test      # 테스트만 (test_thrift.py)
#   ./test-dpumesh.sh logs      # DPU + pod 로그 확인
#   ./test-dpumesh.sh cleanup   # test-dpumesh ns 삭제
#   ./test-dpumesh.sh status    # 전체 상태 확인

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'


# DPU_HOST, DPU_PASS, DPU_PCI, HOST_PCI
if [ -f ".env" ]; then
    echo -e "${GREEN}[INFO]${NC} Loading environment variables from .env"
    set -a
    source .env
    set +a
else
    echo -e "${RED}[ERR]${NC} .env file not found! Please create a .env file."
    exit 1
fi

### 설정 ###
NS="test-dpumesh"
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRANSPORT_SRC="$PROJ_ROOT/lib/cpp/src/thrift/transport"
DOCA_SRC="$TRANSPORT_SRC/doca"
# DPU 쪽 경로 (DPU의 ~/thrift_dpumesh_extended/ 에 배포)
DPU_TRANSPORT="thrift_dpumesh_extended/lib/cpp/src/thrift/transport"
DPU_DOCA="$DPU_TRANSPORT/doca"
DPU_BUILD="$DPU_DOCA/build"
BUILD_DOCA="$PROJ_ROOT/build-doca"
GATEWAY_PORT=9091
DPU_LOG="/tmp/dpumesh_dpu_test.log"
DOCA_LIB_DIR="/opt/mellanox/doca/lib/x86_64-linux-gnu"
FLEXIO_LIB_DIR="/opt/mellanox/flexio/lib"
GATEWAY_IMAGE="social-network/dpumesh-gateway:latest"

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERR]${NC} $*"; }
step()  { echo -e "${BLUE}[STEP]${NC} $*"; }

dpu_sudo() {
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash -c '$1'" 2>&1 | grep -v '^\[sudo\]'
}

### 소스 동기화 ###
sync_sources() {
    step "=== Syncing ALL sources to DPU ==="

    # doca/ 디렉토리 전체 동기화 (build/, builddir/ 제외)
    info "Syncing doca/ directory..."
    rsync -avz --delete \
        --exclude='build/' \
        --exclude='builddir/' \
        --exclude='doca/' \
        --exclude='*.o' \
        --exclude='*.a' \
        "$DOCA_SRC/" "$DPU_HOST:~/$DPU_DOCA/"

    # host-side transport 파일도 동기화 (dpumesh_doca.c, dpumesh.h 등)
    info "Syncing host transport files..."
    rsync -avz \
        "$TRANSPORT_SRC/dpumesh_doca.c" \
        "$TRANSPORT_SRC/dpumesh.h" \
        "$TRANSPORT_SRC/dpumesh_shm.c" \
        "$TRANSPORT_SRC/TDpumeshTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshTransport.h" \
        "$TRANSPORT_SRC/TDpumeshServerTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshServerTransport.h" \
        "$TRANSPORT_SRC/TDpumeshClientTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshClientTransport.h" \
        "$DPU_HOST:~/$DPU_TRANSPORT/" 2>/dev/null || true

    # Fix clock skew (host/DPU 시간 차이 대응)
    info "Fixing timestamps on DPU..."
    ssh "$DPU_HOST" "find ~/$DPU_DOCA -type f -exec touch {} + && find ~/$DPU_TRANSPORT -maxdepth 1 -type f -exec touch {} +" 2>/dev/null || true

    info "Source sync complete"
}

### DPU 빌드 ###
build_dpu() {
    step "=== Building on DPU (ninja) ==="
    # Force DPA kernel rebuild (dpacc) by removing the cached .a
    ssh "$DPU_HOST" "rm -f ~/$DPU_BUILD/dpa_kernel.a" 2>/dev/null || true
    local build_out
    build_out=$(ssh "$DPU_HOST" "cd ~/$DPU_BUILD && ninja" 2>&1)
    if echo "$build_out" | grep -q "error:"; then
        err "DPU build failed:"
        echo "$build_out"
        exit 1
    fi
    if echo "$build_out" | grep -q "no work to do"; then
        info "DPU build: no changes (up to date)"
    else
        info "DPU build OK"
        echo "$build_out" | tail -5
    fi
}

### Host libthrift 빌드 ###
build_host() {
    step "=== Building host libthrift.so ==="

    # CMake 캐시 경로 불일치 시 자동 재설정
    if [ -f "$BUILD_DOCA/CMakeCache.txt" ]; then
        local cached_dir
        cached_dir=$(grep '^CMAKE_HOME_DIRECTORY:' "$BUILD_DOCA/CMakeCache.txt" 2>/dev/null | cut -d= -f2)
        if [ -n "$cached_dir" ] && [ "$cached_dir" != "$PROJ_ROOT" ]; then
            warn "CMake cache path mismatch (cached: $cached_dir, actual: $PROJ_ROOT)"
            info "Clearing stale cache and re-running cmake..."
            rm -f "$BUILD_DOCA/CMakeCache.txt"
            rm -rf "$BUILD_DOCA/CMakeFiles"
            (cd "$BUILD_DOCA" && cmake "$PROJ_ROOT" -DWITH_DOCA=ON -DWITH_CPP=ON -DWITH_C_GLIB=ON \
                -DWITH_SHARED_LIB=ON -DWITH_STATIC_LIB=ON -DWITH_LIBEVENT=ON -DWITH_OPENSSL=ON -DWITH_ZLIB=ON \
                -DBUILD_COMPILER=OFF -DBUILD_TESTING=OFF -DBUILD_TUTORIALS=OFF -DBUILD_EXAMPLES=OFF \
                -DWITH_JAVA=OFF -DWITH_PYTHON=OFF -DWITH_HASKELL=OFF)
        fi
    elif [ ! -f "$BUILD_DOCA/Makefile" ]; then
        info "No build system found, running cmake..."
        mkdir -p "$BUILD_DOCA"
        (cd "$BUILD_DOCA" && cmake "$PROJ_ROOT" -DWITH_DOCA=ON -DWITH_CPP=ON -DWITH_C_GLIB=ON \
            -DWITH_SHARED_LIB=ON -DWITH_STATIC_LIB=ON -DWITH_LIBEVENT=ON -DWITH_OPENSSL=ON -DWITH_ZLIB=ON)
    fi

    local build_out
    build_out=$(cd "$BUILD_DOCA" && make -j"$(nproc)" 2>&1)
    if [ $? -ne 0 ]; then
        err "Host build failed:"
        echo "$build_out"
        exit 1
    fi
    # 결과 확인
    local so_file="$BUILD_DOCA/lib/libthrift.so.0.12.0"
    if [ -f "$so_file" ]; then
        info "Host build OK ($(ls -lh "$so_file" | awk '{print $5}'))"
    else
        err "libthrift.so.0.12.0 not found after build!"
        exit 1
    fi
}

### Gateway 이미지 빌드 ###
build_gateway_image() {
    step "=== Building gateway binary + Docker image ==="

    # Build gateway binary
    local THRIFT_LINK_LIB="-lthriftd"
    if [ ! -e "$BUILD_DOCA/lib/libthriftd.so" ] && [ ! -e "$BUILD_DOCA/lib/libthriftd.a" ]; then
        THRIFT_LINK_LIB="-lthrift"
    fi
    gcc -o "$PROJ_ROOT/gateway" "$PROJ_ROOT/gateway.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" \
        -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    info "Gateway binary built"

    # Install thrift libs for Docker context
    rm -rf "$PROJ_ROOT/thrift-install"
    (cd "$BUILD_DOCA" && make install DESTDIR="$PROJ_ROOT/thrift-install")
    local THRIFT_LIB_DIR="$PROJ_ROOT/thrift-install/usr/local/lib"
    for f in "$THRIFT_LIB_DIR"/lib*d.so; do
        [ -f "$f" ] || continue
        ln -sf "$(basename "$f")" "$THRIFT_LIB_DIR/$(basename "$f" | sed 's/d\.so$/.so/')"
    done
    for f in "$THRIFT_LIB_DIR"/lib*d.so.0.12.0; do
        [ -f "$f" ] || continue
        ln -sf "$(basename "$f")" "$THRIFT_LIB_DIR/$(basename "$f" | sed 's/d\.so\.0\.12\.0$/.so.0.12.0/')"
    done

    # Collect DOCA runtime libs
    rm -rf "$PROJ_ROOT/doca-libs"
    mkdir -p "$PROJ_ROOT/doca-libs"
    for lib in \
        "$DOCA_LIB_DIR"/libdoca_common.so* \
        "$DOCA_LIB_DIR"/libdoca_comch.so* \
        "$DOCA_LIB_DIR"/libdoca_dpa.so* \
        "$FLEXIO_LIB_DIR"/libflexio.so* \
        /lib/x86_64-linux-gnu/libmlx5.so* \
        /lib/x86_64-linux-gnu/libibverbs.so*; do
        [ -e "$lib" ] && cp -a "$lib" "$PROJ_ROOT/doca-libs/"
    done

    # Build Docker image + import to containerd
    # Prime sudo credential cache (avoids stdin conflict with docker save pipe)
    echo "$HOST_PASS" | sudo -S true 2>/dev/null
    docker build \
        -f "$PROJ_ROOT/Dockerfile.gateway" \
        -t "$GATEWAY_IMAGE" "$PROJ_ROOT"
    sudo ctr -n k8s.io images rm "docker.io/$GATEWAY_IMAGE" 2>/dev/null || true
    docker save "$GATEWAY_IMAGE" | sudo ctr -n k8s.io images import -
    docker image prune -f >/dev/null 2>&1 || true
    info "Gateway Docker image built and imported"
}

### DPU 프로세스 관리 ###
stop_dpu() {
    info "Stopping dpumesh_dpu..."
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S killall -9 dpumesh_dpu 2>/dev/null; true" 2>&1 | grep -v '^\[sudo\]' || true
    info "Waiting for devx resources to release..."
    sleep 5
}

start_dpu() {
    step "=== Starting dpumesh_dpu on DPU ==="
    stop_dpu

    info "Launching dpumesh_dpu..."
    ssh "$DPU_HOST" "cat > /tmp/start_dpu.sh << 'LAUNCHER'
#!/bin/bash
screen -dmS dpumesh bash -c \"cd /home/jukebox/$DPU_BUILD && ./dpumesh_dpu $DPU_PCI -l 50 > $DPU_LOG 2>&1\"
sleep 2
pgrep -f 'dpumesh_dpu.*03:00' || echo NO_PID
LAUNCHER
chmod +x /tmp/start_dpu.sh"

    local pid
    pid=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash /tmp/start_dpu.sh" 2>&1 | grep -v '^\[sudo\]')

    if [ "$pid" = "NO_PID" ] || [ -z "$pid" ]; then
        err "dpumesh_dpu failed to start! Log:"
        ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -20 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]'
        exit 1
    fi
    info "dpumesh_dpu running (PID: $pid)"

    # DPU 안정화 대기 — 프로세스 실행 확인 + 로그 체크
    info "Waiting for DPU to stabilize..."
    local attempts=0
    while [ $attempts -lt 15 ]; do
        local log_line
        log_line=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]' || true)
        # INFO 로그 레벨이면 "pods: 0" 확인, ERR 레벨이면 프로세스 존재만 확인
        if echo "$log_line" | grep -q "pods: 0"; then
            info "DPU ready (pods: 0, waiting for connections)"
            return 0
        fi
        if echo "$log_line" | grep -q "elapsed:"; then
            info "DPU ready (main loop running)"
            return 0
        fi
        # 프로세스가 살아있고 로그가 최근 것이면 OK
        local dpu_alive
        dpu_alive=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S pgrep -f 'dpumesh_dpu.*03:00'" 2>&1 | grep -v '^\[sudo\]' | head -1 || true)
        if [ -n "$dpu_alive" ] && [ "$attempts" -ge 5 ]; then
            info "DPU ready (PID: $dpu_alive, log level may suppress INFO)"
            return 0
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    warn "DPU stabilization timeout (may still be initializing)"
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -5 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]'
}

### K8s 리소스 ###
ensure_namespace() {
    if ! kubectl get ns "$NS" &>/dev/null; then
        info "Creating namespace $NS"
        kubectl create ns "$NS"
    fi
}

apply_k8s_resources() {
    step "=== Applying K8s resources (replicas=0) ==="

    # ConfigMap
    cat <<'CFGEOF' | kubectl apply -n "$NS" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: unique-id-service
data:
  jaeger-config.yml: |
    disabled: true
    reporter:
      logSpans: false
    sampler:
      type: "const"
      param: 0
  service-config.json: |
    {
      "secret": "secret",
      "unique-id-service": {
        "addr": "unique-id-service",
        "port": 9090,
        "connections": 512,
        "timeout_ms": 10000,
        "keepalive_ms": 10000,
        "netif": "eth0"
      },
      "ssl": { "enabled": false }
    }
CFGEOF

    # Gateway deployment (replicas: 0 — 순서 제어)
    cat <<GWEOF | kubectl apply -n "$NS" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dpumesh-gateway
spec:
  replicas: 0
  selector:
    matchLabels:
      app: dpumesh-gateway
  template:
    metadata:
      labels:
        app: dpumesh-gateway
    spec:
      hostname: dpumesh-gateway
      containers:
      - name: dpumesh-gateway
        image: docker.io/social-network/dpumesh-gateway:latest
        imagePullPolicy: Never
        ports:
        - containerPort: $GATEWAY_PORT
        env:
        - name: DPUMESH_PCI_ADDR
          value: "$HOST_PCI"
        securityContext:
          privileged: true
        volumeMounts:
        - mountPath: /dev/infiniband
          name: infiniband
        - mountPath: /usr/local/lib/libthrift.so.0.12.0
          name: libthrift-so
          subPath: libthrift.so.0.12.0
      volumes:
      - name: infiniband
        hostPath:
          path: /dev/infiniband
      - name: libthrift-so
        hostPath:
          path: $BUILD_DOCA/lib
          type: Directory
GWEOF

    # Gateway service (NodePort)
    cat <<SVCEOF | kubectl apply -n "$NS" -f -
apiVersion: v1
kind: Service
metadata:
  name: dpumesh-gateway
spec:
  type: NodePort
  selector:
    app: dpumesh-gateway
  ports:
  - port: $GATEWAY_PORT
    targetPort: $GATEWAY_PORT
    nodePort: 30091
SVCEOF

    # unique-id-service deployment (replicas: 0 — 순서 제어)
    cat <<UIDEOF | kubectl apply -n "$NS" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: unique-id-service
spec:
  replicas: 0
  selector:
    matchLabels:
      service: unique-id-service
  template:
    metadata:
      labels:
        app: unique-id-service
        service: unique-id-service
    spec:
      hostname: unique-id-service
      containers:
      - name: unique-id-service
        image: docker.io/social-network/unique-id-service:latest
        imagePullPolicy: Never
        command: ["UniqueIdService"]
        ports:
        - containerPort: 9090
        env:
        - name: DPUMESH_PCI_ADDR
          value: "$HOST_PCI"
        securityContext:
          privileged: true
        volumeMounts:
        - mountPath: /dev/infiniband
          name: infiniband
        - mountPath: /usr/local/lib/libthrift.so.0.12.0
          name: libthrift-so
          subPath: libthrift.so.0.12.0
        - mountPath: /social-network-microservices/config/jaeger-config.yml
          name: config
          subPath: jaeger-config.yml
        - mountPath: /social-network-microservices/config/service-config.json
          name: config
          subPath: service-config.json
      volumes:
      - name: infiniband
        hostPath:
          path: /dev/infiniband
      - name: libthrift-so
        hostPath:
          path: $BUILD_DOCA/lib
          type: Directory
      - name: config
        configMap:
          name: unique-id-service
UIDEOF

    info "K8s resources applied (all replicas=0)"
}

### Pod 순서 제어 ###
scale_down_social_network() {
    info "Scaling down social-network dpumesh pods (avoid DPU conflicts)..."
    kubectl scale deployment dpumesh-gateway --replicas=0 -n social-network 2>/dev/null || true
    kubectl scale deployment unique-id-service --replicas=0 -n social-network 2>/dev/null || true
}

start_gateway() {
    step "=== Starting Gateway pod ==="
    # 기존 pod 삭제 후 새로 시작
    kubectl scale deployment dpumesh-gateway --replicas=0 -n "$NS" 2>/dev/null || true
    sleep 2
    kubectl scale deployment dpumesh-gateway --replicas=1 -n "$NS"

    info "Waiting for Gateway pod to be Ready..."
    if ! kubectl wait --for=condition=Ready pod -l app=dpumesh-gateway -n "$NS" --timeout=120s 2>&1; then
        err "Gateway pod failed to start!"
        kubectl describe pod -l app=dpumesh-gateway -n "$NS" | tail -15
        exit 1
    fi

    # DPU가 gateway 연결을 인식할 때까지 대기
    info "Waiting for DPU to register gateway..."
    local attempts=0
    while [ $attempts -lt 15 ]; do
        local log_line
        log_line=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]' || true)
        if echo "$log_line" | grep -q "pods: 1"; then
            info "DPU registered gateway (pods: 1)"
            return 0
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    warn "DPU gateway registration timeout — continuing anyway"
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]'
}

start_service() {
    step "=== Starting unique-id-service pod ==="
    kubectl scale deployment unique-id-service --replicas=0 -n "$NS" 2>/dev/null || true
    sleep 2
    kubectl scale deployment unique-id-service --replicas=1 -n "$NS"

    info "Waiting for unique-id-service pod to be Ready..."
    if ! kubectl wait --for=condition=Ready pod -l service=unique-id-service -n "$NS" --timeout=120s 2>&1; then
        err "unique-id-service pod failed to start!"
        kubectl describe pod -l service=unique-id-service -n "$NS" | tail -15
        exit 1
    fi

    # DPU가 service 연결을 인식할 때까지 대기
    info "Waiting for DPU to register service..."
    local attempts=0
    while [ $attempts -lt 15 ]; do
        local log_line
        log_line=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]' || true)
        if echo "$log_line" | grep -q "pods: 2"; then
            info "DPU registered service (pods: 2)"
            return 0
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    warn "DPU service registration timeout — continuing anyway"
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]'
}

### 테스트 ###
run_test() {
    step "=== Running Thrift test ==="
    sleep 3

    local gw_ip
    gw_ip=$(kubectl get pod -n "$NS" -l app=dpumesh-gateway -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)
    if [ -z "$gw_ip" ]; then
        err "Gateway pod not found"
        kubectl get pods -n "$NS"
        return 1
    fi

    info "Gateway IP: $gw_ip:$GATEWAY_PORT"

    if [ -f "$PROJ_ROOT/test_thrift.py" ]; then
        info "Running test_thrift.py (5 threads)..."
        python3 "$PROJ_ROOT/test_thrift.py" "$gw_ip" "$GATEWAY_PORT" 5 || true
    else
        warn "test_thrift.py not found, skipping"
    fi
}

### wrk2-style throughput 테스트 (Go client) ###
build_tput_client() {
    if [ ! -f "$PROJ_ROOT/tput_client" ] || \
       [ "$PROJ_ROOT/tput_client.go" -nt "$PROJ_ROOT/tput_client" ]; then
        info "Building tput_client (Go)..."
        if ! command -v go >/dev/null 2>&1; then
            err "go not found in PATH (need Go 1.21+)"
            return 1
        fi
        (cd "$PROJ_ROOT" && go build -o tput_client tput_client.go) || {
            err "go build failed"
            return 1
        }
    fi
    return 0
}

run_throughput_test() {
    step "=== Running Throughput Test (Go client) ==="
    sleep 3

    build_tput_client || return 1

    local gw_ip
    gw_ip=$(kubectl get pod -n "$NS" -l app=dpumesh-gateway -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)
    if [ -z "$gw_ip" ]; then
        err "Gateway pod not found"
        kubectl get pods -n "$NS"
        return 1
    fi

    local rps="${1:-100}"
    local duration="${2:-10}"
    local msg_size="${3:-8192}"
    local conns="${4:-0}"

    info "Gateway: $gw_ip:$GATEWAY_PORT  RPS=$rps  duration=${duration}s  msg=${msg_size}B  conns=${conns} (0=auto)"

    "$PROJ_ROOT/tput_client" \
        -host="$gw_ip" \
        -port="$GATEWAY_PORT" \
        -rps="$rps" \
        -duration="$duration" \
        -msg-size="$msg_size" \
        -conns="$conns"
}

### 고부하 스트레스 테스트 ###
run_stress_test() {
    step "=== Running Stress Test ==="
    sleep 3

    local gw_ip
    gw_ip=$(kubectl get pod -n "$NS" -l app=dpumesh-gateway -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)
    if [ -z "$gw_ip" ]; then
        err "Gateway pod not found"
        kubectl get pods -n "$NS"
        return 1
    fi

    local threads="${1:-10}"
    local reqs="${2:-100}"
    local msg_size="${3:-}"

    info "Gateway IP: $gw_ip:$GATEWAY_PORT"
    if [ -n "$msg_size" ]; then
        info "Stress: $threads threads x $reqs requests = $((threads * reqs)) total, msg_size=$msg_size"
    else
        info "Stress: $threads threads x $reqs requests = $((threads * reqs)) total"
    fi

    if [ -f "$PROJ_ROOT/test_thrift.py" ]; then
        if [ -n "$msg_size" ]; then
            python3 "$PROJ_ROOT/test_thrift.py" "$gw_ip" "$GATEWAY_PORT" stress "$threads" "$reqs" "$msg_size"
        else
            python3 "$PROJ_ROOT/test_thrift.py" "$gw_ip" "$GATEWAY_PORT" stress "$threads" "$reqs"
        fi
    else
        warn "test_thrift.py not found, skipping"
    fi
}

### 사이즈 경계 테스트 ###
run_size_test() {
    step "=== Running DMA Size Boundary Test ==="
    sleep 3

    local gw_ip
    gw_ip=$(kubectl get pod -n "$NS" -l app=dpumesh-gateway -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)
    if [ -z "$gw_ip" ]; then
        err "Gateway pod not found"
        kubectl get pods -n "$NS"
        return 1
    fi

    info "Gateway IP: $gw_ip:$GATEWAY_PORT"

    if [ -f "$PROJ_ROOT/test_thrift.py" ]; then
        python3 "$PROJ_ROOT/test_thrift.py" "$gw_ip" "$GATEWAY_PORT" size
    else
        warn "test_thrift.py not found, skipping"
    fi
}

### 로그 확인 ###
show_logs() {
    echo ""
    step "========== DPU Worker Log (last 50 lines) =========="
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -50 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]' || true

    echo ""
    step "========== Gateway Pod Log (last 30 lines) =========="
    local gw_pod
    gw_pod=$(kubectl get pod -n "$NS" -l app=dpumesh-gateway -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$gw_pod" ]; then
        kubectl logs "$gw_pod" -n "$NS" --tail=30 2>&1 || true
    else
        warn "No gateway pod found"
    fi

    echo ""
    step "========== unique-id-service Pod Log (last 30 lines) =========="
    local uid_pod
    uid_pod=$(kubectl get pod -n "$NS" -l service=unique-id-service -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$uid_pod" ]; then
        kubectl logs "$uid_pod" -n "$NS" --tail=30 2>&1 || true
    else
        warn "No unique-id-service pod found"
    fi
}

### 상태 확인 ###
show_status() {
    step "=== Status ==="
    echo ""

    # DPU process
    info "DPU process:"
    local dpu_pid
    dpu_pid=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S pgrep -f 'dpumesh_dpu.*03:00'" 2>&1 | grep -v '^\[sudo\]' | head -1 || true)
    if [ -n "$dpu_pid" ]; then
        echo "  dpumesh_dpu running (PID: $dpu_pid)"
        ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -1 $DPU_LOG" 2>&1 | grep -v '^\[sudo\]' | sed 's/^/  /'
    else
        echo "  dpumesh_dpu NOT running"
    fi

    echo ""
    info "Pods:"
    kubectl get pods -n "$NS" -o wide 2>/dev/null || echo "  Namespace $NS not found"

    echo ""
    info "Services:"
    kubectl get svc -n "$NS" 2>/dev/null || true
}

### DPU 로그 실시간 ###
follow_dpu_log() {
    info "Following DPU log (Ctrl+C to stop)..."
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -f $DPU_LOG" 2>&1 | grep -v '^\[sudo\]'
}

### Cleanup ###
cleanup() {
    info "Deleting namespace $NS..."
    kubectl delete ns "$NS" --ignore-not-found

    info "Stopping dpumesh_dpu..."
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S killall -9 dpumesh_dpu 2>/dev/null; true" 2>&1 | grep -v '^\[sudo\]' || true

    info "Restoring social-network pods..."
    kubectl scale deployment dpumesh-gateway --replicas=1 -n social-network 2>/dev/null || true
    kubectl scale deployment unique-id-service --replicas=1 -n social-network 2>/dev/null || true

    info "Cleanup done"
}

### Main ###
CMD="${1:-help}"

case "$CMD" in
    deploy)
        # 전체 배포: sync → build → DPU → Gateway → Service → test
        ensure_namespace
        apply_k8s_resources
        scale_down_social_network
        sync_sources
        build_dpu
        build_host
        build_gateway_image
        start_dpu
        start_gateway
        start_service
        run_test
        show_logs
        info "=== Deploy complete ==="
        ;;
    dpu)
        # DPU만: sync → build → restart dpumesh_dpu
        sync_sources
        build_dpu
        start_dpu
        info "DPU ready. Run: $0 restart"
        ;;
    host)
        # Host만: libthrift.so 재빌드
        build_host
        info "Host build done. Pods will pick up new .so on restart."
        info "Run: $0 restart"
        ;;
    restart)
        # Pods만 순서대로 재시작
        scale_down_social_network
        start_gateway
        start_service
        kubectl get pods -n "$NS" -o wide
        info "Pods restarted. Run: $0 test"
        ;;
    test)
        run_test
        ;;
    test-size)
        run_size_test
        ;;
    stress)
        run_stress_test "${2:-10}" "${3:-100}" "${4:-}"
        ;;
    throughput)
        run_throughput_test "${2:-100}" "${3:-10}" "${4:-}" "${5:-}"
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    dpu-log)
        follow_dpu_log
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 {deploy|dpu|host|restart|test|test-size|stress|throughput|logs|status|cleanup|dpu-log}"
        echo ""
        echo "Commands:"
        echo "  deploy   - 전체: sync + build(DPU+Host) + 순서대로 시작 + test"
        echo "  dpu      - DPU만: sync + build + dpumesh_dpu 재시작"
        echo "  host     - Host만: libthrift.so 재빌드"
        echo "  restart  - Pods만: 순서대로 재시작 (Gateway → Service)"
        echo "  test     - test_thrift.py 실행"
        echo "  test-size - DMA 사이즈 경계 테스트 (59~1024B)"
        echo "  stress [T] [N] [SIZE] - 고부하 스트레스 테스트 (T스레드 x N요청, SIZE=메시지크기 예: 8K,128K)"
        echo "  throughput [RPS] [DUR] [SIZE] [CONNS] - wrk2-style 정률 부하 테스트 (Go client; CONNS=0 → auto-size)"
        echo "  logs     - DPU + pod 로그 확인"
        echo "  status   - 전체 상태 확인"
        echo "  dpu-log  - DPU 로그 실시간 follow"
        echo "  cleanup  - 전체 정리 (ns삭제 + DPU중지)"
        echo ""
        echo "개발 사이클:"
        echo "  코드 수정 → $0 deploy          # 전체 재배포+테스트"
        echo "  DPA만 수정 → $0 dpu && $0 restart && $0 test"
        echo "  Host만 수정 → $0 host && $0 restart && $0 test"
        ;;
esac
