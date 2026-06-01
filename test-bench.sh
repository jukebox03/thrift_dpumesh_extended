#!/bin/bash
# test-bench.sh — DPUmesh vs TCP bench (gateway 우회 비교 실험)
#
# 사용법:
#   ./test-bench.sh deploy                         # 전체: build + image + DPU restart + pods 기동
#   ./test-bench.sh dpumesh <RPS> <DUR> <SIZE> [<CONNS>]
#   ./test-bench.sh tcp     <RPS> <DUR> <SIZE> [<CONNS>]
#   ./test-bench.sh logs                           # bench/echo pod 로그
#   ./test-bench.sh status                         # 상태 확인
#   ./test-bench.sh cleanup                        # ns 삭제 + DPU 중지
#
# 구조:
#   bench-dpumesh (pod_id=10) ──dpumesh──▶ echo-dpumesh (pod_id=11)
#   Core 2 = bench-tcp pod                Core 3 = echo-tcp pod
#   ┌──────────────────────┐              ┌──────────────────────┐
#   │ bench   (taskset 2)  │   k8s svc    │ sidecar2 (taskset 3) │
#   │   ↓ 127.0.0.1:9091   │ ───TCP────▶  │   ↓ 127.0.0.1:9092   │
#   │ sidecar1 (taskset 2) │              │ echo-tcp (taskset 3) │
#   └──────────────────────┘              └──────────────────────┘
#   Core 0: bench-dpumesh   Core 1: echo-dpumesh   (single container each)
#
#   각 Envoy: tcp_proxy filter만, L7 parse·tracing·stats sink·admin 모두 제거.
#   sidecar는 app과 같은 pod·같은 core에서 도므로 자원 경합 발생 (실제 Istio
#   sidecar 모델). DPUmesh 측은 transport가 host CPU 외부(DPU/DPA)이므로
#   같은 1 core 안에서 app만 풀로 사용 가능 — 이 차이가 비교의 핵심.
#   bench-* daemon은 control TCP 9092로 RUN/PING 명령 수신.
#
# 기존 test-dpumesh.sh와 같은 DPU를 사용하므로 한 번에 한쪽만 deploy 가능.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

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
NS="test-bench"
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRANSPORT_SRC="$PROJ_ROOT/lib/cpp/src/thrift/transport"
DOCA_SRC="$TRANSPORT_SRC/doca"
DPU_TRANSPORT="thrift_dpumesh_extended/lib/cpp/src/thrift/transport"
DPU_DOCA="$DPU_TRANSPORT/doca"
DPU_BUILD="$DPU_DOCA/build"
BUILD_DOCA="$PROJ_ROOT/build-doca"
BENCH_DIR="$PROJ_ROOT/bench"
DPU_LOG="/tmp/dpumesh_dpu_bench.log"
DOCA_LIB_DIR="/opt/mellanox/doca/lib/x86_64-linux-gnu"
FLEXIO_LIB_DIR="/opt/mellanox/flexio/lib"

IMG_BENCH_DPU="bench/bench-dpumesh:latest"
IMG_ECHO_DPU="bench/echo-dpumesh:latest"
IMG_BENCH_TCP="bench/bench-tcp:latest"
IMG_ECHO_TCP="bench/echo-tcp:latest"
IMG_ENVOY="envoyproxy/envoy:v1.30-latest"
CTRL_PORT=9092
TCP_PORT=9091

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "${RED}[ERR]${NC} $*"; }
step()  { echo -e "${BLUE}[STEP]${NC} $*"; }

dpu_sudo() {
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash -c '$1'" 2>&1 | sed 's/^\[sudo\][^:]*: *//'
}

### --------------------------------------------------------------- 빌드 ###

sync_sources() {
    step "=== Syncing sources to DPU ==="
    rsync -avz --delete \
        --exclude='build/' --exclude='builddir/' --exclude='doca/' \
        --exclude='*.o' --exclude='*.a' \
        "$DOCA_SRC/" "$DPU_HOST:~/$DPU_DOCA/"
    rsync -avz \
        "$TRANSPORT_SRC/dpumesh_doca.c" \
        "$TRANSPORT_SRC/dpumesh.h" \
        "$TRANSPORT_SRC/TDpumeshTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshTransport.h" \
        "$TRANSPORT_SRC/TDpumeshServerTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshServerTransport.h" \
        "$TRANSPORT_SRC/TDpumeshClientTransport.cpp" \
        "$TRANSPORT_SRC/TDpumeshClientTransport.h" \
        "$DPU_HOST:~/$DPU_TRANSPORT/" 2>/dev/null || true
    ssh "$DPU_HOST" "find ~/$DPU_DOCA -type f -exec touch {} + && find ~/$DPU_TRANSPORT -maxdepth 1 -type f -exec touch {} +" 2>/dev/null || true
    info "Source sync complete"
}

build_dpu() {
    step "=== Building on DPU (ninja) ==="
    ssh "$DPU_HOST" "rm -f ~/$DPU_BUILD/dpa_kernel.a" 2>/dev/null || true
    local out
    out=$(ssh "$DPU_HOST" "cd ~/$DPU_BUILD && ninja" 2>&1)
    if echo "$out" | grep -q "error:"; then
        err "DPU build failed:"; echo "$out"; exit 1
    fi
    info "DPU build OK"
}

build_host() {
    step "=== Building host libthrift.so ==="
    if [ ! -f "$BUILD_DOCA/Makefile" ]; then
        info "Running cmake..."
        mkdir -p "$BUILD_DOCA"
        (cd "$BUILD_DOCA" && cmake "$PROJ_ROOT" -DWITH_DOCA=ON -DWITH_CPP=ON -DWITH_C_GLIB=ON \
            -DWITH_SHARED_LIB=ON -DWITH_STATIC_LIB=ON -DWITH_LIBEVENT=ON -DWITH_OPENSSL=ON -DWITH_ZLIB=ON \
            -DBUILD_COMPILER=OFF -DBUILD_TESTING=OFF -DBUILD_TUTORIALS=OFF -DBUILD_EXAMPLES=OFF \
            -DWITH_JAVA=OFF -DWITH_PYTHON=OFF -DWITH_HASKELL=OFF)
    fi
    if ! (cd "$BUILD_DOCA" && make -j"$(nproc)" 2>&1 | tail -20); then
        err "Host build failed"; exit 1
    fi
    rm -rf "$PROJ_ROOT/thrift-install"
    (cd "$BUILD_DOCA" && make install DESTDIR="$PROJ_ROOT/thrift-install" >/dev/null)
    # de-debug suffix symlinks (libthriftd.so → libthrift.so) so Dockerfile glob matches
    local THRIFT_LIB_DIR="$PROJ_ROOT/thrift-install/usr/local/lib"
    for f in "$THRIFT_LIB_DIR"/lib*d.so; do
        [ -f "$f" ] || continue
        ln -sf "$(basename "$f")" "$THRIFT_LIB_DIR/$(basename "$f" | sed 's/d\.so$/.so/')"
    done
    for f in "$THRIFT_LIB_DIR"/lib*d.so.0.12.0; do
        [ -f "$f" ] || continue
        ln -sf "$(basename "$f")" "$THRIFT_LIB_DIR/$(basename "$f" | sed 's/d\.so\.0\.12\.0$/.so.0.12.0/')"
    done
    info "Host build OK"
}

collect_doca_libs() {
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
}

build_bench_binaries() {
    step "=== Building bench/echo binaries ==="
    local THRIFT_LINK_LIB="-lthriftd"
    if [ ! -e "$BUILD_DOCA/lib/libthriftd.so" ] && [ ! -e "$BUILD_DOCA/lib/libthriftd.a" ]; then
        THRIFT_LINK_LIB="-lthrift"
    fi
    gcc -O2 -o "$BENCH_DIR/bench_dpumesh" "$BENCH_DIR/bench_dpumesh.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    gcc -O2 -o "$BENCH_DIR/echo_dpumesh" "$BENCH_DIR/echo_dpumesh.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    info "C bench binaries built"

    if ! command -v go >/dev/null 2>&1; then
        err "go not found in PATH"; exit 1
    fi
    (cd "$BENCH_DIR" && go build -o bench_tcp bench_tcp.go && go build -o echo_tcp echo_tcp.go)
    info "Go bench binaries built"
}

build_image() {
    # $1 = Dockerfile basename, $2 = image tag, $3 = build context
    local dockerfile="$1" tag="$2" ctx="$3"
    docker build -f "$dockerfile" -t "$tag" "$ctx"
    sudo ctr -n k8s.io images rm "docker.io/$tag" 2>/dev/null || true
    docker save "$tag" | sudo ctr -n k8s.io images import -
    docker image prune -f >/dev/null 2>&1 || true
}

build_images() {
    step "=== Building Docker images ==="
    collect_doca_libs
    echo "$HOST_PASS" | sudo -S true 2>/dev/null

    # bench/echo dpumesh — need libthrift + DOCA libs from PROJ_ROOT
    cp -f "$BENCH_DIR/bench_dpumesh" "$PROJ_ROOT/"
    build_image "$BENCH_DIR/Dockerfile.bench_dpumesh" "$IMG_BENCH_DPU" "$PROJ_ROOT"
    rm -f "$PROJ_ROOT/bench_dpumesh"

    cp -f "$BENCH_DIR/echo_dpumesh" "$PROJ_ROOT/"
    build_image "$BENCH_DIR/Dockerfile.echo_dpumesh" "$IMG_ECHO_DPU" "$PROJ_ROOT"
    rm -f "$PROJ_ROOT/echo_dpumesh"

    # tcp bench — slim, build from BENCH_DIR directly
    build_image "$BENCH_DIR/Dockerfile.bench_tcp" "$IMG_BENCH_TCP" "$BENCH_DIR"
    build_image "$BENCH_DIR/Dockerfile.echo_tcp" "$IMG_ECHO_TCP" "$BENCH_DIR"

    # Reclaim Docker build cache. It grows by GBs every deploy (4 builds) and
    # `docker image prune` does NOT touch it. Left unchecked the disk fills,
    # the kubelet hits DiskPressure and evicts the BestEffort bench/echo pods —
    # leaving Failed-phase corpses that break the next deploy's `kubectl wait`.
    docker builder prune -f >/dev/null 2>&1 || true

    info "All images built and imported to containerd"
}

ensure_envoy_image() {
    local img="$IMG_ENVOY"
    if echo "$HOST_PASS" | sudo -S ctr -n k8s.io images list -q 2>/dev/null \
           | grep -v '^\[sudo\]' | grep -q "docker.io/$img"; then
        info "Envoy image already in containerd"
        return 0
    fi
    if ! docker image inspect "$img" >/dev/null 2>&1; then
        info "Pulling Envoy image: $img"
        docker pull "$img" || { err "docker pull $img failed"; exit 1; }
    fi
    info "Importing Envoy image to containerd k8s.io ns..."
    echo "$HOST_PASS" | sudo -S true 2>/dev/null
    docker save "$img" | sudo ctr -n k8s.io images import -
}

### ---------------------------------------------------------------- DPU ###

stop_dpu() {
    info "Stopping dpumesh_dpu..."
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S killall -9 dpumesh_dpu 2>/dev/null; true" 2>&1 | sed 's/^\[sudo\][^:]*: *//' || true
    sleep 5
}

start_dpu() {
    local dpa_threads="${DPUMESH_DPA_THREADS:-1}"
    local dpa_affinity="${DPUMESH_DPA_AFFINITY:-1}"
    step "=== Starting dpumesh_dpu on DPU (DPA EU threads=$dpa_threads, affinity=$dpa_affinity) ==="
    stop_dpu
    ssh "$DPU_HOST" "cat > /tmp/start_dpu_bench.sh << 'LAUNCHER'
#!/bin/bash
screen -dmS dpumesh-bench bash -c \"cd /home/jukebox/$DPU_BUILD && DPUMESH_DPA_THREADS=$dpa_threads DPUMESH_DPA_AFFINITY=$dpa_affinity ./dpumesh_dpu $DPU_PCI -l 40 > $DPU_LOG 2>&1\"
sleep 2
pgrep -f 'dpumesh_dpu.*03:00' || echo NO_PID
LAUNCHER
chmod +x /tmp/start_dpu_bench.sh"

    local pid
    pid=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash /tmp/start_dpu_bench.sh" 2>&1 | sed 's/^\[sudo\][^:]*: *//')
    if [ "$pid" = "NO_PID" ] || [ -z "$pid" ]; then
        err "dpumesh_dpu failed to start"; exit 1
    fi
    info "dpumesh_dpu running (PID: $pid)"
}

### --------------------------------------------------------- CPU pinning ###
#
# Hard-pin each pod to a dedicated host core via taskset on the container PID
# (and immediate children). Multi-container pods (bench-tcp/echo-tcp) get
# both containers pinned to the SAME core, forcing app↔sidecar to share —
# the realistic Istio "tax" of co-located proxy. CFS quotas are NOT used
# because they only cap CPU time, not which cores; pinning is the sole
# enforcer here.
#
# DVFS is also frozen at 2.5 GHz (cores 0-7) so latency tail noise from
# frequency scaling doesn't mask transport-level differences.

#
# Pin profiles:
#   fair (default): 1 host core per pod. dpumesh side gets 1 core for app
#                   (transport on DPU/DPA), tcp side gets 1 core shared
#                   between app + sidecar. This is the apples-to-apples
#                   Istio-like comparison.
#   hw            : multi-core for dpumesh side. Goal is to remove host
#                   software bottleneck so dpumesh can approach the
#                   chain ceiling (Method 2 = 66K RPS). TCP side untouched
#                   since that comparison only makes sense in fair mode.
#
get_pod_cores() {
    local app="$1" profile="${2:-fair}"
    case "$profile" in
        hw)
            case "$app" in
                bench-dpumesh) echo "0,4" ;;
                echo-dpumesh)  echo "1,5" ;;
                bench-tcp)     echo "2" ;;   # untouched
                echo-tcp)      echo "3" ;;   # untouched
                *) echo "" ;;
            esac
            ;;
        fair|*)
            case "$app" in
                bench-dpumesh) echo "0" ;;
                echo-dpumesh)  echo "1" ;;
                bench-tcp)     echo "2" ;;   # bench + sidecar1 share core 2
                echo-tcp)      echo "3" ;;   # echo  + sidecar2 share core 3
                *) echo "" ;;
            esac
            ;;
    esac
}

pin_pods() {
    local profile="${1:-fair}"
    step "=== Pinning pods to dedicated cores (taskset, profile=$profile) ==="
    if ! command -v jq >/dev/null 2>&1; then
        err "jq not found — needed to parse crictl output. apt install jq"
        return 1
    fi

    if command -v cpupower >/dev/null 2>&1; then
        info "CPU governor=performance, fixed 2.5 GHz on cores 0-7"
        echo "$HOST_PASS" | sudo -S cpupower -c 0-7 frequency-set -g performance >/dev/null 2>&1 || true
        echo "$HOST_PASS" | sudo -S cpupower -c 0-7 frequency-set -d 2.5GHz -u 2.5GHz >/dev/null 2>&1 || true
    else
        warn "cpupower not found; skipping DVFS lock"
    fi

    for app in bench-dpumesh echo-dpumesh bench-tcp echo-tcp; do
        local cores pod_id
        cores=$(get_pod_cores "$app" "$profile")
        [ -z "$cores" ] && continue

        pod_id=$(echo "$HOST_PASS" | sudo -S crictl pods --label "app=$app" -q 2>/dev/null | head -n 1)
        if [ -z "$pod_id" ]; then
            warn "$app: pod not found, skipping"
            continue
        fi
        info "$app → core(s) $cores (pod=$pod_id)"

        for cid in $(echo "$HOST_PASS" | sudo -S crictl ps --pod "$pod_id" -q 2>/dev/null); do
            local cname pid
            cname=$(echo "$HOST_PASS" | sudo -S crictl inspect "$cid" 2>/dev/null | jq -r '.status.metadata.name' 2>/dev/null)
            pid=$(echo "$HOST_PASS"   | sudo -S crictl inspect "$cid" 2>/dev/null | jq -r '.info.pid'              2>/dev/null)
            if [ -z "$pid" ] || [ "$pid" = "null" ]; then continue; fi
            info "  $cname (PID $pid) → $cores"
            # -a = all threads; -p = pid; -c = cpu list. Includes the main
            # process and any pre-existing kernel-side threads. New child
            # processes inherit affinity automatically.
            echo "$HOST_PASS" | sudo -S taskset -apc "$cores" "$pid" >/dev/null
            for child in $(pgrep -P "$pid" 2>/dev/null); do
                echo "$HOST_PASS" | sudo -S taskset -apc "$cores" "$child" >/dev/null 2>&1 || true
            done
        done
    done
    info "Pinning done"
}

### ---------------------------------------------------------------- K8s ###

ensure_namespace() {
    # If a previous cleanup left the namespace in Terminating state, wait
    # for it to finish before creating a new one. Without this wait, every
    # subsequent kubectl apply fails with "is being terminated".
    local phase
    phase=$(kubectl get ns "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$phase" = "Terminating" ]; then
        info "Namespace $NS is Terminating — waiting up to 120s..."
        local i=0
        while [ $i -lt 60 ]; do
            kubectl get ns "$NS" &>/dev/null || break
            sleep 2
            i=$((i + 1))
        done
        if kubectl get ns "$NS" &>/dev/null; then
            err "Namespace $NS still Terminating after 120s; aborting"
            exit 1
        fi
        phase=""
    fi
    if [ "$phase" != "Active" ]; then
        info "Creating namespace $NS"
        kubectl create ns "$NS"
    fi
}

apply_k8s() {
    step "=== Applying K8s resources (replicas=0) ==="

    cat <<EOF | kubectl apply -n "$NS" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bench-dpumesh
spec:
  replicas: 0
  selector: { matchLabels: { app: bench-dpumesh } }
  template:
    metadata: { labels: { app: bench-dpumesh } }
    spec:
      hostname: bench-dpumesh
      containers:
      - name: bench-dpumesh
        image: docker.io/$IMG_BENCH_DPU
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "10" }
        - { name: BENCH_DST_POD_ID, value: "11" }
        securityContext: { privileged: true }
        # CPU 1-core 제한은 pin_pods()의 taskset으로 처리 (CFS quota 미사용).
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
apiVersion: v1
kind: Service
metadata: { name: bench-dpumesh }
spec:
  selector: { app: bench-dpumesh }
  ports: [{ port: $CTRL_PORT, targetPort: $CTRL_PORT }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-dpumesh
spec:
  replicas: 0
  selector: { matchLabels: { app: echo-dpumesh } }
  template:
    metadata: { labels: { app: echo-dpumesh } }
    spec:
      hostname: echo-dpumesh
      containers:
      - name: echo-dpumesh
        image: docker.io/$IMG_ECHO_DPU
        imagePullPolicy: Never
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "11" }
        - { name: ECHO_THREADS, value: "64" }
        securityContext: { privileged: true }
        # CPU 1-core 제한은 pin_pods()의 taskset으로 처리.
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bench-tcp
spec:
  replicas: 0
  selector: { matchLabels: { app: bench-tcp } }
  template:
    metadata: { labels: { app: bench-tcp } }
    spec:
      # === Realistic Istio sidecar pattern: bench app + sidecar1 (Envoy)
      # in the SAME pod, sharing 1 host core (CFS quota 0.5 + 0.5 = 1).
      # bench connects to localhost:$TCP_PORT (= sidecar1 listener);
      # sidecar1 forwards to echo-tcp Service (which routes to sidecar2).
      # CPU pinning (1 core, shared between bench + sidecar1) is applied
      # post-deploy via pin_pods() — both containers' PIDs taskset'd to
      # the same core.
      containers:
      - name: bench-tcp
        image: docker.io/$IMG_BENCH_TCP
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: BENCH_TARGET, value: "127.0.0.1:$TCP_PORT" }
      - name: sidecar1
        image: docker.io/$IMG_ENVOY
        imagePullPolicy: IfNotPresent
        args: ["-c", "/etc/envoy/envoy.yaml", "--log-level", "warn"]
        ports: [{ containerPort: $TCP_PORT }]
        volumeMounts:
        - { mountPath: /etc/envoy, name: sidecar1-config, readOnly: true }
      volumes:
      - { name: sidecar1-config, configMap: { name: sidecar1-config } }
---
apiVersion: v1
kind: Service
metadata: { name: bench-tcp }
spec:
  selector: { app: bench-tcp }
  ports: [{ port: $CTRL_PORT, targetPort: $CTRL_PORT }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-tcp
spec:
  replicas: 0
  selector: { matchLabels: { app: echo-tcp } }
  template:
    metadata: { labels: { app: echo-tcp } }
    spec:
      # echo app + sidecar2 in same pod sharing 1 host core.
      # echo listens on 9092 (internal); sidecar2 listens on 9091 and
      # forwards to 127.0.0.1:9092. Service targetPort 9091 → sidecar2.
      # CPU pinning (1 core, shared between echo + sidecar2) via pin_pods().
      containers:
      - name: echo-tcp
        image: docker.io/$IMG_ECHO_TCP
        imagePullPolicy: Never
        args: ["-port", "9092"]
        ports: [{ containerPort: 9092 }]
      - name: sidecar2
        image: docker.io/$IMG_ENVOY
        imagePullPolicy: IfNotPresent
        args: ["-c", "/etc/envoy/envoy.yaml", "--log-level", "warn"]
        ports: [{ containerPort: $TCP_PORT }]
        volumeMounts:
        - { mountPath: /etc/envoy, name: sidecar2-config, readOnly: true }
      volumes:
      - { name: sidecar2-config, configMap: { name: sidecar2-config } }
---
apiVersion: v1
kind: Service
metadata: { name: echo-tcp }
spec:
  selector: { app: echo-tcp }
  ports: [{ port: $TCP_PORT, targetPort: $TCP_PORT }]
---
# === sidecar1 ConfigMap (mounted as a container in bench-tcp pod) ===
# tcp_proxy filter only. No HTTP/L7 parse, no admin, no tracing, no stats sink,
# no access log, no runtime — minimum viable Envoy doing pure TCP forwarding
# at userspace. Sidecar runs IN the bench-tcp pod (sharing 1 host core), and
# forwards to the echo-tcp Service which targets sidecar2 in the echo-tcp pod.
apiVersion: v1
kind: ConfigMap
metadata: { name: sidecar1-config }
data:
  envoy.yaml: |
    static_resources:
      listeners:
      - name: tcp_listener
        address:
          socket_address: { address: 0.0.0.0, port_value: $TCP_PORT }
        filter_chains:
        - filters:
          - name: envoy.filters.network.tcp_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
              stat_prefix: tcp
              cluster: upstream
      clusters:
      - name: upstream
        type: STRICT_DNS
        connect_timeout: 5s
        load_assignment:
          cluster_name: upstream
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  socket_address: { address: echo-tcp, port_value: $TCP_PORT }
---
# === sidecar2 ConfigMap (mounted as a container in echo-tcp pod) ===
# Forwards to the echo container in the SAME pod via 127.0.0.1:9092.
apiVersion: v1
kind: ConfigMap
metadata: { name: sidecar2-config }
data:
  envoy.yaml: |
    static_resources:
      listeners:
      - name: tcp_listener
        address:
          socket_address: { address: 0.0.0.0, port_value: $TCP_PORT }
        filter_chains:
        - filters:
          - name: envoy.filters.network.tcp_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
              stat_prefix: tcp
              cluster: upstream
      clusters:
      - name: upstream
        connect_timeout: 5s
        load_assignment:
          cluster_name: upstream
          endpoints:
          - lb_endpoints:
            - endpoint:
                address:
                  socket_address: { address: 127.0.0.1, port_value: 9092 }
EOF
    info "K8s resources applied"
}

scale_up_with_wait() {
    local app="$1" expected_log="$2"
    kubectl scale deployment "$app" --replicas=0 -n "$NS" 2>/dev/null || true
    sleep 1
    kubectl scale deployment "$app" --replicas=1 -n "$NS"
    if ! kubectl wait --for=condition=Ready pod -l "app=$app" -n "$NS" --timeout=120s 2>&1; then
        err "$app failed to start"
        kubectl describe pod -l "app=$app" -n "$NS" | tail -15
        exit 1
    fi
    info "$app pod Ready"

    if [ -n "$expected_log" ]; then
        info "Waiting for DPU register: $expected_log"
        local attempts=0
        while [ $attempts -lt 15 ]; do
            local line
            line=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -3 $DPU_LOG" 2>&1 | sed 's/^\[sudo\][^:]*: *//' || true)
            if echo "$line" | grep -q "$expected_log"; then
                info "DPU registered ($expected_log)"
                return 0
            fi
            sleep 1
            attempts=$((attempts + 1))
        done
        warn "DPU register timeout — continuing"
    fi
}

start_pods() {
    step "=== Starting pods (innermost first) ==="
    # DPUmesh: echo first (so bench finds dst), then bench
    scale_up_with_wait "echo-dpumesh"  "pods: 1"
    scale_up_with_wait "bench-dpumesh" "pods: 2"
    # TCP: echo-tcp pod (echo + sidecar2) first, then bench-tcp pod
    # (bench + sidecar1). Sidecars are containers in these pods, not
    # standalone deployments. STRICT_DNS lazy resolution makes order
    # tolerant, but innermost-first keeps first connect Ready.
    scale_up_with_wait "echo-tcp"  ""
    scale_up_with_wait "bench-tcp" ""
}

### -------------------------------------------------------------- 실행 ###

ctrl_send() {
    # $1 = bench app label, $2 = command line
    local app="$1" cmd="$2"
    local pod_ip
    pod_ip=$(kubectl get pod -n "$NS" -l "app=$app" --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$pod_ip" ]; then
        err "$app pod not found"
        return 1
    fi
    info "Sending '$cmd' to $app ($pod_ip:$CTRL_PORT)"
    # nc with read timeout: bench may take dur+grace seconds
    printf '%s\n' "$cmd" | nc -q 0 "$pod_ip" "$CTRL_PORT"
}

run_bench() {
    # $1 = "dpumesh"|"tcp", $2..$5 = rps dur size [conns]
    local mode="$1"; shift
    local rps="${1:-10000}" dur="${2:-10}" size="${3:-8192}" conns="${4:-0}"
    local app="bench-${mode}"

    local cmd="RUN $rps $dur $size"
    [ "$conns" != "0" ] && cmd="$cmd $conns"

    step "=== $mode bench: rps=$rps dur=${dur}s size=${size}B conns=${conns} ==="
    local timeout_sec=$((dur + 30))
    local pod_ip
    pod_ip=$(kubectl get pod -n "$NS" -l "app=$app" --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$pod_ip" ]; then
        err "$app pod not found — run '$0 deploy' first"
        return 1
    fi

    local resp
    resp=$(printf '%s\n' "$cmd" | timeout "${timeout_sec}s" nc "$pod_ip" "$CTRL_PORT" || true)
    if [ -z "$resp" ]; then
        err "no response (timeout or pod down)"
        return 1
    fi

    if [[ "$resp" == ERR* ]]; then
        err "bench replied: $resp"
        return 1
    fi

    # Parse: OK <rps> <p50> <p99> <p999> <ok> <fail> <mb_s>
    read -r tag rps_a p50 p99 p999 ok fail mbs <<<"$resp"
    echo
    echo "============================================================"
    echo "  $mode bench result"
    echo "============================================================"
    printf "  Achieved RPS:   %s\n"   "$rps_a"
    printf "  p50 latency:    %s us\n" "$p50"
    printf "  p99 latency:    %s us\n" "$p99"
    printf "  p999 latency:   %s us\n" "$p999"
    printf "  OK / Fail:      %s / %s\n" "$ok" "$fail"
    printf "  Throughput:     %s MB/s (RTT)\n" "$mbs"
    echo "============================================================"
}

### ---------------------------------------------------------- utility ###

show_logs() {
    # bench-tcp/echo-tcp pods now have 2 containers each (app + sidecar);
    # --all-containers prefixes each line with the container name.
    for app in bench-dpumesh echo-dpumesh bench-tcp echo-tcp; do
        echo "=== $app ==="
        kubectl logs -n "$NS" -l "app=$app" --all-containers=true --prefix=true --tail=20 2>/dev/null || true
        echo
    done
}

show_status() {
    echo "=== pods ===";    kubectl get pods    -n "$NS" -o wide
    echo "=== services ==="; kubectl get svc     -n "$NS"
    echo "=== deploys ===";  kubectl get deploy  -n "$NS"
}

cleanup() {
    info "Deleting namespace $NS (waiting for full termination)"
    # Synchronous delete so a follow-up `deploy` can recreate without racing
    # the Terminating state. ensure_namespace also handles the leftover case,
    # but waiting here makes the cleanup→deploy sequence predictable.
    kubectl delete ns "$NS" --ignore-not-found=true 2>/dev/null || true
    stop_dpu
}

# Remove terminal-phase (Evicted/Error) pods. Kubernetes never auto-GCs Failed
# pods, so after a DiskPressure eviction they linger indefinitely under the same
# `app=` label — breaking `kubectl wait` (deploy hangs → exit 1) and run_bench's
# `.items[0]` pod-IP lookup. Clear them before (re)starting so selectors only
# ever see live pods.
clean_failed_pods() {
    local n
    n=$(kubectl get pods -n "$NS" --field-selector=status.phase=Failed --no-headers 2>/dev/null | wc -l)
    if [ "$n" -gt 0 ]; then
        info "Removing $n stale Failed/Evicted pod(s) in $NS"
        kubectl delete pod -n "$NS" --field-selector=status.phase=Failed --ignore-not-found=true >/dev/null 2>&1 || true
    fi
}

### ---------------------------------------------------------- main ###

CMD="${1:-help}"

case "$CMD" in
    deploy)
        ensure_namespace
        clean_failed_pods
        apply_k8s
        sync_sources
        build_dpu
        build_host
        build_bench_binaries
        build_images
        ensure_envoy_image
        start_dpu
        start_pods
        pin_pods fair
        info "=== Deploy complete ==="
        echo
        echo "  Run (fair 1-core/pod, TCP vs DPUmesh 비교용):"
        echo "    $0 dpumesh    <RPS> <DUR> <SIZE> [<CONNS>]"
        echo "    $0 tcp        <RPS> <DUR> <SIZE> [<CONNS>]"
        echo "  Run (HW limit chase, dpumesh 측만 multi-core):"
        echo "    $0 dpumesh-hw <RPS> <DUR> <SIZE> [<CONNS>]"
        echo "  If pods restart, re-pin: $0 pin (or pin-hw)"
        ;;
    dpumesh)
        # fair-mode가 묵시적 default. 직전이 hw-mode였으면 fair로 되돌리는 게
        # 안전함 — re-pin 비용은 한 번 작은 taskset 호출들이라 무시 가능.
        pin_pods fair >/dev/null
        run_bench "dpumesh" "${@:2}"
        ;;
    dpumesh-hw)
        # HW limit chase: dpumesh 측만 multi-core. echo-dpumesh "1,5", bench
        # "0,4". 이 모드의 결과는 TCP와 직접 비교 불가 (자원 비대칭) — 오직
        # chain ceiling 까지 dpumesh 가 도달하는지 보는 용도.
        pin_pods hw >/dev/null
        run_bench "dpumesh" "${@:2}"
        ;;
    tcp)
        # TCP는 항상 fair-mode로 강제 (B안 자체가 1-core 비교 전제)
        pin_pods fair >/dev/null
        run_bench "tcp" "${@:2}"
        ;;
    pin|pin-fair)
        pin_pods fair
        ;;
    pin-hw)
        pin_pods hw
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 {deploy|dpumesh|tcp|dpumesh-hw|pin|pin-hw|logs|status|cleanup}"
        echo
        echo "  deploy                                    # 전체 배포 (fair 핀 자동)"
        echo "  dpumesh     <RPS> <DUR> <SIZE> [<CONNS>]  # 1-core fair (TCP 대조군용)"
        echo "  tcp         <RPS> <DUR> <SIZE> [<CONNS>]  # 1-core fair (sidecar 모델)"
        echo "  dpumesh-hw  <RPS> <DUR> <SIZE> [<CONNS>]  # multi-core (HW 한계 측정)"
        echo "  pin / pin-fair                            # fair 모드 재핀"
        echo "  pin-hw                                    # hw 모드 재핀 (수동 토글)"
        echo "  logs                                      # bench/echo pod 로그"
        echo "  status                                    # 상태"
        echo "  cleanup                                   # ns 삭제 + DPU 중지"
        echo
        echo "Note: 같은 DPU를 사용하므로 test-dpumesh.sh와 동시 deploy 불가"
        echo "      pin profile은 dpumesh/dpumesh-hw/tcp 명령마다 자동으로 맞춰줌"
        ;;
esac
