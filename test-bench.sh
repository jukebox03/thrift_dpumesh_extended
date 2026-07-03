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
IMG_LOOPBACK_DPU="bench/loopback-dpumesh:latest"   # self-routing validator (1 pod = client+server)
IMG_PRELOAD_DPU="bench/preload-dpumesh:latest"     # LD_PRELOAD shim validator (vanilla TCP apps)
IMG_STREAM_DPU="bench/stream-dpumesh:latest"       # byte-stream / L7-proxy frame validator (DPUMESH_PROXY=frame)
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
    # Optimization level for the ARM control plane (dpumesh_dpu). meson bakes
    # buildtype at `meson setup` time, so a plain `ninja` will NOT re-apply a
    # buildtype change to an already-configured build dir. `meson configure`
    # forces it (idempotent) and marks the dir dirty so the .o files actually
    # rebuild with the new -O level. Default debugoptimized (-O2 -g); override
    # with DPU_BUILDTYPE=debug for an -O0 debugging build.
    local bt="${DPU_BUILDTYPE:-debugoptimized}"
    ssh "$DPU_HOST" "rm -f ~/$DPU_BUILD/dpa_kernel.a" 2>/dev/null || true
    local out
    out=$(ssh "$DPU_HOST" "cd ~/$DPU_BUILD && meson configure -Dbuildtype=$bt 2>&1; ninja 2>&1" 2>&1)
    if echo "$out" | grep -q "error:"; then
        err "DPU build failed:"; echo "$out"; exit 1
    fi
    local nobj
    nobj=$(echo "$out" | grep -cE "Compiling C object" || true)
    info "DPU build OK (buildtype=$bt, recompiled $nobj C objects)"
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
    # Client + echo server are the socket/epoll-façade implementations
    # (bench_sock.c / echo_sock.c over dpm.h). Binary names stay
    # bench_dpumesh / echo_dpumesh (the Docker images + k8s reference those names).
    gcc -O2 -o "$BENCH_DIR/bench_dpumesh" "$BENCH_DIR/bench_sock.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    gcc -O2 -o "$BENCH_DIR/echo_dpumesh" "$BENCH_DIR/echo_sock.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    # loopback validator: one pod is BOTH client and server of its own service
    # (self-routing / loopback proof for the oriented-tuple demux).
    gcc -O2 -o "$BENCH_DIR/loopback_dpumesh" "$BENCH_DIR/loopback_sock.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    # byte-stream / L7-proxy frame validator (drives DPUMESH_PROXY=frame): sends
    # length-prefixed frames, verifies byte-exact echo + served-byte exact-count.
    gcc -O2 -o "$BENCH_DIR/stream_dpumesh" "$BENCH_DIR/stream_sock.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    info "C bench binaries built (façade: bench_sock.c + echo_sock.c + loopback_sock.c + stream_sock.c)"

    # LD_PRELOAD socket shim + its validators. tcp_echo / tcp_client are PURE
    # POSIX socket programs (no dmesh headers) — running them unmodified over
    # DPUmesh via the shim is exactly what the preload test proves.
    gcc -O2 -fPIC -shared -o "$BENCH_DIR/libdmesh_preload.so" \
        "$TRANSPORT_SRC/dmesh_preload.c" \
        -I"$PROJ_ROOT/lib/cpp/src" \
        -L"$BUILD_DOCA/lib" -L"$DOCA_LIB_DIR" \
        $THRIFT_LINK_LIB -lpthread -ldl -ldoca_common -ldoca_comch \
        -Wl,-rpath,/usr/local/lib -Wl,-rpath,"$DOCA_LIB_DIR"
    gcc -O2 -o "$BENCH_DIR/tcp_echo"       "$BENCH_DIR/tcp_echo.c"
    gcc -O2 -o "$BENCH_DIR/tcp_client"     "$BENCH_DIR/tcp_client.c" -lpthread
    gcc -O2 -o "$BENCH_DIR/preload_runner" "$BENCH_DIR/preload_runner.c"
    info "LD_PRELOAD shim + vanilla TCP validators built"

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

    cp -f "$BENCH_DIR/loopback_dpumesh" "$PROJ_ROOT/"
    build_image "$BENCH_DIR/Dockerfile.loopback_dpumesh" "$IMG_LOOPBACK_DPU" "$PROJ_ROOT"
    rm -f "$PROJ_ROOT/loopback_dpumesh"

    cp -f "$BENCH_DIR/stream_dpumesh" "$PROJ_ROOT/"
    build_image "$BENCH_DIR/Dockerfile.stream_dpumesh" "$IMG_STREAM_DPU" "$PROJ_ROOT"
    rm -f "$PROJ_ROOT/stream_dpumesh"

    cp -f "$BENCH_DIR/preload_runner" "$BENCH_DIR/tcp_echo" "$BENCH_DIR/tcp_client" \
          "$BENCH_DIR/libdmesh_preload.so" "$PROJ_ROOT/"
    build_image "$BENCH_DIR/Dockerfile.preload_dpumesh" "$IMG_PRELOAD_DPU" "$PROJ_ROOT"
    rm -f "$PROJ_ROOT/preload_runner" "$PROJ_ROOT/tcp_echo" "$PROJ_ROOT/tcp_client" \
          "$PROJ_ROOT/libdmesh_preload.so"

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
    # DPU log level (40=WARN+; 50=INFO+).
    local log_level="${DPUMESH_LOG_LEVEL:-40}"
    # L7-proxy L4 engine (api.md §8, dpu_proxy.c). Unset (default) = engine off,
    # legacy per-slot path bit-identical. "passthru" = one seg per message (wire-
    # identical boundaries; parity/regression). "frame" = length-prefix byte-stream
    # parser routed by svc byte (drive it with the `stream` command / stream_sock.c).
    # (DPA EU count, rings/pod, event-loop are baked into the binary — no longer env.)
    local proxy="${DPUMESH_PROXY:-}"
    step "=== Starting dpumesh_dpu (proxy='$proxy') ==="
    stop_dpu
    ssh "$DPU_HOST" "cat > /tmp/start_dpu_bench.sh << 'LAUNCHER'
#!/bin/bash
screen -dmS dpumesh-bench bash -c \"cd /home/jukebox/$DPU_BUILD && DPUMESH_PROXY=$proxy ./dpumesh_dpu $DPU_PCI -l $log_level > $DPU_LOG 2>&1\"
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
        hw3)
            # 3 cores/side for dpumesh (cores 2,3 reserved for TCP pods).
            case "$app" in
                bench-dpumesh) echo "0,4,6" ;;
                echo-dpumesh)  echo "1,5,7" ;;
                bench-tcp)     echo "2" ;;
                echo-tcp)      echo "3" ;;
                *) echo "" ;;
            esac
            ;;
        hw6)
            # 6 cores/side — de-bottleneck the SERVICE (echo/client) so the
            # dpumesh TRANSPORT ceiling shows (cores 2,3 reserved for TCP).
            case "$app" in
                bench-dpumesh) echo "0,4,6,8,10,12" ;;
                echo-dpumesh)  echo "1,5,7,9,11,13" ;;
                bench-tcp)     echo "2" ;;
                echo-tcp)      echo "3" ;;
                *) echo "" ;;
            esac
            ;;
        fair|*)
            case "$app" in
                bench-dpumesh)    echo "0" ;;
                echo-dpumesh)     echo "1" ;;
                bench-tcp)        echo "2" ;;   # bench + sidecar1 share core 2
                echo-tcp)         echo "3" ;;   # echo  + sidecar2 share core 3
                loopback-dpumesh) echo "4,5" ;; # client+echo+PE threads in one pod
                preload-dpumesh)  echo "4,5" ;; # shares loopback's cores (both are
                                                # on-demand validators, never active
                                                # at the same time)
                stream-dpumesh)   echo "4,5" ;; # on-demand frame validator (shares
                                                # the loopback/preload cores)
                echo-dpumesh-13)  echo "6" ;;   # test-7 extra backend
                echo-dpumesh-14)  echo "7" ;;   # test-7 extra backend
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

    for app in bench-dpumesh echo-dpumesh echo-dpumesh-13 echo-dpumesh-14 loopback-dpumesh stream-dpumesh preload-dpumesh bench-tcp echo-tcp; do
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
        - { name: ASYNC_THREADS, value: "${ASYNC_THREADS:-4}" }
        # Pipeline depth (mode=2 / dpumesh-pipeline): outstanding msgs PER conn.
        # Default 8 (unchanged); set >16 to exercise deep-pipeline TX-slot custody.
        - { name: BENCH_PIPELINE, value: "${BENCH_PIPELINE:-8}" }
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
        - { name: ECHO_THREADS, value: "${ECHO_THREADS:-3}" }
        securityContext: { privileged: true }
        # CPU 1-core 제한은 pin_pods()의 taskset으로 처리.
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
# echo-dpumesh-13 / -14 (pod_id=13,14): extra backends for the test-7 weighted-LB
# demo (DPU LBs service 11 across pods 11,13,14). Same echo image, different id.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-dpumesh-13
spec:
  replicas: 0
  selector: { matchLabels: { app: echo-dpumesh-13 } }
  template:
    metadata: { labels: { app: echo-dpumesh-13 } }
    spec:
      hostname: echo-dpumesh-13
      containers:
      - name: echo-dpumesh-13
        image: docker.io/$IMG_ECHO_DPU
        imagePullPolicy: Never
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "13" }
        - { name: ECHO_THREADS, value: "${ECHO_THREADS:-3}" }
        securityContext: { privileged: true }
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
  name: echo-dpumesh-14
spec:
  replicas: 0
  selector: { matchLabels: { app: echo-dpumesh-14 } }
  template:
    metadata: { labels: { app: echo-dpumesh-14 } }
    spec:
      hostname: echo-dpumesh-14
      containers:
      - name: echo-dpumesh-14
        image: docker.io/$IMG_ECHO_DPU
        imagePullPolicy: Never
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "14" }
        - { name: ECHO_THREADS, value: "${ECHO_THREADS:-3}" }
        securityContext: { privileged: true }
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
# loopback-dpumesh (pod_id=12, service_id=12): ONE pod that is both client and
# server of its own service — proves self-routing (DPU resolves service 12 -> pod
# 12 = itself) + the oriented-tuple demux on a single host.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loopback-dpumesh
spec:
  replicas: 0
  selector: { matchLabels: { app: loopback-dpumesh } }
  template:
    metadata: { labels: { app: loopback-dpumesh } }
    spec:
      hostname: loopback-dpumesh
      containers:
      - name: loopback-dpumesh
        image: docker.io/$IMG_LOOPBACK_DPU
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "12" }
        - { name: DPUMESH_ARENA_SLOTS, value: "${DPUMESH_ARENA_SLOTS:-512}" }  # zero-copy arena (dmesh_alloc)
        securityContext: { privileged: true }
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
apiVersion: v1
kind: Service
metadata: { name: loopback-dpumesh }
spec:
  selector: { app: loopback-dpumesh }
  ports: [{ port: $CTRL_PORT, targetPort: $CTRL_PORT }]
---
# stream-dpumesh (pod_id=16, service_id=16): byte-stream / L7-proxy frame
# validator. Like loopback it is BOTH client and echo server of its OWN service,
# but it emits length-prefixed frames the DPU frame mock reframes + routes (needs
# the DPU launched with DPUMESH_PROXY=frame). replicas:0 + NOT in start_pods (it
# is a special-mode validator, and MAX_PODS=8 is tight) — the `stream` command
# scales it up on demand.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-dpumesh
spec:
  replicas: 0
  selector: { matchLabels: { app: stream-dpumesh } }
  template:
    metadata: { labels: { app: stream-dpumesh } }
    spec:
      hostname: stream-dpumesh
      containers:
      - name: stream-dpumesh
        image: docker.io/$IMG_STREAM_DPU
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "16" }
        securityContext: { privileged: true }
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
apiVersion: v1
kind: Service
metadata: { name: stream-dpumesh }
spec:
  selector: { app: stream-dpumesh }
  ports: [{ port: $CTRL_PORT, targetPort: $CTRL_PORT }]
---
# preload-dpumesh (service_id=15): LD_PRELOAD shim validator. The runner (NOT
# preloaded) spawns tcp_echo + tcp_client — both VANILLA POSIX TCP binaries —
# under LD_PRELOAD=libdmesh_preload.so. Each child registers ONE dmesh channel
# at boot (2 pod registrations total, within MAX_PODS); every RUN opens fresh
# connections, so conn churn (connect/FIN/close) is exercised per RUN.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: preload-dpumesh
spec:
  replicas: 0
  selector: { matchLabels: { app: preload-dpumesh } }
  template:
    metadata: { labels: { app: preload-dpumesh } }
    spec:
      hostname: preload-dpumesh
      containers:
      - name: preload-dpumesh
        image: docker.io/$IMG_PRELOAD_DPU
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: PRELOAD_SVC, value: "15" }
        - { name: ECHO_PORT, value: "9095" }
        - { name: DMESH_PRELOAD_DEBUG, value: "${DMESH_PRELOAD_DEBUG:-0}" }
        securityContext: { privileged: true }
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
---
apiVersion: v1
kind: Service
metadata: { name: preload-dpumesh }
spec:
  selector: { app: preload-dpumesh }
  ports: [{ port: $CTRL_PORT, targetPort: $CTRL_PORT }]
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
    # DPUmesh: echo first (so bench finds dst), then bench, then loopback (self-svc)
    scale_up_with_wait "echo-dpumesh"     "pods: 1"
    scale_up_with_wait "echo-dpumesh-13"  ""   # extra backends for test-7 weighted LB
    scale_up_with_wait "echo-dpumesh-14"  ""
    scale_up_with_wait "bench-dpumesh"    "pods: 2"
    scale_up_with_wait "loopback-dpumesh" "pods: 3"
    scale_up_with_wait "preload-dpumesh"  "pods: 4"   # LD_PRELOAD shim validator
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

    # mode: 0=rpc(reuse) 1=one-way 2=pipeline. RUN_ONEWAY=1 kept for back-compat.
    local rmode="${RUN_MODE:-0}"
    [ "${RUN_ONEWAY:-0}" = "1" ] && rmode=1
    local cmd="RUN $rps $dur $size"
    if [ "$rmode" != "0" ]; then
        cmd="RUN $rps $dur $size ${conns} $rmode"   # conns must precede mode
    elif [ "$conns" != "0" ]; then
        cmd="$cmd $conns"
    fi

    step "=== $mode bench: rps=$rps dur=${dur}s size=${size}B conns=${conns} oneway=${RUN_ONEWAY:-0} ==="
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

run_loopback() {
    # $1 = N round-trips, $2 = msg size. The loopback-dpumesh pod (pod_id=12) is
    # BOTH client and server of its OWN service (12): connect(svc 12) -> DPU
    # resolves svc 12 -> pod 12 (itself) -> own echo replies. Proves self-routing
    # + the oriented-tuple demux on a single host (request dst_port=0 -> accept,
    # reply dst_port=pc -> client, distinguished even though both legs are local).
    local N="${1:-50000}" size="${2:-8192}" zc="${3:-0}"
    local pod_ip
    pod_ip=$(kubectl get pod -n "$NS" -l app=loopback-dpumesh --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$pod_ip" ]; then
        err "loopback-dpumesh pod not found — run '$0 deploy' first"; return 1
    fi
    step "=== loopback (self-service): pod12 → service12 → pod12, N=$N size=${size}B zerocopy=$zc ==="
    local resp
    resp=$(printf 'RUN %s %s %s\n' "$N" "$size" "$zc" | timeout 120s nc "$pod_ip" "$CTRL_PORT" || true)
    if [ -z "$resp" ]; then err "no response (timeout or pod down)"; return 1; fi
    if [[ "$resp" == ERR* ]]; then err "loopback replied: $resp"; return 1; fi
    # Parse: OK <ok> <fail> <served> <p50us>
    read -r tag ok fail served p50 <<<"$resp"
    echo
    echo "============================================================"
    echo "  loopback (self-routing) result"
    echo "============================================================"
    printf "  OK / Fail:       %s / %s\n" "$ok" "$fail"
    printf "  Served (echo):   %s\n" "$served"
    printf "  p50 latency:     %s us\n" "$p50"
    echo "============================================================"
}

run_preload() {
    # LD_PRELOAD shim validation: the VANILLA tcp_client/tcp_echo binaries run
    # UNMODIFIED over DPUmesh via libdmesh_preload.so inside the preload-dpumesh
    # pod. $1 = N round-trips, $2 = msg size (may exceed 8KB — exercises
    # auto-chunk + the conn route pin), $3 = conns opened fresh per RUN.
    local N="${1:-5000}" size="${2:-1024}" conns="${3:-8}"
    local pod_ip
    pod_ip=$(kubectl get pod -n "$NS" -l app=preload-dpumesh --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$pod_ip" ]; then
        err "preload-dpumesh pod not found — run '$0 deploy' first"; return 1
    fi
    step "=== preload (LD_PRELOAD shim): N=$N size=${size}B conns=$conns ==="
    local resp
    resp=$(printf 'RUN %s %s %s\n' "$N" "$size" "$conns" | timeout 620s nc "$pod_ip" "$CTRL_PORT" || true)
    if [ -z "$resp" ]; then err "no response (timeout or pod down)"; return 1; fi
    if [[ "$resp" == ERR* ]]; then err "preload replied: $resp"; return 1; fi
    # Parse: OK <ok> <fail> <p50us> <p99us> <rps>
    read -r tag ok fail p50 p99 rps <<<"$resp"
    echo
    echo "============================================================"
    echo "  LD_PRELOAD shim (vanilla TCP apps over DPUmesh)"
    echo "============================================================"
    printf "  OK / Fail:      %s / %s\n" "$ok" "$fail"
    printf "  p50 latency:    %s us\n" "$p50"
    printf "  p99 latency:    %s us\n" "$p99"
    printf "  Achieved RPS:   %s\n" "${rps:-n/a}"
    echo "============================================================"
}

run_stream() {
    # Byte-stream / L7-proxy frame validator (needs the DPU launched with
    # DPUMESH_PROXY=frame). The stream-dpumesh pod (pod_id=16) is client+echo of
    # its own service 16; it emits length-prefixed frames the DPU frame mock
    # reframes + routes by svc byte. Scaled up on demand (not in start_pods).
    #   $1 = N round-trips, $2 = payload bytes/frame, $3 = svc list (default self,
    #        e.g. "11,13,14" fans out to the echo backends), $4 = frames/write.
    # svcs default "self" (NOT empty): an empty middle field collapses under the
    # daemon's positional sscanf and slides fpw into the svc slot. "self" = the
    # pod's own service (self-loopback). Skip the DPU-log wait ("" 2nd arg) — at
    # -l 40 the register line isn't emitted, so it only ever false-times-out.
    local N="${1:-20000}" size="${2:-1024}" svcs="${3:-self}" fpw="${4:-1}"
    # IDEMPOTENT: only scale up if no Running pod exists. scale_up_with_wait
    # restarts the pod (scale 0→1), and REPEATED restarts hit a DPU control-plane
    # setup fragility (DPA msgq AGAIN on ADD_RING → setup_pod_dma fails → the pod
    # never becomes dma_ready). So reuse a healthy pod across back-to-back runs;
    # a broken/absent pod needs a clean redeploy, not another restart here.
    local have
    have=$(kubectl get pod -n "$NS" -l app=stream-dpumesh --field-selector=status.phase=Running -o name 2>/dev/null | head -1)
    if [ -z "$have" ]; then
        scale_up_with_wait "stream-dpumesh" ""
    else
        info "reusing running stream-dpumesh pod (no restart)"
    fi
    local pod_ip
    pod_ip=$(kubectl get pod -n "$NS" -l app=stream-dpumesh --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$pod_ip" ]; then
        err "stream-dpumesh pod not found"; return 1
    fi
    step "=== stream (L7-proxy frame): N=$N size=${size}B svcs='${svcs:-self(16)}' frames/write=$fpw ==="
    local resp
    resp=$(printf 'RUN %s %s %s %s\n' "$N" "$size" "$svcs" "$fpw" | timeout 180s nc "$pod_ip" "$CTRL_PORT" || true)
    if [ -z "$resp" ]; then err "no response (timeout or pod down)"; return 1; fi
    if [[ "$resp" == ERR* ]]; then err "stream replied: $resp"; return 1; fi
    # Parse: OK <ok> <fail> <served_bytes> <p50us>
    read -r tag ok fail served p50 <<<"$resp"
    echo
    echo "============================================================"
    echo "  stream (L7-proxy frame byte-stream) result"
    echo "============================================================"
    printf "  OK / Fail:        %s / %s\n" "$ok" "$fail"
    printf "  Served (echo B):  %s\n" "$served"
    printf "  p50 latency:      %s us\n" "$p50"
    echo "  (byte-exact: OK==N & fail==0; the DPU frame mock reframed + routed"
    echo "   every frame; served bytes = echoed byte-stream volume.)"
    echo "============================================================"
}

### ---------------------------------------------------------- utility ###

show_logs() {
    # bench-tcp/echo-tcp pods now have 2 containers each (app + sidecar);
    # --all-containers prefixes each line with the container name.
    for app in bench-dpumesh echo-dpumesh echo-dpumesh-13 echo-dpumesh-14 loopback-dpumesh stream-dpumesh preload-dpumesh bench-tcp echo-tcp; do
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
    dpumesh-oneway)
        # One-way (fire-and-forget) load: client does write->close, no read.
        pin_pods fair >/dev/null
        RUN_ONEWAY=1 run_bench "dpumesh" "${@:2}"
        ;;
    dpumesh-pipeline)
        # Pipelined load: each conn carries BENCH_PIPELINE (default 8) outstanding
        # messages → one harvest drains a BATCH (multi-slot read). conn-reuse path.
        pin_pods fair >/dev/null
        RUN_MODE=2 run_bench "dpumesh" "${@:2}"
        ;;
    dpumesh-large)
        # Large-message (>slot_size) round-trip: plain dmesh_write AUTO-CHUNKS the
        # payload across slots + route-affinity pins EVERY chunk to ONE backend; the
        # client reassembles with a plain dmesh_read loop (byte stream, app frames) and
        # verifies content per-offset — arrival-order correctness IS the affinity proof
        # (scatter would arrive out of order). Async/windowed → also measures MB/s.
        # SIZE is the LOGICAL message size (e.g. 32768); pass CONNS ($4) high for
        # throughput.
        pin_pods fair >/dev/null
        RUN_MODE=3 run_bench "dpumesh" "${@:2}"
        ;;
    loopback)
        # Self-routing / loopback: pod 12 is client+server of its own service 12.
        pin_pods fair >/dev/null
        run_loopback "${2:-50000}" "${3:-8192}" "${4:-0}"   # $4=1 → zero-copy (dmesh_alloc)
        ;;
    stream)
        # Byte-stream / L7-proxy frame validator. REQUIRES the DPU launched with
        # DPUMESH_PROXY=frame (redeploy: DPUMESH_PROXY=frame $0 deploy). Scales the
        # stream-dpumesh pod up on demand, sends length-prefixed frames the DPU
        # reframes + routes, checks byte-exact echo. $4 = svc list (default self;
        # "11,13,14" fans out), $5 = frames/write (default 1; >1 packs a burst).
        pin_pods fair >/dev/null
        run_stream "${2:-20000}" "${3:-1024}" "${4:-}" "${5:-1}"
        ;;
    preload)
        # LD_PRELOAD shim: vanilla TCP apps (tcp_client/tcp_echo) over DPUmesh.
        pin_pods fair >/dev/null
        run_preload "${2:-5000}" "${3:-1024}" "${4:-8}"
        ;;
    dpumesh-hw)
        # HW limit chase: dpumesh 측만 multi-core. echo-dpumesh "1,5", bench
        # "0,4". 이 모드의 결과는 TCP와 직접 비교 불가 (자원 비대칭) — 오직
        # chain ceiling 까지 dpumesh 가 도달하는지 보는 용도.
        pin_pods hw >/dev/null
        run_bench "dpumesh" "${@:2}"
        ;;
    dpumesh-hw3)
        # 3 cores/side — host multi-core scalability test.
        pin_pods hw3 >/dev/null
        run_bench "dpumesh" "${@:2}"
        ;;
    dpumesh-hw6)
        # 6 cores/side — de-bottleneck the service to expose the transport ceiling.
        pin_pods hw6 >/dev/null
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
    dpulog)
        # Read-only tail of the dpumesh_dpu log on the DPU. $2 = lines (default 40).
        n="${2:-40}"
        ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -$n $DPU_LOG" 2>&1 | sed 's/^\[sudo\][^:]*: *//'
        ;;
    dpucpu)
        # Read-only per-thread CPU snapshot of dpumesh_dpu on the DPU ARM (2 top
        # samples 1s apart; the 2nd is the accurate one). Compares the busy-poll
        # driver (full-core spin) vs the event-loop driver (epoll sleep).
        dpu_sudo 'pid=$(pgrep -x dpumesh_dpu | head -1); [ -z "$pid" ] && { echo "dpumesh_dpu not running"; exit 0; }; echo "=== dpumesh_dpu pid=$pid per-thread %CPU (2nd of 2 samples) ==="; top -bH -d 1 -n 2 -p "$pid" | awk "/ PID +USER/{n++} n==2{print}"'
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
        echo "  preload     <N> <SIZE> [<CONNS>]          # LD_PRELOAD shim (vanilla TCP 앱)"
        echo "  stream      <N> <SIZE> [<SVCS>] [<FPW>]   # L7-proxy frame byte-stream (DPUMESH_PROXY=frame로 배포)"
        echo "  pin / pin-fair                            # fair 모드 재핀"
        echo "  pin-hw                                    # hw 모드 재핀 (수동 토글)"
        echo "  logs                                      # bench/echo pod 로그"
        echo "  status                                    # 상태"
        echo "  cleanup                                   # ns 삭제 + DPU 중지"
        echo
        echo "Note: pin profile은 dpumesh/dpumesh-hw/tcp 명령마다 자동으로 맞춰줌"
        ;;
esac
