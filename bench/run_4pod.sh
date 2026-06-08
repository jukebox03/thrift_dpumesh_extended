#!/usr/bin/env bash
#
# run_4pod.sh — 4-pod (2 echo-pair) harness for the DPUmesh chain.
#
# Adds a SECOND dpumesh echo pair (pod_id 12 -> 13) alongside the standard pair
# (10 -> 11) that `test-bench.sh deploy` already brings up. With the DPU started
# at DPUMESH_DPA_THREADS=4, the four pods hash to four DISTINCT EUs
#   10%4=2  11%4=3  12%4=0  13%4=1  -> 4 active EUs
# which (with DPUMESH_SPLIT_SEND=3) drives 4 shard workers. This is the decisive
# test of whether the sharded control plane scales chain throughput past the
# 2-EU ~104K ceiling.
#
# Prereq: run the standard deploy FIRST, e.g.
#   DPUMESH_DPA_THREADS=4 DPUMESH_SPLIT_SEND=3 ./test-bench.sh deploy
# then:
#   ./bench/run_4pod.sh up                       # add + pin pair2 (cores 4,5)
#   ./bench/run_4pod.sh run <RPS> <DUR> <SIZE>   # drive BOTH pairs, sum achieved
#   ./bench/run_4pod.sh down                     # remove pair2
#
# RPS is PER PAIR; aggregate offered load = 2*RPS. The script prints each pair's
# achieved/p99/fail and the summed achieved.

set -euo pipefail
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $*"; }
warn(){ echo -e "${YELLOW}[WARN]${NC} $*"; }
err(){ echo -e "${RED}[ERR]${NC} $*"; }
step(){ echo -e "${BLUE}[STEP]${NC} $*"; }

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ_ROOT"
if [ -f ".env" ]; then set -a; source .env; set +a; else err ".env not found"; exit 1; fi

NS="test-bench"
BUILD_DOCA="$PROJ_ROOT/build-doca"
DPU_LOG="/tmp/dpumesh_dpu_bench.log"
IMG_BENCH_DPU="bench/bench-dpumesh:latest"
IMG_ECHO_DPU="bench/echo-dpumesh:latest"
CTRL_PORT=9092
NUM_SLOTS="${DPUMESH_NUM_SLOTS:-2048}"
# Host cores for pair2 (fair profile uses 0-3 for pair1 + tcp; 4-7 free).
BENCH2_CORE="${BENCH2_CORE:-4}"
ECHO2_CORE="${ECHO2_CORE:-5}"

apply_pair2() {
    step "=== Applying pair2 (bench-dpumesh2 pod 12 -> echo-dpumesh2 pod 13) ==="
    cat <<EOF | kubectl apply -n "$NS" -f -
apiVersion: apps/v1
kind: Deployment
metadata: { name: bench-dpumesh2 }
spec:
  replicas: 0
  selector: { matchLabels: { app: bench-dpumesh2 } }
  template:
    metadata: { labels: { app: bench-dpumesh2 } }
    spec:
      hostname: bench-dpumesh2
      containers:
      - name: bench-dpumesh2
        image: docker.io/$IMG_BENCH_DPU
        imagePullPolicy: Never
        ports: [{ containerPort: $CTRL_PORT }]
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "12" }
        - { name: BENCH_DST_POD_ID, value: "13" }
        - { name: DPUMESH_NUM_SLOTS, value: "$NUM_SLOTS" }
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
metadata: { name: echo-dpumesh2 }
spec:
  replicas: 0
  selector: { matchLabels: { app: echo-dpumesh2 } }
  template:
    metadata: { labels: { app: echo-dpumesh2 } }
    spec:
      hostname: echo-dpumesh2
      containers:
      - name: echo-dpumesh2
        image: docker.io/$IMG_ECHO_DPU
        imagePullPolicy: Never
        env:
        - { name: DPUMESH_PCI_ADDR, value: "$HOST_PCI" }
        - { name: BENCH_WORKER_ID, value: "13" }
        - { name: ECHO_THREADS, value: "64" }
        - { name: DPUMESH_NUM_SLOTS, value: "$NUM_SLOTS" }
        securityContext: { privileged: true }
        volumeMounts:
        - { mountPath: /dev/infiniband, name: infiniband }
        - { mountPath: /usr/local/lib/libthrift.so.0.12.0, name: libthrift-so, subPath: libthrift.so.0.12.0 }
      volumes:
      - { name: infiniband, hostPath: { path: /dev/infiniband } }
      - { name: libthrift-so, hostPath: { path: $BUILD_DOCA/lib, type: Directory } }
EOF
}

scale_wait() {
    local app="$1"
    kubectl scale deployment "$app" --replicas=0 -n "$NS" 2>/dev/null || true
    sleep 1
    kubectl scale deployment "$app" --replicas=1 -n "$NS"
    if ! kubectl wait --for=condition=Ready pod -l "app=$app" -n "$NS" --timeout=120s; then
        err "$app failed to start"; kubectl describe pod -l "app=$app" -n "$NS" | tail -15; exit 1
    fi
    info "$app Ready"
}

pin_app() {
    local app="$1" core="$2"
    local pod_id
    pod_id=$(echo "$HOST_PASS" | sudo -S crictl pods --label "app=$app" -q 2>/dev/null | head -n1)
    if [ -z "$pod_id" ]; then warn "$app: pod not found, skip pin"; return; fi
    for cid in $(echo "$HOST_PASS" | sudo -S crictl ps --pod "$pod_id" -q 2>/dev/null); do
        local pid
        pid=$(echo "$HOST_PASS" | sudo -S crictl inspect "$cid" 2>/dev/null | jq -r '.info.pid' 2>/dev/null)
        if [ -z "$pid" ] || [ "$pid" = "null" ]; then continue; fi
        echo "$HOST_PASS" | sudo -S taskset -apc "$core" "$pid" >/dev/null
        for ch in $(pgrep -P "$pid" 2>/dev/null); do
            echo "$HOST_PASS" | sudo -S taskset -apc "$core" "$ch" >/dev/null 2>&1 || true
        done
    done
    info "$app -> core $core (pod=$pod_id)"
}

cmd_up() {
    apply_pair2
    scale_wait echo-dpumesh2     # echo first so bench finds dst
    scale_wait bench-dpumesh2
    pin_app echo-dpumesh2 "$ECHO2_CORE"
    pin_app bench-dpumesh2 "$BENCH2_CORE"
    info "pair2 up. Re-pin pair1 if needed via: ./test-bench.sh pin"
}

cmd_down() {
    kubectl scale deployment bench-dpumesh2 --replicas=0 -n "$NS" 2>/dev/null || true
    kubectl scale deployment echo-dpumesh2  --replicas=0 -n "$NS" 2>/dev/null || true
    info "pair2 scaled to 0"
}

# Send "RUN ..." to one bench app's pod IP, write the raw OK line to $2.
run_one() {
    local app="$1" outfile="$2" rps="$3" dur="$4" size="$5"
    local ip
    ip=$(kubectl get pod -n "$NS" -l "app=$app" --field-selector=status.phase=Running -o jsonpath='{.items[0].status.podIP}')
    if [ -z "$ip" ]; then echo "ERR no-pod" >"$outfile"; return; fi
    printf 'RUN %s %s %s\n' "$rps" "$dur" "$size" \
        | timeout "$((dur + 30))s" nc "$ip" "$CTRL_PORT" >"$outfile" 2>&1 || echo "ERR timeout" >>"$outfile"
}

parse_field() { # $1=line, $2=index (1-based after OK)
    echo "$1" | awk -v i="$2" '{print $(i+1)}'
}

cmd_run() {
    local rps="${1:-50000}" dur="${2:-10}" size="${3:-8192}"
    step "=== 4-pod run: ${rps} RPS/pair (offered 2x=${rps}+${rps}), dur=${dur}s size=${size}B ==="
    local o1 o2
    o1=$(mktemp); o2=$(mktemp)
    run_one bench-dpumesh  "$o1" "$rps" "$dur" "$size" &
    local p1=$!
    run_one bench-dpumesh2 "$o2" "$rps" "$dur" "$size" &
    local p2=$!
    wait "$p1"; wait "$p2"
    local l1 l2; l1=$(cat "$o1"); l2=$(cat "$o2"); rm -f "$o1" "$o2"
    # OK <rps> <p50> <p99> <p999> <ok> <fail> <mb_s>
    echo "  pair1 (10->11): $l1"
    echo "  pair2 (12->13): $l2"
    if [[ "$l1" == OK* && "$l2" == OK* ]]; then
        local a1 a2 f1 f2 p99a p99b
        a1=$(parse_field "$l1" 1); a2=$(parse_field "$l2" 1)
        p99a=$(parse_field "$l1" 3); p99b=$(parse_field "$l2" 3)
        f1=$(parse_field "$l1" 6); f2=$(parse_field "$l2" 6)
        echo "------------------------------------------------------------"
        printf "  pair1 achieved=%s p99=%sus fail=%s\n" "$a1" "$p99a" "$f1"
        printf "  pair2 achieved=%s p99=%sus fail=%s\n" "$a2" "$p99b" "$f2"
        awk -v a="$a1" -v b="$a2" 'BEGIN{printf "  AGGREGATE achieved = %.1f RPS\n", a+b}'
        echo "------------------------------------------------------------"
    else
        err "one or both pairs did not return OK (see lines above)"
    fi
}

# Tail the DPU log and show the SHARD bottleneck diagnostic lines (1 Hz).
cmd_dpulog() {
    local n="${1:-40}"
    ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S tail -n $n $DPU_LOG" 2>&1 \
        | sed 's/^\[sudo\][^:]*: *//' | grep -E "SHARD-DIAG|elapsed:" || echo "(no diag lines yet)"
}

case "${1:-}" in
    up)     cmd_up ;;
    down)   cmd_down ;;
    run)    shift; cmd_run "$@" ;;
    dpulog) shift; cmd_dpulog "$@" ;;
    *) echo "Usage: $0 {up|down|run <RPS/pair> <DUR> <SIZE>|dpulog [N]}"; exit 1 ;;
esac
