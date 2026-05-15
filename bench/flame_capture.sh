#!/bin/bash
# flame_capture.sh — Capture flame graphs of bench-dpumesh + echo-dpumesh (CPU)
# and dpumesh_dpu (DPU) during a sustained load run.
#
# Usage:
#   ./bench/flame_capture.sh <rps_hint> <dur> <size> <label> [<K>]
#
# rps_hint is ignored by the open-loop bench (kept for arg position compat).
# K is the in-flight ring depth (default 256). Pick K at the throughput
# plateau so the flame reflects the actual cap, not RTT-bound idle time.
#
# Output: bench/${label}_{bench,echo,dpu}_flame.svg
set -euo pipefail

RPS="${1:-130000}"
DUR="${2:-10}"
SIZE="${3:-256}"
LABEL="${4:-fg}"
K="${5:-256}"

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/home/jukebox/FlameGraph}"

if [ -f "${PROJ_ROOT}/.env" ]; then
    set -a; source "${PROJ_ROOT}/.env"; set +a
fi

BENCH_POD_IP=$(kubectl get pod -n test-bench -l app=bench-dpumesh -o jsonpath='{.items[0].status.podIP}')
[ -z "$BENCH_POD_IP" ] && { echo "bench-dpumesh pod not found"; exit 1; }

BENCH_PID=$(pgrep -f '^.*bench_dpumesh($| )' | head -1)
ECHO_PID=$(pgrep -f '^.*echo_dpumesh($| )' | head -1)
[ -z "$BENCH_PID" ] || [ -z "$ECHO_PID" ] && {
    echo "ERR: bench_dpumesh PID=$BENCH_PID echo_dpumesh PID=$ECHO_PID"; exit 1;
}
echo "Host PIDs: bench=$BENCH_PID echo=$ECHO_PID"

# Find DPU dpumesh_dpu PID via SSH
DPU_PIDS=$(ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S pgrep dpumesh_dpu 2>/dev/null" 2>/dev/null | head -3)
DPU_PID=$(echo "$DPU_PIDS" | head -1)
[ -z "$DPU_PID" ] && { echo "ERR: dpumesh_dpu PID not found on DPU"; exit 1; }
echo "DPU PID: $DPU_PID"

CTRL_PORT=9092
PERF_DUR=$DUR

echo "=== Step 1: kick off load (rps_hint=$RPS dur=$DUR size=$SIZE K=$K) ==="
RUN_CMD="RUN $RPS $DUR $SIZE $K"
( printf '%s\n' "$RUN_CMD" | timeout $((DUR + 10))s nc "$BENCH_POD_IP" "$CTRL_PORT" > /tmp/flame_bench_resp.txt 2>&1 ) &
BENCH_NC_PID=$!
# Brief delay to let bench enter the steady state
sleep 1

echo "=== Step 2: perf record host (bench+echo) for ${PERF_DUR}s ==="
# --call-graph=dwarf,16384: DWARF unwinding via .eh_frame, works through
# libc/libdoca even though they're built without -fno-omit-frame-pointer
# (Ubuntu 22.04). Default -g (frame-pointer mode) collapses to [unknown]
# the moment the stack enters an FP-less library, which is most of the
# CPU time for this workload. 16K stack capture size is enough for our
# deepest call chains (worker → libthrift → libdoca → kernel).
echo "$HOST_PASS" | sudo -S perf record -F 199 --call-graph=dwarf,16384 -o /tmp/perf_bench.data -p "$BENCH_PID" -- sleep "$PERF_DUR" &
PERF_BENCH=$!
echo "$HOST_PASS" | sudo -S perf record -F 199 --call-graph=dwarf,16384 -o /tmp/perf_echo.data -p "$ECHO_PID" -- sleep "$PERF_DUR" &
PERF_ECHO=$!

echo "=== Step 3: perf record DPU dpumesh_dpu for ${PERF_DUR}s ==="
ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S perf record -F 199 -g -o /tmp/perf_dpu.data -p $DPU_PID -- sleep $PERF_DUR" 2>/tmp/perf_dpu_err.log &
PERF_DPU=$!

wait $PERF_BENCH $PERF_ECHO $PERF_DPU || true
wait $BENCH_NC_PID || true

echo "Bench result:"
cat /tmp/flame_bench_resp.txt | tr '\r' '\n'

echo "=== Step 4: render flame graphs ==="
cd "$FLAMEGRAPH_DIR"

echo "$HOST_PASS" | sudo -S chmod +r /tmp/perf_bench.data /tmp/perf_echo.data
echo "$HOST_PASS" | sudo -S perf script -i /tmp/perf_bench.data 2>/dev/null | ./stackcollapse-perf.pl > /tmp/perf_bench.folded
./flamegraph.pl --title "bench-dpumesh @ ${SIZE}B K=${K} (sat)" /tmp/perf_bench.folded > "${PROJ_ROOT}/bench/${LABEL}_bench_flame.svg"

echo "$HOST_PASS" | sudo -S perf script -i /tmp/perf_echo.data 2>/dev/null | ./stackcollapse-perf.pl > /tmp/perf_echo.folded
./flamegraph.pl --title "echo-dpumesh @ ${SIZE}B K=${K} (sat)" /tmp/perf_echo.folded > "${PROJ_ROOT}/bench/${LABEL}_echo_flame.svg"

# DPU side: render perf.script on DPU (perf data format is ARM), pipe back.
ssh "$DPU_HOST" "echo '$DPU_PASS' | sudo -S perf script -i /tmp/perf_dpu.data" 2>/dev/null > /tmp/perf_dpu.script
./stackcollapse-perf.pl /tmp/perf_dpu.script > /tmp/perf_dpu.folded
./flamegraph.pl --title "dpumesh_dpu @ ${SIZE}B K=${K} (sat)" /tmp/perf_dpu.folded > "${PROJ_ROOT}/bench/${LABEL}_dpu_flame.svg"

echo "=== Output ==="
ls -la "${PROJ_ROOT}/bench/${LABEL}_"*"_flame.svg"
