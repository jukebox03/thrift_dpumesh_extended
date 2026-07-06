#!/usr/bin/env bash
# Sync + build + run the DMA MR-working-set microbench ON THE BF3 ARM, using the
# same ssh/rsync path test-bench.sh uses. Run this FROM THE HOST (repo root env).
#
#   ./bench/mrbench/run.sh                       # default sweep
#   ./bench/mrbench/run.sh -N 1,16,256,4096 -o 3000000
#   ./bench/mrbench/run.sh --stop-dpu            # also stop the running transport
#
# Any args after the flags below are forwarded verbatim to ./dma_mrbench.
#
# NOTE (shared state): a running `dpumesh_dpu` shares the DOCA device. For a
# clean op-rate number you want the DMA engine to yourself. This script does NOT
# kill the transport unless you pass --stop-dpu; otherwise it only WARNS.
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)

# shellcheck disable=SC1091
source "$ROOT/.env"                       # DPU_HOST, DPU_PASS, DPU_PCI
PCI=$(printf '%s' "${DPU_PCI:-}" | sed -n 's/.*-p \([0-9a-fA-F:.]*\).*/\1/p')
PCI=${PCI:-03:00.0}
REMOTE=mrbench                            # relative to the DPU login home
SSH_OPTS="-o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o BatchMode=yes"
SSH="ssh $SSH_OPTS"

STOP_DPU=0
if [[ "${1:-}" == "--stop-dpu" ]]; then STOP_DPU=1; shift; fi
BENCH_ARGS=("$@")

echo "== sync -> $DPU_HOST:~/$REMOTE =="
$SSH "$DPU_HOST" "mkdir -p $REMOTE"
rsync -avz -e "$SSH" dma_mrbench.c build.sh "$DPU_HOST:$REMOTE/"

echo "== build on DPU =="
$SSH "$DPU_HOST" "cd $REMOTE && bash build.sh"

if $SSH "$DPU_HOST" "pgrep -x dpumesh_dpu >/dev/null 2>&1"; then
    if [[ $STOP_DPU == 1 ]]; then
        echo "== stopping dpumesh_dpu (--stop-dpu) =="
        $SSH "$DPU_HOST" "echo '$DPU_PASS' | sudo -S killall -9 dpumesh_dpu 2>/dev/null; true" \
            | sed 's/^\[sudo\][^:]*: *//'
    else
        echo "!! WARNING: dpumesh_dpu is RUNNING — it shares the DOCA device."
        echo "!! Results may be contended. Re-run with --stop-dpu for a clean number,"
        echo "!! or stop the transport yourself first. Continuing anyway..."
    fi
fi

echo "== run: ./dma_mrbench -p $PCI ${BENCH_ARGS[*]} =="
$SSH "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash -c 'cd $REMOTE && ./dma_mrbench -p $PCI -c mrbench.csv ${BENCH_ARGS[*]}'" \
    | sed 's/^\[sudo\][^:]*: *//' | tee mrbench.out

echo "== fetch CSV =="
scp $SSH_OPTS "$DPU_HOST:$REMOTE/mrbench.csv" ./mrbench.csv 2>/dev/null || true
echo "wrote ./bench/mrbench/mrbench.out  and  ./bench/mrbench/mrbench.csv"
