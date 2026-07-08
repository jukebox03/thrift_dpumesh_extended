#!/usr/bin/env bash
# run_countbw.sh — DMA COUNT-bottleneck vs DATA-VOLUME(bandwidth)-bottleneck diagnostic.
#
# Sweeps the TRANSFER SIZE per DMA op at max in-flight depth (mode C = "flat control":
# 1 MR, 1 slice reused, so NO MR/MTT working-set effect — pure engine op at size S) and
# measures ops/s + GB/s at each size. Then fits
#       time_per_op(S) = 1/ops_s(S) = c + b*S
# to split the cost into:
#   c = FIXED per-op cost  (ns/op)   -> the "DMA COUNT" component (dominates small S)
#   b = per-BYTE cost      (ns/byte) -> the "DATA VOLUME" component (dominates large S)
# Crossover S* = c/b: below it you are COUNT-bound, above it BANDWIDTH-bound.
# For 1 KB and 8 KB (our message / slot sizes) we print the count-vs-bytes split + verdict.
#
# Runs the SAME doca_dma engine as dma_mrbench (ARM-issued, DPU-local). ABSOLUTE numbers
# are the DMA engine's, not the DPA-over-PCIe transport path — the SHAPE (crossover, split)
# is what transfers. Complement with the transport-level size sweep (real path).
#
#   ./bench/mrbench/run_countbw.sh                       # default size sweep
#   ./bench/mrbench/run_countbw.sh "64 256 1024 8192"    # custom sizes (bytes)
#   OPS=2000000 DEPTH=32 ./bench/mrbench/run_countbw.sh
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
# shellcheck disable=SC1091
source "$ROOT/.env"
PCI=$(printf '%s' "${DPU_PCI:-}" | sed -n 's/.*-p \([0-9a-fA-F:.]*\).*/\1/p'); PCI=${PCI:-03:00.0}
SSH_OPTS="-o ConnectTimeout=10 -o BatchMode=yes"
SSH="ssh $SSH_OPTS"
SIZES="${1:-64 128 256 512 1024 2048 4096 8192 16384 32768 65536}"
OPS="${OPS:-1000000}"
DEPTH="${DEPTH:-32}"

echo "== sync + build mrbench on $DPU_HOST =="
$SSH "$DPU_HOST" "mkdir -p mrbench"
rsync -az -e "$SSH" dma_mrbench.c build.sh "$DPU_HOST:mrbench/"
$SSH "$DPU_HOST" "cd mrbench && bash build.sh" >/dev/null

if $SSH "$DPU_HOST" "pgrep -x dpumesh_dpu >/dev/null 2>&1"; then
    echo "!! NOTE: dpumesh_dpu is running (shares the DMA engine). It is idle with no bench"
    echo "!! traffic, so contention is small and the count-vs-bandwidth SHAPE still holds."
fi

echo "size_B,ops_s,gbs" > countbw.csv
printf "%8s  %12s  %10s\n" "size_B" "Mops/s" "GB/s"
for S in $SIZES; do
    OUT=$($SSH "$DPU_HOST" "echo '$DPU_PASS' | sudo -S bash -c 'cd mrbench && ./dma_mrbench -p $PCI -m C -N 1 -s $S -d $DEPTH -o $OPS -w 100000'" 2>/dev/null | sed 's/^\[sudo\][^:]*: *//')
    # mode-C data row: "  C:reuse      1     <Mops>   <GB/s>  ..."   (awk fields: $1 name, $2 N, $3 Mops, $4 GB/s)
    read -r mops gbs <<<"$(echo "$OUT" | awk '/C:reuse/{print $3, $4}')"
    [ -z "${mops:-}" ] && { echo "  size $S: no result (mrbench output below)"; echo "$OUT" | tail -3; continue; }
    ops=$(awk -v m="$mops" 'BEGIN{printf "%.0f", m*1e6}')
    printf "%8d  %12s  %10s\n" "$S" "$mops" "$gbs"
    echo "$S,$ops,$gbs" >> countbw.csv
done

echo
echo "== fit + verdict =="
python3 - <<'PY'
import csv
rows=[]
with open('countbw.csv') as f:
    for r in csv.DictReader(f):
        rows.append((int(r['size_B']), float(r['ops_s']), float(r['gbs'])))
if len(rows) < 2:
    print("not enough points for a fit"); raise SystemExit
xs=[S for S,_,_ in rows]; ys=[1.0/ops for _,ops,_ in rows]   # sec/op
n=len(xs); sx=sum(xs); sy=sum(ys); sxx=sum(x*x for x in xs); sxy=sum(x*y for x,y in zip(xs,ys))
b=(n*sxy-sx*sy)/(n*sxx-sx*sx); c=(sy-b*sx)/n
print(f"per-op FIXED cost  c = {c*1e9:8.2f} ns/op     (the DMA-COUNT component)")
print(f"per-BYTE cost      b = {b*1e9:8.4f} ns/byte   (the DATA-VOLUME component; {1e0/b/1e9:.1f} GB/s asymptote)")
if b>0:
    Sx=c/b
    print(f"crossover  S* = c/b = {Sx:8.0f} B         (< S*: COUNT-bound ; > S*: BANDWIDTH-bound)")
for S in (1024, 8192):
    tot=c+b*S; frac=c/tot if tot>0 else 0
    print(f"  at {S:5d} B:  count={frac*100:5.1f}%  volume={(1-frac)*100:5.1f}%  ->  "
          f"{'COUNT-bound' if frac>0.5 else 'BANDWIDTH-bound'}")
print("\ntable (raw):")
print(f"  {'size_B':>8}  {'Mops/s':>9}  {'GB/s':>8}")
for S,ops,gbs in rows:
    print(f"  {S:8d}  {ops/1e6:9.3f}  {gbs:8.2f}")
PY
