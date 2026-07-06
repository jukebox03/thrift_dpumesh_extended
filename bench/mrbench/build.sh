#!/usr/bin/env bash
# Build the DMA MR-working-set microbench. Run this ON THE BF3 ARM (where DOCA +
# the DMA engine live). Needs the DOCA SDK (same one the DPU build uses).
set -euo pipefail
cd "$(dirname "$0")"

DOCA=${DOCA_ROOT:-/opt/mellanox/doca}

# Prefer pkg-config; fall back to explicit -l flags if the .pc files are absent.
if pkg-config --exists doca-common doca-dma 2>/dev/null; then
    CFLAGS=$(pkg-config --cflags doca-common doca-dma)
    LIBS=$(pkg-config --libs doca-common doca-dma)
else
    CFLAGS="-I${DOCA}/include"
    LIBS="-ldoca_common -ldoca_dma"
fi

set -x
gcc -O2 -g -Wall -Wextra -DDOCA_ALLOW_EXPERIMENTAL_API \
    ${CFLAGS} dma_mrbench.c -o dma_mrbench ${LIBS}
set +x
echo "built ./dma_mrbench"
