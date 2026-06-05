"""
Ceiling layering at matched EU count: pure_dma / M0 / M2 / chain.

Data: bench.md S5.5 "pure/M0/M2/chain same-EU comparison" [measured + arithmetic].
Grouped bars (dma_copy/s) at single-EU / 2-EU buckets, four configs. Each added
layer of work (host gate -> per-op forward -> full echo RTT) lowers the ceiling.
Bar labels use the exact strings from the S5.5 table.
"""
import os
import numpy as np
import matplotlib.pyplot as plt

buckets = ["single-EU", "2-EU"]
x = np.arange(len(buckets))
w = 0.20

# heights in millions of dma_copy/s; labels = exact S5.5 strings
pure_h, pure_l   = [0.556, 1.07], ["556K", "1.07M"]
m0_h,   m0_l     = [0.325, 0.629], ["325K", "629K"]
m2_h,   m2_l     = [0.322, 0.626], ["322K", "626K"]
chain_h, chain_l = [0.309, 0.416], ["309K", "416K"]

fig, ax = plt.subplots(figsize=(11, 7))

ax.bar(x - 1.5 * w, pure_h,  w, color="#43a047", edgecolor="#1b5e20", zorder=3, label="pure_dma (engine)")
ax.bar(x - 0.5 * w, m0_h,    w, color="#4c72b0", edgecolor="#2a4d75", zorder=3, label="M0 (drain only)")
ax.bar(x + 0.5 * w, m2_h,    w, color="#90a4ae", edgecolor="#455a64", zorder=3, label="M2 (+ host forward)")
ax.bar(x + 1.5 * w, chain_h, w, color="#d98c3f", edgecolor="#9c5e1c", zorder=3, label="chain (full echo RTT)")

def label(xs, heights, texts, color):
    for xi, h, t in zip(xs, heights, texts):
        ax.text(xi, h + 0.015, t, ha="center", va="bottom",
                fontsize=9.5, fontweight="bold", color=color)
label(x - 1.5 * w, pure_h, pure_l, "#1b5e20")
label(x - 0.5 * w, m0_h,   m0_l,   "#2a4d75")
label(x + 0.5 * w, m2_h,   m2_l,   "#37474f")
label(x + 1.5 * w, chain_h, chain_l, "#9c5e1c")

# chain as % of M2 — the gap widens with EU count (96% -> 67%)
for i, pct in zip([0, 1], ["96% of M2", "67% of M2"]):
    ax.text(x[i] + 1.5 * w, chain_h[i] + 0.09, pct, ha="center", va="bottom",
            fontsize=8.5, color="#9c5e1c", style="italic")

ax.set_xticks(x)
ax.set_xticklabels(buckets, fontsize=11.5)
ax.set_xlabel("active EU count", fontsize=11)
ax.set_ylabel("dma_copy/s   (millions,  8 KB payload)", fontsize=12)
ax.set_ylim(0, 1.25)
ax.set_title("Ceiling layering at matched EU count: pure / M0 / M2 / chain",
             fontsize=14, fontweight="bold", pad=12)
ax.legend(loc="upper left", fontsize=9.5, framealpha=0.95, ncol=2)
ax.grid(axis="y", ls=":", alpha=0.4, zorder=0)
ax.set_axisbelow(True)

fig.text(0.012, 0.012,
         "Each added layer lowers the ceiling: host descriptor gate (M0),\n"
         "per-op host forward (M2), full echo RTT = 4 dma_copy (chain).\n"
         "chain/M2 gap widens 96% (single-EU) -> 67% (2-EU)\n"
         "  = cost of reverse + admission + lossless send.",
         fontsize=8.0, va="bottom", ha="left", family="monospace", color="#333")

fig.text(0.988, 0.012,
         "chain = RPS x 4 [arith]; pure/M0/M2 measured directly.\n"
         "Cause attributions are elimination-only (bench.md S5.5 / S7.2).",
         fontsize=8.0, va="bottom", ha="right", style="italic", color="#777")

fig.subplots_adjust(bottom=0.24, top=0.90)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ceiling_layering.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print("wrote", out)
