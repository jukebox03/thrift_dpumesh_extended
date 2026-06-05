"""
DMA-engine ceiling vs mesh baselines (M0 / M2), scaled over EU count (N=1/2/4/8).

Data: bench.md S5.2 (pure_dma multi-EU) + S5.4 (M0 / M2). dma_copy/s.
Grouped bars:
  green = pure_dma  (1 EU fires dma_copy back-to-back, no host descriptor gate)
  blue  = M0        (+ host descriptor ring gate; DPU only counts -> drain only)
  grey  = M2        (+ DPU ARM forwards every completion to host)
pure scales near-linear to N=4 then drops at N=8; M0 drain keeps scaling to
1.53M; M2 forward plateaus ~1.0M from N>=4.
"""
import os
import numpy as np
import matplotlib.pyplot as plt

# --- Data from bench.md (all [measured], dma_copy/s) ------------------------
eu = [1, 2, 4, 8]
pure = [554789, 1070930, 1804363, 1466216]   # S5.2 pure_dma multi-EU
pure_scale = ["1.00x", "1.93x", "3.25x", "2.64x"]  # vs N=1 (N=8 = absolute drop)
m0 = [324630, 628622, 1187147, 1534325]      # S5.4 M0 (drain only)
m2 = [321754, 625708, 1006224, 1024805]      # S5.4 M2 (+ host forward)

n = len(eu)
x = np.arange(n)
w = 0.27

fig, ax = plt.subplots(figsize=(12.5, 7.5))

ax.bar(x - w, [v / 1e6 for v in pure], w, color="#43a047", edgecolor="#1b5e20",
       zorder=3, label="pure_dma (DMA-engine ceiling)")
ax.bar(x,     [v / 1e6 for v in m0],   w, color="#4c72b0", edgecolor="#2a4d75",
       zorder=3, label="M0 (drain only)")
ax.bar(x + w, [v / 1e6 for v in m2],   w, color="#90a4ae", edgecolor="#455a64",
       zorder=3, label="M2 (+ host forward)")

def label(xs, vals, color):
    for xi, v in zip(xs, vals):
        ax.text(xi, v / 1e6 + 0.02, f"{v/1e6:.2f}M", ha="center", va="bottom",
                fontsize=9, fontweight="bold", color=color)
label(x - w, pure, "#1b5e20")
label(x,     m0,   "#2a4d75")
label(x + w, m2,   "#37474f")

# pure scaling factor (vs N=1) above the pure bars
for xi, v, s in zip(x, pure, pure_scale):
    ax.text(xi - w, v / 1e6 + 0.10, s, ha="center", va="bottom",
            fontsize=8.5, color="#2e7d32")

ax.set_xticks(x)
ax.set_xticklabels([f"N = {e}\nEU" for e in eu], fontsize=10.5)
ax.set_ylabel("dma_copy/s   (millions,  8 KB payload)", fontsize=12)
ax.set_ylim(0, 2.05)
ax.set_title("DMA-engine ceiling vs mesh baselines (M0 / M2) per EU count",
             fontsize=14, fontweight="bold", pad=12)
ax.legend(loc="upper left", fontsize=9.5, framealpha=0.95)
ax.grid(axis="y", ls=":", alpha=0.4, zorder=0)
ax.set_axisbelow(True)

# concise definitions + takeaway
fig.text(0.012, 0.012,
         "pure_dma: 1 DPA EU fires dma_copy back-to-back, no host descriptor gate (engine ceiling).\n"
         "M0:       + host descriptor ring gate; DPU only counts completions (drain only).\n"
         "M2:       + DPU ARM forwards every completion to host (single-ARM control plane).\n"
         "Takeaway: M0 drain scales to 1.53M at N=8; the per-op host forward (M2) plateaus ~1.0M from N>=4.",
         fontsize=8.0, va="bottom", ha="left", family="monospace", color="#333")

# honesty note (bench.md S5.2/S5.4/S7.2)
fig.text(0.988, 0.012,
         "pure 1.8M is a measured lower bound (per-op DPU completion included).\n"
         "N=8 pure regression cause is elimination-only - no positive evidence.\n"
         "(bench.md S5.2 / S5.4 / S7.2)",
         fontsize=8.0, va="bottom", ha="right", style="italic", color="#777")

fig.subplots_adjust(bottom=0.24, top=0.90)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pure_dma_scaling.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print("wrote", out)
