"""
Single-EU chain cumulative optimization (8 KB, 0 fail).

Data: bench.md S3.2 "cumulative optimization" table. Each step is a measured
endpoint (cumulative, applied on top of the previous one). One figure:
  - bars  = sustainable elbow RPS (p99 <= ~14 ms, 0 fail)
  - line  = overload ceiling RPS
The baseline bar (52K) is greyed as the un-optimized reference; the green
gradient marks the optimization steps.
"""
import os
import numpy as np
import matplotlib.pyplot as plt

# --- Data from bench.md S3.2 (all [measured], 0 fail across the board) -------
# (short x-label, full description of the code change)
phases = [
    ("0\nBaseline",            "baseline (S3.1), un-optimized reference"),
    ("1\nDead-code\ncleanup",  "Delete dead DMA_REQ chain (~150 lines) + chunking fallback; lazy drain; poll throttle"),
    ("2\nCompletion\nmsg 28->16B","Pack wire completion msg 28B->16B (fits 1 HW WQE BB) + lock-free comch_server"),
    ("3\nSDK batch\ncompletion","Delegate producer slot counter to SDK + FLUSH|OPTIMIZE_REPORTS completion batching"),
    ("4\nFence/batch\ndrain",  "Hoist window read_inv to once/iter + RING_BATCH_CAP=32 batch drain + once/iter writeback"),
]
sustain_k = [52, 60, 65, 68, 74]                       # sustainable elbow RPS (K)
overload  = [53841, 62068, 65859, 68127, 77434]        # overload ceiling RPS
dma_ops   = [215364, 248272, 263436, 272508, 309736]   # dma_copy/s = RPS*4
vs_engine = [67, 78, 82, 85, 96.8]                     # % of M2-N=1 engine (320K dma_copy/s)

n = len(phases)
x = np.arange(n)
overload_k = [v / 1000.0 for v in overload]

fig, ax = plt.subplots(figsize=(11.5, 7.5))

# baseline grey, optimization steps (1..n-1) green gradient
cmap = plt.cm.YlGn
colors = ["#9e9e9e"] + [cmap(0.32 + 0.55 * (i - 1) / (n - 2)) for i in range(1, n)]
ax.bar(x, sustain_k, width=0.62, color=colors, edgecolor="#33691e", zorder=3,
       label="Sustainable RPS (p99 <= ~14 ms, 0 fail)")

# overload ceiling line
ax.plot(x, overload_k, "o--", color="#d84315", lw=1.8, ms=7, zorder=4,
        label="Overload ceiling RPS")

# annotate sustainable value on each bar
for xi, v in zip(x, sustain_k):
    ax.text(xi, v + 0.6, f"{v}K", ha="center", va="bottom",
            fontsize=11.5, fontweight="bold", color="#1b5e20")

# annotate overload value on each marker
for xi, v in zip(x, overload_k):
    ax.text(xi, v + 1.5, f"{v:.1f}K", ha="center", va="bottom",
            fontsize=8.5, color="#bf360c")

# annotate "% of engine ceiling" at the base of each bar (subtle)
for xi, pct in zip(x, vs_engine):
    ax.text(xi, 2.2, f"{pct:g}%", ha="center", va="bottom",
            fontsize=8.5, color="#1b5e20", alpha=0.75)
ax.text(-0.55, 2.2, "% of\nengine", ha="right", va="bottom",
        fontsize=7.5, color="#1b5e20", alpha=0.75)

ax.set_xticks(x)
ax.set_xticklabels([p[0] for p in phases], fontsize=9.5)
ax.set_ylabel("RPS   (8 KB payload,  1 RTT = 4 dma_copy)", fontsize=12)
ax.set_ylim(0, 84)
ax.set_title("DPUmesh single-EU chain - cumulative optimization",
             fontsize=14, fontweight="bold", pad=12)
ax.legend(loc="lower right", fontsize=10, framealpha=0.95)
ax.grid(axis="y", ls=":", alpha=0.4, zorder=0)
ax.set_axisbelow(True)

# numbered description of each code change, in bar order
desc = "Code change at each step:\n" + "\n".join(
    f"  {p[0].split(chr(10))[0]}. {' '.join(p[0].split(chr(10))[1:])}: {p[1]}"
    for p in phases)
fig.text(0.012, 0.012, desc, fontsize=8.0, va="bottom", ha="left",
         family="monospace", color="#333")

# honesty note (bench.md S3.3)
fig.text(0.988, 0.012,
         "Cumulative climb is robust (0-fail back-to-back).\n"
         "Per-step deltas < ~2% sit at the run-to-run noise floor (+/-1.3%) -\n"
         "read the trend, not each individual increment.  (bench.md S3.3)",
         fontsize=8.0, va="bottom", ha="right", style="italic", color="#777")

fig.subplots_adjust(bottom=0.30, top=0.90)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "optimization_journey.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print("wrote", out)
