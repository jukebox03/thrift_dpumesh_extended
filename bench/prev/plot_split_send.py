"""
DPU ARM core split (DPUMESH_SPLIT_SEND) vs single-thread: overload ceiling.

Data: bench.md S6.3 table. Grouped bars (overload ceiling RPS) by EU count,
split modes off / sends-only / rebalanced. The 'off' bar carries a +/-1.3%
run-to-run-noise whisker; every split delta lands inside it -> the ARM send
split does NOT raise the throughput ceiling (send is not the binding work).
dma_copy/s = overload RPS x 4 (exact).
"""
import os
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

C_OFF, C_SENDS, C_REBAL = "#90a4ae", "#4c72b0", "#d98c3f"
w = 0.24

# x, ceiling RPS, color, mode, delta-annotation
bars = [
    (-0.13, 75921,  C_OFF,   "off",        None),
    ( 0.13, 76079,  C_SENDS, "sends-only", "+0.2%\n(noise)"),
    (0.73,  104894, C_OFF,   "off",        None),
    (1.00,  103912, C_SENDS, "sends-only", "-0.9%\n(noise)"),
    (1.27,  107012, C_REBAL, "rebalanced", None),
]

fig, ax = plt.subplots(figsize=(11.5, 7.5))

for xx, v, c, mode, dl in bars:
    ax.bar(xx, v, w, color=c, edgecolor="#37474f", zorder=3)
    ax.text(xx, v + 700, f"{v:,}", ha="center", va="bottom",
            fontsize=9.5, fontweight="bold", color="#263238")

ax.set_xticks([0, 1])
ax.set_xticklabels(["1-EU", "2-EU"], fontsize=12.5, fontweight="bold")
ax.set_ylabel("Throughput (RPS)", fontsize=12)
ax.set_ylim(0, 116000)
ax.set_title("DPU ARM core split (SPLIT_SEND) vs single-thread: overload ceiling",
             fontsize=13.5, fontweight="bold", pad=12)
ax.grid(axis="y", ls=":", alpha=0.4, zorder=0)
ax.set_axisbelow(True)

legend_handles = [
    Patch(facecolor=C_OFF,   edgecolor="#37474f", label="off (single ARM thread)"),
    Patch(facecolor=C_SENDS, edgecolor="#37474f", label="sends-only (A=route, B=send)"),
    Patch(facecolor=C_REBAL, edgecolor="#37474f", label="rebalanced (A=drain, B=route+send)"),
]
ax.legend(handles=legend_handles, loc="upper left", fontsize=9, framealpha=0.95)

fig.subplots_adjust(bottom=0.10, top=0.91)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "split_send_ceiling.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print("wrote", out)
