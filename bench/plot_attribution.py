"""
Plot per-dma_copy attribution: M2 baseline (3.11 us/op) → dpumesh (4.55 us/op).
Generates 3 panels:
  1) Waterfall: how each component stacks from M2 to dpumesh
  2) Horizontal bar: components ranked by cost
  3) Pie: composition of the 1.44 us gap
"""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

# ---------------------------------------------------------------------------
# Data (from bench_clean.md §18, all values in microseconds per dma_copy)
# ---------------------------------------------------------------------------
M2_BASELINE = 3.11         # E0 baseline (321,803 dma_copy/s)
DPUMESH_TOTAL = 4.55       # dpumesh measured (220K dma_copy/s)
GAP = DPUMESH_TOTAL - M2_BASELINE  # 1.44 us = 32%

# Components: (short_label, long_label, cost_us, category, evidence)
# category: 'measured', 'arch', 'residual'
components = [
    ("yield/wake",        "Yield/wake architecture",          0.55, "arch",     "§13.2 (M2+yield+wake, -15.2%)"),
    ("comp_msg ×7",       "Larger comp_msg (7 vs 3 fields)",  0.29, "measured", "§11.3 E5 (M2 add, -8.6%)"),
    ("unmeasured",        "Unmeasured",                       0.27, "residual", "unmeasured (residual)"),
    ("producer_slot",     "Per-call ensure_producer_slot",    0.17, "measured", "§11.2 E3 (M2 add, -5.1%)"),
    ("handle_msgs",       "handle_msgs per iter",             0.06, "measured", "§11.3 E6 (M2 add, -2.0%)"),
    ("chunk wrap",        "Chunking loop wrapper",            0.05, "measured", "§11.2 E2 (M2 add, -1.6%)"),
    ("desc clear",        "Desc clear (lossless ctx)",        0.05, "measured", "§11.1 STRIP_DESC_CLEAR"),
    ("validation",        "Validation",                       0.00, "measured", "§11.2 E1 (noise)"),
    ("consumer wait",     "Extra consumer empty wait",        0.00, "measured", "§11.3 E7 (noise)"),
]

# Tuple positions: c[0]=short, c[1]=long, c[2]=cost, c[3]=cat, c[4]=evidence
# Sanity check: components sum to gap
assert abs(sum(c[2] for c in components) - GAP) < 0.01, \
    f"Components sum {sum(c[2] for c in components):.3f} != gap {GAP:.3f}"

COLORS = {
    "baseline": "#888888",   # gray — M2 reference
    "measured": "#4878CF",   # blue — directly attributed via add/remove tests
    "arch":     "#E5704D",   # orange — architectural cost (yield/wake)
    "residual": "#B8B8B8",   # light gray hatch — unmeasured residual
    "total":    "#C44E52",   # red — dpumesh final
}

# ---------------------------------------------------------------------------
# Figure 1: Waterfall (full width) + Pie inset
# ---------------------------------------------------------------------------
fig1, ax_water = plt.subplots(figsize=(14, 7.5))
fig1.subplots_adjust(left=0.07, right=0.97, top=0.84, bottom=0.13)

fig1.suptitle(
    "DPUmesh per-dma_copy cost attribution: M2 baseline (3.11 µs) → DPUmesh (4.55 µs)",
    fontsize=14, fontweight="bold", y=0.97
)

# Order: M2 baseline first, then components largest-first, then dpumesh total
sorted_comp = sorted(components, key=lambda c: -c[2])

labels = ["M2\nbaseline"] + [c[0] for c in sorted_comp] + ["DPUmesh\ntotal"]
values = [M2_BASELINE] + [c[2] for c in sorted_comp] + [DPUMESH_TOTAL]
n = len(labels)

# Cumulative bottoms for waterfall
bottoms = [0]
running = M2_BASELINE
for c in sorted_comp:
    bottoms.append(running)
    running += c[2]
bottoms.append(0)

colors = [COLORS["baseline"]]
for c in sorted_comp:
    colors.append(COLORS[c[3]])
colors.append(COLORS["total"])

bars = ax_water.bar(range(n), values, bottom=bottoms, color=colors,
                    edgecolor="black", linewidth=0.6, width=0.7)

# Hatch residual bar
for i, c in enumerate(sorted_comp, start=1):
    if c[3] == "residual":
        bars[i].set_hatch("////")

# Value labels above each bar
for i, (b, v, btm) in enumerate(zip(bars, values, bottoms)):
    if i == 0 or i == n - 1:
        ax_water.text(i, v + btm + 0.12, f"{v:.2f} µs",
                      ha="center", va="bottom", fontsize=11, fontweight="bold")
    else:
        if v > 0:
            pct = 100 * v / GAP
            ax_water.text(i, v + btm + 0.08,
                          f"+{v:.2f}\n({pct:.0f}%)",
                          ha="center", va="bottom", fontsize=9.5)
        else:
            ax_water.text(i, btm + 0.08, "~0", ha="center", va="bottom",
                          fontsize=9, style="italic", color="#666")

# Dotted connectors showing cumulative line
cumul_x, cumul_y = [0], [M2_BASELINE]
running = M2_BASELINE
for i, c in enumerate(sorted_comp, start=1):
    cumul_x.append(i - 0.35); cumul_y.append(running)
    cumul_x.append(i + 0.35); cumul_y.append(running + c[2])
    running += c[2]
cumul_x.append(n - 1); cumul_y.append(DPUMESH_TOTAL)
ax_water.plot(cumul_x, cumul_y, "k--", alpha=0.3, linewidth=1, zorder=0)

# Horizontal reference lines
ax_water.axhline(M2_BASELINE, color=COLORS["baseline"], linestyle=":", alpha=0.5,
                  linewidth=1)
ax_water.axhline(DPUMESH_TOTAL, color=COLORS["total"], linestyle=":", alpha=0.5,
                  linewidth=1)

ax_water.set_xticks(range(n))
ax_water.set_xticklabels(labels, fontsize=10, rotation=0)
ax_water.set_ylabel("Per-dma_copy time (µs)", fontsize=12)
ax_water.set_ylim(0, DPUMESH_TOTAL * 1.20)
ax_water.grid(axis="y", alpha=0.25, linestyle="--")
ax_water.set_axisbelow(True)

# Gap callout
ax_water.annotate(
    f"Gap = {GAP:.2f} µs (+{100*GAP/M2_BASELINE:.0f}%)",
    xy=(n - 1, DPUMESH_TOTAL), xytext=(n - 3.2, DPUMESH_TOTAL + 0.5),
    fontsize=11, fontweight="bold", color=COLORS["total"],
    arrowprops=dict(arrowstyle="->", color=COLORS["total"], alpha=0.6),
)

# Legend
legend_handles = [
    Patch(facecolor=COLORS["baseline"], edgecolor="black", linewidth=0.5,
          label="M2 baseline (reference)"),
    Patch(facecolor=COLORS["measured"], edgecolor="black", linewidth=0.5,
          label="Measured (direct add/remove tests)"),
    Patch(facecolor=COLORS["arch"], edgecolor="black", linewidth=0.5,
          label="Architectural (yield/wake — §13.2)"),
    Patch(facecolor=COLORS["residual"], edgecolor="black", linewidth=0.5,
          hatch="////", label="Unmeasured"),
    Patch(facecolor=COLORS["total"], edgecolor="black", linewidth=0.5,
          label="DPUmesh total"),
]
fig1.legend(handles=legend_handles, loc="upper center", fontsize=10,
             ncol=5, frameon=False, bbox_to_anchor=(0.5, 0.93))

out1 = "/home/jukebox/thrift_dma_copy/thrift_dpumesh_extended/bench/attribution_waterfall.png"
plt.savefig(out1, dpi=150, bbox_inches="tight")
print(f"Saved: {out1}")
plt.close(fig1)

# ---------------------------------------------------------------------------
# Figure 2: Horizontal bar with evidence + pie composition
# ---------------------------------------------------------------------------
fig2 = plt.figure(figsize=(20, 9))
gs2 = fig2.add_gridspec(1, 2, width_ratios=[3.0, 1.1],
                          wspace=0.20, left=0.20, right=0.97,
                          top=0.92, bottom=0.10)
ax_bar = fig2.add_subplot(gs2[0, 0])
ax_pie = fig2.add_subplot(gs2[0, 1])

fig2.suptitle("Component ranking & gap composition",
              fontsize=14, fontweight="bold", y=0.96)

# --- Horizontal bar
sorted_asc = sorted(components, key=lambda c: c[2])
labels_h = [c[1] for c in sorted_asc]
costs_h = [c[2] for c in sorted_asc]
colors_h = [COLORS[c[3]] for c in sorted_asc]
evidence_h = [c[4] for c in sorted_asc]

bars_h = ax_bar.barh(range(len(sorted_asc)), costs_h, color=colors_h,
                      edgecolor="black", linewidth=0.6, height=0.7)
for i, c in enumerate(sorted_asc):
    if c[3] == "residual":
        bars_h[i].set_hatch("////")

for i, (bar, cost, ev) in enumerate(zip(bars_h, costs_h, evidence_h)):
    width = bar.get_width()
    if cost > 0:
        ax_bar.text(width + 0.008, i, f"{cost:.2f} µs",
                    va="center", ha="left",
                    fontsize=12, fontweight="bold")
        ax_bar.text(width + 0.10, i, ev,
                    va="center", ha="left",
                    fontsize=10, color="#555", style="italic")
    else:
        ax_bar.text(0.005, i, "~0 µs", va="center", ha="left",
                    fontsize=11, color="#666", style="italic")
        ax_bar.text(0.07, i, ev, va="center", ha="left",
                    fontsize=10, color="#555", style="italic")

ax_bar.set_yticks(range(len(sorted_asc)))
ax_bar.set_yticklabels(labels_h, fontsize=12)
ax_bar.set_xlabel("Per-dma_copy cost (µs)", fontsize=12)
ax_bar.set_xlim(0, max(costs_h) * 1.95)
ax_bar.grid(axis="x", alpha=0.25, linestyle="--")
ax_bar.set_axisbelow(True)
ax_bar.axvline(GAP, color=COLORS["total"], linestyle=":", alpha=0.4, linewidth=1)
ax_bar.text(GAP + 0.008, -0.75, f"total gap = {GAP:.2f} µs",
             color=COLORS["total"], fontsize=10, fontweight="bold")
ax_bar.set_title("Components ranked by cost", fontsize=13, pad=8)
ax_bar.tick_params(axis="x", labelsize=10)

# --- Pie
cat_totals = {"measured": 0, "arch": 0, "residual": 0}
for c in components:
    cat_totals[c[3]] += c[2]

cat_labels = [
    f"Measured\n{cat_totals['measured']:.2f} µs",
    f"Architectural\n{cat_totals['arch']:.2f} µs",
    f"Unmeasured\n{cat_totals['residual']:.2f} µs",
]
cat_values = [cat_totals["measured"], cat_totals["arch"], cat_totals["residual"]]
cat_colors = [COLORS["measured"], COLORS["arch"], COLORS["residual"]]

wedges, texts, autotexts = ax_pie.pie(
    cat_values, labels=cat_labels, colors=cat_colors,
    autopct=lambda p: f"{p:.0f}%",
    startangle=90, counterclock=False,
    wedgeprops=dict(edgecolor="white", linewidth=1.5),
    textprops=dict(fontsize=12),
)
for at in autotexts:
    at.set_fontsize(13)
    at.set_fontweight("bold")
    at.set_color("white")
wedges[2].set_hatch("////")
ax_pie.set_title(f"Gap composition\n(total {GAP:.2f} µs = 32% of M2)",
                  fontsize=13, pad=8)

out2 = "/home/jukebox/thrift_dma_copy/thrift_dpumesh_extended/bench/attribution_ranking.png"
plt.savefig(out2, dpi=150, bbox_inches="tight")
print(f"Saved: {out2}")
plt.close(fig2)

# ---------------------------------------------------------------------------
# Figure 3: Per-component latency reduction (horizontal bar)
# ---------------------------------------------------------------------------
fig3, ax_t = plt.subplots(figsize=(11, 5.5))
fig3.subplots_adjust(left=0.30, right=0.96, top=0.88, bottom=0.16)

labels_t = [c[1] for c in sorted_asc]
costs_t = [c[2] for c in sorted_asc]
colors_t = [COLORS[c[3]] for c in sorted_asc]

bars_t = ax_t.barh(range(len(sorted_asc)), costs_t, color=colors_t,
                    edgecolor="black", linewidth=0.5, height=0.7)
for i, c in enumerate(sorted_asc):
    if c[3] == "residual":
        bars_t[i].set_hatch("////")

for i, (bar, cost) in enumerate(zip(bars_t, costs_t)):
    if cost > 0:
        ax_t.text(bar.get_width() + 0.005, i, f"−{cost:.2f} µs",
                  va="center", ha="left", fontsize=10, fontweight="bold")
    else:
        ax_t.text(0.003, i, "~0 µs (noise)", va="center", ha="left",
                  fontsize=9.5, color="#666", style="italic")

ax_t.set_yticks(range(len(sorted_asc)))
ax_t.set_yticklabels(labels_t, fontsize=10)
ax_t.set_xlabel(
    "Potential latency reduction if removed (µs)\n"
    "current DPUmesh = 4.55 µs  →  M2 baseline = 3.11 µs (floor)",
    fontsize=10.5
)
ax_t.set_title("Component impact on per-dma_copy latency", fontsize=12.5, pad=8,
                fontweight="bold")
ax_t.grid(axis="x", alpha=0.25, linestyle="--")
ax_t.set_axisbelow(True)
ax_t.set_xlim(0, max(costs_t) * 1.45)

out3 = "/home/jukebox/thrift_dma_copy/thrift_dpumesh_extended/bench/attribution_latency.png"
plt.savefig(out3, dpi=150, bbox_inches="tight")
print(f"Saved: {out3}")
plt.close(fig3)
