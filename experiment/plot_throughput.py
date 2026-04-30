#!/usr/bin/env python3
"""
DPUmesh throughput sweep analysis (2026-04-30 data set).

Forward path: gateway -> DPU/DPA -> unique-id-service -> DPU/DPA -> gateway.
Single sweep at 8192 B frame, nominal 30 s per point, conns = rps/25.

Key features of this dataset:
  - Linear scaling 5k -> 45k offered RPS (wall_rps tracks offered at ~98 %).
  - Hard ceiling at 45k offered = 44.3k wall RPS / 2.90 Gbps with 100 % OK
    and P99 = 7.55 ms.
  - Overload collapse at 50k offered: wall-clock test duration extends from
    30.5 s to 52.16 s; effective throughput DROPS from 44.3k to 28.8k (i.e.
    not graceful saturation - the 1024-slot pool / single DPU ARM thread
    cannot keep up and queueing tail dominates).
  - Recovery confirmed: rerun @ 40k after the 50k/55k overdrive runs still
    achieves 39.3k wall RPS at sub-ms P50, proving no state/slot leak.

Output PNGs:
  - throughput_bps.png        - hockey stick (Gbps vs avg latency)
  - latency_vs_rps.png        - percentile sweep (raw + e2e)
  - throughput_vs_latency.png - wall RPS vs offered + e2e P99
  - queueing_gap.png          - service time vs schedule wait

Usage:
    python3 plot_throughput.py
"""

import os
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MSG_BYTES = 8192  # frame size, pad=8116

# Columns: target_rps, wall_rps, gbps,
#          corr_avg, corr_p50, corr_p95, corr_p99, corr_max,
#          raw_avg,  raw_p50,  raw_p95,  raw_p99,  raw_max
# All latencies in ms. gbps is wall-clock based (DMA bandwidth).
DATA = [
    ( 5000,  4917.5, 0.322,    2.42,    2.32,    3.95,    4.96,    14.09,  2.10,  2.00,   3.79,    4.77,   14.04),
    (10000,  9835.3, 0.645,    1.74,    1.63,    3.17,    4.53,    30.25,  1.50,  1.29,   2.94,    4.17,   29.62),
    (15000, 14752.3, 0.967,    1.36,    1.24,    2.65,    3.66,    29.58,  1.14,  1.04,   2.32,    3.35,   29.02),
    (20000, 19670.0, 1.289,    1.13,    1.04,    2.24,    2.97,    13.09,  0.92,  0.86,   1.97,    2.73,   12.20),
    (25000, 24589.9, 1.612,    0.96,    0.81,    2.08,    2.95,    36.56,  0.82,  0.66,   1.82,    2.70,   36.42),
    (30000, 29506.9, 1.934,    0.71,    0.58,    1.49,    2.30,    11.84,  0.67,  0.55,   1.39,    2.08,   11.41),
    (35000, 34424.5, 2.256,    0.67,    0.51,    1.48,    2.47,    13.79,  0.64,  0.48,   1.41,    2.29,   13.77),
    (40000, 39343.7, 2.578,    0.66,    0.43,    1.44,    3.73,    38.72,  0.63,  0.41,   1.38,    3.65,   33.12),
    (45000, 44260.2, 2.901,    0.62,    0.34,    1.51,    7.55,    17.75,  0.59,  0.32,   1.48,    7.47,   17.73),
    (50000, 28756.5, 1.885,  495.92,    3.52, 2672.86, 7682.99, 21667.69, 18.07,  3.49,  48.57,  154.02,  186.41),
    (55000, 27881.5, 1.827, 1984.11, 1353.62, 5304.38, 9521.09, 28680.30, 40.66, 44.13,  53.29,  156.15,  189.47),
]


def to_arrays(rows):
    a = np.array(rows, dtype=float)
    return {
        "target":    a[:, 0],
        "wall":      a[:, 1],
        "gbps":      a[:, 2],
        "corr_avg":  a[:, 3],
        "corr_p50":  a[:, 4],
        "corr_p95":  a[:, 5],
        "corr_p99":  a[:, 6],
        "corr_max":  a[:, 7],
        "raw_avg":   a[:, 8],
        "raw_p50":   a[:, 9],
        "raw_p95":   a[:, 10],
        "raw_p99":   a[:, 11],
        "raw_max":   a[:, 12],
    }


def detect_knee(d):
    """Last point before the first ratio>5x in end-to-end avg."""
    ratios = d["corr_avg"][1:] / d["corr_avg"][:-1]
    big = np.where(ratios > 5.0)[0]
    pos = int(big[0]) if len(big) else int(np.argmax(ratios))
    return pos, float(d["gbps"][pos]), int(d["target"][pos]), float(ratios[pos])


d = to_arrays(DATA)
knee_idx, knee_gbps, knee_rps, knee_jump = detect_knee(d)
peak_idx = int(np.argmax(d["wall"]))


# ---------------------------------------------------------------------------
# Figure 1: hockey stick - true throughput (Gbps) vs avg latency (log)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.8))

ax.plot(d["gbps"], d["corr_avg"], "s-", color="#1f77b4",
        linewidth=2.2, markersize=7, label="end-to-end (corrected)")
ax.plot(d["gbps"], d["raw_avg"],  "o-", color="#2ca02c",
        linewidth=2.0, markersize=6, label="service time (raw RTT)")

ax.axvline(d["gbps"][peak_idx], color="#1f77b4", linestyle="--",
           linewidth=1.4, alpha=0.7)
ax.text(d["gbps"][peak_idx], 0.5,
        f"hard ceiling\n{d['gbps'][peak_idx]:.2f} Gbps\n"
        f"({int(d['wall'][peak_idx])//1000}k wall RPS)",
        ha="right", va="bottom", fontsize=10, color="#0d4a8b", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#1f77b4"))

# Annotate every point with its target RPS
for i in range(len(d["target"])):
    rps = int(d["target"][i])
    ax.annotate(f"{rps//1000}k", (d["gbps"][i], d["corr_avg"][i]),
                xytext=(5, 5), textcoords="offset points",
                fontsize=9, color="#0d4a8b")

ax.set_yscale("log")
ax.set_xlabel("Throughput (Gbps, wall-clock)", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title(f"Throughput-latency curve  (forward path, msg {MSG_BYTES} B)",
             fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_bps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 2: latency percentiles vs offered load
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.8))

ax.plot(d["target"], d["raw_p50"],  "o--", color="#2ca02c", alpha=0.7,
        linewidth=1.5, label="raw P50 (service time)")
ax.plot(d["target"], d["raw_p99"],  "^--", color="#2ca02c", alpha=0.7,
        linewidth=1.5, label="raw P99 (service time)")
ax.plot(d["target"], d["corr_p50"], "o-",  color="#1f77b4",
        linewidth=2.0, label="e2e P50 (corrected)")
ax.plot(d["target"], d["corr_p99"], "^-",  color="#d62728",
        linewidth=2.0, label="e2e P99 (corrected)")

ax.axvline(knee_rps, color="#444444", linestyle="--", linewidth=1.4, alpha=0.7)
ax.text(knee_rps, ax.get_ylim()[1] * 0.5, f"  knee {knee_rps//1000}k",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold")

# Overdrive zone (target > knee)
overdrive = d["target"] > knee_rps
od = d["target"][overdrive]
if len(od):
    ax.axvspan(od.min(), d["target"].max(), color="#fff4b3", alpha=0.4, zorder=0)
    ax.text(od.min(), ax.get_ylim()[1] * 0.05,
            "  overdrive (queueing collapse, 100% OK)",
            color="#7a5b00", fontsize=9, fontweight="bold")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Latency (ms, log scale)", fontsize=11)
ax.set_title("Latency percentiles vs offered load (forward path)", fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_vs_rps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 3: dual-axis - wall-clock RPS (left) vs target,
#           plus e2e P99 latency (right).
# ---------------------------------------------------------------------------
fig, ax1 = plt.subplots(figsize=(10, 5.8))

ident_max_k = d["target"].max() / 1000
ident_x = np.linspace(0, ident_max_k, 200)
ax1.plot(ident_x, ident_x, ":", color="#888888", linewidth=1.2,
         label="ideal (wall = offered)")

ax1.plot(d["target"] / 1000, d["wall"] / 1000, "o-", color="#1f77b4",
         linewidth=2.4, markersize=7, label="wall RPS (k)")

ax1.set_xlabel("Target RPS (k)", fontsize=11)
ax1.set_ylabel("Achieved wall-clock RPS (k)", fontsize=11)
ax1.grid(True, alpha=0.3)

ax2 = ax1.twinx()
ax2.plot(d["target"] / 1000, d["corr_p99"], "^-", color="#d62728",
         linewidth=1.8, markersize=5, label="e2e P99 (ms)")
ax2.set_ylabel("End-to-end P99 latency (ms, log scale)", fontsize=11)
ax2.set_yscale("log")

# Plateau / collapse annotation
ax1.annotate(f"hard ceiling\n{int(d['wall'][peak_idx])//1000}k wall RPS  "
             f"({d['gbps'][peak_idx]:.2f} Gbps)\n"
             f"@ {int(d['target'][peak_idx])//1000}k offered",
             xy=(d["target"][peak_idx] / 1000, d["wall"][peak_idx] / 1000),
             xytext=(20, d["wall"][peak_idx] / 1000 - 12),
             fontsize=9, color="#0d4a8b",
             arrowprops=dict(arrowstyle="->", color="#0d4a8b"))

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
ax1.set_title("Achieved RPS vs offered load (forward path)", fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_vs_latency.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 4: latency decomposition - service time vs schedule wait
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.4))

ax.fill_between(d["target"], 0, d["raw_avg"], color="#2ca02c", alpha=0.55,
                label="Service time   (send_ts -> recv_done)")
ax.fill_between(d["target"], d["raw_avg"], d["corr_avg"],
                color="#d62728", alpha=0.40,
                label="Schedule wait  (sched_ts -> send_ts)")
ax.plot(d["target"], d["corr_avg"], "o-", color="#d62728", linewidth=1.5)
ax.plot(d["target"], d["raw_avg"],  "o-", color="#2ca02c", linewidth=1.5)

ax.axvline(knee_rps, color="#444444", linestyle="--", linewidth=1.4, alpha=0.7)
ax.text(knee_rps, ax.get_ylim()[1] * 0.5, f"  knee {knee_rps//1000}k",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title("End-to-end latency = service time + schedule wait", fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "queueing_gap.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Stdout summary
# ---------------------------------------------------------------------------
print(f"Msg size              : {MSG_BYTES} bytes")
print(f"Knee                  : {knee_rps//1000}k RPS = {knee_gbps:.3f} Gbps "
      f"(P99 corr {d['corr_p99'][knee_idx]:.2f} ms)")
if knee_idx + 1 < len(d["target"]):
    print(f"  next step ({int(d['target'][knee_idx+1])} RPS): "
          f"e2e avg jumps {knee_jump:.1f}x "
          f"({d['corr_avg'][knee_idx]:.2f} -> {d['corr_avg'][knee_idx+1]:.2f} ms)")
print(f"Hard ceiling          : {int(d['wall'][peak_idx])} wall RPS  "
      f"= {d['gbps'][peak_idx]:.3f} Gbps "
      f"({int(d['target'][peak_idx])} offered)")
print(f"Overdrive (failed=0)  : 50k/55k offered still 100% OK but "
      f"throughput collapses to ~28k due to queueing past nominal duration")

print("\nSaved figures:")
for fname in ("throughput_bps.png", "latency_vs_rps.png",
              "throughput_vs_latency.png", "queueing_gap.png"):
    print(f"  {os.path.join(OUT_DIR, fname)}")
