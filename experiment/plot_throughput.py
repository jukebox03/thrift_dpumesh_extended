#!/usr/bin/env python3
"""
dpumesh throughput test result analyzer (Go client run).

Input:  hard-coded measurements from `test-dpumesh.sh throughput <RPS>` using
        the new Go-based load generator (tput_client.go), 8 KB message size,
        connections auto-sized so the client itself is not the bottleneck.

Output: PNG plots in this directory:
          - throughput_bps.png        — hockey stick (Gbps vs avg latency)
          - latency_vs_rps.png        — percentile sweep
          - throughput_vs_latency.png — dual-axis (Gbps + P99)
          - queueing_gap.png          — service time vs schedule wait

Compared to the previous Python-client run, the ceiling moved from 21k RPS
(1.29 Gbps) to ~61k RPS (3.81 Gbps) — the Python figure was a client artifact.

Usage:
    python3 plot_throughput.py
"""

import os
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MSG_BYTES = 8192  # frame size, pad=8124

# Columns: target_rps, wall_rps, gbps,
#          corr_avg, corr_p50, corr_p95, corr_p99, corr_max,
#          raw_avg,  raw_p50,  raw_p95,  raw_p99,  raw_max
# Throughput (gbps) is wall-clock based — already corrected by tput_client.
DATA = [
    (10000,  9523.6, 0.624,    0.42,  0.32,   1.25,    1.69,    4.33,    0.23,  0.23,   0.53,    0.90,    4.30),
    (20000, 19047.4, 1.248,    0.22,  0.13,   0.45,    0.75,   31.82,    0.17,  0.08,   0.35,    0.61,   31.04),
    (30000, 28571.3, 1.872,    0.17,  0.10,   0.43,    0.80,    5.82,    0.14,  0.08,   0.38,    0.70,    5.12),
    (40000, 38094.8, 2.497,    0.14,  0.09,   0.35,    0.63,    7.75,    0.12,  0.07,   0.33,    0.60,    7.73),
    (50000, 47618.0, 3.121,    0.13,  0.09,   0.33,    0.62,   14.86,    0.11,  0.07,   0.31,    0.59,   14.85),
    (60000, 57144.4, 3.745,    0.13,  0.09,   0.30,    0.78,   18.91,    0.11,  0.07,   0.28,    0.68,   18.86),
    (61000, 58093.5, 3.807,    0.13,  0.09,   0.29,    0.60,   15.15,    0.11,  0.07,   0.27,    0.56,   15.11),
    (62000, 59045.8, 3.870,    1.51,  0.09,   8.60,   34.49,  174.58,    1.31,  0.07,   6.77,   30.59,  174.48),
    (63000, 59993.0, 3.932,   19.04,  7.82,  74.60,  125.40,  425.98,   14.13,  6.56,  53.16,   82.86,  233.19),
    (65000, 60689.3, 3.977,  111.87, 80.25, 324.58,  481.29, 1470.69,   34.80, 26.52,  99.52,  145.90,  367.18),
    (70000, 42620.5, 2.793, 3218.61,3260.91,6147.90, 6631.58, 7764.24,   61.51, 51.41, 178.81,  267.33,  874.20),
]

arr = np.array(DATA, dtype=float)
target_rps = arr[:, 0]
wall_rps   = arr[:, 1]
true_Gbps  = arr[:, 2]
corr_avg   = arr[:, 3]
corr_p50   = arr[:, 4]
corr_p95   = arr[:, 5]
corr_p99   = arr[:, 6]
corr_max   = arr[:, 7]
raw_avg    = arr[:, 8]
raw_p50    = arr[:, 9]
raw_p95    = arr[:, 10]
raw_p99    = arr[:, 11]
raw_max    = arr[:, 12]

# ---------------------------------------------------------------------------
# Ceiling = last point before the largest jump in end-to-end avg latency.
# Ignore the post-collapse 70k point when picking the knee — it's diagnostic,
# not a sustained operating point (wall-clock RPS dropped vs offered load).
# ---------------------------------------------------------------------------
collapse_mask = wall_rps < target_rps * 0.85           # >15 % deficit ⇒ collapse
clean_idx     = np.where(~collapse_mask)[0]
ratios_clean  = corr_avg[clean_idx][1:] / corr_avg[clean_idx][:-1]
# Ceiling = last point BEFORE the FIRST jump > 5× (saturation onset).
# Picking the largest ratio would land one step past saturation since the
# post-knee region keeps jumping multiplicatively.
big_jumps = np.where(ratios_clean > 5.0)[0]
jump_pos  = int(big_jumps[0]) if len(big_jumps) else int(np.argmax(ratios_clean))
ceiling_i     = int(clean_idx[jump_pos])               # last clean point before knee
ceiling_Gbps  = float(true_Gbps[ceiling_i])
ceiling_rps   = int(target_rps[ceiling_i])
ceiling_jump  = float(ratios_clean[jump_pos])
next_rps      = int(target_rps[clean_idx[jump_pos + 1]])

CLIENT_LABEL = "Go client (tput_client), conns=auto"


# ---------------------------------------------------------------------------
# Figure 1: hockey stick — true throughput (Gbps) vs latency (log)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 6))

# Skip the 70k collapse point for the curve — its Gbps is BELOW 65k because
# the system fell over, so plotting it on a Gbps axis reverses the line.
plot_mask = ~collapse_mask
ax.plot(true_Gbps[plot_mask], raw_avg[plot_mask], "o-", color="#2ca02c",
        linewidth=2.2, markersize=6,
        label="Service time   (send_ts → recv_done)")
ax.plot(true_Gbps[plot_mask], corr_avg[plot_mask], "s-", color="#d62728",
        linewidth=2.2, markersize=6,
        label="End-to-end     (sched_ts → recv_done)")

# Mark the collapse point separately (lower Gbps, huge latency)
if collapse_mask.any():
    ci = np.where(collapse_mask)[0][0]
    ax.plot(true_Gbps[ci], corr_avg[ci], "X", color="#000000", markersize=14,
            label=f"Collapse @ {int(target_rps[ci])//1000}k offered RPS")

# Annotate selected target-RPS points
label_targets = {10000, 30000, 50000, 60000, 61000, 62000, 65000}
for i in range(len(target_rps)):
    if int(target_rps[i]) in label_targets:
        ax.annotate(f"{int(target_rps[i])//1000}k",
                    (true_Gbps[i], corr_avg[i]),
                    xytext=(5, 5), textcoords="offset points",
                    fontsize=9, color="#444444")

# Sustainable-throughput ceiling — the knee
ax.axvline(ceiling_Gbps, color="#444444", linestyle="--", linewidth=1.5, alpha=0.7)
ymax_for_label = ax.get_ylim()[1] * 0.5
ax.text(ceiling_Gbps, ymax_for_label,
        f"  sustainable ceiling\n  {ceiling_Gbps:.2f} Gbps  ({ceiling_rps//1000}k RPS)",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#444444"))

ax.set_yscale("log")
ax.set_xlabel("Throughput (Gbps, wall-clock)", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title(f"Throughput–latency curve   (msg {MSG_BYTES} B, {CLIENT_LABEL})",
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

ax.plot(target_rps, raw_p50,  "o-",  color="#2ca02c",             label="Service time P50")
ax.plot(target_rps, raw_p95,  "s-",  color="#2ca02c", alpha=0.55, label="Service time P95")
ax.plot(target_rps, raw_p99,  "^-",  color="#2ca02c", alpha=0.35, label="Service time P99")
ax.plot(target_rps, corr_p50, "o--", color="#d62728",             label="End-to-end P50")
ax.plot(target_rps, corr_p95, "s--", color="#d62728", alpha=0.55, label="End-to-end P95")
ax.plot(target_rps, corr_p99, "^--", color="#d62728", alpha=0.35, label="End-to-end P99")

ax.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax.text(ceiling_rps, ax.get_ylim()[1] * 0.5,
        f"  ceiling {ceiling_rps//1000}k",
        ha="left", va="top", fontsize=9, color="#444444", fontweight="bold")

# Mark collapse zone
collapse_x = target_rps[collapse_mask]
if len(collapse_x):
    ax.axvspan(collapse_x.min(), target_rps.max(), color="#ffcccc", alpha=0.4)
    ax.text(collapse_x.min(), ax.get_ylim()[1] * 0.05, "  collapse",
            color="#990000", fontsize=9, fontweight="bold")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Latency (ms, log scale)", fontsize=11)
ax.set_title(f"Service time vs end-to-end latency across offered load  ({CLIENT_LABEL})",
             fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(ncol=2, fontsize=9, loc="upper left")
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_vs_rps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 3: dual-axis — throughput (Gbps bars+line) and P99 latency
# ---------------------------------------------------------------------------
fig, ax1 = plt.subplots(figsize=(10, 5.8))

bar_width = (target_rps[1] - target_rps[0]) * 0.7
ax1.bar(target_rps, true_Gbps, width=bar_width, alpha=0.30,
        color="#1f77b4", label="Throughput (Gbps, wall-clock)")
ax1.plot(target_rps, true_Gbps, "o-", color="#1f77b4", linewidth=1.8, markersize=5)
ax1.set_xlabel("Target RPS", fontsize=11)
ax1.set_ylabel("Throughput (Gbps)", color="#1f77b4", fontsize=11)
ax1.tick_params(axis="y", labelcolor="#1f77b4")
ax1.grid(True, alpha=0.25)

ax2 = ax1.twinx()
ax2.plot(target_rps, raw_p99,  "s-",  color="#2ca02c", linewidth=1.6, label="Service time P99")
ax2.plot(target_rps, corr_p99, "s--", color="#d62728", linewidth=1.6, label="End-to-end P99")
ax2.set_ylabel("Latency P99 (ms, log scale)", fontsize=11)
ax2.set_yscale("log")

ax1.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax1.text(ceiling_rps, true_Gbps.max() * 1.05,
         f"  ceiling {ceiling_Gbps:.2f} Gbps",
         ha="left", va="bottom", fontsize=9, color="#444444", fontweight="bold")

if len(collapse_x):
    ax1.axvspan(collapse_x.min(), target_rps.max(), color="#ffcccc", alpha=0.35)

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
ax1.set_title(f"Throughput and P99 latency vs offered load  ({CLIENT_LABEL})",
              fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_vs_latency.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 4: latency decomposition — service time vs schedule wait
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.5))

ax.fill_between(target_rps, 0, raw_avg,        color="#2ca02c", alpha=0.55,
                label="Service time   (send_ts → recv_done)")
ax.fill_between(target_rps, raw_avg, corr_avg, color="#d62728", alpha=0.40,
                label="Schedule wait  (sched_ts → send_ts)")
ax.plot(target_rps, corr_avg, "o-", color="#d62728", linewidth=1.5)
ax.plot(target_rps, raw_avg,  "o-", color="#2ca02c", linewidth=1.5)

ax.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax.text(ceiling_rps, ax.get_ylim()[1] * 0.5,
        f"  ceiling {ceiling_rps//1000}k",
        ha="left", va="top", fontsize=9, color="#444444", fontweight="bold")

if len(collapse_x):
    ax.axvspan(collapse_x.min(), target_rps.max(), color="#ffcccc", alpha=0.35)

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title(f"End-to-end latency = service time + schedule wait  ({CLIENT_LABEL})",
             fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "queueing_gap.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
peak_Gbps_idx = int(np.argmax(true_Gbps[~collapse_mask]))
peak_clean = np.where(~collapse_mask)[0][peak_Gbps_idx]

print(f"Msg size                : {MSG_BYTES} bytes")
print(f"Client                  : {CLIENT_LABEL}")
print(f"Sustainable ceiling     : {ceiling_rps:>6d} RPS  = {ceiling_Gbps:.3f} Gbps "
      f"(P99 corr {corr_p99[ceiling_i]:.2f} ms)")
print(f"  next step ({next_rps} RPS): end-to-end avg jumps {ceiling_jump:.1f}× "
      f"({corr_avg[ceiling_i]:.2f} → {corr_avg[ceiling_i+1]:.2f} ms)")
print(f"Peak Gbps (pre-collapse): {int(target_rps[peak_clean]):>6d} RPS  "
      f"= {true_Gbps[peak_clean]:.3f} Gbps "
      f"(P99 corr {corr_p99[peak_clean]:.1f} ms — past saturation)")
if collapse_mask.any():
    cidx = int(np.where(collapse_mask)[0][0])
    print(f"Collapse observed       : offered {int(target_rps[cidx])} RPS, "
          f"actual only {wall_rps[cidx]:.0f} RPS, "
          f"corrected avg {corr_avg[cidx]:.0f} ms")

print("\nSaved figures:")
for fname in ("throughput_bps.png", "latency_vs_rps.png",
              "throughput_vs_latency.png", "queueing_gap.png"):
    print(f"  {os.path.join(OUT_DIR, fname)}")
