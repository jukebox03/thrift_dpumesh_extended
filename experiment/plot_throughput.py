#!/usr/bin/env python3
"""
dpumesh throughput test result analyzer.

Input: hard-coded measurements from wrk2-style throughput test
       (test-dpumesh.sh throughput <RPS>) with 8KB message size.
Output: PNG plots in the same directory, showing throughput (bps) and
        latency behaviour across the target-RPS sweep.

Usage:
    python3 plot_throughput.py
"""

import os
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MSG_BYTES = 8192  # frame size used in the sweep (pad=8124)

# columns: target_rps, actual_rps, bw_MBps,
#          corr_avg, corr_p50, corr_p95, corr_p99, corr_max,
#          raw_avg,  raw_p50,  raw_p95,  raw_p99,  raw_max
DATA = [
    ( 1000,  1000.0,   7.81,   0.52,   0.49,   0.62,   0.70,   23.01,  0.41,  0.41,   0.50,   0.54,  15.19),
    ( 8000,  8000.0,  62.50,   1.11,   0.36,   2.48,   6.47,  145.60,  0.63,  0.27,   1.85,   3.34, 123.92),
    ( 9000,  9000.0,  70.31,   1.38,   0.34,   2.56,  22.22,  185.43,  0.69,  0.24,   1.92,   3.76, 184.92),
    (10000, 10000.0,  78.12,   1.31,   0.31,   2.61,  23.12,  168.72,  0.68,  0.20,   1.98,   4.26, 156.28),
    (11000, 11000.0,  85.94,   2.13,   0.26,   2.71,  66.94,  261.97,  0.79,  0.17,   2.00,   4.39, 261.44),
    (12000, 12000.0,  93.75,   1.85,   0.28,   2.85,  43.57,  248.06,  0.80,  0.18,   2.17,   5.76, 243.58),
    (13000, 13000.0, 101.56,   2.50,   0.37,   4.46,  58.54,  305.34,  1.05,  0.27,   3.03,  10.15, 303.63),
    (14000, 14000.0, 109.38,   3.77,   0.49,   5.24, 118.08,  413.46,  1.17,  0.35,   3.21,   9.98, 407.64),
    (15000, 15000.0, 117.19,   3.92,   0.72,   8.34, 108.54,  393.28,  1.40,  0.54,   4.87,  12.44, 390.69),
    (17000, 17000.0, 132.81,   9.16,   3.11,  18.74, 188.46,  462.19,  3.99,  2.48,  11.49,  18.71, 456.65),
    (18000, 18000.0, 140.62,  17.71,   7.40,  41.10, 299.13,  557.05,  6.93,  5.49,  16.73,  23.79, 554.57),
    (19000, 19000.0, 148.44,  23.42,  10.63,  81.75, 337.98,  564.81,  8.70,  7.45,  18.63,  25.80, 529.15),
    (20000, 20000.0, 156.25,  35.48,  16.45, 168.22, 400.34,  595.84, 10.43,  9.03,  22.04,  29.80, 568.38),
    (21000, 21000.0, 164.06,  56.63,  24.41, 280.29, 442.65,  639.57, 10.76,  9.37,  22.95,  31.90, 570.08),
    (22000, 22000.0, 171.88, 278.67, 234.38, 672.44, 844.73, 1160.18, 11.61,  9.99,  24.74,  33.06, 602.95),
    (23000, 23000.0, 179.69, 359.98, 332.43, 806.13, 971.97, 1344.31, 11.50,  9.81,  24.59,  32.84, 608.04),
    (24000, 24000.0, 187.50, 674.24, 653.83,1331.54,1546.90, 1858.08, 11.64,  9.90,  24.93,  33.86, 691.67),
    (25000, 25000.0, 195.31, 812.29, 782.37,1576.05,1849.56, 2308.05, 11.56,  9.98,  24.42,  32.75, 688.39),
    (26000, 26000.0, 203.12, 979.37, 960.50,1843.11,2085.90, 2404.20, 11.31,  9.70,  24.15,  32.43, 692.24),
    (27000, 27000.0, 210.94,1447.93,1445.72,2726.59,3012.24, 3344.56, 11.84, 10.05,  25.40,  34.25, 662.11),
    (28000, 28000.0, 218.75,1601.89,1636.24,2934.46,3196.13, 3575.61, 11.68, 10.01,  25.07,  33.60, 574.92),
    (30000, 30000.0, 234.38,2074.37,2214.30,3650.49,3956.06, 4391.62, 11.53, 10.06,  24.74,  34.13, 707.41),
]

arr = np.array(DATA, dtype=float)
target_rps   = arr[:, 0]
actual_rps   = arr[:, 1]
bw_MiBps     = arr[:, 2]                          # what the script prints (MiB/s)
corr_avg     = arr[:, 3]
corr_p50     = arr[:, 4]
corr_p95     = arr[:, 5]
corr_p99     = arr[:, 6]
corr_max     = arr[:, 7]
raw_avg      = arr[:, 8]
raw_p50      = arr[:, 9]
raw_p95      = arr[:, 10]
raw_p99      = arr[:, 11]
raw_max      = arr[:, 12]

# ------------------------------------------------------------------------
# The test script (test_thrift.py:371-380) divides ok_count by the *nominal*
# duration (10 s), not the wall-clock elapsed time. Once the backlog grows,
# the test actually runs for (duration + tail-latency) seconds, so reported
# throughput is inflated by exactly that ratio.
#
# We reconstruct the real elapsed time as:
#     real_elapsed_s ≈ duration_s + (max corrected latency / 1000)
# because the last request to complete defines when the test actually ended,
# and its corrected latency is the time from its scheduled slot to its reply.
# ------------------------------------------------------------------------
NOMINAL_DURATION_S = 10.0
real_elapsed_s    = NOMINAL_DURATION_S + corr_max / 1000.0
total_reqs        = target_rps * NOMINAL_DURATION_S
total_bytes       = total_reqs * MSG_BYTES

# Reported (inflated) throughput — what the test prints.
throughput_bps  = actual_rps * MSG_BYTES * 8
throughput_Gbps = throughput_bps / 1e9
throughput_Mbps = throughput_bps / 1e6

# True throughput — bytes ÷ actual wall-clock elapsed.
true_Gbps = total_bytes * 8 / real_elapsed_s / 1e9

CLIENT_THREADS = 256

# Data-driven ceiling: the throughput at the last offered-load step BEFORE the
# biggest jump in end-to-end latency (the knee of the hockey stick).
ratios = corr_avg[1:] / corr_avg[:-1]
jump_idx = int(np.argmax(ratios))
ceiling_i    = jump_idx
ceiling_Gbps = float(true_Gbps[ceiling_i])
ceiling_rps  = int(target_rps[ceiling_i])
ceiling_next_rps = int(target_rps[ceiling_i + 1])
ceiling_jump = float(ratios[jump_idx])


# ---------------------------------------------------------------------------
# Figure 1: throughput–latency curve. X = true throughput. Y = latency (log).
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 6))

ax.plot(true_Gbps, raw_avg,  "o-", color="#2ca02c", linewidth=2.2, markersize=6,
        label="Service time   (send_ts → recv_done)")
ax.plot(true_Gbps, corr_avg, "s-", color="#d62728", linewidth=2.2, markersize=6,
        label="End-to-end     (sched_ts → recv_done)")

# Label a handful of points with their offered RPS.
for i in range(len(target_rps)):
    rps = int(target_rps[i])
    if rps in (1000, 10000, 17000, 21000, 25000, 30000):
        ax.annotate(f"{rps//1000}k RPS",
                    (true_Gbps[i], corr_avg[i]),
                    xytext=(5, 5), textcoords="offset points",
                    fontsize=8.5, color="#555555")

# Sustainable-throughput ceiling — the knee of the end-to-end curve.
ax.axvline(ceiling_Gbps, color="#444444", linestyle="--", linewidth=1.5, alpha=0.7)
ax.text(ceiling_Gbps, ax.get_ylim()[1] if False else 1500,
        f"  sustainable ceiling\n  {ceiling_Gbps:.2f} Gbps  ({ceiling_rps//1000}k RPS)",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#444444"))

ax.set_yscale("log")
ax.set_xlabel("Throughput (Gbps)", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title(f"Throughput–latency curve   (msg {MSG_BYTES} B, {CLIENT_THREADS} client threads)",
             fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_bps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 2: Latency percentiles vs offered load.
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.8))
ax.plot(target_rps, raw_p50,  "o-",  color="#2ca02c",               label="Service time P50")
ax.plot(target_rps, raw_p95,  "s-",  color="#2ca02c", alpha=0.55,   label="Service time P95")
ax.plot(target_rps, raw_p99,  "^-",  color="#2ca02c", alpha=0.35,   label="Service time P99")
ax.plot(target_rps, corr_p50, "o--", color="#d62728",               label="End-to-end P50")
ax.plot(target_rps, corr_p95, "s--", color="#d62728", alpha=0.55,   label="End-to-end P95")
ax.plot(target_rps, corr_p99, "^--", color="#d62728", alpha=0.35,   label="End-to-end P99")

ax.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax.text(ceiling_rps, ax.get_ylim()[1] * 0.8 if False else 1500,
        f"  ceiling {ceiling_rps//1000}k RPS",
        ha="left", va="top", fontsize=9, color="#444444")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Latency (ms, log scale)", fontsize=11)
ax.set_title("Service time vs end-to-end latency across offered load", fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(ncol=2, fontsize=9, loc="upper left")
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_vs_rps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 3: dual-axis — throughput (Gbps, bars+line) and latency (right axis).
# ---------------------------------------------------------------------------
fig, ax1 = plt.subplots(figsize=(10, 5.8))
ax1.bar(target_rps, true_Gbps, width=700, alpha=0.30,
        color="#1f77b4", label="Throughput (Gbps)")
ax1.plot(target_rps, true_Gbps, "o-", color="#1f77b4", linewidth=1.8, markersize=5)
ax1.set_xlabel("Target RPS", fontsize=11)
ax1.set_ylabel("Throughput (Gbps)", color="#1f77b4", fontsize=11)
ax1.tick_params(axis="y", labelcolor="#1f77b4")
ax1.grid(True, alpha=0.25)

ax2 = ax1.twinx()
ax2.plot(target_rps, raw_p99,  "s-",  color="#2ca02c", linewidth=1.6,
         label="Service time P99")
ax2.plot(target_rps, corr_p99, "s--", color="#d62728", linewidth=1.6,
         label="End-to-end P99")
ax2.set_ylabel("Latency P99 (ms, log scale)", fontsize=11)
ax2.set_yscale("log")

ax1.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax1.text(ceiling_rps, true_Gbps.max() * 1.02,
         f"  ceiling {ceiling_Gbps:.2f} Gbps",
         ha="left", va="bottom", fontsize=9, color="#444444", fontweight="bold")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
ax1.set_title("Throughput and P99 latency vs offered load", fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_vs_latency.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 4: latency decomposition — service time vs schedule wait.
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.5))
ax.fill_between(target_rps, 0, raw_avg,        color="#2ca02c", alpha=0.55,
                label="Service time  (send_ts → recv_done)")
ax.fill_between(target_rps, raw_avg, corr_avg, color="#d62728", alpha=0.40,
                label="Schedule wait  (sched_ts → send_ts)")
ax.plot(target_rps, corr_avg, "o-", color="#d62728", linewidth=1.5)
ax.plot(target_rps, raw_avg,  "o-", color="#2ca02c", linewidth=1.5)

ax.axvline(ceiling_rps, color="#444444", linestyle="--", linewidth=1.2, alpha=0.6)
ax.text(ceiling_rps, ax.get_ylim()[1] * 0.8 if False else 1500,
        f"  ceiling {ceiling_rps//1000}k RPS",
        ha="left", va="top", fontsize=9, color="#444444")

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
# Summary printed to stdout.
# ---------------------------------------------------------------------------
peak_true_idx = int(np.argmax(true_Gbps))
print(f"Msg size              : {MSG_BYTES} bytes")
print(f"Client threads        : {CLIENT_THREADS}")
print(f"Sustainable ceiling   : {ceiling_rps:>6d} RPS  = {ceiling_Gbps:.3f} Gbps")
print(f"                         (end-to-end latency jumps {ceiling_jump:.1f}× at the next step, "
      f"{ceiling_next_rps} RPS)")
print(f"Peak measured         : {int(target_rps[peak_true_idx]):>6d} RPS  "
      f"= {true_Gbps.max():.3f} Gbps")

print("\nSaved figures:")
for fname in ("throughput_bps.png", "latency_vs_rps.png",
              "throughput_vs_latency.png", "queueing_gap.png"):
    print(f"  {os.path.join(OUT_DIR, fname)}")
