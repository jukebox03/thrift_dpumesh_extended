#!/usr/bin/env python3
"""
dpumesh throughput test result analyzer.

Produces four PNG plots in this directory using two datasets:
  - PRE   : Go client + original gateway (pthread-per-conn) and original
            dpumesh transport (nanosleep-based fc/slot waits, MAX_PENDING=4096)
  - POST  : Go client + epoll thread-pool gateway, dpumesh with cond_var-based
            slot/fc waits, MAX_PENDING=65536

Key story (PRE → POST):
  * Sustainable ceiling moved 61k RPS / 3.81 Gbps → 100k RPS / 6.24 Gbps (1.64×)
  * 70k offered no longer collapses (43k actual → 118k actual at 130k offered);
    the system now degrades gracefully when overdriven instead of falling off
    a cliff.

Output PNGs:
  - throughput_bps.png        — hockey stick (Gbps vs avg latency), POST + PRE
  - latency_vs_rps.png        — percentile sweep, POST + PRE
  - throughput_vs_latency.png — dual-axis (Gbps + P99) with ceiling markers
  - queueing_gap.png          — service time vs schedule wait (POST)

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
# Throughput (gbps) is wall-clock based.

# PRE — original gateway + original dpumesh transport (Go client baseline).
PRE = [
    (10000,  9523.6,  0.624,    0.42,  0.32,   1.25,    1.69,    4.33,    0.23,  0.23,   0.53,    0.90,    4.30),
    (20000, 19047.4,  1.248,    0.22,  0.13,   0.45,    0.75,   31.82,    0.17,  0.08,   0.35,    0.61,   31.04),
    (30000, 28571.3,  1.872,    0.17,  0.10,   0.43,    0.80,    5.82,    0.14,  0.08,   0.38,    0.70,    5.12),
    (40000, 38094.8,  2.497,    0.14,  0.09,   0.35,    0.63,    7.75,    0.12,  0.07,   0.33,    0.60,    7.73),
    (50000, 47618.0,  3.121,    0.13,  0.09,   0.33,    0.62,   14.86,    0.11,  0.07,   0.31,    0.59,   14.85),
    (60000, 57144.4,  3.745,    0.13,  0.09,   0.30,    0.78,   18.91,    0.11,  0.07,   0.28,    0.68,   18.86),
    (61000, 58093.5,  3.807,    0.13,  0.09,   0.29,    0.60,   15.15,    0.11,  0.07,   0.27,    0.56,   15.11),
    (62000, 59045.8,  3.870,    1.51,  0.09,   8.60,   34.49,  174.58,    1.31,  0.07,   6.77,   30.59,  174.48),
    (63000, 59993.0,  3.932,   19.04,  7.82,  74.60,  125.40,  425.98,   14.13,  6.56,  53.16,   82.86,  233.19),
    (65000, 60689.3,  3.977,  111.87, 80.25, 324.58,  481.29, 1470.69,   34.80, 26.52,  99.52,  145.90,  367.18),
    (70000, 42620.5,  2.793, 3218.61,3260.91,6147.90, 6631.58, 7764.24,   61.51, 51.41, 178.81,  267.33,  874.20),
]

# POST — epoll gateway + cond_var dpumesh transport + MAX_PENDING=65536.
POST = [
    ( 10000,   9523.6,  0.624,    0.46,    0.34,    1.27,    1.64,    4.17,    0.23,  0.17,   0.52,   0.77,    3.24),
    ( 30000,  28571.4,  1.872,    0.12,    0.09,    0.29,    0.46,    5.00,    0.09,  0.07,   0.27,   0.39,    3.47),
    ( 50000,  47618.5,  3.121,    0.12,    0.09,    0.24,    0.50,    6.22,    0.10,  0.07,   0.20,   0.44,    6.18),
    ( 70000,  66666.9,  4.369,    0.12,    0.10,    0.26,    0.54,    6.79,    0.11,  0.08,   0.23,   0.51,    6.77),
    ( 90000,  85708.1,  5.617,    0.14,    0.10,    0.25,    0.54,   10.37,    0.12,  0.08,   0.22,   0.50,   10.35),
    (100000,  95226.6,  6.241,    0.15,    0.11,    0.28,    0.72,   10.93,    0.13,  0.09,   0.25,   0.67,   10.91),
    (110000, 104757.2,  6.865,    0.78,    0.12,    1.26,   22.45,   74.71,    0.73,  0.10,   1.18,  21.88,   56.54),
    (120000, 114045.7,  7.474,   11.05,    0.39,   75.99,  146.61,  213.07,    6.32,  0.37,  39.99,  46.61,   58.07),
    (130000, 118051.5,  7.737,  197.88,   67.14,  864.54, 1306.96, 1501.05,   26.70, 35.14,  50.71,  55.59,   71.66),
    (140000, 118135.3,  7.742,  678.60,  525.08, 1813.42, 2316.84, 2592.09,   41.96, 43.95,  55.31,  60.72,   85.75),
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


def detect_ceiling(d):
    """Return (idx, gbps, target_rps, jump_ratio, next_target).
    Last point BEFORE the first ratio>5x in end-to-end avg, ignoring
    points where wall_rps < 0.85 * target (collapse / saturation cap)."""
    collapse_mask = d["wall"] < d["target"] * 0.85
    clean = np.where(~collapse_mask)[0]
    if len(clean) < 2:
        i = int(np.argmax(d["gbps"]))
        return i, float(d["gbps"][i]), int(d["target"][i]), float("nan"), int(d["target"][i])
    ratios = d["corr_avg"][clean][1:] / d["corr_avg"][clean][:-1]
    big = np.where(ratios > 5.0)[0]
    pos = int(big[0]) if len(big) else int(np.argmax(ratios))
    i = int(clean[pos])
    nxt = int(d["target"][clean[min(pos + 1, len(clean) - 1)]])
    return i, float(d["gbps"][i]), int(d["target"][i]), float(ratios[pos]), nxt


pre  = to_arrays(PRE)
post = to_arrays(POST)

pre_ceil  = detect_ceiling(pre)
post_ceil = detect_ceiling(post)


# ---------------------------------------------------------------------------
# Figure 1: hockey stick — true throughput (Gbps) vs avg latency (log)
#           PRE and POST overlaid.
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 6.2))

# PRE — exclude the collapse point so the line doesn't reverse on Gbps axis
pre_collapse = pre["wall"] < pre["target"] * 0.85
mpre = ~pre_collapse
ax.plot(pre["gbps"][mpre], pre["corr_avg"][mpre], "s--", color="#d62728",
        linewidth=1.7, markersize=5, alpha=0.7,
        label="PRE  end-to-end")
ax.plot(pre["gbps"][mpre], pre["raw_avg"][mpre],  "o--", color="#ff9896",
        linewidth=1.7, markersize=5, alpha=0.7,
        label="PRE  service time")
if pre_collapse.any():
    ci = int(np.where(pre_collapse)[0][0])
    ax.plot(pre["gbps"][ci], pre["corr_avg"][ci], "X", color="#000000",
            markersize=14, label=f"PRE collapse ({int(pre['target'][ci])//1000}k offered)")

# POST — same convention
post_collapse = post["wall"] < post["target"] * 0.85
mpost = ~post_collapse
ax.plot(post["gbps"][mpost], post["corr_avg"][mpost], "s-", color="#1f77b4",
        linewidth=2.4, markersize=7,
        label="POST end-to-end")
ax.plot(post["gbps"][mpost], post["raw_avg"][mpost],  "o-", color="#2ca02c",
        linewidth=2.4, markersize=7,
        label="POST service time")

# Ceiling markers
ax.axvline(pre_ceil[1],  color="#d62728", linestyle=":", linewidth=1.3, alpha=0.6)
ax.axvline(post_ceil[1], color="#1f77b4", linestyle="--", linewidth=1.6, alpha=0.8)
ax.text(pre_ceil[1], 800,
        f"PRE ceiling\n{pre_ceil[1]:.2f} Gbps\n({pre_ceil[2]//1000}k RPS)",
        ha="left", va="top", fontsize=9, color="#a83232",
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#d62728"))
ax.text(post_ceil[1], 0.5,
        f"POST ceiling\n{post_ceil[1]:.2f} Gbps\n({post_ceil[2]//1000}k RPS)",
        ha="left", va="bottom", fontsize=10, color="#0d4a8b", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#1f77b4"))

# Annotate selected POST points
for i in range(len(post["target"])):
    rps = int(post["target"][i])
    if rps in (10000, 50000, 90000, 100000, 110000, 130000):
        ax.annotate(f"{rps//1000}k", (post["gbps"][i], post["corr_avg"][i]),
                    xytext=(5, 5), textcoords="offset points",
                    fontsize=9, color="#0d4a8b")

ax.set_yscale("log")
ax.set_xlabel("Throughput (Gbps, wall-clock)", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title(f"Throughput–latency curve  (msg {MSG_BYTES} B, Go client + epoll gateway)",
             fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=9, ncol=2)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_bps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 2: latency percentiles vs offered load (POST + PRE overlay)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 6.0))

ax.plot(pre["target"],  pre["corr_p50"],  "o--", color="#d62728", alpha=0.55, linewidth=1.4, label="PRE  e2e P50")
ax.plot(pre["target"],  pre["corr_p99"],  "^--", color="#d62728", alpha=0.35, linewidth=1.4, label="PRE  e2e P99")

ax.plot(post["target"], post["raw_p50"],  "o-",  color="#2ca02c",                label="POST raw P50")
ax.plot(post["target"], post["raw_p99"],  "^-",  color="#2ca02c", alpha=0.5,     label="POST raw P99")
ax.plot(post["target"], post["corr_p50"], "o-",  color="#1f77b4",                label="POST e2e P50")
ax.plot(post["target"], post["corr_p99"], "^-",  color="#1f77b4", alpha=0.5,     label="POST e2e P99")

ax.axvline(pre_ceil[2],  color="#d62728", linestyle=":",  linewidth=1.2, alpha=0.6)
ax.axvline(post_ceil[2], color="#1f77b4", linestyle="--", linewidth=1.5, alpha=0.8)
ax.text(pre_ceil[2],  ax.get_ylim()[1] * 0.5, f" PRE  {pre_ceil[2]//1000}k",
        ha="left", va="top", fontsize=9, color="#a83232")
ax.text(post_ceil[2], ax.get_ylim()[1] * 0.5, f" POST {post_ceil[2]//1000}k",
        ha="left", va="top", fontsize=10, color="#0d4a8b", fontweight="bold")

# Highlight POST overdrive zone (offered > sustainable, but no collapse)
overdrive_mask = post["target"] > post_ceil[2] * 1.0
od = post["target"][overdrive_mask]
if len(od):
    ax.axvspan(od.min(), post["target"].max(), color="#fff4b3", alpha=0.5, zorder=0)
    ax.text(od.min(), ax.get_ylim()[1] * 0.05,
            "  POST overdrive (graceful)", color="#7a5b00", fontsize=9, fontweight="bold")

# PRE collapse zone
pcc = pre["target"][pre_collapse]
if len(pcc):
    ax.axvspan(pcc.min(), pre["target"].max(), color="#ffd6d6", alpha=0.6, zorder=0)
    ax.text(pcc.min(), ax.get_ylim()[1] * 0.005,
            "  PRE collapse", color="#990000", fontsize=9, fontweight="bold")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Latency (ms, log scale)", fontsize=11)
ax.set_title("Latency percentiles vs offered load  (PRE vs POST)", fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(ncol=3, fontsize=9, loc="upper left")
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "latency_vs_rps.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 3: dual-axis — wall-clock RPS (left) and Gbps (left) vs target,
#           plus P99 latency (right). Direct visualization of "graceful
#           degradation" vs "collapse" — wall_rps tracks target up to ceiling
#           in POST, but DROPS in PRE collapse.
# ---------------------------------------------------------------------------
fig, ax1 = plt.subplots(figsize=(11, 6.0))

# Identity (perfect tracking) line — both axes are in "k" units, so y=x
ident_max_k = max(pre["target"].max(), post["target"].max()) / 1000
ident_x = np.linspace(0, ident_max_k, 200)
ax1.plot(ident_x, ident_x, ":", color="#888888", linewidth=1.2,
         label="ideal (wall = offered)")

ax1.plot(pre["target"]  / 1000, pre["wall"]  / 1000, "s--", color="#d62728",
         linewidth=1.8, markersize=5, alpha=0.75, label="PRE  wall RPS (k)")
ax1.plot(post["target"] / 1000, post["wall"] / 1000, "o-",  color="#1f77b4",
         linewidth=2.4, markersize=7,                 label="POST wall RPS (k)")

ax1.set_xlabel("Target RPS (k)", fontsize=11)
ax1.set_ylabel("Achieved wall-clock RPS (k)", fontsize=11)
ax1.grid(True, alpha=0.3)

ax2 = ax1.twinx()
ax2.plot(pre["target"]  / 1000, pre["corr_p99"],  "^--", color="#ff9896",
         linewidth=1.4, markersize=4, alpha=0.7, label="PRE  e2e P99 (ms)")
ax2.plot(post["target"] / 1000, post["corr_p99"], "^-",  color="#7eb6e8",
         linewidth=1.8, markersize=5,            label="POST e2e P99 (ms)")
ax2.set_ylabel("End-to-end P99 latency (ms, log scale)", fontsize=11)
ax2.set_yscale("log")

# PRE collapse annotation
if pre_collapse.any():
    ci = int(np.where(pre_collapse)[0][0])
    ax1.annotate("PRE collapse:\n70k offered → 43k actual",
                 xy=(pre["target"][ci] / 1000, pre["wall"][ci] / 1000),
                 xytext=(pre["target"][ci] / 1000 - 30, pre["wall"][ci] / 1000 + 25),
                 fontsize=9, color="#990000", fontweight="bold",
                 arrowprops=dict(arrowstyle="->", color="#990000"))

# POST graceful annotation
last = len(post["target"]) - 1
ax1.annotate(f"POST graceful cap:\n{int(post['target'][last])//1000}k offered → "
             f"{int(post['wall'][last])//1000}k actual",
             xy=(post["target"][last] / 1000, post["wall"][last] / 1000),
             xytext=(post["target"][last] / 1000 - 60, post["wall"][last] / 1000 - 50),
             fontsize=9, color="#0d4a8b", fontweight="bold",
             arrowprops=dict(arrowstyle="->", color="#0d4a8b"))

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
ax1.set_title("Achieved RPS vs offered load  (PRE collapse vs POST graceful saturation)",
              fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "throughput_vs_latency.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 4: latency decomposition — service time vs schedule wait (POST only)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(11, 5.5))

ax.fill_between(post["target"], 0, post["raw_avg"], color="#2ca02c", alpha=0.55,
                label="Service time   (send_ts → recv_done)")
ax.fill_between(post["target"], post["raw_avg"], post["corr_avg"],
                color="#d62728", alpha=0.40,
                label="Schedule wait  (sched_ts → send_ts)")
ax.plot(post["target"], post["corr_avg"], "o-", color="#d62728", linewidth=1.5)
ax.plot(post["target"], post["raw_avg"],  "o-", color="#2ca02c", linewidth=1.5)

ax.axvline(post_ceil[2], color="#444444", linestyle="--", linewidth=1.3, alpha=0.7)
ax.text(post_ceil[2], ax.get_ylim()[1] * 0.5,
        f"  ceiling {post_ceil[2]//1000}k",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold")

ax.set_yscale("log")
ax.set_xlabel("Target RPS", fontsize=11)
ax.set_ylabel("Average latency (ms, log scale)", fontsize=11)
ax.set_title("End-to-end latency = service time + schedule wait  (POST)", fontsize=12)
ax.grid(True, which="both", alpha=0.3)
ax.legend(loc="upper left", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "queueing_gap.png"), dpi=140)
plt.close(fig)


# ---------------------------------------------------------------------------
# Stdout summary
# ---------------------------------------------------------------------------
def fmt(d, ceil):
    i = ceil[0]
    return (f"{int(d['target'][i])//1000:>3}k RPS  "
            f"= {ceil[1]:.3f} Gbps "
            f"(P99 corr {d['corr_p99'][i]:.2f} ms)")

print("Msg size                : 8192 bytes")
print("PRE  sustainable ceiling: " + fmt(pre,  pre_ceil))
print(f"  next step ({pre_ceil[4]} RPS): e2e avg jumps {pre_ceil[3]:.1f}× "
      f"({pre['corr_avg'][pre_ceil[0]]:.2f} → {pre['corr_avg'][pre_ceil[0]+1]:.2f} ms)")
print("POST sustainable ceiling: " + fmt(post, post_ceil))
print(f"  next step ({post_ceil[4]} RPS): e2e avg jumps {post_ceil[3]:.1f}× "
      f"({post['corr_avg'][post_ceil[0]]:.2f} → {post['corr_avg'][post_ceil[0]+1]:.2f} ms)")

improvement_rps  = post_ceil[2] / pre_ceil[2]
improvement_gbps = post_ceil[1] / pre_ceil[1]
print(f"Improvement              : RPS {improvement_rps:.2f}×, "
      f"Gbps {improvement_gbps:.2f}×")

# Hard ceiling: maximum sustained wall RPS
post_peak_idx = int(np.argmax(post["wall"]))
print(f"POST hard ceiling (max wall RPS): "
      f"{int(post['wall'][post_peak_idx])} RPS  "
      f"= {post['gbps'][post_peak_idx]:.3f} Gbps "
      f"({int(post['target'][post_peak_idx])} offered)")

# Collapse check
if pre_collapse.any():
    ci = int(np.where(pre_collapse)[0][0])
    print(f"PRE collapse             : {int(pre['target'][ci])} offered → "
          f"only {int(pre['wall'][ci])} wall RPS, e2e avg {pre['corr_avg'][ci]:.0f} ms")
if post_collapse.any():
    print(f"POST collapse            : observed at offered RPS:")
    for ci in np.where(post_collapse)[0]:
        print(f"  {int(post['target'][ci])} → {int(post['wall'][ci])} RPS")
else:
    print("POST collapse            : NONE (graceful saturation up to "
          f"{int(post['target'].max())} offered RPS)")

print("\nSaved figures:")
for fname in ("throughput_bps.png", "latency_vs_rps.png",
              "throughput_vs_latency.png", "queueing_gap.png"):
    print(f"  {os.path.join(OUT_DIR, fname)}")
