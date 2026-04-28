#!/usr/bin/env python3
"""
dpumesh throughput sweep analysis — service-side path with end-node ACK
guarantee.

Single dataset measured after the following library-side fixes are in
place (transport-layer only; no Thrift server / service-binary changes):

  1. TDpumeshTransport: read() transparently fetches the next dpumesh
     descriptor after flush(), so a single TConnectedClient runner thread
     amortises its pthread spawn cost across many requests.
  2. TDpumeshTransport: write() lazy-allocates the TX slot and writes
     directly into it — no write_buf_ vector, no flush memcpy.
  3. TDpumeshTransport: flush() registers a pending entry, attaches the
     TX slot BEFORE enqueue (TX_ACK cannot precede enqueue, so attach
     before enqueue eliminates the TX_ACK-arrives-first race), then
     issues release_async so the TX_ACK handler frees the slot.
  4. process_forward_entry (DPU): reverse-DMA fc_header carries
     target_pod->rx_consumer_tail, not src_pod's — fixes a forwarding
     flow-control corruption that bypassed echo (where src==target).
  5. process_forward_entry (DPU): TX_ACK on AGAIN is deferred to a
     side queue and retried by the main loop after pe_progress, instead
     of inline retry-and-drop. Dropping a TX_ACK parks the host's
     pending entry at state=-2 until the 2-second register_pending
     reclaim, which is the long-tail-latency cliff observed previously.
     The DPU is the sole authority that can release a host TX slot via
     the pending pathway, so the queue treats TX_ACK as a hard
     guarantee.

Output PNGs:
  - throughput_bps.png        — hockey stick (Gbps vs avg latency)
  - latency_vs_rps.png        — percentile sweep
  - throughput_vs_latency.png — wall RPS tracking + e2e P99
  - queueing_gap.png          — service time vs schedule wait

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
# Throughput (gbps) is wall-clock based.
DATA = [
    ( 5000,  4545.3, 0.298,   0.69,   0.68,    1.17,    1.48,    5.61,   0.54,  0.54,  0.86,  1.12,   5.38),
    (10000,  9090.4, 0.596,   0.47,   0.43,    0.89,    1.13,    5.80,   0.37,  0.33,  0.72,  0.90,   5.08),
    (15000, 13636.0, 0.894,   0.37,   0.31,    0.76,    0.98,    4.03,   0.30,  0.22,  0.65,  0.84,   3.33),
    (20000, 18181.0, 1.192,   0.31,   0.25,    0.66,    0.88,    4.01,   0.25,  0.19,  0.58,  0.78,   3.99),
    (25000, 22724.8, 1.488,   0.29,   0.25,    0.55,    0.78,    5.67,   0.24,  0.21,  0.46,  0.69,   3.70),
    (30000, 27271.7, 1.786,   0.26,   0.22,    0.47,    0.75,    6.31,   0.23,  0.20,  0.43,  0.70,   4.19),
    (35000, 31816.8, 2.083,   0.30,   0.26,    0.50,    0.84,   10.43,   0.27,  0.23,  0.46,  0.79,   7.80),
    (40000, 36361.0, 2.381,   0.33,   0.29,    0.57,    0.88,    5.79,   0.31,  0.27,  0.54,  0.84,   5.78),
    (45000, 39224.1, 2.568,  38.73,   3.18,  272.72,  447.60,  570.64,  11.91,  3.15, 45.13, 48.71,  53.92),
    (50000, 38254.9, 2.505, 304.49, 177.26,  980.51, 1369.57, 1593.89,  32.30, 41.65, 53.87, 59.50,  64.14),
    (55000, 39671.3, 2.597, 559.60, 455.10, 1481.16, 1765.53, 1860.85,  43.74, 47.29, 58.97, 61.97,  66.82),
    (60000, 40784.6, 2.670, 870.90, 754.01, 2018.38, 2205.11, 2257.71,  49.18, 51.20, 64.52, 66.80,  71.26),
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
# Figure 1: hockey stick — true throughput (Gbps) vs avg latency (log)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.8))

ax.plot(d["gbps"], d["corr_avg"], "s-", color="#1f77b4",
        linewidth=2.2, markersize=7, label="end-to-end")
ax.plot(d["gbps"], d["raw_avg"],  "o-", color="#2ca02c",
        linewidth=2.0, markersize=6, label="service time")

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
ax.set_title(f"Throughput–latency curve  (forward path, msg {MSG_BYTES} B)",
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
        linewidth=1.5, label="raw P50 (service)")
ax.plot(d["target"], d["raw_p99"],  "^--", color="#2ca02c", alpha=0.7,
        linewidth=1.5, label="raw P99 (service)")
ax.plot(d["target"], d["corr_p50"], "o-",  color="#1f77b4",
        linewidth=2.0, label="e2e P50")
ax.plot(d["target"], d["corr_p99"], "^-",  color="#d62728",
        linewidth=2.0, label="e2e P99")

ax.axvline(knee_rps, color="#444444", linestyle="--", linewidth=1.4, alpha=0.7)
ax.text(knee_rps, ax.get_ylim()[1] * 0.5, f"  knee {knee_rps//1000}k",
        ha="left", va="top", fontsize=10, color="#444444", fontweight="bold")

# Overdrive zone
overdrive = d["target"] > knee_rps
od = d["target"][overdrive]
if len(od):
    ax.axvspan(od.min(), d["target"].max(), color="#fff4b3", alpha=0.4, zorder=0)
    ax.text(od.min(), ax.get_ylim()[1] * 0.05,
            "  overdrive (graceful)", color="#7a5b00", fontsize=9, fontweight="bold")

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
# Figure 3: dual-axis — wall-clock RPS (left) vs target,
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

# Plateau annotation
ax1.annotate(f"hard ceiling ≈ {int(d['wall'][peak_idx])//1000}k wall RPS\n"
             f"({d['gbps'][peak_idx]:.2f} Gbps) — flat for 45k–60k offered",
             xy=(55, d["wall"][peak_idx] / 1000),
             xytext=(20, d["wall"][peak_idx] / 1000 + 8),
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
# Figure 4: latency decomposition — service time vs schedule wait
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 5.4))

ax.fill_between(d["target"], 0, d["raw_avg"], color="#2ca02c", alpha=0.55,
                label="Service time   (send_ts → recv_done)")
ax.fill_between(d["target"], d["raw_avg"], d["corr_avg"],
                color="#d62728", alpha=0.40,
                label="Schedule wait  (sched_ts → send_ts)")
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
          f"e2e avg jumps {knee_jump:.1f}× "
          f"({d['corr_avg'][knee_idx]:.2f} → {d['corr_avg'][knee_idx+1]:.2f} ms)")
print(f"Hard ceiling          : {int(d['wall'][peak_idx])} wall RPS  "
      f"= {d['gbps'][peak_idx]:.3f} Gbps "
      f"({int(d['target'][peak_idx])} offered)")
print(f"Overdrive envelope    : up to {int(d['target'].max())} offered RPS, "
      f"no collapse (graceful saturation)")

print("\nSaved figures:")
for fname in ("throughput_bps.png", "latency_vs_rps.png",
              "throughput_vs_latency.png", "queueing_gap.png"):
    print(f"  {os.path.join(OUT_DIR, fname)}")
