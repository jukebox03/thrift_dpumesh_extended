"""
Lab-meeting figures (2026-06-12), one message per figure, minimal text.

All numbers are measured values from bench/scale_log.md (2-pod chain, 8 KB,
0 fail unless stated). Labels use generic systems terms only — no internal
knob/enum names. Output: bench/labmeeting/fig*.png
"""
import os
import numpy as np
import matplotlib.pyplot as plt

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "labmeeting")
os.makedirs(OUT, exist_ok=True)

GREY   = "#9e9e9e"
GREEN  = "#43a047"
DGREEN = "#1b5e20"
ORANGE = "#ef6c00"
RED    = "#d84315"
BLUE   = "#1e88e5"

plt.rcParams.update({"font.size": 12, "axes.spines.top": False,
                     "axes.spines.right": False})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", name)


# ── Fig 1 (slides 1 & 10): throughput ladder ────────────────────────────────
steps = ["Last week",
         "Async client\n+ no per-req ACK",
         "Use more\nDPA cores",
         "Server RX\npolling",
         "Batched\ncompletions",
         "App overhead\nout of bench"]
rps = [104, 130, 160, 200, 235, 240]

fig, ax = plt.subplots(figsize=(9.5, 5))
colors = [GREY] + [plt.cm.YlGn(0.30 + 0.55 * i / 4) for i in range(5)]
ax.bar(range(6), rps, color=colors, edgecolor=DGREEN, width=0.62, zorder=3)
for i, v in enumerate(rps):
    ax.text(i, v + 4, f"{v}K", ha="center", fontweight="bold", color=DGREEN)
    if i:
        ax.text(i, v - 14, f"+{(rps[i]-rps[i-1])/rps[i-1]*100:.0f}%",
                ha="center", fontsize=10, color="white", fontweight="bold")
ax.set_xticks(range(6), steps, fontsize=10.5)
ax.set_ylabel("Sustainable throughput (K req/s)")
ax.set_ylim(0, 270)
ax.set_title("2-pod chain: 104K -> 240K req/s (+130%), all runs 0 failures",
             fontsize=13)
save(fig, "fig1_ladder.png")

# ── Fig 2 (slide 2): where a request spends its time at the old ceiling ─────
fig, ax = plt.subplots(figsize=(8.5, 2.6))
ax.barh([0], [97.2], color=ORANGE, height=0.5, label="Host side: 97.2%")
ax.barh([0], [2.8], left=[97.2], color=BLUE, height=0.5, label="DPU side: 2.8%")
ax.text(48.6, 0, "Host side  97.2%", ha="center", va="center",
        color="white", fontweight="bold", fontsize=13)
ax.annotate("DPU 2.8%", xy=(98.6, 0.25), xytext=(88, 0.62),
            arrowprops=dict(arrowstyle="->", color=BLUE), color=BLUE,
            fontweight="bold", fontsize=12)
ax.set_xlim(0, 100); ax.set_ylim(-0.6, 0.9)
ax.set_yticks([]); ax.set_xticks([])
for s in ax.spines.values():
    s.set_visible(False)
ax.set_title("Round-trip time breakdown at the 104K ceiling\n"
             "-> the DPU was never the bottleneck", fontsize=13)
save(fig, "fig2_rtt_breakdown.png")

# ── Fig 3 (slide 3): blocking vs polling client, 1 host core ────────────────
fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.8))
panels = [("Throughput\n(K req/s)", [104, 125], "{:.0f}"),
          ("Median latency\n(ms)", [14.2, 1.9], "{:.1f}"),
          ("Client threads\n(count)", [1300, 2], "{:.0f}")]
for ax, (title, vals, fmt) in zip(axes, panels):
    ax.bar([0, 1], vals, color=[GREY, GREEN], width=0.55, zorder=3)
    for i, v in enumerate(vals):
        ax.text(i, v * 1.03, fmt.format(v), ha="center", fontweight="bold")
    ax.set_xticks([0, 1], ["blocking", "polling"])
    ax.set_title(title, fontsize=12)
    ax.set_ylim(0, max(vals) * 1.25)
axes[2].set_yscale("log"); axes[2].set_ylim(1, 4000)
fig.suptitle("Client API: blocking wait vs polling harvest  (1 host core)",
             fontsize=13, y=1.02)
save(fig, "fig3_sync_async.png")

# ── Fig 4 (slide 4): DPA cores — gain then saturation ───────────────────────
fig, axes = plt.subplots(1, 2, figsize=(9.5, 4))
ax = axes[0]
ax.bar([0, 1], [130, 160], color=[GREY, GREEN], width=0.5, zorder=3)
for i, v in enumerate([130, 160]):
    ax.text(i, v + 4, f"{v}K", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["2 DPA cores", "4 DPA cores"])
ax.set_ylabel("K req/s"); ax.set_ylim(0, 200)
ax.set_title("Spreading one pod over\nmore DPA cores: +23%", fontsize=12)

ax = axes[1]
ax.bar([0, 1], [0.81, 0.813], color=[GREEN, GREY], width=0.5, zorder=3)
for i, v in enumerate([0.81, 0.813]):
    ax.text(i, v + 0.03, f"{v:.2f}M", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["4 DPA cores", "8 DPA cores"])
ax.set_ylabel("DMA operations / s (millions)"); ax.set_ylim(0, 1.05)
ax.set_title("Beyond 4 cores: no gain\n(measured DMA op rate, 200K config)", fontsize=12)
save(fig, "fig4_dpa_cores.png")

# ── Fig 5 (slide 5): server wake-up removal ─────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(9.5, 4))
ax = axes[0]
ax.bar([0, 1], [69, 2], color=[RED, GREEN], width=0.5, zorder=3)
for i, v in enumerate([69, 2]):
    ax.text(i, v + 2, f"{v}%", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["wake-up\nper request", "polling\nworkers"])
ax.set_ylabel("Server kernel time (%sys)"); ax.set_ylim(0, 85)
ax.set_title("Server CPU in the kernel\n(futex wake-ups)", fontsize=12)

ax = axes[1]
ax.bar([0, 1], [160, 200], color=[GREY, GREEN], width=0.5, zorder=3)
for i, v in enumerate([160, 200]):
    ax.text(i, v + 5, f"{v}K", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["before", "after"])
ax.set_ylabel("K req/s"); ax.set_ylim(0, 240)
ax.set_title("Throughput: +25%", fontsize=12)
fig.suptitle("Removing the per-request server wake-up", fontsize=13, y=1.02)
save(fig, "fig5_server_wakeup.png")

# ── Fig 6 (slide 6): one RX thread saturated -> batch notifications ─────────
fig, axes = plt.subplots(1, 2, figsize=(10, 4))
ax = axes[0]
names = ["server pod\nRX dispatch thread", "server pod\nworkers (avg of 3)",
         "client pod\nRX dispatch thread", "client pod\nworkers (avg of 4)"]
vals = [96.4, 23, 77.2, 38]
colors = [RED, GREY, ORANGE, GREY]
ax.barh([0, 1, 2.5, 3.5], vals, color=colors, height=0.55, zorder=3)
for y, v in zip([0, 1, 2.5, 3.5], vals):
    ax.text(v + 1.5, y, f"{v:g}%", va="center", fontweight="bold")
ax.set_yticks([0, 1, 2.5, 3.5], names, fontsize=10)
ax.invert_yaxis()
ax.set_xlim(0, 112); ax.set_xlabel("per-thread CPU (%)")
ax.set_title("Every pod has ONE RX dispatch thread\n— it saturates first",
             fontsize=12)

ax = axes[1]
ax.bar([0, 1], [200, 235], color=[GREY, GREEN], width=0.5, zorder=3)
for i, v in enumerate([200, 235]):
    ax.text(i, v + 5, f"{v}K", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["1 notification\nper response", "16 responses\nper notification"])
ax.set_ylabel("K req/s"); ax.set_ylim(0, 280)
ax.set_title("Batching completion\nnotifications: +18%", fontsize=12)
save(fig, "fig6_rx_thread.png")

# ── Fig 7 (slide 7): HW limit vs current — SAME 4 DPA cores, 8 KB ───────────
# Raw sources (all measured, /home/jukebox/test_dma/):
#   DMA engine alone, 4 EU: 1,804,363 (pure_dma_results_20260529_194834.csv)
#                           1,710,039 (pure_dma_results_20260602_115553.csv)
#   + host desc feed + 1 DPU drain thread (M0, 4 EU): 1,187,147
#   + per-op notify to host (M2, 4 EU):               1,006,224
#                                       (bench_results_20260602_021256.csv)
#   full mesh closed loop (chain, DPA=4 K=2): 240K RTT/s x 4 ops = 0.96M
#                                       (scale_log.md final verified config)
fig, ax = plt.subplots(figsize=(9.8, 4.4))
names = ["DMA engine alone\n(measured HW limit)",
         "+ host descriptor feed,\n   one DPU drain thread",
         "+ per-operation\n   notification to host",
         "Full mesh, closed loop\n(current system)"]
vals = [1.80, 1.19, 1.01, 0.96]
errs = [[0.09, 0, 0, 0], [0, 0, 0, 0]]   # engine run-to-run spread 1.71-1.80M
labels = ["1.71-1.80M", "1.19M", "1.01M", "0.96M"]
colors = [BLUE, GREY, GREY, GREEN]
ax.barh(range(4), vals, xerr=errs, color=colors, height=0.55, zorder=3,
        error_kw=dict(ecolor="#37474f", lw=1.5, capsize=4))
for i, (v, t) in enumerate(zip(vals, labels)):
    ax.text(v + 0.04, i, t, va="center", fontweight="bold")
ax.text(0.48, 3, "53% of HW", color="white", fontweight="bold",
        va="center", ha="center", fontsize=11)
ax.set_yticks(range(4), names, fontsize=11)
ax.invert_yaxis()
ax.set_xlabel("DMA operations / second (millions) — same 4 DPA cores, 8 KB")
ax.set_xlim(0, 2.15)
ax.set_title("Hardware limit vs current: what each software stage costs",
             fontsize=13)
save(fig, "fig7_ceiling.png")

# ── Fig 8 (slide 8): host-CPU lightening (lock time only; copies/memory are
#    single numbers better stated as slide text: -2 copies/req, -32 MB/pod) ──
fig, ax = plt.subplots(figsize=(5.5, 4))
ax.bar([0, 1], [6.5, 3.5], color=[GREY, GREEN], width=0.5, zorder=3)
for i, v in enumerate([6.5, 3.5]):
    ax.text(i, v + 0.15, f"{v:.1f}%", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["mutex-based\nqueues", "lock-free\nqueues"])
ax.set_ylabel("Host CPU spent in locks (%)")
ax.set_ylim(0, 8.5)
ax.set_title("Lock overhead nearly halved\n(throughput unchanged — by design)",
             fontsize=12.5)
save(fig, "fig8_lightening.png")

print("done ->", OUT)
