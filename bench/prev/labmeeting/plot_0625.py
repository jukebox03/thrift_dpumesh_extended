"""
Lab-meeting figures (2026-06-25). Three parts:
  Part 1 — DPM API design (socket/epoll façade)
  Part 2 — Notification-based change (busy-poll -> event-driven)
  Part 3 — Step-by-step current implementation (data path, floor, ceiling)

All numbers are MEASURED values from bench/scale_log.md (2-pod chain, 8 KB,
0 fail unless stated; FIX2 prod build, EVENT_LOOP=1, DPA=4/K=2). Labels use
generic systems terms only — no internal knob/enum names. Output: 0625/fig*.png
"""
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "0625")
os.makedirs(OUT, exist_ok=True)

GREY   = "#9e9e9e"
LGREY  = "#cfcfcf"
GREEN  = "#43a047"
DGREEN = "#1b5e20"
ORANGE = "#ef6c00"
RED    = "#d84315"
BLUE   = "#1e88e5"
DBLUE  = "#0d47a1"
PURPLE = "#6a1b9a"

plt.rcParams.update({"font.size": 12, "axes.spines.top": False,
                     "axes.spines.right": False})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("wrote", name)


# ===========================================================================
# PART 1 — DPM API design
# ===========================================================================

# ── Fig 1: BSD socket -> _dpm twin mapping (table-as-figure) ────────────────
fig, ax = plt.subplots(figsize=(10, 6))
ax.axis("off")
ax.set_ylim(0, 1); ax.set_xlim(0, 1)
rows = [
    ("socket() + bind() + listen()", "socket_dpm()", "folded into one"),
    ("accept()",                     "accept_dpm()", "holds the full request body"),
    ("connect()",                    "connect_dpm()", "no round-trip — binds target pod"),
    ("read()",                       "read_dpm()",  "atomic message, no partial loop"),
    ("write()",                      "write_dpm()", "buffers; read/close auto-ships"),
    ("sendfile()",                   "sendfile_dpm()", "appends file bytes (<=8 KB)"),
    ("close()",                      "close_dpm()", "ships reply + frees the slot"),
    ("epoll_create/_ctl/_wait()",    "UNCHANGED (native epoll)", "register event_fd_dpm(s)"),
]
fig.suptitle("Port a non-blocking epoll server/client to the DPU data plane:\n"
             "swap each BSD call for its $\\_dpm$ twin — epoll stays native",
             fontsize=13.5, y=1.02)
ax.text(0.17, 0.95, "BSD socket / epoll", ha="center", fontsize=12.5, fontweight="bold", color=GREY)
ax.text(0.55, 0.95, "DPUmesh (_dpm twin)", ha="center", fontsize=12.5, fontweight="bold", color=DGREEN)
ax.text(0.88, 0.95, "what changes", ha="center", fontsize=12.5, fontweight="bold", color=BLUE)
ax.axhline(0.905, xmin=0.02, xmax=0.98, color="#ddd", lw=1)
y = 0.82
for bsd, dpm, note in rows:
    ax.text(0.17, y, bsd, ha="center", va="center", fontsize=10.5, family="monospace",
            color="#555")
    ax.annotate("", xy=(0.355, y), xytext=(0.295, y),
                arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.4))
    ax.text(0.55, y, dpm, ha="center", va="center", fontsize=10.5, family="monospace",
            fontweight="bold", color=DGREEN)
    ax.text(0.88, y, note, ha="center", va="center", fontsize=9.3, color="#444",
            style="italic")
    y -= 0.107
save(fig, "fig01_socket_mapping.png")

# ── Fig 2: dpmconn_t is a request/response PAIR, not a connection ───────────
fig, (axl, axr) = plt.subplots(1, 2, figsize=(11, 3.6),
                               gridspec_kw={"width_ratios": [1, 1]})
# left: a TCP connection (persistent stream)
axl.axis("off")
axl.set_xlim(0, 10); axl.set_ylim(0, 10)
axl.add_patch(FancyBboxPatch((0.5, 3.6), 9, 3.0, boxstyle="round,pad=0.1",
              fc="#eeeeee", ec=GREY, lw=1.5))
axl.text(5, 7.6, "BSD socket: a CONNECTION", ha="center", fontweight="bold", color="#555")
axl.text(5, 5.9, "3-way handshake -> persistent\nbidirectional byte stream -> in-order",
         ha="center", fontsize=10.5, color="#444")
axl.annotate("", xy=(9.2, 4.4), xytext=(0.8, 4.4),
             arrowprops=dict(arrowstyle="<->", color=GREY, lw=2))
axl.text(5, 2.2, "reuse the same channel; ordering guaranteed",
         ha="center", fontsize=9.5, style="italic", color="#777")
# right: DPM pairs (single-shot, out of order)
axr.axis("off")
axr.set_xlim(0, 10); axr.set_ylim(0, 10)
for i, (yy, col, lbl) in enumerate([(7.6, GREEN, "req#7 -> resp#7"),
                                    (5.4, BLUE,  "req#9 -> resp#9"),
                                    (3.2, ORANGE,"req#8 -> resp#8")]):
    axr.add_patch(FancyBboxPatch((1.2, yy-0.55), 7.6, 1.1, boxstyle="round,pad=0.08",
                  fc="white", ec=col, lw=2))
    axr.annotate("", xy=(8.4, yy), xytext=(1.6, yy),
                 arrowprops=dict(arrowstyle="->", color=col, lw=2))
    axr.text(5, yy, lbl, ha="center", va="center", fontweight="bold", color=col, fontsize=10.5)
axr.text(5, 9.3, "DPUmesh: a request/response PAIR", ha="center", fontweight="bold", color=DGREEN)
axr.text(5, 1.4, "single-shot, no handshake, matched by req_id\n-> many in flight, arrive OUT OF ORDER",
         ha="center", fontsize=9.5, style="italic", color="#777")
fig.suptitle("The key idea: a $dpmconn\\_t$ is NOT a connection — it is one RPC pair",
             fontsize=13.5, y=1.02)
save(fig, "fig02_pair_not_connection.png")

# ── Fig 3: façade is zero-overhead — same 240K ceiling as the raw API ───────
targets  = [30, 100, 200, 240]
achieved = [29.838, 99.451, 198.925, 238.699]
fig, ax = plt.subplots(figsize=(8.8, 4.6))
x = np.arange(len(targets))
ax.bar(x - 0.19, targets, width=0.36, color=LGREY, edgecolor=GREY, label="target", zorder=3)
ax.bar(x + 0.19, achieved, width=0.36, color=GREEN, edgecolor=DGREEN,
       label="achieved (full façade stack)", zorder=3)
for i, v in enumerate(achieved):
    ax.text(i + 0.19, v + 4, f"{v:.0f}K", ha="center", fontweight="bold", color=DGREEN, fontsize=10)
ax.axhline(240, ls="--", color=GREY, lw=1)
ax.text(3.05, 246, "raw-API ceiling", color=GREY, fontsize=9.5, ha="right")
ax.set_xticks(x, [f"{t}K" for t in targets])
ax.set_xlabel("target load (K req/s)")
ax.set_ylabel("achieved throughput (K req/s)")
ax.set_ylim(0, 270)
ax.legend(loc="upper left", frameon=False, fontsize=10)
ax.set_title("Client + server written ENTIRELY on the $\\_dpm$ façade\n"
             "sustain the SAME ~240K, 0 failures — the abstraction is free", fontsize=13)
save(fig, "fig03_facade_zero_overhead.png")

# ── Fig 4: façade latency vs load + idle CPU 1.2% callout ───────────────────
load = [30, 100, 200, 240]
p50  = [170, 200, 255, 541]
p99  = [501, 493, 4789, 4577]
fig, ax = plt.subplots(figsize=(8.8, 4.6))
x = np.arange(len(load))
ax.bar(x - 0.19, p50, width=0.36, color=GREEN, edgecolor=DGREEN, label="p50", zorder=3)
ax.bar(x + 0.19, p99, width=0.36, color=LGREY, edgecolor=GREY, label="p99", zorder=3)
for i, v in enumerate(p50):
    ax.text(i - 0.19, v * 1.15, f"{v}", ha="center", fontweight="bold", fontsize=9.5, color=DGREEN)
ax.set_yscale("log")
ax.set_xticks(x, [f"{t}K" for t in load])
ax.set_xlabel("load (req/s)")
ax.set_ylabel("RTT latency (µs, log)")
ax.set_ylim(80, 9000)
ax.legend(loc="upper left", frameon=False, fontsize=10)
ax.set_title("Façade latency: 170–255 µs p50 at low–mid load\n"
             "idle CPU 1.2% — notification-driven, not busy-poll", fontsize=13)
save(fig, "fig04_facade_latency_idle.png")


# ===========================================================================
# PART 2 — Notification-based change
# ===========================================================================

# ── Fig 5: idle CPU — busy-poll vs notification ────────────────────────────
fig, ax = plt.subplots(figsize=(8.2, 4.4))
comps = ["DPU control thread\n(ARM event loop)", "Host RX-progress\nthread"]
busy  = [100, 3]
event = [2, 0]
y = np.arange(len(comps))
ax.barh(y + 0.18, busy,  height=0.34, color=RED,   label="busy-poll (before)", zorder=3)
ax.barh(y - 0.18, event, height=0.34, color=GREEN, label="notification (after)", zorder=3)
for yy, v in zip(y + 0.18, busy):
    ax.text(v + 1.5, yy, f"{v}%", va="center", fontweight="bold", color=RED)
for yy, v in zip(y - 0.18, event):
    ax.text(v + 1.5, yy, f"{v}%", va="center", fontweight="bold", color=DGREEN)
ax.set_yticks(y, comps, fontsize=10.5)
ax.invert_yaxis()
ax.set_xlim(0, 112)
ax.set_xlabel("idle CPU (%)")
ax.legend(loc="lower right", frameon=False, fontsize=10)
ax.set_title("Idle CPU: a full DPU core (100%) -> ~2% by sleeping on a notification fd",
             fontsize=12.5)
save(fig, "fig05_idle_cpu.png")

# ── Fig 6: HOST_EPOLL tail-latency win (p99) ───────────────────────────────
loads = ["30K", "100K", "200K"]
spin  = [2.17, 2.58, 46.46]
epoll = [2.98, 2.61, 2.83]
fig, ax = plt.subplots(figsize=(8.2, 4.4))
x = np.arange(len(loads))
ax.bar(x - 0.19, spin,  width=0.36, color=RED,   label="busy-poll (before)", zorder=3)
ax.bar(x + 0.19, epoll, width=0.36, color=GREEN, label="notification (after)", zorder=3)
for i, v in enumerate(spin):
    ax.text(i - 0.19, v * 1.05, f"{v:.1f}", ha="center", fontweight="bold", fontsize=9.5, color=RED)
for i, v in enumerate(epoll):
    ax.text(i + 0.19, v * 1.05, f"{v:.1f}", ha="center", fontweight="bold", fontsize=9.5, color=DGREEN)
ax.annotate("16×\nlower", xy=(2.19, 3.5), xytext=(2.42, 18),
            arrowprops=dict(arrowstyle="->", color=DGREEN, lw=1.6),
            color=DGREEN, fontweight="bold", ha="center", fontsize=11)
ax.set_yscale("log")
ax.set_xticks(x, loads)
ax.set_xlabel("load (req/s)")
ax.set_ylabel("p99 latency (ms, log)")
ax.set_ylim(1.5, 80)
ax.legend(loc="upper left", frameon=False, fontsize=10)
ax.set_title("Host: spinning poller inflated the tail — epoll cut 200K p99 46.5 -> 2.8 ms",
             fontsize=12.5)
save(fig, "fig06_host_epoll_tail.png")

# ── Fig 7: DPA grace-period polling — p50 10× lower ────────────────────────
park  = [2.03, 2.12, 1.59]
grace = [0.21, 0.23, 0.31]
fig, ax = plt.subplots(figsize=(8.2, 4.4))
x = np.arange(len(loads))
ax.bar(x - 0.19, park,  width=0.36, color=GREY,  label="park on every idle drain (before)", zorder=3)
ax.bar(x + 0.19, grace, width=0.36, color=GREEN, label="grace-period polling (after)", zorder=3)
for i, v in enumerate(park):
    ax.text(i - 0.19, v + 0.05, f"{v:.2f}", ha="center", fontweight="bold", fontsize=9.5, color="#555")
for i, v in enumerate(grace):
    ax.text(i + 0.19, v + 0.05, f"{v:.2f}", ha="center", fontweight="bold", fontsize=9.5, color=DGREEN)
ax.set_xticks(x, loads)
ax.set_xlabel("load (req/s)")
ax.set_ylabel("p50 latency (ms)")
ax.set_ylim(0, 2.5)
ax.legend(loc="upper right", frameon=False, fontsize=9.5)
ax.set_title("DPA engine polls continuously (dedicated silicon), parks only after a long\n"
             "idle grace window: p50 ~2 ms -> ~0.3 ms (10×)", fontsize=12.5)
save(fig, "fig07_grace_p50.png")

# ── Fig 8: under LOAD, busy-spin is essential (event-driven ARM collapses) ──
fig, axes = plt.subplots(1, 2, figsize=(9.6, 4.2))
ax = axes[0]
ax.bar([0, 1], [233, 676], color=[GREEN, RED], width=0.55, zorder=3)
for i, v in enumerate([233, 676]):
    ax.text(i, v + 12, f"{v}µs", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["spin\nunder load", "event-driven\nunder load"])
ax.set_ylabel("p50 latency at 30K (µs)")
ax.set_ylim(0, 800)
ax.set_title("Latency at 30K", fontsize=12)
ax = axes[1]
ax.bar([0, 1], [257, 56], color=[GREEN, RED], width=0.55, zorder=3)
for i, v in enumerate([257, 56]):
    ax.text(i, v + 5, f"{v}K", ha="center", fontweight="bold")
ax.set_xticks([0, 1], ["spin\nunder load", "event-driven\nunder load"])
ax.set_ylabel("throughput ceiling (K req/s)")
ax.set_ylim(0, 290)
ax.annotate("4.5×\nworse", xy=(1, 70), xytext=(1, 170),
            arrowprops=dict(arrowstyle="->", color=RED, lw=1.6),
            color=RED, fontweight="bold", ha="center", fontsize=11)
ax.set_title("Throughput ceiling", fontsize=12)
fig.suptitle("Design rule: POLL under load, SLEEP at idle — "
             "blocking the control thread under load is 4.5× worse", fontsize=13, y=1.03)
save(fig, "fig08_poll_under_load.png")


# ===========================================================================
# PART 3 — Step-by-step current implementation
# ===========================================================================

# ── Fig 9: data-path diagram (4-leg chain) ─────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 4.3))
ax.axis("off"); ax.set_xlim(0, 100); ax.set_ylim(0, 40)

def box(x, w, y, h, label, fc, ec, fs=10.5, tc="white"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4",
                 fc=fc, ec=ec, lw=1.6))
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
            color=tc, fontweight="bold", fontsize=fs)

box(2,  20, 22, 12, "Client host\n(pod 10)\napp + load-gen", BLUE, DBLUE)
box(40, 20, 26, 8,  "DPU ARM (control)\nroute on dst_pod_id (O(1))", PURPLE, "#4a148c", fs=9.5)
box(40, 20, 10, 8,  "DPA EU (data)\n4 dma_copy/RTT", ORANGE, "#bf360c", fs=9.5)
ax.add_patch(FancyBboxPatch((38, 7), 24, 28, boxstyle="round,pad=0.4",
             fc="none", ec="#777", lw=1.2, ls="--"))
ax.text(50, 36, "BlueField DPU", ha="center", color="#555", fontsize=10, fontweight="bold")
box(78, 20, 22, 12, "Echo host\n(pod 11)\napp", GREEN, DGREEN)

# forward legs (top)
ax.annotate("", xy=(40, 28), xytext=(22, 28),
            arrowprops=dict(arrowstyle="->", color=GREY, lw=2.2))
ax.text(31, 30, "1: host→DPU DMA", ha="center", fontsize=8.5, color="#444")
ax.annotate("", xy=(78, 28), xytext=(60, 28),
            arrowprops=dict(arrowstyle="->", color=GREY, lw=2.2))
ax.text(69, 30, "2: DPU→echo DMA", ha="center", fontsize=8.5, color="#444")
# reverse legs (bottom)
ax.annotate("", xy=(60, 13), xytext=(78, 13),
            arrowprops=dict(arrowstyle="->", color=GREY, lw=2.2))
ax.text(69, 9.5, "3: echo→DPU DMA", ha="center", fontsize=8.5, color="#444")
ax.annotate("", xy=(22, 13), xytext=(40, 13),
            arrowprops=dict(arrowstyle="->", color=GREY, lw=2.2))
ax.text(31, 9.5, "4: DPU→client DMA", ha="center", fontsize=8.5, color="#444")
ax.set_title("One RTT = a 4-leg DMA chain (host→DPU→echo→DPU→host)\n"
             "the DPU stages every body to route on dst_pod_id — host→host is foreclosed",
             fontsize=12.5)
save(fig, "fig09_datapath.png")

# ── Fig 10: RTT breakdown (~220 µs floor) ──────────────────────────────────
stages = ["1  client post\n(lock-free, no doorbell)",
          "2  FWD leg1 host→DPU\n(poll-detect + dma_copy)",
          "3  FWD_DONE relay + ARM\nroute + post delivery",
          "4  FWD leg2 DPU→echo\n(poll-detect + dma_copy)",
          "5  echo poll_rx + copy\n+ reply enqueue",
          "6  REV legs 3+4\n+ completions",
          "7  REV_DONE relay + host\nPE reap + event_fd wake"]
us = [3, 28, 30, 28, 18, 84, 29]
cols = [GREEN, ORANGE, PURPLE, ORANGE, GREEN, RED, PURPLE]
fig, ax = plt.subplots(figsize=(10.5, 4.8))
left = 0
for s, v, c in zip(stages, us, cols):
    ax.barh([0], [v], left=[left], color=c, edgecolor="white", height=0.6, zorder=3)
    if v >= 18:
        ax.text(left + v / 2, 0, f"{v}", ha="center", va="center",
                color="white", fontweight="bold", fontsize=10)
    left += v
ax.set_xlim(0, 230); ax.set_ylim(-1.4, 0.7)
ax.set_yticks([])
ax.set_xlabel("RTT latency (µs) — total ≈ 220 µs, FLAT vs payload size (64 B ≈ 8 KB)")
# legend below
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in cols]
ax.legend(handles, [s.replace("\n", " ") for s in stages],
          loc="upper center", bbox_to_anchor=(0.5, -0.32), ncol=2, fontsize=8.3, frameon=False)
ax.set_title("Where one RTT spends its ~220 µs: 4 DMA poll-detect legs (~112 µs)\n"
             "+ 2 completion relays (~60 µs) — fixed overhead, not data transfer", fontsize=12.5)
save(fig, "fig10_rtt_breakdown.png")

# ── Fig 11: latency vs load (p50/p99/p999) ─────────────────────────────────
load = [1, 10, 30, 60, 100, 150, 200, 240]
p50  = [371, 252, 233, 222, 230, 268, 295, 340]
p99  = [497, 499, 510, 309, 317, 437, 443, 586]
p999 = [867, 1245, 879, 1065, 5896, 673, 626, 1272]
fig, ax = plt.subplots(figsize=(9, 4.6))
ax.plot(load, p50,  "-o", color=GREEN,  lw=2, label="p50")
ax.plot(load, p99,  "-s", color=ORANGE, lw=1.8, label="p99")
ax.plot(load, p999, "-^", color=GREY,   lw=1.5, label="p999")
ax.axvspan(235, 245, color="#ffe0b2", alpha=0.6, zorder=0)
ax.text(240, 5200, "knee\n~240K", ha="center", fontsize=9, color=ORANGE)
ax.set_xlabel("load (K req/s)")
ax.set_ylabel("RTT latency (µs)")
ax.set_ylim(0, 6200)
ax.legend(frameon=False, fontsize=10)
ax.set_title("Latency holds flat (~220–340 µs p50) up to ~240K, then the knee",
             fontsize=12.5)
save(fig, "fig11_latency_vs_load.png")

# ── Fig 12: CPU vs load — the DPU control thread saturates first ────────────
load = [30, 60, 100, 150, 200, 240]
arm  = [39, 67, 81, 88, 96, 98]
cli  = [18, 13, 32, 44, 46, 51]
echo = [18, 16, 19, 36, 30, 20]
fig, ax = plt.subplots(figsize=(9, 4.6))
ax.plot(load, arm,  "-o", color=PURPLE, lw=2.4, label="DPU control thread")
ax.plot(load, cli,  "-s", color=BLUE,   lw=1.8, label="host client core")
ax.plot(load, echo, "-^", color=GREEN,  lw=1.8, label="host echo core")
ax.axhline(100, ls="--", color=GREY, lw=1)
ax.text(60, 102, "1 core saturated", fontsize=9, color=GREY)
ax.annotate("98% @240K\n(saturates first)", xy=(240, 98), xytext=(170, 70),
            arrowprops=dict(arrowstyle="->", color=PURPLE, lw=1.5),
            color=PURPLE, fontweight="bold", fontsize=9.5, ha="center")
ax.set_xlabel("load (K req/s)")
ax.set_ylabel("CPU utilization (%)")
ax.set_ylim(0, 115)
ax.legend(frameon=False, fontsize=10, loc="upper left")
ax.set_title("The single DPU control thread saturates first;\nhost client (51%) and echo (≤36%) keep headroom",
             fontsize=12.5)
save(fig, "fig12_cpu_scaling.png")

# ── Fig 13: throughput ceiling = DMA op-rate; EU count helps only to 4 ──────
fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.4))
ax = axes[0]
eus = ["1 core", "4 cores\n(default)", "8 cores"]
ceil = [72, 257, 256]
ax.bar(range(3), ceil, color=[GREY, GREEN, GREY], edgecolor=DGREEN, width=0.6, zorder=3)
for i, v in enumerate(ceil):
    ax.text(i, v + 5, f"{v}K", ha="center", fontweight="bold")
ax.set_xticks(range(3), eus)
ax.set_ylabel("throughput ceiling (K req/s)")
ax.set_ylim(0, 300)
ax.set_title("DPA cores: a resource only up to 4\n(8 cores = 4 cores → shared op-rate wall)", fontsize=11.5)
ax = axes[1]
tgt  = [200, 240, 260, 280, 300]
ach  = [198.9, 238.7, 256.0, 257.9, 257.3]
p50r = [0.31, 0.34, 0.62, 418, 798]   # ms
ax.bar(range(5), ach, color=[GREEN, GREEN, ORANGE, RED, RED], edgecolor="#333", width=0.62, zorder=3)
for i, v in enumerate(ach):
    ax.text(i, v + 4, f"{v:.0f}K", ha="center", fontweight="bold", fontsize=9)
ax.axhline(257, ls="--", color=GREY, lw=1)
ax.text(0.1, 263, "hard ceiling ~257K", color=GREY, fontsize=9)
ax.set_xticks(range(5), [f"{t}K" for t in tgt])
ax.set_xlabel("target load")
ax.set_ylabel("achieved (K req/s)")
ax.set_ylim(0, 300)
ax.set_title("Achieved plateaus at ~257K = DMA op-rate\n(1.03M ops/s ÷ 4 DMA per RTT)", fontsize=11.5)
fig.suptitle("Throughput ceiling is structural: the shared DMA-engine op-rate, not host/ARM CPU",
             fontsize=13, y=1.03)
save(fig, "fig13_ceiling.png")

# ── Fig 14: DPUmesh vs TCP + Envoy sidecar (p50) ───────────────────────────
loads = ["1K", "30K", "100K"]
dpm   = [371, 233, 230]
tcp   = [1194, 9347, 54488]
fig, ax = plt.subplots(figsize=(8.5, 4.6))
x = np.arange(len(loads))
ax.bar(x - 0.19, dpm, width=0.36, color=GREEN, edgecolor=DGREEN, label="DPUmesh", zorder=3)
ax.bar(x + 0.19, tcp, width=0.36, color=RED,   edgecolor="#7f1d1d", label="TCP + Envoy sidecar", zorder=3)
for i, v in enumerate(dpm):
    ax.text(i - 0.19, v * 1.25, f"{v}µs", ha="center", fontweight="bold", fontsize=9, color=DGREEN)
for i, v in enumerate(tcp):
    ax.text(i + 0.19, v * 1.25, f"{v/1000:.1f}ms", ha="center", fontweight="bold", fontsize=9, color=RED)
ax.annotate("40× lower", xy=(1.19, 9347), xytext=(1.0, 800),
            arrowprops=dict(arrowstyle="->", color=DGREEN, lw=1.6),
            color=DGREEN, fontweight="bold", ha="center", fontsize=11)
ax.text(2.19, 70000, "collapses\n(15/1000 ok)", ha="center", color=RED, fontsize=9, fontweight="bold")
ax.set_yscale("log")
ax.set_xticks(x, loads)
ax.set_xlabel("load (req/s), same 1 shared core")
ax.set_ylabel("p50 latency (µs, log)")
ax.set_ylim(100, 200000)
ax.legend(loc="upper left", frameon=False, fontsize=10)
ax.set_title("vs TCP + Envoy sidecar (Istio tax): ~40× lower p50 at 30K,\n"
             "and DPUmesh frees the host core — the sidecar collapses by 100K", fontsize=12.5)
save(fig, "fig14_vs_tcp.png")

print("done ->", OUT)
