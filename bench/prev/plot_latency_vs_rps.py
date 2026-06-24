#!/usr/bin/env python3
"""
Plot: dpumesh latency (p50, p99) vs achieved RPS, comparing two host-side
core configurations.

Goal: show that giving the host side more cores does NOT increase the
achieved RPS — the cap sits inside the DPU/DPA chain, not on the host.

Output: bench/latency_vs_rps.png  (linear scale on both axes)
"""

import os
import matplotlib.pyplot as plt

# (target_rps, achieved_rps, p50_us, p99_us)
fair = [
    ( 5_000,  4_968.6,   1_104.9,    1_927.3),
    (10_000,  9_939.2,   1_597.9,    2_817.9),
    (15_000, 14_906.5,   2_083.9,    3_720.6),
    (20_000, 19_875.0,   2_566.2,    4_642.6),
    (25_000, 24_838.1,   3_062.3,    5_552.5),
    (30_000, 29_807.8,   3_537.2,    6_508.3),
    (35_000, 34_767.7,   4_047.3,    7_449.7),
    (40_000, 39_732.7,   4_529.1,    8_348.8),
    (45_000, 44_691.3,   5_015.7,    9_291.9),
    (50_000, 49_664.7,   5_408.7,   11_892.6),
    (52_000, 51_644.8,   5_534.0,   12_158.4),
    (55_000, 53_553.4,  74_136.3,  189_589.0),
    (57_000, 53_404.0, 319_625.4,  604_520.0),
    (60_000, 53_698.8, 587_514.8, 1_154_246.2),
    (65_000, 53_841.1,1_043_727.5, 1_961_626.1),
]

hw = [
    ( 5_000,  4_971.8,   1_309.0,    2_266.5),
    (10_000,  9_942.9,   1_787.7,    3_007.6),
    (15_000, 14_912.8,   2_296.1,    4_007.1),
    (20_000, 19_883.4,   2_815.8,    5_086.0),
    (25_000, 24_850.7,   3_368.8,    6_136.1),
    (30_000, 29_816.1,   3_917.4,    7_230.1),
    (35_000, 34_786.2,   4_442.4,    8_705.0),
    (40_000, 39_751.4,   4_815.7,    9_127.4),
    (45_000, 44_716.5,   5_336.8,   11_906.2),
    (50_000, 49_672.9,   8_200.3,   19_847.8),
    (55_000, 52_032.3, 311_153.5,  475_783.2),
    (60_000, 52_316.2, 938_320.3, 1_438_215.1),
    (65_000, 51_313.6,1_467_699.2, 2_567_533.4),
]

def split(rows):
    rps   = [r[1] for r in rows]
    p50ms = [r[2] / 1000.0 for r in rows]
    p99ms = [r[3] / 1000.0 for r in rows]
    return rps, p50ms, p99ms

f_rps, f_p50, f_p99 = split(fair)
h_rps, h_p50, h_p99 = split(hw)

fig, ax = plt.subplots(figsize=(10, 6))

# 1 host core per pod — blue
ax.plot(f_rps, f_p50, 'o-',  color='#1f77b4', linewidth=2,
        markersize=6, label='1 host core / pod — p50')
ax.plot(f_rps, f_p99, 's--', color='#1f77b4', linewidth=2,
        markersize=6, label='1 host core / pod — p99', alpha=0.85)

# 2 host cores per pod — red
ax.plot(h_rps, h_p50, 'o-',  color='#d62728', linewidth=2,
        markersize=6, label='2 host cores / pod — p50')
ax.plot(h_rps, h_p99, 's--', color='#d62728', linewidth=2,
        markersize=6, label='2 host cores / pod — p99', alpha=0.85)

ax.set_xlabel('Achieved RPS (ops/s)', fontsize=12)
ax.set_ylabel('Latency (ms)', fontsize=12)
ax.set_title('DPUmesh: latency vs achieved RPS\n'
             '(host-side cores increased; achieved RPS unchanged → cap is inside DPU/DPA chain)',
             fontsize=12)

ax.grid(True, alpha=0.3)
ax.legend(loc='upper left', fontsize=10)

# Annotate the saturation cap region for clarity
sat_1c = max(f_rps)
sat_2c = max(h_rps)
ax.axvline(sat_1c, color='#1f77b4', linestyle=':', alpha=0.4)
ax.axvline(sat_2c, color='#d62728', linestyle=':', alpha=0.4)
ax.text(sat_1c, ax.get_ylim()[1] * 0.95,
        f' cap (1 core)\n ≈ {sat_1c/1000:.1f}K',
        color='#1f77b4', fontsize=9, va='top')
ax.text(sat_2c, ax.get_ylim()[1] * 0.80,
        f' cap (2 cores)\n ≈ {sat_2c/1000:.1f}K',
        color='#d62728', fontsize=9, va='top')

plt.tight_layout()

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'latency_vs_rps.png')
plt.savefig(out_path, dpi=140, bbox_inches='tight')
print(f"saved: {out_path}")
