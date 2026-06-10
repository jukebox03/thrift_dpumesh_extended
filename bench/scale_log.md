# DPUmesh scalability — direct-experiment log (2026-06-08)

Goal: (1) raise 2-pod chain RPS, (2) make DPA/DPU multithread actually scale.
Constraint: all traffic stays host→DPU→host (L7 routing computed on DPU; no host→host).
Method: DIRECT experiments via `test-bench.sh` only. No elimination logic.

## Code-certain baseline facts (from code map, wf_a080182c-52b)
- `pod_id % num_dpa_threads` (dpa.c:1091,1278) → 2 pods always use exactly 2 EUs (N=4/8 idle the rest). This alone caps DPA-thread scaling for 2 pods.
- Chain inserts a mandatory per-request EU→ARM→EU round-trip: reverse `dma_copy` waits for the single ARM thread to drain the fwd completion (consumer_pe→comp_queue), route, and post a reverse desc (dpu_worker.c:194). pure_dma/M2 have no such wait → they scale.
- EU-sharding one pod across K EUs IS feasible (host posts K fwd rings; ARM round-robins reverse to K tx_rings; admission globals already [eu_index][ring]). Docs' "foreclosed" was elimination, not code-true.
- SPLIT_SHARD(=3)+DRAIN_SHARDS machinery is BUILT but never benchmarked on the chain. DRAIN_SHARDS>1 races for cross-pod echo (fwd EU≠rev EU → shard_work[k] gets 2 producers) — so test SPLIT_SHARD=3 with DRAIN_SHARDS=1 only.

## Hypotheses (each has a positive yes/no test)
- H1 single-ARM funnel bounds throughput → test: SPLIT_SHARD=3 raises 2-EU ceiling?
- H2 per-request data/host cost (8KB memset+memcpy, DMA xfer) dominates W → test: 128B vs 8KB ceiling differ?

---

## Experiments

### E0/E1 — deploy DPUMESH_DPA_THREADS=2 (split off), 8KB ceiling + size sweep
config: N=2, affinity=1, split=off, slots=2048, fair pin
(results below)

| exp | size | target RPS | achieved | p50 ms | p99 ms | ok/fail | notes |
|---|---|---|---|---|---|---|---|
| E0 | 8192 | 95000 | 94,166 | 12.1 | 16.8 | 950000/0 | healthy |
| E0 | 8192 | 104000 | 103,178 | 14.2 | 21.6 | 1.04M/0 | |
| E0 | 8192 | 105000 | **104,204** | 14.2 | 20.3 | 1.05M/0 | **sustainable ceiling** (=docs 104,202) |
| E0 | 8192 | 110000 | 106,963 | 117.5 | 206 | 1.1M/0 | overload (p50 jumps) |
| E1 | 128 | 105000 | 104,104 | 14.9 | 22.5 | 1.05M/0 | healthy, ~same as 8KB |
| E1 | 128 | 150000 | 108,730 | 1908 | 3710 | 1.5M/0 | deep overload (lat in seconds) |
| E1 | 128 | 200000 | 100,052 | 3771 | 7441 | — | past-knee decline |
| E1 | 128 | 250000 | 11,927 | 11428 | 15560 | 195300/1498 | collapse |
| E1 | 1024 | 105000 | 103,156 | 61.2 | 124 | 1.05M/0 | healthy-ish (lat noisier) |
| E1 | 1024 | 150000 | 102,077 | 2282 | 4559 | 1.5M/0 | overload |
| E1 | 1024 | 200000 | 120,261 | 3011 | 5933 | — | past-knee (unreliable) |
| E1 | 1024 | 250000 | 9,296 | 415 | 14824 | — | collapse |

**E0/E1 verdict: ceiling is SIZE-INDEPENDENT (~104K sustainable for 128B/1KB/8KB).**
→ H2 (data/host-memcpy/DMA-bandwidth bound) REJECTED by direct measurement. 64× less payload (8KB→128B) does not raise RPS. The cap is a fixed per-request op/handoff cost (dma_copy op-rate and/or EU↔ARM round-trip cadence), NOT bytes moved. Consistent with pure_dma op-rate boundedness, but chain caps at 416K dma_copy/s vs pure 2-EU 1.07M → the limiter is the closed-loop HANDOFF cadence, not the dma_copy engine.

### E2 — SPLIT_SHARD=3 (DRAIN_SHARDS=1), N=2, 8KB — H1 test (parallelize ARM route+reverse-post+send)
(results below)

| exp | target RPS | achieved | p50 ms | p99 ms | ok/fail | notes |
|---|---|---|---|---|---|---|
| E2 | 104000 | 103,036 | 14.4 | 20.4 | 1.04M/0 | |
| E2 | 105000 | **104,112** | 14.8 | 20.7 | 1.05M/0 | **= E0 104,204 (±0.1%)** |
| E2 | 110000 | 104,962 | 253 | 472 | 1.1M/0 | overload |
| E2 | 120000 | 102,691 | 827 | 1622 | 1.2M/0 | overload |

**E2 verdict: SPLIT_SHARD (4 ARM cores: drain+2 workers+sender) = E0 baseline within noise → H1 REJECTED.**
ARM route/reverse-post/send compute is NOT the throughput limiter. Combined with E1 → limiter is the closed-loop HANDOFF latency. Supporting prior evidence: bench.md §3.3 busy-spin (no EU yield) improved p99 → yielded EU only re-woken by msgq msg; ARM reverse-post is a silent memory write → EU waits up to keepalive(1kHz)=1ms to re-check tx_ring.

### Refined ladder (decision tree)
- E2 SPLIT_SHARD=3/DRAIN=1: parallelize ARM route+reverse-post(×2 workers)+send(1). Helps → ARM compute was serializing. Flat → ARM compute not the limiter (drain alone does >1M/s in M2, so not throughput-bound either) → limiter is closed-loop HANDOFF latency.
- E3 keepalive freq 1kHz→10kHz (+ later: ARM targeted-WAKE after reverse-post): EU yields when it out-runs ARM and only re-checks tx_ring on keepalive/recv-msg (dpa_kernel.c:491; ARM reverse-post is a silent memory write). If raising keepalive raises RPS → yield/wake latency is a real handoff component.
- E4 EU-sharding (CODE): host posts pod10/11 each across K fwd rings; ARM round-robins reverse to K tx_rings; admission globals already [eu][ring]. The ONLY way DPA multithread scales for 2 pods. Helps → per-pod serial EU work was binding; scalable multithread achieved.

### E3 — keepalive 1kHz→20kHz (50us), N=2 split off — handoff yield-wake test
knob: DPUMESH_KEEPALIVE_US (new), default 1000. E3 uses 50.
Prior context: 4-pod 2-pairs-halve (memory project_shard_bottleneck_consumer_pe) → shared sub-ARM resource (PCIe/comch op-rate OR closed-loop RTT). E1 rules out bandwidth, E2 rules out ARM CPU. E3 probes one RTT component (EU yield→keepalive wake). If flat → do per-request STAMP breakdown to positively localize W.

| exp | target RPS | achieved | p50 ms | p99 ms | ok/fail | notes |
|---|---|---|---|---|---|---|

### E3 results (keepalive 50us, N=2, split off)
| target | achieved | p50 ms | p99 ms | note |
|---|---|---|---|---|
| 50000 | 49,655 | 4.0 | 5.8 | |
| 95000 | 94,261 | 11.9 | 16.6 | =E0 (12.1/16.8) |
| 105000 | 104,182 | 14.7 | 21.5 | **=E0 104,204** |
| 110000 | 103,961 | 278 | 486 | overload |

**E3 verdict: keepalive 50us (20kHz) = E0 exactly (ceiling AND low-load latency). EU yield-wake REJECTED** — EU is not yield-bound at load (it spins). The 1kHz keepalive only matters at <10K RPS (the ~1.1ms idle floor).

### E4 — ARM occupancy under 105K load (DPU 1Hz stat, -l 50, reverted after)
Sustained 105K (achieved 104,539, p99 20.8ms):
- **recv: ~420,000/s** (= 4 dma_copy/RTT × 105K) — completions drained at expected rate
- **cq_depth: 0** EVERY second — comp_queue ALWAYS empty
- sent: 2000/s = keepalive only (sent_msg_cnt counts DPU→DPA wakes, NOT host sends)

**E4 verdict (POSITIVE evidence): cq_depth=0 → ARM is STARVED, never backed up.** Processes every completion instantly, waits for the next. Perfectly balanced at 420K completions/s, zero queueing → textbook **latency-bound closed loop** (T=C/W, W fixed). The cap is the round-trip W itself, not any stage's throughput.

## CONSOLIDATED DIAGNOSIS (all direct, no elimination-as-proof)
The 2-pod chain ceiling (~104K RPS) is set by the **per-request host↔DPU round-trip latency W**, which is:
- **shared** (4-pod: 2 pairs halve each other — memory) — not per-pair
- **size-independent** (E1: 128B=1KB=8KB) — not bandwidth/memcpy
- **not ARM compute** (E2 SPLIT_SHARD flat; E4 cq_depth=0/starved)
- **not EU yield/wake** (E3: 20× keepalive flat)
- **not concurrency/depth** (prior M/M/1 flat)
- DPA N>2 structurally idle for 2 pods (pod%N → 2 EUs) [code]

→ The bottleneck is OUTSIDE the DPU compute (EU/ARM). It's the closed-loop round-trip itself (PCIe op-rate / completion-delivery latency / host-side turnaround). **This is why DPA/DPU multithreading doesn't raise throughput: the threaded resource is not the bottleneck.** μ≈104K is a hard service-rate of the round-trip, and threading the EU or ARM cannot move a latency-bound, non-compute cap.

### E5 — DPU-side hop-latency trace (DPUMESH_TRACE=1, -l 40, separate file) — PIVOTAL
Trace = fwd-completion-dequeue → rev-completion-dequeue (same req_id) = full DPU contribution to one hop.

| load | achieved | host p50 | host p99 | DPU hop avg | DPU hop max | DPU ×2 / RTT |
|---|---|---|---|---|---|---|
| 105K | 104,867 | 14,879 us | 26,216 us | **207.9 us** | 1417 us | **2.8%** |
| 50K | 49,801 | 4,882 us | 7,761 us | **987.8 us** | 2014 us | **40%** |

**E5 verdict (POSITIVE, decisive):**
1. **Throughput ceiling = HOST-TRANSPORT-bound.** At 105K the DPU contributes 2.8% of RTT and cq_depth=0 (DPU never backed up). The other 97% + the 104K cap are HOST-side (single fwd ring_lock, single RX PE thread, single rx_queue — host-side code map). **DPA/DPU multithreading can't raise throughput because the DPU is NOT the bottleneck.** Overturns the prior memory guess ("cap below ARM = PCIe/comch").
2. **DPU latency floor = EU yield→keepalive wake for reverse pickup.** Inverts with load (988us@50K > 208us@105K): low load → EU yields → waits ~1 keepalive to pick up the ARM-posted reverse desc. (Confirms E3's small 50K drop 4.88→4.0ms was real, not noise.) Fixable by a targeted ARM→EU wake right after reverse-post → cuts DPU hop ~10-50× → big p50/p99 win at low/mid load (throughput unchanged — host-bound).

## REVISED CONCLUSION
- **Why DPU/DPA multithread doesn't help:** the bottleneck is the HOST transport, not the DPU. Proven directly (DPU=2.8% of RTT, cq=0). The EU(pod%N→2)/ARM threading is correct engineering aimed at a non-bottleneck.
- **Throughput lever (real):** parallelize the HOST transport — multiple RX PE threads / lock-free rx_queue / per-worker forward rings (remove single ring_lock). This is where "scalable multithreading" must happen.
- **Latency lever (DPU, quick win):** targeted ARM→EU wake after reverse-post (or higher keepalive). Cuts DPU hop from ~0.2-1ms to ~tens of us.
- TODO: redeploy with DPUMESH_TRACE=0 to drop the per-hop clock_gettime overhead once done measuring.

### E6 — host TX free-list (O(1) alloc replacing O(in-flight) scan) — REGRESSED
Hypothesis: tx_alloc's linear bitmap scan under slot_lock (held ~1500 iters/alloc, ~1050 workers contend) was the host cap.
| target | achieved | p50 ms | p99 ms | note |
|---|---|---|---|---|
| 30000 | 29,802 | 3.9 | 6.3 | fine |
| 50000 | 49,658 | 5.4 | 7.9 | fine |
| 95000 | 94,261 | 13.6 | 20.0 | fine |
| 100000 | 97,300 | 98 | 214 | OVERLOAD (baseline handled 104K @14ms) |
| 100000 | 97,302 | 118 | 203 | back-to-back stable (NOT a leak) |
| 105000 | 99,118 | 290 | 575 | overload |

**E6 verdict: TX free-list REGRESSED ceiling 104K→~96K (LIFO worse than lowest-index-first scan; back-to-back stable so not a leak).**
→ tx_alloc scan was NOT the host cap (removing it didn't help — it hurt). REVERTED. The allocation *pattern* mattering (LIFO worse) hints the host TX-buffer access locality matters, but the scan itself isn't the bottleneck. Lesson: even an "obvious" O(n)-under-lock wasn't it — must measure, not assume.

### E7 — host localization instrumentation (reverted free-list + rx_queue depth / tx_inflight stats)
Added dpumesh_debug_stats(): echo rx_queue_depth (host analog of cq_depth) + tx_inflight, printed 1Hz to pod stderr by both daemons.
Decision map:
- echo rx_queue_depth HIGH → echo consumers (workers/rx_lock/tx_alloc/enqueue) are the cap.
- echo rx_queue_depth ~0 + bench tx_inflight near num_slots → upstream of echo (RX PE-thread delivery rate) is the cap.
- bench tx_inflight low → client generation is the cap.
(results below)

### E7 results — host localization (rx_queue_depth / tx_inflight at 105K & 110K)
| load | echo rx_queue_depth | echo tx_inflight | bench tx_inflight (max) |
|---|---|---|---|
| 105K (knee) | 0 (max 1) | 67-525 | 554-751 (1009) |
| 110K (overload) | 0 (max 1) | 92-298 | 851-1061 (1062) |

**E7 verdict: echo NEVER backs up (rx_queue_depth≈0 even at overload) → echo re-send is NOT the cap.**
Bench tx_inflight maxes at ~n_workers (not 2048) → workers stuck awaiting TX_ACK (request-delivery), not slot-starved. The cap is the host SEND-side serial round-trip processing.

### E8 — host SEND concurrency (conns) sweep @160K + dpumesh-hw
| config | achieved | note |
|---|---|---|
| hw 2-core 105K/130K/160K | 104,217 / 105,020 / 103,055 | hw FLAT = serial element, not core count |
| conns=1000 | 107,465 | best (more conns than this hurts) |
| conns=2048 | 91,984 | WORSE — context-switch contention |
| conns=4096 | (wedge) | system deadlocked |

**E8 verdict: host send is a SERIAL-RATE cap (~104-107K). More concurrency HURTS. hw 2-core flat. → single host RX progress thread (pe + consumer_pe drained serially) + per-request worker wakeup on one core is the serial element.**

### WEDGE (conns=4096 overcommit) — robustness bug
conns=4096 > echo's 2048 TX slots → echo rx_queue filled to 1984, tx_inflight=2048 (exhausted) → echo can't alloc TX to respond → can't drain rx_queue → bench forward DMA ring stuck (head=1106, 34M spin retries) → permanent wedge (workers stuck in dpumesh_enqueue, never check stop). Needs redeploy. Lesson: keep conns ≤ ~num_slots; the closed loop has no admission cap on offered concurrency vs echo TX capacity.

## ANSWER (host-send vs echo-resend): HOST SEND side.
- echo re-send fine (rx_queue=0); host send is the serial cap (single PE thread + worker-wakeup on 1 core; more concurrency hurts; hw flat).
- FIX direction: split host RX progress (pe vs consumer_pe → 2 threads) so dispatch parallelizes on hw; and/or reduce per-request wakeup cost.

## ★ WIN — TX_ACK elimination unlocks host multi-core scaling (2026-06-08)
Lever: `DPUMESH_SKIP_REQ_TXACK=1` — DPU does NOT send TX_ACK for request forwards; the CLIENT frees its TX slot when the response (REV_DONE) arrives (response implies request round-tripped). Response forwards still ACK their src (echo). Host-side free is always-on + no-op-if-already-freed → safe. (User's idea; avoids any new DPA host-memory read — just skips a message.)

### E9 — SKIP_REQ_TXACK (verified active: bench tx_inflight 750→1500 full-RTT hold; 0-fail; back-to-back leak-safe)
| profile | sustainable | overload | p50 | note |
|---|---|---|---|---|
| fair 1-core + SKIP | 104K | — | 14ms | NO gain (single core saturated) |
| hw 2-core, NO skip (E8) | ~104K | ~105K | — | NO gain (serial PE thread; 2nd core wasted) |
| **hw 2-core + SKIP** | **~124K** | 130K | **6.6ms** | +19%, p50 −53% |
| **hw3 3-core + SKIP** | **~130K** | **137K** | 6.1ms | +25-32% |

back-to-back hw+SKIP @124K: 122,685→122,806 (0-fail, recovery clean) — robust, no leak.

**Mechanism:** the host RX was a single serial progress thread (`pe`, draining all comch control msgs + per-request worker wakeup). Reducing its per-request work (2→1 msg/RTT) ALONE doesn't help on 1 core (saturated), and a 2nd core ALONE doesn't help (serial element). TOGETHER they break it: less host work drops the `pe` thread below the serial-bottleneck point, letting extra cores run workers in parallel → host scales 104K→124K→130K for 1→2→3 cores.

**Next limit = TX slots.** SKIP holds TX slots the full RTT (vs half via TX_ACK) → tx_inflight 2×; at ~150K it hits num_slots=2048 → tx_alloc blocks → ~137K plateau. Levers to push further:
  (a) more TX slots + matching DPU_BUFFER_SIZE (2048/16MB → 4096/32MB), or
  (b) batched half-RTT TX free (DPU writes request-delivered cumulative counter to host mem, host polls) — keeps tx_inflight at half-RTT, removes slot pressure (user's original producer-consumer idea, more complex), or
  (c) more host cores (diminishing: +6K for the 3rd).
New knobs: DPUMESH_SKIP_REQ_TXACK; test-bench.sh dpumesh-hw3 (3-core profile).

### A result — 4096 slots / 32MB buffer (SKIP + hw3)
135K→133,890 (p99 15ms healthy), overload plateau ~138K. tx_inflight now ~1700 (was pinned 2048) → slots NOT the real cap (+3K only). The ~137K cap is now CORES (hw3 uses all 6 available dpumesh cores; 2,3 are TCP) + echo starting to back up (rx_queue max 99 @145K).

### ★ rec-2 — adaptive PE polling (core-efficiency) — CLEAN WIN
knob DPUMESH_PE_ADAPTIVE (default 0=busy-poll). PE thread spins while RX work present, yields core (20us nanosleep after 2048 empty polls) when idle.
- IDLE: cores 0,1 = **100% idle** (busy-poll = 0% idle) → transport returns core to app.
- LOAD: throughput UNCHANGED (fair 104,021 / hw3 133,664, 0-fail); cores busy (core0 0% idle) → polls tight when work present.
→ Directly serves "minimize transport cores": the transport no longer burns a full core when idle. Throughput-neutral.

### B (next) — batched TX_ACK for echo
Note from analysis: SKIP already removed the request-side TX_ACK (bench frees on response). The remaining echo message is the response-forward TX_ACK — but that's the LIGHT message (just frees a slot); the echo's HEAVY work is the REV_DONE (request data RX: rx_slot_alloc+memcpy+enqueue+signal). So batched-TX_ACK is expected marginal; implementing to verify per direct-test policy.

### B — batched TX_ACK (DPUMESH_BATCH_TXACK): first deploy WEDGED (bug found+fixed)
DPU coalesces response-forward TX_ACKs into dmesh_batch_tx_ack_msg (DMESH_MSG_BATCH_FWD_ACK=5, up to 14/msg), flush-on-full + 1kHz tail flush; host frees K slots/msg.
BUG: comch_client.c client_message_recv_callback dispatches on recv_buffer[0] in a SWITCH that only forwards known types to rx_data_hook; BATCH_FWD_ACK=5 hit `default` → message DROPPED (echo never freed TX → leak → echo tx_inflight=4096/rx_queue=4032 WEDGE, 0 RPS / mass fail) AND logged "unknown message type" per msg → LOG FLOOD. Gatekeeper missed: a new DPU→host msg type needs a case in BOTH comch_client switch AND rx_data_hook.
FIX: added `case DMESH_MSG_BATCH_FWD_ACK` → rx_data_hook. Reverted to good config (BATCH=0) to recover, then redeployed with fix.
Recovery health (good config): fair 101,497 / hw3 128,780, 0-fail.

### B result (fixed) — CORRECT, throughput-neutral
fair 104,204 / hw3 128,981 (0-fail), back-to-back 126,815→126,823→30K all 0-fail, echo tx_inflight 91-134 bounded (no leak). Reduces echo TX_ACK messages ~14× (65K→~4.6K msg/s @130K) = echo core-efficiency, but THROUGHPUT-NEUTRAL — confirms message count is not the cap (matches SKIP-on-fair=flat and A=marginal). Value: echo per-request host work ↓ (core-efficiency), not throughput.

## SESSION FINAL (deployed config: SKIP=1, NUM_SLOTS=4096, PE_ADAPTIVE=1, BATCH_TXACK=1; all 0-fail)
| change | throughput | latency/efficiency |
|---|---|---|
| baseline (fair 1c) | 104K | p50 14ms |
| **SKIP + 2-3 cores** | **124-130K (+25-30%)** | **p50 ~6ms (halved)** |
| A: 4096 slots | +3K (marginal) | removed slot cap |
| **rec2: adaptive PE poll** | neutral | **frees transport core at idle (100% idle vs busy-poll)** |
| B: batched TX_ACK | neutral | echo msgs ~14× fewer (echo core-efficiency) |

Throughput levers exhausted on host cores (hw3 = all 6 available dpumesh cores); message/slot reductions are core-efficiency not throughput (cap is closed-loop service rate / per-request data-RX + wakeup). DPU/DPA threading was a non-bottleneck AT 104K (DPU=2.8% of RTT) — but the bottleneck MOVED: after the host fixes raised the ceiling to 130K, the 2 EUs become CO-limiting (see Q3 below; DPU hop grows 208→580µs = 16.6% of RTT).

## Q3 — DPA EU limit re-examination (the bottleneck MOVED to dual after host fixes)
User Q: original N=1=74K, N=2=104K = only 1.4× (not 2×) — is the DPA thread a limit? Re-measured N=1/N=2/N=4 with the host-fixed config (SKIP+4096+adaptive+batch) on hw3 (host NOT limiting):

| N (DPA threads) | hw3 sustainable | note |
|---|---|---|
| N=1 (1 EU) | **~74K** (overload ~75K) | = original N=1 even with host unblocked → original 74K was **EU-bound, NOT host-bound** |
| N=2 (2 EU) | **~130K = 1.76× N=1** | 2nd EU helps, but EU↔ARM handoff eats the rest of the 2× |
| N=4 (4 EU) | **128,735 = N=2** | pod%N → 2 pods always use EUs 2,3 = 2 active. More DPA threads useless for 2 pods |

DPU-hop trace (DPUMESH_TRACE) vs load: @104K (original, host-bound) = 208µs = **2.8%** of RTT (E5). @130K (host-fixed) = **580µs** (max 2033) = **~16.6%** of RTT (p50 ~7ms); N=1 @80K = 421µs. → hop GROWS with load = reverse work queuing on the busy EUs = EU saturation signal.

**Q3 verdict: the DPA EUs ARE a real (co-)limit — the bottleneck MOVED there as the host was fixed.**
- @104K: host-bound (DPU 2.8%, E5 correct). After SKIP+multicore lifted the host to 130K, the 2 EUs are co-binding (DPU 16.6% of RTT, ~88% of the 2-EU chain capacity).
- "74K→148K (2×)" didn't happen because (1) the EU↔ARM handoff makes the 2nd EU 1.76× not 2×, and (2) at 104K the host capped before the EU mattered.
- 2 pods → pod%N locks 2 EUs → adding DPA threads (N=4) is useless. The ONLY way to add EU capacity = **EU-sharding** (host posts K fwd rings/pod; ARM round-robins reverse to K tx_rings; admission globals already [eu][ring]).

→ Revises the "purely host-bound" framing: at 130K it is **DUAL-limited** (host cores hw3-maxed + DPA 2-EU). Two remaining levers: **Option A** (async client / kill the client wakeup — is the wakeup or the DPA the cap?) and **Option B** (EU-sharding — break the 2-EU lock). Either could be the next binding one.

## Q1/Q2 quick answers
- **Q1 (single core same?):** YES — fair(1 core) = 104K with ALL changes. The +25-30% is multi-core-only; SKIP/zerocopy/etc. don't raise single-core throughput, they UNLOCK multi-core scaling.
- **Q2 (TX_ACK deleted? producer-consumer?):** SKIP = TRUE elimination for the BENCH's request TX_ACK (bench frees on response arrival = piggyback, no new msg, no DPA host-read — your concern avoided). B = the echo's TX_ACK was BATCHED (coalesced), NOT the full producer-consumer ring (echo has no response to piggyback). Full ring not built: B(batch) measured throughput-neutral → message count isn't the cap → full ring would also be neutral.

## HOST WEIGHT REVIEW (workflow w7w94rbhn) — why the host is "too heavy"
Per-request host cost (dpumesh_doca.c): SEND ~7-20µs (tx_alloc O(in-flight) scan dominates), RX cleanup ~8-19µs (rx_slot_alloc scan + 8KB staging memcpy on the PE thread). Per request ≈ 5-7 lock acquires + 2-3× 8KB memcpy + 2 O(n) scans + 2 context switches (cond wait/signal).
5 biggest weight sources: (1) single PE dispatcher doing heavy work (scan+copy+wakeup) inline; (2) per-request worker WAKEUP (cond_signal/wait); (3) ~5 coarse global locks (ring_lock serializes ALL TX posts); (4) redundant 8KB copies (RX staging + echo rx→tx); (5) O(num_slots) bitmap scans.
**ALL inside the dpumesh API (dpumesh_doca.c), NOT the bench harness.** Verified: TDpumeshClientTransport/TDpumeshTransport (real Thrift) call IDENTICAL dpumesh_* functions → fixing dpumesh_doca.c helps every user. Caller-specific only: worker COUNT (bench 1050 vs a real pool) and echo's body-copy shortcut.
→ #2 (zerocopy) attacks (1)(4)(5); #3 (poll) attacks (2). Both behind flags.

## HOST REFACTOR #2 — zero-copy RX (DPUMESH_ZEROCOPY_RX) — CORRECT, throughput-neutral
Change: PE thread no longer does rx_slot_alloc(O(n) scan) + 8KB staging memcpy. It delivers the landing offset `pos` (in body_buf_slot); the consumer reads rx_dma_buffer[pos] directly, rx_free returns the admission credit. Removes per-request: 1 O(n) scan + 1 8KB copy from the PE thread, + 1 echo copy (echo reads landing→tx directly), + the 32MB staging buffer + rx_slot pool. Behind a flag (A/B). Added bench body-validation (echo returns request pattern verbatim → catches silent landing corruption).
Safety: DPA admission (rq_depth=num_slots, credit bumped in rx_free) caps outstanding reverse DMAs at num_slots = buffer capacity → landing never laps an unread/un-freed position. Historical "negative frame size near wrap" was from SHARING staging==landing WITHOUT this gate; reading landing directly WITH the gate + rx_free-after-read is safe.

| config | fair | hw3 | fail (6.3M+ reqs @ high load) |
|---|---|---|---|
| zerocopy=0 (baseline) | 104,021 | 128,786 | 0 |
| zerocopy=1 | 106,357 | 128,786 / 137K overload | **0** (validation ON, no corruption) |

**Verdict: CORRECT (0-fail validated), THROUGHPUT-NEUTRAL.** → The host RX *weight* (scan+copy) was NOT the binding cost at the ceiling; removing it = a leanness/core-efficiency + memory (−32MB/pod) win, not a throughput lever. The ceiling at 130K is the DPA EUs (co-limiting, 88% of 2-EU capacity) + the per-request WAKEUP (cond_signal/wait), which zero-copy does NOT touch.
**→ Next host lever = #3 (lock-free rx_queue + eliminate per-request wakeup under load).** That attacks the actual remaining host cost (the wakeup), which E8's "more conns hurts" already fingered.

## HOST REFACTOR #3 (wakeup) — echo spin-poll: REGRESSED (echo not the bottleneck)
DPUMESH_POLL_RX: dpumesh_dequeue adaptive spin-polls rx_count (no rx_cond block); PE thread stops per-request cond_signal(rx_cond). Tested with ECHO_THREADS=8.
Result: hw3 130K→125,635 (p99 316ms, was healthy), ceiling ~129K (was ~137K) → REGRESSION. Causes: (1) ECHO_THREADS 64→8 cut processing parallelism; (2) echo was NEVER the bottleneck (rx_queue≈0), so removing its wakeup can't raise the ceiling, and 8 spinners stole CPU. Reverted (POLL_RX=0, ECHO_THREADS=64).
**Finding: the dominant wakeup is the CLIENT (1050 blocking workers; E8 "more conns hurts"), not the echo.** The echo's wakeup is on a non-binding path. The poll machinery is kept (knob, default off) but the echo isn't where it pays off.
**Client wakeup = inherent to blocking thread-per-request.** Eliminating it needs an ASYNC client (few threads, many in-flight, spin-poll completions) — a consumer-model change. Transport offers the poll API; only async consumers benefit. Real blocking Thrift services pay the wakeup inherently. The DPA EUs also co-limit at 130K, so removing the wakeup may not raise the ceiling past ~137K anyway (core-efficiency, not necessarily throughput).

## CURRENT BEST CONFIG (deployed, verified)
SKIP_REQ_TXACK=1 + NUM_SLOTS=4096 + PE_ADAPTIVE=1 + BATCH_TXACK=1 + ZEROCOPY_RX=1 + POLL_RX=0, hw3:
~130K sustainable (vs 104K baseline, +25%), p50 ~6ms (halved), 0-fail. Host leaner: −1 O(n) scan, −1-2 8KB copies, −32MB staging/pod (zerocopy); transport yields idle cores (adaptive); echo msgs ~14× fewer (batch). Single-core still 104K (gain is multi-core).

## OPTION A — async (poll-based) client: kill the per-request CLIENT wakeup
Implements the missing async consumer that HOST REFACTOR #3 identified (the dominant wakeup is the client's 1050 blocking workers, not echo). Two parts, both behind DPUMESH_ASYNC_CLIENT (default off; bench-only env — Thrift transport keeps blocking wait_response, unchanged):
1. **Transport** (dpumesh_doca.c / dpumesh.h): new `dpumesh_poll_response(ctx, req_id, resp)` — non-blocking, lock-free fast path (`__atomic_load_n(&p->state)`; takes the per-pending mutex only when state≠0). Returns 0=arrived / 1=waiting / -1=abandoned. In async mode `rx_deliver_desc` STOPS the per-response `pthread_cond_signal(&p->cond)` (state=1 store is the only signal). Removes 1 futex + 2 context switches PER response.
2. **Bench** (bench_dpumesh.c): new `worker_fn_async` — a few generator threads (ASYNC_THREADS), each holding a window of `inflight` outstanding requests, paced by wrk2 scheduled time (coordinated-omission preserved: t0=scheduled), harvested by poll_response. Window = target_concurrency / ASYNC_THREADS, so RPS is apples-to-apples with the blocking model but the host runs 2 threads instead of ~1300. Per-slot wall-clock timeout = WAIT_TIMEOUT_MS (lost response can't wedge the window). Body validation kept.

### MEASURED A/B (DPA=2, SKIP+4096+adaptive+batch+zerocopy; async=1 uses ASYNC_THREADS=2)

| config | async=0 (blocking, ~1040-1300 thr) | async=1 (2 generator thr) | delta |
|---|---|---|---|
| **fair 1-core sustainable** | 103,580 · p50 14.2ms | **~125,000** · p50 1.4-2.3ms | **+21% RPS, 6-10× lower p50** |
| **fair 1-core ceiling (overload)** | ~104K | **~125-127K** | **+20-22%** |
| **hw3 3-core @130K offered** | 129,400 · p50 6.2ms · p99 14.8ms | 129,615 · **p50 3.1ms · p99 7.0ms** | =RPS, **2× lower p50/p99** |
| **hw3 3-core ceiling (overload)** | 136,823 | 135,300-135,800 | = (neutral) |

0-fail everywhere; back-to-back fair 130K rerun = 126,966 (p50 1.58ms) → **no slot leak**. async=0 path is code-identical to prior best → reproduces the documented baseline exactly (non-regressing). 2 generator threads sustaining 129K proves the window works (blocking 2 thr would give ~2/RTT ≈ 333 RPS).

### Verdict — the CLIENT wakeup was a real host-CPU cost, but only THE cap below ~125K (direct experiment, not elimination)
1. **1-core: wakeup WAS the throughput cap.** Removing it lifts the 1-core ceiling **104K → 125K (+21%)** and cuts p50 up to **10×**. This DIRECTLY confirms HOST REFACTOR #3's hypothesis (dominant wakeup = client's blocking workers) — by A/B, not by elimination.
2. **3-core: wakeup is NOT the throughput cap.** async ceiling ~135K ≈ blocking ~137K → at 3 cores the binding constraint is the **DPA 2-EU** (Q3), which async cannot lift. But p50 still halves (the wakeup was a *latency* component even where not throughput-binding).
3. **Leanness achieved (user goal "transport must use minimal host cores"):** 1-core async (**125K**) ≈ 3-core blocking (**129K**). ~126K reachable with **1 core + 2 threads** instead of 3 cores + ~1300 threads (~600× fewer threads). The host transport footprint collapses.

**Cap map after Option A:** below ~125K → host wakeup/CPU was binding (async removes it). ~125K-137K → DPA 2-EU is binding (only EU-sharding / Option B raises it). → Option A delivered leanness + latency; the remaining THROUGHPUT lever above 137K is Option B (EU-sharding). NB: DPUMESH_DPA_THREADS defaults to 1 in test-bench.sh → a deploy WITHOUT it pins single-EU = 74K (Q3 N=1 ceiling); 130K config requires DPA_THREADS=2.

## Q4 — is the ~580µs DPU hop the reverse-EU keepalive wakeup? NO (keepalive 1ms→100µs = negative)
Hypothesis: ARM posts the reverse desc (dpu_enqueue_reverse_dma, dma->valid=1) as a SILENT write; the reverse EU only re-wakes on the 1 kHz keepalive (DPA_MSG_WAKE) after it yields → up-to-1ms handoff = the hop. Proposed fix: on-demand kick (dmesh_doca_dpa_msgq_send_try to dst EU(dst_pod%N) right after valid=1 — mechanism already exists).
**Cheap proxy test (zero code): DPUMESH_KEEPALIVE_US 1000→100 (10 kHz), async=1, hw3.**
| metric | keepalive=1000 | keepalive=100 |
|---|---|---|
| hw3 ceiling (overload) | ~135K | ~134K (=) |
| 130K p50 | 3.1ms | 3.5ms (=) |
| **130K DPU hop avg** | (blocking 580µs) | **1009µs** (async, higher in-flight) |
**Verdict: REFUTED.** At keepalive=100 the max wakeup wait is 100µs, yet the hop is still ~1009µs → ≥900µs of the hop is ARM-bridge SERVICE/QUEUING, not wakeup latency. Ceiling unchanged (the EU is hot at saturation, so keepalive is irrelevant there — as predicted). → the on-demand kick is NOT worth building (it would save <100µs of a ~1ms hop, 0 throughput). The ~135K ceiling and the ~1ms hop are the **single ARM bridge's per-request service rate** (route + comp_queue + comch egress; 135K × 2 ARM-crossings = 270K ARM ops/s), NOT the EU wakeup. Reverted keepalive to default; the per-request "fat" worth removing is the ARM-path software cost, not the wakeup (see cleanup).
**Direct experiments that raise the ceiling: NONE so far** (async=no, keepalive=no, N=4=no, ARM-shard=no[prior]). The ceiling is robustly the DPU per-request service rate through the single ARM bridge.

## Q5 — pure_dma's DPA scales but the chain doesn't: WHY (what "DPA-bound" actually means) [workflow w56lrbvhz, code-verified]
Side-by-side read of `/home/jukebox/test_dma/pure_dma/` vs the chain DPA:

**pure_dma = embarrassingly-parallel, share-nothing copy farm:**
- Each EU: own thread, own comch channel (1p/1c), own completion queue, own buffer slice. **No host, no ARM, no admission gate on the per-op path** (`test_dma/pure_dma/device/dpa_kernel.c:100-134`).
- **1 dma_copy per op** (the copy carries its own completion immediate). EU self-drives, gates only on its OWN consumer credit.
- → adding EUs adds fully-independent issue engines → **linear**: N=1 555K → N=2 1.07M (1.93×) → N=4 1.74-1.80M (3.2×), until the physical DMA-engine op-rate (~1.5-1.6M) at N=8.

**chain DPA = request/response pipeline forced through a single ARM bridge:**
- EU = `pod_id % N` (static; `dpa.c:1091,1278`) → **2-pod = exactly 2 EUs for ANY N** (N=4: 10%4=2, 11%4=3 → still 2; rest idle).
- One cross-pod message's **forward runs on EU(src%N), reverse on EU(dst%N) = DIFFERENT EUs**; they never talk directly — every completion goes UP to a **single ARM** (one `consumer_pe` + one `comp_queue`) which routes + posts the reverse desc, then the reverse EU picks it up. **ARM crossed 2×/request.**

**Two reasons more EUs don't help a 2-pod chain (both code-true):** (a) `pod%N` caps active EUs at 2; (b) the 2 EUs share one serial ARM bridge per request → N=2 gives **1.76× not 2×** (Amdahl on the serial bridge).

**Is the EU itself maxed?** Use the right reference: the chain's 2 EUs do 416K dma_copy/s @104K and ~540K @135K (RPS×4); M2-N=2 (same 2 EUs doing drain+forward but no full chain) = **626K**. So EU util = 416/626 = **66% @104K → 540/626 = 86% @135K** (matches Q3's "88% of 2-EU capacity"). The looser pure-copy 1.07M reference (no completion handling) overstates headroom — vs M2 the EU is **near its realistic chain-work capacity at the ceiling**, with the residual 14-32% idle being the EU→ARM→EU round-trip wait (shrinks as load rises). → **"DPA-bound" = the DPU's per-request round-trip *service rate* through the single ARM bridge (EU near M2 capacity + ARM-bridge cadence), NOT the DMA engine maxed and NOT EU count.** pure_dma has none of this structure → it scales.

## Option B (EU-sharding) status: FORECLOSED — corrects the earlier "feasible / only lever" framing
Earlier entries (the line-10 "Code-certain" note + Q3 line 230/232 + Option-A cap-map) framed EU-sharding — *host posts K fwd rings/pod; ARM round-robins reverse to K tx_rings* — as "feasible, the ONLY way to add EU capacity for 2 pods." **Re-reading `multithread_unified_plan.md` §5.0, that is FORECLOSED, and on code grounds (not elimination):**
- Sharding one pod across K EUs → K EUs write that pod's **tx_ring → breaks single-producer** (`ring.c`) and `dpa_sent_count[e][r]` **single-writer**.
- The cross-EU completion fan-out (an EU's completion reaching a consumer it doesn't own) is a **×N vs ×N² branch needing SDK `max_num_consumers>1`** (`dpa.c:470,477`), which is **=1** → no SDK support.
- 2-pod is a **HARD target constraint** (§8) → active EU permanently 2 → even if sharded, no benefit.
→ So **Option B is blocked by the SDK gate + the 2-pod target**, NOT a near-term lever. (Supersedes the line-10 "foreclosed was elimination, not code-true" — the re-read shows it IS code-true.) The plan's actual forward direction is **not** sharding/more-EUs but **reducing per-request handoff latency on the fixed 2-EU+1-ARM** (plan Levers 1-3: per-request stamp measurement → shorten reverse-desc post → `find_pod_by_id` O(n)→O(1)). NB those are LATENCY levers; per Q4 the keepalive/wake is NOT the throughput cap, so even these are p50/p99 wins, not ceiling wins.

## DPU (ARM) multithreading — current implementation state (+ cleanup decision: KEEP)
Two ORTHOGONAL multithreading axes (commonly conflated):
- **DPA (data plane), `DPUMESH_DPA_THREADS=N`** — N EU threads, share-nothing, EU=`pod%N`. **Currently N=2 = active & working** (2-pod → 2 EUs). This is the "DPA multithreading" (tested N=1/2/4).
- **DPU (ARM control plane), `DPUMESH_SPLIT_SEND` + `DPUMESH_DRAIN_SHARDS`** — splits the ARM completion-processing. **Default = OFF = single ARM thread.** 4 modes (`object.h:140-153`): OFF(0,default,single thread) / SENDS(1: A=drain+route+reverse, B=comch sends, via `send_spsc`) / REBAL(2: A=drain only, B=full work, via `work_spsc`) / **SHARD(3: N-way — A routes by effdst%N → per-EU `shard_work[k]` → N worker threads (each sole writer of its EU's tx_ring) → per-EU `shard_send[k]` → 1 SENDER → cc_server)**; `DRAIN_SHARDS=M` splits the single `consumer_pe` drain into M independent doca_pe drains (`dpu_worker.c:998-1008,1026-1068`).

**Currently deployed = DPA_THREADS=2 + SPLIT_OFF + DRAIN=1 → 2 EUs (parallel data plane) + single ARM thread (serial control plane).** `consumer_pe_shard[0]=consumer_pe`, M=1 byte-identical to single drain (`object.h:997`).

**Status:** the SPLIT/SHARD/DRAIN machinery is **fully implemented (commit 332dcfd53) + correctness-proven (0-fail×30)** but **throughput-neutral for the 2-pod chain** (E2 SPLIT_SHARD=3 flat = baseline; ARM was never the bottleneck, §4.1/§4.4; and 2-pod → only 2 EUs so nothing to scale). Per plan §4.4/§5.0 it is **preserved as a diagnostic toggle + future-multi-pod scaffolding**, NOT dead code.
**CLEANUP DECISION:** do **NOT** delete SPLIT/DRAIN/SHARD (nor CASE_INGRESS — `project_recv_pool_coupling` "KEPT" it deliberately). My earlier "delete ~600 lines of 0-gain machinery" was wrong: 0-gain is a *2-pod artifact* (pod%N), and the plan keeps the machinery by design. Cleanup, if done, is **readability-only** (stale comments, misnamed macro, clear toggle grouping) with **zero functional removal**. KEEP all leanness knobs too (ZEROCOPY/ADAPTIVE/SKIP/BATCH/ASYNC/TRACE) — the keep-criterion is "does it make the runtime lighter," not "did it raise throughput."

---

# Phase 1 cleanup (2026-06-09) — fix config (no env selection) + compose SHARD with host wins

Goal: remove the runtime SELECTION (env toggles) and bake each feature to its
enabled/fixed state. KEEP all feature code. Assume "neutral" verdicts may have been
masked by a different bottleneck → enable everything and RE-MEASURE.

## Baked ON (env selection removed)
- host transport-internal (always ON): ZEROCOPY_RX, PE_ADAPTIVE.
- host consumer model (via dpumesh_config_t, not env): bench → async_client=1, echo →
  poll_rx=1; Thrift keeps blocking (config default 0) — async is unusable by sync Thrift.
- DPU (always ON): SKIP_REQ_TXACK, BATCH_TXACK, DPA_AFFINITY.
- DPU control plane fixed to **SPLIT_SHARD** (multi-ARM: thread A route → N workers →
  SENDER). **DRAIN_SHARDS=1** fixed for correctness (cross-pod echo: fwd EU≠rev EU, a
  single drain keeps each shard_work single-producer).
- NUM_SLOTS default 2048 → **4096** (matches DPU_BUFFER_SIZE=32MB invariant).
- Kept as topology/sizing/measurement env (not feature toggles): DPUMESH_DPA_THREADS(=2),
  DPUMESH_NUM_SLOTS, DPUMESH_KEEPALIVE_US, DPUMESH_TRACE, DPUMESH_LOG_LEVEL, ECHO_THREADS,
  ASYNC_THREADS.

## New integration — SHARD now honors SKIP + BATCH
SHARD's send path (send_via_spsc) predated SKIP/BATCH and ignored both (it always pushed
DMA_COMPLETION + TX_ACK per request). Composed them so multi-ARM + host wins run together:
- SKIP: SHARD worker gates the TX_ACK push with `keep_ack = echo_mode || (flags&OP_RESPONSE)`
  — request forwards send no TX_ACK (client frees on response). (dpu_worker.c process_rev_notify_entry)
- BATCH: the SENDER (drain_send_spsc) routes each TX_ACK through batch_or_send_tx_ack
  (per-src-pod coalesce) instead of an immediate send; DMA_COMPLETION still sent inline.
  SENDER tail-flushes partial batches every 1 ms. The SENDER is the single owner of the
  per-pod batch (main-loop A's flush is disabled under SHARD → no race).

## Files
dpu_worker.c (SKIP/BATCH baked; SHARD fixed; SKIP gate + SENDER batching + tail flush);
dpa.c (affinity baked); dpumesh_doca.c + dpumesh.h (ZEROCOPY/PE_ADAPTIVE baked, poll_rx/
async_client via config, NUM_SLOTS_DEFAULT=4096); dpumesh_common.h (stale comment);
bench_dpumesh.c (async-only, cfg.async_client=1); echo_dpumesh.c (cfg.poll_rx=1);
test-bench.sh (dropped baked-knob env; DPA_THREADS:-2, NUM_SLOTS:-4096).

## Measured (2026-06-09) — build OK (DPU recompiled 13 objs; host OK), all toolchains compile

### hw3 (3-core) — 0-fail, >= prior best + ~2x lower p50
| target | achieved | p50 | p99 | ok/fail |
|---|---|---|---|---|
| 50000  | 49,731  | 1.46ms | 2.90ms | 500000/0 |
| 105000 | 104,427 | 1.29ms | 3.66ms | 1.05M/0 |
| 130000 | 129,295 | 3.07ms | 3.44ms | 1.30M/0 |
| 137000 | 136,245 | 4.99ms | 5.39ms | 1.37M/0 (sustainable ceiling) |
| 145000 | 142,778 | 55.8ms | 106ms  | 1.45M/0 (overload onset) |
| 150000 | 138,172 | 372ms  | 795ms  | overload |
| 160000 | 146,041 | 373ms  | 870ms  | past-knee |

back-to-back hw3: 130K->129,277 / 130K->129,285 / 30K->29,838, ALL 0-fail -> no slot leak.
DPU log: no ERR / no flood (-l 40). Sustainable ~137K (vs prior ~130K); p50 at 130K halved
(3.07ms vs ~6ms blocking). SHARD+SKIP+BATCH+async compose correctly.

### 1-core fair — REGRESSED by echo poll_rx=1
| target | achieved | p50 | ok/fail |
|---|---|---|---|
| 105000 | 54,312 | 3.66 s | 812355/0 |
| 125000 | 58,637 | 4.00 s | 877027/1 |

Prior async 1-core was ~125K. hw3 fine + only 1-core broken => host-core-sensitive => NOT
SHARD (DPU, identical in both profiles). Only new host variable = echo poll_rx=1: 64 echo
threads spin-poll -> thrash 1 core (the "POLL_RX regressed" finding, NOT masked — genuinely
bad at low echo core count). Documented best had echo poll_rx OFF. -> revert echo poll_rx,
re-measure 1-core (below).

### CONFIRMED: echo poll_rx OFF (rebuild) — 1-core recovers, hw3 unchanged
1-core fair: 105K -> 104,427 (p50 1.43ms, 0-fail) [was 54K @ p50 3.6s]; 125K -> 117,320
(p50 272ms, overload). hw3: 130K -> 129,282 (0-fail); 137K -> 134,904 (knee this run; first
sweep had 137K healthy = run-to-run boundary). => echo poll_rx=1 was the sole 1-core culprit;
reverting fixes 1-core and is neutral at hw3. poll_rx feature kept in transport, off for echo.

## Phase 1 verdict
Final config (all baked, no env selection): SHARD + SKIP + BATCH + AFFINITY (DPU),
ZEROCOPY + PE_ADAPTIVE (host always), async (bench), poll_rx OFF (echo), NUM_SLOTS=4096,
DPA_THREADS=2.
- hw3 sustainable ~130K (knee ~135K), 0-fail, back-to-back leak-safe, p50 ~3ms (vs ~6ms
  blocking) = LATENCY win. Throughput ceiling unchanged vs prior OFF+host-wins (~130-137K).
- 1-core fair ~105K clean (p50 1.43ms); slightly below prior async-1-core ~125K because
  SHARD's longer per-request DPU pipeline (A->workerK->SENDER + batch) costs 1-core async
  throughput where the host is the binding constraint. Neutral at hw3.
- SHARD (multi-ARM) did NOT raise the 2-pod ceiling -> confirms the 2-pod = 2-EU structural
  limit (pod%N). Higher throughput needs >2 pods (more active EUs), which costs host cores.
  Net Phase-1 gain = latency (async) + correctness composition; throughput ceiling flat.

## Phase 1 cleanup (step 2: comments + dead-path) — verified non-regressing (2026-06-09)
- Comment cleanup: 8-agent workflow trimmed history/rationale/dead-env comments across 39 files.
  Verified comment-ONLY (comment-stripped diff vs pre-cleanup snapshot = 0 code changes).
- Host RX staging path removed (zerocopy baked-on made it dead): rx_buffer / rx_slot_bitmap /
  rx_slot_lock / rx_slot_alloc removed; rx_buf/rx_free/rx_reclaim landing-only; −32MB/pod.
  Also removed now-dead fields pe_adaptive/zerocopy_rx (always-on baked into pe_progress_fn /
  process_rx_dma_entry). poll_rx/async_client kept (consumer model via config).
- BUG FIX (latent): dpumesh_cancel_pending state==1 freed rx_slot_bitmap[body_buf_slot] where
  body_buf_slot is the zerocopy landing OFFSET (0..32MB) vs a 4096-byte bitmap → heap OOB.
  Now rx_credit_return(). (Not hit by the 0-fail bench — only on cancel-after-arrival.)
- Async review (separate): sync Thrift can't get leanness at the stub level (blocking read,
  1-in-flight/instance). Recommended path = gateway async (raw C API, 0 Thrift change, port
  worker_fn_async window+poll); cob_style+TDpumeshAsyncChannel only for inter-service callers.
- Verify (deploy3, all toolchains compile): hw3 130K->129,291 (p50 2.34ms, 0-fail);
  1-core 105K->104,424 (p50 1.39ms, 0-fail); back-to-back 130K/30K 0-fail (no leak). Non-regressing.

## Phase 2 lightening (2026-06-09) — per-RTT compute, honest verdict
Investigation (workflow): "completion 제거" is NOT a lever —
- immediate completion comch_dma_comp_msg (16B) = routing-essential EU→ARM channel, can't remove
  (already 1 WQE BB).
- producer completion drain = already batched (every 8 iter), not per-op.
- request TX_ACK = already skipped (g_skip_req_txack=1); echo/response amortized via BATCH.
- DPA EU per-op already stripped (bench.md §3.2). Cap = closed-loop round-trip μ, not EU compute.
Real μ levers: host→host (FORECLOSED by L7-proxy design) or more active EUs (>2 pods → host cores).

Applied (safe leanness, code-lighter, throughput-neutral by design):
- C2: dpu_enqueue_reverse_dma takes scalars (req_id/dst/src/flags) instead of a 64B sw_descriptor_t
  → removes per-forward memset(64) + intermediate struct copy on the ARM worker path.
- H5: register_pending drops the per-request memset(&p->desc,0,64) (desc fully overwritten before
  state=1, only read at state==1).
Deferred (medium risk / ~0 throughput payoff since ARM not the bottleneck): C1 (send-buffer pool
vs per-send malloc/memcpy), H2 (merge register_pending+attach_tx lock), H1 (tx_alloc hint cursor).
Rejected: H4a (enqueue mfence→release — device-visibility subtle, gain negligible), WORKER inline
send (cc_server single-submitter), TX_ACK full removal, per-thread ring (foreclosed).

Verify (deploy4, all toolchains compile): hw3 130K->129,292 (0-fail), 137K->136,254 (0-fail),
back-to-back 130K/30K 0-fail (no leak). Throughput flat ~130-137K (expected — leanness, not a
ceiling mover). Confirms per-RTT lightening cannot raise the 2-pod closed-loop ceiling.

## 4-pod (2 echo pairs, 4 active EUs) — DOES scale past the 2-pod ceiling (2026-06-09)
Config: DPUMESH_DPA_THREADS=4 deploy (10%4=2,11%4=3,12%4=0,13%4=1 → 4 distinct EUs), SHARD baked
(4 workers + 1 SENDER + DRAIN=1 single drain), SKIP+BATCH+async+zerocopy. fair pin: pair1 cores
0,1 / pair2 cores 4,5 (each pod 1 host core). Single-pair anchor (fair 1-core) = 104,429.

| offered (RPS/pair ×2) | pair1 | pair2 | AGGREGATE | p99 | ok/fail |
|---|---|---|---|---|---|
| 120K (60K) | 59,673 | 59,675 | **119,347** | ~3.2ms | 1.2M/0 |
| 160K (80K) | 79,557 | 79,558 | **159,115** | 4.4-9.1ms | 1.6M/0 |
| 210K (105K) | 92,469 | 93,292 | **185,761** | overload (p50 0.6s) | 2.1M/0 |

**Verdict: 4-pod scales. Aggregate ~160K healthy / ~186K knee vs single-pair ~105K** — the 2-pod
"ceiling" is per-pair, NOT a global wall. This OVERTURNS [[project_shard_bottleneck_consumer_pe]]
("4-pod 2 pairs halve each other ~110K") — that was BEFORE the host fixes (SKIP+async) + SHARD
multi-ARM + 4 EUs. With the current baked config, 2 pairs scale.

Not perfectly linear: per-pair drops 105K(solo) → ~80-93K(paired) = ~12-24% sharing loss (shared:
single ARM drain DRAIN=1 + single SENDER + single cc_server + host↔DPU PCIe/comch op-rate). So
adding pods buys throughput sub-linearly: 2 cores→105K, 4 cores→160-186K (~40-46K/core vs 52K/core
solo).

ANSWER to "is adding pods worth the host CPU": YES — throughput scales ~1.5-1.8× for 2× pods/cores;
the per-RTT closed-loop cap is per-pair-parallelizable, not a shared hard wall (anymore). Further
scaling (>4 pods / >4 EUs) limited by the shared ARM drain+SENDER+cc_server (next lever if needed:
shard the SENDER / DRAIN_SHARDS>1 with group-affine routing — currently DRAIN=1 for cross-echo).

## Q2 — WHY does 4-pod throughput rise? (attribution, controlled) (2026-06-09)
Isolated EU count by running 4-pod at DPA_THREADS=2 (4 pods share 2 EUs: 10,12→EU0; 11,13→EU1)
vs DPA_THREADS=4 (4 distinct EUs). Host cores (4) + pair count (2) held equal.

| config | 120K off | 160K off | knee | healthy aggregate |
|---|---|---|---|---|
| 4-pod, 2 EU | 119,350 (0-fail) | 143,406 (p99 1.1s OVERLOAD) | ~144K | ~130K |
| 4-pod, 4 EU | 119,347 (0-fail) | 159,115 (p99 9ms HEALTHY) | ~186K | ~159K |
| 1 pair, 2 EU (fair 1c) | — | — | ~105K | ~105K |

Layered attribution (both host cores AND DPA EUs contribute; ARM does NOT):
- **host cores (= more src/dst pods)**: a single fair pair (1 core/app) is HOST-bound at 105K — it
  under-drives even 2 EUs. Adding the 2nd pair (more host cores) drives the SAME 2 EUs to ~130K
  healthy (+25K at fixed EU count). So adding pods adds host cores that saturate the EUs.
- **DPA threads (EU count)**: 2 EUs cap ~144K knee; 4 EUs reach ~186K knee (+42K). More EUs = more
  DMA-issue capacity, binding only after host cores have saturated the existing EUs.
- **DPU ARM cores (SHARD workers): NOT the cause** — cq_depth=0 (ARM starved); ARM was never the bottleneck.

VERDICT: 4-pod is faster because (1) each added pod brings a host core that pushes the EUs past the
single-fair-pair host limit, and (2) added pods map to added EUs (pod%N) giving more DMA capacity.
It is host-core + DPA-EU bound, NOT ARM-bound. To scale: add pods (host cores) until EUs saturate,
then add EUs — both cost host cores (1 core/pod). Shared ARM drain/SENDER/cc_server is the eventual
ceiling above ~186K.

## Q3/Q4 — completion rethink (workflow + adversarial verify vs DOCA headers) (2026-06-09)
Q3 (send reverse completion DPA->host directly, skip ARM relay): **NOT possible** in the current
binding. DOCA: producer sends to a consumer_id on the SAME comch connection (doca_comch_producer.h
:20-23); the DPA producer msgq is anchored to the DPU device (max_num_consumers=1=ARM, dpa.c:457);
the host datapath consumer is on a different connection. The EU immediate can only reach the DPU ARM.
The ARM relay is a 16B metadata endpoint-conversion ONLY — the body already DMA'd straight to host RX
in hop1 (dpa_kernel.c:336). A NEW host<->DPA msgq is theoretically allowed but device-match for a
host-side consumer is UNVERIFIED + needs per-dst-host msgq. The TX_ACK-skip analogy does NOT transfer:
TX_ACK was skippable (response arrival = alternative signal); the reverse completion IS the arrival
signal, no alternative + no DPA->host path.
Q4 (do we need completion?): forward completion = irreducible (carries pos/src_pod_id that exist only
in DPA per-EU state; a flag just moves the PCIe write + adds poll + loses HW copy-then-imm ordering).
reverse completion-as-polled-flag = possible in principle (data already in host RX) but needs re-adding
an in-band metadata trailer (undo zerocopy) + body-before-flag fence + wrap redesign; net win unproven.
Bottom line: completion is needed; the only ARM-relay removal (reverse) is SDK-blocked AND targets
~2.8% of RTT (host-transport-bound ceiling) — not a throughput lever. Real lever = more pods (Q2/4-pod).

## Q2.2 — EU-sharding (2 pods, >2 EUs) feasibility (workflow + adversarial verify) (2026-06-09)
Re-examined as K-rings-per-pod (each EU owns its own ring), NOT the old 1-ring-per-pod.
VERDICT: mechanism foreclosure OVERTURNED — single-producer (ring.c: 1 ring=1p/1c, not 1 pod=1 ring),
admission [eu][ring] (already isolated), max_num_consumers=1 (per-MsgQ, already worked around by
per-EU channels), 2-pod-HARD (ring count independent of pod count, MAX_DPA_RINGS=8) all preserved/
non-binding under K-rings. One real obstacle: reverse ADD_REV_RING keys on pod_id (dpa_kernel.c:84-90)
→ must re-key to (pod_id, ring_idx).
BUT performance gate HOLDS: rings alone ~0 gain. Real gates = (1) host 1-core feed/RX ~105K (single
ring_lock + single pe_progress_fn) — K EUs starve; (2) single ARM bridge ~103K (next ceiling).
→ Q2.1 (host-library lighten) + host RX multicore are PREREQUISITES; then EU-sharding rings; then ARM
K-way. Change scope: host dma_ring[K]/ring_lock[K]/enqueue-select + DPA per-pod K rings + reverse-ADD
re-key + ARM reverse K-ring single-writer routing (the hard part). Coupled multi-layer build.

## Q2.1 — 1-core attribution (TRACE) + SHARD reverted to OFF (measured) (2026-06-09)
Measured 1-core fair host/DPU split (DPA=2, TRACE=1, -l 50):
- SHARD (baked): 1-core 105K, DPU hop avg 416µs (max 1343), cq_depth=0, recv 420K/s.
- OFF (reverted): 1-core **120K** (p50 1.36ms healthy; 125K overloads), DPU hop avg **171µs**.
- Prior OFF reference (scale_log E5): hop 208µs @105K. SHARD ~2.4x the hop.

Attribution: at 1-core the gate is HOST (cq_depth=0 → DPU has headroom; hw3 reaches 137K). But SHARD
inflated the DPU pipeline latency (A→worker→SENDER) ~2.4x, and with a fixed async window that cut
1-core throughput 120→105K. SHARD has NO throughput upside at 2 pods (neutral at hw3) → it was a pure
latency/1-core cost. => Per user framing (Q2.1: library-core can't be solved by adding cores), the
biggest 1-core lever was NOT a host micro-op but turning SHARD OFF.

ACTION: split_send baked SPLIT_SHARD → SPLIT_OFF (dpu_worker.c). SHARD/SENDER/shard machinery KEPT
(dormant) for future EU-sharding (single ARM has headroom: cq_depth=0, so EU-sharding can use OFF+K-rings
without SHARD until the single ARM saturates).
Verify (OFF, deploy): 1-core 120K (was 105K, +14%); hop 171µs (was 416µs, -59%); hw3 130K→129,282 /
137K→136,244 (0-fail, unchanged); back-to-back 130K/30K 0-fail (no leak). Net win, simpler, no 2-pod loss.
Residual 1-core gap (120K → EU ceiling ~137K) = host CPU (library) → next: host-library lightening (Q2.1).

## Q2.2 — EU-sharding decisive experiment: does EU count cap throughput? (2026-06-09)
Positive-evidence test (per project_bench_elimination_unreliable: demand positive evidence). Resolve the
memory conflict (4pod_scales "~160K scales" vs shard_bottleneck "pairs halve"). 4-pod, SPLIT_OFF baseline,
fair 1-core/app (pair1 cores 0,1; pair2 cores 4,5 — HOST cores constant = 4 client procs). Only variable =
DPA EU count. Mapping pod_id % N:  N=2 → 10,11,12,13 = EU 0,1,0,1 (2 pairs SHARE 2 EUs);
N=4 → EU 2,3,0,1 (2 pairs use 4 DISJOINT EUs). size=8192, 10s.

### DPA_THREADS=2 (2 pairs share 2 EUs):
| offered (RPS/pair x2) | aggregate achieved | fail | p99 |
|---|---|---|---|
| 100K (50K) | 99,460  | 0 | 3.2/3.4 ms |
| 140K (70K) | 139,235 | 0 | 4.5/3.8 ms |
| 180K (90K) | 143,342 (SAT) | 0 | **2.5 s** (queue blowup; achieved capped ~71.7K/pair) |

=> 2 EUs cap 4-pod aggregate at **~140K** — identical to 2-pod hw3 ceiling (137K). The EU capacity is
shared across pods: pod count does NOT add throughput when EU count is fixed. Strong positive evidence
that DPA EU count (not pod count, not host cores here) is the binding throughput resource at the chain
ceiling. (DPA_THREADS=4 measurement next to confirm 4 EUs ~= 2x.)

### DPA_THREADS=4 (2 pairs use 4 DISJOINT EUs), SPLIT_OFF, fair 1-core/app:
(NOTE: pair2 fails entirely if probed immediately after `run_4pod up` — DPU pod-register race;
re-run after a few s and both pairs are healthy. First 90K/pair probe with pair2=fail/1800 was this race.)
| offered (RPS/pair x2) | aggregate achieved | fail | p99 |
|---|---|---|---|
| 60K (30K)  | 59,676  | 0 | 3.2 ms |
| 140K (70K) | 139,237 | 0 | 4.0/3.1 ms |
| 180K (90K) | **179,004** | 0 | 11.8/17.3 ms (knee) |
| 220K (110K)| 178,059 (SAT) | 0 | **2.26 s** (achieved capped ~89K/pair) |
| 260K (130K)| 176,189 (SAT) | 0 | **4.66 s** |

### VERDICT (positive evidence, resolves 4pod_scales vs shard_bottleneck conflict):
- 2 EU → 4-pod ceiling ~140K; 4 EU → ~178K. **EU count IS a binding throughput resource** (+27% for 2x
  EUs, with host cores=4 and pod count=4 held CONSTANT — clean isolation). Memory conflict resolved:
  4pod_scales (scales) correct; shard_bottleneck (pairs fully halve) was an over-read.
- BUT sub-linear: per-EU throughput 70K (@2EU) → 44.5K (@4EU). Doubling EUs gives +27%, not +100%.
  A shared resource BELOW the EUs caps absolute aggregate ~178K. Not the DMA engine (178K x4 dma = 712K
  ops/s << 1.6M N=8 ceiling) → likely PCIe body BW or host↔DPU comch op-rate. (Confirming needs >4 EUs =
  >4 pods, not available in 4-pod harness; DPA=8 with 4 pods still uses only 4 EUs: 10,11,12,13 %8=2,3,4,5.)
- Per-pair under contention: single-pair fair ~120K → 89K/pair when a 2nd pair shares (even on DISJOINT
  EUs) → the shared sub-EU resource, not the EUs, is what pairs contend for past ~140K.

### Q2 ANSWER ("why does 4-pod throughput rise? dpu core / dpa thread / src,dst pod?"):
It is the **DPA EU count (= "dpa thread")**, proven by isolation: holding pods=4 and host-cores=4 fixed and
varying ONLY EUs 2→4 lifts 140K→178K. NOT pod count (4-pod@2EU = 2-pod@2EU = 140K), NOT host cores
(constant). The original "4-pod scales" was really "more pods activated more EUs" (pod_id % num_dpa_threads).

### EU-SHARDING cost/benefit (for the 2-pod case the user wants to scale):
EU-sharding (1 pair's traffic split across K EUs via K-rings/pod) would lift the 2-pod pair from ~137K
toward the ~178K 4-EU shared-resource ceiling = **~+20-30%, NOT linear 2x** (the sub-EU shared resource
caps it). Build = coupled host (K-ring round-robin post) + DPA (K rings/pod across K EUs) + ARM (K-way
reverse routing). Decision: modest gain for a large coupled build; it is the only remaining 2-pod
throughput lever (host-micro exhausted per Q2.1). Above ~178K needs attacking the sub-EU shared resource
(PCIe body BW / comch op-rate), not more EUs.

### 2-pod EU-sharding BEFORE baseline (DPA=4, no sharding, pods 10,11 -> EU 2,3 = 2 EU), hw3:
| RPS | p50 | p99 | state |
|---|---|---|---|
| 120K | 1.39 ms | 2.73 ms | healthy |
| 140K | 253 ms  | 495 ms  | SATURATED |
| 160K | 905 ms  | 1.79 s  | SAT |
=> 2-pod / 2-EU ceiling ~130K (120K healthy). EU-sharding target: 1 pair using 4 EUs (K=2 rings/pod)
toward the measured 4-EU ceiling ~178K. This is the "before" for the EU-sharding gain comparison.

## Q2.3 — EU-sharding BUILT (K-rings/pod). K=1 non-regressive verify (2026-06-09)
Implemented K-rings-per-pod EU-sharding (DPUMESH_RINGS_PER_POD, default 1): host K forward rings +
round-robin post + per-ring credit (ring_idx = pos/region_size); DPU setup_pod_dma loops K rings across
K EUs k_j=(pod_id*K+j)%N, partitions DPU staging + host RX into K disjoint regions, region_off makes
completion pos absolute (dpa_ring_info._pad_credit -> region_off, no ABI size change); ARM tx_rings[K]
round-robin reverse. DPA kernel unchanged except comp.pos += region_off (2 lines). Host + DPU compile
clean (recompiled 13 C objects).
K=1 DPA=4 2-pod hw3 (bit-identical check vs pre-sharding DPA=4 baseline ~120K):
| RPS | achieved | p50 | p99 | state |
|---|---|---|---|---|
| 100K | 99,457  | 1.35 ms | 4.53 ms | healthy |
| 120K | 119,347 | 1.55 ms | 4.26 ms | healthy |
| 130K | 129,279 | 4.29 ms | 10.9 ms | knee |
=> K=1 reproduces the baseline exactly -> refactor is non-regressive. Next: K=2 DPA=4 (pair drives 4 EUs).

## Q2.4 — EU-sharding RESULT: K=2 lifts the 2-pod ceiling (2026-06-09)
K=2 DPA=4: the SAME 2 pods (10,11) now each shard across 2 EUs -> the pair drives 4 EUs
(10 -> EU 0,1; 11 -> EU 2,3). 2-pod hw3, size=8192, 10s:
| RPS | K=1 (2 EU) | K=2 (4 EU) | note |
|---|---|---|---|
| 130K | 129K p50 4.3ms (knee) | 129K p50 **1.71ms** | K=2 same rate, far lower latency = EU headroom |
| 150K | (saturated) | 149K p50 2.16ms healthy | K=1 saturates by 140K |
| 160K | — | 159K p50 2.45ms 0-fail (peak, borderline: 1 of 2 runs saturated) |
| 165K | — | saturated p50 675ms | knee |

RESULT: 2-pod healthy ceiling **~130K (K=1) -> ~150-160K (K=2) = +15-23%**. EU-sharding works: making a
2-pod pair use 4 EUs raises throughput, exactly the user's ask ("even with 2 pods, more DPA threads ->
more throughput"). 0-fail throughout; back-to-back runs (165K saturate -> 160K healthy) show no slot leak.

Gap to the 4-pod 4-EU ceiling (178K): the 2-pod pair feeds 4 EUs from only 2 host processes (1 PE-RX
thread each) + a single ARM, vs 4 host procs at 4-pod. So the residual cap is the host PE-RX feed / ARM
(plan risk R5), not the EUs. Pushing past ~160K on 2 pods needs a leaner/multi-threaded host RX feed.

Knobs: DPUMESH_RINGS_PER_POD=K (default 1=legacy), requires DPUMESH_DPA_THREADS>=K. K=1 verified
bit-identical (Q2.3). Build: host (K rings + rr post + per-ring credit), DPU setup_pod_dma (K rings/EUs +
region partition), ARM tx_rings[K] rr, kernel comp.pos += region_off. dpa_ring_info ABI unchanged
(_pad_credit repurposed). Deployed state: DPA=4 K=2.

## Q2.5 — Where is the 2-pod K=2 cap? HOST CPU measurement (2026-06-09)
K=2 DPA=4, hw3, 150K (149.5K achieved, 0-fail, p50 2.0ms = healthy). mpstat per-core (bench host10 pinned
0,4,6; echo host11 pinned 1,5,7):
| side          | %usr | %sys | %idle | busy |
|---|---|---|---|---|
| CLIENT host10 | ~47  | ~2   | ~50   | ~50% (HALF IDLE) |
| ECHO  host11  | ~20  | ~69  | ~11   | ~89% (near saturation) |

POSITIVE EVIDENCE (confirms the user's "destination re-transmit overhead" intuition):
- The DESTINATION/server (echo) is the hot side (~89% vs client ~50%). The client has headroom; the cap
  is the server re-entering its transport to send the response.
- The server is dominated by **%sys ~69%**, NOT %usr and NOT DMA/PCIe. That is kernel/syscall time =
  the blocking 64-thread echo model (ECHO_THREADS=64, poll_rx OFF): one pthread_cond_signal (PE thread)
  + one cond_wait wakeup per request -> ~150K futex ops/s = the %sys wall.
=> The 2-pod ceiling above ~160K is gated by the HOST SERVER-SIDE WAKEUP/THREADING model, not the EUs,
   not the dma_copy count, not PCIe BW. Leaning the server RX path (poll instead of per-req cond_signal,
   or batched wakeup, or an async/fiber server) is the highest-leverage next lever. This is the
   server-side analogue of [[project_option_a_async_result]] (async client removed per-req wakeup on the
   CLIENT; the SERVER still pays it). Levers from the perf-levers workflow ranked separately.

## Q2.6 — Perf-lever multi-angle analysis (5 levers, adversarially verified) (2026-06-09)
Workflow (5 finders + 5 refuters + synthesis) over the code, cross-checked vs the Q2.5 measurement.
Ranked:
1. **Lean server RX (poll_rx on echo) — TOP, KEEP.** Only lever with POSITIVE measured evidence (Q2.5:
   echo 89% busy / %sys~69% futex, client 50% idle). Already built (poll path dpumesh_doca.c:789-826;
   per-req signal removed = dpumesh_doca.c:251). Server-side twin of the proven async-client win
   (option_a: +21%). Expected +15-30% (160K -> ~185-210K). Effort XS (config + 1 deploy). Pair with small
   ECHO_THREADS (~= pinned cores) so spin workers don't oversubscribe.
2. dma_copy 4->2 (direct host->host DMA) — **DROP, unbuildable.** Forward producer_dma_copy
   (dpa_kernel.c:222) is a FUSED op: moves body AND delivers the 16B FWD_DONE to the SINGLE
   dpu_consumer_id (max_num_consumers=1, dpa.c:470; consumer = ARM). EU physically can't deliver the
   routing completion to a host consumer -> ARM relay irreducible. "4->2" miscounts (best 4->3 on a
   non-bottleneck path). Forecloses body-L7.
3. Host PE/RX multi-thread (K PE/consumers) — **DROP, targets nothing.** The host datapath consumer is
   VESTIGIAL (dpumesh_doca.c:483-495, ID no longer advertised); reverse signal is 16B REV_DONE over the
   ONE comch control conn. pe_progress is single-threaded but NOT the cap (the server futex is, Q2.5).
4. Activate SPLIT_SHARD multi-ARM — **DROP, latent DATA RACE + wrong stage.** At M>1 with K-rings, two
   EUs (p*K, p*K+1)%N can push into the same non-atomic shard_work[effdst%N] SPSC -> corrupts completions
   -> stall. Egress (SENDER->single cc_server) is NOT sharded anyway. ARM is cq_depth=0 / ~2.8%. KEEP code
   OFF (preserved scaffolding).
5. Batch reverse-DMA completions — **DROP, structurally impossible.** The rev completion is a FUSED
   per-body DMA (distinct src/req, advances rev_pos); K bodies need K calls. Only ARM->host REV_DONE is
   batchable but it's the ~2.8% non-binding path. Per-body DMA cost irreducible.

ATTACK ORDER: (1) poll_rx now (free, measured-backed). (2) re-mpstat BOTH sides at the new knee + add a
positive DPU SENDER/cc_server submit-rate + EAGAIN + cq_depth counter to distinguish "single ARM egress
op-rate" vs "sub-EU PCIe BW / comch op-rate" — the ~178K attribution is elimination-only & has flip-
flopped, demand a positive counter first. (3) ONLY if egress proven binding, build a NEW K-way cc_server
egress (NOT the racy SPLIT_SHARD). Running the poll_rx experiment now (ECHO_POLL_RX=1 ECHO_THREADS=3).

## Q2.7 — poll_rx lean server RESULT: the biggest single win (2026-06-09)
Deployed ECHO_POLL_RX=1 ECHO_THREADS=3 (K=2 DPA=4). The poll path removes the per-request
cond_signal/cond_wait. 2-pod hw3, size 8192:
| RPS | achieved | p50 | echo %sys | state |
|---|---|---|---|---|
| 150K | 149,631 | 1.70ms | **2%** (was 69%) | echo 40% busy (was 89%) |
| 180K | 179,008 | 1.77ms | — | healthy |
| 200K | 198,9xx | 2.30ms | — | healthy 0-fail (x3 runs) |
| 220K | 200,780 (SAT) | 455ms | — | knee |

RESULT: **echo %sys 69%->2%** (futex eliminated). 2-pod healthy ceiling **~160K -> ~200K (+25%)**.
Combined with EU-sharding: **K=1 130K -> K=2+poll_rx 200K = +54%**. NB: 200K on 2 pods BEATS the 4-pod
blocking number (178K) — because 4-pod also paid the server futex. The user's "destination re-transmit
overhead" intuition was the single biggest lever, larger than EU-sharding itself.

NEXT CAP (mpstat at 200K knee): binding side FLIPPED off the server. client host10 ~72% busy (usr 67%,
sys 4%), echo host11 ~50% busy (usr 46%, sys 3%). %sys now low on BOTH (futex gone everywhere). Neither
host fully saturated at the 200K knee + both have idle => the residual cap is now the DPU/PCIe shared
op-rate (~200K, consistent with the 4-pod sub-EU resource, higher because lean). Per Q2.6 attack order:
to go above 200K, instrument a POSITIVE DPU SENDER/cc_server submit-rate + EAGAIN + cq_depth counter to
prove "single ARM egress op-rate" vs "PCIe body BW" BEFORE building a K-way cc_server egress. The client
72% is partly bench load-gen artifact (real clients are leaner).
Deployed state: DPA=4 K=2 ECHO_POLL_RX=1 ECHO_THREADS=3 (~200K). Knobs: DPUMESH_RINGS_PER_POD, ECHO_POLL_RX.

## Q2.8 — >200K cap attribution: DPU egress counters (positive evidence) (2026-06-09)
Added g_egress_again (cc_server DMA_COMPLETION send AGAIN/s) to the 1Hz DPU stat; cq_depth already logged.
K=2 DPA=4 ECHO_POLL_RX=1, -l 50:
| offered | achieved | recv (DPA->ARM comp/s) | cq_depth | egress_again/s |
|---|---|---|---|---|
| 200K (healthy) | 199K | **~800,000** | 0 | 0 |
| 220K (saturated) | ~202K | ~813,000 | 0 | 0 |
(sent=4000/s = the DPU->DPA keepalive WAKE, 4 EUs x 1kHz — NOT the host egress; the host-egress signal is
egress_again, which is 0.)

ATTRIBUTION (positive, not elimination):
- egress_again=0 + cq_depth=0 => the single ARM ingest AND the ARM->host cc_server egress both keep up
  with headroom. The single ARM / egress op-rate is NOT the cap. Refutes the "single ARM egress" lever
  (Q2.6 ranked #... would-be) with a positive counter.
- recv PLATEAUS at ~810K/s (200K and 220K offered both ~810K, achieved stuck ~200K) => the binding cap is
  the **DPA dma_copy + completion op-rate ~810K/s** (4 dma_copy/RTT x 200K = 810K). It is an OP-RATE, not
  BW (810K x 8KB = 6.4 GB/s << PCIe Gen4). Consistent with [[project_n8_dma_engine_ceiling]] shared op-rate.
DECISIVE NEXT TEST: is ~810K per-EU-summed (more EUs lift it) or a hard shared cap (DMA engine / comch
completion)? K=4 DPA=8 makes the 2-pod pair drive 8 EUs (pod10->EU0-3, pod11->EU4-7). If recv rises above
810K and RPS above 200K => EU-bound, EU-sharding K=4 scales further. If recv stays ~810K => hard shared
op-rate cap, more EUs won't help and the only lever is fewer dma_copy/RTT.

## Q2.9 — K=4 (8 EU) test: the ~810K op-rate is a HARD shared cap, NOT per-EU (2026-06-09)
K=4 DPA=8 (2-pod pair drives 8 EUs: pod10->EU0-3, pod11->EU4-7), ECHO_POLL_RX=1, -l 50:
| offered | achieved | recv (DPA->ARM comp/s) | cq_depth | egress_again |
|---|---|---|---|---|
| 220K | 204K (SAT p50 523ms) | ~813,000 | 0 | 0 |
| 260K | 204K (SAT) | ~816,000 | 0 | 0 |
| 300K | 202K (SAT) | ~813,000 | 0 | 0 |
(sent=8000/s = keepalive WAKE, 8 EUs x 1kHz — confirms sent = keepalive, not egress.)

DECISIVE: 8 EUs gives the SAME ~200-204K and recv stays pinned at ~813K/s (= K=2's 4-EU number). So the
~810K dma_copy+completion/s is a **HARD SHARED op-rate cap that does NOT scale with EUs**. K=2 (4 EUs)
already saturates it; **K=4 is useless** (same throughput, 2x EUs + 2x memory). EU-sharding sweet spot =
K=2. cq_depth/egress_again stay 0 at 8 EUs => still not ARM/egress; pure DPA-side op-rate.

Since pure_dma hits ~1.6M ops/s ([[project_n8_dma_engine_ceiling]]) but dma_copy-WITH-completion caps at
~810K, the per-op COMPLETION (comch msg attached to each dma_copy, EU->ARM) is ~half the op cost. The cap
is the DPA producer completion delivery op-rate, not DMA BW (6.4 GB/s) and not the DMA engine raw rate.

CONCLUSION (2-pod): architecture ceiling ~200K RTT/s = ~810K dma_copy+completion/s, a GLOBAL DPA op-rate
resource. Levers that DON'T work (positive evidence): more EUs (K=4 flat), single-ARM egress sharding
(egress_again=0). The ONLY remaining lever to exceed 200K is REDUCING dma_copy+completion per RTT (the 4):
either fewer dma_copy (host->host direct = foreclosed for body-L7; refuted 4->2 fused completion) or
completion-free RX (host polls landed RX buffer instead of a per-resp REV_DONE msg) — an architectural
redesign. Baking K=2 + poll_rx as the standing 200K config; K=4 reverted.

## Q2.10 — (a) BAKED standing config + (b) DONE + log level reverted (2026-06-09)
(a) Baked the measured 200K config as test-bench.sh defaults (env still overridable):
DPUMESH_DPA_THREADS:-4, DPUMESH_RINGS_PER_POD:-2, ECHO_POLL_RX:-1, ECHO_THREADS:-3. A plain
`test-bench.sh deploy` now yields the K=2 + lean-server 200K config at -l 40.
VERIFY (no env overrides): baked deploy -> 199K hw3 (p50 1.95ms, 0-fail). Fair-mode sanity (1-core echo
w/ poll_rx+3 threads) -> 99.5K (p50 1.46ms, 0-fail) — NO regression (the old 1-core poll_rx regression was
64 threads; 3 threads + adaptive yield is healthy). So poll_rx default-on is safe for both hw3 and fair.
(b) DONE: cap attributed to the ~810K DPA dma_copy+completion op-rate (Q2.8-2.9), NOT ARM/egress (egress_
again=0, cq_depth=0) and NOT EU count (K=4 flat). g_egress_again diagnostic kept (1Hz, silent at -l 40).
Log level reverted to 40 in the final deploy (user request).

FINAL LADDER (2-pod, hw3, 8KB): 130K (K=1) -> 160K (K=2 EU-sharding) -> **200K (K=2 + poll_rx lean server)**
= +54% over baseline, and above the 4-pod blocking number (178K). 200K is the architecture's 2-pod DPA
op-rate ceiling; exceeding it needs fewer dma_copy+completion per RTT (architectural), not more EUs/pods.

## Q2.11 — CORRECTION of Q2.8/Q2.9 attribution (per bench.md hierarchy) (2026-06-09)
Q2.8/Q2.9 claimed "~810K = the DPA dma_copy+completion op-rate; the per-op completion is ~half the cost;
single comch consumer delivery." **That attribution is WRONG** — bench.md §5 already measured the engine
hierarchy (test_dma micro-bench, recv/s == dma_copy/s, completions INCLUDED in all):
- pure single-EU 556K; pure 4-EU **1.8M**; M0 (single consumer_pe drain) N=8 **1.53M**; M2 (single ARM
  fwd) N>=4 **1.0M**. ALL above ~810K. So 810K is NOT the engine/DMA limit, and NOT the single consumer
  reaping (M0 uses the SAME single consumer_pe and reaches 1.53M). pure_dma ALSO emits a completion per
  copy and still does 556K single-EU -> "completion = half the cost" is false.

WHAT 810K ACTUALLY IS: the **chain ceiling at 4 active EUs**. bench.md §5.1 chain 2-active-EU = 416K
(104K x4). My EU-sharding made 2 pods drive 4 active EUs (the decisive experiment bench.md §8 marked
"미실시") -> chain 2-EU 416K -> 4-EU ~810K = ~1.95x (near-linear) => CONFIRMS bench.md's untested "chain
scales with active EUs". Then 4-EU -> 8-EU (K=4) is FLAT (810->813K) => a NEW chain wall at ~810K.
Per-active-EU: 208K(2EU) -> 200K(4EU) -> 102K(8EU). The chain EU runs ~200K/EU vs pure 556K/EU => the
chain EU is **STALL-bound (~64% idle), not op-rate-bound**.

CAUSE OF THE 810K CHAIN WALL: UNPROVEN. Per bench.md §7.2 (chain-cap attribution is elimination-only and
flip-flopped 3x), do NOT name a mechanism without positive evidence. It IS below engine/M0/M2 (so chain-
specific: closed-loop + reverse DMA + admission per-RTT structure), but the specific stall is unmeasured.
NEXT (bench.md §8.2): DPA-side stall-cycle instrumentation — measure where the chain EU stalls
(is_consumer_empty / admission-gate / ring-empty) with POSITIVE counters.

## Q2.12 — POSITIVE cap localization via DPA EU-stall instrumentation (2026-06-09)
Added device-side EU counters (dpa_thread_arg: stat_dma/consumer_wait/admission_brk/idle_resched),
incremented in dpa_kernel.c, read back by the ARM via doca_dpa_d2h_memcpy in the 1Hz stat
(dmesh_log_eu_stats in dpa.c). This is bench.md §8.2's "DPA stall-cycle 계측" — positive, not elimination.

Measured at the ~810K knee (8KB, hw3):
| config | RPS | recv (dma/s) | dma | consumer_wait | admission_brk | idle_resched |
|---|---|---|---|---|---|---|
| K=2 (4 EU) @200K | 199K | 800K | 800,076/s | **0** | **0** | 1,811/s (~9%) |
| K=3 (6 EU) @200K | 199K | 794K | 793,801/s | **0** | **0** | 5,958/s |

POSITIVE FINDINGS (counters, not elimination):
1. recv plateaus ~800K for 4 AND 6 AND 8 EU (Q2.9 K=4 also 813K) => chain caps ~800K dma/s (200K RTT),
   does NOT scale with EUs past 4. (Refutes "K=3/6-EU may beat the engine N=8 regression" — it's a real
   chain cap, not the pure N=8 dip.)
2. **consumer_wait=0** => EU NEVER stalls delivering completions to the single DPU consumer (refutes my
   earlier "810K = single-consumer reaping"; M0=1.53M already implied this).
3. **admission_brk=0** => EU NEVER stalls on host RX credit (the closed-loop admission gate is not binding).
4. At 4 EU the EU is COMPUTE-busy (idle 9%); at 6 EU idle_resched scales with EU count (per-EU 453->994/s)
   while total dma stays ~800K => extra EUs are STARVED. So the cap is UPSTREAM of the EU: the
   RING-FILL rate (host forward-post + ARM reverse-enqueue) is pinned at ~800K, starving added EUs.
=> The 2-pod ~200K ceiling is the CLOSED-LOOP RING-FILL rate (host side), NOT the DPA engine (pure 1.8M),
   NOT the DPU consumer (consumer_wait=0, M0 1.53M), NOT host credit (admission_brk=0), NOT the ARM
   (cq_depth=0), NOT egress (egress_again=0). Consistent with bench.md §6.1 M/M/1 fixed-service-rate +
   [[project_chain_host_transport_bound]]/[[project_host_send_serial_cap]] (single host PE progress thread).
   host->host (dma_copy count) is MOOT for this cap — it is host ring-fill, not DPA op-rate.
CORRECTS my Q2.8/Q2.9 ("DPA op-rate / single-consumer / completion-half-cost") AND the "stall-bound"
guess — all refuted by positive counters. Remaining: localize host ring-fill (single PE thread vs posting);
per-thread CPU probe failed (container ns); headline (host closed-loop ring-fill) is positive-evidenced.
Diagnostics (egress_again, EU-stall) kept in code, silent at -l 40.

## Q2.13 — Host cap POSITIVELY localized: single PE progress thread (2026-06-09)
Per-thread CPU (crictl PID + top -H) at K=2/200K, hw3 (3 cores/side):
- ECHO (server): ONE thread **96.4%** (saturated) + 3 poll workers ~23% each (idle-ish, waiting on PE).
- CLIENT: ONE thread 77.2% + 4 async workers ~38% each.
=> The single **PE progress thread (pe_progress_fn, doca_pe_progress + rx_data_hook RX landing) per pod is
the 200K cap**. Workers have headroom; they starve waiting for the PE thread to land RX. Confirms
[[project_host_send_serial_cap]] / [[project_chain_host_transport_bound]] positively (per-thread, not
elimination). The DPU EU starvation (Q2.12 idle_resched) is the downstream symptom of this host-side cap:
the PE thread can't post/land fast enough to feed >4 EUs.
LEVER: parallelize/lighten the host RX landing (single pe_progress_fn). Candidates: drop the vestigial
consumer_pe drain if empty; offload rx_data_hook off the PE callback; or multiple RX PEs.

## Q2.14 — consumer_pe drain removed (lean-up); confirms cap = comch RX reaping (2026-06-09)
Removed the vestigial doca_pe_progress(consumer_pe) from pe_progress_fn (host datapath consumer ID is never
advertised → nothing lands on it; RX arrives via the comch client/pe → rx_data_hook). Safe, host-only.
Result (K=2, hw3): 200K healthy (p50 1.89ms 0-fail); 215K saturates (~204K, p50 296ms). => marginal ~+2%,
ceiling still ~200-204K. So the single PE thread's saturation is dominated by the REAL comch RX reaping
(doca_pe_progress on the comch client), NOT the vestigial consumer_pe. Kept the removal as a lean-up
(runtime lighter), but it is not the lever.

FINAL POSITIVE PICTURE (this session): 130K (K=1) → 160K (EU-sharding K=2) → 200K (lean server poll_rx) →
~204K (consumer_pe lean-up). The 2-pod ceiling ~200K = the **single host comch-RX PE thread per pod**
(echo PE 96.4%), positively localized through the whole stack (DPA engine/consumer/ARM/egress/host-credit
all ruled OUT with counters=0; EU starves at 6 EU; per-thread top -H shows the one PE thread saturated).
NEXT LEVER to exceed ~204K (all are MAJOR host-transport changes, uncertain payoff, deploy-only verified):
  (a) completion-free polled RX — host workers poll the RX buffer for landed data (DPA writes a valid/seq
      marker), eliminating the per-response comch REV_DONE reaping (removes the PE-thread bottleneck +
      cuts completions). Needs req_id correlation in the landed data + DPA reverse format change.
  (b) multi-connection comch RX — K host comch connections, ARM round-robins REV_DONE, K PE threads.
  (c) lock-free rx_queue (echo PE 96% vs client 77% gap may be rx_lock contention: 1 producer PE + 3
      worker consumers). Smaller, targets the server-side gap only.
host->host is MOOT here (cap is host comch-RX, not DPA dma_copy) and stays FORECLOSED (DPU L7 proxy).
Deployed state: baked K=2 DPA=4 poll_rx + consumer_pe-drain-removed, -l 40, ~200K 0-fail.

## Q2.15 — BATCH_REV_DONE built (the real ceiling lever); first deploy WEDGED (gatekeeper) then fixed
Mirrored BATCH_FWD_ACK for REV_DONE: new DMESH_MSG_BATCH_REV_DONE(6) + dmesh_rev_done_entry(16B) +
dmesh_batch_rev_done_msg(BATCH_REVDONE_MAX=16); pod_state.rev_done_batch[]; server_send_batch_rev_done_to;
dpu_worker batch_or_send_rev_done + flush (proc==0 idle-flush + 1kHz tail; cross-pod batched, echo_mode
un-batched for REV_DONE-before-TX_ACK order); host rx_data_hook BATCH_REV_DONE unpack loop.
FIRST DEPLOY WEDGED (0 achieved / all fail) — exactly [[project_dpu_host_msg_gatekeeper]]: a new DPU→host
comch type needs a case in BOTH rx_data_hook AND the comch_client.c client_message_recv_callback switch.
Added rx_data_hook but missed the client switch → type 6 hit default ("unknown message type") → silent
drop → no responses → wedge. FIX: added the DMESH_MSG_BATCH_REV_DONE case to comch_client.c:119. Redeploy.

## Q2.16 — BATCH_REV_DONE RESULT: 200K -> ~220-235K, bottleneck redistributed (2026-06-09)
After the gatekeeper fix (Q2.15), K=2 hw3, 8KB, 0-fail throughout:
| RPS | achieved | p50 | state |
|---|---|---|---|
| 200K | 199,070 | **1.25ms** (was 1.89ms pre-batch) | healthy, lower latency |
| 220K | 218,956 | 1.42ms | healthy (new sustainable knee) |
| 235K | 233,866 | 17.9ms | knee edge |
| 250K | 238,905 | 246ms | saturated |
=> BATCH_REV_DONE lifts ~200K -> ~220-235K (+10-18%) AND cuts p50 at 200K (PE less loaded). It is the
user's "batch the completion" idea, realized by mirroring the proven BATCH_FWD_ACK (1 comch reap per K
responses), always-on for cross-pod (echo_mode un-batched for ordering). 0-fail; effectively baked.

NEW bottleneck (per-thread top -H @220K): echo now has TWO threads ~90%/87% (was a SINGLE 96% PE pre-batch)
+ 2 workers ~18%; client 73%/70% + 2 workers ~33%. => batching relieved the single PE; the bottleneck
REDISTRIBUTED to the echo node's aggregate per-RTT CPU on its 3 cores (PE reaping the batched msgs + the
WORKERS doing the 8KB memcpy echo_dpumesh.c:68 + the forward post). So the echo 8KB memcpy (the user's
point) is now CO-BINDING. Removing it (echo re-send from the RX slot, no rx->tx copy) is the next lever
BUT is NOT app-local: the forward DMA reads the host TX data buffer (remote_mmap), not the RX buffer, so
re-sending from RX needs the RX buffer registered as a DMA source (a transport change), not just an
echo-app edit. Ladder so far: 130K(K=1) -> 160K(EU-shard) -> 200K(poll_rx) -> ~220-235K(BATCH_REV_DONE).

## Q2.17 — Bench restructured to isolate transport (user was RIGHT) (2026-06-09)
USER INSIGHT (verified correct): the echo memcpy (echo_dpumesh.c:68, 8KB rx->tx) AND the client memset
(bench_dpumesh.c:132/234, 8KB pattern fill) are per-request APP operations for the content-validation
bench, NOT dpumesh transport. The transport DMAs body_len bytes regardless of content. bench.md §1's own
purpose is "application 로직 제거, transport 비용만 분리 측정" — so these 8KB ops polluted the measurement.
KEY: the validation only ever checked 3 byte positions (rb[0], rb[bl/2], rb[bl-1], bench:171) — the full
8KB fill/copy was overkill. FIX: replace the 8KB memset/memcpy with a 3-byte fill/copy (same validation,
no app bandwidth). Transport (8KB DMA x4/RTT) unchanged.
RESULT (transport-only, K=2 + poll_rx + BATCH_REV_DONE, hw3, 8KB, 0-fail):
| RPS | achieved | p50 | state |
|---|---|---|---|
| 235K | 233,882 | 1.24ms | healthy |
| 245K | 243,835 | 1.28ms | healthy (new sustainable knee) |
| 260K | 250,461 | 170ms | saturating |
| 300K | 254,246 | 1.07s | saturated |
=> TRUE transport ceiling ~245K healthy / ~252K saturation, vs ~235K with the 8KB app ops = **+5-10%**.
So the app memcpy/memset CO-BOUND but were NOT dominant (~7%); the main limit is still the transport
(single host comch-RX PE thread). The bench now correctly isolates transport. The 3-byte validation keeps
the same corruption sanity (it always only checked 3 positions). Baked (bench app, not a knob).
LADDER (transport-only is the correct metric): 130K(K=1) -> 160K(EU-shard) -> 200K(poll_rx) ->
~235K(BATCH_REV_DONE) -> ~250K(bench isolates app overhead). Cap remains the host comch-RX PE thread.

## Q2.18 — "more cores to service" REGRESSES; chain reached the M2 engine ceiling (2026-06-09)
USER asked: give the echo SERVICE more cores (or one-way bench), since they want dpumesh TRANSPORT perf
not service perf. Tested hw6 (6 cores/side + ECHO_THREADS=6 ASYNC_THREADS=6), transport-only bench:
| profile | ceiling |
|---|---|
| hw3 (3c/3thr) | ~245-252K |
| hw6 (6c/6thr) | **195K, then collapses 131K->115K at higher RPS** |
=> MORE cores/threads REGRESS. Cause: the bottleneck is the SINGLE host PE thread (1 core, processing
completions); adding workers only adds shared-rx_queue rx_lock contention + poll-spin thrash. So "more
cores to the service" does NOT raise the ceiling, and (b) lock-free RX alone won't exceed it either (the
single PE caps). Restored baked ECHO_THREADS=3 / hw3.

KEY: the chain has REACHED the M2 engine ceiling. ~250K RTT x 4 dma_copy = **~1.0M dma_copy/s = bench.md
M2 (single-ARM one-way forward) N>=4 plateau (1.0M)**. bench.md had the chain at 416K = 67% of M2; the
session's transport work (EU-sharding 4EU + poll_rx + BATCH_REV_DONE + bench app-isolation) brought it to
1.0M = 100% of M2. So the chain is now at the single-host-PE / single-ARM-forward limit. Above M2 is M0
(1.53M, no host forward) and pure (1.8M); exceeding M2 needs to PARALLELIZE the single host PE (multi-conn
comch, or offload the per-entry process_rx_dma_entry to workers) OR a ONE-WAY bench (no round-trip, no
service re-send) to measure the forward transport rate directly. One-way needs the request TX_ACK
re-enabled (g_skip_req_txack) for client TX-slot lifecycle without a response.
LADDER: 130K -> 160K(EU) -> 200K(poll_rx) -> ~235K(BATCH_REV_DONE) -> ~250K(bench isolates app) = M2 ceiling.

## Q2.19 — LIGHTER transport: lock-free TX slot pool (user: transport must be light) (2026-06-09)
USER directive: the transport must be LIGHT on host CPU (NOT use more cores). perf (Q-prev) showed the
reducible host CPU is in MUTEXES + the O(n) TX-slot bitmap scan, not the SDK. Replaced slot_bitmap +
slot_lock + slot_cond with a lock-free Treiber free-list of slot indices (free_head atomic u64 with ABA
tag + slot_next[]); tx_alloc = lock-free pop (spin-backoff when empty), tx_free = lock-free push. Removes
slot_lock (mutex), the O(num_slots) scan, and the per-free cond_signal (futex).
perf BEFORE -> AFTER (echo self-time @240K): dpumesh_tx_alloc 3.42% -> 1.73% (halved); mutex lock+unlock
6.5% -> 5.7% (slot_lock gone); tx_free 0.96%. => host CPU per request is LIGHTER. Correctness: 250K
healthy (p50 1.48ms), 0-fail, back-to-back 218.8K==218.8K = NO SLOT LEAK (free-list correct). Ceiling
~245 -> ~250K (small; host wasn't purely tx-bound). Baked (always-on, no knob).

REMAINING reducible host CPU (next "lighter" levers, all lock-free, no extra cores):
- rx_queue rx_lock + dpumesh_dequeue (~5.3% + part of mutex) = the echo MPSC. Lock-free Vyukov MPSC ring
  (1 PE producer, N worker consumers). Biggest remaining. Caveat: needs a lightweight cond kept ONLY for
  the non-poll (Thrift blocking) path; poll_rx (bench) path is pure spin on the ring.
- pending[req_id] mutex (~2-3%, client side) = lock-free atomic state.
IRREDUCIBLE FLOOR: the DOCA SDK comch polling (priv_doca_cq_poll_one + doca_pe_progress + comch internals)
~12-15% of a host core at 250K — inherent to a completion-notification RX; only a fundamentally different
RX (completion-free polled, not SDK-supported) would go below it. So the transport's host-CPU floor at
~250K is the SDK poll (~12-15%) + the irreducible per-entry deliver. Ladder unchanged (~250K = M2 ceiling).

## Q2.20 — LIGHTER transport: lock-free rx_queue (Vyukov MPSC) (2026-06-10)
USER: "rx_queue lock-free 이어서 해줘" (continue the lighter-transport work; rx_queue was the biggest
remaining reducible host CPU). The echo RX queue was a circular buffer guarded by rx_lock (mutex) + rx_cond
+ rx_not_full (vestigial: signalled, never waited). The single PE producer took rx_lock per enqueue and
N workers took rx_lock per dequeue — PE<->worker mutex contention on EVERY request, on the bottleneck PE.

CHANGE (dpumesh_doca.c, baked/always-on, no knob): replaced rx_queue[]/head/tail/count + rx_lock +
rx_cond + rx_not_full with a lock-free **Vyukov bounded MPSC ring** (RX_QUEUE_SIZE=65536, pow2 -> mask):
- struct rxq_cell { sw_descriptor_t desc; atomic_uint_fast32_t seq; } rx_ring[]; atomic rx_enq, rx_deq.
- Producer (rx_deliver_desc, single PE): seq-gated single-producer enqueue (relaxed load/store rx_enq,
  release-store cell seq), drop+rx_reclaim on full. No rx_lock in poll_rx.
- Consumer (dpumesh_dequeue, N workers): rxq_try_pop = acquire-load cell seq, CAS rx_deq, read desc,
  release-store seq+SIZE. Multi-consumer via CAS.
- poll_rx (bench): pure lock-free spin + adaptive backoff (RX_POLL_SPIN/20us), NO mutex at all.
- non-poll (Thrift blocking): lock-free pop; only when empty, lock rx_lock + re-check (closes lost-wakeup)
  + cond_wait to sleep idle-efficiently. Producer signals rx_cond under rx_lock only when !poll_rx.
- rx_enq/rx_deq cacheline-padded apart (defensive; see below).
Single-producer invariant verified: rx_deliver_desc is called only from process_rx_dma_entry <- rx_data_hook
<- doca_pe_progress <- the single pe_progress_fn thread.

PERF (echo node, crictl PID + sudo perf -g, 240K, self-time):
| symbol | Q2.19 (locked rx_queue) | Q2.20 (lock-free) |
|---|---|---|
| pthread_mutex lock+unlock | 5.7% | **3.48%** (2.07+1.41) |
| rx_deliver_desc (PE enqueue) | took rx_lock/req | **0.74%, lock-free (no mutex)** |
| __lll_lock_wait (futex) | (in mutex) | **0.01%** (no contention) |
| dpumesh_dequeue + rxq_try_pop | dequeue 5.3% + rx_lock share | rxq_try_pop 10.5% + dequeue 2.1% |
=> MUTEX -39% (5.7->3.48): rx_lock ELIMINATED from both the PE producer (bottleneck) and the workers.
Remaining mutex = pending table + ring_lock (next levers). The PE per-request enqueue is now lock-free.

CACHELINE PADDING measured NEUTRAL (rxq_try_pop 10.71% -> 10.50%): rx_enq is producer-PRIVATE (consumers
never read it) so it never false-shared rx_deq. Kept anyway (textbook MPSC hygiene, ~192B, harmless). The
residual rxq_try_pop ~10.5% is INTRINSIC: 3 workers CAS the shared rx_deq (true MPSC sharing) + the cell
seq line bounces producer<->consumer near-empty. NOT reducible by layout.

HONEST framing of "lighter": in poll_rx (bench) the workers are DEDICATED spinners (~100% core regardless),
so wall-clock core use is ~constant; the lock-free win there is (a) the PE BOTTLENECK is now lock-free,
(b) zero futex/contention stalls. The REAL lightness is the NON-POLL (production Thrift) path: rx_lock is
gone from the data path entirely — a worker does one lock-free CAS-pop per request and cond_waits only when
idle (vs old rx_lock per pop + cond). Strictly lighter per request in production.

CORRECTNESS (hw3, 8KB, transport-only bench, K=2 + poll_rx + BATCH_REV_DONE + lock-free TX + lock-free RX):
| RPS | achieved | p50 | OK/Fail |
|---|---|---|---|
| 200K | 199,244 | 1.25ms | 3.0M / 0 |
| 240K (70s sustained) | 239,713 | 1.30ms | 16.8M / 0 |
| 245K | 244,080 | 1.37ms | 3.675M / 0 (healthy knee) |
| 260K | 250,645 | 205ms | 27 fail / 3.9M (saturation cliff timeouts) |
Back-to-back 200K after the 260K saturation: 199,063 == 199,058, 0-fail = CLEAN recovery, NO leak
(Vyukov positions self-balance as items drain). Ceiling unchanged ~250K = M2 (throughput-neutral, as
expected: the cap is the single host PE / M2 engine, NOT rx_lock). Goal was LIGHTER, not faster.
LADDER (unchanged ceiling): 130K -> 160K(EU) -> 200K(poll_rx) -> 235K(BATCH_REV_DONE) -> ~250K(M2). Host
CPU per request: TX pool lock-free (Q2.19) + RX queue lock-free (Q2.20) => mutex 6.5% -> 3.48% cumulatively.
NEXT lighter lever: pending[req_id] lock-free (the remaining ~3.48% mutex = pending + ring_lock).

### Q2.20 verification (adversarial, 2026-06-10) — mutex attribution CONFIRMED + corrected
Challenged the "3.48% mutex = pending + ring_lock" attribution (memory: elimination is unreliable; demand
positive evidence). Perf caller-graph was inconclusive: frame-pointer unwind fails (libthrift -O2 omits
frame ptr); --call-graph dwarf is Heisenberg-perturbed (16KB/sample stack copy -> the recording is dominated
by nanosleep context-switch storm + perf's own native_write_msr/sched_in; userspace symbols squished below
threshold). So the AUTHORITATIVE evidence is an EXHAUSTIVE CODE AUDIT (all 14 pthread_mutex_lock sites),
re-run by 4 independent agents (2 enumerate, 2 adversarial-refute) + synthesis.
VERDICT: claim CORRECT (no refuter found a missed lock; rx_lock=0 in poll_rx confirmed by all 4). EXACT
per-request mutex count (poll_rx=1) = **5 acquisitions**:
- p->lock (pending) = **4x** : register_pending(:1021) + pending_attach_tx(:1068) + pending_release_async
  (:1248) [worker] + TX_ACK handler(:415) [PE]. ALL on the SAME pending[req_id%65536] slot -> genuinely
  CROSS-THREAD CONTENDED (worker vs PE) -> this is the DOMINANT mutex self-time (4 of 5).
- ring_locks[ridx] = **1x** : dpumesh_enqueue(:825). Spread over K=2 rings (test-bench.sh forces
  DPUMESH_RINGS_PER_POD=2, NOT the K=1 default) -> lightly contended, minor.
CORRECTIONS to the Q2.20 narrative: (1) "pending + ring_lock" is right but NOT 50/50 — it's ~4/5 pending
(the cross-thread-contended slot) + ~1/5 ring; "mostly pending" is accurate. (2) bench runs K=2 so the ring
lock is NOT a single contended object. Lock-free structures (Treiber TX, Vyukov RX, __sync credit) correctly
do NOT appear as pthread_mutex self-time (single RMW, not lock loops). 3.48% is fully consistent with the 5
acquisitions; nothing unaccounted. NEXT lever (pending lock-free) correctly targets the dominant 4/5.
SIDE FINDING (dwarf): at 240K the echo workers are ~84% IDLE (3 workers, ~16% busy) and the worker cores'
biggest cost is the 20us backoff nanosleep sleep/wake churn (poll_rx-bench artifact), NOT the mutexes or the
lock-free ring. The mutex (3.48%) + rxq_try_pop (10.5%) are secondary to the idle-poll scheduler traffic.

## Q2.21 — Periodic hot-path log audit + bench stat gated off (2026-06-10)
USER noticed periodic logs during deploy/test and asked to audit log level + log statements, and whether
more host-CPU lightening remains.
LOG AUDIT (positive, by source):
- The periodic lines the user saw = bench app stat threads: echo_dpumesh.c:31 "[echo-stat] rx_queue_depth=
  .. tx_inflight=.." and bench_dpumesh.c:51 "[bench-stat] ..", printed every 1s whenever tx_inflight>0.
  These are raw fprintf(stderr) -> NOT gated by -l or any DOCA log level (that's why they always show).
  Each also calls dpumesh_debug_stats every 1s, which walks the lock-free TX free-list O(num_slots).
- DPU 1Hz DOCA_LOG_INFO (dpu_worker.c:1191 "elapsed/sent/recv/cq_depth" + dmesh_log_eu_stats) is INFO(50),
  correctly FILTERED at -l 40 -> does NOT appear (verified -l stays 40). test-bench.sh:250 documents 50=INFO.
- Host transport per-request logs are DOCA_LOG_DBG (dpumesh_doca.c:393 TX_ACK, :864 ENQUEUE) = level 60,
  filtered (host backend shows INFO at init only, one-time, not periodic). No periodic host transport log.
FIX: gated both stat threads behind BENCH_STAT (default OFF) — the pthread_create is skipped unless
BENCH_STAT=1, so by default NO periodic [echo-stat]/[bench-stat] line AND no 1Hz debug_stats free-list walk
(host a touch leaner). Verified post-deploy: 0 stat lines in pod logs, 240K 238,889 p50 1.34ms 0-fail
(throughput unchanged). Re-enable: `BENCH_STAT=1 ./test-bench.sh deploy` — BENCH_STAT is now plumbed into
BOTH pod manifests (test-bench.sh bench env :445 + echo env :484, default 0); without that passthrough the
pods (env comes only from the hardcoded manifest env: lists, NOT the shell) would never see it.

MORE LIGHTENING — analyzed, user chose to STOP. Remaining reducible host mutex = the verified 3.48%, of which
the DOMINANT 4/5 is the pending p->lock (cross-thread worker<->PE on the same hashed slot). Offered: (A)
coalesce echo's register+attach+release 3->1 worker lock (total 4->2, moderate risk: must preserve the -2
deferred-ACK + collision-wait protocol; state=-2 settable pre-enqueue since TX_ACK only arrives post-enqueue),
(B) full lock-free pending (delicate: slot reuse / late-ACK / owner_req_id guard / client cond). USER PICKED
"stop here": throughput is at the M2 ceiling so pending lock-free wouldn't raise it, the remaining mutex is
small, and below it sits the irreducible SDK comch poll (~12-15%). The transport is at its lock-free-
achievable host-CPU floor. Deployed: baked K=2 + poll_rx + BATCH_REV_DONE + lock-free TX + lock-free RX +
stat-gated-off, -l 40, ~240K 0-fail.

## Cleanup — bake the winning config fixed, delete everything else (2026-06-10)
USER directive: keep ONLY the best-performing option (no env/compile toggles — fixed in code); keep only
thread/EU counts + sizing + pod-identity configurable; delete all dead code / unused vars / inappropriate
names / record-keeping comments. This intentionally OVERRIDES the earlier "bake ON but KEEP the SHARD/SPLIT/
DRAIN scaffolding + keep diagnostics + KEEP CASE_INGRESS" decisions ([[project_option_b_foreclosed_cleanup_keep]],
[[feedback_bake_dont_delete]], [[project_recv_pool_coupling]]). Confirmed by the user before editing.

### Removed (the losing / dormant options)
- DPU ARM control-plane SPLIT machinery: SPLIT_SEND (SENDS/REBAL/SHARD modes), DRAIN_SHARDS>1, the SENDER /
  shard-worker / drain-shard threads + send_spsc/work_spsc/shard_work/shard_send SPSC types + consumer_lock(_shard)
  + arm_core_a/b + shard_worker_of/drain_group_of_eu/send_via_spsc. dpu_worker.c kept ONLY the single-ARM
  SPLIT_OFF path (the measured winner, Q2.1). ~600 lines from dpu_worker.c + object.h + comch_*.
- All measurement instrumentation (the bottleneck investigation is DONE): DPU 1 Hz sent/recv/cq_depth stat,
  dmesh_log_eu_stats + the EU-stall counters (stat_dma/consumer_wait/admission_brk/idle_resched) + their
  device-side increments + d2h readback; g_egress_again; DPUMESH_TRACE hop-timing; recv_err_count; the bench
  [echo-stat]/[bench-stat] stat threads + dpumesh_debug_stats + the BENCH_STAT gate.
- Baked features fixed ON (env/flag selection removed): SKIP_REQ_TXACK, BATCH_TXACK, BATCH_REV_DONE, DPA EU
  affinity (host); ZEROCOPY_RX + PE_ADAPTIVE were already baked. Consumer model fixed per caller: bench→async,
  echo→poll_rx, Thrift→blocking (async/poll impossible under the sync Thrift API). DPUMESH_KEEPALIVE_US baked
  to 1 ms.
- Dead host code: blocking bench worker_fn + sleep_until (async is the only model now), dpumesh_get_notify_fd
  (unimplemented -1 stub), 7 never-read sw_descriptor_t fields (step_id, src_body/header_pool_type/pod_id/buf_slot)
  + their writers in TDpumeshTransportBase/bench/gateway, POOL_NONE/POOL_HOST_TX_BODY (no users left).
- Record-keeping/history comments across the transport (vestigial-consumer_pe, "~5% host CPU", old handshake,
  legacy-pattern, SHARD-measurement narrative, backward-compat).

### Kept (delete-before-verify risk check overruled deletion — NOT dead)
- CASE_INGRESS / CASE_EXTERNAL: the flags byte is wire-ABI (dma_desc.flags offset + comch_dma_comp_msg==16B
  _Static_asserts) and DMA-copied across the host/ARM/DPA boundary; CASE_INGRESS is live write-once metadata.
- comch_msg union add_ring_msg/add_rev_ring_msg members: their 80 B size sets the msgq imm_data_len
  (dpa.c set_imm_data_len(sizeof(comch_msg))==84) that RECEIVES the 80 B ADD_RING — shrinking would truncate
  ring setup. (A survey agent labelled these "safe to delete"; its own cited evidence proved the opposite —
  adversarial verification caught it. Kept.)

### Bug fixed during the audit
- comch_client.c set_recv_queue_size error path logged "Failed to set msg size property" (copy-paste from the
  set_max_msg_size path); corrected to "recv queue size". Log-only, no behavior change.

### Configurable surface after cleanup (user's choice = thread counts + sizing + identity)
env kept: DPUMESH_DPA_THREADS, DPUMESH_RINGS_PER_POD (thread/EU counts) · DPUMESH_NUM_SLOTS / SLOT_SIZE /
MAX_DESCRIPTORS (sizing) · DPUMESH_POD_ID / PCI_ADDR (deploy identity) · ECHO_THREADS / ASYNC_THREADS (bench
thread counts). Everything else is fixed in code. test-bench.sh dropped the baked-off env (KEEPALIVE_US, TRACE,
BENCH_STAT, ECHO_POLL_RX) and its dead `trace)` subcommand.

### VERIFIED non-regressing (clean deploy, hw3, 8KB, baked DPA=4 K=2, -l 40)
The deploy compiled all 3 toolchains clean (host libthrift + DPU ARM + DPA device build_dpu) — a compile
error would have aborted before pod rollout. Back-to-back, 0-fail throughout:
| RPS | achieved | p50 | p99 | OK/Fail |
|---|---|---|---|---|
| 30000 (warmup) | 29,838 | 1.79 ms | 3.11 ms | 300000/0 |
| 200000 | 198,917 | 1.40 ms | 3.63 ms | 2.0M/0 |
| 240000 | 238,691 | 1.46 ms | 3.99 ms | 2.4M/0 (sustainable knee) |
| 200000 (back-to-back) | 198,895 | 1.39 ms | 3.37 ms | 2.0M/0 |
| 30000 (recovery) | 29,838 | 1.80 ms | 3.11 ms | 300000/0 |
Back-to-back 200K==200K (198,917 vs 198,895, <0.02%) → NO slot leak. Ceiling unchanged ~240K = Q2.20
(240K→239,713). Net: ~810 source lines removed across 16 files; the standing 200-240K config is now the
only code path (no env/compile toggles), with thread/EU/sizing counts still tunable.
