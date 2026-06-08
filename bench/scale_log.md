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
