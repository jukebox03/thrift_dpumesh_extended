export const meta = {
  name: 'dpa-multithread-feasibility',
  description: 'Analyze feasibility of per-pod/per-EU DPA multithreading for DPUmesh transport',
  phases: [
    { title: 'Understand', detail: 'parallel readers map DPU-drain, DPA/comch model, host posting, design docs' },
    { title: 'Design', detail: 'synthesize concrete per-pod/per-EU sharding design + EU budget' },
    { title: 'Verify', detail: '3 adversarial critics try to refute feasibility from distinct lenses' },
  ],
}

const ROOT = '/home/jukebox/thrift_dma_copy/thrift_dpumesh_extended'
const DOCA = ROOT + '/lib/cpp/src/thrift/transport/doca'

const SHARED_CONTEXT = [
  'PROJECT: DPUmesh transport for Apache Thrift. Host pods talk to each other via a BlueField-3 DPU.',
  'Data path per RTT = 4 dma_copy (fwd x2 + rev x2). DPA (FlexIO) does the dma_copy; DPU ARM routes.',
  '',
  'ESTABLISHED FACTS (already verified by the orchestrator - build on these, do not re-derive):',
  '- DPA EU partition: ' + DOCA + '/dpa_resource_config.yaml -> PARTITION_1, vhca_id 0, abs_EUs: 0-63 = 64 EUs available.',
  '- Current DPA side is SINGLE-EU: one doca_dpa_thread runs run_dma_manager (' + DOCA + '/device/dpa_kernel.c).',
  '  One struct dpa_thread_arg holds ALL pods forward+reverse rings (rings[MAX_DPA_RINGS], rev_rings[...]).',
  '  drain_all_rings() loops over num_rings fwd + num_rev_rings rev each wake; yields when idle.',
  '- Single comch msgq pair: dpa.c dmesh_doca_dpa_msgq_create sets doca_comch_msgq_set_max_num_consumers(.,1)',
  '  and set_max_num_producers(.,1). One dpa_producer, one dpa_consumer, one dpu_consumer_id. ALL DPA->DPU',
  '  completions flow through this ONE producer to ONE DPU consumer.',
  '- Rings registered dynamically: first pod via doca_dpa_h2d_memcpy of the whole arg; later pods via',
  '  COMCH_MSG_TYPE_ADD_RING / ADD_REV_RING messages handled in dpa_kernel.c handle_dpu_msg.',
  '- Constants (' + DOCA + '/dpumesh_common.h): MAX_PODS=8, MAX_DPA_RINGS=8, DMA_RING_SIZE=2048,',
  '  DPU_BUFFER_SIZE=16MB; CC_DPA_MAX_MSG_NUM=1024 (dpa.h); DPU_COMP_QUEUE_SIZE=4096 (object.h).',
  '- DPU side: one consumer PE + one dpu_comp_queue_t comp_queue (object.h) drained by run_dpu_worker',
  '  (dpu_worker.c) via process_completion_queue. Recv callback (dpa.c dmesh_doca_dpa_msgq_recv_cb) enqueues.',
  '',
  'BENCHMARK (bench/bench.md) MULTI-EU FINDINGS - central evidence:',
  '- 6.2 single EU pure-DMA op-rate = ~556K dma_copy/s (size-independent, op-rate bound, NOT bandwidth).',
  '- 6.3 multi-EU op-rate scales with EU count: 1->2 EU = 1.93x, 4 EU = 3.25x (1.8M/s) PEAK,',
  '  8 EU = REGRESSION (2.64x, 1.47M). So peak ~ 4 EU.',
  '- 6.4 N=8 regression is NOT EU spin-contention (backoff had no effect - hypothesis rejected).',
  '- 6.5 leading hypothesis for N=8 regression: the SINGLE DPU drain thread processes N consumer contexts',
  '  via one doca_pe_progress; N up raises per-cycle poll overhead -> effective drain rate down -> producer',
  '  throttle. Secondary: EU oversubscription (8 threads may not land on 8 distinct EUs) or shared',
  '  DMA-engine/PCIe contention.',
  '- 6.1 the pure_dma multi-EU experiment used N threads + N INDEPENDENT comch channels but kept ONE DPU',
  '  drain thread (all consumers on the same consumer_pe; one pe_progress drains all).',
  '- 5.1 EU active% = 0.51% (99.49% idle); single-EU cap is yield/wake + per-op work, NOT EU compute.',
  '- 7 lists "Multi-EU DPA thread" as a lever worth ~+50-100%; notes it needs producer/consumer/comp_queue',
  '  split + resolving the N=8 regression. dpumesh sustainable is currently 74K RPS (309,736 dma_copy/s);',
  '  measured 1.8M op-rate implies ~450K RPS ceiling.',
  '- NOTE: pure_dma test dir is NOT in this repo (separate experiment); reconstruct its design only from',
  '  bench.md 6.1/6.3/6.4 prose.',
  '',
  'USER PROPOSAL (the thing to assess): assign an INDEPENDENT DPA thread (EU) per pod/per-DPU-peer, so each',
  'pods rings are drained by its own EU instead of one EU multiplexing all rings. User intuition: beyond the',
  'EU core count it becomes inefficient.',
].join('\n')

phase('Understand')

const UNDERSTAND_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['area', 'keyFindings', 'sharedSingletons', 'perUnitCandidates', 'scalingBlockers', 'openQuestions'],
  properties: {
    area: { type: 'string' },
    keyFindings: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['claim', 'evidence', 'confidence'],
        properties: {
          claim: { type: 'string' },
          evidence: { type: 'string', description: 'file:line or function name' },
          confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
        },
      },
    },
    sharedSingletons: { type: 'array', items: { type: 'string' },
      description: 'objects/resources currently 1-per-system that would block or complicate N-way sharding' },
    perUnitCandidates: { type: 'array', items: { type: 'string' },
      description: 'objects already per-pod/per-ring or that could cleanly become per-EU' },
    scalingBlockers: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['blocker', 'why', 'severity'],
        properties: {
          blocker: { type: 'string' },
          why: { type: 'string' },
          severity: { type: 'string', enum: ['hard', 'soft', 'minor'] },
        },
      },
    },
    openQuestions: { type: 'array', items: { type: 'string' } },
  },
}

const understandTasks = [
  {
    label: 'understand:dpu-drain',
    prompt: SHARED_CONTEXT + '\n\n' + [
      'YOUR AREA: The DPU-side drain & completion path - the SUSPECTED N=8 bottleneck (bench 6.5).',
      'READ FULLY: ' + DOCA + '/dpu_worker.c (run_dpu_worker main loop, process_completion_queue,',
      'process_forward_entry, process_rev_notify_entry, dpu_enqueue_reverse_dma, drain_deferred_tx_acks),',
      DOCA + '/object.h (comp_queue, pod_state, objects), ' + DOCA + '/comch_server.c (how DPA->DPU',
      'completions are received; consumer/producer setup; pod registration), and the recv callback in',
      DOCA + '/dpa.c (dmesh_doca_dpa_msgq_recv_cb).',
      '',
      'ANSWER PRECISELY:',
      '1. Is the DPU drain truly single-threaded / single-PE / single comp_queue? Trace every doca_pe_progress.',
      '2. If we run N DPA EUs each on its OWN comch channel (own consumer_id), what on the DPU side must change',
      '   to drain N channels without the 6.5 per-cycle-poll regression? Could each DPA EU map to its own DPU',
      '   drain thread + own comp_queue + own consumer PE? What is shared and would contend (pods[] table, send',
      '   pool, TX_ACK path, find_pod_by_id)?',
      '3. Is pod routing (src_pod -> dst_pod, reverse DMA enqueue) compatible with per-pod-EU sharding, or does',
      '   cross-pod routing force completions to cross EU boundaries?',
      '4. Quantify: at 74K RPS today, what is the DPU drain doing per RTT, and is ARM core count (8) or PE',
      '   polling the limit if we go to 4-8 EUs?',
      'Return structured findings. evidence MUST cite file:line or function names you actually read.',
    ].join('\n'),
  },
  {
    label: 'understand:dpa-comch-eu',
    prompt: SHARED_CONTEXT + '\n\n' + [
      'YOUR AREA: The DPA thread/EU creation & comch replication model - what must be duplicated per EU.',
      'READ FULLY: ' + DOCA + '/dpa.c (init_dpa_objects, dmesh_doca_dpa_thread_create,',
      'dmesh_doca_dpa_msgq_create, dmesh_doca_dpa_comch_create, dmesh_fill_dpa_thread_arg, setup_pod_dma - esp.',
      'the first-pod h2d_memcpy vs later ADD_RING path, and thread_init_rpc / doca_dpa_thread_run / trigger),',
      DOCA + '/dpa.h (struct dmesh_doca_dpa_thread, dmesh_doca_dpa_comch, dmesh_doca_dpa_msgq),',
      DOCA + '/dpa_common.h (dpa_thread_arg), ' + DOCA + '/device/dpa_kernel.c (run_dma_manager, handle_dpu_msg',
      'ADD_RING), ' + DOCA + '/dpa_resource_config.yaml, ' + DOCA + '/build_dpacc.sh, ' + DOCA + '/meson.build.',
      '',
      'ANSWER PRECISELY:',
      '1. Confirm the EU partition size (64) and what it means for max concurrent EUs. Does one doca_dpa',
      '   (doca_dpa_create) host multiple doca_dpa_thread? Does each doca_dpa_thread occupy a distinct EU, and',
      '   does DOCA/FlexIO expose any thread->EU pinning/affinity (relevant to 6.5 "8 threads may not land on 8',
      '   distinct EUs")? Cite DOCA API facts visible in code/headers; flag what needs SDK-doc check.',
      '2. Enumerate EXACTLY the per-EU replication set to give each EU its own independent path: doca_dpa_thread,',
      '   dpa_thread_arg, producer_comp, consumer_comp, msgq (send+recv), producer, consumer, dpu_consumer_id.',
      '   Which are cheap to replicate Nx and which hit a hard limit (e.g. max_num_consumers=1 per msgq - does',
      '   that mean N msgqs needed)? CC_DPA_MAX_MSG_NUM budget across N EUs?',
      '3. The single-EU yield/wake (run_dma_manager doca_dpa_dev_thread_reschedule) and 1kHz keepalive - how',
      '   does wake/trigger work per-EU? Each EU needs its own trigger source.',
      '4. Sketch the minimal code-change surface (functions/structs) to go from 1 EU to N EUs with N',
      '   independent comch channels, mapping pods to EUs. Note what setup_pod_dma must branch on.',
      'Return structured findings. evidence MUST cite file:line / function names.',
    ].join('\n'),
  },
  {
    label: 'understand:host-posting',
    prompt: SHARED_CONTEXT + '\n\n' + [
      'YOUR AREA: Host-side descriptor posting & ring registration - does the host need changes for per-pod-EU?',
      'READ: ' + DOCA + '/ring.c, ' + ROOT + '/lib/cpp/src/thrift/transport/dpumesh_doca.c,',
      ROOT + '/lib/cpp/src/thrift/transport/dpumesh.h, and host-facing transport headers',
      ROOT + '/lib/cpp/src/thrift/transport/TDpumesh*.h. Also skim ' + ROOT + '/bench/echo_dpumesh.c and',
      ROOT + '/bench/bench_dpumesh.c for how a pod posts/consumes.',
      '',
      'ANSWER PRECISELY:',
      '1. Is the host a per-pod independent poster already (each pod = own process/container with own dma_ring),',
      '   or is there host-side shared serialization that would bound per-pod-EU gains?',
      '2. When a pod registers (REGISTER -> setup_pod_dma on DPU), what identifies which EU/channel it binds to?',
      '   Is pod_id -> EU mapping a pure DPU-side decision (host-transparent), or does the host need to know?',
      '3. Does anything in the host descriptor format (dma_desc, credit slot at index DMA_RING_SIZE) assume a',
      '   single DPA consumer? Would per-pod-EU change the host ABI at all?',
      '4. Confirm whether the MEMORY note "get_external_ptr load-bearing / cannot cache per-desc" or "DMA count',
      '   dominates (pack to 8KB)" constrain a multi-EU design.',
      'Return structured findings. evidence MUST cite file:line.',
    ].join('\n'),
  },
  {
    label: 'understand:arch-docs-bench',
    prompt: SHARED_CONTEXT + '\n\n' + [
      'YOUR AREA: Intended architecture + exhaustive extraction of the bench multi-EU evidence.',
      'READ FULLY: ' + ROOT + '/architecture/dpumesh_architecture.md, ' + ROOT + '/architecture/README.md. Then',
      'RE-READ the multi-EU / scaling parts of ' + ROOT + '/bench/bench.md (5.1, 5.12 single-pod/idle-ring',
      'effect, 6.2-6.5, 7 and the "HW limit" guide). Also ' + ROOT + '/TODO_remaining_work.md if it mentions',
      'DPA threading.',
      '',
      'ANSWER PRECISELY:',
      '1. Does the architecture doc describe an intended multi-EU / sharded design, a routing model that',
      '   assumes single-EU, or any constraint (e.g. all-pods-on-one-EU for cross-pod routing)? Extract the',
      '   pod->pod routing model and where the DPA fits.',
      '2. From bench: give exact measured multi-EU scaling numbers, the N=8 regression diagnosis state, and',
      '   5.12 analysis of idle-ring polling cost (ratio 1.017 when 4 rings active vs 2.0 single-pod) - this',
      '   bears on whether per-pod-EU (fewer active rings per EU) helps or wastes polls.',
      '3. What does 7 say is REQUIRED to realize the multi-EU lever, and what gain does it project?',
      '4. Identify contradictions between the user per-pod-EU idea and measured evidence (peak at 4 EU,',
      '   regression at 8 EU, idle-ring poll waste, yield-as-throttle necessity from 5.10).',
      'Return structured findings. evidence MUST cite bench.md section numbers or arch doc headings.',
    ].join('\n'),
  },
]

const maps = await parallel(understandTasks.map(t => () =>
  agent(t.prompt, { label: t.label, phase: 'Understand', schema: UNDERSTAND_SCHEMA })))

const validMaps = maps.filter(Boolean)
log('Understand: ' + validMaps.length + '/4 maps returned')

phase('Design')

const DESIGN_SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['mappingOptions', 'recommendedMapping', 'euBudget', 'dpuDrainSolution', 'replicationSet',
             'migrationSteps', 'expectedGain', 'hardBlockers', 'risks', 'openQuestions'],
  properties: {
    mappingOptions: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['name', 'unit', 'euCountFormula', 'pros', 'cons'],
        properties: {
          name: { type: 'string' },
          unit: { type: 'string', description: 'what each EU owns' },
          euCountFormula: { type: 'string' },
          pros: { type: 'array', items: { type: 'string' } },
          cons: { type: 'array', items: { type: 'string' } },
        },
      },
    },
    recommendedMapping: { type: 'string' },
    euBudget: {
      type: 'object', additionalProperties: false,
      required: ['available', 'usefulCeiling', 'reasoning'],
      properties: {
        available: { type: 'string' },
        usefulCeiling: { type: 'string', description: 'practical EU count before diminishing/negative returns' },
        reasoning: { type: 'string' },
      },
    },
    dpuDrainSolution: { type: 'string', description: 'how to avoid the 6.5 single-DPU-drain regression' },
    replicationSet: { type: 'array', items: { type: 'string' } },
    migrationSteps: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['step', 'files', 'risk'],
        properties: {
          step: { type: 'string' },
          files: { type: 'string' },
          risk: { type: 'string', enum: ['high', 'medium', 'low'] },
        },
      },
    },
    expectedGain: { type: 'string', description: 'realistic RPS/op-rate projection with caveats' },
    hardBlockers: { type: 'array', items: { type: 'string' } },
    risks: { type: 'array', items: { type: 'string' } },
    openQuestions: { type: 'array', items: { type: 'string' } },
  },
}

const designInput = validMaps.map(function (m, i) {
  return '=== MAP ' + (i + 1) + ': ' + m.area + ' ===\n' + JSON.stringify(m, null, 1)
}).join('\n\n')

const design = await agent(
  SHARED_CONTEXT + '\n\n' + [
    'You are the design synthesizer. Below are 4 structured understanding maps from agents that read the',
    'code and the benchmark. Use them as ground truth (cite their evidence).',
    '',
    designInput,
    '',
    'PRODUCE a concrete feasibility-grade design for DPA multithreading per the user proposal, but be honest:',
    'the user proposes per-pod/per-DPU EU. Evaluate that AND alternatives.',
    '',
    'REQUIREMENTS:',
    '- mappingOptions: at least (a) per-pod EU, (b) fixed EU pool with rings sharded across EUs (decoupled',
    '  from pod count), (c) per-direction (fwd EU / rev EU). For each give the EU-count formula and the',
    '  idle-ring poll-waste implication (bench 5.12: an EU polling mostly-idle rings wastes ~0.19us/op per',
    '  idle ring; per-pod EU with a self-loop pod = 2 active + N idle rings is the BAD case).',
    '- euBudget.usefulCeiling MUST reconcile "64 EUs available" with "measured peak at 4 EU, regression at 8".',
    '  The honest ceiling is ~4 EUs of USEFUL parallelism today, gated by the DPU drain - NOT 64.',
    '- dpuDrainSolution: the central engineering problem. Per 6.5 the DPU single-drain is the suspected cap.',
    '  Propose the matching DPU-side change (e.g. one drain thread + comp_queue + consumer PE per EU, pinned',
    '  to distinct ARM cores; ARM has 8 cores) and note the unverified risk.',
    '- Be explicit that per-pod EU does NOT help if pod count < EU peak and creates idle-ring poll waste; a',
    '  sharded fixed pool may be better. Recommend accordingly.',
    '- migrationSteps: minimal incremental path (e.g. first prove 2-EU with 2 pods each on own channel+drain',
    '  thread, measure, then generalize). Cite the exact functions/structs to touch.',
    'Return the structured design.',
  ].join('\n'),
  { label: 'design:synthesize', phase: 'Design', schema: DESIGN_SCHEMA })

phase('Verify')

const VERDICT_SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['lens', 'feasibilityVerdict', 'strongestObjection', 'flaws', 'missedConsiderations', 'confidence'],
  properties: {
    lens: { type: 'string' },
    feasibilityVerdict: { type: 'string', enum: ['feasible', 'feasible-with-caveats', 'high-risk', 'blocked'] },
    strongestObjection: { type: 'string' },
    flaws: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['flaw', 'severity', 'fix'],
        properties: {
          flaw: { type: 'string' },
          severity: { type: 'string', enum: ['critical', 'major', 'minor'] },
          fix: { type: 'string' },
        },
      },
    },
    missedConsiderations: { type: 'array', items: { type: 'string' } },
    confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
  },
}

const designStr = JSON.stringify(design, null, 1)

const verifyLenses = [
  {
    label: 'verify:n8-regression',
    lens: 'N=8 regression root cause & EU/HW limits',
    extra: [
      'Attack the euBudget and dpuDrainSolution. The bench REJECTED EU spin-contention (6.4) and narrowed to',
      'DPU-drain OR EU-oversubscription OR DMA-HW contention - but did NOT confirm which. If the real cause is',
      'shared DMA-engine/PCIe contention or EU-oversubscription (not DPU drain), then per-EU drain threads',
      'WONT fix it. Is the design betting on an unverified root cause? What cheap experiment (bench 6.5',
      'suggests top -H + dpa-statistics) must run FIRST before any code? Is "useful ceiling ~4 EU" defensible',
      'or could it be lower for the real (4-dma_copy-chain, lossless) dpumesh vs the (1-copy, lossy) pure_dma',
      'microbench?',
    ].join('\n'),
  },
  {
    label: 'verify:correctness-coherency',
    lens: 'Correctness / cache-coherency / routing integrity',
    extra: [
      'Attack migration correctness. bench 4.8 / 5.6-E4 / 5.10 show this system is brutally sensitive to PCIe',
      'cache-coherency: lossless rings + multi-EU busy-spin already caused slot-leak hangs (5.10), and',
      'shared-cache-line writeback corrupted neighbours (4.8). With N EUs each doing window_writeback, and',
      'cross-pod routing where pod A forward completion triggers a reverse DMA on pod B (different EU?), are',
      'there NEW coherency or ordering hazards? Does per-pod EU break reverse-DMA in-place forwarding',
      '(desc->mmap carries SENDER handle - sender and receiver may be on different EUs)? Does the MEMORY',
      '"get_external_ptr load-bearing" note bite? Trace the routing across EU boundaries.',
    ].join('\n'),
  },
  {
    label: 'verify:roi-altitude',
    lens: 'ROI / simpler alternatives / does this even help',
    extra: [
      'Attack whether this is worth it vs alternatives. bench 7 lists "Direct host->host DMA" (chain 4->2',
      'dma_copy, ~+100%) as a PEER lever that may be simpler and EU-count-independent. 5.1 says EU is 0.51%',
      'active - the cap is yield/wake + per-op work, not EU compute, so does adding EUs attack the actual',
      'bottleneck? For the CURRENT bench (2 pods), does per-pod EU (2 EUs) even change anything, given 4 rings',
      'are already amortized (ratio 1.017, 5.4)? Is the user idea solving a problem that only appears at higher',
      'pod counts? Whats the honest expected gain for the 2-pod bench vs a many-pod deployment?',
    ].join('\n'),
  },
]

const verdicts = await parallel(verifyLenses.map(function (v) {
  return function () {
    return agent(
      SHARED_CONTEXT + '\n\n' + [
        'You are an adversarial reviewer with the lens: "' + v.lens + '". Try hard to REFUTE the',
        'feasibility/quality of the design below. Default to skepticism; only concede what survives scrutiny.',
        '',
        '=== PROPOSED DESIGN ===',
        designStr,
        '',
        'SPECIFIC ATTACK FOR YOUR LENS:',
        v.extra,
        '',
        'Return your verdict. Be concrete and cite bench.md sections or file evidence where possible.',
      ].join('\n'),
      { label: v.label, phase: 'Verify', schema: VERDICT_SCHEMA })
  }
}))

return {
  maps: validMaps,
  design: design,
  verdicts: verdicts.filter(Boolean),
}
