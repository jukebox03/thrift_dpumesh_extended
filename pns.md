# Proposal and Strategy (PNS): DPA DMA Completion Stall (Code-Verified)

## 1. Problem Definition
- Symptom: traffic reaches DPA, log shows "DMA copy submitted", but upper layers stop progressing (for example, recv remains 0/s on DPU side).
- Scope: the current blocking behavior is observed in the DPA DMA/completion path, not yet proven as a strict 128-byte payload boundary issue.

## 2. Verified Facts From Current Code

### A. DPA DMA completion metadata is intentionally small
- File: lib/cpp/src/thrift/transport/doca/dpa_common.h
- comch_dma_comp_msg is constrained to fit immediate data for dma_copy:
	- Static assert requires sizeof(struct comch_dma_comp_msg) <= 32.
	- Comments in kernel path also state a 32-byte immediate data limit for this API usage.
- Implication: in this path, immediate data carries completion metadata only, not full payload.

### B. Completion channel wiring is present and internally consistent
- File: lib/cpp/src/thrift/transport/doca/dpa.c
- DPA producer completion is:
	- created (doca_dpa_completion_create),
	- bound to DPA thread (doca_dpa_completion_set_thread),
	- started (doca_dpa_completion_start),
	- attached to producer (doca_comch_producer_dpa_completion_attach).
- The same completion handle is exported into dpa_thread_arg and consumed in DPA kernel.

### C. DPA kernel has two blocking wait points
- File: lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c
- In process_one_desc:
	- waits until producer has credits (producer_is_consumer_empty loop),
	- then submits doca_dpa_dev_comch_producer_dma_copy,
	- then spin-waits on dpa_producer_comp completion.
- Either wait can stall forward progress if events/credits never arrive.

### D. DPU ARM completion consumer exists
- File: lib/cpp/src/thrift/transport/doca/dpa.c
- dmesh_doca_dpa_msgq_recv_cb handles COMCH_MSG_TYPE_DMA_COMPLETED and performs:
	- TX_ACK to source pod,
	- RX_DATA forwarding to destination pod.
- If this callback does not fire, the end-to-end pipeline appears frozen above DPA.

## 3. What Is Not Proven (Yet)
- A hard 128-byte threshold as the primary root cause for the current stall in this specific DPA DMA completion path.
- In particular, the DPA dma_copy immediate payload here is already <= 32 bytes, so stall cannot be explained solely by oversized immediate metadata.

## 4. Most Likely Failure Modes (Current Priority)
1. Credit starvation before dma_copy submit (producer_is_consumer_empty loop does not exit).
2. Completion not observed on dpa_producer_comp after submit (get_completion loop does not exit).
3. Downstream DPU callback path not consuming DMA_COMPLETED even when DMA completes.

## 5. Strategy (Updated)

### Strategy 1: Instrument the exact stall point
- Add loop heartbeat counters/timestamps around:
	- credit wait loop,
	- completion wait loop,
	- dmesh_doca_dpa_msgq_recv_cb entry/exit.
- Goal: distinguish "submit never happened" vs "submit happened, completion missing" vs "completion delivered but not consumed".

### Strategy 2: Bound unproductive spin waits
- Replace infinite waits with bounded retry + yield/reschedule + diagnostic logs.
- Goal: avoid hard deadlock and preserve observability under fault.

### Strategy 3: Keep metadata path minimal and explicit
- Keep comch_dma_comp_msg within immediate size limits (already enforced).
- Do not route payload through immediate data; payload stays in DMA buffer path.

## 6. Implementation Plan
1. Add targeted diagnostics around both DPA wait loops and callback consume points.
2. Run small and large payload tests and compare where progress stops.
3. If completion queue starvation is confirmed, adjust completion/credit flow first before revisiting payload-size hypotheses.
