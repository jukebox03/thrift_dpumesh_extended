# Proposal and Strategy (PNS): Resolving the 128B Data Limit

## 1. Problem Definition
*   **Issue**: Thrift requests or responses larger than 128 bytes fail or cause the system to hang.
*   **Observation**: The Host-to-DPU path (using DMA) works correctly, but the **DPU-to-Host (Service/Gateway)** path is restricted by a 128-byte hardware limit.

## 2. Root Cause Analysis
The system utilizes three distinct communication methods, but the DPU-to-Host delivery is failing due to configuration omissions and path misuse.

### A. Missing Data Path Configuration (Primary Cause)
*   **File**: `lib/cpp/src/thrift/transport/doca/comch_producer.c`
*   **Root Cause**: The `doca_comch_producer` is initialized without explicitly calling `doca_comch_producer_set_max_msg_size()`.
*   **Consequence**: The hardware defaults the MTU to **128 bytes**. Even though `comch_datapath_send_payload` is designed for large transfers, it is being throttled by this hardware-default limit.

### B. Misuse of Control Path (Fallback Path Issue)
*   **File**: `lib/cpp/src/thrift/transport/doca/comch_server.c`
*   **Root Cause**: The function `server_send_rx_data` attempts to send Thrift payloads inline via the **Control Path** (`doca_comch_server_task_send`).
*   **Consequence**: The Control Path has a strict hardware MTU of **128 bytes** (including headers). After the 72-byte `dmesh_rx_data_msg` header, only **56 bytes** remain for actual data.

### C. DPA Immediate Data Constraints
*   **File**: `lib/cpp/src/thrift/transport/doca/device/dpa_kernel.c`
*   **Detail**: The `doca_dpa_dev_comch_producer_dma_copy` function uses "Immediate Data" for completion signals, which is hardware-limited to **128 bytes**. While currently used for small metadata (~52B), this path cannot be used for payload delivery.

## 3. Communication Matrix (Current State)

| Component | Inbound Method | Outbound Method | 128B Limit? |
| :--- | :--- | :--- | :--- |
| **Gateway (Host)** | Comch Data/Ctrl | **DMA** + Doorbell | **Yes (on Recv)** |
| **DPU (ARM)** | Ctrl Path / DPA Signal | Data Path (Producer) | **Yes (Default MTU)** |
| **DPA (Device)** | DMA | Comch MsgQ (Imm Data) | **Yes (Hardware)** |
| **Service (Host)** | Comch Data/Ctrl | **DMA** + Doorbell | **Yes (on Recv)** |

## 4. Proposed Solution Strategy

### Strategy 1: Explicit Data Path Sizing
*   Modify `comch_producer.c` and `comch_consumer.c` to explicitly set `max_msg_size` to **64KB** (or higher) during initialization. This unlocks the full capacity of Method 2 (Data Path).

### Strategy 2: Standardize on Data Path
*   Deprecate the Control Path-based payload delivery (`server_send_rx_data`).
*   Unify all DPU-to-Host data forwarding to use the high-capacity **Data Path (Method 2)**.

### Strategy 3: Zero-Copy Optimization (Future)
*   Implement **RDMA WRITE** from the DPU directly into Host RX slots, using the Control Path only for minimal doorbell signals (~32 bytes).

## 5. Implementation Plan
1.  **Fix Producer MTU**: Add `doca_comch_producer_set_max_msg_size` call in `init_comch_datapath_producer`.
2.  **Verify Forwarding Logic**: Ensure `dpa.c` correctly utilizes the reconfigured Data Path for all pod-to-pod routing.
3.  **Validation**: Update `test_thrift.py` to send 1KB+ payloads and confirm successful end-to-end delivery.
