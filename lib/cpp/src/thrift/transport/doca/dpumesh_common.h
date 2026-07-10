#ifndef DPUMESH_COMMON_H
#define DPUMESH_COMMON_H

/* ====== DOCA / DPA limits ====== */
#define MAX_DPA_RINGS       8   /* per-EU forward ring capacity */
#define MAX_PODS            8

/* EU-sharding: a pod spreads its forward traffic across K rings, each on
 * a distinct EU, so a 2-pod pair can drive >2 EUs. K = DPUMESH_RINGS_PER_POD.
 * Default 2 = the measured production config (K=2 drives 4 EUs for a 2-pod pair);
 * set to 1 for the legacy single-ring-per-pod path. MAX_EU_PER_POD bounds the
 * per-pod ring arrays; a pod shards across at most min(K, num_dpa_threads) EUs. */
#define MAX_EU_PER_POD      MAX_DPA_RINGS
#define DPUMESH_RINGS_PER_POD_DEFAULT 2

/* pod_id is int8 on the wire (valid ids [0,127]); this sizes the pod_id->slot
 * map to cover that id space. Always >= MAX_PODS. Widen together with the int8
 * wire fields if pod_id ever needs to exceed 127. */
#define POD_ID_SPACE        128

/* DPU-side DMA staging buffer per pod: the forward host→DPU hop, from which the
 * ARM SG-DMA egress reads bodies in place. MUST equal DPUMESH_NUM_SLOTS_DEFAULT ×
 * DPUMESH_SLOT_SIZE_DEFAULT — each conn's staging MIRRORS its host TX byte-ring
 * offset-for-offset (moff = desc->addr - host_addr), so staging occupancy is
 * bounded by the source's own host TX buffer and never overflows. */
#define DPU_BUFFER_SIZE     (32 * 1024 * 1024)  /* 32MB = 4096 × 8KB */
#define DPUMESH_SLOT_SIZE   8192               /* matches DPUMESH_SLOT_SIZE_DEFAULT */
/* DMA descriptor ring depth (host→DPU forward). Mirrored from ring.h so
 * the DPA kernel — which can't include ring.h — knows the ring length.
 * Host's setup_dma_ring allocates this many slots PLUS 1 extra for the
 * credit counter at index DMA_RING_SIZE. */
#define DMA_RING_SIZE       4096

/* ====== Endpoint addressing — oriented-tuple model ======
 * A message carries src=(pod,port[,service]) and dst=(service,pod,port). MODEL B
 * (the DPU owns every connection): a CLIENT always sends dst_pod=BLANK and the DPU
 * resolves dst_service -> a backend pod PER MESSAGE (dpu_route mock, future L7;
 * per-message load balancing), owning the upstream. A backend REPLY carries a
 * concrete dst_pod (its DPU-facing peer) -> delivered direct, no re-routing.
 *   service_id : own int8 space [0,127]   (declared by the host at register)
 *   pod_id     : own int8 space [0,127]   (ASSIGNED BY THE DPU at register)
 * service_id and pod_id are SEPARATE fields (not a shared/partitioned namespace). */
#define DMESH_POD_BLANK     (-1)   /* dst_pod == -1 -> DPU must resolve dst_service */
#define DMESH_PORT_BLANK     0     /* dst_port == 0 -> service listener / accept queue */
#define DMESH_SVC_NONE      (-1)   /* no service id */

/* Model B (the DPU owns every connection): DPU-assigned upstream connection ids
 * live in [DMESH_UPORT_BASE, 65535]; host client conns use [1, DMESH_UPORT_BASE).
 * The split lets a host that is BOTH client and backend (loopback) keep both
 * kinds of conn in its one ports[] table with no number collision. Shared by the
 * host (dpm.h / dpumesh_doca.c) and the DPU (object.h / dpu_worker.c). */
#define DMESH_UPORT_BASE    32768

#endif /* DPUMESH_COMMON_H */
