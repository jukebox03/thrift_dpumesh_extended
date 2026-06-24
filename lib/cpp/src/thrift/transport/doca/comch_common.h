#ifndef COMCH_COMMON_H
#define COMCH_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <doca_error.h>
#include <doca_mmap.h>

struct objects;

enum msg_direction {
    HOST_TO_DPU = 0,
    DPU_TO_HOST = 1,
};

/* Control-channel message types (Host ↔ DPU ARM, over the DOCA Comch control
 * path). Explicit values, contiguous from 1; 0 is reserved INVALID so a zeroed
 * buffer never decodes to a live type. The verb vocabulary (POD_, MMAP_, FWD_,
 * REV_) is shared with the DPU<->DPA datapath enum (enum dpa_msg_type) for a
 * consistent naming scheme across both channels. The type travels on the wire
 * as the low byte of this field; the host dispatches by reading a single byte
 * (little-endian), so values must stay < 256. */
enum dmesh_msg_type {
    DMESH_MSG_INVALID      = 0, /* reserved: zeroed buffer is never a live type */
    DMESH_MSG_POD_REGISTER = 1, /* Host→DPU: register this connection's pod_id */
    DMESH_MSG_MMAP_EXPORT  = 2, /* Host→DPU: export an mmap region (ring / TX buf / RX buf) */
    DMESH_MSG_FWD_ACK      = 3, /* DPU→Host: forward DMA (CPU→DPU) consumed — free TX slot */
    DMESH_MSG_REV_DONE     = 4, /* DPU→Host: reverse DMA (DPU→CPU) done — data in Host RX buf */
    DMESH_MSG_BATCH_FWD_ACK= 5, /* DPU→Host: batch of req_ids whose forward DMA is done — free all */
    DMESH_MSG_BATCH_REV_DONE=6, /* DPU→Host: batch of reverse-DMA completions — deliver all */
};

/* DPU→Host: batched TX_ACK. Coalesces up to BATCH_TXACK_MAX per-request
 * FWD_ACKs into one comch message so the host PE thread processes 1 message
 * instead of K. Flushed when full or on a periodic tail-flush. */
#define BATCH_TXACK_MAX 14
struct dmesh_batch_tx_ack_msg {
    uint8_t  type;       /* = DMESH_MSG_BATCH_FWD_ACK */
    uint8_t  count;      /* number of valid entries in req_ids[] (1..BATCH_TXACK_MAX) */
    uint8_t  _pad[2];    /* align req_ids to 4B */
    uint32_t req_ids[BATCH_TXACK_MAX];
};
_Static_assert(sizeof(struct dmesh_batch_tx_ack_msg) == 4 + 4 * BATCH_TXACK_MAX,
               "dmesh_batch_tx_ack_msg must pack tightly");

/* DPU→Host: batched REV_DONE. Coalesces up to BATCH_REVDONE_MAX per-response
 * reverse-DMA completions into one comch message so the host PE thread reaps 1
 * message per K responses instead of K — the per-RTT PE reap is the 2-pod cap.
 * Each entry mirrors the dmesh_dma_completion_msg payload minus the type byte. */
#define BATCH_REVDONE_MAX 16
struct dmesh_rev_done_entry {
    int8_t   flags;
    int8_t   src_pod_id;
    int8_t   dst_pod_id;
    uint8_t  _pad;
    uint32_t pos;
    uint32_t length;
    uint32_t req_id;
};
_Static_assert(sizeof(struct dmesh_rev_done_entry) == 16, "dmesh_rev_done_entry must pack to 16B");
struct dmesh_batch_rev_done_msg {
    uint8_t  type;       /* = DMESH_MSG_BATCH_REV_DONE */
    uint8_t  count;      /* number of valid entries (1..BATCH_REVDONE_MAX) */
    uint8_t  _pad[2];    /* align entries to 4B */
    struct dmesh_rev_done_entry entries[BATCH_REVDONE_MAX];
};
_Static_assert(sizeof(struct dmesh_batch_rev_done_msg) == 4 + 16 * BATCH_REVDONE_MAX,
               "dmesh_batch_rev_done_msg must pack tightly");

enum mmap_type {
    DMA_BUFFER = 1,
    DMA_RING = 2,
    DMA_HOST_RX_BUFFER = 3, /* Host RX buffer for DPU→CPU reverse DMA */
};

struct dmesh_mmap_msg {
    enum dmesh_msg_type type;
    enum mmap_type mmap_type;
    void *host_addr;
    size_t buf_size;
    size_t export_desc_len;
    uint8_t export_desc[];
};

typedef uint64_t doca_dpa_dev_comch_consumer_completion_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_comch_consumer_t;

/* Host→DPU: register this connection's pod_id */
struct dmesh_register_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_POD_REGISTER */
    int32_t pod_id;
    char app_name[64];
};

/* DPU→Host: per-request notification that forward DMA (CPU→DPU) is done —
 * sender can release the TX slot tied to req_id. Pure event signal. 1-byte
 * type (host dispatches by reading a single byte). The slot is freed by
 * req_id alone. */
struct dmesh_tx_ack_msg {
    uint8_t  type;       /* = DMESH_MSG_FWD_ACK */
    uint8_t  _pad[3];    /* align req_id to its natural 4B boundary */
    uint32_t req_id;
};
_Static_assert(sizeof(struct dmesh_tx_ack_msg) == 8,
               "dmesh_tx_ack_msg must pack to 8B");

/* DPU→Host: reverse DMA (DPU→CPU) completion — data landed in Host RX buffer.
 * Byte-identical to struct comch_dma_comp_msg (dpa_common.h): the DPA emits
 * that 16B packed struct to the DPU and the DPU relays the SAME content up to
 * the host, rewriting only the type byte (DPA_MSG_REV_DONE → DMESH_MSG_REV_DONE)
 * on relay. type is 1 byte (not the 4-byte enum) so the layout matches the DPA
 * struct exactly; the host dispatches by reading type as a single byte. */
struct dmesh_dma_completion_msg {
    uint8_t  type;        /* = DMESH_MSG_REV_DONE */
    int8_t   flags;       /* OP_REQUEST/OP_RESPONSE + CASE_* */
    int8_t   src_pod_id;
    int8_t   dst_pod_id;
    uint32_t pos;         /* offset in Host RX DMA buffer */
    uint32_t length;      /* DMA'd body length */
    uint32_t req_id;
};
_Static_assert(sizeof(struct dmesh_dma_completion_msg) == 16,
               "dmesh_dma_completion_msg must pack to 16B (mirrors comch_dma_comp_msg)");



/* Type-peek wrapper: control-path recv buffers are cast to this to read the
 * leading type, then re-cast to the concrete message struct (mmap/register). */
struct dmesh_comch_msg {
    enum dmesh_msg_type type;
};
doca_error_t
export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction);
struct doca_comch_connection;
doca_error_t
process_mmap_msg(struct objects *objs, struct doca_comch_connection *conn,
                 struct dmesh_mmap_msg *mmap_msg);
#endif // COMCH_COMMON_H