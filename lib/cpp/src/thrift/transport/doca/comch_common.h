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
};

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
 * sender can release the TX slot tied to req_id. Pure event signal; DPU does
 * NOT communicate any flow-control position. 1-byte type (host dispatches by
 * reading a single byte). The old int32 dst_pod_id was set by the sender but
 * never read by the host (the slot is freed by req_id alone) — dropped. */
struct dmesh_tx_ack_msg {
    uint8_t  type;       /* = DMESH_MSG_FWD_ACK */
    uint8_t  _pad[3];    /* align req_id to its natural 4B boundary */
    uint32_t req_id;
};
_Static_assert(sizeof(struct dmesh_tx_ack_msg) == 8,
               "dmesh_tx_ack_msg must pack to 8B");

/* DPU→Host: reverse DMA (DPU→CPU) completion — data landed in Host RX buffer.
 *
 * Byte-identical to struct comch_dma_comp_msg (dpa_common.h): the DPA emits
 * that 16B packed struct to the DPU (DPA_MSG_REV_DONE) and the DPU relays the
 * SAME content up to the host. type is 1 byte (not the 4-byte enum) so the
 * layout matches the DPA struct exactly — only the type byte is rewritten
 * (DPA_MSG_REV_DONE → DMESH_MSG_REV_DONE) on relay. The host dispatches control
 * messages by reading type as a single byte, so the 1-byte type here and the
 * 4-byte enum on the other Family-A messages both dispatch correctly. */
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



struct dmesh_comch_msg {
    enum dmesh_msg_type type;
    union 
    {
        struct dmesh_mmap_msg mmap_msg;
    };
};
doca_error_t
export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction);
struct doca_comch_connection;
doca_error_t
process_mmap_msg(struct objects *objs, struct doca_comch_connection *conn,
                 struct dmesh_mmap_msg *mmap_msg);
#endif // COMCH_COMMON_H