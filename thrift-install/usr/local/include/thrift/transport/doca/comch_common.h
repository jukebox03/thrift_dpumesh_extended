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
    DMESH_MSG_BATCH_FWD_ACK= 3, /* DPU→Host: batch of (port,seq) keys whose forward DMA is done — free all */
    DMESH_MSG_BATCH_REV_DONE=4, /* DPU→Host: batch of reverse-DMA completions — deliver all */
};

/* DPU→Host: batched TX_ACK. Coalesces up to BATCH_TXACK_MAX per-request
 * FWD_ACKs into one comch message so the host PE thread processes 1 message
 * instead of K. Flushed when full or on the idle (drain-empty) flush. */
#define BATCH_TXACK_MAX 14
/* DPU->Host TX_ACK frees the SENDER's TX slot. Keyed by the SOURCE endpoint
 * (port,seq) of the acked forward leg. The port's range (client-ephemeral vs
 * server-accepted, both from one host-unique pool) keeps client/server keys
 * disjoint even when both roles live on the same (loopback) host. */
struct dmesh_tx_ack_entry {
    uint16_t port;       /* source endpoint port of the acked leg */
    uint16_t seq;        /* its sequence */
};
struct dmesh_batch_tx_ack_msg {
    uint8_t  type;       /* = DMESH_MSG_BATCH_FWD_ACK */
    uint8_t  count;      /* number of valid entries in acks[] (1..BATCH_TXACK_MAX) */
    uint8_t  _pad[2];    /* align acks to 4B */
    struct dmesh_tx_ack_entry acks[BATCH_TXACK_MAX];
};
_Static_assert(sizeof(struct dmesh_batch_tx_ack_msg) == 4 + 4 * BATCH_TXACK_MAX,
               "dmesh_batch_tx_ack_msg must pack tightly");

/* DPU→Host: batched REV_DONE. Coalesces up to BATCH_REVDONE_MAX per-response
 * reverse-DMA completions into one comch message so the host PE thread reaps 1
 * message per K responses instead of K — the per-RTT PE reap is the 2-pod cap.
 * Each entry mirrors the comch_dma_comp_msg payload minus the type byte. */
#define BATCH_REVDONE_MAX 16
struct dmesh_rev_done_entry {
    int8_t   src_pod_id;   /* sender pod (the peer, for the receiving conn) */
    int8_t   src_service;  /* caller service */
    int8_t   dst_service;  /* callee service (selects local accept queue when dst_port==BLANK) */
    uint8_t  _pad;
    uint16_t src_port;     /* sender port */
    uint16_t dst_port;     /* dest port: PORT_BLANK -> accept queue, else socket lookup */
    uint16_t seq;          /* per-conn sequence (match key with dst_port) */
    uint16_t length;       /* payload length (<= slot_size) */
    uint32_t pos;          /* landing byte-offset in host RX buffer */
};  /* dst_pod omitted: host demuxes by dst_port; peer is captured from src_* */
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
    int32_t service_id;         /* this node's service id; DPU sets service_table[service_id]=pod_id
                                 * (SVC_NONE = client-only, no service to advertise) */
    char app_name[64];
};



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