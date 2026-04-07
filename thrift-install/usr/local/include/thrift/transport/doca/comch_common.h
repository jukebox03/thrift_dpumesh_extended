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

enum dmesh_msg_type {
    DMESH_MSG_EXPORT_DESC,
    DMESH_MSG_EXPORT_DPA_COMP,
    DMESH_MSG_RX_DATA,
    DMESH_MSG_REGISTER,          /* Host→DPU: register pod_id */
    DMESH_MSG_CONSUMER_ID,       /* DPU→Host: consumer ID reply */
    DMESH_MSG_TX_ACK,            /* DPU→Host: DMA completed, TX slot can be freed */
    DMESH_MSG_POD_CONSUMER_ID,   /* Host→DPU: advertise host datapath consumer ID */
};

/* DPU→Host: tell the client what consumer ID to use for producer */
struct dmesh_consumer_id_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_CONSUMER_ID */
    uint32_t consumer_id;
};

enum mmap_type {
    DMA_BUFFER = 1,
    DMA_RING = 2,
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

struct dmesh_dpa_comp_msg {
    enum dmesh_msg_type type;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_comch_consumer_t dpa_consumer;
};

struct dmesh_rx_data_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_RX_DATA (4B) */
    uint8_t desc[64];           /* sw_descriptor_t, opaque */
    uint32_t body_len;          /* payload length (4B) */
    uint8_t body[];             /* body data (flexible array) */
};
/* Header overhead: 72 bytes, max body = max_msg_size - 72 */

/* Host→DPU: register this connection's pod_id */
struct dmesh_register_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_REGISTER */
    int32_t pod_id;
    char app_name[64];
};

/* Host→DPU: advertise this pod's datapath consumer ID for DPU→Host payload sends */
struct dmesh_pod_consumer_id_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_POD_CONSUMER_ID */
    int32_t pod_id;
    uint32_t consumer_id;
};

/* DPU→Host: ACK for Host TX completion (keyed by req_id + dst_pod_id) */
struct dmesh_tx_ack_msg {
    enum dmesh_msg_type type;   /* = DMESH_MSG_TX_ACK */
    uint32_t req_id;
    int32_t dst_pod_id;
};



struct dmesh_comch_msg {
    enum dmesh_msg_type type;
    union 
    {
        struct dmesh_mmap_msg mmap_msg;
        struct dmesh_dpa_comp_msg dpa_comp_msg;
    };
};
doca_error_t
export_mmap_to_remote(struct objects *objs, struct doca_mmap *mmap, void *buffer, size_t buf_size, enum mmap_type mmap_type, enum msg_direction direction);
struct doca_comch_connection;
doca_error_t
process_mmap_msg(struct objects *objs, struct doca_comch_connection *conn,
                 struct dmesh_mmap_msg *mmap_msg);
doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg);
#endif // COMCH_COMMON_H