#ifndef OBJECT_H_
#define OBJECT_H_

#include <pthread.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#include "comch_server.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct dma_ring;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

#define MAX_CONSUMERS 16
#define MAX_PODS 8

/* Per-pod state (DPU only) */
struct pod_state {
    struct doca_comch_connection *connection;
    int32_t pod_id;
    char app_name[64];
    int registered;         /* 1 = DMESH_MSG_REGISTER received */
    int dma_ready;          /* 1 = both mmaps arrived, DPA ring added */

    /* Per-pod mmap (Host에서 export) */
    struct doca_mmap *ring_mmap;
    struct doca_mmap *remote_mmap;
    void *remote_addr;
    size_t remote_buf_size;

    /* Per-pod DPA buffer array (ring에 매핑) */
    struct doca_buf_arr *buf_arr;

    /* Per-pod local DMA buffer (DPU working buffer) */
    struct doca_mmap *local_mmap;
    void *dma_buffer;
};

struct objects {
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    union {
        struct doca_comch_server *cc_server;
        struct doca_comch_client *cc_client;
    };
    struct doca_comch_connection *connection;  /* primary (first) connection — backward compat */

    /* Host-only fields (used by dpumesh_doca.c client side) */
    struct doca_mmap *local_mmap;
    struct doca_mmap *remote_mmap;
    void *dma_buffer;
    void *remote_addr;
    size_t remote_buf_size;
    struct dma_ring *dma_ring;
    struct doca_mmap *ring_mmap;    /* used for DMA ring mmap */

    struct doca_buf_arr *buf_arr;

    /* DPA (shared, 1 thread for all pods) */
    struct dmesh_doca_dpa_thread *dpa_thread;
	struct dmesh_doca_dpa_comch *dpa_comch;
    doca_dpa_dev_comch_producer_t remote_dpa_producer;
    doca_dpa_dev_completion_t remote_dpa_producer_comp;
    int dpa_thread_running;  /* 1 = DPA thread started */

    /* comch control path related */
    bool server_finish;             /* Controls whether server progress loop should be run */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;

    struct local_mem_bufs *producer_mem;
    struct doca_comch_producer *producer;
    struct doca_pe *producer_pe;

    uint32_t remote_consumer_id;
    doca_error_t producer_result;		  /* Holds result will be updated in producer callbacks */
	bool producer_finish;			  /* Controls whether producer progress loop should be run */
	doca_error_t consumer_result;		  /* Holds result will be updated in consumer callbacks */
	bool consumer_finish;			  /* Controls whether consumer progress loop should be run */

    int recv_msg_cnt;                  /* Counts number of messages received by consumer */
    int sent_msg_cnt;

    long unsigned int start_time_ns;
    long unsigned int end_time_ns;

    /* RX data hook (comch control path → dpumesh_ctx) */
    void (*rx_data_hook)(void *hook_ctx, const uint8_t *data, uint32_t len);
    void *rx_hook_ctx;

    /* TX ACK hook (comch control path → dpumesh_ctx) */
    void (*tx_ack_hook)(void *hook_ctx, const uint8_t *data, uint32_t len);
    void *tx_ack_hook_ctx;

    /* Multi-pod table (DPU only) */
    struct pod_state pods[MAX_PODS];
    int num_pods;
    pthread_mutex_t pods_lock;
};

void
cleanup_objects(struct objects *objs);

#endif // OBJECT_H_
