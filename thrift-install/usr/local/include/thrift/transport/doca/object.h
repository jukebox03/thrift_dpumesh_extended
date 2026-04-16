#ifndef OBJECT_H_
#define OBJECT_H_

#include <pthread.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>

#include "comch_server.h"
#include "dpumesh_common.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct dma_ring;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

#define MAX_CONSUMERS 16

/* Deferred completion queue — DPU only.
 * Consumer callback enqueues; main loop drains.
 * Single-threaded (same DPU worker), so no lock needed. */
#define DPU_COMP_QUEUE_SIZE 4096

#define COMP_ENTRY_FORWARD     0  /* Forward DMA completed (CPU→DPU), needs TX_ACK + reverse route */
#define COMP_ENTRY_REV_NOTIFY  1  /* Reverse DMA completed (DPU→CPU), needs Host notification */

typedef struct {
    uint8_t  entry_type;   /* COMP_ENTRY_FORWARD or COMP_ENTRY_REV_NOTIFY */
    int32_t  src_pod_id;
    int32_t  dst_pod_id;
    uint32_t req_id;
    uint32_t length;
    int8_t   flags;
    uint32_t buf_offset;   /* FORWARD: offset in pod's RX DMA buffer; REV_NOTIFY: pos in Host RX buf */
    int32_t  pod_idx;      /* FORWARD: index into pods[]; REV_NOTIFY: unused (-1) */
} dpu_comp_entry_t;

typedef struct {
    dpu_comp_entry_t entries[DPU_COMP_QUEUE_SIZE];
    uint32_t head;  /* dequeue index */
    uint32_t tail;  /* enqueue index */
} dpu_comp_queue_t;

static inline int comp_queue_full(const dpu_comp_queue_t *q) {
    return ((q->tail + 1) % DPU_COMP_QUEUE_SIZE) == q->head;
}

static inline int comp_queue_empty(const dpu_comp_queue_t *q) {
    return q->head == q->tail;
}

static inline int comp_queue_enqueue(dpu_comp_queue_t *q, const dpu_comp_entry_t *e) {
    if (comp_queue_full(q)) return -1;
    q->entries[q->tail] = *e;
    q->tail = (q->tail + 1) % DPU_COMP_QUEUE_SIZE;
    return 0;
}

static inline dpu_comp_entry_t *comp_queue_peek(dpu_comp_queue_t *q) {
    if (comp_queue_empty(q)) return NULL;
    return &q->entries[q->head];
}

static inline void comp_queue_dequeue(dpu_comp_queue_t *q) {
    if (!comp_queue_empty(q))
        q->head = (q->head + 1) % DPU_COMP_QUEUE_SIZE;
}

static inline uint32_t comp_queue_usage(const dpu_comp_queue_t *q) {
    if (q->tail >= q->head)
        return q->tail - q->head;
    return DPU_COMP_QUEUE_SIZE - q->head + q->tail;
}

/* Backpressure threshold: defer recv task resubmission when queue
 * exceeds this fraction (3/4) to slow DPA inflow. */
#define COMP_QUEUE_BP_HIGH  (DPU_COMP_QUEUE_SIZE * 3 / 4)
#define COMP_QUEUE_BP_LOW   (DPU_COMP_QUEUE_SIZE / 2)

/* Max deferred recv tasks (one per CC_DPA_MAX_MSG_NUM) */
#define MAX_DEFERRED_RECV  1024

/* Per-pod state (DPU only) */
struct pod_state {
    struct doca_comch_connection *connection;
    int32_t pod_id;
    char app_name[64];
    int registered;         /* 1 = DMESH_MSG_REGISTER received */
    int dma_ready;          /* 1 = both mmaps arrived, DPA ring added */
    uint32_t remote_consumer_id; /* Host datapath consumer ID (for DPU->Host payload) */

    /* Per-pod mmap (Host에서 export, CPU→DPU forward direction) */
    struct doca_mmap *ring_mmap;
    struct doca_mmap *remote_mmap;   /* Host TX buffer mmap */
    void *remote_addr;
    size_t remote_buf_size;

    /* Per-pod DPA buffer array (forward ring에 매핑) */
    struct doca_buf_arr *buf_arr;

    /* Per-pod RX DMA buffer (DPU receives CPU→DPU data here) */
    struct doca_mmap *local_mmap;
    void *dma_buffer;
    size_t dma_buf_size;

    /* === Reverse direction (DPU→CPU) === */

    /* DPU TX buffer (DPU writes data here for DPA to DMA to Host) */
    struct doca_mmap *tx_mmap;
    void *tx_buffer;
    size_t tx_buf_size;

    /* DPU→CPU descriptor ring */
    struct dma_ring *tx_ring;
    struct doca_mmap *tx_ring_mmap;
    struct doca_buf_arr *tx_buf_arr;

    /* Host RX buffer mmap (exported from Host, DPA DMAs into this) */
    struct doca_mmap *host_rx_mmap;
    void *host_rx_addr;
    size_t host_rx_buf_size;

    /* === Flow control state === */

    /* As receiver (CPU→DPU): tracks consumption of RX DMA buffer */
    uint32_t rx_consumer_tail;

    /* As sender (DPU→CPU): tracks production into TX buffer */
    uint32_t tx_producer_head;
    uint32_t tx_last_consumer_tail; /* last known Host RX consumption from received header */
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

    /* Multi-pod table (DPU only) */
    struct pod_state pods[MAX_PODS];
    int num_pods;
    pthread_mutex_t pods_lock;

    /* Deferred completion queue (DPU only) */
    dpu_comp_queue_t comp_queue;

    /* Backpressure: deferred consumer recv tasks (DPU only).
     * When comp_queue is nearly full, consumer callbacks defer recv task
     * resubmission here. DPA sees consumer_empty and pauses. Main loop
     * resubmits when queue drains below BP_LOW. */
    struct doca_task *deferred_recv[MAX_DEFERRED_RECV];
    int num_deferred_recv;
};

/* Progress both PEs: control-path PE + consumer PE.
 * consumer_pe must be progressed here to resubmit DPA recv tasks during
 * server_send_msg_to_conn retry loops. Without this, DPA exhausts consumer
 * credits and permanently stalls under load.
 * Safe because server_send_msg_to_conn is only called from:
 *   - process_completion_queue (main loop, not inside any callback)
 *   - server_message_recv_callback (pe callback, not consumer_pe callback)
 * So consumer_pe is never re-entered. */
static inline void progress_all_pes(struct objects *objs) {
    doca_pe_progress(objs->pe);
    if (objs->consumer_pe)
        doca_pe_progress(objs->consumer_pe);
}

void
cleanup_objects(struct objects *objs);

#endif // OBJECT_H_
