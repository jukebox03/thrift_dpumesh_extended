#ifndef DPA_H_
#define DPA_H_

#include <doca_dpa.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <doca_buf_array.h>
// #include <doca_dpa_dev.h>
// #include <doca_dpa_dev_comch_msgq.h>

#define CC_DPA_MAX_MSG_NUM  512

struct objects;

/* DOCA DPA thread related objects */
struct dmesh_doca_dpa_thread {
    struct doca_dpa *dpa;           /* DOCA DPA */
    struct doca_dpa_thread *thread; /* DPA thread */
    doca_dpa_dev_uintptr_t arg;     /* argument to be used by DPA thread */
    doca_dpa_dev_uintptr_t buf;     /* buffer to be used by DPA thread */
	doca_dpa_dev_buf_arr_t dpa_buf_arr; /* DPA buffer array */
};

struct dmesh_doca_dpa_msgq {
	struct doca_pe *pe;
	struct doca_comch_msgq *msgq;	      /**< The DOCA Comch MsgQ */
	struct doca_comch_producer *producer; /**< The DOCA Comch Producer */
	struct doca_comch_consumer *consumer; /**< The DOCA Comch Consumer */
	bool is_send;			      /**< Indicates if MsgQ is used for sending from DPU to DPA */
	
	/* variables to measure latency of msgq */	
	long unsigned int send_start_time_ns;  /**< Timestamp when the first send message is posted */
	long unsigned int send_end_time_ns;	/**< Timestamp when the last send message is completed */
	int msg_cnt;
	uint64_t total_ns;
	bool completed;
};

struct dmesh_doca_dpa_comch {
	struct dmesh_doca_dpa_msgq send;			      /**< MsgQ used to send message from DPU to DPA */
	struct doca_dpa_completion *producer_comp;	      /**< The producer completion context used by DPA */
	struct dmesh_doca_dpa_msgq recv;			      /**< MsgQ used to receive message DPA */
	struct doca_comch_consumer_completion *consumer_comp; /**< The consumer completion context used by DPA */
};

struct dmesh_doca_dpa_msgq_create_attr {
	struct doca_dev *dev; /**< A doca device representing the emulation manager */
	struct doca_dpa *dpa; /**< DOCA DPA for accessing DPA resources */
	bool is_send;	      /**< If MsgQ is used to send to DPA or receive from DPA */
	uint32_t max_num_msg; /**< The maximal number of messages that can be sent/received */
	struct doca_comch_consumer_completion *consumer_comp; /**< Consumer completion context used by DPA to poll
								 arrival of messages */
	struct doca_dpa_completion *producer_comp; /**< Producer completion context used by DPA to poll completion of
						      send message */
	struct doca_pe *pe;			   /**< Progress engine to be used by DPU consumer and producer */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb; /**< Callback invoked once consumer/producer state
								   changes */
	void *ctx_user_data; /**< The user data to associate with the producer/consumer */
};

void dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb(const union doca_data user_data,
							  struct doca_ctx *ctx,
							  enum doca_ctx_states prev_state,
							  enum doca_ctx_states next_state);

doca_error_t
init_dpa_objects(struct objects *objs);

doca_error_t
launch_dpa_kernel(struct dmesh_doca_dpa_thread *dpa_thread);

doca_error_t
dmesh_doca_dpa_msgq_create(const struct dmesh_doca_dpa_msgq_create_attr *attr,
                            struct dmesh_doca_dpa_msgq *msgq);

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread);

struct objects;
doca_error_t
dmesh_doca_dpa_comch_create(struct objects *objs);

doca_error_t
dmesh_doca_run_dpa_thread(struct objects *objs, struct dmesh_doca_dpa_thread *dpa_thread, struct dmesh_doca_dpa_comch *comch);

doca_error_t 
dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size);

doca_error_t 
dmesh_doca_dpa_msgq_send_bulk(struct dmesh_doca_dpa_msgq *msgq, uint32_t num_msg,
                                void *msg, uint32_t msg_size);

doca_error_t
setup_dpa_buf_array(struct objects *objs, size_t num_elem, struct doca_mmap *mmap);					
#endif