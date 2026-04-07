#ifndef BUFFER_H
#define BUFFER_H

#include <doca_buf.h>
#include <doca_buf_pool.h>
#include <doca_buf_inventory.h>
#include <doca_mmap.h>
#include <doca_dev.h>
#include <doca_log.h>
#include <doca_error.h>
#include <stdbool.h>

#define CACHE_ALIGN 64 /* Cache line alignment for performance */

enum buf_inv_type {
	BUF_INV_TYPE_INVENTORY = 0,
	BUF_INV_TYPE_POOL = 1
};

struct local_mem_bufs {
	void *mem;								/* Memory address for DOCA buf mmap */
	struct doca_mmap *mmap;					/* DOCA mmap object */
	union
	{
		struct doca_buf_inventory *buf_inv;	/* DOCA buf inventory object */
		struct doca_buf_pool *bpool;		/* DOCA buf pool object */
	};
	uint8_t buf_inv_type;					/* DOCA buf inventory type */
	bool need_alloc_mem;		    		/* Whether need to allocate memory */
};

doca_error_t init_local_mem_bufs(struct local_mem_bufs *local, struct doca_dev *dev, 
								uint8_t buf_inv_type, size_t buf_len, size_t max_bufs);

void clean_local_mem_bufs(struct local_mem_bufs *local);


doca_error_t
alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                        void **buffer, size_t buffer_size, uint32_t access_mask);
						
doca_error_t
destroy_mmap_and_free_buffer(struct doca_mmap *mmap, void *buffer);

#endif // BUFFER_H