#include "buffer.h"
#include "comch_common.h"

#include <doca_dev.h>
#include <doca_mmap.h>

DOCA_LOG_REGISTER(BUFFER);

void clean_local_mem_bufs(struct local_mem_bufs *local)
{
	doca_error_t result;
	void *mem;
	size_t mem_size;

	if (local == NULL)
		return;

	if (local->need_alloc_mem == true) {
		result = doca_mmap_get_memrange(local->mmap, &mem, &mem_size);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Unable to get mmap memrange: %s", doca_error_get_descr(result));
			return;
		}
		free(mem);
	}
	local->mem = NULL;

	result = doca_mmap_destroy(local->mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to destroy mmap: %s", doca_error_get_descr(result));
		return;
	}
	local->mmap = NULL;

	if (local->buf_inv_type == BUF_INV_TYPE_INVENTORY) {
		result = doca_buf_inventory_destroy(local->buf_inv);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to destroy inventory: %s", doca_error_get_descr(result));
			return;
		}
		local->buf_inv = NULL;
	} else if (local->buf_inv_type == BUF_INV_TYPE_POOL) {
		result = doca_buf_pool_destroy(local->bpool);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to destroy bpool: %s", doca_error_get_descr(result));
			return;
		}
		local->bpool = NULL;
	} else {
		DOCA_LOG_ERR("Invalid buf_inv_type when cleaning local mem bufs");
		return;
	}
}

doca_error_t init_local_mem_bufs(struct local_mem_bufs *local, struct doca_dev *dev, 
								uint8_t buf_inv_type, size_t buf_len, size_t max_bufs)
{
	doca_error_t result;

	if (local->need_alloc_mem == true) {

		assert(buf_len * max_bufs % CACHE_ALIGN == 0);

		/* allocate aligned buffer for mmap */
		if (posix_memalign(&local->mem, CACHE_ALIGN, buf_len * max_bufs) != 0) {
			result = DOCA_ERROR_NO_MEMORY;
			DOCA_LOG_ERR("Unable to alloc memory to mmap: %s", doca_error_get_descr(result));
			return result;
		}
	}

	result = doca_mmap_create(&local->mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create mmap: %s", doca_error_get_descr(result));
		goto free_mem;
	}

	result = doca_mmap_add_dev(local->mmap, dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to add device to mmap: %s", doca_error_get_descr(result));
		goto destroy_mmap;
	}

	result = doca_mmap_set_permissions(local->mmap, DOCA_ACCESS_FLAG_PCI_READ_WRITE);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to set permission to mmap: %s", doca_error_get_descr(result));
		goto destroy_mmap;
	}

	result = doca_mmap_set_memrange(local->mmap, local->mem, max_bufs * buf_len);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to set memrange to mmap: %s", doca_error_get_descr(result));
		goto destroy_mmap;
	}

	result = doca_mmap_start(local->mmap);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to start mmap: %s", doca_error_get_descr(result));
		goto destroy_mmap;
	}

	if (buf_inv_type == BUF_INV_TYPE_INVENTORY) {
		result = doca_buf_inventory_create(max_bufs, &(local->buf_inv));
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Unable to create inventory: %s", doca_error_get_descr(result));
			goto destroy_mmap;
		}

		result = doca_buf_inventory_start(local->buf_inv);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Unable to start inventory: %s", doca_error_get_descr(result));
			goto destroy_inv;
		}

	} else if (buf_inv_type == BUF_INV_TYPE_POOL) {
		result = doca_buf_pool_create(max_bufs, buf_len, local->mmap, &(local->bpool));
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Unable to create buf pool: %s", doca_error_get_descr(result));
			goto destroy_mmap;
		}

		result = doca_buf_pool_start(local->bpool);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Unable to start buf pool: %s", doca_error_get_descr(result));
			goto destroy_inv;
		}
	} else {
		result = DOCA_ERROR_INVALID_VALUE;
		DOCA_LOG_ERR("Invalid buf_inv_type: %s", doca_error_get_descr(result));
		goto destroy_mmap;
	}
	local->buf_inv_type = buf_inv_type;

	return DOCA_SUCCESS;

destroy_inv:
	if (buf_inv_type == BUF_INV_TYPE_INVENTORY) {
		doca_buf_inventory_destroy(local->buf_inv);
		local->buf_inv = NULL;
	} else if (buf_inv_type == BUF_INV_TYPE_POOL) {
		doca_buf_pool_destroy(local->bpool);
		local->bpool = NULL;
	}
destroy_mmap:
	doca_mmap_destroy(local->mmap);
	local->mmap = NULL;
free_mem:
	if (local->need_alloc_mem == true) {
		free(local->mem);
		local->mem = NULL;
	}
	return result;
}

doca_error_t
alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                        void **buffer, size_t buffer_size, uint32_t access_mask)
{
    doca_error_t result;
    int ret;

    result = doca_mmap_create(mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create local mmap - %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_add_dev(*mmap, dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to add device to mmap - %s",
                doca_error_get_name(result));
        goto destroy_mmap;
    }

    result = doca_mmap_set_permissions(*mmap, access_mask);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set mmap permissions - %s",
                doca_error_get_name(result));
        goto destroy_mmap;
    }

    ret = posix_memalign(buffer, CACHE_ALIGN, buffer_size);
    if (ret != 0) {
        result = DOCA_ERROR_NO_MEMORY;
        DOCA_LOG_ERR("Failed to allocate aligned memory for buffer - %s",
                strerror(ret));
        goto destroy_mmap;
    }
	
	memset(*buffer, 0, buffer_size);
	DOCA_LOG_INFO("Allocated buffer at address %p with size %zu", *buffer, buffer_size);

    result = doca_mmap_set_memrange(*mmap, *buffer, buffer_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set mmap memrange - %s",
                doca_error_get_name(result));
        goto free_buffer;
    }
    result = doca_mmap_start(*mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start mmap - %s",   
                doca_error_get_name(result));
        goto free_buffer;
    }

    return DOCA_SUCCESS;

free_buffer:
    free(*buffer);
    buffer = NULL;
destroy_mmap:
    doca_mmap_destroy(*mmap);
    *mmap = NULL;

    return result;
}

doca_error_t
destroy_mmap_and_free_buffer(struct doca_mmap *mmap, void *buffer)
{
    doca_error_t result;
    
    result = doca_mmap_destroy(mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to destroy mmap - %s",
                doca_error_get_name(result));
        return result;
    }

    free(buffer);

    return DOCA_SUCCESS;
}
