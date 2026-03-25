/*
 * dpumesh_shm.c - DPUmesh SHM transport layer implementation
 *
 * Stripped-down version: only SHM infrastructure + raw buffer API.
 * No HTTP header serialization, no JSON parsing, no header pools.
 *
 * Binary-compatible with Python dpumesh/common.py SHM layout.
 */

#define _GNU_SOURCE
#include "dpumesh.h"

#ifndef DPUMESH_SHM_PREFIX_DEFAULT
#define DPUMESH_SHM_PREFIX_DEFAULT "/dpumesh"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <time.h>
#include <pthread.h>
#include <poll.h>

/* Runtime SHM prefix (configurable via SHM_PREFIX env var) */
static const char *shm_prefix(void) {
    static const char *cached = NULL;
    if (!cached) {
        cached = getenv("SHM_PREFIX");
        if (!cached) cached = DPUMESH_SHM_PREFIX_DEFAULT;
    }
    return cached;
}

/* ====================================================================
 * BufferPool - matches Python BufferPool layout exactly
 * Layout: [bitmap: num_slots bytes] [data: num_slots * slot_size bytes]
 * ==================================================================== */

typedef struct {
    char    name[128];
    char    shm_path[256];
    char    lock_path[256];
    int     num_slots;
    int     slot_size;
    size_t  total_size;
    int     shm_fd;
    void   *mm;
} buffer_pool_t;

static int bp_init(buffer_pool_t *bp, const char *name, int create,
                   int num_slots, int slot_size) {
    memset(bp, 0, sizeof(*bp));
    snprintf(bp->name, sizeof(bp->name), "%s", name);
    snprintf(bp->shm_path, sizeof(bp->shm_path), "/dev/shm/%s_%s",
             shm_prefix(), name);
    snprintf(bp->lock_path, sizeof(bp->lock_path), "%s.lock", bp->shm_path);
    bp->num_slots = num_slots;
    bp->slot_size = slot_size;
    bp->total_size = bp->num_slots + ((size_t)bp->num_slots * bp->slot_size);
    bp->shm_fd = -1;
    bp->mm = MAP_FAILED;

    if (create) {
        int fd = open(bp->shm_path, O_RDWR | O_CREAT, 0666);
        if (fd < 0) { perror("bp_init create"); return -1; }
        fchmod(fd, 0666);
        if (ftruncate(fd, bp->total_size) < 0) { close(fd); return -1; }
        void *m = mmap(NULL, bp->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (m == MAP_FAILED) { close(fd); return -1; }
        memset(m, 0, bp->num_slots); /* clear bitmap */
        msync(m, bp->num_slots, MS_SYNC);
        munmap(m, bp->total_size);
        close(fd);
        /* create lock file */
        int lf = open(bp->lock_path, O_CREAT | O_WRONLY, 0666);
        if (lf >= 0) { fchmod(lf, 0666); close(lf); }
    }

    /* open for use */
    bp->shm_fd = open(bp->shm_path, O_RDWR);
    if (bp->shm_fd < 0) return -1;
    bp->mm = mmap(NULL, bp->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, bp->shm_fd, 0);
    if (bp->mm == MAP_FAILED) { close(bp->shm_fd); bp->shm_fd = -1; return -1; }
    return 0;
}

static void bp_destroy(buffer_pool_t *bp) {
    if (bp->mm != MAP_FAILED) munmap(bp->mm, bp->total_size);
    if (bp->shm_fd >= 0) close(bp->shm_fd);
}

static int bp_flock(buffer_pool_t *bp) {
    int fd = open(bp->lock_path, O_RDWR);
    if (fd < 0) return -1;
    flock(fd, LOCK_EX);
    return fd;
}

static void bp_funlock(int lock_fd) {
    if (lock_fd >= 0) { flock(lock_fd, LOCK_UN); close(lock_fd); }
}

static int bp_alloc(buffer_pool_t *bp) {
    int lk = bp_flock(bp);
    uint8_t *bitmap = (uint8_t *)bp->mm;
    for (int i = 0; i < bp->num_slots; i++) {
        if (bitmap[i] == 0) {
            bitmap[i] = 1;
            msync(bitmap + i, 1, MS_SYNC);
            bp_funlock(lk);
            return i;
        }
    }
    bp_funlock(lk);
    return -1;
}

static void bp_free(buffer_pool_t *bp, int slot) {
    if (slot < 0 || slot >= bp->num_slots) return;
    int lk = bp_flock(bp);
    uint8_t *bitmap = (uint8_t *)bp->mm;
    bitmap[slot] = 0;
    msync(bitmap + slot, 1, MS_SYNC);
    bp_funlock(lk);
}

static int bp_write(buffer_pool_t *bp, int slot, const void *data, size_t len) {
    if (slot < 0 || slot >= bp->num_slots) return -1;
    if ((int)len > bp->slot_size) return -1;
    size_t offset = bp->num_slots + ((size_t)slot * bp->slot_size);
    memcpy((char *)bp->mm + offset, data, len);
    msync((char *)bp->mm + offset, len, MS_SYNC);
    return (int)len;
}

/* Get direct pointer to slot data (zero-copy) */
static uint8_t *bp_data_ptr(buffer_pool_t *bp, int slot) {
    if (slot < 0 || slot >= bp->num_slots) return NULL;
    if (bp->mm == MAP_FAILED) return NULL;
    size_t offset = bp->num_slots + ((size_t)slot * bp->slot_size);
    return (uint8_t *)bp->mm + offset;
}


/* ====================================================================
 * DescriptorRing - matches Python DescriptorRing layout
 * Layout: [header: 12 bytes (head,tail,count as uint32)] [descs: N*64]
 * ==================================================================== */

typedef struct {
    char    name[128];
    char    shm_path[256];
    char    lock_path[256];
    int     max_descs;
    size_t  total_size;
    int     shm_fd;
    void   *mm;
} desc_ring_t;

static int dr_init(desc_ring_t *dr, const char *name, int create,
                   int max_descs) {
    memset(dr, 0, sizeof(*dr));
    snprintf(dr->name, sizeof(dr->name), "%s", name);
    snprintf(dr->shm_path, sizeof(dr->shm_path), "/dev/shm/%s_%s",
             shm_prefix(), name);
    snprintf(dr->lock_path, sizeof(dr->lock_path), "%s.lock", dr->shm_path);
    dr->max_descs = max_descs;
    dr->total_size = 12 + (dr->max_descs * DPUMESH_DESCRIPTOR_SIZE);
    dr->shm_fd = -1;
    dr->mm = MAP_FAILED;

    if (create) {
        int fd = open(dr->shm_path, O_RDWR | O_CREAT, 0666);
        if (fd < 0) return -1;
        fchmod(fd, 0666);
        if (ftruncate(fd, dr->total_size) < 0) { close(fd); return -1; }
        void *m = mmap(NULL, dr->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (m == MAP_FAILED) { close(fd); return -1; }
        memset(m, 0, 12); /* head=0, tail=0, count=0 */
        msync(m, 12, MS_SYNC);
        munmap(m, dr->total_size);
        close(fd);
        int lf = open(dr->lock_path, O_CREAT | O_WRONLY, 0666);
        if (lf >= 0) { fchmod(lf, 0666); close(lf); }
    }

    dr->shm_fd = open(dr->shm_path, O_RDWR);
    if (dr->shm_fd < 0) return -1;
    dr->mm = mmap(NULL, dr->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, dr->shm_fd, 0);
    if (dr->mm == MAP_FAILED) { close(dr->shm_fd); dr->shm_fd = -1; return -1; }
    return 0;
}

static void dr_destroy(desc_ring_t *dr) {
    if (dr->mm != MAP_FAILED) munmap(dr->mm, dr->total_size);
    if (dr->shm_fd >= 0) close(dr->shm_fd);
}

static int dr_flock(desc_ring_t *dr) {
    int fd = open(dr->lock_path, O_RDWR);
    if (fd < 0) return -1;
    flock(fd, LOCK_EX);
    return fd;
}

static int dr_put(desc_ring_t *dr, const sw_descriptor_t *desc) {
    int lk = dr_flock(dr);
    uint32_t *hdr = (uint32_t *)dr->mm;
    uint32_t head = hdr[0], tail = hdr[1], count = hdr[2];
    if (count >= (uint32_t)dr->max_descs) {
        bp_funlock(lk);
        return -1;
    }
    size_t offset = 12 + (tail * DPUMESH_DESCRIPTOR_SIZE);
    memcpy((char *)dr->mm + offset, desc, DPUMESH_DESCRIPTOR_SIZE);
    hdr[0] = head;
    hdr[1] = (tail + 1) % dr->max_descs;
    hdr[2] = count + 1;
    msync(dr->mm, dr->total_size, MS_SYNC);
    bp_funlock(lk);
    return 0;
}

static int dr_get(desc_ring_t *dr, sw_descriptor_t *desc) {
    int lk = dr_flock(dr);
    uint32_t *hdr = (uint32_t *)dr->mm;
    uint32_t head = hdr[0], tail = hdr[1], count = hdr[2];
    if (count == 0) {
        bp_funlock(lk);
        return -1;
    }
    size_t offset = 12 + (head * DPUMESH_DESCRIPTOR_SIZE);
    memcpy(desc, (char *)dr->mm + offset, DPUMESH_DESCRIPTOR_SIZE);
    hdr[0] = (head + 1) % dr->max_descs;
    hdr[1] = tail;
    hdr[2] = count - 1;
    msync(dr->mm, 12, MS_SYNC);
    bp_funlock(lk);
    return 0;
}


/* ====================================================================
 * PodRegistry - JSON file with flock (matches Python PodRegistry)
 * ==================================================================== */

static const char *registry_path(void) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/dev/shm/%s_pod_registry", shm_prefix());
    return buf;
}
static const char *registry_lock(void) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/dev/shm/%s_pod_registry.lock", shm_prefix());
    return buf;
}
static const char *pod_counter_path(void) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/dev/shm/%s_pod_id_counter", shm_prefix());
    return buf;
}
static const char *pod_counter_lock(void) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/dev/shm/%s_pod_id_counter.lock", shm_prefix());
    return buf;
}

static int pod_registry_alloc_id(void) {
    int lf = open(pod_counter_lock(), O_CREAT | O_RDWR, 0666);
    if (lf < 0) return -1;
    fchmod(lf, 0666);
    flock(lf, LOCK_EX);

    int new_id = 1;
    FILE *f = fopen(pod_counter_path(), "r");
    if (f) {
        int cur = 0;
        if (fscanf(f, "%d", &cur) == 1)
            new_id = cur + 1;
        fclose(f);
    }
    f = fopen(pod_counter_path(), "w");
    if (f) { fprintf(f, "%d", new_id); fclose(f); }
    chmod(pod_counter_path(), 0666);

    flock(lf, LOCK_UN);
    close(lf);
    return new_id;
}

static int pod_registry_register(const char *worker_name, int pod_id, const char *service) {
    int lf = open(registry_lock(), O_CREAT | O_RDWR, 0666);
    if (lf < 0) return -1;
    fchmod(lf, 0666);
    flock(lf, LOCK_EX);

    char buf[8192] = "{}";
    FILE *f = fopen(registry_path(), "r");
    if (f) {
        size_t n = fread(buf, 1, sizeof(buf) - 1, f);
        buf[n] = '\0';
        fclose(f);
    }

    char *end = strrchr(buf, '}');
    if (!end) end = buf + strlen(buf);

    int has_entries = 0;
    for (char *p = buf; p < end; p++) {
        if (*p == ':') { has_entries = 1; break; }
    }

    char newbuf[8192];
    *end = '\0';
    snprintf(newbuf, sizeof(newbuf),
        "%s%s\"%s\": {\"pod_id\": %d, \"service\": \"%s\"}}",
        buf, has_entries ? ", " : "", worker_name, pod_id, service);

    f = fopen(registry_path(), "w");
    if (f) { fputs(newbuf, f); fclose(f); }
    chmod(registry_path(), 0666);

    flock(lf, LOCK_UN);
    close(lf);
    return 0;
}


/* ====================================================================
 * dpumesh_ctx - internal state (body pools only, no header pools)
 * ==================================================================== */

struct dpumesh_ctx {
    char        app_name[64];
    char        worker_id[128];
    int         pod_id;
    int         notify_fd;          /* pipe read end */
    int         notify_write_fd;    /* pipe write end — used by poller thread */
    pthread_t   poller_tid;
    volatile int poller_running;

    /* resolved config */
    int         num_slots;
    int         slot_size;
    int         max_descriptors;

    buffer_pool_t tx_pool;          /* TX body */
    buffer_pool_t rx_pool;          /* RX body */
    desc_ring_t   tx_sq;
    desc_ring_t   rx_sq;
};


/* ====================================================================
 * Poller thread — polls RX SQ, signals notify pipe
 * ==================================================================== */

static void *dpumesh_poller_fn(void *arg) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)arg;
    int idle_count = 0;

    while (ctx->poller_running) {
        sw_descriptor_t desc;
        if (dr_get(&ctx->rx_sq, &desc) == 0) {
            if (desc.valid != 1) continue;
            idle_count = 0;
            /* Write descriptor to notify pipe for dpumesh_dequeue() */
            write(ctx->notify_write_fd, &desc, sizeof(desc));
        } else {
            idle_count++;
            if (idle_count > 10000)
                usleep(1000);
            else if (idle_count > 100)
                usleep(100);
        }
    }

    return NULL;
}


/* ====================================================================
 * Public API Implementation
 * ==================================================================== */

int dpumesh_init(dpumesh_ctx_t **out, const char *app_name, int worker_num,
                 const dpumesh_config_t *config) {
    dpumesh_ctx_t *ctx = (dpumesh_ctx_t *)calloc(1, sizeof(dpumesh_ctx_t));
    if (!ctx) return -1;

    /* Resolve config: provided value > env var > compiled default */
    const char *env_val;

    if (config && config->num_slots > 0)
        ctx->num_slots = config->num_slots;
    else if ((env_val = getenv("DPUMESH_NUM_SLOTS")) != NULL && atoi(env_val) > 0)
        ctx->num_slots = atoi(env_val);
    else
        ctx->num_slots = DPUMESH_NUM_SLOTS_DEFAULT;

    if (config && config->slot_size > 0)
        ctx->slot_size = config->slot_size;
    else if ((env_val = getenv("DPUMESH_SLOT_SIZE")) != NULL && atoi(env_val) > 0)
        ctx->slot_size = atoi(env_val);
    else
        ctx->slot_size = DPUMESH_SLOT_SIZE_DEFAULT;

    if (config && config->max_descriptors > 0)
        ctx->max_descriptors = config->max_descriptors;
    else if ((env_val = getenv("DPUMESH_MAX_DESCRIPTORS")) != NULL && atoi(env_val) > 0)
        ctx->max_descriptors = atoi(env_val);
    else
        ctx->max_descriptors = DPUMESH_MAX_DESCRIPTORS_DEFAULT;

    snprintf(ctx->app_name, sizeof(ctx->app_name), "%s", app_name);

    ctx->pod_id = pod_registry_alloc_id();
    if (ctx->pod_id < 0) {
        free(ctx); return -1;
    }

    snprintf(ctx->worker_id, sizeof(ctx->worker_id),
             "%s-worker-%d", app_name, worker_num);

    pod_registry_register(ctx->worker_id, ctx->pod_id, app_name);

    /* Create service buffer pools (body only) */
    char name[128];
    snprintf(name, sizeof(name), "%s_tx_body", app_name);
    if (bp_init(&ctx->tx_pool, name, 1, ctx->num_slots, ctx->slot_size) < 0)
        goto fail;

    snprintf(name, sizeof(name), "%s_rx_body", app_name);
    if (bp_init(&ctx->rx_pool, name, 1, ctx->num_slots, ctx->slot_size) < 0)
        goto fail;

    /* Create worker SQs */
    snprintf(name, sizeof(name), "pod_%d_tx_sq", ctx->pod_id);
    if (dr_init(&ctx->tx_sq, name, 1, ctx->max_descriptors) < 0) goto fail;

    snprintf(name, sizeof(name), "pod_%d_rx_sq", ctx->pod_id);
    if (dr_init(&ctx->rx_sq, name, 1, ctx->max_descriptors) < 0) goto fail;

    /* Create notification pipe + poller thread */
    int pfd[2];
    if (pipe(pfd) < 0) goto fail;
    int flags = fcntl(pfd[0], F_GETFL, 0);
    fcntl(pfd[0], F_SETFL, flags | O_NONBLOCK);
    flags = fcntl(pfd[1], F_GETFL, 0);
    fcntl(pfd[1], F_SETFL, flags | O_NONBLOCK);

    ctx->notify_fd = pfd[0];
    ctx->notify_write_fd = pfd[1];
    ctx->poller_running = 1;

    if (pthread_create(&ctx->poller_tid, NULL, dpumesh_poller_fn, ctx) != 0)
        goto fail;

    printf("[dpumesh] initialized: worker=%s pod_id=%d app=%s "
           "slots=%d slot_size=%d max_desc=%d\n",
           ctx->worker_id, ctx->pod_id, ctx->app_name,
           ctx->num_slots, ctx->slot_size, ctx->max_descriptors);

    *out = ctx;
    return 0;

fail:
    free(ctx);
    return -1;
}

void dpumesh_destroy(dpumesh_ctx_t *ctx) {
    if (!ctx) return;
    ctx->poller_running = 0;
    pthread_join(ctx->poller_tid, NULL);
    if (ctx->notify_fd >= 0) close(ctx->notify_fd);
    if (ctx->notify_write_fd >= 0) close(ctx->notify_write_fd);
    bp_destroy(&ctx->tx_pool);
    bp_destroy(&ctx->rx_pool);
    dr_destroy(&ctx->tx_sq);
    dr_destroy(&ctx->rx_sq);
    free(ctx);
}

int dpumesh_get_notify_fd(dpumesh_ctx_t *ctx) {
    return ctx->notify_fd;
}

int dpumesh_get_pod_id(dpumesh_ctx_t *ctx) {
    return ctx->pod_id;
}

const char *dpumesh_get_worker_id(dpumesh_ctx_t *ctx) {
    return ctx->worker_id;
}

int dpumesh_get_slot_size(dpumesh_ctx_t *ctx) {
    return ctx->slot_size;
}


/* ====================================================================
 * Raw Buffer API
 * ==================================================================== */

int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc, int timeout_ms) {
    /* Try non-blocking read first */
    ssize_t n = read(ctx->notify_fd, desc, sizeof(sw_descriptor_t));
    if (n == (ssize_t)sizeof(sw_descriptor_t)) {
        if (desc->valid == 1)
            return 0;
    }

    if (timeout_ms == 0)
        return -1;

    /* Poll with timeout */
    struct pollfd pfd;
    pfd.fd = ctx->notify_fd;
    pfd.events = POLLIN;

    while (1) {
        int ret = poll(&pfd, 1, timeout_ms);
        if (ret > 0 && (pfd.revents & POLLIN)) {
            n = read(ctx->notify_fd, desc, sizeof(sw_descriptor_t));
            if (n == (ssize_t)sizeof(sw_descriptor_t) && desc->valid == 1)
                return 0;
            /* Got invalid descriptor, keep waiting if blocking */
            if (timeout_ms < 0)
                continue;
            return -1;
        }
        if (ret == 0) return -1;  /* timeout */
        if (errno == EINTR) continue;
        return -1;  /* error */
    }
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot) {
    return bp_data_ptr(&ctx->rx_pool, slot);
}

void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot) {
    bp_free(&ctx->rx_pool, slot);
}

int dpumesh_tx_alloc(dpumesh_ctx_t *ctx) {
    return bp_alloc(&ctx->tx_pool);
}

uint8_t *dpumesh_tx_buf(dpumesh_ctx_t *ctx, int slot) {
    return bp_data_ptr(&ctx->tx_pool, slot);
}

void dpumesh_tx_free(dpumesh_ctx_t *ctx, int slot) {
    bp_free(&ctx->tx_pool, slot);
}

int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc) {
    return dr_put(&ctx->tx_sq, desc);
}
