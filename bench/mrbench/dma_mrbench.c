/*
 * dma_mrbench.c — isolate the "Memory-Region working-set" effect on DMA op-rate.
 *
 * Standalone DOCA `doca_dma` microbenchmark, ARM-issued, DPU-local. It sweeps
 * the number of DISTINCT source Memory Regions (mkeys) touched in a tight
 * round-robin, at CONSTANT total DMA work, and reports ops/sec + latency.
 *
 * WHY: the transport keeps mkeys O(pods) (a handful per process) so the device
 * translation cache always hits and the DMA engine runs at its op-rate ceiling.
 * The "per-conn mmap" idea would make mkeys O(conns). This bench tests whether a
 * large hot-MR working set actually thrashes the on-chip translation cache and
 * drops op-rate — the physics behind that trade-off — WITHOUT re-architecting
 * the transport (which would confound the MR effect with a dozen other changes).
 *
 * Three modes, IDENTICAL memory-access pattern, differing ONLY in registration:
 *   A "mkeys" : N distinct mmaps, one per slice   (MPT+MTT working set = N)
 *   B "pages" : 1 mmap, N distinct slices          (MTT working set = N, MPT = 1)
 *   C "reuse" : 1 mmap, 1 slice reused             (working set = 1; flat control)
 *
 * Reading the result:
 *   - A degrades but B stays flat  -> the mkey/MPT COUNT is the binding resource
 *                                     (this is the "per-conn mmap caps scaling" claim).
 *   - B degrades too (tracks A)    -> the page/MTT translation footprint dominates.
 *   - all three flat across N      -> no translation-cache wall in the swept range.
 *   - mode A create fails at some N -> the hard device MR-count limit (also a data point).
 *
 * CAVEATS (state these when reporting):
 *   - ARM-issued doca_dma, DPU-local src->dst. The real forward/reverse path is
 *     DPA-issued over PCIe, so ABSOLUTE numbers here != the ~200K RPS ceiling.
 *     What transfers is the SHAPE: op-rate vs N.
 *   - Backing memory is posix_memalign'd (like the transport). The kernel MAY back
 *     it with transparent huge pages, which shrinks the MTT footprint and can hide
 *     mode-B pressure. Pass -H to madvise MADV_HUGEPAGE explicitly, or rely on 4K.
 *   - On-chip cache sizes are undocumented -> the knee is found empirically.
 *
 * Build:  see build.sh   (gcc + pkg-config doca-common doca-dma)
 * Run  :  ./dma_mrbench                 # auto-pick a DMA-capable device, default sweep
 *         ./dma_mrbench -p 03:00.0 -N 1,4,16,64,256,1024,4096 -o 2000000 -s 8192 -d 32
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>
#include <sys/mman.h>

#include <doca_dev.h>
#include <doca_mmap.h>
#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#include <doca_pe.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_types.h>

/* ---- config (overridable via argv) ---------------------------------------- */
#define MAX_SWEEP 64
static int      g_sweep[MAX_SWEEP];
static int      g_sweep_n = 0;
static long     g_ops     = 2000000;   /* timed ops per (mode,N) point          */
static long     g_warmup  = 100000;    /* untimed warmup ops per point          */
static size_t   g_size    = 8192;      /* bytes per DMA op (= transport slot)    */
static int      g_depth   = 32;        /* in-flight ops (matches real depth)     */
static const char *g_pci  = NULL;      /* NULL => auto-pick first DMA-capable    */
static int      g_hugepage = 0;        /* -H => madvise MADV_HUGEPAGE on backing */
static char     g_modes[4] = "ABC";    /* which modes to run                     */
static const char *g_csv  = NULL;      /* optional CSV output path               */

/* ---- helpers -------------------------------------------------------------- */
static double mono_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

#define CHK(expr, msg) do {                                                    \
    doca_error_t _r = (expr);                                                  \
    if (_r != DOCA_SUCCESS) {                                                  \
        fprintf(stderr, "FATAL %s: %s (%s)\n", (msg),                          \
                doca_error_get_name(_r), doca_error_get_descr(_r));            \
        exit(1);                                                               \
    }                                                                          \
} while (0)

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* ---- bench state (pointed to by the DMA ctx user_data) -------------------- */
struct opctx {
    double          t0;      /* submit timestamp (us)                          */
    int             slot;    /* index into bench.ops[] (free-stack element)    */
    int             rec;     /* 1 = record this op's latency                   */
    struct doca_buf *src;    /* held bufs, released in completion              */
    struct doca_buf *dst;
};

struct bench {
    int      inflight;
    long     completed;
    long     errors;
    /* latency samples (us) */
    double  *lat;
    long     lat_n, lat_cap;
    /* free-stack of opctx slots */
    struct opctx *ops;       /* [depth] */
    int     *freestk;
    int      freetop;
};

static void done_cb(struct doca_dma_task_memcpy *t, union doca_data tud, union doca_data cud) {
    struct bench  *b = (struct bench *)cud.ptr;
    struct opctx  *o = (struct opctx *)tud.ptr;
    doca_task_free(doca_dma_task_memcpy_as_task(t));
    if (o->src) { doca_buf_dec_refcount(o->src, NULL); o->src = NULL; }
    if (o->dst) { doca_buf_dec_refcount(o->dst, NULL); o->dst = NULL; }
    if (o->rec && b->lat_n < b->lat_cap)
        b->lat[b->lat_n++] = mono_us() - o->t0;
    b->completed++;
    b->inflight--;
    b->freestk[b->freetop++] = o->slot;   /* return the slot */
}

static void err_cb(struct doca_dma_task_memcpy *t, union doca_data tud, union doca_data cud) {
    struct bench  *b = (struct bench *)cud.ptr;
    struct opctx  *o = (struct opctx *)tud.ptr;
    doca_error_t st = doca_task_get_status(doca_dma_task_memcpy_as_task(t));
    (void)st;
    doca_task_free(doca_dma_task_memcpy_as_task(t));
    if (o->src) { doca_buf_dec_refcount(o->src, NULL); o->src = NULL; }
    if (o->dst) { doca_buf_dec_refcount(o->dst, NULL); o->dst = NULL; }
    b->errors++;
    b->completed++;
    b->inflight--;
    b->freestk[b->freetop++] = o->slot;
}

/* ---- device / mmap -------------------------------------------------------- */
static doca_error_t open_dma_dev(const char *pci, struct doca_dev **out) {
    struct doca_devinfo **list;
    uint32_t nb;
    doca_error_t r = doca_devinfo_create_list(&list, &nb);
    if (r != DOCA_SUCCESS) return r;
    r = DOCA_ERROR_NOT_FOUND;
    for (uint32_t i = 0; i < nb; i++) {
        if (pci) {
            uint8_t eq = 0;
            if (doca_devinfo_is_equal_pci_addr(list[i], pci, &eq) != DOCA_SUCCESS || !eq)
                continue;
        } else {
            if (doca_dma_cap_task_memcpy_is_supported(list[i]) != DOCA_SUCCESS)
                continue;
        }
        if (doca_dev_open(list[i], out) == DOCA_SUCCESS) { r = DOCA_SUCCESS; break; }
    }
    doca_devinfo_destroy_list(list);
    return r;
}

static doca_error_t make_mmap(struct doca_dev *dev, void *addr, size_t len,
                              struct doca_mmap **out) {
    struct doca_mmap *m;
    doca_error_t r = doca_mmap_create(&m);
    if (r != DOCA_SUCCESS) return r;
    if ((r = doca_mmap_add_dev(m, dev)) != DOCA_SUCCESS)                   goto e;
    if ((r = doca_mmap_set_permissions(m, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE)) != DOCA_SUCCESS) goto e;
    if ((r = doca_mmap_set_memrange(m, addr, len)) != DOCA_SUCCESS)        goto e;
    if ((r = doca_mmap_start(m)) != DOCA_SUCCESS)                          goto e;
    *out = m;
    return DOCA_SUCCESS;
e:
    doca_mmap_destroy(m);
    return r;
}

/* run `count` ops in one (mode,N) configuration. `record` gates latency capture.
 * src slice i is chosen per mode; dst cycles a `depth`-slot ring so in-flight
 * ops never target overlapping bytes. Returns elapsed microseconds. */
static double run_ops(struct bench *b, struct doca_pe *pe, struct doca_dma *dma,
                      struct doca_buf_inventory *inv,
                      char mode, int N,
                      uint8_t *src_backing, struct doca_mmap *src_mmap0,
                      struct doca_mmap **src_mmaps,
                      uint8_t *dst_backing, struct doca_mmap *dst_mmap,
                      long count, int record) {
    long submitted = 0;
    long stride = record ? (count + b->lat_cap - 1) / (b->lat_cap ? b->lat_cap : 1) : 0;
    if (stride < 1) stride = 1;

    double t_start = mono_us();
    while (submitted < count || b->inflight > 0) {
        while (b->inflight < g_depth && submitted < count && b->freetop > 0) {
            int slice = (mode == 'C') ? 0 : (int)(submitted % N);
            struct doca_mmap *smmap = (mode == 'A') ? src_mmaps[slice] : src_mmap0;
            uint8_t *saddr = src_backing + (size_t)slice * g_size;
            int dslot = (int)(submitted % g_depth);
            uint8_t *daddr = dst_backing + (size_t)dslot * g_size;

            struct doca_buf *src = NULL, *dst = NULL;
            if (doca_buf_inventory_buf_get_by_addr(inv, smmap, saddr, g_size, &src) != DOCA_SUCCESS)
                break;   /* inventory momentarily dry -> go progress */
            if (doca_buf_set_data(src, saddr, g_size) != DOCA_SUCCESS) {
                doca_buf_dec_refcount(src, NULL); break;
            }
            if (doca_buf_inventory_buf_get_by_addr(inv, dst_mmap, daddr, g_size, &dst) != DOCA_SUCCESS) {
                doca_buf_dec_refcount(src, NULL); break;
            }

            int si = b->freestk[--b->freetop];
            struct opctx *o = &b->ops[si];
            o->slot = si; o->src = src; o->dst = dst;
            o->rec  = record && (submitted % stride == 0);
            o->t0   = o->rec ? mono_us() : 0.0;

            union doca_data ud = { .ptr = o };
            struct doca_dma_task_memcpy *t = NULL;
            if (doca_dma_task_memcpy_alloc_init(dma, src, dst, ud, &t) != DOCA_SUCCESS) {
                doca_buf_dec_refcount(src, NULL); doca_buf_dec_refcount(dst, NULL);
                b->freestk[b->freetop++] = si; o->src = o->dst = NULL;
                break;
            }
            if (doca_task_try_submit(doca_dma_task_memcpy_as_task(t)) != DOCA_SUCCESS) {
                doca_task_free(doca_dma_task_memcpy_as_task(t));
                doca_buf_dec_refcount(src, NULL); doca_buf_dec_refcount(dst, NULL);
                b->freestk[b->freetop++] = si; o->src = o->dst = NULL;
                break;   /* task pool momentarily full -> progress */
            }
            b->inflight++;
            submitted++;
        }
        doca_pe_progress(pe);
    }
    return mono_us() - t_start;
}

/* build N source MRs for mode A (returns 0 on device MR-limit / failure). */
static int build_src_mmaps_A(struct doca_dev *dev, uint8_t *backing, int N,
                             struct doca_mmap **mmaps) {
    for (int i = 0; i < N; i++) {
        if (make_mmap(dev, backing + (size_t)i * g_size, g_size, &mmaps[i]) != DOCA_SUCCESS) {
            for (int j = 0; j < i; j++) doca_mmap_destroy(mmaps[j]);
            return 0;
        }
    }
    return 1;
}

static void print_row(FILE *csv, char mode, const char *mname, int N,
                      double elapsed_us, struct bench *b) {
    double ops_s = (double)g_ops / (elapsed_us / 1e6);
    double gbs   = ops_s * (double)g_size / 1e9;
    double p50 = 0, p99 = 0, mean = 0;
    if (b->lat_n > 0) {
        qsort(b->lat, b->lat_n, sizeof(double), cmp_double);
        p50 = b->lat[(long)(b->lat_n * 0.50)];
        p99 = b->lat[(long)(b->lat_n * 0.99)];
        for (long i = 0; i < b->lat_n; i++) mean += b->lat[i];
        mean /= b->lat_n;
    }
    printf("  %-7s %6d   %8.3f   %7.2f   %7.2f %7.2f %7.2f   %8ld\n",
           mname, N, ops_s / 1e6, gbs, mean, p50, p99, b->errors);
    if (csv)
        fprintf(csv, "%c,%d,%.0f,%.4f,%.3f,%.3f,%.3f,%ld\n",
                mode, N, ops_s, gbs, mean, p50, p99, b->errors);
    fflush(stdout);
}

static void usage(const char *p) {
    fprintf(stderr,
      "usage: %s [-p pci] [-N n0,n1,..] [-o ops] [-w warmup] [-s size] [-d depth]\n"
      "          [-m ABC] [-H] [-c csvfile]\n"
      "  -p pci     DOCA device PCI addr (default: auto-pick first DMA-capable)\n"
      "  -N list    MR-count sweep (default 1,2,4,8,16,32,64,128,256,512,1024,2048,4096)\n"
      "  -o ops     timed ops per point (default 2000000)\n"
      "  -w warmup  untimed warmup ops per point (default 100000)\n"
      "  -s size    bytes per op (default 8192 = transport slot)\n"
      "  -d depth   in-flight ops (default 32)\n"
      "  -m modes   subset of ABC (A=mkeys B=pages C=reuse; default ABC)\n"
      "  -H         madvise MADV_HUGEPAGE on backing (fewer MTT entries)\n"
      "  -c file    also write a CSV\n", p);
}

static void parse_sweep(const char *s) {
    g_sweep_n = 0;
    char buf[512]; strncpy(buf, s, sizeof buf - 1); buf[sizeof buf - 1] = 0;
    for (char *tok = strtok(buf, ","); tok && g_sweep_n < MAX_SWEEP; tok = strtok(NULL, ","))
        g_sweep[g_sweep_n++] = atoi(tok);
}

int main(int argc, char **argv) {
    parse_sweep("1,2,4,8,16,32,64,128,256,512,1024,2048,4096");
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-p") && i + 1 < argc)      g_pci = argv[++i];
        else if (!strcmp(argv[i], "-N") && i + 1 < argc) parse_sweep(argv[++i]);
        else if (!strcmp(argv[i], "-o") && i + 1 < argc) g_ops = atol(argv[++i]);
        else if (!strcmp(argv[i], "-w") && i + 1 < argc) g_warmup = atol(argv[++i]);
        else if (!strcmp(argv[i], "-s") && i + 1 < argc) g_size = (size_t)atol(argv[++i]);
        else if (!strcmp(argv[i], "-d") && i + 1 < argc) g_depth = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-m") && i + 1 < argc) { strncpy(g_modes, argv[++i], 3); g_modes[3]=0; }
        else if (!strcmp(argv[i], "-H"))                 g_hugepage = 1;
        else if (!strcmp(argv[i], "-c") && i + 1 < argc) g_csv = argv[++i];
        else { usage(argv[0]); return 2; }
    }
    if (g_depth < 1) g_depth = 1;

    int Nmax = 1;
    for (int i = 0; i < g_sweep_n; i++) if (g_sweep[i] > Nmax) Nmax = g_sweep[i];

    /* ---- device + DMA engine ------------------------------------------------ */
    struct doca_dev *dev = NULL;
    if (open_dma_dev(g_pci, &dev) != DOCA_SUCCESS) {
        fprintf(stderr, "FATAL: no DMA-capable DOCA device (pci=%s)\n", g_pci ? g_pci : "auto");
        return 1;
    }

    struct bench b; memset(&b, 0, sizeof b);
    b.ops     = calloc(g_depth, sizeof(struct opctx));
    b.freestk = calloc(g_depth, sizeof(int));
    b.lat_cap = 1000000;
    b.lat     = calloc(b.lat_cap, sizeof(double));
    if (!b.ops || !b.freestk || !b.lat) { fprintf(stderr, "OOM\n"); return 1; }

    struct doca_pe *pe = NULL;
    CHK(doca_pe_create(&pe), "pe_create");

    struct doca_dma *dma = NULL;
    CHK(doca_dma_create(dev, &dma), "dma_create");
    struct doca_ctx *ctx = doca_dma_as_ctx(dma);
    CHK(doca_dma_task_memcpy_set_conf(dma, done_cb, err_cb, (unsigned)(g_depth + 8)), "set_conf");
    CHK(doca_pe_connect_ctx(pe, ctx), "connect_ctx");
    union doca_data cud = { .ptr = &b };
    CHK(doca_ctx_set_user_data(ctx, cud), "set_user_data");
    CHK(doca_ctx_start(ctx), "ctx_start");

    struct doca_buf_inventory *inv = NULL;
    CHK(doca_buf_inventory_create((size_t)(g_depth * 4 + 64), &inv), "inv_create");
    CHK(doca_buf_inventory_start(inv), "inv_start");

    /* ---- backing memory + fixed dst MR ------------------------------------- */
    size_t src_bytes = (size_t)Nmax * g_size;
    size_t dst_bytes = (size_t)g_depth * g_size;
    uint8_t *src_backing = NULL, *dst_backing = NULL;
    if (posix_memalign((void **)&src_backing, 4096, src_bytes) ||
        posix_memalign((void **)&dst_backing, 4096, dst_bytes)) {
        fprintf(stderr, "OOM backing (%zu + %zu bytes)\n", src_bytes, dst_bytes); return 1;
    }
    memset(src_backing, 0xAB, src_bytes);
    memset(dst_backing, 0, dst_bytes);
    if (g_hugepage) {
        madvise(src_backing, src_bytes, MADV_HUGEPAGE);
        madvise(dst_backing, dst_bytes, MADV_HUGEPAGE);
    }

    struct doca_mmap *dst_mmap = NULL;
    CHK(make_mmap(dev, dst_backing, dst_bytes, &dst_mmap), "dst mmap");

    struct doca_mmap **src_mmaps = calloc(Nmax, sizeof(*src_mmaps));  /* mode A */
    if (!src_mmaps) { fprintf(stderr, "OOM\n"); return 1; }

    FILE *csv = NULL;
    if (g_csv) { csv = fopen(g_csv, "w"); if (csv) fprintf(csv, "mode,N,ops_s,gbs,mean_us,p50_us,p99_us,errors\n"); }

    printf("# dma_mrbench  pci=%s  size=%zuB  depth=%d  ops=%ld  warmup=%ld  hugepage=%d\n",
           g_pci ? g_pci : "auto", g_size, g_depth, g_ops, g_warmup, g_hugepage);
    printf("# modes: A=mkeys(N distinct MRs)  B=pages(1 MR,N slices)  C=reuse(1 MR,1 slice)\n");
    printf("# %-7s %6s   %8s   %7s   %7s %7s %7s   %8s\n",
           "mode", "N", "Mops/s", "GB/s", "mean_us", "p50_us", "p99_us", "errors");

    for (unsigned mi = 0; g_modes[mi]; mi++) {
        char mode = g_modes[mi];
        const char *mname = (mode=='A') ? "A:mkeys" : (mode=='B') ? "B:pages" : "C:reuse";
        if (mode != 'A' && mode != 'B' && mode != 'C') continue;

        for (int si = 0; si < g_sweep_n; si++) {
            int N = g_sweep[si];
            if (N < 1) continue;
            /* mode C is N-independent by construction; it runs once and breaks below. */

            /* build source MR(s) for this configuration */
            struct doca_mmap *src_mmap0 = NULL;
            if (mode == 'A') {
                if (!build_src_mmaps_A(dev, src_backing, N, src_mmaps)) {
                    printf("  %-7s %6d   -- device MR create failed (hard MR-count limit) --\n", mname, N);
                    if (csv) fprintf(csv, "%c,%d,MR_LIMIT\n", mode, N);
                    break;   /* no point sweeping higher */
                }
            } else {
                /* B/C: one MR spanning the slices actually used (N for B, 1 for C) */
                size_t span = (mode == 'B') ? (size_t)N * g_size : g_size;
                CHK(make_mmap(dev, src_backing, span, &src_mmap0), "src mmap0");
            }

            /* warmup (untimed), then measured */
            b.completed = 0; b.errors = 0; b.inflight = 0; b.lat_n = 0;
            b.freetop = 0; for (int k = 0; k < g_depth; k++) b.freestk[b.freetop++] = k;
            if (g_warmup > 0)
                run_ops(&b, pe, dma, inv, mode, N, src_backing, src_mmap0, src_mmaps,
                        dst_backing, dst_mmap, g_warmup, 0);

            b.completed = 0; b.errors = 0; b.lat_n = 0;   /* inflight already 0, freestk full */
            double el = run_ops(&b, pe, dma, inv, mode, N, src_backing, src_mmap0, src_mmaps,
                                dst_backing, dst_mmap, g_ops, 1);
            print_row(csv, mode, mname, N, el, &b);

            /* teardown source MR(s) */
            if (mode == 'A') for (int i = 0; i < N; i++) doca_mmap_destroy(src_mmaps[i]);
            else doca_mmap_destroy(src_mmap0);

            if (mode == 'C') break;   /* one point is enough for the flat control */
        }
    }

    if (csv) fclose(csv);

    /* ---- cleanup ------------------------------------------------------------ */
    doca_ctx_stop(ctx);
    for (int spin = 0; spin < 1000 && doca_pe_progress(pe); spin++) { }
    doca_mmap_destroy(dst_mmap);
    doca_buf_inventory_destroy(inv);
    doca_dma_destroy(dma);
    doca_pe_destroy(pe);
    doca_dev_close(dev);
    free(src_mmaps); free(src_backing); free(dst_backing);
    free(b.ops); free(b.freestk); free(b.lat);
    return 0;
}
