/*
 * dpu_main.c - DPUmesh DPU binary entry point
 *
 * Runs on BlueField DPU ARM cores.
 * Usage: dpumesh_dpu -p <pci-addr> -r <rep-pci-addr>
 */

#include <stdio.h>
#include <stdlib.h>

#include <doca_dev.h>
#include <doca_log.h>
#include <doca_build_config.h>

#include "config.h"
#include "common.h"
#include "object.h"
#include "dpu_worker.h"

DOCA_LOG_REGISTER(DPU_MAIN);

int main(int argc, char **argv)
{
    /* Heap-allocated (struct objects is large); never freed — the process runs
     * until killed and run_dpu_worker() below blocks forever. */
    struct objects *objs = calloc(1, sizeof(*objs));
    struct global_config gcfg = {0};
    doca_error_t result;
    struct doca_log_backend *sdk_log;

    if (!objs) {
        fprintf(stderr, "Failed to allocate objects struct\n");
        return 1;
    }

    /* Logging setup */
    result = doca_log_backend_create_standard();
    if (result != DOCA_SUCCESS)
        goto exit;

    result = doca_log_backend_create_with_file_sdk(stderr, &sdk_log);
    if (result != DOCA_SUCCESS)
        goto exit;

    result = doca_log_backend_set_sdk_level(sdk_log, DOCA_LOG_LEVEL_WARNING);
    if (result != DOCA_SUCCESS)
        goto exit;

    /* Detect mode */
#ifdef DOCA_ARCH_DPU
    gcfg.mode = DPU_MODE;
#else
    gcfg.mode = HOST_MODE;
#endif

    /* Parse command-line arguments (-p, -r) */
    result = init_argp(NULL, &gcfg, argc, argv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to parse arguments: %s", doca_error_get_descr(result));
        goto exit;
    }

    /* Open DOCA device */
    result = open_doca_device_with_pci(gcfg.dev_pci_addr, NULL, &(objs->dev));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to open DOCA device at %s", gcfg.dev_pci_addr);
        goto argp_cleanup;
    }

    /* Open representor device (DPU mode) */
    if (gcfg.mode == DPU_MODE) {
        result = open_doca_device_rep_with_pci(objs->dev,
                                               DOCA_DEVINFO_REP_FILTER_NET,
                                               gcfg.dev_rep_pci_addr,
                                               &(objs->rep_dev));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to open representor device at %s",
                         gcfg.dev_rep_pci_addr);
            cleanup_objects(objs);
            goto argp_cleanup;
        }
    }

    DOCA_LOG_INFO("Starting %s application",
                  gcfg.mode == DPU_MODE ? "DPU" : "Host");

    /* Run DPU worker (blocking) */
    run_dpu_worker(objs);

argp_cleanup:
    clean_argp();
exit:
    return 0;
}
