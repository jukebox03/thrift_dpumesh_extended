#include <stdio.h>
#include <stdlib.h>
#include <doca_dpa.h>
#include <doca_dev.h>
#include <doca_error.h>

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: %s <pci-addr>\n", argv[0]); return 1; }
    struct doca_devinfo **list = NULL;
    uint32_t nb = 0;
    if (doca_devinfo_create_list(&list, &nb) != DOCA_SUCCESS) { fprintf(stderr, "list fail\n"); return 1; }
    struct doca_dev *dev = NULL;
    char pci[32];
    for (uint32_t i = 0; i < nb; i++) {
        if (doca_devinfo_get_pci_addr_str(list[i], pci) != DOCA_SUCCESS) continue;
        if (strcmp(pci, argv[1]) == 0) {
            if (doca_dev_open(list[i], &dev) != DOCA_SUCCESS) { fprintf(stderr, "open fail\n"); return 1; }
            break;
        }
    }
    doca_devinfo_destroy_list(list);
    if (!dev) { fprintf(stderr, "device not found\n"); return 1; }

    struct doca_dpa *dpa = NULL;
    if (doca_dpa_create(dev, &dpa) != DOCA_SUCCESS) { fprintf(stderr, "dpa_create fail\n"); return 1; }

    unsigned long long max_run_time = 0;
    doca_error_t r = doca_dpa_get_kernel_max_run_time(dpa, &max_run_time);
    printf("doca_dpa_get_kernel_max_run_time: rc=%d value=%llu seconds\n", (int)r, max_run_time);

    unsigned int max_threads = 0;
    r = doca_dpa_get_max_threads_per_kernel(dpa, &max_threads);
    printf("doca_dpa_get_max_threads_per_kernel: rc=%d value=%u\n", (int)r, max_threads);

    doca_dpa_destroy(dpa);
    doca_dev_close(dev);
    return 0;
}
