/*
 * config.h - DPUmesh DOCA configuration
 *
 * PCI addresses are parsed from command-line arguments via doca_argp
 * (init_argp), for both host and DPU mode. The only mode-dependent
 * behavior is that the representor PCI address (-r) is mandatory on the DPU.
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#include <doca_dev.h>
#include <doca_error.h>

enum program_mode {
    HOST_MODE = 1,
    DPU_MODE = 2,
};

struct global_config {
    enum program_mode mode;
    char dev_pci_addr[DOCA_DEVINFO_PCI_ADDR_SIZE];
    char dev_rep_pci_addr[DOCA_DEVINFO_REP_PCI_ADDR_SIZE];
};

/* Command-line argument parsing (DPU binary) */
doca_error_t init_argp(const char *program_name, void *config,
                       int argc, char **argv);
void clean_argp(void);

#endif /* CONFIG_H_ */
