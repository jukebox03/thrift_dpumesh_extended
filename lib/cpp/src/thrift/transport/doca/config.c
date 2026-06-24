#include "config.h"

#include <string.h>

#include <doca_argp.h>
#include <doca_log.h>

DOCA_LOG_REGISTER(CONFIG);

static doca_error_t pci_addr_callback(void *param, void *config)
{
    struct global_config *gcfg = (struct global_config *)config;
    const char *dev_pci_addr = (char *)param;
    int len;

    len = strnlen(dev_pci_addr, DOCA_DEVINFO_PCI_ADDR_SIZE);
    if (len >= DOCA_DEVINFO_PCI_ADDR_SIZE) {
        DOCA_LOG_ERR("Device PCI address exceeds max size of %d",
                     DOCA_DEVINFO_PCI_ADDR_SIZE - 1);
        return DOCA_ERROR_INVALID_VALUE;
    }

    strncpy(gcfg->dev_pci_addr, dev_pci_addr, len + 1);
    return DOCA_SUCCESS;
}

static doca_error_t rep_pci_addr_callback(void *param, void *config)
{
    struct global_config *gcfg = (struct global_config *)config;
    const char *rep_pci_addr = (char *)param;
    int len;

    len = strnlen(rep_pci_addr, DOCA_DEVINFO_PCI_ADDR_SIZE);
    if (len >= DOCA_DEVINFO_PCI_ADDR_SIZE) {
        DOCA_LOG_ERR("Representor PCI address exceeds max size of %d",
                     DOCA_DEVINFO_PCI_ADDR_SIZE - 1);
        return DOCA_ERROR_INVALID_VALUE;
    }

    strncpy(gcfg->dev_rep_pci_addr, rep_pci_addr, len + 1);
    return DOCA_SUCCESS;
}

doca_error_t
init_argp(const char *program_name, void *config, int argc, char **argv)
{
    doca_error_t result;
    struct doca_argp_param *dev_pci_addr_param, *rep_pci_addr_param;

    result = doca_argp_init(program_name, config);
    if (result != DOCA_SUCCESS)
        return result;

    /* -p / --pci-addr */
    result = doca_argp_param_create(&dev_pci_addr_param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_error_get_descr(result));
        goto exit;
    }
    doca_argp_param_set_short_name(dev_pci_addr_param, "p");
    doca_argp_param_set_long_name(dev_pci_addr_param, "pci-addr");
    doca_argp_param_set_description(dev_pci_addr_param, "DOCA device PCI address");
    doca_argp_param_set_callback(dev_pci_addr_param, pci_addr_callback);
    doca_argp_param_set_type(dev_pci_addr_param, DOCA_ARGP_TYPE_STRING);
    doca_argp_param_set_mandatory(dev_pci_addr_param);
    result = doca_argp_register_param(dev_pci_addr_param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register param: %s", doca_error_get_descr(result));
        goto exit;
    }

    /* -r / --rep-pci */
    result = doca_argp_param_create(&rep_pci_addr_param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_error_get_descr(result));
        goto exit;
    }
    doca_argp_param_set_short_name(rep_pci_addr_param, "r");
    doca_argp_param_set_long_name(rep_pci_addr_param, "rep-pci");
    doca_argp_param_set_description(rep_pci_addr_param,
                                    "DOCA device representor PCI address (needed only on DPU)");
    doca_argp_param_set_callback(rep_pci_addr_param, rep_pci_addr_callback);
    doca_argp_param_set_type(rep_pci_addr_param, DOCA_ARGP_TYPE_STRING);
#ifdef DOCA_ARCH_DPU
    doca_argp_param_set_mandatory(rep_pci_addr_param);
#endif
    result = doca_argp_register_param(rep_pci_addr_param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register param: %s", doca_error_get_descr(result));
        goto exit;
    }

    result = doca_argp_start(argc, argv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to parse arguments: %s", doca_error_get_descr(result));
        goto exit;
    }

    return DOCA_SUCCESS;

exit:
    doca_argp_destroy();
    return result;
}

void clean_argp(void)
{
    doca_argp_destroy();
}
