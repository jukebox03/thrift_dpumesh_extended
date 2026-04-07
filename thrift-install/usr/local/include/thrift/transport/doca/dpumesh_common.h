#ifndef DPUMESH_COMMON_H
#define DPUMESH_COMMON_H

/* ====== Common Flags (shared by Host, DPU ARM, and DPA) ====== */

/* CaseFlag (match Python CaseFlag) */
#define CASE_EXTERNAL  1
#define CASE_INGRESS   2
#define CASE_LOCAL     3

/* OpFlag (match Python OpFlag) */
#define OP_REQUEST     0x00
#define OP_RESPONSE    0x10

/* PoolType (match Python PoolType) */
#define POOL_NONE           0
#define POOL_HOST_TX_BODY   2
#define POOL_HOST_RX_BODY   4
#define POOL_DPU_TX_BODY    6
#define POOL_DPU_RX_BODY    7

/* ====== DOCA / DPA limits ====== */
#define MAX_DPA_RINGS       8
#define MAX_PODS            8

#endif /* DPUMESH_COMMON_H */
