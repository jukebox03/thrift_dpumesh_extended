#ifndef COMCH_MSGQ_H
#define COMCH_MSGQ_H

#include <doca_comch.h>
#include <doca_error.h>
#include <doca_comch_msgq.h>

struct objects;
struct doca_pe;

doca_error_t 
init_comch_dpa_msgq(struct objects *objs, struct doca_pe *pe);

#endif // COMCH_MSGQ_H