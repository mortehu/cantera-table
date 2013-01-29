#ifndef CA_FUNCTIONS_H_
#define CA_FUNCTIONS_H_ 1

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

float
ca_stats_correlation (const float *lhs, const float *rhs, size_t count);

float
ca_stats_rank_correlation (const float *values, size_t count);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !CA_FUNCTIONS_H_ */
