#ifndef CA_FUNCTIONS_H_
#define CA_FUNCTIONS_H_ 1

#include <stddef.h>

float
ca_stats_correlation (const float *lhs, const float *rhs, size_t count);

float
ca_stats_rank_correlation (const float *values, size_t count);

#endif /* !CA_FUNCTIONS_H_ */
