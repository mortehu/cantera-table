#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <string.h>

#include "ca-functions.h"
#include "ca-table.h"
#include "query.h"

#define POW2(x) ((x) * (x))

/*****************************************************************************/

int
CA_compare_like (const char *haystack, const char *filter)
{
  /* Process everything up to the first '%' */
  for (;;)
    {
      if (!*filter)
        return !*haystack;

      if (*filter == '%')
        break;

      if (*haystack != *filter && (*filter != '_' || !*haystack))
        return 0;

      ++haystack;
      ++filter;
    }

  if (filter[0] == '%' && !filter[1])
    return 1;

  ca_set_error ("LIKE expression is too complex");

  return -1;
}

/*****************************************************************************/

float
ca_stats_correlation (const float *lhs,
                      const float *rhs,
                      size_t count)
{
  double sum_lhs = 0, sum_rhs = 0;
  double sum_sqerr_lhs = 0, sum_sqerr_rhs = 0;
  double sum_prod = 0;
  double sample_stdev_lhs, sample_stdev_rhs;
  double avg_lhs, avg_rhs;

  size_t i;

  for (i = 0; i < count; ++i)
    {
      sum_lhs += lhs[i];
      sum_rhs += rhs[i];
    }

  avg_lhs = sum_lhs / count;
  avg_rhs = sum_rhs / count;

  for (i = 0; i < count; ++i)
    {
      sum_sqerr_lhs += POW2 (lhs[i] - avg_lhs);
      sum_sqerr_rhs += POW2 (rhs[i] - avg_rhs);
      sum_prod += (lhs[i] - avg_lhs) * (rhs[i] - avg_rhs);
    }

  sample_stdev_lhs = sqrt (sum_sqerr_lhs);
  sample_stdev_rhs = sqrt (sum_sqerr_rhs);

  return sum_prod / (sample_stdev_lhs * sample_stdev_rhs);
}

/*****************************************************************************/

struct float_rank
{
  float value;
  unsigned int rank;
};

static size_t
partition (struct float_rank *data, size_t count, size_t pivot_index)
{
  size_t i, store_index;
  struct float_rank pivot, tmp;

  store_index = 0;

  pivot = data[pivot_index];

  data[pivot_index] = data[count - 1];
  data[count - 1] = pivot;

  for (i = 0; i < count - 1; ++i)
    {
      if (data[i].value < pivot.value)
        {
          tmp = data[store_index];
          data[store_index] = data[i];
          data[i] = tmp;

          ++store_index;
        }
    }

  data[count - 1] = data[store_index];
  data[store_index] = pivot;

  return store_index;
}

void
quicksort (struct float_rank *data, size_t count)
{
  size_t pivot_index;

  while (count >= 2)
    {
      pivot_index = count / 2;

      pivot_index = partition (data, count, pivot_index);

      quicksort (data, pivot_index);

      data += pivot_index + 1;
      count -= pivot_index + 1;
    }
}

float
ca_stats_rank_correlation (const float *values, size_t count)
{
  struct float_rank *sorted_values = NULL;
  unsigned int *ranks = NULL;
  size_t i;

  uint64_t sum_lhs = 0, sum_rhs = 0;
  double sum_sqerr_lhs = 0, sum_sqerr_rhs = 0;
  double sum_prod = 0;
  double sample_stdev_lhs, sample_stdev_rhs;
  double avg_lhs, avg_rhs;

  float result = -2.0;

  if (!(sorted_values = ca_malloc (sizeof (*sorted_values) * count)))
    goto fail;

  if (!(ranks = ca_malloc (sizeof (*ranks) * count)))
    goto fail;

  for (i = 0; i < count; ++i)
    {
      sorted_values[i].value = values[i];
      sorted_values[i].rank = i;
    }

  quicksort (sorted_values, count);

  /* XXX: Tiebreaker does not match Wikipedia description */

  for (i = 0; i < count; )
    {
      size_t first_rank;
      float value;

      first_rank = i;
      value = sorted_values[i].value;

      do
        {
          ranks[sorted_values[i].rank] = first_rank;

          sum_rhs += first_rank;

          ++i;
        }
      while (i < count && sorted_values[i].value == value);
    }

  sum_lhs = count * (count - 1) / 2; /* \sum_{x=1}^{count-1} x */

  avg_lhs = (double) sum_lhs / count;
  avg_rhs = (double) sum_rhs / count;

  for (i = 0; i < count; ++i)
    {
      sum_sqerr_lhs += POW2 (i - avg_lhs);
      sum_sqerr_rhs += POW2 (ranks[i] - avg_rhs);
      sum_prod += (i - avg_lhs) * (ranks[i] - avg_rhs);
    }

  sample_stdev_lhs = sqrt (sum_sqerr_lhs);
  sample_stdev_rhs = sqrt (sum_sqerr_rhs);

  result = sum_prod / (sample_stdev_lhs * sample_stdev_rhs);

fail:

  free (ranks);
  free (sorted_values);

  return result;
}
