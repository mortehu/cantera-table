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
  while (*haystack && *filter)
    {
      if (*filter == '%')
        break;

      if (*haystack != *filter && *filter != '_')
        return 0;

      ++haystack;
      ++filter;
    }

  if (!*filter)
    return !*haystack;

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

static int
float_compare (const void *vlhs, const void *vrhs)
{
  const float *lhs = vlhs;
  const float *rhs = vrhs;

  if (*lhs != *rhs)
    return (*lhs < *rhs) ? -1 : 1;

  return 0;
}

static size_t
float_lower_bound (float value, const float *array, size_t count)
{
  size_t first = 0, middle, half;

  while (count > 0)
    {
      half = count >> 1;
      middle = first + half;

      if (array[middle] < value)
        {
          first = middle + 1;
          count -= half + 1;
        }
      else
        count = half;
    }

  return first;
}

float
ca_stats_rank_correlation (const float *values, size_t count)
{
  float *sorted_values = NULL;
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

  /* XXX: The rank can be stored directly into an array instead of being looked
   *      up once per item.  If so, we also need a tie-finder */

  memcpy (sorted_values, values, sizeof (*sorted_values) * count);

  qsort (sorted_values, count, sizeof (*sorted_values), float_compare);

  for (i = 0; i < count; ++i)
    {
      ranks[i] = float_lower_bound (values[i], sorted_values, count);
      sum_rhs += ranks[i];
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
