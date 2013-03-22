#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <string.h>

#include "ca-functions.h"
#include "ca-internal.h"
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

float
ca_stats_rank_correlation (const float *values, size_t count)
{
  struct CA_float_rank *sorted_values = NULL;
  float *ranks = NULL;
  size_t i;

  double sum_lhs = 0;
  double sum_rhs = 0;
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

  CA_sort_float_rank (sorted_values, count);

  for (i = 0; i < count; )
    {
      size_t end, sum;
      float value, average_rank;

      value = sorted_values[i].value;

      sum = i;
      end = i + 1;

      while (end < count && sorted_values[end].value == value)
        sum += end++;

      average_rank = (float) sum / (end - i);
      sum_rhs += (end - i) * average_rank;

      while (i != end)
        ranks[sorted_values[i++].rank] = average_rank;
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

/*****************************************************************************/

float
ca_sql_ts_sample (struct iovec *iov, int64_t timestamp)
{
  const uint8_t *begin, *end;
  float result = NAN;
  struct ca_offset_score *samples;
  uint32_t i, sample_count;

  begin = (const uint8_t *) iov->iov_base;
  end = begin + iov->iov_len;

  if (-1 == ca_parse_offset_score_array (&begin,
                                         &samples, &sample_count))
    return result;


  for (i = 0; i < sample_count; ++i)
    {
      if (timestamp == samples[i].offset)
        result = samples[i].score;
    }

  free (samples);

  return result;
}
