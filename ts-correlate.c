#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sysexits.h>

#include "memory.h"
#include "smalltable.h"

#define DATADIR "/tmp/ts"

static int print_version;
static int print_help;

static struct option long_options[] =
{
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

struct sample
{
  uint64_t time;
  float value;
};

static struct sample *samples;
static size_t sample_alloc, sample_count;

static float *samples_lhs;
static float *samples_rhs;

static float
sum (const float *values, size_t count)
{
  const float *end = values + count;
  float sum = 0.0;

  while (values != end)
    sum += *values++;

  return sum;
}

static void
data_callback (const void *data, size_t size)
{
  const char *key;
  const uint8_t *begin, *end;
  size_t key_length;
  uint64_t last_time = 0;

  size_t sample_offset = 0;
  size_t sample_pair_count = 0;

  key = data;
  key_length = strlen (key) + 1;

  begin = (const uint8_t *) data + key_length;
  end = (const uint8_t *) data + size;

  while (begin != end)
    {
      uint64_t start_time;
      uint32_t interval;
      const float *sample_values;
      size_t count, i;

      switch (*begin++)
        {
        case TABLE_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &start_time, &interval,
                                   &sample_values, &count);

          break;

        case TABLE_RELATIVE_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &start_time, &interval,
                                   &sample_values, &count);
          start_time += last_time;

          break;

        default:

          errx (EX_DATAERR, "Unexpected data type %u", begin[-1]);
        }

      for (i = 0; i < count; ++i)
        {
          uint64_t time;

          time = start_time + i * interval;

          if (samples[sample_offset].time < time)
            return;

          if (time < samples[sample_offset].time)
            continue;

          samples_lhs[sample_pair_count] = samples[sample_offset].value;
          samples_rhs[sample_pair_count] = sample_values[i];
          ++sample_pair_count;
          ++sample_offset;
        }

      last_time = start_time;
    }

  if (sample_pair_count != sample_count)
    return;

    {
      float mean_lhs, mean_rhs, stddev_lhs = 0, stddev_rhs = 0;
      float cov = 0, r;
      size_t i;

      mean_lhs = sum (samples_lhs, sample_pair_count) / sample_pair_count;
      mean_rhs = sum (samples_rhs, sample_pair_count) / sample_pair_count;

      for (i = 0; i < sample_pair_count; ++i)
        {
          float lhs, rhs;

          lhs = samples_lhs[i] - mean_lhs;
          rhs = samples_rhs[i] - mean_rhs;

          cov += lhs * rhs;
          stddev_lhs += lhs * lhs;
          stddev_rhs += rhs * rhs;
        }

      stddev_lhs = sqrt (stddev_lhs);
      stddev_rhs = sqrt (stddev_rhs);

      if (!stddev_lhs || !stddev_rhs)
        return;

      r = cov / (stddev_lhs * stddev_rhs);

      float mean = atanh (r);
      float error = 3 / sqrt (sample_pair_count - 3);

      float low_estimate = tanh (mean - error);
      float high_estimate = tanh (mean + error);

      if (low_estimate > 0.0)
        printf ("%.6g\t%s\n", low_estimate, key);
      else if (high_estimate < -0.0)
        printf ("%.6g\t%s\n", high_estimate, key);
    }
}

int
main (int argc, char **argv)
{
  struct table *table;
  int i;

  while ((i = getopt_long (argc, argv, "", long_options, 0)) != -1)
    {
      switch (i)
        {
        case 0:

          break;

        case '?':

          errx (EX_USAGE, "Try '%s --help' for more information.", argv[0]);
        }
    }

  if (print_help)
    {
      printf ("Usage: %s [OPTION]... TABLE KEY\n"
             "\n"
             "      --help     display this help and exit\n"
             "      --version  display version information\n"
             "\n"
             "Report bugs to <morten.hustveit@gmail.com>\n",
             argv[0]);

      return EXIT_SUCCESS;
    }

  if (print_version)
    {
      fprintf (stdout, "%s\n", PACKAGE_STRING);

      return EXIT_SUCCESS;
    }

  if (optind + 2 != argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... TABLE KEY", argv[0]);

  table = table_open (argv[optind++]);

  const char *key = argv[optind++];

  uint64_t last_time = 0;
  const uint8_t *begin, *end;
  size_t size;

  if (!(begin = table_lookup (table, key, &size)))
    err (EX_DATAERR, "Key '%s' not found", key);

  end = begin + size;

  while (begin != end)
    {
      uint64_t start_time;
      uint32_t interval;
      const float *sample_values;
      size_t count, i;

      switch (*begin++)
        {
        case TABLE_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &start_time, &interval,
                                   &sample_values, &count);

          break;

        case TABLE_RELATIVE_TIME_SERIES:

          table_parse_time_series (&begin,
                                   &start_time, &interval,
                                   &sample_values, &count);
          start_time += last_time;

          break;

        default:

          errx (EX_DATAERR, "Unexpected data type %u", begin[-1]);
        }

      for (i = 0; i < count; ++i)
        {
          if (sample_count == sample_alloc)
            ARRAY_GROW (&samples, &sample_alloc);

          samples[sample_count].time = start_time + i * interval;
          samples[sample_count].value = sample_values[i];
          ++sample_count;
        }

      last_time = start_time;
    }

  samples_lhs = safe_malloc (sizeof (*samples_lhs) * sample_count);
  samples_rhs = safe_malloc (sizeof (*samples_rhs) * sample_count);

  table_iterate (table, data_callback, TABLE_ORDER_PHYSICAL);

  table_close (table);

  return EXIT_SUCCESS;
}
