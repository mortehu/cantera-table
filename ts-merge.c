#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sysexits.h>
#include <unistd.h>

#include "ca-table.h"
#include "memory.h"

enum aggregate_function
{
  aggregate_abort,
  aggregate_avg,
  aggregate_sum,
  aggregate_min,
  aggregate_max
};

static enum aggregate_function aggregate_function = aggregate_abort;
static int do_debug;
static int do_ignore_missing;
static int do_no_fsync;
static int print_version;
static int print_help;

static struct option long_options[] =
{
    { "aggregate",      required_argument, NULL,               'A' },
    { "debug",          no_argument,       &do_debug,          1 },
    { "ignore-missing", no_argument,       &do_ignore_missing, 1 },
    { "no-fsync",       no_argument,       &do_no_fsync,       1 },
    { "version",        no_argument,       &print_version,     1 },
    { "help",           no_argument,       &print_help,        1 },
    { NULL, 0, NULL, 0 }
};

static struct ca_table *output;

struct sample
{
  uint64_t time;
  float value;
};

static char *prev_key;
static struct sample *samples;
static size_t sample_alloc, sample_count;

static int
samplecmp (const void *vlhs, const void *vrhs)
{
  const struct sample *lhs = vlhs;
  const struct sample *rhs = vrhs;

  if (lhs->time != rhs->time)
    return (lhs->time < rhs->time) ? -1 : 1;

  return 0;
}

static int
data_flush (const char *key) CA_USE_RESULT;

static float
aggregate (const float *values, size_t count)
{
  double tmp = 0.0;
  size_t i;

  switch (aggregate_function)
    {
    case aggregate_avg:

      for (i = 0; i < count; ++i)
        tmp += values[count];

      return tmp / i;

    case aggregate_sum:

      for (i = 0; i < count; ++i)
        tmp += values[count];

      return tmp;

    case aggregate_min:

      tmp = values[0];

      for (i = 1; i < count; ++i)
        {
          if (tmp > values[i])
            tmp = values[i];
        }

      return tmp;

    case aggregate_max:

      tmp = values[0];

      for (i = 1; i < count; ++i)
        {
          if (tmp < values[i])
            tmp = values[i];
        }

      return tmp;

    default:

      assert (!"invalid aggregate function");

      return 0;
    }
}

static int
data_flush (const char *key)
{
  size_t i, next;
  int result = -1;

  float *aggregate_values = NULL;
  size_t aggregate_alloc = 0, aggregate_count;

  qsort (samples, sample_count, sizeof (*samples), samplecmp);

  /* XXX: Should be able to perform these two loops as one */

  /* Combine samples with duplicate time values */

  for (i = 0; i < sample_count; ++i)
    {
      next = i + 1;

      if (next == sample_count || samples[next].time != samples[i].time)
        continue;

      if (aggregate_function == aggregate_abort)
        {
          ca_set_error ("Duplicate time values found for '%s', but no aggregate function given", key);

          return -1;
        }

      aggregate_count = 0;

      do
        {
          if (aggregate_count == aggregate_alloc
              && -1 == ARRAY_GROW (&aggregate_values, &aggregate_alloc))
            return -1;

          aggregate_values[aggregate_count++] = samples[next].value;

          ++next;
        }
      while (next < sample_count && samples[next].time == samples[i].time);

      samples[i].value = aggregate (aggregate_values, aggregate_count);

      sample_count -= (next - i - 1);

      memmove (samples + i + 1,
               samples + next,
               sizeof (*samples) * (sample_count - i - 1));
    }

  float *series_values = NULL;
  size_t series_alloc = 0, series_count = 0;

  /* Make sure there's room for the first element */
  if (-1 == ARRAY_GROW (&series_values, &series_alloc))
    goto done;

  for (i = 0; i < sample_count; )
    {
      uint64_t start_time;
      uint32_t interval = 0;

      start_time = samples[i].time;

      series_values[0] = samples[i].value;
      series_count = 1;

      if (++i < sample_count)
        {
          interval = samples[i].time - start_time;

          do
            {
              if (series_count == series_alloc
                  && -1 == ARRAY_GROW (&series_values, &series_alloc))
                goto done;

              series_values[series_count++] = samples[i++].value;
            }
          while (i != sample_count && samples[i].time - samples[i - 1].time == interval);
        }

      if (-1 == ca_table_write_time_float4 (output, key, start_time, interval, series_values, series_count))
        goto done;
    }

  sample_count = 0;
  result = 0;

done:

  free (series_values);

  return result;
}

static int
data_callback (const char *key, const struct iovec *value, void *opaque)
{
  const uint8_t *begin, *end;

  if (!prev_key || strcmp (key, prev_key))
    {
      if (prev_key && -1 == data_flush (prev_key))
        return -1;

      free (prev_key);

      if (!(prev_key = safe_strdup (key)))
        return -1;
    }

  begin = value->iov_base;
  end = begin + value->iov_len;

  while (begin != end)
    {
      uint64_t start_time;
      uint32_t i, interval, count;
      const float *sample_values;

      ca_data_parse_time_float4 (&begin,
                                 &start_time, &interval,
                                 &sample_values, &count);

      for (i = 0; i < count; ++i)
        {
          if (sample_count == sample_alloc
              && -1 == ARRAY_GROW (&samples, &sample_alloc))
            return -1;

          samples[sample_count].time = start_time + i * interval;
          samples[sample_count].value = sample_values[i];
          ++sample_count;
        }
    }

  return 0;
}

int
main (int argc, char **argv)
{
  int i, result = EXIT_FAILURE;

  while ((i = getopt_long (argc, argv, "", long_options, 0)) != -1)
    {
      switch (i)
        {
        case 'A':

          if (!strcmp (optarg, "avg"))
            aggregate_function = aggregate_avg;
          else if (!strcmp (optarg, "sum"))
            aggregate_function = aggregate_sum;
          else if (!strcmp (optarg, "min"))
            aggregate_function = aggregate_min;
          else if (!strcmp (optarg, "max"))
            aggregate_function = aggregate_max;
          else
            errx (EX_USAGE, "Unknown aggregate function '%s'", optarg);

          break;

        case 0:

          break;

        case '?':

          errx (EX_USAGE, "Try '%s --help' for more information.", argv[0]);
        }
    }

  if (print_help)
    {
      printf ("Usage: %s [OPTION]... OUTPUT INPUT...\n"
             "\n"
             "      --aggregate=NAME       set aggregate function for duplicates\n"
             "                             (avg, sum, min, max)\n"
             "      --debug                print debug output\n"
             "      --ignore-missing       ignore missing input files\n"
             "      --no-relative          do not store relative timestamps\n"
             "      --no-fsync             do not use fsync on new tables\n"
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

  if (optind + 2 > argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... OUTPUT INPUT...", argv[0]);

  char *output_path = argv[optind];

  struct ca_table **inputs;
  char **input_paths;
  int input_count;

  input_paths = argv + optind + 1;
  input_count = argc - optind - 1;

  inputs = safe_malloc (sizeof (*inputs) * input_count);

  for (i = 0; i < input_count; )
    {
      if (!(inputs[i] = ca_table_open ("write-once", input_paths[i], O_RDONLY, 0)))
        {
          if (do_ignore_missing)
            {
              fprintf (stderr, "Warning: %s\n", ca_last_error ());

              --input_count;

              memmove (input_paths + i,
                       input_paths + i + 1,
                       sizeof (*input_paths) * (input_count - i));

              continue;
            }

          goto done;
        }

      if (!ca_table_is_sorted (inputs[i]))
        {
          struct ca_table *sort_tmp;

          if (do_debug)
            fprintf (stderr, "Sorting '%s'...\n", input_paths[i]);

          sort_tmp = ca_table_open ("write-once", input_paths[i], O_CREAT | O_TRUNC | O_RDWR, 0666);

          if (do_no_fsync)
            ca_table_set_flag (sort_tmp, CA_TABLE_NO_FSYNC);

          if (-1 == ca_table_sort (sort_tmp, inputs[i])
              || -1 == ca_table_sync (sort_tmp))
            {
              ca_table_close (sort_tmp);
              ca_table_close (inputs[i]);

              goto done;
            }

          ca_table_close (inputs[i]);

          inputs[i] = sort_tmp;
        }

      ++i;
    }

  if (!input_count)
    errx (EX_NOINPUT, "No input files");

  if (!(output = ca_table_open ("write-once", output_path, O_CREAT | O_TRUNC | O_WRONLY, 0666)))
    goto done;

  if (do_no_fsync)
    ca_table_set_flag (output, CA_TABLE_NO_FSYNC);

  if (-1 == ca_table_merge (inputs, input_count, data_callback, NULL))
    goto done;

  if (prev_key)
    {
      if (-1 == data_flush (prev_key))
        goto done;
    }

  if (-1 == ca_table_sync (output))
    goto done;

  result = EXIT_SUCCESS;

done:

  if (result == EXIT_FAILURE)
    fprintf (stderr, "%s\n", ca_last_error ());

  free (prev_key);

  ca_table_close (output);

  for (i = 0; i < input_count; ++i)
    ca_table_close (inputs[i]);

  free (inputs);

  return EXIT_SUCCESS;
}
