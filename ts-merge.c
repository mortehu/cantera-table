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

static struct table *output;

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

static void
data_flush (const char *key)
{
  size_t i, next;

  qsort (samples, sample_count, sizeof (*samples), samplecmp);

  /* XXX: Should be able to perform these two loops as one */

  /* Average samples with duplicate time values */

  for (i = 0; i < sample_count; ++i)
    {
      next = i + 1;

      if (next == sample_count || samples[next].time != samples[i].time)
        continue;

      do
        {
          samples[i].value += samples[next].value;

          ++next;
        }
      while (next < sample_count && samples[next].time == samples[i].time);

      samples[i].value /= (next - i);

      sample_count -= (next - i - 1);

      memmove (samples + i + 1,
               samples + next,
               sizeof (*samples) * sample_count - i - 1);
    }

  float *series_values = NULL;
  size_t series_alloc = 0, series_count = 0;

  /* Make sure there's room for the first element */
  ARRAY_GROW (&series_values, &series_alloc);

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
              if (series_count == series_alloc)
                ARRAY_GROW (&series_values, &series_alloc);

              series_values[series_count++] = samples[i++].value;
            }
          while (i != sample_count && samples[i].time - samples[i - 1].time == interval);
        }

      table_write_samples (output, key, start_time, interval, series_values, series_count);
    }

  sample_count = 0;
}

static void
data_callback (const char *key, const void *value, size_t value_size,
               void *opaque)
{
  const uint8_t *begin, *end;

  if (!prev_key || strcmp (key, prev_key))
    {
      if (prev_key)
        data_flush (prev_key);

      free (prev_key);
      prev_key = safe_strdup (key);
    }

  begin = (const uint8_t *) value;
  end = (const uint8_t *) value + value_size;

  while (begin != end)
    {
      switch (*begin++)
        {
        case CA_TIME_SERIES:

            {
              uint64_t start_time;
              uint32_t interval;
              const float *sample_values;
              size_t count, i;

              table_parse_time_series (&begin,
                                       &start_time, &interval,
                                       &sample_values, &count);

              for (i = 0; i < count; ++i)
                {
                  if (sample_count == sample_alloc)
                    ARRAY_GROW (&samples, &sample_alloc);

                  samples[sample_count].time = start_time + i * interval;
                  samples[sample_count].value = sample_values[i];
                  ++sample_count;
                }
            }

          break;

        default:

          errx (EX_DATAERR, "Unexpected data type %d for key '%s'",
                begin[-1], key);
        }
    }
}

int
main (int argc, char **argv)
{
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
      printf ("Usage: %s [OPTION]... OUTPUT INPUT...\n"
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

  if (optind + 2 > argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... OUTPUT INPUT...", argv[0]);

  struct table **inputs;

  inputs = safe_malloc (sizeof (*inputs) * (argc - optind - 1));

  for (i = 0; i < argc - optind - 1; ++i)
    inputs[i] = table_open (argv[optind + i + 1]);

  output = table_create (argv[optind]);

  table_iterate_multiple (inputs, argc - optind - 1, data_callback, NULL);

  if (prev_key)
    {
      data_flush (prev_key);

      free (prev_key);
    }

  table_close (output);

  for (i = 0; i < argc - optind - 1; ++i)
    table_close (inputs[i]);

  return EXIT_SUCCESS;
}
