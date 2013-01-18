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

#define DATADIR "/tmp/ts"

static int do_ignore_missing;
static int do_destructive;
static int print_version;
static int print_help;

static struct option long_options[] =
{
    { "ignore-missing", no_argument, &do_ignore_missing, 1 },
    { "destructive",    no_argument, &do_destructive,    1 },
    { "version",        no_argument, &print_version,     1 },
    { "help",           no_argument, &print_help,        1 },
    { NULL, 0, NULL, 0 }
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
             "      --ignore-missing       ignore missing input files\n"
             "      --destructive          remove input files after processing\n"
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

  struct table **inputs;
  char **input_paths;
  int input_count;

  input_paths = argv + optind + 1;
  input_count = argc - optind - 1;

  inputs = safe_malloc (sizeof (*inputs) * input_count);

  for (i = 0; i < input_count; )
    {
      if (!(inputs[i] = table_open (input_paths[i])))
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

          errx (EX_NOINPUT, "Error: %s", ca_last_error ());
        }

      ++i;
    }

  if (!input_count)
    errx (EX_NOINPUT, "No input files");

  if (!(output = table_create (output_path)))
    errx (EX_CANTCREAT, "Failed to create '%s': %s", output_path, ca_last_error ());

  table_iterate_multiple (inputs, input_count, data_callback, NULL);

  if (prev_key)
    {
      data_flush (prev_key);

      free (prev_key);
    }

  table_close (output);

  for (i = 0; i < input_count; ++i)
    {
      table_close (inputs[i]);

      if (do_destructive && strcmp (input_paths[i], output_path))
        unlink (input_paths[i]);
    }

  return EXIT_SUCCESS;
}
