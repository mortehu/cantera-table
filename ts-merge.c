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
  aggregate_max,
  aggregate_equal
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

static enum ca_type *column_types;
static size_t column_alloc, column_count;

enum sample_type
{
  SAMPLE_TIME_FLOAT4,
  SAMPLE_DATA
};

struct sample
{
  enum sample_type type;

  union
    {
      struct
        {
          uint64_t time;
          float value;
        } time_float4;
      struct iovec data;
    } v;
};

static char *prev_key;
static struct sample *samples;
static size_t sample_alloc, sample_count;

static int
samplecmp (const void *vlhs, const void *vrhs)
{
  const struct sample *lhs = vlhs;
  const struct sample *rhs = vrhs;

  if (lhs->type != rhs->type)
    return (lhs->type < rhs->type) ? -1 : 1;

  switch (lhs->type)
    {
    case SAMPLE_TIME_FLOAT4:

      if (lhs->v.time_float4.time != rhs->v.time_float4.time)
        return (lhs->v.time_float4.time < rhs->v.time_float4.time) ? -1 : 1;

      break;

    case SAMPLE_DATA:

        {
          size_t i;
          const uint8_t *begin_lhs, *end_lhs;
          const uint8_t *begin_rhs, *end_rhs;

          begin_lhs = lhs->v.data.iov_base;
          end_lhs = begin_lhs + lhs->v.data.iov_len;
          begin_rhs = rhs->v.data.iov_base;
          end_rhs = begin_rhs + rhs->v.data.iov_len;

          for (i = 0; i < column_count; ++i)
            {
              switch (column_types[i])
                {
                case CA_TEXT:

                  assert (!end_lhs[-1]);
                  assert (!end_rhs[-1]);

                  return strcmp ((const char *) begin_lhs, (const char *) begin_rhs);

                  break;

                case CA_TIME:
                case CA_INT64:

                    {
                      int64_t int_lhs, int_rhs;

                      memcpy (&int_lhs, begin_lhs, sizeof (int64_t)); begin_lhs += sizeof (int64_t);
                      memcpy (&int_rhs, begin_rhs, sizeof (int64_t)); begin_rhs += sizeof (int64_t);

                      if (int_lhs != int_rhs)
                        return (int_lhs < int_rhs) ? -1 : 1;
                    }

                  break;

                default:

                  errx (EXIT_FAILURE, "Don't know how to compare type %d", column_types[i]);
                }

              assert (begin_lhs <= end_rhs);
            }

          assert (begin_lhs == end_rhs);
        }

      break;
    }

  return 0;
}

static int
data_flush (const char *key) CA_USE_RESULT;

static int
sample_aggregate (struct sample *samples, size_t count)
{
  size_t i;

  if (aggregate_function == aggregate_abort)
    {
      ca_set_error ("Duplicate values found, but no aggregate function given");

      return -1;
    }

  switch (samples[0].type)
    {
    case SAMPLE_TIME_FLOAT4:

        {
          double tmp = samples[0].v.time_float4.value;

          switch (aggregate_function)
            {
            case aggregate_avg:

              for (i = 1; i < count; ++i)
                tmp += samples[count].v.time_float4.value;

              tmp /= i;

              break;

            case aggregate_sum:

              for (i = 1; i < count; ++i)
                tmp += samples[count].v.time_float4.value;

              break;

            case aggregate_min:

              for (i = 1; i < count; ++i)
                {
                  if (tmp > samples[i].v.time_float4.value)
                    tmp = samples[i].v.time_float4.value;
                }

              break;

            case aggregate_max:

              for (i = 1; i < count; ++i)
                {
                  if (tmp < samples[i].v.time_float4.value)
                    tmp = samples[i].v.time_float4.value;
                }

              break;

            case aggregate_equal:

              for (i = 1; i < count; ++i)
                {
                  if (tmp != samples[i].v.time_float4.value)
                    {
                      for (i = 0; i < count; ++i)
                        fprintf (stderr, " %.4g", samples[i].v.time_float4.value);
                      fprintf (stderr, "\n");
                      ca_set_error ("Aggregate \"equal\" used, but not all samples are equal");

                      return -1;
                    }
                }

              break;

            default:

              ca_set_error ("Unsupported aggregate function for time_float4");

              return -1;
            }

          samples[0].v.time_float4.value = tmp;
        }

      return 0;

    default:

      switch (aggregate_function)
        {
        case aggregate_min:

          /* Smallest element already in position 0 */

          break;

        case aggregate_max:

          samples[0] = samples[count - 1];

          break;

        case aggregate_equal:

          if (0 != samplecmp (&samples[0], &samples[count - 1]))
            {
              ca_set_error ("Aggregate \"equal\" used, but not all values are equal");

              return -1;
            }

          break;

        default:

          ca_set_error ("Unsupported aggregate function for data");

          return -1;
        }

      return 0;
    }
}

static int
data_flush (const char *key)
{
  size_t i, next;
  int result = -1;

  qsort (samples, sample_count, sizeof (*samples), samplecmp);

  /* XXX: Should be able to perform these two loops as one */

  /* Combine samples with duplicate time values */

  switch (samples[0].type)
    {
    case SAMPLE_TIME_FLOAT4:

        {
          for (i = 0; i < sample_count; ++i)
            {
              next = i + 1;

              if (next == sample_count || 0 != samplecmp (&samples[next], &samples[i]))
                continue;

              do
                {
                  ++next;
                }
              while (next < sample_count && 0 == samplecmp (&samples[next], &samples[i]));

              if (-1 == sample_aggregate (&samples[i], next - i))
                {
                  ca_set_error ("Error merging '%s': %s", key, ca_last_error ());
                  return -1;
                }

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

              start_time = samples[i].v.time_float4.time;

              series_values[0] = samples[i].v.time_float4.value;
              series_count = 1;

              if (++i < sample_count)
                {
                  interval = samples[i].v.time_float4.time - start_time;

                  do
                    {
                      if (series_count == series_alloc
                          && -1 == ARRAY_GROW (&series_values, &series_alloc))
                        {
                          free (series_values);
                          goto done;
                        }

                      series_values[series_count++] = samples[i++].v.time_float4.value;
                    }
                  while (i != sample_count && samples[i].v.time_float4.time - samples[i - 1].v.time_float4.time == interval);
                }

              if (-1 == ca_table_write_time_float4 (output, key, start_time, interval, series_values, series_count))
                {
                  free (series_values);
                  goto done;
                }
            }

          free (series_values);
        }

      break;

    case SAMPLE_DATA:

      if (sample_count > 1
          && -1 == sample_aggregate (samples, sample_count))
        goto done;

      if (-1 == ca_table_insert_row (output, key, &samples[0].v.data, 1))
        goto done;

      break;
    }

  sample_count = 0;
  result = 0;

done:

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

  if (column_count == 1 && column_types[0] == CA_TIME_SERIES)
    {
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

              samples[sample_count].type = SAMPLE_TIME_FLOAT4;
              samples[sample_count].v.time_float4.time = start_time + i * interval;
              samples[sample_count].v.time_float4.value = sample_values[i];
              ++sample_count;
            }
        }
    }
  else
    {
      if (sample_count == sample_alloc
          && -1 == ARRAY_GROW (&samples, &sample_alloc))
        return -1;

      samples[sample_count].type = SAMPLE_DATA;
      samples[sample_count].v.data = *value;
      ++sample_count;
    }

  return 0;
}

const int
columns_parse (char *columns)
{
  char *column;

  for (column = strtok (columns, ","); column; column = strtok (NULL, ","))
    {
      enum ca_type type;

      if (CA_INVALID == (type = ca_type_from_string (column)))
        {
          ca_set_error ("Unknown type '%s'", column);

          return -1;
        }

      if (column_count == column_alloc
          && -1 == ARRAY_GROW (&column_types, &column_alloc))
        return -1;

      column_types[column_count++] = type;
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
          else if (!strcmp (optarg, "equal"))
            aggregate_function = aggregate_equal;
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
      printf ("Usage: %s [OPTION]... COL0,COL1,... OUTPUT INPUT...\n"
             "\n"
             "      --aggregate=NAME       set aggregate function for duplicates\n"
             "                             (avg, sum, min, max, equal)\n"
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

  if (optind + 3 > argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... COL0,COL1,... OUTPUT INPUT...", argv[0]);

  if (-1 == columns_parse (argv[optind++]))
    errx (EXIT_FAILURE, "Error parsing column list: %s", ca_last_error ());

  char *output_path = argv[optind++];

  struct ca_table **inputs;
  char **input_paths;
  int input_count;

  time_t newest_input = 0;
  int input_is_dirty = 0;

  input_paths = argv + optind;
  input_count = argc - optind;

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

  return result;
}
