#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sysexits.h>
#include <unistd.h>

#include "ca-table.h"

static int print_version;
static int print_help;

static char delimiter = '\t';
static const char *date_format = "%Y-%m-%d %H:%M:%S";

static time_t interval = 1;

static struct option long_options[] =
{
    { "delimiter",      required_argument, NULL,           'D' },
    { "date-format",    required_argument, NULL,           'A' },
    { "output-format",  required_argument, NULL,           'O' },
    { "schema",         required_argument, NULL,           'S' },
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

static struct ca_table *table_handle;

enum parse_state
{
  parse_key,
  parse_offset,
  parse_value
};

static enum parse_state state;

static char *key;
static size_t key_alloc;
static size_t key_length;

static char *offset;
static size_t offset_alloc;
static size_t offset_length;

static uint64_t current_offset;

static char *value_string;
static size_t value_string_alloc;
static size_t value_string_length;

static struct ca_offset_score *values;
static size_t value_alloc;
static size_t value_count;

static int do_map_documents;
static struct ca_table **summary_tables;
static uint64_t *summary_table_offsets;
static ssize_t summary_table_count;

static int do_summaries;

static void
flush_values (void)
{
  ca_sort_offset_score_by_offset (values, value_count);

  if (-1 == ca_table_write_offset_score (table_handle, key,
                                         values, value_count))
    errx (EXIT_FAILURE, "%s", ca_last_error ());

  value_count = 0;
}

static void
parse_data (const char *begin, const char *end)
{
  int no_match = 0;

  for (; begin != end; ++begin)
    {
      switch (state)
        {
        case parse_key:

          if (key_length == key_alloc
              && -1 == CA_ARRAY_GROW (&key, &key_alloc))
            errx (EXIT_FAILURE, "%s", ca_last_error ());

          if (*begin == delimiter)
            {
              if (value_count && key[key_length])
                flush_values ();

              key[key_length] = 0;

              state = do_summaries ? parse_value : parse_offset;

              break;
            }

          if (value_count && key[key_length] != *begin)
            flush_values ();

          key[key_length++] = *begin;

          break;

        case parse_offset:

          if (offset_length == offset_alloc
              && -1 == CA_ARRAY_GROW (&offset, &offset_alloc))
            errx (EXIT_FAILURE, "%s", ca_last_error ());

          if (*begin == delimiter)
            {
              offset[offset_length] = 0;

              if (do_map_documents)
                {
                  size_t i;
                  int ret;

                  no_match = 1;

                  for (i = summary_table_count; i-- > 0; )
                    {
                      if (-1 == (ret = ca_table_seek_to_key (summary_tables[i], offset)))
                        errx (EXIT_FAILURE, "Failed to seek to key '%s': %s", offset, ca_last_error ());

                      if (ret == 1)
                        {
                          current_offset = ca_table_offset (summary_tables[i])
                                         + summary_table_offsets[i];

                          no_match = 0;

                          break;
                        }
                    }
                }
              else
                {
                  struct tm tm;
                  char *end;

                  memset (&tm, 0, sizeof (tm));

                  if (!(end = strptime (offset, date_format, &tm)))
                    errx (EX_DATAERR, "Unable to parse date '%s' according to format '%s'",
                          offset, date_format);

                  if (*end)
                    errx (EX_DATAERR, "Junk at end of offset '%s': %s", offset, end);

                  current_offset = timegm (&tm);

                  if (!current_offset)
                    fprintf (stderr, "Warning: %s maps to 1970-01-01", date_format);
                }

              offset_length = 0;
              state = parse_value;

              break;
            }

          offset[offset_length++] = *begin;

          break;

        case parse_value:

          if (*begin == '\r')
            continue;

          if (value_string_length == value_string_alloc
              && -1 == CA_ARRAY_GROW (&value_string, &value_string_alloc))
            errx (EXIT_FAILURE, "%s", ca_last_error ());

          if (*begin == '\n')
            {
              if (do_summaries)
                {
                  struct iovec iov[2];

                  iov[0].iov_base = key;
                  iov[0].iov_len = key_length + 1;
                  iov[1].iov_base = value_string;
                  iov[1].iov_len = value_string_length;

                  if (value_string_length < 2
                      || value_string[0] != '{'
                      || value_string[value_string_length - 1] != '}')
                    errx (EXIT_FAILURE, "Summary values must start with '{' and end width '}'");

                  if (-1 == ca_table_insert_row (table_handle, iov, 2))
                    errx (EXIT_FAILURE, "%s", ca_last_error ());
                }
              else if (no_match)
                no_match = 0;
              else
                {
                  float fvalue;
                  char *endptr;

                  value_string[value_string_length] = 0;

                  fvalue = strtod (value_string, &endptr);

                  if (*endptr)
                    errx (EX_DATAERR, "Unable to parse value '%s'.  Unexpected suffix: '%s'",
                          value_string, endptr);


                  if (value_count == value_alloc
                      && -1 == CA_ARRAY_GROW (&values, &value_alloc))
                    errx (EXIT_FAILURE, "%s", ca_last_error ());

                  values[value_count].offset = current_offset;
                  values[value_count].score = fvalue;
                  ++value_count;
                }

              value_string_length = 0;
              key_length = 0;
              state = parse_key;

              break;
            }

          value_string[value_string_length++] = *begin;

          break;
        }
    }
}

static int
merge_time_series_callback (const struct iovec *value, void *opaque)
{
  const uint8_t *begin;
  const char *new_key;
  size_t new_key_length;

  struct ca_offset_score *new_values;
  uint32_t new_count;

  begin = value->iov_base;

  new_key = (const char *) begin;
  new_key_length = strlen (new_key) + 1;

  begin += new_key_length;

  if (!key || strcmp (key, new_key))
    {
      if (value_count)
        flush_values ();

      free (key);

      if (!(key = ca_strdup (new_key)))
        errx (EXIT_FAILURE, "ca_strdup failed: %s", ca_last_error ());
    }

  if (-1 == ca_offset_score_parse (&begin,
                                   &new_values, &new_count))
    errx (EXIT_FAILURE, "ca_offset_score_parse failed: %s", ca_last_error ());

  if (value_count + new_count > value_alloc
      && -1 == CA_ARRAY_GROW_N (&values, &value_alloc, new_count))
    errx (EXIT_FAILURE, "CA_ARRAY_GROW failed: %s", ca_last_error ());

  memcpy (&values[value_count], new_values, sizeof (*new_values) * new_count);
  value_count += new_count;

  free (new_values);

  return 0;
}

int
main (int argc, char **argv)
{
  const char *output_format = "time-series";
  const char *output_path;

  const char *schema_path = NULL;
  struct ca_schema *schema = NULL;

  int input_fd = STDIN_FILENO;
  int i;

  off_t file_size;

  setenv ("TZ", "", 1);

  while ((i = getopt_long (argc, argv, "", long_options, 0)) != -1)
    {
      switch (i)
        {
        case 0:

          break;

        case 'D':

          if (!*optarg)
            errx (EX_USAGE, "Provided delimiter is empty");

          if (optarg[1])
            errx (EX_USAGE, "Provided delimiter is more than one ASCII character");

          delimiter = *optarg;

          break;

        case 'A':

          date_format = optarg;

          break;

        case 'O':

          output_format = optarg;

          break;

        case 'S':

          schema_path = optarg;

          break;

        case 'I':

            {
              char *endptr;

              interval = strtol (optarg, &endptr, 0);

              if (*endptr)
                errx (EX_USAGE, "Failed to parse interval '%s'", optarg);

              if (interval <= 0)
                errx (EX_USAGE, "Sample interval too small");
            }

          break;

        case '?':

          errx (EX_USAGE, "Try '%s --help' for more information.", argv[0]);
        }
    }

  if (print_help)
    {
      printf ("Usage: %s [OPTION]... TABLE\n"
             "\n"
             "      --delimiter=DELIMITER  set input delimiter [%c]\n"
             "      --date-format=FORMAT   use provided date format [%s]\n"
             "      --date=DATE            use DATE as timestamp\n"
             "      --key=KEY              use KEY as key\n"
             "      --interval=INTERVAL    sample interval if both --date and --key are\n"
             "                             given\n"
             "      --help     display this help and exit\n"
             "      --version  display version information\n"
             "\n"
             "Report bugs to <morten.hustveit@gmail.com>\n",
             argv[0], delimiter, date_format);

      return EXIT_SUCCESS;
    }

  if (print_version)
    {
      fprintf (stdout, "%s\n", PACKAGE_STRING);

      return EXIT_SUCCESS;
    }

  if (!strcmp (output_format, "index"))
    {
      if (!schema_path)
        errx (EX_USAGE, "--output-format=index can only be used with --schema=PATH");

      if (!(schema = ca_schema_load (schema_path)))
        errx (EXIT_FAILURE, "Failed to load schema: %s", ca_last_error ());

      if (-1 == (summary_table_count = ca_schema_summary_tables (schema, &summary_tables,
                                                                 &summary_table_offsets)))
        errx (EXIT_FAILURE, "Failed to open summary tables: %s", ca_last_error ());

      do_map_documents = 1;
    }
  else if (!strcmp (output_format, "summaries"))
    do_summaries = 1;
  else if (strcmp (output_format, "time-series"))
    errx (EX_USAGE, "Invalid output format '%s'", output_format);

  if (optind + 1 > argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... TABLE [INPUT]...", argv[0]);

  output_path = argv[optind++];

  if (!(table_handle = ca_table_open ("write-once", output_path, O_CREAT | O_TRUNC | O_RDWR, 0666)))
    errx (EXIT_FAILURE, "Failed to create '%s': %s", output_path, ca_last_error ());

  if (optind == argc)
    {
      state = parse_key;

      if (-1 == (file_size = lseek (input_fd, 0, SEEK_END)))
        {
          char buffer[65536];
          ssize_t ret;

          while (0 < (ret = read (input_fd, buffer, sizeof (buffer))))
            parse_data (buffer, buffer + ret);

          if (ret == -1)
            err (EX_IOERR, "read failed");
        }
      else
        {
          void *map;

          if (!file_size)
            errx (EX_DATAERR, "input file has zero size");

          if (MAP_FAILED == (map = mmap (NULL, file_size, PROT_READ, MAP_SHARED, input_fd, 0)))
            err (EX_NOINPUT, "failed to mmap input");

          parse_data (map, (char *) map + file_size);

          munmap (map, file_size);
        }
    }
  else
    {
      struct ca_table **tables;
      size_t table_count;

      if (strcmp (output_format, "time-series"))
        errx (EX_USAGE, "File input only support for time-series data for now.  Please use standard input");

      table_count = argc - optind;

      if (!(tables = ca_malloc (sizeof (*tables) * table_count)))
        errx (EXIT_FAILURE, "ca_malloc failed: %s", ca_last_error ());

      for (i = optind; i < argc; ++i)
        {
          if (!(tables[i - optind] = ca_table_open ("write-once", argv[i], O_RDONLY, 0)))
            errx (EXIT_FAILURE, "ca_table_open failed: %s", ca_last_error ());

          if (!ca_table_is_sorted (tables[i - optind]))
            errx (EX_DATAERR, "Tabe '%s' is not sorted", ca_last_error ());
        }

      if (-1 == ca_table_merge (tables, table_count,
                                merge_time_series_callback, table_handle))
        errx (EXIT_FAILURE, "ca_table_merge failed: %s", ca_last_error ());
    }

  if (value_count)
    flush_values ();

  if (-1 == ca_table_sync (table_handle))
    errx (EXIT_FAILURE, "Failed to sync '%s': %s", argv[optind], ca_last_error ());

  ca_table_close (table_handle);

  return EXIT_SUCCESS;
}
