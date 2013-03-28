#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
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
    { "format",         required_argument, NULL,           'O' },
    { "schema",         required_argument, NULL,           'S' },
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

static struct ca_table *table_handle;

static struct ca_table **summary_tables;
static uint64_t *summary_table_offsets;
static ssize_t summary_table_count;

static void
dump_index (void)
{
  struct iovec offset_score, summary;
  int ret;

  while (1 == (ret = ca_table_read_row (table_handle, &offset_score)))
    {
      const char *key;
      const uint8_t *data;

      struct ca_offset_score *offsets;
      uint32_t i, offset_count;
      ssize_t j;

      key = offset_score.iov_base;

      data = (const uint8_t *) strchr (offset_score.iov_base, 0) + 1;

      if (-1 == (ret = ca_offset_score_parse (&data, &offsets,
                                              &offset_count)))
        break;

      for (i = 0; i < offset_count; ++i)
        {
          j = summary_table_count;

          while (--j && summary_table_offsets[j] > offsets[i].offset)
            ;

          if (-1 == (ret = ca_table_seek (summary_tables[j],
                                          offsets[i].offset - summary_table_offsets[j],
                                          SEEK_SET)))
            break;

          if (1 != (ret = ca_table_read_row (summary_tables[j], &summary)))
            {
              if (ret >= 0)
                ca_set_error ("ca_table_read_row unexpectedly returned %d",
                              (int) ret);

              break;
            }

          printf ("%s\t%s\t%.9g\n", key, summary.iov_base, offsets[i].score);

        }

      free (offsets);
    }

  if (ret == -1)
    errx (EXIT_FAILURE, "Error reading table: %s", ca_last_error ());
}

static void
dump_summaries (void)
{
  struct iovec summary;
  int ret;

  while (1 == (ret = ca_table_read_row (table_handle, &summary)))
    {
      const char *key, *value, *end;

      key = summary.iov_base;
      end = key + summary.iov_len;

      value = strchr (summary.iov_base, 0) + 1;

      printf ("%s\t%.*s\n", key, (int) (end - value), value);
    }

  if (ret == -1)
    errx (EXIT_FAILURE, "Error reading table: %s", ca_last_error ());
}

static void
dump_time_series (void)
{
  struct iovec offset_score;
  int ret;

  while (1 == (ret = ca_table_read_row (table_handle, &offset_score)))
    {
      const char *key;
      const uint8_t *data;

      struct ca_offset_score *offsets;
      uint32_t i, offset_count;

      key = offset_score.iov_base;

      data = (const uint8_t *) strchr (offset_score.iov_base, 0) + 1;

      if (-1 == (ret = ca_offset_score_parse (&data, &offsets,
                                              &offset_count)))
        break;

      if (!strcmp (date_format, "%s"))
        {
          for (i = 0; i < offset_count; ++i)
            printf ("%s\t%llu\t%.9g\n", key, (long long unsigned) offsets[i].offset, offsets[i].score);
        }
      else
        {
          char time_buffer[64];
          time_t time;
          struct tm tm;

          for (i = 0; i < offset_count; ++i)
            {
              time = offsets[i].offset;
              memset (&tm, 0, sizeof (tm));

              gmtime_r (&time, &tm);

              strftime (time_buffer, sizeof (time_buffer), date_format, &tm);

              printf ("%s\t%s\t%.9g\n", key, time_buffer, offsets[i].score);
            }
        }

      free (offsets);
    }

  if (ret == -1)
    errx (EXIT_FAILURE, "Error reading table: %s", ca_last_error ());
}

int
main (int argc, char **argv)
{
  const char *format = "time-series";

  const char *schema_path = NULL;
  struct ca_schema *schema = NULL;

  int i;

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

          format = optarg;

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

  if (optind + 1 != argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... TABLE", argv[0]);

  if (!(table_handle = ca_table_open ("write-once", argv[optind], O_RDONLY, 0666)))
    errx (EXIT_FAILURE, "Failed to open '%s': %s", argv[optind], ca_last_error ());

  if (!strcmp (format, "index"))
    {
      if (!schema_path)
        errx (EX_USAGE, "--output-format=index can only be used with --schema=PATH");

      if (!(schema = ca_schema_load (schema_path)))
        errx (EXIT_FAILURE, "Failed to load schema: %s", ca_last_error ());

      if (-1 == (summary_table_count = ca_schema_summary_tables (schema, &summary_tables,
                                                                 &summary_table_offsets)))
        errx (EXIT_FAILURE, "Failed to open summary tables: %s", ca_last_error ());

      dump_index ();
    }
  else if (!strcmp (format, "summaries"))
    dump_summaries ();
  else if (!strcmp (format, "time-series"))
    dump_time_series ();
  else
    errx (EX_USAGE, "Invalid format '%s'", format);

  ca_table_close (table_handle);

  return EXIT_SUCCESS;
}
