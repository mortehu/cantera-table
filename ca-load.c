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
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

static struct ca_table *table_handle;

enum parse_state
{
  parse_key,
  parse_date,
  parse_value
};

static enum parse_state state;

static char *key;
static size_t key_alloc;
static size_t key_length;

static char *date;
static size_t date_alloc;
static size_t date_length;

static char *value_string;
static size_t value_string_alloc;
static size_t value_string_length;

static struct ca_offset_score *values;
static size_t value_alloc;
static size_t value_count;

static void
flush_values (void)
{
  if (-1 == ca_table_write_offset_score (table_handle, key,
                                         values, value_count))
    errx (EXIT_FAILURE, "%s", ca_last_error ());

  value_count = 0;
}

static void
parse_data (const char *begin, const char *end)
{
  time_t current_time = 0;

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
              key[key_length] = 0;

              key_length = 0;

              state = parse_date;

              break;
            }

          if (value_count && key[key_length] != *begin)
            flush_values ();

          key[key_length++] = *begin;

          break;

        case parse_date:

          if (date_length == date_alloc
              && -1 == CA_ARRAY_GROW (&date, &date_alloc))
            errx (EXIT_FAILURE, "%s", ca_last_error ());

          if (*begin == delimiter)
            {
              struct tm tm;
              char *end;

              memset (&tm, 0, sizeof (tm));

              date[date_length] = 0;

              if (!(end = strptime (date, date_format, &tm)))
                errx (EX_DATAERR, "Unable to parse date '%s' according to format '%s'",
                      date, date_format);

              if (*end)
                errx (EX_DATAERR, "Junk at end of date '%s': %s", date, end);

              current_time = timegm (&tm);

              date_length = 0;

              state = parse_value;

              break;
            }

          date[date_length++] = *begin;

          break;

        case parse_value:

          if (*begin == '\r')
            continue;

          if (value_string_length == value_string_alloc
              && -1 == CA_ARRAY_GROW (&value_string, &value_string_alloc))
            errx (EXIT_FAILURE, "%s", ca_last_error ());

          if (*begin == '\n')
            {
              float fvalue;
              char *endptr;

              value_string[value_string_length] = 0;

              fvalue = strtod (value_string, &endptr);

              if (*endptr)
                errx (EX_DATAERR, "Unable to parse value '%s'.  Unexpected suffix: '%s'",
                      value_string, endptr);

              value_string_length = 0;

              if (value_count == value_alloc
                  && -1 == CA_ARRAY_GROW (&values, &value_alloc))
                errx (EXIT_FAILURE, "%s", ca_last_error ());

              values[value_count].offset = current_time;
              values[value_count].score = fvalue;
              ++value_count;

              state = parse_key;

              break;
            }

          value_string[value_string_length++] = *begin;

          break;
        }
    }
}

int
main (int argc, char **argv)
{
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

  if (!(table_handle = ca_table_open ("write-once", argv[optind], O_CREAT | O_TRUNC | O_WRONLY, 0666)))
    errx (EXIT_FAILURE, "Failed to create '%s': %s", argv[optind], ca_last_error ());

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

  if (value_count)
    flush_values ();

  if (-1 == ca_table_sync (table_handle))
    errx (EXIT_FAILURE, "Failed to sync '%s': %s", argv[optind], ca_last_error ());

  ca_table_close (table_handle);

  return EXIT_SUCCESS;
}
