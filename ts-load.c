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

#include "array.h"
#include "journal.h"

#define DATADIR "/tmp/ts"

static int print_version;
static int print_help;

static char delimiter = ',';
static const char *date_format = "%Y-%m-%d %H:%M:%S";

static const char *input_key;
static int has_input_key;

static time_t input_time;
static int has_input_time;

static time_t interval = 1;

static struct option long_options[] =
{
    { "delimiter",      required_argument, NULL,           'D' },
    { "date-format",    required_argument, NULL,           'A' },
    { "date-from-path", required_argument, NULL,           'F' },
    { "date",           required_argument, NULL,           'T' },
    { "key",            required_argument, NULL,           'K' },
    { "interval",       required_argument, NULL,           'I' },
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

static int db_handle, db_index_handle;

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

static char *value;
static size_t value_alloc;
static size_t value_length;

static void
write_integer (int file_handle, uint64_t value)
{
  uint8_t buffer[10];
  unsigned int ptr = 9;

  buffer[ptr] = value & 0x7f;
  value >>= 7;

  while (value)
    {
      buffer[--ptr] = 0x80 | value;

      value >>= 7;
    }

  journal_file_append (file_handle, &buffer[ptr], 10 - ptr);
}

static void
write_time_value (const char *key, uint64_t time, float value)
{
  uint64_t db_offset;

  db_offset = journal_file_size (db_handle);

  journal_file_append (db_handle, key, strlen (key) + 1);
  write_integer (db_handle, time);
  journal_file_append (db_handle, &value, sizeof(float));

  journal_file_append (db_index_handle, &db_offset, sizeof (uint64_t));
}

static void
parse_data (const char *begin, const char *end)
{
  for (; begin != end; ++begin)
    {
      switch (state)
        {
        case parse_key:

          if (key_length == key_alloc)
            ARRAY_GROW (&key, &key_alloc);

          if (*begin == delimiter)
            {
              key[key_length] = 0;

              input_key = key;

              key_length = 0;

              state = has_input_time ? parse_value : parse_date;

              break;
            }

          key[key_length++] = *begin;

          break;

        case parse_date:

          if (date_length == date_alloc)
            ARRAY_GROW (&date, &date_alloc);

          if (*begin == delimiter)
            {
              struct tm tm;

              date[date_length] = 0;

              if (!strptime (date, date_format, &tm))
                errx (EX_DATAERR, "Unable to parse date '%s' according to format '%s'",
                      date, date_format);

              input_time = mktime (&tm);

              date_length = 0;

              state = parse_value;

              break;
            }

          date[date_length++] = *begin;

          break;

        case parse_value:

          if (*begin == '\r')
            continue;

          if (value_length == value_alloc)
            ARRAY_GROW (&value, &value_alloc);

          if (*begin == '\n')
            {
              float fvalue;
              char *endptr;

              value[value_length] = 0;

              fvalue = strtod (value, &endptr);

              if (*endptr)
                errx (EX_DATAERR, "Unable to parse value '%s'.  Unexpected suffix: '%s'",
                      value, endptr);

              value_length = 0;

              write_time_value (input_key, input_time, fvalue);

              if (!has_input_key)
                state = parse_key;
              else if (!has_input_time)
                state = parse_date;
              else
                {
                  state = parse_value;
                  input_time += interval;
                }

              break;
            }

          value[value_length++] = *begin;

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

        case 'F':

            {
              struct stat st;

              if (-1 == stat (optarg, &st))
                errx (EX_NOINPUT, "Could not stat '%s''", optarg);

              input_time = st.st_mtime;
              has_input_time = 1;
            }

          break;

        case 'T':

            {
              struct tm tm;

              if (!strptime (optarg, date_format, &tm))
                errx (EX_USAGE, "Unable to parse date '%s' according to format '%s'",
                      optarg, date_format);

              input_time = mktime (&tm);
              has_input_time = 1;
            }

          break;

        case 'K':

          input_key = optarg;
          has_input_key = 1;

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
      printf ("Usage: %s [OPTION]...\n"
             "\n"
             "      --delimiter=DELIMITER  set input delimiter [%c]\n"
             "      --date-format=FORMAT   use provided date format [%s]\n"
             "      --date=DATE            use DATE as timestamp\n"
             "      --date-from-path=PATH  get timestamp from PATH's mtime\n"
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

  journal_init (DATADIR "/journal");

  db_handle = journal_file_open (DATADIR "/input.data");
  db_index_handle = journal_file_open (DATADIR "/input.index");

  if (!has_input_key)
    state = parse_key;
  else if (!has_input_time)
    state = parse_date;
  else
    state = parse_value;

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
    }

  journal_commit ();

  return EXIT_SUCCESS;
}
