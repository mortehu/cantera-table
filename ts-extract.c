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
          printf ("%llu\t%.7g\n",
                  (unsigned long long) (start_time + i * interval),
                  sample_values[i]);
        }

      last_time = start_time;
    }

  table_close (table);

  return EXIT_SUCCESS;
}
