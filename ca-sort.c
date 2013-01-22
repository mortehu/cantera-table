#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sysexits.h>

#include "ca-table.h"
#include "memory.h"

static int do_debug;
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
  struct ca_table *input = NULL, *output = NULL;
  int i, result = EXIT_FAILURE;

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
             "      --debug                print debug output\n"
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

  if (optind + 1 != argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... TABLE", argv[0]);

  if (!(input = ca_table_open ("write-once", argv[optind], O_RDONLY, 0)))
    errx (EXIT_FAILURE, "%s", ca_last_error ());

  if (ca_table_is_sorted (input))
    {
      ca_table_close (input);

      if (do_debug)
        fprintf (stderr, "Table '%s' is already sorted\n", argv[optind]);

      return EXIT_SUCCESS;
    }

  if (!(output = ca_table_open ("write-once", argv[optind], O_CREAT | O_TRUNC | O_WRONLY, 0666))
      || -1 == ca_table_sort (output, input)
      || -1 == ca_table_sync (output))
    {
      fprintf (stderr, "%s\n", ca_last_error ());

      goto done;
    }

  result = EXIT_SUCCESS;

done:

  ca_table_close (output);
  ca_table_close (input);

  return result;
}
