#include <stdio.h>
#include <stdlib.h>

#include <err.h>
#include <getopt.h>
#include <sysexits.h>

#include "journal.h"

#define DATADIR "/tmp/ts"

static int print_version;
static int print_help;

static struct option long_options[] =
{
    { "version",        no_argument,       &print_version, 1 },
    { "help",           no_argument,       &print_help,    1 },
    { 0, 0, 0, 0 }
};

static int db_handle, db_index_handle;

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
      printf ("Usage: %s [OPTION]...\n"
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

  journal_init (DATADIR "/journal");

  db_handle = journal_file_open (DATADIR "/input.data");
  db_index_handle = journal_file_open (DATADIR "/input.index");

  journal_commit ();

  return EXIT_SUCCESS;
}
