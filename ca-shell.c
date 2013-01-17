#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <err.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sysexits.h>
#include <unistd.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "error.h"
#include "memory.h"
#include "smalltable.h"
#include "schema.h"
#include "query.h"

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

  if (optind != argc)
    errx (EX_USAGE, "Usage: %s [OPTION]...", argv[0]);

  if (-1 == ca_schema_load ("/data/tables/schema.ca"))
    errx (EXIT_FAILURE, "Failed to load schema: %s", ca_last_error ());

  if (isatty (STDIN_FILENO))
    {
      const char *prompt = "\033[32;1mca\033[00m$ ";
      FILE *file;
      char *line;

      while (NULL != (line = readline (prompt)))
        {
          ca_clear_error ();

          add_history (line);

          if (!(file = fmemopen ((void *) line, strlen (line), "r")))
            fprintf (stderr, "fmemopen failed: %s\n", strerror (errno));

          if (-1 == ca_query_parse (file))
            fprintf (stderr, "Error: %s\n", ca_last_error ());

          fclose (file);

          free (line);
        }

      printf ("\n");
    }
  else
    {
      if (-1 == ca_query_parse (stdin))
        fprintf (stderr, "Error: %s\n", ca_last_error ());
    }

  ca_schema_close ();

  return EXIT_SUCCESS;
}
