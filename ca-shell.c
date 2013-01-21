#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

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

#ifdef HAVE_LIBREADLINE
#  if defined(HAVE_READLINE_READLINE_H)
#    include <readline/readline.h>
#  elif defined(HAVE_READLINE_H)
#    include <readline.h>
#  else
extern char *readline ();
#  endif
char *cmdline = NULL;
#endif

#ifdef HAVE_READLINE_HISTORY
#  if defined(HAVE_READLINE_HISTORY_H)
#    include <readline/history.h>
#  elif defined(HAVE_HISTORY_H)
#    include <history.h>
#  else
extern void add_history ();
extern int write_history ();
extern int read_history ();
#  endif
#endif

#include "ca-table.h"
#include "memory.h"
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

      for (;;)
        {
          const char *prompt = "\033[32;1mca\033[00m$ ";
          FILE *file;
          char *line = NULL;

#if HAVE_LIBREADLINE
          if (!(line = readline (prompt)))
            break;
#else
          size_t line_alloc = 0, line_length = 0;
          int ch;

          printf ("%s", prompt);
          fflush (stdout);

          while (EOF != (ch = getchar ()))
            {
              /* Perform this check before the EOL check to make room for
               * terminating NUL */
              if (line_length == line_alloc)
                ARRAY_GROW (&line, &line_alloc);

              if (ch == '\n')
                break;

              line[line_length++] = ch;
            }

          if (!line_length)
            {
              if (ch == EOF)
                break;

              continue;
            }

          line[line_length] = 0;
#endif

#if HAVE_READLINE_HISTORY
          add_history (line);
#endif

          ca_clear_error ();

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
