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
#include "query.h"

static const char *schema_path = "/data/tables";
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
  struct ca_schema *schema;
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

  if (!(schema = ca_schema_load (schema_path)))
    errx (EXIT_FAILURE, "Failed to load schema: %s", ca_last_error ());

  if (isatty (STDIN_FILENO))
    {
      char *home, *history_path = NULL;

      if (NULL != (home = getenv ("HOME")))
        {
          if (-1 != asprintf (&history_path, "%s/.ca-shell_history", home))
            read_history (history_path);
        }

      for (;;)
        {
          char *prompt;
          FILE *file;
          char *line = NULL;

          if (-1 == asprintf (&prompt, "%s$ ", schema_path))
            err (EXIT_FAILURE, "asprintf failed");

#if HAVE_LIBREADLINE
          if (!(line = readline (prompt)))
            break;

          if (!*line)
            continue;
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
                CA_ARRAY_GROW (&line, &line_alloc);

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

          free (prompt);

#if HAVE_READLINE_HISTORY
          add_history (line);
#endif

          ca_clear_error ();

          if (!(file = fmemopen ((void *) line, strlen (line), "r")))
            fprintf (stderr, "fmemopen failed: %s\n", strerror (errno));

          if (-1 == ca_schema_parse_script (schema, file))
            fprintf (stderr, "Error: %s\n", ca_last_error ());

          fclose (file);

          free (line);
        }

      if (history_path)
        write_history (history_path);

      printf ("\n");
    }
  else
    {
      if (-1 == ca_schema_parse_script (schema, stdin))
        fprintf (stderr, "Error: %s\n", ca_last_error ());
    }

  ca_schema_close (schema);

  return EXIT_SUCCESS;
}
