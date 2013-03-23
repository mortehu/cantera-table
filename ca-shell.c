/*
    Interactive shell for Cantera Table databases
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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

static int print_version;
static int print_help;

static struct option long_options[] =
{
    { "command",  required_argument,  NULL,           'c' },
    { "version",        no_argument,  &print_version, 1 },
    { "help",           no_argument,  &print_help,    1 },
    { 0, 0, 0, 0 }
};

int
main (int argc, char **argv)
{
  struct ca_query_parse_context context;
  const char *schema_path;
  const char *command = NULL;
  int i;

  memset (&context, 0, sizeof (context));
  strcpy (CA_time_format, "%Y-%m-%dT%H:%M:%S");

  while ((i = getopt_long (argc, argv, "c:", long_options, 0)) != -1)
    {
      switch (i)
        {
        case 0:

          break;

        case 'c':

          command = optarg;

          break;

        case '?':

          errx (EX_USAGE, "Try '%s --help' for more information.", argv[0]);
        }
    }

  if (print_help)
    {
      printf ("Usage: %s [OPTION]...\n"
             "\n"
             "  -c, --command=STRING       execute commands in STRING and exit\n"
             "      --help     display this help and exit\n"
             "      --version  display version information and exit\n"
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
    errx (EX_USAGE, "Usage: %s [OPTION]... SCHEMA", argv[0]);

  schema_path = argv[optind++];

  if (!(context.schema = ca_schema_load (schema_path)))
    errx (EXIT_FAILURE, "Failed to load schema: %s", ca_last_error ());

  if (command)
    {
      FILE *file;

      ca_clear_error ();

      if (!(file = fmemopen ((void *) command, strlen (command), "r")))
        fprintf (stderr, "fmemopen failed: %s\n", strerror (errno));

      if (-1 == CA_parse_script (&context, file))
        fprintf (stderr, "Error: %s\n", ca_last_error ());

      fclose (file);
    }
  else if (isatty (STDIN_FILENO))
    {
      char *home, *history_path = NULL;
      char *prompt = NULL;

      if (NULL != (home = getenv ("HOME")))
        {
          if (-1 != asprintf (&history_path, "%s/.ca-shell_history", home))
            read_history (history_path);
        }

      for (;;)
        {
          FILE *file;
          char *line = NULL;

          free (prompt);

          if (-1 == asprintf (&prompt,
                              "[%1$c\033[32;1m%2$cca-table%1$c\033[00m%2$c:%1$c\033[1m%2$c%3$s%1$c\033[00m%2$c]$ ",
                              RL_PROMPT_START_IGNORE, RL_PROMPT_END_IGNORE,
                              get_current_dir_name ()))
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

#if HAVE_READLINE_HISTORY
          add_history (line);
#endif

          ca_clear_error ();
          context.error = 0;

          if (!(file = fmemopen ((void *) line, strlen (line), "r")))
            fprintf (stderr, "fmemopen failed: %s\n", strerror (errno));

          if (-1 == CA_parse_script (&context, file))
            fprintf (stderr, "Error: %s\n", ca_last_error ());

          fclose (file);

          free (line);
        }

      free (prompt);

      if (history_path)
        {
          write_history (history_path);

          free (history_path);
        }

      printf ("\n");
    }
  else
    {
      char buf[BUFSIZ];

      setvbuf (stdout, buf, _IOFBF, sizeof buf);

      if (-1 == CA_parse_script (&context, stdin))
        fprintf (stderr, "Error: %s\n", ca_last_error ());

      fflush (stdout);
    }

  ca_schema_close (context.schema);

  return EXIT_SUCCESS;
}
