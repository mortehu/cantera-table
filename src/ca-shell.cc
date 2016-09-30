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
#include "config.h"
#endif

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>

#include <err.h>
#include <getopt.h>
#include <kj/debug.h>
#include <sys/mman.h>
#include <sysexits.h>
#include <unistd.h>

#include <json/value.h>
#include <json/writer.h>
#include <readline/history.h>
#include <readline/readline.h>

#include "src/ca-table.h"
#include "src/query.h"

namespace ca_table = cantera::table;

namespace {

enum Option : int {
  kOptionCommand = 'c',
  kOptionUnknown = '?',
};

int print_version;
int print_help;

const char kDefaultSchemaPath[] = "/data/index/current/schema.txt";

struct option kLongOptions[] = {
    {"command", required_argument, NULL, kOptionCommand},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {nullptr, 0, nullptr, 0}};

}  // namespace

static void stdout_error(const char *errmsg) {
  Json::Value error;
  error["error"] = errmsg;
  std::cout << Json::FastWriter().write(error);
}

static void parse_string(ca_table::QueryParseContext &context, const char *command, bool use_std_err) {
  FILE* file;

#if HAVE_FMEMOPEN
  if (!(file = fmemopen((void*)command, strlen(command), "r")))
    throw KJ_EXCEPTION(FAILED, "fmemopen failed: %s\n", strerror(errno));
#else
  KJ_UNIMPLEMENTED("need a fallback for missing fmemopen()");
#endif

  try {
    CA_parse_script(&context, file);
  } catch (kj::Exception e) {
    if (use_std_err)
      stdout_error(e.getDescription().cStr());
    else
      KJ_LOG(ERROR, e);
  }

  fclose(file);
}

int main(int argc, char** argv) try {
  ca_table::QueryParseContext context;
  const char* schema_path = nullptr;
  const char* command = nullptr;
  int i;

  strcpy(ca_table::CA_time_format, "%Y-%m-%dT%H:%M:%S");

  while (-1 != (i = getopt_long(argc, argv, "c:", kLongOptions, 0))) {
    if (!i) continue;

    switch (static_cast<Option>(i)) {
      case kOptionCommand:
        command = optarg;
        break;

      case kOptionUnknown:
        errx(EX_USAGE, "Try '%s --help' for more information.", argv[0]);
    }
  }

  if (print_help) {
    printf(
        "Usage: %s [OPTION]... [SCHEMA]\n"
        "\n"
        "  -c, --command=STRING       execute commands in STRING and exit\n"
        "      --help     display this help and exit\n"
        "      --version  display version information and exit\n"
        "\n"
        "If SCHEMA is not specified, %s will be used instead.\n"
        "\n"
        "Report bugs to <morten.hustveit@gmail.com>\n",
        argv[0], kDefaultSchemaPath);

    return EXIT_SUCCESS;
  }

  if (print_version) {
    puts(PACKAGE_STRING);
    return EXIT_SUCCESS;
  }

  if (optind == argc) {
    schema_path = kDefaultSchemaPath;
  } else if (optind + 1 == argc) {
    schema_path = argv[optind++];
  } else {
    errx(EX_USAGE, "Usage: %s [OPTION]... [SCHEMA]", argv[0]);
  }

  context.schema = std::make_unique<ca_table::Schema>(schema_path);

  if (command) {
    KJ_CONTEXT(command);

    parse_string(context, command, true);

  } else if (isatty(STDIN_FILENO)) {
    // Standard input is a TTY; enter interactive mode.

    char *home, *history_path = NULL;
    char* prompt = NULL;

    if (NULL != (home = getenv("HOME"))) {
      if (-1 != asprintf(&history_path, "%s/.ca-shell_history", home))
        read_history(history_path);
    }

    for (;;) {
      char* line = NULL;

      free(prompt);

#if HAVE_GET_CURRENT_DIR_NAME
      std::unique_ptr<char[], decltype(free) *> dir(get_current_dir_name(), free);
#else
      std::unique_ptr<char[], decltype(free) *> dir(getcwd(NULL, 0), free);
#endif
      if (-1 == asprintf(&prompt,
                         "[%1$c\033[32;1m%2$cca-table%1$c\033[00m%2$c:%1$c\033["
                         "1m%2$c%3$s%1$c\033[00m%2$c]$ ",
                         RL_PROMPT_START_IGNORE, RL_PROMPT_END_IGNORE, dir.get()))
        err(EXIT_FAILURE, "asprintf failed");

      if (!(line = readline(prompt))) break;

      if (!*line) continue;

      add_history(line);

      parse_string(context, line, false);

      free(line);
    }

    free(prompt);

    if (history_path) {
      write_history(history_path);

      free(history_path);
    }

    printf("\n");
  } else {
    char buf[BUFSIZ];

    setvbuf(stdout, buf, _IOFBF, sizeof buf);

    try {
      CA_parse_script(&context, stdin);
    } catch (kj::Exception e) {
      stdout_error(e.getDescription().cStr());
    }
  }
} catch (kj::Exception e) {
  KJ_LOG(FATAL, e);
  return EXIT_FAILURE;
}
