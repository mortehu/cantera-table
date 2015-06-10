#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <vector>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <sysexits.h>
#include <unistd.h>

#include <kj/debug.h>

#include "base/stringref.h"
#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"

enum Option {
  kOptionFirstKey = 'f',
  kOptionKey = 'K',
  kOptionLastKey = 'l',
  kOptionPrefix = 'P',
  kOptionTSFormat = 'F',
};

enum TSFormat {
  kTSFormatNormal,
  kTSFormatCount,
};

static int print_version;
static int print_help;

static int count_only;
static int keys_only;

static char delimiter = '\t';
static const char* date_format = "%Y-%m-%d %H:%M:%S";
static TSFormat ts_format;

static std::string first_key, last_key;

static time_t interval = 1;

static struct option kLongOptions[] = {
    {"count", no_argument, &count_only, 1},
    {"delimiter", required_argument, NULL, 'D'},
    {"date-format", required_argument, NULL, 'A'},
    {"ts-format", required_argument, nullptr, kOptionTSFormat},
    {"format", required_argument, NULL, 'O'},
    {"first-key", required_argument, NULL, kOptionFirstKey},
    {"last-key", required_argument, NULL, kOptionLastKey},
    {"key", required_argument, NULL, kOptionKey},
    {"keys-only", no_argument, &keys_only, 1},
    {"schema", required_argument, NULL, 'S'},
    {"prefix", required_argument, NULL, kOptionPrefix},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {0, 0, 0, 0}};

static std::unique_ptr<ca_table::Table> table_handle;

static std::vector<ca_table::Table*> summary_tables;
static uint64_t* summary_table_offsets;

static void dump_index(void) {
  struct iovec key_iov, offset_score, summary;
  int ret;

  if (!first_key.empty()) table_handle->SeekToKey(first_key);

  while (1 == (ret = table_handle->ReadRow(&key_iov, &offset_score))) {
    ev::StringRef key(key_iov);

    if (!last_key.empty() && key > last_key) break;

    if (!first_key.empty()) {
      if (key < first_key) continue;
      first_key.clear();
    }

    std::vector<ca_offset_score> offsets;

    ca_offset_score_parse(offset_score, &offsets);

    for (size_t i = 0; i < offsets.size(); ++i) {
      auto j = summary_tables.size();

      while (--j && summary_table_offsets[j] > offsets[i].offset)
        ;

      summary_tables[j]->Seek(offsets[i].offset - summary_table_offsets[j],
                              SEEK_SET);

      if (1 != (ret = summary_tables[j]->ReadRow(&key_iov, &summary))) {
        if (ret >= 0)
          ca_set_error("ca_table_read_row unexpectedly returned %d", (int)ret);

        break;
      }

      printf("%.*s\t%.*s\t%.9g\n", static_cast<int>(key_iov.iov_len),
             reinterpret_cast<const char*>(key_iov.iov_base),
             static_cast<int>(summary.iov_len),
             reinterpret_cast<const char*>(summary.iov_base), offsets[i].score);
    }
  }

  if (ret == -1) errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
}

static void dump_summaries(void) {
  struct iovec key_iov, summary;
  int ret;

  if (!first_key.empty()) table_handle->SeekToKey(first_key);

  while (1 == (ret = table_handle->ReadRow(&key_iov, &summary))) {
    ev::StringRef key(key_iov);

    if (!last_key.empty() && key > last_key) break;

    if (!first_key.empty()) {
      if (key < first_key) continue;
      first_key.clear();
    }

    printf("%.*s\t%.*s\n", static_cast<int>(key.size()),
           reinterpret_cast<const char*>(key.data()),
           static_cast<int>(summary.iov_len),
           reinterpret_cast<const char*>(summary.iov_base));
  }

  if (ret == -1) errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
}

static void dump_time_series(void) {
  struct iovec key_iov, offset_score;
  int ret;

  if (!first_key.empty()) table_handle->SeekToKey(first_key);

  while (1 == (ret = table_handle->ReadRow(&key_iov, &offset_score))) {
    ev::StringRef key(key_iov);

    if (!last_key.empty() && key > last_key) break;

    if (!first_key.empty()) {
      if (key < first_key) continue;
      first_key.clear();
    }

    std::vector<ca_offset_score> offsets;
    size_t i;

    ca_offset_score_parse(offset_score, &offsets);

    if (ts_format == kTSFormatCount) {
      printf("%.*s\t%zu\n", static_cast<int>(key.size()), key.data(),
             offsets.size());
    } else {
      if (!strcmp(date_format, "%s")) {
        for (i = 0; i < offsets.size(); ++i) {
          KJ_REQUIRE(!offsets[i].HasPercentiles());

          printf("%.*s\t%llu\t%.9g\n", static_cast<int>(key.size()), key.data(),
                 (long long unsigned)offsets[i].offset, offsets[i].score);
        }
      } else {
        char time_buffer[64];
        time_t time;
        struct tm tm;

        for (i = 0; i < offsets.size(); ++i) {
          KJ_REQUIRE(!offsets[i].HasPercentiles());

          time = offsets[i].offset;
          memset(&tm, 0, sizeof(tm));

          gmtime_r(&time, &tm);

          strftime(time_buffer, sizeof(time_buffer), date_format, &tm);

          printf("%.*s\t%s\t%.9g\n", static_cast<int>(key.size()), key.data(),
                 time_buffer, offsets[i].score);
        }
      }
    }
  }

  if (ret == -1) errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
}

int main(int argc, char** argv) try {
  const char* format = "time-series";

  const char* schema_path = NULL;
  struct ca_schema* schema = NULL;

  int i;

  setenv("TZ", "", 1);

  while ((i = getopt_long(argc, argv, "", kLongOptions, 0)) != -1) {
    switch (i) {
      case 0:
        break;

      case 'D':
        if (!*optarg) errx(EX_USAGE, "Provided delimiter is empty");
        if (optarg[1])
          errx(EX_USAGE, "Provided delimiter is more than one ASCII character");
        delimiter = *optarg;
        break;

      case 'A':
        date_format = optarg;
        break;

      case kOptionTSFormat:
        if (!strcmp(optarg, "normal"))
          ts_format = kTSFormatNormal;
        else if (!strcmp(optarg, "count"))
          ts_format = kTSFormatCount;
        else
          errx(EX_USAGE, "Unknown time series format '%s'", optarg);
        break;

      case kOptionFirstKey:
        first_key = optarg;
        break;

      case kOptionKey:
        first_key = optarg;
        last_key = first_key;
        break;

      case kOptionLastKey:
        last_key = optarg;
        break;

      case kOptionPrefix:
        first_key = optarg;
        if (!first_key.empty()) {
          last_key = first_key;
          while (!last_key.empty()) {
            unsigned char ch = last_key.back();
            last_key.pop_back();
            if (ch < 255) {
              last_key.push_back(ch + 1);
              break;
            }
          }
        }
        break;

      case 'O':
        format = optarg;
        break;

      case 'S':
        schema_path = optarg;
        break;

      case 'I': {
        char* endptr;
        interval = strtol(optarg, &endptr, 0);

        if (*endptr) errx(EX_USAGE, "Failed to parse interval '%s'", optarg);

        if (interval <= 0) errx(EX_USAGE, "Sample interval too small");
      } break;

      case '?':
        errx(EX_USAGE, "Try '%s --help' for more information.", argv[0]);
    }
  }

  if (print_help) {
    printf(
        "Usage: %s [OPTION]... TABLE\n"
        "\n"
        "      --count                print record count instead of normal "
        "output\n"
        "      --delimiter=DELIMITER  set input delimiter [%c]\n"
        "      --date-format=FORMAT   use provided date format [%s]\n"
        "      --date=DATE            use DATE as timestamp\n"
        "      --key=KEY              print the values for just one key\n"
        "      --keys-only            do not print values\n"
        "      --interval=INTERVAL    sample interval if both --date and --key "
        "are\n"
        "                             given\n"
        "      --help     display this help and exit\n"
        "      --version  display version information\n"
        "\n"
        "Report bugs to <morten.hustveit@gmail.com>\n",
        argv[0], delimiter, date_format);

    return EXIT_SUCCESS;
  }

  if (print_version) {
    fprintf(stdout, "%s\n", PACKAGE_STRING);

    return EXIT_SUCCESS;
  }

  if (optind + 1 != argc)
    errx(EX_USAGE, "Usage: %s [OPTION]... TABLE", argv[0]);

  table_handle = ca_table_open(nullptr, argv[optind], O_RDONLY);

  if (!first_key.empty()) table_handle->SeekToKey(first_key);

  if (count_only) {
    size_t count = 0;
    int ret;
    struct iovec key_iov, value;
    if (!keys_only && !strcmp(format, "time-series")) {
      while (1 == (ret = table_handle->ReadRow(&key_iov, &value))) {
        ev::StringRef key(key_iov);

        if (!last_key.empty() && key > last_key) break;

        if (!first_key.empty()) {
          if (key < first_key) continue;
          first_key.clear();
        }

        auto begin = reinterpret_cast<uint8_t*>(value.iov_base);
        auto end = begin + value.iov_len;
        count += ca_offset_score_count(begin, end);
      }
      ++count;
    } else {
      while (1 == (ret = table_handle->ReadRow(&key_iov, &value))) {
        ev::StringRef key(key_iov);

        if (!last_key.empty() && key > last_key) break;

        if (!first_key.empty()) {
          if (key < first_key) continue;
          first_key.clear();
        }

        ++count;
      }
    }
    if (ret == -1)
      errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
    printf("%zu\n", count);
  } else if (keys_only) {
    int ret;
    struct iovec key_iov, value;
    while (1 == (ret = table_handle->ReadRow(&key_iov, &value))) {
      ev::StringRef key(key_iov);
      printf("%.*s\n", static_cast<int>(key.size()), key.data());

      if (!last_key.empty() && key > last_key) break;

      if (!first_key.empty()) {
        if (key < first_key) continue;
        first_key.clear();
      }
    }
    if (ret == -1)
      errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
  } else if (!strcmp(format, "index")) {
    if (!schema_path)
      errx(EX_USAGE,
           "--output-format=index can only be used with --schema=PATH");

    if (!(schema = ca_schema_load(schema_path)))
      errx(EXIT_FAILURE, "Failed to load schema: %s", ca_last_error());

    summary_tables = ca_schema_summary_tables(schema, &summary_table_offsets);

    dump_index();
  } else if (!strcmp(format, "summaries")) {
    dump_summaries();
  } else if (!strcmp(format, "time-series")) {
    dump_time_series();
  } else {
    errx(EX_USAGE, "Invalid format '%s'", format);
  }
} catch (kj::Exception e) {
  fprintf(stderr, "Error: %s:%d: %s\n", e.getFile(), e.getLine(),
          e.getDescription().cStr());
  return EXIT_FAILURE;
}
