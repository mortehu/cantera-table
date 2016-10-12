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
#include <re2/re2.h>

#include "src/ca-table.h"
#include "src/schema.h"
#include "src/util.h"

namespace ca_table = cantera::table;

enum Option {
  kOptionKeyFilter = 'k',
  kOptionPrefix = 'P',
  kOptionTSFormat = 'F',
};

enum TSFormat {
  kTSFormatNormal,
  kTSFormatCount,
};

namespace {

int print_version;
int print_help;

int count_only;
int keys_only;

char delimiter = '\t';
const char* date_format = "%Y-%m-%d %H:%M:%S";
TSFormat ts_format;

std::unique_ptr<re2::RE2> key_filter;
std::string first_key, last_key;

time_t interval = 1;

struct option kLongOptions[] = {
    {"count", no_argument, &count_only, 1},
    {"delimiter", required_argument, NULL, 'D'},
    {"date-format", required_argument, NULL, 'A'},
    {"ts-format", required_argument, nullptr, kOptionTSFormat},
    {"format", required_argument, NULL, 'O'},
    {"key-filter", required_argument, NULL, kOptionKeyFilter},
    {"keys-only", no_argument, &keys_only, 1},
    {"schema", required_argument, NULL, 'S'},
    {"raw", no_argument, NULL, 'R'},
    {"prefix", required_argument, NULL, kOptionPrefix},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {0, 0, 0, 0}};

std::unique_ptr<ca_table::Schema> schema;
std::unique_ptr<ca_table::Table> table_handle;

void DumpIndexRaw() {
  cantera::string_view key, offset_score;

  while (table_handle->ReadRow(key, offset_score)) {
    if (key_filter &&
        !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                        *key_filter)) {
      if (key >= last_key) break;
      continue;
    }

    printf("%.*s\n", static_cast<int>(key.size()), key.data());

    std::vector<ca_table::ca_offset_score> offsets;

    ca_offset_score_parse(offset_score, &offsets);
    for (size_t i = 0; i < offsets.size(); ++i) {
      printf("\t%lu %g\n", offsets[i].offset, offsets[i].score);
    }
  }
}

void DumpIndex() {
  cantera::string_view key, offset_score, summary;

  while (table_handle->ReadRow(key, offset_score)) {
    if (key_filter &&
        !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                        *key_filter)) {
      if (key >= last_key) break;
      continue;
    }

    std::vector<ca_table::ca_offset_score> offsets;

    ca_offset_score_parse(offset_score, &offsets);

    for (size_t i = 0; i < offsets.size(); ++i) {
      auto j = schema->summary_tables.size();

      while (--j &&
             std::get<uint64_t>(schema->summary_tables[j]) > offsets[i].offset)
        ;

      schema->summary_tables[j].second->Seek(
          offsets[i].offset - std::get<uint64_t>(schema->summary_tables[j]),
          SEEK_SET);

      KJ_REQUIRE(schema->summary_tables[j].second->ReadRow(key, summary));

      printf("%.*s\t%.*s\t%.9g\n", static_cast<int>(key.size()),
             key.data(), static_cast<int>(summary.size()),
             summary.data(), offsets[i].score);
    }
  }
}

void DumpSummaries() {
  cantera::string_view key, summary;

  while (table_handle->ReadRow(key, summary)) {
    if (key_filter &&
        !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                        *key_filter)) {
      if (ca_table::internal::CompareUTF8(last_key, key)) break;
      continue;
    }

    printf("%.*s\t%.*s\n", static_cast<int>(key.size()),
           key.data(),
           static_cast<int>(summary.size()), summary.data());
  }
}

void DumpTimeSeries() {
  cantera::string_view key, offset_score;

  while (table_handle->ReadRow(key, offset_score)) {
    if (key_filter &&
        !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                        *key_filter)) {
      if (ca_table::internal::CompareUTF8(last_key, key)) break;
      continue;
    }

    std::vector<ca_table::ca_offset_score> offsets;

    ca_table::ca_offset_score_parse(offset_score, &offsets);

    if (ts_format == kTSFormatCount) {
      printf("%.*s\t%zu\n", static_cast<int>(key.size()), key.data(),
             offsets.size());
    } else {
      if (!strcmp(date_format, "%s")) {
        for (size_t i = 0; i < offsets.size(); ++i) {
          KJ_REQUIRE(!offsets[i].HasPercentiles());

          printf("%.*s\t%llu\t%.9g\n", static_cast<int>(key.size()), key.data(),
                 (long long unsigned)offsets[i].offset, offsets[i].score);
        }
      } else {
        for (size_t i = 0; i < offsets.size(); ++i) {
          if (offsets[i].HasPercentiles()) {
            const time_t time = offsets[i].offset;
            struct tm tm;
            memset(&tm, 0, sizeof(tm));

            gmtime_r(&time, &tm);

            char time_buffer[64];
            strftime(time_buffer, sizeof(time_buffer), date_format, &tm);

            printf("%.*s\t%s\t%.9g %.9g %.9g %.9g %.9g\n",
                   static_cast<int>(key.size()), key.data(), time_buffer,
                   offsets[i].score, offsets[i].score_pct5,
                   offsets[i].score_pct25, offsets[i].score_pct75,
                   offsets[i].score_pct95);

          } else {
            const time_t time = offsets[i].offset;
            struct tm tm;
            memset(&tm, 0, sizeof(tm));

            gmtime_r(&time, &tm);

            char time_buffer[64];
            strftime(time_buffer, sizeof(time_buffer), date_format, &tm);

            printf("%.*s\t%s\t%.9g\n", static_cast<int>(key.size()), key.data(),
                   time_buffer, offsets[i].score);
          }
        }
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) try {
  const char* format = "time-series";

  const char* schema_path = NULL;
  bool raw = false;

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

      case kOptionKeyFilter:
        key_filter = std::make_unique<re2::RE2>(optarg);
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

      case 'R':
        raw = true;
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
        "      --key-filter=REGEX     only read keys matching REGEX\n"
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

  table_handle = ca_table::TableFactory::Open(nullptr, argv[optind]);

  if (key_filter) {
    KJ_REQUIRE(key_filter->PossibleMatchRange(&first_key, &last_key, 64));
  }

  table_handle->SeekToKey(first_key);

  if (count_only) {
    cantera::string_view key, value;
    if (!keys_only && !strcmp(format, "time-series")) {
      while (table_handle->ReadRow(key, value)) {
        if (key_filter &&
            !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                            *key_filter)) {
          if (ca_table::internal::CompareUTF8(last_key, key)) break;
          continue;
        }
        const auto begin = reinterpret_cast<const uint8_t*>(value.data());
        const auto end = begin + value.size();
        printf("%.*s\t%zu\n", static_cast<int>(key.size()), key.data(),
               ca_table::ca_offset_score_count(begin, end));
      }
    } else {
      size_t count = 0;
      while (table_handle->ReadRow(key, value)) {
        if (key_filter &&
            !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                            *key_filter)) {
          if (ca_table::internal::CompareUTF8(last_key, key)) break;
          continue;
        }
        ++count;
      }
      printf("%zu\n", count);
    }
  } else if (keys_only) {
    cantera::string_view key, value;
    while (table_handle->ReadRow(key, value)) {
      if (key_filter &&
          !RE2::FullMatch(re2::StringPiece(key.data(), key.size()),
                          *key_filter)) {
        if (ca_table::internal::CompareUTF8(last_key, key)) break;
        continue;
      }
      printf("%.*s\n", static_cast<int>(key.size()), key.data());
    }
  } else if (!strcmp(format, "index")) {
    if (!schema_path && !raw) {
      errx(
          EX_USAGE,
          "--output-format=index can only be used with --schema=PATH or --raw");
    }

    if (raw) {
      DumpIndexRaw();
    } else {
      schema = std::make_unique<ca_table::Schema>(schema_path);
      schema->Load();

      DumpIndex();
    }
  } else if (!strcmp(format, "summaries")) {
    DumpSummaries();
  } else if (!strcmp(format, "time-series")) {
    DumpTimeSeries();
  } else {
    errx(EX_USAGE, "Invalid format '%s'", format);
  }
} catch (kj::Exception e) {
  KJ_LOG(FATAL, e);
  return EXIT_FAILURE;
}
