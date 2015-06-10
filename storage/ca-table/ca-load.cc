#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <future>
#include <map>
#include <regex>
#include <string>
#include <vector>

#include <err.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sysexits.h>
#include <syslog.h>
#include <unistd.h>

#include <kj/debug.h>

#include "base/columnfile.h"
#include "base/file.h"
#include "base/hash.h"
#include "base/string.h"
#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"

namespace {

enum MergeMode {
  kMergePickOne,
  kMergeSum,
  kMergeUnion,
};

enum Format { kFormatAuto, kFormatCSV, kFormatCaTable, kFormatColumnFile };

enum DataType { kDataTypeIndex, kDataTypeSummaries, kDataTypeTimeSeries };

uint64_t shard_count = 1;
uint64_t shard_index = 0;

int print_version;
int print_help;
int do_syslog;
int no_unescape;

MergeMode merge_mode = kMergeUnion;

char delimiter = '\t';
const char* date_format = "%Y-%m-%d %H:%M:%S";

std::regex key_filter;
bool has_key_filter;

enum Option {
  kAddKeyPrefixOption = 1,
  kDateFormatOption,
  kDelimiterOption,
  kInputFormatOption,
  kKeyFilterOption,
  kMergeModeMotion,
  kOutputTypeOption,
  kSchemaOption,
  kShardCountOption,
  kShardIndexOption,
  kStripKeyPrefixOption,
  kThresholdOption,
};

struct option kLongOptions[] = {
    {"add-key-prefix", required_argument, nullptr, kAddKeyPrefixOption},
    {"date-format", required_argument, nullptr, kDateFormatOption},
    {"delimiter", required_argument, nullptr, kDelimiterOption},
    {"input-format", required_argument, nullptr, kInputFormatOption},
    {"key-filter", required_argument, nullptr, kKeyFilterOption},
    {"merge-mode", required_argument, nullptr, kMergeModeMotion},
    {"no-unescape", no_argument, &no_unescape, 1},
    {"output-type", required_argument, nullptr, kOutputTypeOption},
    {"output-format", required_argument, nullptr, kOutputTypeOption},
    {"schema", required_argument, nullptr, kSchemaOption},
    {"shard-count", required_argument, nullptr, kShardCountOption},
    {"shard-index", required_argument, nullptr, kShardIndexOption},
    {"strip-key-prefix", required_argument, nullptr, kStripKeyPrefixOption},
    {"syslog", no_argument, &do_syslog, 1},
    {"threshold", required_argument, nullptr, kThresholdOption},
    {"version", no_argument, &print_version, 1},
    {"help", no_argument, &print_help, 1},
    {0, 0, 0, 0}};

std::unique_ptr<ca_table::Table> table_handle;

enum token_state { parse_key, parse_offset, parse_value };

struct parse_state {
  enum token_state token_state = parse_key;

  int no_match = 0;
  bool escape = false;
};

}  // namespace

static std::string current_key, next_key;

std::string offset;

static uint64_t current_offset;

std::string value_string;

static std::vector<ca_offset_score> values;
static size_t max_sorted_value_index = 0;

static float threshold;
static bool has_threshold;

static std::string strip_key_prefix;
static std::string add_key_prefix;

static int do_map_documents;
static std::vector<ca_table::Table*> summary_tables;
static uint64_t* summary_table_offsets;

static int do_summaries;

struct LessThanOffset {
  bool operator()(const ca_offset_score& lhs,
                  const ca_offset_score& rhs) const {
    return lhs.offset < rhs.offset;
  }

  bool operator()(const ca_offset_score& lhs, uint64_t offset) const {
    return lhs.offset < offset;
  }
};

static void AddOffsetScore(uint64_t offset, const ca_score& score) {
  if (max_sorted_value_index == values.size() &&
      (values.empty() || offset > values.back().offset))
    ++max_sorted_value_index;

  ca_offset_score value;
  value.offset = offset;
  value.score = score.score;
  value.score_pct5 = score.score_pct5;
  value.score_pct25 = score.score_pct25;
  value.score_pct75 = score.score_pct75;
  value.score_pct95 = score.score_pct95;
  values.push_back(value);
}

static void FlushValues(const std::string& key,
                        std::vector<ca_offset_score>&& time_series) {
  if ((shard_count > 1 && (ev::Hash(key) % shard_count) != shard_index) ||
      (has_key_filter && std::regex_search(key, key_filter))) {
    max_sorted_value_index = 0;
    return;
  }

  max_sorted_value_index = 0;

  if (has_threshold) {
    auto new_end = std::remove_if(
        time_series.begin(), time_series.end(),
        [](const ca_offset_score& v) { return v.score < threshold; });

    if (new_end == time_series.begin()) return;

    time_series.erase(new_end, time_series.end());
  }

  auto c_key = key.c_str();

  if (!strip_key_prefix.empty()) {
    KJ_REQUIRE(current_key.size() >= strip_key_prefix.size());
    KJ_REQUIRE(0 ==
               memcmp(c_key, strip_key_prefix.data(), strip_key_prefix.size()));
    c_key += strip_key_prefix.size();
  }

  std::string buffer;
  if (!add_key_prefix.empty()) {
    buffer = add_key_prefix;
    buffer += c_key;
    c_key = buffer.c_str();
  }

  std::stable_sort(
      time_series.begin(), time_series.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });

  ca_table_write_offset_score(table_handle.get(), c_key, &time_series[0],
                              time_series.size());
}

static void FlushValues(const std::string& key) {
  std::sort(values.begin() + max_sorted_value_index, values.end(),
            LessThanOffset());

  FlushValues(key, std::move(values));
  values.clear();
}

static void parse_data(const char* begin, const char* end, parse_state& state) {
  for (; begin != end; ++begin) {
    char ch = *begin;
    bool was_escaped = false;

    if (state.escape) {
      switch (ch) {
        case 't':
          ch = '\t';
          break;
        case 'r':
          ch = '\r';
          break;
        case 'n':
          ch = '\n';
          break;
      }
      state.escape = false;
      was_escaped = true;
    } else if (ch == '\\' && !no_unescape) {
      state.escape = true;
      continue;
    }

    switch (state.token_state) {
      case parse_key: {
        if (!was_escaped && ch == delimiter) {
          current_key.swap(next_key);
          next_key.clear();

          state.token_state = do_summaries ? parse_value : parse_offset;

          break;
        }

        // If we're getting a new key, flush the values associated with the
        // old key.
        if (!values.empty() && (next_key.size() == current_key.size() ||
                                current_key[next_key.size()] != ch)) {
          FlushValues(current_key);
        }

        next_key.push_back(ch);

        break;
      }

      case parse_offset: {
        if (!was_escaped && ch == delimiter) {
          if (do_map_documents) {
            state.no_match = 1;

            for (auto i = summary_tables.size(); i-- > 0;) {
              if (summary_tables[i]->SeekToKey(offset)) {
                current_offset =
                    summary_tables[i]->Offset() + summary_table_offsets[i];

                state.no_match = 0;

                break;
              }
            }
          } else {
            struct tm tm;
            char* end;

            memset(&tm, 0, sizeof(tm));

            if (!(end = strptime(offset.c_str(), date_format, &tm)))
              errx(EX_DATAERR,
                   "Unable to parse date '%s' according to format '%s'",
                   offset.c_str(), date_format);

            if (*end)
              errx(EX_DATAERR, "Junk at end of offset '%s': %s", offset.c_str(),
                   end);

            current_offset = timegm(&tm);

            if (!current_offset)
              fprintf(stderr, "Warning: %s maps to 1970-01-01\n", date_format);
          }

          offset.clear();
          state.token_state = parse_value;

          break;
        }

        offset.push_back(ch);

        break;
      }

      case parse_value: {
        if (!was_escaped && ch == '\r') continue;

        if (!was_escaped && ch == '\n') {
          if (do_summaries) {
            struct iovec iov[2];

            std::string buffer;
            if (!add_key_prefix.empty()) {
              buffer = add_key_prefix;
              buffer += current_key;
              iov[0].iov_base = const_cast<char*>(buffer.data());
              iov[0].iov_len = buffer.size();
            } else {
              iov[0].iov_base = const_cast<char*>(current_key.data());
              iov[0].iov_len = current_key.size();
            }
            iov[1].iov_base = const_cast<char*>(value_string.data());
            iov[1].iov_len = value_string.size();

            table_handle->InsertRow(iov, 2);
          } else if (state.no_match) {
            state.no_match = 0;
          } else {
            ca_score score;
            char* endptr;

            score.score = strtod(value_string.c_str(), &endptr);

            if (*endptr)
              errx(EX_DATAERR,
                   "Unable to parse value '%s'.  Unexpected suffix: '%s'",
                   value_string.c_str(), endptr);

            AddOffsetScore(current_offset, score);
          }

          value_string.clear();
          state.token_state = parse_key;

          break;
        }

        value_string.push_back(ch);

        break;
      }
    }
  }
}

static void SimpleMergeCallback(const std::string& key,
                                std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ev::Hash(key) % shard_count) != shard_index) return;

  KJ_REQUIRE(data.size() == 1, data.size(),
             "Duplicate detected; you must choose a merge mode");

  if (add_key_prefix.empty()) {
    table_handle->InsertRow(key, data[0]);
  } else {
    table_handle->InsertRow(add_key_prefix + key, data[0]);
  }
}

static void MergeTimeSeriesCallback(const std::string& key,
                                    std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ev::Hash(key) % shard_count) != shard_index) return;

  if (data.size() == 1) {
    // TODO(mortehu): Support strip_key_prefix.

    if (add_key_prefix.empty()) {
      table_handle->InsertRow(key, data[0]);
    } else {
      table_handle->InsertRow(add_key_prefix + key, data[0]);
    }
  } else {
    typedef std::vector<ca_offset_score> TimeSeries;

    std::vector<std::future<TimeSeries>> decoded_data_promises;

    for (const auto& d : data) {
      decoded_data_promises.emplace_back(
          std::async(std::launch::deferred, [&d] {
            TimeSeries result;
            ca_offset_score_parse(d, &result);
            return result;
          }));
    }

    std::vector<TimeSeries> decoded_data;
    std::vector<std::tuple<TimeSeries::const_iterator,
                           TimeSeries::const_iterator, size_t>> iterators;

    size_t idx = 0;
    for (auto& p : decoded_data_promises) {
      decoded_data.emplace_back(std::move(p.get()));

      const auto& d = decoded_data.back();
      iterators.emplace_back(d.begin(), d.end(), idx++);
    }

    const auto kHeapComparator =
        [](const std::tuple<TimeSeries::const_iterator,
                            TimeSeries::const_iterator, size_t>& lhs,
           const std::tuple<TimeSeries::const_iterator,
                            TimeSeries::const_iterator, size_t>& rhs) -> bool {
      if (std::get<0>(lhs)->offset > std::get<0>(rhs)->offset) return true;
      return (std::get<0>(lhs)->offset == std::get<0>(rhs)->offset &&
              std::get<2>(lhs) > std::get<2>(rhs));
    };

    std::make_heap(iterators.begin(), iterators.end(), kHeapComparator);

    std::vector<ca_offset_score> merged_values;

    while (!iterators.empty()) {
      auto front = iterators.front();
      std::pop_heap(iterators.begin(), iterators.end(), kHeapComparator);
      iterators.pop_back();

      const auto& new_value = *std::get<0>(front);

      if (merged_values.empty() ||
          new_value.offset > merged_values.back().offset) {
        merged_values.emplace_back(new_value);
      } else {
        auto& existing_value = merged_values.back();
        KJ_REQUIRE(new_value.offset == existing_value.offset, new_value.offset,
                   existing_value.offset);

        if (merge_mode == kMergeUnion) {
          KJ_REQUIRE(existing_value.score == new_value.score,
                     "attempted union merge on conflicting data sets",
                     existing_value.offset, existing_value.score,
                     new_value.offset, new_value.score);
        } else if (merge_mode == kMergePickOne) {
          if (existing_value.HasPercentiles() && !new_value.HasPercentiles()) {
            existing_value.score = new_value.score;
            existing_value.score_pct5 = new_value.score_pct5;
            existing_value.score_pct25 = new_value.score_pct25;
            existing_value.score_pct75 = new_value.score_pct75;
            existing_value.score_pct95 = new_value.score_pct95;
          }
        } else {
          KJ_ASSERT(merge_mode == kMergeSum);
          KJ_REQUIRE(!new_value.HasPercentiles());
          existing_value.score += new_value.score;
        }
      }

      if (++std::get<0>(front) != std::get<1>(front)) {
        iterators.emplace_back(front);
        std::push_heap(iterators.begin(), iterators.end(), kHeapComparator);
      }
    }

    FlushValues(key, std::move(merged_values));
  }
}

static void MergeSummariesCallback(std::string key,
                                   std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ev::Hash(key) % shard_count) != shard_index) return;

  if (!add_key_prefix.empty()) key += add_key_prefix;

  if (data.size() == 1 || merge_mode == kMergePickOne) {
    table_handle->InsertRow(key, data[0]);
    return;
  }

  KJ_REQUIRE(merge_mode == kMergeUnion);

  std::string merged;

  for (const auto& d : data) {
    ev::StringRef json(d);

    if (!json.empty() && json.front() == '{') {
      KJ_REQUIRE(json.back() == '}');
      json.pop_front();
      json.pop_back();
    }

    while (!json.empty() && std::isspace(json.front())) json.pop_front();

    if (json.empty()) continue;

    if (!merged.empty()) merged.push_back(',');
    merged.append(json.data(), json.size());
  }

  table_handle->InsertRow(key, merged);
}

void CopyTable(ca_table::Table* input, ca_table::Table* output) {
  struct iovec key, value;
  int ret;

  while (1 == (ret = input->ReadRow(&key, &value))) {
    if (shard_count > 1 && (ev::Hash(key) % shard_count) != shard_index)
      continue;

    auto key_begin = reinterpret_cast<char*>(key.iov_base);
    auto key_end = key_begin + key.iov_len;

    if (has_key_filter && std::regex_search(key_begin, key_end, key_filter))
      continue;

    if (!strip_key_prefix.empty()) {
      KJ_REQUIRE(key.iov_len >= strip_key_prefix.size());
      KJ_REQUIRE(0 == memcmp(key.iov_base, strip_key_prefix.data(),
                             strip_key_prefix.size()));
      key.iov_base = key_begin + strip_key_prefix.size();
      key.iov_len -= strip_key_prefix.size();
    }

    std::string buffer;
    if (!add_key_prefix.empty()) {
      buffer = add_key_prefix;
      auto key_begin = reinterpret_cast<const char*>(key.iov_base);
      buffer.insert(buffer.end(), key_begin, key_begin + key.iov_len);
      key.iov_base = const_cast<char*>(buffer.data());
      key.iov_len += add_key_prefix.size();
    }

    output->InsertRow(key, value);
  }

  if (ret == -1) errx(EXIT_FAILURE, "Error reading table: %s", ca_last_error());
}

int main(int argc, char** argv) try {
  Format input_format = kFormatAuto;
  DataType output_type = kDataTypeTimeSeries;
  const char* output_path;

  const char* schema_path = NULL;
  struct ca_schema* schema = NULL;

  int i;

  off_t file_size;

  setenv("TZ", "", 1);

  while ((i = getopt_long(argc, argv, "", kLongOptions, 0)) != -1) {
    if (i == 0) continue;

    if (i == '?')
      errx(EX_USAGE, "Try '%s --help' for more information.", argv[0]);

    switch (static_cast<Option>(i)) {
      case kAddKeyPrefixOption:
        add_key_prefix = optarg;
        break;

      case kDelimiterOption:
        if (!*optarg) errx(EX_USAGE, "Provided delimiter is empty");

        if (optarg[1])
          errx(EX_USAGE, "Provided delimiter is more than one ASCII character");

        delimiter = *optarg;

        break;

      case kDateFormatOption:
        date_format = optarg;
        break;

      case kInputFormatOption:
        if (!strcmp(optarg, "ca-table")) {
          input_format = kFormatCaTable;
        } else if (!strcmp(optarg, "columnfile")) {
          input_format = kFormatColumnFile;
        } else if (!strcmp(optarg, "csv")) {
          input_format = kFormatCSV;
        }
        break;

      case kKeyFilterOption:
        key_filter = optarg;
        has_key_filter = true;
        break;

      case kMergeModeMotion:
        if (!strcmp(optarg, "pick-one")) {
          merge_mode = kMergePickOne;
        } else if (!strcmp(optarg, "sum")) {
          merge_mode = kMergeSum;
        } else if (!strcmp(optarg, "union")) {
          merge_mode = kMergeUnion;
        } else {
          errx(EX_USAGE, "Unknown merge mode '%s'", optarg);
        }
        break;

      case kOutputTypeOption:
        if (!strcmp(optarg, "time-series")) {
          output_type = kDataTypeTimeSeries;
        } else if (!strcmp(optarg, "summaries")) {
          output_type = kDataTypeSummaries;
        } else if (!strcmp(optarg, "index")) {
          output_type = kDataTypeIndex;
        } else {
          errx(EX_USAGE, "Unknown output type '%s'", optarg);
        }
        break;

      case kSchemaOption:
        schema_path = optarg;
        break;

      case kShardCountOption:
        shard_count = ev::StringToUInt64(optarg);
        KJ_REQUIRE(shard_count > 0);
        break;

      case kShardIndexOption:
        shard_index = ev::StringToUInt64(optarg);
        break;

      case kStripKeyPrefixOption:
        strip_key_prefix = optarg;
        break;

      case kThresholdOption:
        threshold = ev::StringToUInt64(optarg);
        has_threshold = true;
        break;
    }
  }

  if (print_help) {
    printf(
        "Usage: %s [OPTION]... TABLE [INPUT]...\n"
        "\n"
        "      --add-key-prefix=PREFIX\n"
        "                             add prefix to output keys\n"
        "      --date-format=FORMAT   use provided date format [%s]\n"
        "      --date=DATE            use DATE as timestamp\n"
        "      --delimiter=DELIMITER  input delimiter [%c]\n"
        "      --input-format=FORMAT  format of input data\n"
        "      --key=KEY              use KEY as key\n"
        "      --key-filter=REGEX     skip input keys matching REGEX\n"
        "      --merge-mode=MODE      merge mode (pick-one|sum|union)\n"
        "      --no-unescape          don't apply any unescaping logic\n"
        "      --output-type=TYPE     type of output table\n"
        "                               (index|summaries|time-series)\n"
        "      --schema=PATH          schema file for index building\n"
        "      --strip-key-prefix=PREFIX\n"
        "                             remove PREFIX from keys\n"
        "      --threshold=SCORE      minimum score to include in output\n"
        "      --help     display this help and exit\n"
        "      --version  display version information\n"
        "\n"
        "Report bugs to <morten.hustveit@gmail.com>\n",
        argv[0], date_format, delimiter);

    return EXIT_SUCCESS;
  }

  if (print_version) {
    fprintf(stdout, "%s\n", PACKAGE_STRING);

    return EXIT_SUCCESS;
  }

  const char* output_binary_format = "leveldb-table";

  if (output_type == kDataTypeIndex) {
    KJ_REQUIRE(schema_path != nullptr,
               "--output-format=index can only be used with --schema=PATH");

    KJ_REQUIRE(nullptr != (schema = ca_schema_load(schema_path)), schema_path);

    summary_tables = ca_schema_summary_tables(schema, &summary_table_offsets);

    do_map_documents = 1;
  } else if (output_type == kDataTypeSummaries) {
    // Summary tables need to be quickly seekable, which LevelDB Tables are
    // not.
    output_binary_format = "write-once";

    do_summaries = 1;
  }

  if (optind + 1 > argc)
    errx(EX_USAGE, "Usage: %s [OPTION]... TABLE [INPUT]...", argv[0]);

  output_path = argv[optind++];

  // TODO(mortehu): Make all input formats work with any number of arguments.

  if (input_format == kFormatColumnFile) {
    std::vector<kj::AutoCloseFd> inputs;

    if (optind == argc) {
      inputs.emplace_back(STDIN_FILENO);
    } else {
      while (optind < argc)
        inputs.emplace_back(ev::OpenFile(argv[optind++], O_RDONLY));
    }

    table_handle = ca_table_open(output_binary_format, output_path,
                                 O_CREAT | O_TRUNC | O_WRONLY, 0444);

    for (auto& input : inputs) {
      ev::ColumnFileReader reader(std::move(input));

      switch (output_type) {
        case kDataTypeSummaries:
          while (!reader.End()) {
            auto& row = reader.GetRow();
            KJ_REQUIRE(row.size() == 2);
            KJ_REQUIRE(!row[0].second.empty());
            KJ_REQUIRE(!row[1].second.empty());
            table_handle->InsertRow(row[0].second, row[1].second);
          }
          break;

        case kDataTypeIndex:
        case kDataTypeTimeSeries: {
          std::string key;
          std::vector<ca_offset_score> data;
          while (!reader.End()) {
            auto& row = reader.GetRow();
            KJ_REQUIRE(row.size() >= 2 && row.size() <= 3);
            KJ_REQUIRE(!row[0].second.empty());
            KJ_REQUIRE(!row[1].second.empty());

            if (row[0].second != key) {
              if (!data.empty()) {
                ca_table_write_offset_score(table_handle.get(), key, &data[0],
                                            data.size());
                data.clear();
              }
              key = row[0].second.str();
            }

            uint64_t offset;
            if (output_type == kDataTypeIndex) {
              bool match_found = false;
              for (auto i = summary_tables.size(); i-- > 0;) {
                if (summary_tables[i]->SeekToKey(row[1].second)) {
                  offset =
                      summary_tables[i]->Offset() + summary_table_offsets[i];

                  match_found = true;
                  break;
                }
              }

              if (!match_found) continue;
            } else {
              auto time_string = row[1].second.str();

              struct tm tm;
              memset(&tm, 0, sizeof(tm));

              auto endptr = strptime(time_string.c_str(), date_format, &tm);
              KJ_REQUIRE(endptr != nullptr, time_string, date_format);
              KJ_REQUIRE(!*endptr, time_string);

              offset = timegm(&tm);

              if (!offset)
                fprintf(stderr, "Warning: %s maps to 1970-01-01\n",
                        time_string.c_str());
            }

            float score = 0.0f;
            if (row.size() == 3) {
              KJ_REQUIRE(row[2].second.size() == sizeof(score));
              memcpy(&score, row[2].second.data(), sizeof(score));
            }

            data.emplace_back(offset, score);
          }

          if (!data.empty()) {
            ca_table_write_offset_score(table_handle.get(), key, &data[0],
                                        data.size());
            data.clear();
          }
        } break;
      }
    }
  } else if (optind == argc) {
    KJ_REQUIRE(input_format == kFormatAuto || input_format == kFormatCSV);

    table_handle = ca_table_open(output_binary_format, output_path,
                                 O_CREAT | O_TRUNC | O_WRONLY, 0444);

    kj::AutoCloseFd input(STDIN_FILENO);

    if (-1 == (file_size = lseek(input, 0, SEEK_END))) {
      parse_state state;

      for (;;) {
        char buffer[65536];
        ssize_t ret;
        KJ_SYSCALL(ret = read(input, buffer, sizeof(buffer)));
        if (!ret) break;

        parse_data(buffer, buffer + ret, state);
      }
    } else {
      void* map;

      if (!file_size) errx(EX_DATAERR, "input file has zero size");

      if (MAP_FAILED ==
          (map = mmap(NULL, file_size, PROT_READ, MAP_SHARED, input, 0))) {
        KJ_FAIL_SYSCALL("mmap", errno, file_size);
      }

      parse_state state;

      parse_data(reinterpret_cast<const char*>(map),
                 reinterpret_cast<const char*>(map) + file_size, state);

      munmap(map, file_size);
    }
  } else {
    KJ_REQUIRE(input_format == kFormatAuto || input_format == kFormatCaTable);
    std::vector<std::unique_ptr<ca_table::Table>> tables;

    for (i = optind; i < argc; ++i) {
      auto table = ca_table_open(NULL, argv[i], O_RDONLY, 0);

      KJ_REQUIRE(table->IsSorted());

      tables.emplace_back(std::move(table));
    }

    table_handle = ca_table_open(output_binary_format, output_path,
                                 O_CREAT | O_TRUNC | O_WRONLY, 0444);

    if (tables.size() == 1) {
      CopyTable(tables[0].get(), table_handle.get());
    } else {
      switch (output_type) {
        case kDataTypeTimeSeries:
          ca_table_merge(tables, MergeTimeSeriesCallback);
          break;

        case kDataTypeSummaries:
          ca_table_merge(tables, MergeSummariesCallback);

        default:
          ca_table_merge(tables, SimpleMergeCallback);
      }
    }
  }

  if (!values.empty()) FlushValues(current_key);

  table_handle->Sync();
} catch (kj::Exception e) {
  if (do_syslog) {
    syslog(LOG_ERR, "Error: %s:%d: %s", e.getFile(), e.getLine(),
           e.getDescription().cStr());
  } else {
    fprintf(stderr, "Error: %s:%d: %s\n", e.getFile(), e.getLine(),
            e.getDescription().cStr());
  }
  return EXIT_FAILURE;
} catch (std::runtime_error e) {
  if (do_syslog) {
    syslog(LOG_ERR, "Runtime errir: %s", e.what());
  } else {
    fprintf(stderr, "Runtime error: %s\n", e.what());
  }
  return EXIT_FAILURE;
}
