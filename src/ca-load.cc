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

#include <columnfile.h>
#include <kj/debug.h>
#include <re2/re2.h>

#include "src/ca-table.h"
#include "src/util.h"
#include "src/schema.h"

namespace ca_table = cantera::table;

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
int no_unescape;
int verbose;

MergeMode merge_mode = kMergeUnion;

char delimiter = '\t';
const char* date_format = "%Y-%m-%d %H:%M:%S";

std::unique_ptr<re2::RE2> key_filter;
std::string min_key, max_key;

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
    {"threshold", required_argument, nullptr, kThresholdOption},
    {"verbose", no_argument, &verbose, 1},
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

std::string current_key, next_key;

std::string offset;

uint64_t current_offset;

std::string value_string;

std::vector<ca_table::ca_offset_score> values;
size_t max_sorted_value_index = 0;

float threshold;
bool has_threshold;

std::string strip_key_prefix;
std::string add_key_prefix;

int do_map_documents;
std::unique_ptr<ca_table::Schema> schema;

int do_summaries;

struct LessThanOffset {
  bool operator()(const ca_table::ca_offset_score& lhs,
                  const ca_table::ca_offset_score& rhs) const {
    return lhs.offset < rhs.offset;
  }

  bool operator()(const ca_table::ca_offset_score& lhs, uint64_t offset) const {
    return lhs.offset < offset;
  }
};

void AddOffsetScore(uint64_t offset, const ca_table::ca_score& score) {
  if (max_sorted_value_index == values.size() &&
      (values.empty() || offset > values.back().offset))
    ++max_sorted_value_index;

  ca_table::ca_offset_score value;
  value.offset = offset;
  value.score = score.score;
  value.score_pct5 = score.score_pct5;
  value.score_pct25 = score.score_pct25;
  value.score_pct75 = score.score_pct75;
  value.score_pct95 = score.score_pct95;
  values.push_back(value);
}

void FlushValues(const std::string& key,
                        std::vector<ca_table::ca_offset_score>&& time_series) {
  if ((shard_count > 1 && (ca_table::internal::Hash(key) % shard_count) != shard_index) ||
      (key_filter && !RE2::FullMatch(key, *key_filter))) {
    max_sorted_value_index = 0;
    return;
  }

  max_sorted_value_index = 0;

  if (has_threshold) {
    auto new_end = std::remove_if(
        time_series.begin(), time_series.end(),
        [](const ca_table::ca_offset_score& v) { return v.score < threshold; });

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

void FlushValues(const std::string& key) {
  std::sort(values.begin() + max_sorted_value_index, values.end(),
            LessThanOffset());

  FlushValues(key, std::move(values));
  values.clear();
}

void parse_data(const char* begin, const char* end, parse_state& state) {
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

            for (auto i = schema->summary_tables.size(); i-- > 0;) {
              if (schema->summary_tables[i].second->SeekToKey(offset)) {
                current_offset = schema->summary_tables[i].second->Offset() +
                                 std::get<uint64_t>(schema->summary_tables[i]);

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
            if (add_key_prefix.empty()) {
              table_handle->InsertRow(current_key, value_string);
            } else {
              table_handle->InsertRow(add_key_prefix + current_key, value_string);
            }
          } else if (state.no_match) {
            state.no_match = 0;
          } else {
            ca_table::ca_score score;
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

void SimpleMergeCallback(const std::string& key,
                                std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ca_table::internal::Hash(key) % shard_count) != shard_index) return;

  KJ_REQUIRE(data.size() == 1, data.size(),
             "Duplicate detected; you must choose a merge mode");

  cantera::string_view value{data[0].data(), data[0].size()};

  if (add_key_prefix.empty()) {
    table_handle->InsertRow(key, value);
  } else {
    table_handle->InsertRow(add_key_prefix + key, value);
  }
}

void MergeTimeSeriesCallback(const std::string& key,
                                    std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ca_table::internal::Hash(key) % shard_count) != shard_index) return;

  if (data.size() == 1) {
    // TODO(mortehu): Support strip_key_prefix.
    cantera::string_view value{data[0].data(), data[0].size()};

    if (add_key_prefix.empty()) {
      table_handle->InsertRow(key, value);
    } else {
      table_handle->InsertRow(add_key_prefix + key, value);
    }
  } else {
    typedef std::vector<ca_table::ca_offset_score> TimeSeries;

    std::vector<std::future<TimeSeries>> decoded_data_promises;

    for (const auto& d : data) {
      decoded_data_promises.emplace_back(
          std::async(std::launch::deferred, [&d] {
            TimeSeries result;
            ca_table::ca_offset_score_parse(cantera::string_view{d.data(), d.size()}, &result);
            return result;
          }));
    }

    std::vector<TimeSeries> decoded_data;
    std::vector<std::tuple<TimeSeries::const_iterator,
                           TimeSeries::const_iterator, size_t>> iterators;

    size_t idx = 0;
    for (auto& p : decoded_data_promises) {
      decoded_data.emplace_back(p.get());

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

    std::vector<ca_table::ca_offset_score> merged_values;

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

void MergeSummariesCallback(std::string key,
                                   std::vector<std::vector<char>>& data) {
  if (shard_count > 1 && (ca_table::internal::Hash(key) % shard_count) != shard_index) return;

  if (!add_key_prefix.empty()) key += add_key_prefix;

  if (data.size() == 1 || merge_mode == kMergePickOne) {
    cantera::string_view value{data[0].data(), data[0].size()};
    table_handle->InsertRow(key, value);
    return;
  }

  KJ_REQUIRE(merge_mode == kMergeUnion);

  std::string merged;

  for (const auto& d : data) {
    cantera::string_view json{d.data(), d.size()};

    if (!json.empty() && json.front() == '{') {
      KJ_REQUIRE(json.back() == '}');
      json.remove_prefix(1);
      json.remove_suffix(1);
    }

    while (!json.empty() && std::isspace(json.front())) json.remove_suffix(1);

    if (json.empty()) continue;

    if (!merged.empty()) merged.push_back(',');
    merged.append(json.data(), json.size());
  }

  table_handle->InsertRow(key, merged);
}

void CopyTable(ca_table::Table* input, ca_table::Table* output) {
  cantera::string_view key, value;

  while (input->ReadRow(key, value)) {
    if (shard_count > 1 && (ca_table::internal::Hash(key) % shard_count) != shard_index)
      continue;

    if (key_filter &&
        !RE2::FullMatch(re2::StringPiece(key.data(), key.size()), *key_filter))
      continue;

    if (!strip_key_prefix.empty()) {
      KJ_REQUIRE(std::equal(key.begin(), key.end(), strip_key_prefix.begin(), strip_key_prefix.end()));
      key.remove_prefix(strip_key_prefix.size());
    }

    std::string buffer;
    if (!add_key_prefix.empty()) {
      buffer = add_key_prefix;
      buffer.insert(buffer.end(), key.begin(), key.end());
      key = buffer;
    }

    output->InsertRow(key, value);
  }
}

}  // namespace

int main(int argc, char** argv) try {
  Format input_format = kFormatAuto;
  DataType output_type = kDataTypeTimeSeries;
  const char* output_path;

  const char* schema_path = NULL;

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
        key_filter = std::make_unique<re2::RE2>(optarg);
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
        shard_count = ca_table::internal::StringToUInt64(optarg);
        KJ_REQUIRE(shard_count > 0);
        break;

      case kShardIndexOption:
        shard_index = ca_table::internal::StringToUInt64(optarg);
        break;

      case kStripKeyPrefixOption:
        strip_key_prefix = optarg;
        break;

      case kThresholdOption:
        threshold = ca_table::internal::StringToUInt64(optarg);
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
        "      --verbose  display format information\n"
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

    schema = std::make_unique<ca_table::Schema>(schema_path);
    schema->Load();

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

  if (key_filter) {
    KJ_REQUIRE(key_filter->PossibleMatchRange(&min_key, &max_key, 64));
  }

  // TODO(mortehu): Make all input formats work with any number of arguments.

  if (input_format == kFormatColumnFile) {
    std::vector<kj::AutoCloseFd> inputs;

    if (optind == argc) {
      inputs.emplace_back(STDIN_FILENO);
    } else {
      while (optind < argc)
        inputs.emplace_back(ca_table::internal::OpenFile(argv[optind++], O_RDONLY));
    }

    ca_table::TableOptions opts =
        ca_table::TableOptions::Create().SetFileMode(0444);
    table_handle =
        ca_table::TableFactory::Create(output_binary_format, output_path, opts);

    for (auto& input : inputs) {
      cantera::ColumnFileReader reader(std::move(input));

      switch (output_type) {
        case kDataTypeSummaries:
          while (!reader.End()) {
            auto& row = reader.GetRow();
            KJ_REQUIRE(row.size() == 2);
            table_handle->InsertRow(row[0].second.value(),
                                    row[1].second.value());
          }
          break;

        case kDataTypeIndex:
        case kDataTypeTimeSeries: {
          std::string key;
          std::vector<ca_table::ca_offset_score> data;
          while (!reader.End()) {
            auto& row = reader.GetRow();
            KJ_REQUIRE(row.size() >= 2 && row.size() <= 3);

            if (row[0].second.value() != key) {
              if (!data.empty()) {
                ca_table::ca_table_write_offset_score(table_handle.get(), key, &data[0],
                                            data.size());
                data.clear();
              }
              key = row[0].second.value().to_string();
            }

            uint64_t offset;
            if (output_type == kDataTypeIndex) {
              bool match_found = false;
              for (auto i = schema->summary_tables.size(); i-- > 0;) {
                if (schema->summary_tables[i].second->SeekToKey(
                        row[1].second.value())) {
                  offset = schema->summary_tables[i].second->Offset() +
                           std::get<uint64_t>(schema->summary_tables[i]);

                  match_found = true;
                  break;
                }
              }

              if (!match_found) continue;
            } else {
              auto time_string = row[1].second.value().to_string();

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
              KJ_REQUIRE(row[2].second.value().size() == sizeof(score));
              memcpy(&score, row[2].second.value().data(), sizeof(score));
            }

            data.emplace_back(offset, score);
          }

          if (!data.empty()) {
            ca_table::ca_table_write_offset_score(table_handle.get(), key, &data[0],
                                        data.size());
            data.clear();
          }
        } break;
      }
    }
  } else if (optind == argc) {
    KJ_REQUIRE(input_format == kFormatAuto || input_format == kFormatCSV);

    ca_table::TableOptions opts =
        ca_table::TableOptions::Create().SetFileMode(0444);
    table_handle =
        ca_table::TableFactory::Create(output_binary_format, output_path, opts);

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
      KJ_CONTEXT(argv[i]);
      auto table = ca_table::TableFactory::Open(NULL, argv[i]);

      KJ_REQUIRE(table->IsSorted());

      if (!min_key.empty()) table->SeekToKey(min_key);

      tables.emplace_back(std::move(table));
    }

    ca_table::TableOptions opts =
        ca_table::TableOptions::Create().SetFileMode(0444);
    table_handle =
        ca_table::TableFactory::Create(output_binary_format, output_path, opts);

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
  KJ_LOG(FATAL, e);
  return EXIT_FAILURE;
} catch (std::runtime_error e) {
  fprintf(stderr, "Runtime error: %s\n", e.what());
  return EXIT_FAILURE;
}
