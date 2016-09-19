/*
    Inverted index query processor
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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <unordered_map>

#include <ca-cas/client.h>
#include <json/value.h>
#include <json/writer.h>
#include <kj/async-io.h>
#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/keywords.h"
#include "src/query.h"
#include "src/util.h"

namespace cantera {
namespace table {

using namespace internal;

namespace {

std::unordered_map<uint64_t, Json::Value> extra_data;

std::unique_ptr<kj::AsyncIoContext> aio_context;
std::unique_ptr<cantera::CASClient> cas_client;

void CreateCASClient() {
  // TODO(mortehu): Create this in `main()` instead.
  aio_context = std::make_unique<kj::AsyncIoContext>(kj::setupAsyncIo());
  cas_client = std::make_unique<cantera::CASClient>(*aio_context);
}

std::vector<ca_offset_score> UnionOffsets(
    const std::vector<ca_offset_score>& lhs,
    const std::vector<ca_offset_score>& rhs) {
  std::vector<ca_offset_score> result;

  result.reserve(lhs.size() + rhs.size());

  auto lhs_iter = lhs.begin();
  auto rhs_iter = rhs.begin();
  auto lhs_end = lhs.end();
  auto rhs_end = rhs.end();

  while (lhs_iter != lhs_end && rhs_iter != rhs_end) {
    if (lhs_iter->offset < rhs_iter->offset) {
      result.emplace_back(*lhs_iter++);
    } else {
      if (lhs_iter->offset == rhs_iter->offset) ++lhs_iter;

      result.emplace_back(*rhs_iter++);
    }
  }

  result.insert(result.end(), lhs_iter, lhs_end);
  result.insert(result.end(), rhs_iter, rhs_end);

  return result;
}

size_t IntersectOffsets(struct ca_offset_score* lhs, size_t lhs_count,
                        const struct ca_offset_score* rhs, size_t rhs_count) {
  struct ca_offset_score* output, *o;
  const struct ca_offset_score* lhs_end, *rhs_end;

  output = o = lhs;

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end) {
    if (lhs->offset == rhs->offset) {
      const auto offset = lhs->offset;
      do {
        *o++ = *lhs++;
      } while (lhs != lhs_end && lhs->offset == offset);

      ++rhs;

      continue;
    }

    if (lhs->offset < rhs->offset)
      ++lhs;
    else
      ++rhs;
  }

  return o - output;
}

// Checks whether a string may be a valid domain name.
bool IsValidDomainName(const std::string& name) {
  if (name.size() < 3) return false;

  if (name[0] == '.' || name.back() == '.') return false;

  return true;
}

// Removes duplicate offsets, keeping either the maximum or minimum score.
void RemoveDuplicates(std::vector<ca_offset_score>& data, const bool use_max) {
  auto in = data.begin();
  auto out = data.begin();

  if (in == data.end()) return;
  ++in;
  ++out;

  while (in != data.end()) {
    if (in->offset != (out - 1)->offset) {
      *out++ = *in++;
      continue;
    }

    if (use_max == (in->score > (out - 1)->score)) (out - 1)->score = in->score;
    ++in;
  }

  data.erase(out, data.end());
}

std::string TimeToDateString(double time) {
  auto tt = static_cast<time_t>(time * 86400);
  struct tm t;
  gmtime_r(&tt, &t);
  char buf[64];
  KJ_REQUIRE(0 != strftime(buf, sizeof(buf), "%B %e, %Y", &t));

  std::string result(buf);
  auto i = result.find("  ");
  if (i != std::string::npos) result.erase(i, 1);
  return result;
}

template <typename Filter>
void Join(std::vector<ca_offset_score>& lhs,
          const std::vector<ca_offset_score>& rhs, Filter filter) {
  auto out = lhs.begin();

  auto l = lhs.begin();
  auto r = rhs.begin();

  while (l != lhs.end() && r != rhs.end()) {
    if (l->offset < r->offset) {
      ++l;
      continue;
    }
    if (r->offset < l->offset) {
      ++r;
      continue;
    }

    if (filter(l->score, r->score)) *out++ = *l;

    ++l;
    ++r;
  }

  lhs.erase(out, lhs.end());
}

}  // namespace

void LookupIndexKey(
    const std::vector<std::unique_ptr<Table>>& index_tables,
    const char* key,
    std::function<void(std::vector<ca_offset_score>)>&& callback) {
  const auto unescaped_key = DecodeURIComponent(key);

  for (size_t i = 0; i < index_tables.size(); ++i) {
    if (!index_tables[i]->SeekToKey(unescaped_key)) continue;

    string_view key, data;
    KJ_REQUIRE(index_tables[i]->ReadRow(key, data));

    std::vector<ca_offset_score> new_offsets;
    ca_offset_score_parse(data, &new_offsets);

    callback(std::move(new_offsets));
  }
}

void LookupIndexKey(
    const std::vector<std::unique_ptr<Table>>& index_tables,
    const char* token, bool make_headers,
    std::function<void(std::vector<ca_offset_score>)>&& callback) {
  auto delimiter = strchr(token, ':');

  if (delimiter > token + 3 && !memcmp(delimiter - 3, "-in", 3)) {
    // The "FIELD-in:KEY" keyword retrieves an object from CAS using the
    // provided key, and extracts DNS names from that object.  Each of these
    // names are looked up with the "FIELD" prefix, and added to the results.

    // The file is also scanned for headers on the format "{HEADER}", which
    // are inserted into the response objects.

    std::string field(token, delimiter - 3);
    field.push_back(':');

    if (field == "links:") field = "name:";
    // TODO(mortehu): Actually make this work with more than one data type.

    std::string key = delimiter + 1;
    if (!cas_client) CreateCASClient();
    auto data = cas_client->Get(key);

    std::unordered_map<std::string, std::pair<std::string, std::string>> names;
    auto add_name = [&names](std::string name, const std::string& header,
                             const std::string& header_key) {
      if (HasPrefix(name, "www.")) name.erase(0, 4);
      if (IsValidDomainName(name))
        names.emplace(std::move(name), std::make_pair(header, header_key));
    };

    std::string name, header, header_key;
    bool in_header = false;
    size_t header_idx = 0;
    for (auto ch : data) {
      if (in_header) {
        if (std::isalnum(ch) || strchr(" .,_&-", ch)) {
          header.push_back(ch);
        } else if (ch == '}') {
          header_key = StringPrintf("%06zu", header_idx++);
          in_header = false;
        } else {
          header.clear();
          in_header = false;
        }
      } else if (ch == '{') {
        in_header = true;
        header.clear();
      } else if (std::isalnum(ch) || ch == '.' || ch == '-') {
        name.push_back(std::tolower(ch));
      } else if (!name.empty()) {
        add_name(std::move(name), header, header_key);
        assert(name.empty());
      }
    }
    if (!name.empty()) add_name(std::move(name), header, header_key);

    // The UnionOffsets function is expensive if one of the arrays is big, so
    // we use an std::set to get unique sorted elements instead.
    std::set<uint64_t> offset_buffer;

    // Look up one "name:X" token per potential hostname found.
    for (const auto& name : names) {
      LookupIndexKey(
          index_tables, (field + name.first).c_str(),
          [&name, &header_key, &offset_buffer, make_headers](auto new_offsets) {
            for (const auto& offset : new_offsets) {
              offset_buffer.emplace(offset.offset);

              // Record headers.
              if (!name.second.first.empty() && !make_headers) {
                extra_data[offset.offset]["_header"] =
                    Json::Value(name.second.first);
                extra_data[offset.offset]["_header_key"] =
                    Json::Value(name.second.second);
              }
            }
          });
    }

    std::vector<ca_offset_score> tmp;
    for (auto offset : offset_buffer) tmp.emplace_back(offset, 0.0f);
    callback(std::move(tmp));
  } else if (!strncmp(token, "in-", 3)) {
    auto delimiter = strchr(token + 3, ':');

    if (!delimiter) return;

    string_view key(token + 3, delimiter - (token + 3));
    string_view parameter(delimiter + 1);

    // The UnionOffsets function is expensive if one of the arrays is big, so
    // we use an std::set to get unique sorted elements instead.
    std::set<uint64_t> offset_buffer;

    for (size_t i = 0; i < index_tables.size(); ++i) {
      index_tables[i]->Seek(0, SEEK_SET);

      // Seek to first key in range.
      index_tables[i]->SeekToKey(key);

      string_view row_key, data;
      while (index_tables[i]->ReadRow(row_key, data)) {
        std::vector<ca_offset_score> new_offsets;

        if (!HasPrefix(row_key, key)) {
          if (row_key < key)
            continue;
          break;
        }

        if (row_key.end() == std::search(row_key.begin(),
          row_key.end(), parameter.begin(), parameter.end(),
          [] (const auto &c1, const auto &c2) {
            return std::tolower(c1) == std::tolower(c2); }
          )) continue;

        ca_offset_score_parse(data, &new_offsets);

        for (const auto& offset : new_offsets)
          offset_buffer.emplace(offset.offset);
      }
    }

    std::vector<ca_offset_score> tmp;
    for (auto offset : offset_buffer) tmp.emplace_back(offset, 0.0f);
    callback(std::move(tmp));
  } else {
    LookupIndexKey(index_tables, token, std::move(callback));
  }
}

size_t SubtractOffsets(struct ca_offset_score* lhs, size_t lhs_count,
                       const struct ca_offset_score* rhs, size_t rhs_count) {
  // We can't use std::set_difference() here, because it will not delete
  // duplicate offsets from `lhs' unless the same duplicate count exists in
  // `rhs'.

  struct ca_offset_score* output, *o;
  const struct ca_offset_score* lhs_end, *rhs_end;

  output = o = lhs;

  lhs_end = lhs + lhs_count;
  rhs_end = rhs + rhs_count;

  while (lhs != lhs_end && rhs != rhs_end) {
    if (lhs->offset == rhs->offset) {
      do
        ++lhs;
      while (lhs != lhs_end && lhs->offset == rhs->offset);

      ++rhs;

      continue;
    }

    if (lhs->offset < rhs->offset)
      *o++ = *lhs++;
    else
      ++rhs;
  }

  while (lhs != lhs_end) *o++ = *lhs++;

  return o - output;
}

void ProcessSubQuery(std::vector<ca_offset_score>& offsets, const Query* query,
                     Schema* schema, bool make_headers) {
  switch (query->type) {
    case kQueryKey: {
      string_view key(query->identifier);
      for (const auto& st : schema->summary_tables) {
        if (st.second->SeekToKey(key)) {
          offsets.emplace_back(st.second->Offset() + std::get<uint64_t>(st),
                               0.0f);
          break;
        }
      }
    } break;

    case kQueryLeaf:
      LookupIndexKey(
          schema->IndexTables(), query->identifier, make_headers,
          [&offsets](auto new_offsets) { offsets = std::move(new_offsets); });
      break;

    case kQueryBinaryOperator:
      ProcessSubQuery(offsets, query->lhs, schema, make_headers);

      switch (query->operator_type) {
        case kOperatorOr: {
          if (offsets.empty()) {
            ProcessSubQuery(offsets, query->rhs, schema, make_headers);
          } else {
            std::vector<ca_offset_score> rhs;
            ProcessSubQuery(rhs, query->rhs, schema, make_headers);

            offsets = UnionOffsets(offsets, rhs);
          }
        } break;

        case kOperatorAnd: {
          if (offsets.empty()) return;

          std::vector<ca_offset_score> rhs;
          ProcessSubQuery(rhs, query->rhs, schema, make_headers);

          const auto new_size = IntersectOffsets(offsets.data(), offsets.size(),
                                                 rhs.data(), rhs.size());
          offsets.resize(new_size);
        } break;

        case kOperatorSubtract: {
          if (offsets.empty()) return;

          std::vector<ca_offset_score> rhs;
          ProcessSubQuery(rhs, query->rhs, schema, make_headers);

          const auto new_size = SubtractOffsets(
              offsets.data(), offsets.size(), rhs.data(), rhs.size());
          offsets.resize(new_size);
        } break;

        case kOperatorEQ:
          offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                       [value = query->value](const auto& v) {
                          return !(v.score == value);
                        }),
                        offsets.end());
          break;

        case kOperatorGT:
          if (query->rhs) {
            std::vector<ca_offset_score> rhs;
            ProcessSubQuery(rhs, query->rhs, schema, make_headers);

            Join(offsets, rhs,
                 [](const auto lhs, const auto rhs) { return lhs > rhs; });
          } else {
            offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                         [value = query->value](const auto& v) {
                            return !(v.score > value);
                          }),
                          offsets.end());
          }
          break;

        case kOperatorGE:
          offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                       [value = query->value](const auto& v) {
                          return !(v.score >= value);
                        }),
                        offsets.end());
          break;

        case kOperatorLT:
          if (query->rhs) {
            std::vector<ca_offset_score> rhs;
            ProcessSubQuery(rhs, query->rhs, schema, make_headers);

            Join(offsets, rhs,
                 [](const auto lhs, const auto rhs) { return lhs < rhs; });
          } else {
            offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                         [value = query->value](const auto& v) {
                            return !(v.score < value);
                          }),
                          offsets.end());
          }
          break;

        case kOperatorLE:
          offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                       [value = query->value](const auto& v) {
                          return !(v.score <= value);
                        }),
                        offsets.end());
          break;

        case kOperatorInRange: {
          auto low = query->value;
          auto high = query->value2;
          if (low > high) std::swap(low, high);
          offsets.erase(std::remove_if(offsets.begin(), offsets.end(),
                                       [low, high](const auto& v) {
                          return !(v.score >= low && v.score <= high);
                        }),
                        offsets.end());
        } break;

        case kOperatorOrderBy: {
          std::vector<ca_offset_score> rhs;
          ProcessSubQuery(rhs, query->rhs, schema, make_headers);

          auto l = offsets.begin();
          auto r = rhs.begin();

          while (l != offsets.end() && r != rhs.end()) {
            if (l->offset < r->offset) {
              l->score = -HUGE_VAL;
              ++l;
              continue;
            }

            if (r->offset < l->offset) {
              ++r;
              continue;
            }

            l->score = r->score;
            ++l;
            ++r;
          }

          while (l != offsets.end()) {
            l->score = -HUGE_VAL;
            ++l;
          }
        } break;

        case kOperatorRandomSample: {
          const auto count = static_cast<size_t>(query->value);

          if (offsets.size() <= count) break;

          std::mt19937_64 rng(1234);

          for (size_t i = count; i < offsets.size(); ++i) {
            std::uniform_int_distribution<size_t> dist(0, i);
            const auto j = dist(rng);
            if (j < count) std::swap(offsets[i], offsets[j]);
          }

          offsets.resize(count);

          std::sort(offsets.begin(), offsets.end(),
                    [](const auto& lhs,
                       const auto& rhs) { return lhs.offset < rhs.offset; });
        } break;

        default:
          KJ_FAIL_REQUIRE("Unsupported operator type", query->operator_type);
      }
      break;

    case kQueryUnaryOperator:
      ProcessSubQuery(offsets, query->lhs, schema, make_headers);

      switch (query->operator_type) {
        case kOperatorMax:
          if (!offsets.empty()) {
            size_t o = 1;
            for (size_t i = 1; i < offsets.size(); ++i) {
              if (offsets[i].offset != offsets[o - 1].offset) {
                offsets[o++] = offsets[i];
              } else {
                if (offsets[i].score > offsets[o - 1].score)
                  offsets[o - 1].score = offsets[i].score;
              }
            }
            offsets.resize(o);
          }
          break;

        case kOperatorMin:
          if (!offsets.empty()) {
            size_t o = 1;
            for (size_t i = 1; i < offsets.size(); ++i) {
              if (offsets[i].offset != offsets[o - 1].offset) {
                offsets[o++] = offsets[i];
              } else {
                if (offsets[i].score < offsets[o - 1].score)
                  offsets[o - 1].score = offsets[i].score;
              }
            }
            offsets.resize(o);
          }
          break;

        case kOperatorNegate:
          for (auto& o : offsets) o.score = -o.score;
          break;

        default:
          KJ_FAIL_REQUIRE("Unsupported operator type", query->operator_type);
      }

      break;

    default:
      KJ_FAIL_REQUIRE("Unsupported query type", query->type);
  }
}

void ProcessQuery(std::vector<ca_offset_score>& offsets, const Query* query,
                  Schema* schema, bool make_headers, bool use_max) {
  ProcessSubQuery(offsets, query, schema, make_headers);
  RemoveDuplicates(offsets, use_max);
}

void PrintQuery(const Query* query) {
  switch (query->type) {
    case kQueryKey:
      printf("KEY=%s", query->identifier);
      break;

    case kQueryLeaf:
      printf("%s", query->identifier);
      break;

    case kQueryUnaryOperator:
      switch (query->operator_type) {
        case kOperatorMax:
          printf("MAX(");
          PrintQuery(query->lhs);
          break;

        case kOperatorMin:
          printf("MIN(");
          PrintQuery(query->lhs);
          break;

        case kOperatorNegate:
          printf("~(");
          PrintQuery(query->lhs);
          printf(")");
          break;

        default:
          KJ_FAIL_ASSERT("invalid operator", query->operator_type);
      }
      break;

    case kQueryBinaryOperator:
      if (query->operator_type == kOperatorRandomSample) {
        printf("RANDOM_SAMPLE(");
        PrintQuery(query->lhs);
        printf(", %.9g)", query->value);
        break;
      }

      printf("(");
      PrintQuery(query->lhs);
      bool scalar_rhs = false;
      bool range_rhs = false;
      switch (query->operator_type) {
        case kOperatorOr:
          printf(" + ");
          break;
        case kOperatorAnd:
          printf(" AND ");
          break;
        case kOperatorSubtract:
          printf(" - ");
          break;
        case kOperatorEQ:
          printf("=");
          if (!query->rhs) scalar_rhs = true;
          break;
        case kOperatorGT:
          printf(">");
          if (!query->rhs) scalar_rhs = true;
          break;
        case kOperatorGE:
          printf(">=");
          if (!query->rhs) scalar_rhs = true;
          break;
        case kOperatorLT:
          printf("<");
          if (!query->rhs) scalar_rhs = true;
          break;
        case kOperatorLE:
          printf("<=");
          if (!query->rhs) scalar_rhs = true;
          break;
        case kOperatorInRange:
          range_rhs = true;
          break;
        case kOperatorOrderBy:
          printf(" ORDER BY ");
          break;

        default:
          KJ_FAIL_ASSERT("invalid operator", query->operator_type);
      }
      if (range_rhs)
        printf("[%.9g,%.9g]", query->value, query->value2);
      else if (scalar_rhs)
        printf("%.9g", query->value);
      else
        PrintQuery(query->rhs);
      printf(")");
      break;
  }
}

void ca_schema_query(Schema* schema,
                     const struct query_statement& stmt) {
  try {
    schema->Load();

    std::vector<ca_offset_score> offsets;

    std::string key_buffer;

    auto& summary_tables = schema->summary_tables;
    auto& index_tables = schema->IndexTables();
    auto& summary_override_tables = schema->summary_override_tables;

    KJ_REQUIRE(!summary_tables.empty());

    ProcessQuery(offsets, stmt.query, schema,
                           stmt.thresholds != nullptr);

    std::vector<double> thresholds;
    bool reverse_thresholds = false;

    // Set to true if the threshold key is an event list, so that the section
    // heads should be date ranges rather than number ranges.
    bool use_date_headers = false;

    if (stmt.thresholds) {
      // The caller has provided a group of score thresholds for grouping the
      // search results.
      for (auto th = stmt.thresholds->values; th; th = th->next)
        thresholds.emplace_back(th->value);
      std::sort(thresholds.begin(), thresholds.end());

      auto key = stmt.thresholds->key;
      if (*key == '~') {
        ++key;
        reverse_thresholds = true;
      }

      if (Keywords::GetInstance().IsTimestamped(key))
        use_date_headers = true;

      // Filter `offsets' array by offsets within range.
      LookupIndexKey(index_tables, key, [&offsets, &thresholds](auto values) {
        auto output = offsets.begin();

        auto thr_iter = values.begin();
        auto off_iter = offsets.begin();
        auto thr_end = values.end();
        auto off_end = offsets.end();

        while (thr_iter != thr_end && off_iter != off_end) {
          if (thr_iter->offset == off_iter->offset) {
            if (thr_iter->score >= thresholds.front() &&
                thr_iter->score < thresholds.back()) {
              output->offset = thr_iter->offset;
              output->score = thr_iter->score;
              ++output;
            }
            ++thr_iter;
            continue;
          }

          if (thr_iter->offset < off_iter->offset)
            ++thr_iter;
          else
            ++off_iter;
        }

        offsets.erase(output, offsets.end());
      });
    }

    if (stmt.offset >= offsets.size()) {
      printf("[]\n");
      return;
    }

    size_t limit = stmt.limit;

    if (stmt.limit < 0 || stmt.offset + limit > offsets.size())
      limit = offsets.size() - stmt.offset;

    std::partial_sort(
        offsets.begin(), offsets.begin() + stmt.offset + limit, offsets.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });

    if (stmt.keys_only) {
      for (auto i = stmt.offset; i < stmt.offset + limit; ++i) {
        const auto& v = offsets[i];

        auto summary_table_idx = summary_tables.size();

        while (--summary_table_idx &&
               std::get<uint64_t>(summary_tables[summary_table_idx]) > v.offset)
          ;

        summary_tables[summary_table_idx].second->Seek(
            v.offset - std::get<uint64_t>(summary_tables[summary_table_idx]),
            SEEK_SET);

        string_view row_key, data;
        KJ_REQUIRE(summary_tables[summary_table_idx].second->ReadRow(
            row_key, data));

        printf("%.*s\n", static_cast<int>(row_key.size()), row_key.data());
      }
    } else {
      // First, order the results by their physical location in the `summaries`
      // table, to minimize the total seek distance in rotational storage.
      std::vector<std::pair<ca_offset_score, size_t>> sorted_offsets;
      for (auto i = stmt.offset; i < stmt.offset + limit; ++i)
        sorted_offsets.emplace_back(offsets[i], i - stmt.offset);
      std::sort(sorted_offsets.begin(), sorted_offsets.end(),
                [](const auto& lhs, const auto& rhs) {
        return lhs.first.offset < rhs.first.offset;
      });

      std::vector<std::string> results;
      results.resize(sorted_offsets.size());

      for (const auto& o : sorted_offsets) {
        const auto& v = o.first;

        auto summary_table_idx = summary_tables.size();

        while (--summary_table_idx &&
               std::get<uint64_t>(summary_tables[summary_table_idx]) > v.offset)
          ;

        summary_tables[summary_table_idx].second->Seek(
            v.offset - std::get<uint64_t>(summary_tables[summary_table_idx]),
            SEEK_SET);

        string_view row_key, data;
        KJ_REQUIRE(summary_tables[summary_table_idx].second->ReadRow(row_key, data));
        KJ_REQUIRE(row_key.size() < 100'000'000, row_key.size());
        KJ_REQUIRE(data.size() < 100'000'000, data.size());

        std::string result;
        result.append("\"_key\":");
        ToJSON(row_key, result);

        result.push_back(',');
        string_view json(data);
        // TODO(mortehu): Remove this logic when we're no longer producing
        // summaries with curly braces in them.
        if (json[0] == '{') {
          KJ_ASSERT(json.size() > 2);
          result.append(json.data() + 1, json.size() - 2);
        } else {
          result.append(json.data(), json.size());
        }

        for (auto& summary_override_table : summary_override_tables) {
          if (!summary_override_table->SeekToKey(row_key)) break;

          string_view tmp_key, json_extra;
          KJ_REQUIRE(summary_override_table->ReadRow(tmp_key, json_extra));

          result.push_back(',');
          // TODO(mortehu): Remove this logic when we're no longer producing
          // summaries with curly braces in them.
          if (json_extra[0] == '{')
            result.append(json_extra.data() + 1, json_extra.size() - 2);
          else
            result.append(json_extra.data(), json_extra.size());
        }

        auto ed = extra_data.find(v.offset);
        if (ed != extra_data.end()) {
          result.push_back(',');
          auto extra_json = Json::FastWriter().write(ed->second);
          if (std::isspace(extra_json.back())) extra_json.pop_back();
          result.append(extra_json.data() + 1, extra_json.size() - 2);
        }

        if (stmt.thresholds) {
          // The score is known to be within range from earlier tests.
          auto i = std::lower_bound(thresholds.begin() + 1, thresholds.end(),
                                    v.score);
          if (*i == v.score && i + 1 < thresholds.end()) ++i;
          const auto min_value = *(i - 1);
          const auto max_value = *i;
          std::string header;
          if (!use_date_headers) {
            header = DoubleToString(min_value) + "–" + DoubleToString(max_value);
          } else if (min_value + 1 != max_value) {
            header = TimeToDateString(min_value) + "–" + TimeToDateString(max_value);
          } else {
            header = TimeToDateString(min_value);
          }
          auto key = i - thresholds.begin();
          if (reverse_thresholds) key = thresholds.size() - key;
          result.append(",\"_header\":");
          ToJSON(header, result);

          // Make a key on the form "AAAAA".."ZZZZZ", so that a client can sort
          // the headers easily, without parsing them.
          result.append(",\"_header_key\":\"");
          for (auto j = 26*26*26*26; j > 0; j /= 26)
            result.push_back('A' + (key / j) % 26);
          result.push_back('\"');
        }

        results[o.second] = std::move(result);
      }

      printf("{\"result-count\":%zu,\"result\":[{", offsets.size());

      for (size_t i = 0; i < results.size(); ++i) {
        if (i > 0) fwrite_unlocked("},\n{", 1, 4, stdout);

        fwrite_unlocked(results[i].data(), 1, results[i].size(), stdout);
      }

      printf("}]}\n");
    }
  } catch (kj::Exception e) {
    Json::Value error;
    error["error"] = e.getDescription().cStr();
    std::cout << Json::FastWriter().write(error);
  }
}

}  // namespace table
}  // namespace cantera
