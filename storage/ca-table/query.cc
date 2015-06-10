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
#include <set>
#include <unordered_map>

#include <json/value.h>
#include <json/writer.h>
#include <kj/debug.h>

#include "base/async.h"
#include "base/cat.h"
#include "base/escape.h"
#include "base/macros.h"
#include "base/string.h"
#include "base/stringref.h"
#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"
#include "storage/ca-table/keywords.h"

namespace {

std::unordered_map<uint64_t, Json::Value> extra_data;

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

// Updates `lhs' with the union of the `lhs' and `rhs' arrays.  May also alter
// `rhs'.
void UnionOffsetsInplace(std::vector<ca_offset_score>& lhs,
                         std::vector<ca_offset_score>& rhs) {
  // Handle the trivial case where at least one of the vectors are empty.
  if (lhs.empty() || rhs.empty()) {
    if (lhs.empty()) lhs.swap(rhs);
    return;
  }

  auto merged = UnionOffsets(lhs, rhs);
  lhs.swap(merged);
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

size_t FilterOffsets(struct ca_offset_score* offsets, size_t count, int op,
                     float operand0, float operand1) {
  size_t i, result = 0;

  for (i = 0; i < count; ++i) {
    switch (op) {
      case '[':

        if (offsets[i].score < operand0 || offsets[i].score > operand1)
          continue;

        break;

      case L'≤':

        if (offsets[i].score > operand0) continue;

        break;

      case '<':

        if (offsets[i].score >= operand0) continue;

        break;

      case '=':

        if (offsets[i].score != operand0) continue;

        break;

      case '>':

        if (offsets[i].score <= operand0) continue;

        break;

      case L'≥':

        if (offsets[i].score < operand0) continue;

        break;
    }

    offsets[result++] = offsets[i];
  }

  return result;
}

float ParseValue(char* string, char** endptr) {
  float result;

  result = strtod(string, endptr);

  if (**endptr == '-') {
    struct tm tm;

    memset(&tm, 0, sizeof(tm));
    *endptr = strptime(string, "%Y-%m-%d", &tm);

    result = timegm(&tm) / 86400;
  }

  return result;
}

std::string Unquote(const ev::StringRef& str) {
  static const char kHexHelper[] = "0123456789abcdef";

  std::string result;
  int quote_char = 0;

  for (auto ch : str) {
    if (ch == quote_char) {
      quote_char = 0;
    } else if (quote_char) {
      switch (ch) {
        case '~':
        case '-':
        case '+':
        case ' ':
        case '%':
        case '\t':
          result.push_back('%');
          result.push_back(kHexHelper[static_cast<uint8_t>(ch) >> 4]);
          result.push_back(kHexHelper[static_cast<uint8_t>(ch) & 15]);
          break;
        default:
          result.push_back(ch);
      }
    } else {
      if (ch == '\'' || ch == '"') {
        quote_char = ch;
      } else {
        result.push_back(ch);
      }
    }
  }

  return result;
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

}  // namespace

namespace ca_table {

void LookupKey(const std::vector<ca_table::Table*>& index_tables,
               const char* key,
               std::function<void(std::vector<ca_offset_score>)>&& callback) {
  auto unescaped_key = ev::DecodeURIComponent(key);

  for (size_t i = 0; i < index_tables.size(); ++i) {
    if (!index_tables[i]->SeekToKey(unescaped_key)) continue;

    iovec key_iov, data_iov;
    KJ_REQUIRE(1 == index_tables[i]->ReadRow(&key_iov, &data_iov));

    std::vector<ca_offset_score> new_offsets;
    ca_offset_score_parse(data_iov, &new_offsets);

    callback(std::move(new_offsets));
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

void Query(std::vector<ca_offset_score>& ret_offsets, const char* query,
           const std::vector<ca_table::Table*>& index_tables, bool make_headers,
           bool use_max) {
  std::vector<ca_offset_score> offsets;
  ssize_t ret;
  int first = 1;

  struct iovec key_iov, data_iov;

  size_t i;

  std::unique_ptr<char[]> query_buf(new char[strlen(query) + 1]);
  strcpy(query_buf.get(), query);

  char* saveptr = nullptr;
  for (auto token = strtok_r(query_buf.get(), " \t", &saveptr); token;
       token = strtok_r(nullptr, " \t", &saveptr)) {
    std::vector<ca_offset_score> token_offsets;
    int invert_rank = 0, subtract = 0, add = 0;

    int op = 0;
    float operand0 = 0, operand1 = 0;

    if (*token == '-')
      ++token, subtract = 1;
    else if (*token == '+')
      ++token, add = 1;

    if (*token == '~') ++token, invert_rank = 1;

    char* ch;
    if (nullptr != (ch = strchr(token, '>')) ||
        nullptr != (ch = strchr(token, '<')) ||
        nullptr != (ch = strchr(token, '=')) ||
        nullptr != (ch = strchr(token, '['))) {
      char* endptr, *delimiter = NULL;
      op = *ch;
      *ch++ = 0;

      if (*ch == '=') {
        if (op == '>')
          op = L'≥';
        else if (op == '<')
          op = L'≤';

        ++ch;
      }

      if (op == '[' && NULL != (delimiter = strchr(ch, ','))) {
        *delimiter++ = 0;

        operand1 = ParseValue(delimiter, &endptr);
      }

      operand0 = ParseValue(ch, &endptr);
    }

    enum AggregateType { kMin, kMax, kNone };
    AggregateType aggregate = kNone;

    if (ev::HasSuffix(token, ")")) {
      if (ev::HasPrefix(token, "min(")) {
        aggregate = kMin;
        token += 4;
        token[strlen(token) - 1] = 0;  // Remove ')'
      } else if (ev::HasPrefix(token, "max(")) {
        aggregate = kMax;
        token += 4;
        token[strlen(token) - 1] = 0;  // Remove ')'
      }
    }

    if (!strncmp(token, "in-", 3)) {
      auto delimiter = strchr(token + 3, ':');

      if (!delimiter) {
        ret_offsets.clear();
        return;
      }

      ev::StringRef key(token + 3, delimiter);
      ev::StringRef parameter(delimiter + 1);

      // The UnionOffsets function is expensive if one of the arrays is big, so
      // we use an std::set to get unique sorted elements instead.
      std::set<uint64_t> offset_buffer;

      for (i = 0; i < index_tables.size(); ++i) {
        index_tables[i]->Seek(0, SEEK_SET);

        // Seek to first key in range.
        index_tables[i]->SeekToKey(key);

        while (1 == (ret = index_tables[i]->ReadRow(&key_iov, &data_iov))) {
          std::vector<ca_offset_score> new_offsets;

          ev::StringRef row_key(key_iov);

          if (!HasPrefix(row_key, key)) {
            if (row_key < key)
              continue;
            else
              break;
          }

          if (!row_key.contains(parameter)) continue;

          ca_offset_score_parse(data_iov, &new_offsets);

          for (const auto& offset : new_offsets)
            offset_buffer.emplace(offset.offset);
        }
      }

      for (auto offset : offset_buffer)
        token_offsets.emplace_back(offset, 0.0f);
    } else {
      LookupKey(index_tables, token, [&token_offsets](auto new_offsets) {
        UnionOffsetsInplace(token_offsets, new_offsets);
      });
    }

    switch (aggregate) {
      case kMin:
        RemoveDuplicates(token_offsets, false);
        break;
      case kMax:
        RemoveDuplicates(token_offsets, true);
        break;
      case kNone:
        break;
    }

    if (op) {
      token_offsets.resize(FilterOffsets(
          &token_offsets[0], token_offsets.size(), op, operand0, operand1));
    }

    if (invert_rank) {
      for (i = 0; i < token_offsets.size(); ++i)
        token_offsets[i].score = -token_offsets[i].score;
    }

    if (first) {
      offsets.swap(token_offsets);
    } else {
      if (subtract) {
        offsets.resize(ca_table::SubtractOffsets(&offsets[0], offsets.size(),
                                                 &token_offsets[0],
                                                 token_offsets.size()));
      } else if (add) {
        auto merged_offsets = UnionOffsets(offsets, token_offsets);
        offsets.swap(merged_offsets);
      } else {
        offsets.resize(IntersectOffsets(&offsets[0], offsets.size(),
                                        &token_offsets[0],
                                        token_offsets.size()));
      }
    }

    first = 0;
  }

  RemoveDuplicates(offsets, use_max);

  ret_offsets.swap(offsets);
}

}  // namespace ca_table

int ca_schema_query(struct ca_schema* schema,
                    const struct query_statement& stmt) {
  try {
    struct iovec key_iov, data_iov;

    std::vector<ca_offset_score> offsets;

    ssize_t ret;
    int result = -1;

    std::string key_buffer;

    ca_clear_error();

    uint64_t* summary_table_offsets;
    auto summary_tables =
        ca_schema_summary_tables(schema, &summary_table_offsets);
    KJ_REQUIRE(!summary_tables.empty());

    auto index_tables = ca_schema_index_tables(schema);

    auto summary_override_tables = ca_schema_summary_override_tables(schema);

    auto query = Unquote(stmt.query);

    ca_table::Query(offsets, query.c_str(), index_tables,
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

      if (ca_table::Keywords::GetInstance().IsTimestamped(key))
        use_date_headers = true;

      // Filter `offsets' array by offsets within range.
      LookupKey(index_tables, key, [&offsets, &thresholds](auto values) {
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
      return 0;
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
               summary_table_offsets[summary_table_idx] > v.offset)
          ;

        summary_tables[summary_table_idx]->Seek(
            v.offset - summary_table_offsets[summary_table_idx], SEEK_SET);

        if (1 != (ret = summary_tables[summary_table_idx]->ReadRow(
                      &key_iov, &data_iov))) {
          if (ret >= 0)
            ca_set_error("ca_table_read_row unexpectedly returned %d",
                         (int)ret);

          goto done;
        }

        printf("%.*s\n", static_cast<int>(key_iov.iov_len),
               reinterpret_cast<const char*>(key_iov.iov_base));
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
               summary_table_offsets[summary_table_idx] > v.offset)
          ;

        summary_tables[summary_table_idx]->Seek(
            v.offset - summary_table_offsets[summary_table_idx], SEEK_SET);

        KJ_REQUIRE(
            1 ==
            summary_tables[summary_table_idx]->ReadRow(&key_iov, &data_iov));

        std::string result;
        result.append("\"_key\":");
        ev::ToJSON(key_iov, result);

        result.push_back(',');
        ev::StringRef json(data_iov);
        // TODO(mortehu): Remove this logic when we're no longer producing
        // summaries with curly braces in them.
        if (json[0] == '{')
          result.append(json.data() + 1, json.size() - 2);
        else
          result.append(json.data(), json.size());

        for (auto summary_override_table : summary_override_tables) {
          if (!summary_override_table->SeekToKey(key_iov)) break;

          KJ_REQUIRE(1 == summary_override_table->ReadRow(&key_iov, &data_iov));

          result.push_back(',');
          ev::StringRef json_extra(data_iov);
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
          if (!use_date_headers || (min_value + 1 != max_value)) {
            header = ev::cat(ev::DoubleToString(min_value), "–",
                             ev::DoubleToString(max_value));
          } else {
            auto tt = static_cast<time_t>(min_value * 86400);
            struct tm t;
            gmtime_r(&tt, &t);
            char buf[64];
            KJ_REQUIRE(0 != strftime(buf, sizeof(buf), "%B %e, %Y", &t));
            header = buf;

            auto i = header.find("  ");
            if (i != std::string::npos) header.erase(i, 1);
          }
          auto key = i - thresholds.begin();
          if (reverse_thresholds) key = thresholds.size() - key;
          result.append(",\"_header\":");
          ev::ToJSON(header, result);

          // Make a key on the form "AAAAA".."ZZZZZ", so that a client can sort
          // the headers easily, without parsing them.
          result.append(",\"_header_key\":\"");
          for (auto j = ev::pow(26, 4); j > 0; j /= 26)
            result.push_back('A' + (key / j) % 26);
          result.push_back('\"');
        }

        results[o.second] = std::move(result);
      }

      printf("[{");

      for (size_t i = 0; i < results.size(); ++i) {
        if (i > 0) fwrite_unlocked("},\n{", 1, 4, stdout);

        fwrite_unlocked(results[i].data(), 1, results[i].size(), stdout);
      }

      printf("}]\n");
    }

    result = 0;

  done:

    return result;
  } catch (kj::Exception e) {
    Json::Value error;
    error["error"] = e.getDescription().cStr();
    std::cout << Json::FastWriter().write(error);

    return 0;
  }
}
