/*
    Low-level data parser for Cantera Table
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

#include <assert.h>
#include <string.h>

#include <err.h>
#include <sysexits.h>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/context.h"
#include "src/rle.h"

#include "third_party/oroch/oroch/integer_codec.h"

namespace cantera {
namespace table {

namespace {

void InitRLE(struct CA_rle_context* ctx, const uint8_t* input) {
  ctx->data = (uint8_t*)input;
  ctx->run = 0;
}

uint8_t ReadRLEByte(struct CA_rle_context* ctx) {
  if (ctx->run) {
    --ctx->run;
  } else if (0xc0 == (0xc0 & ctx->data[0])) {
    ctx->run = ctx->data[0] & 0x3f;
    ctx->data += 2;
  } else
    ctx->data += 1;

  return ctx->data[-1];
}

}  // namespace

uint64_t ca_parse_integer(const uint8_t** input) {
  const uint8_t* i;
  uint64_t result = 0;

  i = *input;

  result = *i & 0x7F;

  while (0 != (*i & 0x80)) {
    result <<= 7;
    result |= *++i & 0x7F;
  }

  *input = ++i;

  return result;
}

const char* ca_parse_string(const uint8_t** input) {
  const char* result;

  result = (const char*)*input;

  *input = (const uint8_t*)strchr(result, 0) + 1;

  return result;
}

void ParseOffsetScoreFlexi(const uint8_t*& begin, const uint8_t* end,
                           std::vector<ca_offset_score>* output) {
  auto base_index = output->size();
  auto count = ca_parse_integer(&begin);

  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return;
  }

  output->resize(base_index + count);
  auto values = &(*output)[base_index];

  struct CA_rle_context rle;
  uint8_t score_flags;
  uint32_t min_score = 0;
  size_t parse_score_count;

  values[0].offset = ca_parse_integer(&begin);

  auto step_gcd = ca_parse_integer(&begin);

  uint_fast32_t i;

  if (!step_gcd) {
    for (i = 1; i < count; ++i) values[i].offset = values[0].offset;
  } else {
    auto min_step = ca_parse_integer(&begin);
    auto max_step = ca_parse_integer(&begin) + min_step;

    if (min_step == max_step) {
      for (i = 1; i < count; ++i)
        values[i].offset = values[i - 1].offset + step_gcd * min_step;
    } else if (max_step - min_step <= 0x0f) {
      InitRLE(&rle, begin);

      for (i = 1; i < count; i += 2) {
        uint8_t tmp;

        tmp = ReadRLEByte(&rle);

        values[i].offset =
            values[i - 1].offset + step_gcd * (min_step + (tmp & 0x0f));

        if (i + 1 < count)
          values[i + 1].offset =
              values[i].offset + step_gcd * (min_step + (tmp >> 4));
      }

      assert(!rle.run);
      begin = rle.data;
    } else if (max_step - min_step <= 0xff) {
      InitRLE(&rle, begin);

      for (i = 1; i < count; ++i)
        values[i].offset =
            values[i - 1].offset + step_gcd * (min_step + ReadRLEByte(&rle));

      assert(!rle.run);
      begin = rle.data;
    } else {
      for (i = 1; i < count; ++i)
        values[i].offset = values[i - 1].offset +
                           step_gcd * (min_step + ca_parse_integer(&begin));
    }
  }

  score_flags = *begin++;

  if (score_flags & 3) min_score = ca_parse_integer(&begin);

  parse_score_count = (0 != (score_flags & 0x80)) ? 1 : count;

  switch (score_flags & 0x03) {
    case 0x00:
      for (i = 0; i < parse_score_count; ++i) {
        memcpy(&values[i].score, begin, sizeof(float));
        begin += sizeof(float);
      }
      break;

    case 0x01:
      for (i = 0; i < parse_score_count; ++i) {
        values[i].score = min_score + begin[0];
        ++begin;
      }
      break;

    case 0x02:
      for (i = 0; i < parse_score_count; ++i) {
        values[i].score = min_score + (begin[0] << 8) + begin[1];
        begin += 2;
      }
      break;

    case 0x03:
      for (i = 0; i < parse_score_count; ++i) {
        values[i].score =
            min_score + (begin[0] << 16) + (begin[1] << 8) + begin[2];
        begin += 3;
      }
      break;
  }

  for (; i < count; ++i) values[i].score = values[0].score;
}

size_t CountOffsetScoreFlexi(const uint8_t*& begin, const uint8_t* end) {
  auto count = ca_parse_integer(&begin);

  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return count;
  }

  struct CA_rle_context rle;
  uint8_t score_flags;
  size_t parse_score_count;

  ca_parse_integer(&begin);

  auto step_gcd = ca_parse_integer(&begin);

  if (step_gcd) {
    auto min_step = ca_parse_integer(&begin);
    auto max_step = ca_parse_integer(&begin) + min_step;

    if (min_step == max_step) {
      // Do nothing
    } else if (max_step - min_step <= 0x0f) {
      InitRLE(&rle, begin);

      for (size_t i = 1; i < count; i += 2) {
        ReadRLEByte(&rle);
      }

      assert(!rle.run);
      begin = rle.data;
    } else if (max_step - min_step <= 0xff) {
      InitRLE(&rle, begin);

      for (size_t i = 1; i < count; ++i) ReadRLEByte(&rle);

      assert(!rle.run);
      begin = rle.data;
    } else {
      for (size_t i = 1; i < count; ++i) ca_parse_integer(&begin);
    }
  }

  score_flags = *begin++;

  if (score_flags & 3) ca_parse_integer(&begin);

  parse_score_count = (0 != (score_flags & 0x80)) ? 1 : count;

  switch (score_flags & 0x03) {
    case 0x00:
      begin += parse_score_count * sizeof(float);
      break;

    case 0x01:
      begin += parse_score_count;
      break;

    case 0x02:
      begin += parse_score_count * 2;
      break;

    case 0x03:
      begin += parse_score_count * 3;
      break;
  }

  return count;
}

void ParseOffsetScoreOroch(const uint8_t*& begin, const uint8_t* end,
                           std::vector<ca_offset_score>* output,
                           bool integer_score) {
  auto base_index = output->size();

  // Get the number of encoded offset/score records.
  std::size_t count = 0;
  oroch::varint_codec<size_t>::value_decode(count, begin);
  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return;
  }

  const std::unordered_set<std::uint64_t>* filter = nullptr;
  auto* context = internal::Context::Get();
  if (context->UseFilter()) {
    filter = &context->GetFilter();
    output->reserve(base_index + filter->size());
  } else {
    output->resize(base_index + count);
  }

  // Get the first encoded offset.
  uint64_t offset = 0;
  oroch::varint_codec<uint64_t>::value_decode(offset, begin);

  // Get delta values for offsets.
  std::vector<uint64_t> offset_delta(count - 1);
  oroch::integer_codec<uint64_t>::metadata offset_meta;
  offset_meta.decode(begin);
  auto offset_i = offset_delta.begin();
  auto offset_e = offset_delta.end();
  oroch::integer_codec<uint64_t>::decode(offset_i, offset_e, begin,
                                         offset_meta);

  if (integer_score) {
    // Decode score values.
    std::vector<int64_t> score(count);
    oroch::integer_codec<int64_t>::metadata score_meta;
    score_meta.decode(begin);
    auto score_i = score.begin();
    auto score_e = score.end();
    oroch::integer_codec<int64_t>::decode(score_i, score_e, begin, score_meta);

    if (filter == nullptr) {
      // Store the first offset-score pair.
      auto values = &(*output)[base_index];
      values[0].offset = offset;
      values[0].score = score[0];
      // Store the rest.
      for (size_t i = 1; i < count; i++) {
        // Turn a delta value into the original offset.
        offset += offset_delta[i - 1];
        // Store the current pair.
        values[i].offset = offset;
        values[i].score = score[i];
      }
    } else {
      // Store the first offset-score pair.
      if (filter->count(offset)) output->emplace_back(offset, score[0]);
      // Store the rest.
      for (size_t i = 1; i < count; i++) {
        // Turn a delta value into the original offset.
        offset += offset_delta[i - 1];
        // Store the current pair.
        if (filter->count(offset)) output->emplace_back(offset, score[i]);
      }
    }
  } else {
    if (filter == nullptr) {
      // Store the first offset-score pair (the score is possibly unaligned).
      auto values = &(*output)[base_index];
      values[0].offset = offset;
      memcpy(&values[0].score, begin, sizeof(float));
      begin += sizeof(float);
      // Store the rest.
      for (size_t i = 1; i < count; i++) {
        // Turn a delta value into the original offset.
        offset += offset_delta[i - 1];
        // Store the current pair (the score is possibly unaligned).
        values[i].offset = offset;
        memcpy(&values[i].score, begin, sizeof(float));
        begin += sizeof(float);
      }
    } else {
      std::size_t index = 0;
      // Store the first offset-score pair (the score is possibly unaligned).
      if (filter->count(offset)) {
        ca_offset_score value;
        value.offset = offset;
        memcpy(&value.score, begin, sizeof(float));
        output->push_back(value);
      }
      begin += sizeof(float);
      // Store the rest.
      for (size_t i = 1; i < count; i++) {
        // Turn a delta value into the original offset.
        offset += offset_delta[i - 1];
        // Store the current pair (the score is possibly unaligned).
        if (filter->count(offset)) {
          ca_offset_score value;
          value.offset = offset;
          memcpy(&value.score, begin, sizeof(float));
          output->push_back(value);
        }
        begin += sizeof(float);
      }
    }
  }
}

uint64_t GetMaxOffsetOroch(const uint8_t*& begin, const uint8_t* end,
                           bool integer_score) {
  // Get the number of encoded offset/score records.
  size_t count = 0;
  oroch::varint_codec<size_t>::value_decode(count, begin);
  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return 0;
  }

  // Get the first encoded offset.
  uint64_t offset = 0;
  oroch::varint_codec<uint64_t>::value_decode(offset, begin);

  // Get delta values for offsets.
  std::vector<uint64_t> offset_delta(count - 1);
  oroch::integer_codec<uint64_t>::metadata offset_meta;
  offset_meta.decode(begin);
  auto offset_i = offset_delta.begin();
  auto offset_e = offset_delta.end();
  oroch::integer_codec<uint64_t>::decode(offset_i, offset_e, begin,
                                         offset_meta);

  // Convert delta values to original offset values.
  for (size_t i = 1; i < count; i++) offset += offset_delta[i - 1];

  // Skip score values.
  if (integer_score) {
    std::vector<int64_t> score(count);
    oroch::integer_codec<int64_t>::metadata score_meta;
    score_meta.decode(begin);
    auto score_i = score.begin();
    auto score_e = score.end();
    oroch::integer_codec<int64_t>::decode(score_i, score_e, begin, score_meta);
  } else {
    begin += count * sizeof(float);
  }

  return offset;
}

size_t CountOffsetScoreOroch(const uint8_t*& begin, const uint8_t* end,
                             bool integer_score) {
  // Get the number of encoded offset/score records.
  size_t count = 0;
  oroch::varint_codec<size_t>::value_decode(count, begin);
  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return 0;
  }

  // Get the first encoded offset.
  uint64_t offset = 0;
  oroch::varint_codec<uint64_t>::value_decode(offset, begin);

  // Get delta values for offsets.
  std::vector<uint64_t> offset_delta(count - 1);
  oroch::integer_codec<uint64_t>::metadata offset_meta;
  offset_meta.decode(begin);
  auto offset_i = offset_delta.begin();
  auto offset_e = offset_delta.end();
  oroch::integer_codec<uint64_t>::decode(offset_i, offset_e, begin,
                                         offset_meta);

  // Skip score values.
  if (integer_score) {
    std::vector<int64_t> score(count);
    oroch::integer_codec<int64_t>::metadata score_meta;
    score_meta.decode(begin);
    auto score_i = score.begin();
    auto score_e = score.end();
    oroch::integer_codec<int64_t>::decode(score_i, score_e, begin, score_meta);
  } else {
    begin += count * sizeof(float);
  }

  return count;
}

void ParseOffsetScoreWithPrediction(const uint8_t*& begin, const uint8_t* end,
                                    std::vector<ca_offset_score>* output) {
  auto base_index = output->size();
  auto count = ca_parse_integer(&begin);

  if (!count) {
    KJ_REQUIRE(begin == end, "unexpected zero-sized offset/score array");
    return;
  }

  output->resize(base_index + count);
  (*output)[base_index].offset = ca_parse_integer(&begin);

  std::vector<uint64_t> steps;

  if (count > 1) {
    auto step_count = ca_parse_integer(&begin);
    KJ_REQUIRE(step_count <= count + 1, step_count, count);

    if (step_count > 0) {
      steps.resize(step_count);

      uint64_t prev_step = 0;

      for (auto& step : steps) {
        step = ca_parse_integer(&begin) + prev_step;
        prev_step = step;
      }
    }
  }

  if (!steps.empty()) {
    for (size_t i = 1; i < count; ++i) {
      auto step_index = ca_parse_integer(&begin);
      KJ_REQUIRE(step_index < steps.size(), step_index, steps.size());
      (*output)[base_index + i].offset =
          (*output)[base_index + i - 1].offset + steps[step_index];
    }
  } else {
    for (size_t i = 1; i < count; ++i) {
      (*output)[base_index + i].offset =
          (*output)[base_index + i - 1].offset + ca_parse_integer(&begin);
    }
  }

  std::vector<uint8_t> prob_mask;
  prob_mask.resize((count + 7) / 8);

  struct CA_rle_context rle;
  InitRLE(&rle, begin);
  for (auto& b : prob_mask) b = ReadRLEByte(&rle);

  KJ_REQUIRE(rle.run == 0, rle.run);
  begin = rle.data;

  for (size_t i = 0; i < count; ++i) {
    ca_offset_score& v = (*output)[base_index + i];

    memcpy(&v.score, begin, sizeof(float));
    begin += sizeof(float);

    if (0 != (prob_mask[i >> 3] & (1 << (i & 7)))) {
      memcpy(&v.score_pct5, begin, sizeof(float));
      begin += sizeof(float);
      memcpy(&v.score_pct25, begin, sizeof(float));
      begin += sizeof(float);
      memcpy(&v.score_pct75, begin, sizeof(float));
      begin += sizeof(float);
      memcpy(&v.score_pct95, begin, sizeof(float));
      begin += sizeof(float);
    }
  }
}

size_t CountOffsetScoreWithPrediction(const uint8_t*& begin,
                                      const uint8_t* end) {
  std::vector<ca_offset_score> tmp;

  ParseOffsetScoreWithPrediction(begin, end, &tmp);

  return tmp.size();
}

uint64_t GetMaxOffsetWithPrediction(const uint8_t*& begin, const uint8_t* end) {
  auto count = ca_parse_integer(&begin);
  KJ_REQUIRE(count > 0);

  auto result = ca_parse_integer(&begin);

  std::vector<uint64_t> steps;

  if (count > 1) {
    auto step_count = ca_parse_integer(&begin);
    KJ_REQUIRE(step_count <= count + 1, step_count, count);

    if (step_count > 0) {
      steps.resize(step_count);

      uint64_t prev_step = 0;

      for (auto& step : steps) {
        step = ca_parse_integer(&begin) + prev_step;
        prev_step = step;
      }
    }
  }

  if (!steps.empty()) {
    for (size_t i = 1; i < count; ++i) {
      auto step_index = ca_parse_integer(&begin);
      KJ_REQUIRE(step_index < steps.size(), step_index, steps.size());
      result += steps[step_index];
    }
  } else {
    for (size_t i = 1; i < count; ++i) result += ca_parse_integer(&begin);
  }

  std::vector<uint8_t> prob_mask;
  prob_mask.resize((count + 7) / 8);

  struct CA_rle_context rle;
  InitRLE(&rle, begin);
  for (auto& b : prob_mask) b = ReadRLEByte(&rle);
  begin = rle.data;

  KJ_REQUIRE(rle.run == 0, rle.run);

  for (size_t i = 0; i < count; ++i) {
    begin += sizeof(float);

    if (0 != (prob_mask[i >> 3] & (1 << (i & 7)))) begin += 4 * sizeof(float);
  }

  return result;
}

void ca_offset_score_parse(string_view input,
                           std::vector<ca_offset_score>* output) {
  while (!input.empty()) {
    auto begin = reinterpret_cast<const uint8_t*>(input.begin());
    auto end = reinterpret_cast<const uint8_t*>(input.end());
    auto begin_save = begin;
    uint64_t offset;
    float fscore;
    uint32_t uscore;
    uint32_t* pscore;

    auto type = static_cast<ca_offset_score_type>(*begin++);

    switch (type) {
      case CA_OFFSET_SCORE_WITH_PREDICTION:
        ParseOffsetScoreWithPrediction(begin, end, output);
        break;

      case CA_OFFSET_SCORE_FLEXI:
        ParseOffsetScoreFlexi(begin, end, output);
        break;

      case CA_OFFSET_SCORE_DELTA_OROCH_FLOAT:
        ParseOffsetScoreOroch(begin, end, output, false);
        break;

      case CA_OFFSET_SCORE_DELTA_OROCH_OROCH:
        ParseOffsetScoreOroch(begin, end, output, true);
        break;

      case CA_OFFSET_SCORE_SINGLE_FLOAT:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        pscore = reinterpret_cast<uint32_t*>(&fscore);
        *pscore = *reinterpret_cast<const uint32_t*>(begin);
        output->emplace_back(offset, fscore);
        begin += sizeof(float);
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_1:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        output->emplace_back(offset, uscore);
        break;

      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_1:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        output->emplace_back(offset, static_cast<int32_t>(~uscore));
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_2:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        uscore |= *begin++ << 8;
        output->emplace_back(offset, uscore);
        break;

      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_2:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        uscore |= *begin++ << 8;
        output->emplace_back(offset, static_cast<int32_t>(~uscore));
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_3:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        uscore |= *begin++ << 8;
        uscore |= *begin++ << 16;
        output->emplace_back(offset, uscore);
        break;

      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_3:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        uscore = *begin++;
        uscore |= *begin++ << 8;
        uscore |= *begin++ << 16;
        output->emplace_back(offset, static_cast<int32_t>(~uscore));
        break;

      case CA_OFFSET_SCORE_EMPTY:
        break;

      default:
        KJ_FAIL_REQUIRE("unknown offset score format", type);
    }

    input.remove_prefix(begin - begin_save);
  }
}

size_t ca_offset_score_count(const uint8_t* begin, const uint8_t* end) {
  size_t result = 0;

  while (begin < end) {
    uint64_t offset;

    auto type = static_cast<ca_offset_score_type>(*begin++);

    switch (type) {
      case CA_OFFSET_SCORE_WITH_PREDICTION:
        result += CountOffsetScoreWithPrediction(begin, end);
        break;

      case CA_OFFSET_SCORE_FLEXI:
        result += CountOffsetScoreFlexi(begin, end);
        break;

      case CA_OFFSET_SCORE_DELTA_OROCH_FLOAT:
        result += CountOffsetScoreOroch(begin, end, false);
        break;

      case CA_OFFSET_SCORE_DELTA_OROCH_OROCH:
        result += CountOffsetScoreOroch(begin, end, true);
        break;

      case CA_OFFSET_SCORE_SINGLE_FLOAT:
        result++;
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += sizeof(float);
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_1:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_1:
        result++;
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin++;
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_2:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_2:
        result++;
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += 2;
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_3:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_3:
        result++;
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += 3;
        break;

      case CA_OFFSET_SCORE_EMPTY:
        break;

      default:
        KJ_FAIL_REQUIRE("unknown offset score format", type);
    }
  }

  return result;
}

uint64_t ca_offset_score_max_offset(const uint8_t* begin, const uint8_t* end) {
  uint64_t result = 0;

  while (begin < end) {
    auto type = static_cast<ca_offset_score_type>(*begin++);
    uint64_t offset = 0;

    switch (type) {
      case CA_OFFSET_SCORE_WITH_PREDICTION:
        offset = GetMaxOffsetWithPrediction(begin, end);
        break;

      case CA_OFFSET_SCORE_FLEXI: {
        auto count = ca_parse_integer(&begin);
        if (count == 0) break;

        offset = ca_parse_integer(&begin);

        auto step_gcd = ca_parse_integer(&begin);

        if (step_gcd && count > 1) {
          auto min_step = ca_parse_integer(&begin);
          auto max_step = ca_parse_integer(&begin) + min_step;

          if (min_step == max_step) {
            offset += step_gcd * min_step * (count - 1);
          } else if (max_step - min_step <= 0x0f) {
            CA_rle_context rle;
            InitRLE(&rle, begin);

            for (size_t i = 1; i < count; i += 2) {
              uint8_t tmp;

              tmp = ReadRLEByte(&rle);

              offset += step_gcd * (min_step + (tmp & 0x0f));

              if (i + 1 < count) offset += step_gcd * (min_step + (tmp >> 4));
            }

            KJ_REQUIRE(!rle.run, rle.run);
            begin = rle.data;
          } else if (max_step - min_step <= 0xff) {
            CA_rle_context rle;
            InitRLE(&rle, begin);

            for (size_t i = 1; i < count; ++i)
              offset += step_gcd * (min_step + ReadRLEByte(&rle));

            KJ_REQUIRE(!rle.run, rle.run);
            begin = rle.data;
          } else {
            for (size_t i = 1; i < count; ++i)
              offset += step_gcd * (min_step + ca_parse_integer(&begin));
          }
        }

        auto score_flags = *begin++;

        if (score_flags & 3) ca_parse_integer(&begin);

        auto parse_score_count = (0 != (score_flags & 0x80)) ? 1 : count;

        switch (score_flags & 0x03) {
          case 0x00:
            begin += parse_score_count * sizeof(float);
            break;

          case 0x01:
            begin += parse_score_count;
            break;

          case 0x02:
            begin += parse_score_count * 2;
            break;

          case 0x03:
            begin += parse_score_count * 3;
            break;
        }

        KJ_REQUIRE(begin == end, begin, end);
      } break;

      case CA_OFFSET_SCORE_DELTA_OROCH_FLOAT:
        offset = GetMaxOffsetOroch(begin, end, false);
        break;

      case CA_OFFSET_SCORE_DELTA_OROCH_OROCH:
        offset = GetMaxOffsetOroch(begin, end, true);
        break;

      case CA_OFFSET_SCORE_SINGLE_FLOAT:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += sizeof(float);
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_1:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_1:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin++;
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_2:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_2:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += 2;
        break;

      case CA_OFFSET_SCORE_SINGLE_POSITIVE_3:
      case CA_OFFSET_SCORE_SINGLE_NEGATIVE_3:
        oroch::varint_codec<uint64_t>::value_decode(offset, begin);
        begin += 3;
        break;

      case CA_OFFSET_SCORE_EMPTY:
        break;

      default:
        KJ_FAIL_REQUIRE("unknown offset score format", type);
    }

    if (offset > result) result = offset;
  }

  return result;
}

}  // namespace table
}  // namespace cantera
