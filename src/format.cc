#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <cassert>
#include <cmath>
#include <cstdarg>
#include <cstring>
#include <set>
#include <unordered_map>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/rle.h"

#include "third_party/oroch/oroch/integer_codec.h"

namespace cantera {
namespace table {

namespace {

template <typename T>
T GCD(T a, T b) {
  while (b) {
    auto tmp = b;
    b = a % b;
    a = tmp;
  }

  return a;
}

void EncodeFloat(uint8_t*& output, float value) {
  memcpy(output, &value, sizeof(value));

  output += sizeof(value);
}

void EncodeOffsetScoreSingle(uint8_t*& o, uint8_t* oe,
                             const struct ca_offset_score* values) {
  //
  // NB: small negative values are converted to positive with bitwise
  // inversion, not arithmetic negation, so -1 gets converted to 0 and
  // -256 to 255.
  //

  // Choose the score representation.
  ca_offset_score_type type = CA_OFFSET_SCORE_SINGLE_FLOAT;
  int64_t score = llrintf(values[0].score);
  if (float(score) == values[0].score) {
    if (score >= 0) {
      if (score <= 0xff) {
        type = CA_OFFSET_SCORE_SINGLE_POSITIVE_1;
      } else if (score <= 0xffff) {
        type = CA_OFFSET_SCORE_SINGLE_POSITIVE_2;
      } else if (score <= 0xffffff) {
        type = CA_OFFSET_SCORE_SINGLE_POSITIVE_3;
      }
    } else {
      score = ~score;
      if (score <= 0xff) {
        type = CA_OFFSET_SCORE_SINGLE_NEGATIVE_1;
      } else if (score <= 0xffff) {
        type = CA_OFFSET_SCORE_SINGLE_NEGATIVE_2;
      } else if (score <= 0xffffff) {
        type = CA_OFFSET_SCORE_SINGLE_NEGATIVE_3;
      }
    }
  }

  // Store the chosen representation.
  *o++ = uint8_t(type);

  // Store the offset.
  oroch::varint_codec<uint64_t>::value_encode(o, values[0].offset);

  // Store the score.
  switch (type) {
    case CA_OFFSET_SCORE_SINGLE_POSITIVE_1:
    case CA_OFFSET_SCORE_SINGLE_NEGATIVE_1:
      *o++ = score;
      break;
    case CA_OFFSET_SCORE_SINGLE_POSITIVE_2:
    case CA_OFFSET_SCORE_SINGLE_NEGATIVE_2:
      *o++ = score;
      *o++ = score >> 8;
      break;
    case CA_OFFSET_SCORE_SINGLE_POSITIVE_3:
    case CA_OFFSET_SCORE_SINGLE_NEGATIVE_3:
      *o++ = score;
      *o++ = score >> 8;
      *o++ = score >> 16;
      break;
    default: {
      static_assert(sizeof(float) == sizeof(uint32_t),
                    "Expect sizeof(float) == sizeof(uint32_t)");
      auto p = reinterpret_cast<const uint32_t*>(&values[0].score);
      *reinterpret_cast<uint32_t*>(o) = *p;
      o += sizeof(float);
    } break;
  }
}

void EncodeOffsetScoreOroch(uint8_t*& o, uint8_t* oe,
                            const struct ca_offset_score* values,
                            size_t count) {
  // Special handling of singular offset/score record.
  if (count == 1) {
    EncodeOffsetScoreSingle(o, oe, values);
    return;
  }

  // Check to see if all the score values could be represented as integers.
  ca_offset_score_type type = CA_OFFSET_SCORE_DELTA_OROCH_OROCH;
  for (size_t i = 0; i < count; i++) {
    int64_t score = llrintf(values[i].score);
    if (float(score) != values[i].score) {
      type = CA_OFFSET_SCORE_DELTA_OROCH_FLOAT;
      break;
    }
  }

  // Store the chosen representation.
  *o++ = uint8_t(type);

  // Store the value count.
  oroch::varint_codec<size_t>::value_encode(o, count);

  // Store the first offset.
  uint64_t first = values[0].offset;
  oroch::varint_codec<uint64_t>::value_encode(o, first);

  // Delta-encode the offsets.
  std::vector<uint64_t> offset_delta(count - 1);
  for (size_t i = 1; i < count; i++)
    offset_delta[i - 1] = values[i].offset - values[i - 1].offset;

  // Choose packed represenatation for delta-encoded offsets. 
  oroch::integer_codec<uint64_t>::metadata offset_meta;
  auto offset_i = offset_delta.begin();
  auto offset_e = offset_delta.end();
  oroch::integer_codec<uint64_t>::select(offset_meta, offset_i, offset_e);
  offset_meta.encode(o);
  oroch::integer_codec<uint64_t>::encode(o, offset_i, offset_e, offset_meta);

  // Encode score according to the chosen representation.
  if (type == CA_OFFSET_SCORE_DELTA_OROCH_OROCH) {
    std::vector<int64_t> score(count);
    for (size_t i = 0; i < count; i++)
      score[i] = llrintf(values[i].score);
 
    oroch::integer_codec<int64_t>::metadata score_meta;
    auto score_i = score.begin();
    auto score_e = score.end();
    oroch::integer_codec<int64_t>::select(score_meta, score_i, score_e);
    score_meta.encode(o);
    oroch::integer_codec<int64_t>::encode(o, score_i, score_e, score_meta);
  } else {
    for (size_t i = 0; i < count; i++) {
      static_assert(sizeof(float) == sizeof(uint32_t),
                    "Expect sizeof(float) == sizeof(uint32_t)");
      auto p = reinterpret_cast<const uint32_t*>(&values[i].score);
      *reinterpret_cast<uint32_t*>(o) = *p;
      o += 4;
    }
  }
}

void EncodeOffsetScoreWithPrediction(uint8_t*& o,
                                     const struct ca_offset_score* values,
                                     size_t count) {
  *o++ = CA_OFFSET_SCORE_WITH_PREDICTION;
  ca_format_integer(&o, count);

  if (!count) return;

  ca_format_integer(&o, values[0].offset);

  std::set<uint64_t> steps;

  for (size_t i = 1; i < count; ++i) {
    KJ_REQUIRE(values[i].offset >= values[i - 1].offset);
    steps.insert(values[i].offset - values[i - 1].offset);
  }

  uint64_t prev_step = 0, next_key = 0;
  std::unordered_map<uint64_t, uint64_t> step_keys;
  bool use_step_map = false;

  if (count > 1) {
    if (steps.size() < 256 && steps.size() < (count >> 2)) {
      ca_format_integer(&o, steps.size());

      use_step_map = true;

      for (auto step : steps) {
        step_keys[step] = next_key++;

        ca_format_integer(&o, step - prev_step);
        prev_step = step;
      }
    } else {
      ca_format_integer(&o, 0);
    }
  }

  if (use_step_map) {
    for (size_t i = 1; i < count; ++i)
      ca_format_integer(&o, step_keys[values[i].offset - values[i - 1].offset]);
  } else {
    for (size_t i = 1; i < count; ++i)
      ca_format_integer(&o, values[i].offset - values[i - 1].offset);
  }

  std::vector<uint8_t> prob_mask;
  prob_mask.resize((count + 7) / 8, 0);

  for (size_t i = 0; i < count; ++i) {
    if (std::isfinite(values[i].score_pct5) &&
        std::isfinite(values[i].score_pct25) &&
        std::isfinite(values[i].score_pct75) &&
        std::isfinite(values[i].score_pct95)) {
      prob_mask[i >> 3] |= (1 << (i & 7));
    }
  }

  struct CA_rle_context rle;
  CA_rle_init_write(&rle, o);
  for (const auto& b : prob_mask) CA_rle_put(&rle, b);
  o = CA_rle_flush(&rle);

  for (size_t i = 0; i < count; ++i) {
    EncodeFloat(o, values[i].score);
    if (0 != (prob_mask[i >> 3] & (1 << (i & 7)))) {
      EncodeFloat(o, values[i].score_pct5);
      EncodeFloat(o, values[i].score_pct25);
      EncodeFloat(o, values[i].score_pct75);
      EncodeFloat(o, values[i].score_pct95);
    }
  }
}

}  // namespace

std::string Escape(const string_view& str) {
  std::string result;
  for (auto ch : str) {
    switch (ch) {
      case '\\':
        result += "\\\\";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\t':
        result += "\\t";
        break;
      default:
        result.push_back(ch);
    }
  }
  return result;
}

void ca_format_integer(uint8_t** output, uint64_t value) {
  uint8_t* p = *output;

  if (value > 0x7fffffffffffffffULL) *p++ = 0x80 | (value >> 63);
  if (value > 0xffffffffffffffULL) *p++ = 0x80 | (value >> 56);
  if (value > 0x1ffffffffffffULL) *p++ = 0x80 | (value >> 49);
  if (value > 0x3ffffffffffULL) *p++ = 0x80 | (value >> 42);
  if (value > 0x7ffffffffULL) *p++ = 0x80 | (value >> 35);
  if (value > 0xfffffff) *p++ = 0x80 | (value >> 28);
  if (value > 0x1fffff) *p++ = 0x80 | (value >> 21);
  if (value > 0x3fff) *p++ = 0x80 | (value >> 14);
  if (value > 0x7f) *p++ = 0x80 | (value >> 7);
  *p++ = value & 0x7f;

  *output = p;
}

size_t ca_offset_score_size(const struct ca_offset_score* values,
                            size_t count) {
  return 32 + count * sizeof(struct ca_offset_score);
}

size_t ca_format_offset_score(uint8_t* output, size_t output_size,
                              const struct ca_offset_score* values,
                              size_t count) {
  if (!count) {
    *output++ = CA_OFFSET_SCORE_EMPTY;
    return 1;
  }

  bool has_probabilty_bands = false;
  for (size_t i = 0; i < count; ++i) {
    if (std::isfinite(values[i].score_pct5) &&
        std::isfinite(values[i].score_pct25) &&
        std::isfinite(values[i].score_pct75) &&
        std::isfinite(values[i].score_pct95)) {
      has_probabilty_bands = true;
      break;
    }
  }

  uint8_t* start = output;

  if (has_probabilty_bands)
    EncodeOffsetScoreWithPrediction(output, values, count);
  else
    EncodeOffsetScoreOroch(output, output + output_size, values, count);

  return output - start;
}

}  // namespace table
}  // namespace cantera
