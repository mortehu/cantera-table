#include <cassert>
#include <cmath>
#include <cstring>
#include <set>
#include <unordered_map>

#include <kj/debug.h>

#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/rle.h"

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

void EncodeOffsetScoreFlexi(uint8_t*& o, const struct ca_offset_score* values,
                            size_t count) {
  *o++ = CA_OFFSET_SCORE_FLEXI;

  size_t i;

  uint64_t min_step = 0, max_step = 0, step_gcd = 0;

  float min_score, max_score;
  int all_uint24 = 1;
  uint8_t score_flags = 0;

  float tmp;

  /* Analyze */

  min_score = max_score = values[0].score;

  if (modff(values[0].score, &tmp) || values[0].score < 0 ||
      values[0].score > 0xffffff) {
    all_uint24 = 0;
  }

  for (i = 1; i < count; ++i) {
    uint64_t step = values[i].offset - values[i - 1].offset;

    if (i == 1) {
      min_step = max_step = step_gcd = step;
    } else {
      if (step < min_step) {
        min_step = step;
      } else if (step > max_step) {
        max_step = step;
      }
    }

    if (values[i].score < min_score)
      min_score = values[i].score;
    else if (values[i].score > max_score)
      max_score = values[i].score;

    if (all_uint24) {
      if (modff(values[i].score, &tmp) || values[i].score < 0 ||
          values[i].score > 0xffffff) {
        all_uint24 = 0;
      }
    }
  }

  if (count > 1) {
    step_gcd = GCD(min_step, max_step);

    for (i = 1; i < count && step_gcd > 1; ++i) {
      uint64_t step = values[i].offset - values[i - 1].offset;
      if (step_gcd != step) step_gcd = GCD(step, step_gcd);
    }
  }

  ca_format_integer(&o, count);

  /* Output offsets */

  ca_format_integer(&o, values[0].offset);
  ca_format_integer(&o, step_gcd);

  if (step_gcd) {
    struct CA_rle_context rle;

    min_step /= step_gcd;
    max_step /= step_gcd;

    ca_format_integer(&o, min_step);
    ca_format_integer(&o, max_step - min_step);

    if (min_step == max_step)
      ; /* Nothing needs to be stored */
    else if (max_step - min_step <= 0x0f) {
      CA_rle_init_write(&rle, o);

      for (i = 1; i < count; i += 2) {
        uint8_t tmp;

        tmp = (values[i].offset - values[i - 1].offset) / step_gcd - min_step;

        if (i + 1 < count)
          tmp |=
              ((values[i + 1].offset - values[i].offset) / step_gcd - min_step)
              << 4;

        CA_rle_put(&rle, tmp);
      }

      o = CA_rle_flush(&rle);
    } else if (max_step - min_step <= 0xff) {
      CA_rle_init_write(&rle, o);

      for (i = 1; i < count; ++i)
        CA_rle_put(&rle, (values[i].offset - values[i - 1].offset) / step_gcd -
                             min_step);

      o = CA_rle_flush(&rle);
    } else {
      for (i = 1; i < count; ++i)
        ca_format_integer(
            &o,
            (values[i].offset - values[i - 1].offset) / step_gcd - min_step);
    }
  } else {
    assert(min_step == max_step);
    assert(max_step == 0);
  }

  /* Output scores */

  if (max_score == min_score) {
    score_flags = 0x80;

    count = 1;
  }

  if (all_uint24) {
    if (max_score - min_score <= 0xff)
      score_flags |= 0x01;
    else if (max_score - min_score <= 0xffff)
      score_flags |= 0x02;
    else
      score_flags |= 0x03;
  }

  *o++ = score_flags;

  if (all_uint24) ca_format_integer(&o, min_score);

  for (i = 0; i < count; ++i) {
    switch (score_flags & 0x03) {
      case 0x00:

        EncodeFloat(o, values[i].score);

        break;

      case 0x01:

        *o++ = (uint8_t)(values[i].score - min_score);

        break;

      case 0x02: {
        uint_fast16_t delta;

        delta = (uint_fast16_t)(values[i].score - min_score);

        *o++ = delta >> 8;
        *o++ = delta;
      } break;

      case 0x03: {
        uint_fast32_t delta;

        delta = (uint_fast32_t)(values[i].score - min_score);

        *o++ = delta >> 16;
        *o++ = delta >> 8;
        *o++ = delta;
      } break;
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

namespace ca_table {

std::string Escape(const ev::StringRef& str) {
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

}  // namespace ca_table

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

size_t ca_format_offset_score(uint8_t* output,
                              const struct ca_offset_score* values,
                              size_t count) {
  if (!count) {
    *output++ = CA_OFFSET_SCORE_FLEXI;
    *output++ = 0;
    return 2;
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
    EncodeOffsetScoreFlexi(output, values, count);

  return output - start;
}
