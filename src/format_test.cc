#include <algorithm>
#include <cmath>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "third_party/gtest/gtest.h"

using namespace cantera::table;

namespace {

void ValidateValues(ca_offset_score* values, size_t count) {
  std::sort(values, values + count, [](const auto& lhs, const auto& rhs) {
    return lhs.score < rhs.score;
  });

  auto compressed_size = ca_offset_score_size(values, count);

  std::vector<uint8_t> compressed_data(compressed_size);

  {
    auto data = compressed_data.data();
    auto size = ca_format_offset_score(data, compressed_size, values, count);

    EXPECT_LE(size, compressed_size);
    compressed_data.resize(size);
  }

  {
    std::vector<ca_offset_score> decompressed_values;

    ca_offset_score_parse(cantera::string_view{reinterpret_cast<const char*>(compressed_data.data()), compressed_data.size()}, &decompressed_values);

    EXPECT_EQ(decompressed_values.size(), count);
    EXPECT_TRUE(std::equal(decompressed_values.begin(),
                           decompressed_values.end(), values, values + count,
                           [](auto& lhs, auto& rhs) {
      return lhs.offset == rhs.offset && lhs.score == rhs.score;
    }));

    EXPECT_EQ(
        decompressed_values.back().offset,
        ca_offset_score_max_offset(&compressed_data[0],
                                   &compressed_data[compressed_data.size()]));
  }
}

}  // namespace

struct FormatTest : testing::Test {};

TEST_F(FormatTest, OffsetScoreFuzzTest) {
  const size_t kIterations = 1000;

  auto seed = static_cast<unsigned int>(time(nullptr));
  fprintf(stderr, "Seed: %u\n", seed);
  srand(seed);

  for (size_t i = 0; i < kIterations; ++i) {
    std::vector<ca_offset_score> values;
    std::vector<ca_offset_score> decoded_values;

    values.resize(rand() % 5);

    bool do_probabilities = (rand() % 3) == 0;

    uint64_t offset = rand();
    uint64_t step_min = 1 + (rand() % 100000);
    uint64_t step_max = step_min + rand();
    float scale = pow(2, (rand() % 17) - 8);
    for (auto& os : values) {
      os.offset = offset;
      os.score = rand() * scale;
      if (do_probabilities && (rand() % 10) < 9) {
        os.score_pct25 = os.score - scale;
        os.score_pct75 = os.score + scale;
        os.score_pct5 = os.score_pct25 - scale;
        os.score_pct95 = os.score_pct75 + scale;
      }
      offset += step_min + (rand() % (step_max - step_min));
    }

    auto max_size = ca_offset_score_size(&values[0], values.size());

    std::vector<uint8_t> buffer(max_size);

    auto data = buffer.data();
    auto size = ca_format_offset_score(data, max_size, &values[0], values.size());

    EXPECT_LE(size, max_size);

    buffer.resize(size);

    ca_offset_score_parse(cantera::string_view{reinterpret_cast<const char*>(buffer.data()), buffer.size()}, &decoded_values);
    ASSERT_EQ(decoded_values.size(), values.size());

    for (size_t i = 0; i < values.size(); ++i) {
      EXPECT_EQ(values[i].offset, decoded_values[i].offset);
      EXPECT_EQ(values[i].score, decoded_values[i].score);

      if (std::isnan(values[i].score_pct5)) {
        EXPECT_TRUE(std::isnan(values[i].score_pct5));
        EXPECT_TRUE(std::isnan(values[i].score_pct25));
        EXPECT_TRUE(std::isnan(values[i].score_pct75));
        EXPECT_TRUE(std::isnan(values[i].score_pct95));
        EXPECT_TRUE(std::isnan(decoded_values[i].score_pct5));
        EXPECT_TRUE(std::isnan(decoded_values[i].score_pct25));
        EXPECT_TRUE(std::isnan(decoded_values[i].score_pct75));
        EXPECT_TRUE(std::isnan(decoded_values[i].score_pct95));
      } else {
        EXPECT_EQ(values[i].score_pct5, decoded_values[i].score_pct5);
        EXPECT_EQ(values[i].score_pct25, decoded_values[i].score_pct25);
        EXPECT_EQ(values[i].score_pct75, decoded_values[i].score_pct75);
        EXPECT_EQ(values[i].score_pct95, decoded_values[i].score_pct95);
      }
    }

    if (!values.empty()) {
      EXPECT_EQ(values.back().offset,
                ca_offset_score_max_offset(&buffer[0], &buffer[buffer.size()]));
    }
  }
}

TEST_F(FormatTest, SteppedScore) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].score = (i << 8);

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, LinearScore) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].score = i;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, SawScore) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].score = i & 0xff;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, ZeroScore) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].score = 0;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, NegativeScore) {
  std::vector<ca_offset_score> values;

  for (float v = -1.0f; v >= -16777216.0f; v *= 2.0f) {
    ca_offset_score value;
    value.offset = 1;
    value.score = v;
    ValidateValues(&value, 1);

    values.emplace_back(values.size() + 1, v);
  }

  ValidateValues(values.data(), values.size());
}

// Verifies that we do not discard the list significant bit.
TEST_F(FormatTest, ScoreCloseToOne) {
  static const size_t kValueCount = 16;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i)
    values[i].score = 1.00000012f;  // 0x3f800001

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, LinearOffset) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].offset = i;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, LinearOffset2) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].offset = i * 2;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, LinearOffset3) {
  static const size_t kValueCount = 1024;
  struct ca_offset_score values[kValueCount];

  for (size_t i = 0; i < kValueCount; ++i) values[i].offset = i * 16 + 7;

  ValidateValues(values, kValueCount);
}

TEST_F(FormatTest, SingleValue) {
  struct ca_offset_score value;
  memset(&value, 0, sizeof(value));

  ValidateValues(&value, 1);
}
