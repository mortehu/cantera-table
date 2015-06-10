#include "base/escape.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct EscapeTest : testing::Test {};

TEST_F(EscapeTest, DecodeURIComponent) {
  EXPECT_EQ(" ", DecodeURIComponent("%20"));
  EXPECT_EQ("a ", DecodeURIComponent("a%20"));
  EXPECT_EQ("a a", DecodeURIComponent("a%20a"));
  EXPECT_EQ(" a", DecodeURIComponent("%20a"));
}

TEST_F(EscapeTest, DecodeURIQuery) {
  auto v = DecodeURIQuery("x=%20&%20=z");
  ASSERT_EQ(2U, v.size());
  EXPECT_EQ("x", v[0].first);
  EXPECT_EQ(" ", v[0].second);
  EXPECT_EQ(" ", v[1].first);
  EXPECT_EQ("z", v[1].second);
}

TEST_F(EscapeTest, ToJSON) {
  std::string output;
  ToJSON("\n\r\001x\"\\", output);

  EXPECT_EQ("\"\\n\\r\\u0001x\\\"\\\\\"", output);
}
