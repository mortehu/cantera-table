#include "base/string.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct StringTest : testing::Test {};

TEST_F(StringTest, Ok) {
  EXPECT_EQ(0, StringToInt64("0"));

  EXPECT_EQ(0, StringToInt64("  0"));

  EXPECT_EQ(-9223372036854775807LL - 1, StringToInt64("-9223372036854775808"));

  EXPECT_EQ(9223372036854775807LL, StringToInt64("9223372036854775807"));
}

TEST_F(StringTest, BadSuffix) {
  ASSERT_THROW(StringToInt64("0x"), kj::Exception);
  ASSERT_THROW(StringToInt64("-9223372036854775808.0"), kj::Exception);
  ASSERT_THROW(StringToInt64("9223372036854775807 "), kj::Exception);
}

TEST_F(StringTest, BadPrefix) {
  ASSERT_THROW(StringToInt64("x0"), kj::Exception);
  ASSERT_THROW(StringToInt64(".0"), kj::Exception);
}

TEST_F(StringTest, DoubleToString) {
  EXPECT_EQ("0", DoubleToString(0.0));
  EXPECT_EQ("0.5", DoubleToString(0.5));
  EXPECT_EQ("0.1", DoubleToString(0.1));
  EXPECT_EQ("0.00001", DoubleToString(0.00001));
  EXPECT_EQ("2.5", DoubleToString(2.5));
  EXPECT_EQ("-0.9", DoubleToString(-0.9));
  EXPECT_EQ("1000", DoubleToString(1000.0));
}

TEST_F(StringTest, ExplodeWithLimit) {
  std::vector<ev::StringRef> ok{"a", "", "c:d"};
  EXPECT_EQ(ok, Explode("a::c:d", ":", 3));
}
