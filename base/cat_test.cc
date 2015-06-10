#include "base/cat.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct CatTest : testing::Test {};

TEST_F(CatTest, Empty) { EXPECT_EQ("", cat()); }

TEST_F(CatTest, CStrings) {
  EXPECT_EQ("foo", cat("foo"));
  EXPECT_EQ("foobar", cat("foo", "bar"));
  EXPECT_EQ("foobarbaz", cat("foo", "bar", "baz"));
}

TEST_F(CatTest, IntegerTests) {
  EXPECT_EQ("\001", cat(char(1)));
  EXPECT_EQ("1", cat(short(1)));
  EXPECT_EQ("1", cat(1));
  EXPECT_EQ("12", cat(1, 2));
  EXPECT_EQ("123", cat(1, 2, 3));
  EXPECT_EQ("-2147483648", cat(std::numeric_limits<int>::min()));
  EXPECT_EQ("2147483647", cat(std::numeric_limits<int>::max()));
  EXPECT_EQ("4294967295", cat(std::numeric_limits<unsigned int>::max()));
  EXPECT_EQ("-9223372036854775808", cat(std::numeric_limits<long long>::min()));
  EXPECT_EQ("9223372036854775807", cat(std::numeric_limits<long long>::max()));
}

TEST_F(CatTest, FloatTests) {
  EXPECT_EQ("0", cat(0.0f));
  EXPECT_EQ("0", cat(0.0));
  EXPECT_EQ("inf", cat(std::numeric_limits<float>::infinity()));
  EXPECT_EQ("inf", cat(std::numeric_limits<double>::infinity()));
  EXPECT_EQ("-inf", cat(-std::numeric_limits<float>::infinity()));
  EXPECT_EQ("-inf", cat(-std::numeric_limits<double>::infinity()));
  EXPECT_EQ("nan", cat(std::numeric_limits<float>::quiet_NaN()));
  EXPECT_EQ("nan", cat(std::numeric_limits<double>::quiet_NaN()));
  EXPECT_EQ("1.17549435e-38", cat(std::numeric_limits<float>::min()));
  EXPECT_EQ("3.40282347e+38", cat(std::numeric_limits<float>::max()));
  EXPECT_EQ("2.2250738585072014e-308", cat(std::numeric_limits<double>::min()));
  EXPECT_EQ("1.7976931348623157e+308", cat(std::numeric_limits<double>::max()));
}

TEST_F(CatTest, StdString) {
  EXPECT_EQ("aa", cat(std::string("aa")));
  EXPECT_EQ("aabb", cat(std::string("aa"), std::string("bb")));
  EXPECT_EQ("aabbcc",
            cat(std::string("aa"), std::string("bb"), std::string("cc")));
  EXPECT_EQ("mmnnoo", cat(std::string("mm"), "nn", std::string("oo")));
}

TEST_F(CatTest, StringRef) {
  EXPECT_EQ("xx", cat(StringRef("xx")));
  EXPECT_EQ("xxyy", cat(StringRef("xx"), StringRef("yy")));
  EXPECT_EQ("xxyyzz", cat(StringRef("xx"), StringRef("yy"), StringRef("zz")));
}

TEST_F(CatTest, Mixed) {
  EXPECT_EQ("w00t!", cat("", "w", 0, 0, std::string("t"), StringRef("!")));
  EXPECT_EQ("w0", cat("w", 0));
  EXPECT_EQ("0w", cat(0, "w"));
}
