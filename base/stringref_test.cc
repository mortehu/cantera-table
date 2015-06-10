#include "base/stringref.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct StringRefTest : testing::Test {};

TEST_F(StringRefTest, Compare) {
  EXPECT_GT(0, ev::StringRef("aa").compare(ev::StringRef("ab")));
  EXPECT_LT(0, ev::StringRef("ab").compare(ev::StringRef("aa")));

  EXPECT_GT(0, ev::StringRef("aa").compare(ev::StringRef("aaa")));
  EXPECT_LT(0, ev::StringRef("aaa").compare(ev::StringRef("aa")));

  EXPECT_GT(0, ev::StringRef("a").compare(ev::StringRef("aa")));
  EXPECT_LT(0, ev::StringRef("aa").compare(ev::StringRef("a")));

  EXPECT_EQ(0, ev::StringRef("aa").compare(ev::StringRef("aa")));
}

TEST_F(StringRefTest, CompareToCString) {
  EXPECT_GT(0, ev::StringRef("aa").compare("ab"));
  EXPECT_LT(0, ev::StringRef("ab").compare("aa"));

  EXPECT_GT(0, ev::StringRef("aa").compare("aaa"));
  EXPECT_LT(0, ev::StringRef("aaa").compare("aa"));

  EXPECT_GT(0, ev::StringRef("a").compare("aa"));
  EXPECT_LT(0, ev::StringRef("aa").compare("a"));

  EXPECT_EQ(0, ev::StringRef("aa").compare("aa"));
}
