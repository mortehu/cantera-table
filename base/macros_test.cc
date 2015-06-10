#include "base/macros.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct MacrosTest : testing::Test {};

TEST_F(MacrosTest, Coalesce) {
  EXPECT_EQ(0, Coalesce(0));
  EXPECT_EQ(1, Coalesce(0, 1));
  EXPECT_EQ(1, Coalesce(1, 0));
  EXPECT_EQ(1, Coalesce(0, 0, 1));
  EXPECT_EQ(2, Coalesce(2, 0, 1));
  EXPECT_EQ(2, Coalesce(0, 2, 1));
  EXPECT_EQ(3, Coalesce(3, 2, 1));
}
