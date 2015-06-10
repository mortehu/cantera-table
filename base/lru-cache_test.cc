#include "base/lru-cache.h"
#include "third_party/gtest/gtest.h"

struct LRUCacheTest : testing::Test {};

TEST_F(LRUCacheTest, PutAndGet) {
  ev::LRUCache<int> cache(1);

  EXPECT_EQ(nullptr, cache.Get("a"));

  cache.Add("a", 2);

  auto ret = cache.Get("a");
  ASSERT_TRUE(ret != nullptr);

  EXPECT_EQ(2, *ret);
}
