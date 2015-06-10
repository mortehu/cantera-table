#include "base/async.h"
#include "third_party/gtest/gtest.h"

struct AsyncTest : testing::Test {};

TEST_F(AsyncTest, SemaphoreGetEmpty) {
  kj::UnixEventPort port;
  kj::EventLoop loop(port);
  kj::WaitScope wait_scope(loop);

  ev::Semaphore sem(port);

  bool fulfilled = false;
  auto promise = sem.Get().then([&]() { fulfilled = true; }).eagerlyEvaluate(
      [](kj::Exception&& e) { ASSERT_TRUE(false); });

  port.poll();
  loop.run();

  EXPECT_FALSE(fulfilled);
}

TEST_F(AsyncTest, SemaphorePutThenGet) {
  kj::UnixEventPort port;
  kj::EventLoop loop(port);
  kj::WaitScope wait_scope(loop);

  ev::Semaphore sem(port);

  sem.Put(1);

  bool fulfilled = false;
  auto promise = sem.Get().then([&]() { fulfilled = true; }).eagerlyEvaluate(
      [](kj::Exception&& e) { ASSERT_TRUE(false); });

  loop.run();

  EXPECT_TRUE(fulfilled);
}

TEST_F(AsyncTest, SemaphoreGetThenPut) {
  kj::UnixEventPort port;
  kj::EventLoop loop(port);
  kj::WaitScope wait_scope(loop);

  ev::Semaphore sem(port);

  bool fulfilled = false;
  auto promise = sem.Get().then([&]() { fulfilled = true; }).eagerlyEvaluate(
      [](kj::Exception&& e) { ASSERT_TRUE(false); });

  sem.Put(1);

  port.poll();
  loop.run();

  EXPECT_TRUE(fulfilled);
}
