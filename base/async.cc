#include "base/async.h"

#include <memory>
#include <mutex>
#include <vector>

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <kj/debug.h>

namespace ev {

namespace {

std::mutex async_io_lock;

class AsyncIoData {
 public:
  AsyncIoData() : kj_async_io(kj::setupAsyncIo()) {}

  ~AsyncIoData() {
    for (auto& f : cleanup_handlers) f();
    cleanup_handlers.clear();
  }

  kj::AsyncIoContext kj_async_io;
  std::vector<std::function<void()>> cleanup_handlers;
};

std::unique_ptr<AsyncIoData> async_io;

}  // namespace

Semaphore::Semaphore(kj::UnixEventPort& event_port, int initial_count)
    : event_port_(event_port) {
  int fd;
  KJ_SYSCALL(
      fd = eventfd(initial_count, EFD_CLOEXEC | EFD_SEMAPHORE | EFD_NONBLOCK));

  fd_ = kj::AutoCloseFd(fd);
}

void Semaphore::Put(uint64_t value) {
  ssize_t ret;
  KJ_SYSCALL(ret = write(fd_, &value, sizeof(value)));
  KJ_REQUIRE(ret == sizeof(value));
}

kj::Promise<void> Semaphore::Get() {
  uint64_t buf;
  ssize_t ret;
  KJ_NONBLOCKING_SYSCALL(ret = read(fd_, &buf, sizeof(buf)));
  if (ret > 0) {
    KJ_REQUIRE(ret == sizeof(buf));
    KJ_REQUIRE(buf == 1, buf);
    return kj::READY_NOW;
  }

  auto observer = kj::heap<kj::UnixEventPort::FdObserver>(
      event_port_, fd_, kj::UnixEventPort::FdObserver::OBSERVE_READ);

  return observer->whenBecomesReadable()
      .then([this]() -> kj::Promise<void> {
        uint64_t buf;
        ssize_t ret;
        KJ_SYSCALL(ret = read(fd_, &buf, sizeof(buf)));
        KJ_REQUIRE(ret == sizeof(buf));
        KJ_REQUIRE(buf == 1, buf);
        return kj::READY_NOW;
      })
      .attach(kj::mv(observer));
}

kj::AsyncIoContext& AsyncIoContext() {
  std::unique_lock<std::mutex> lk(async_io_lock);
  if (!async_io) async_io = std::make_unique<AsyncIoData>();

  return async_io->kj_async_io;
}

void AddIoContextCleanupHandler(std::function<void()> callback) {
  std::unique_lock<std::mutex> lk(async_io_lock);
  if (!async_io) async_io = std::make_unique<AsyncIoData>();

  async_io->cleanup_handlers.emplace_back(std::move(callback));
}

}  // namespace ev
