#ifndef BASE_ASYNC_H_
#define BASE_ASYNC_H_ 1

#include <functional>

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

namespace ev {

// Thread-safe synchronization using Cap'n Proto promises.
class Semaphore {
 public:
  Semaphore(kj::UnixEventPort& event_port, int initial_count = 0);

  void Put(uint64_t count);

  kj::Promise<void> Get();

 private:
  kj::UnixEventPort& event_port_;

  kj::AutoCloseFd fd_;
};

// Async IO context for the main thread.
kj::AsyncIoContext& AsyncIoContext();

// Adds a function to be called before destroying the main thread's Async IO
// context.
void AddIoContextCleanupHandler(std::function<void()> callback);

// Converts a promise of any type to a void promise.
template <typename T>
kj::Promise<void> ToVoidPromise(kj::Promise<T>&& p) {
  return p.then([](auto x) -> kj::Promise<void> { return kj::READY_NOW; });
}

}  // namespace ev

#endif  // !BASE_ASYNC_H_
