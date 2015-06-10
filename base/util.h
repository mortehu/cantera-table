#ifndef BASE_UTIL_H_
#define BASE_UTIL_H_ 1

#include <chrono>

#include <sys/types.h>
#include <unistd.h>

namespace ev {

uint64_t WeakRNGSeed() {
  // TODO(mortehu): Find out how to use microseconds.
  static uint64_t base_seed =
      std::chrono::system_clock::now().time_since_epoch().count() * 65536 +
      getpid();
  return base_seed++;
}

}  // namespace ev

#endif  // !BASE_UTIL_H_
