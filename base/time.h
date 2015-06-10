#ifndef BASE_TIME_H_
#define BASE_TIME_H_ 1

#include <cstdint>

#include <sys/time.h>

namespace ev {

inline uint64_t CurrentTimeUSec() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  return static_cast<uint64_t>(now.tv_sec) * 1000000 + now.tv_usec;
}

}  // namespace ev

#endif  // !BASE_TIME_H_
