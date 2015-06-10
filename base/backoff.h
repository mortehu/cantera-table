#ifndef BASE_BACKOFF_H_
#define BASE_BACKOFF_H_ 1

#include "base/random.h"

inline uint64_t BackoffDelayUsec(uint64_t delay) {
  static const uint64_t kMaxSleepUsec = 30 * 1000 * 1000;
  static const uint64_t kMinGrowthUsec = 10 * 1000;
  static const uint64_t kMaxGrowthUsec = 500 * 1000;
  std::uniform_int_distribution<uint64_t> rng_dist(
      kMinGrowthUsec,
      std::min(std::max(kMinGrowthUsec, delay), kMaxGrowthUsec));

  delay += rng_dist(ev::rng);
  if (delay > kMaxSleepUsec) return kMaxSleepUsec;
  return delay;
}

#endif  // !BASE_BACKOFF_H_
