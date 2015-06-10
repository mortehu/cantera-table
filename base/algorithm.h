#ifndef BASE_ALGORITHM_H_
#define BASE_ALGORITHM_H_

#include <algorithm>

#include "base/random.h"

namespace ev {

template <typename ForwardIterator, typename OutputIterator, typename Distance>
OutputIterator RandomSample(ForwardIterator first, ForwardIterator last,
                            OutputIterator out, const Distance n) {
  Distance remaining = std::distance(first, last);
  Distance m = std::min(n, remaining);

  while (m > 0) {
    std::uniform_int_distribution<size_t> rng_dist(0, remaining - 1);

    if (rng_dist(ev::rng) < m) {
      *out++ = *first;
      --m;
    }

    --remaining;
    ++first;
  }

  return out;
}

}  // namespace ev

#endif  // !BASE_ALGORITHM_H_
