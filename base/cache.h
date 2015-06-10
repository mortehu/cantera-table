#ifndef BASE_CACHE_H_
#define BASE_CACHE_H_

#include <functional>
#include <vector>

#include "base/time.h"

namespace ev {

// Implements a simple fixed size cache with constant time expiry.
//
// TODO(mortehu): Make this cache N-way instead of direct mapped.
template <typename KeyType, typename ValueType>
class FixedTTLCache {
 public:
  FixedTTLCache(size_t capacity, uint64_t ttl_usec) : ttl_usec_(ttl_usec) {
    entries_.resize(capacity);
  }

  // Inserts an element into the cache.  May replace a non-expired entry if the
  // keys' hash values alias to the same hash bucket.
  void Insert(KeyType key, ValueType value) {
    const auto hash = hash_(key) % entries_.size();

    entries_[hash].expires = ev::CurrentTimeUSec() + ttl_usec_;
    entries_[hash].key = std::move(key);
    entries_[hash].value = std::move(value);
  }

  // Returns an element from the cache, or nullptr if the entry has expired or
  // does not exist.
  const ValueType* Get(const KeyType& key) {
    const auto hash = hash_(key) % entries_.size();

    const auto now = ev::CurrentTimeUSec();

    if (entries_[hash].expires < now || entries_[hash].key != key)
      return nullptr;

    return &entries_[hash].value;
  }

 private:
  struct Entry {
    uint64_t expires;
    KeyType key;
    ValueType value;
  };

  std::vector<Entry> entries_;

  uint64_t ttl_usec_;

  std::hash<KeyType> hash_;
};

// Implements a simple fixed size cache.
//
// TODO(mortehu): Make this cache N-way instead of direct mapped.
template <typename KeyType, typename ValueType>
class FixedCache {
 public:
  FixedCache(size_t capacity) { entries_.resize(capacity); }

  // Inserts an element into the cache, possibly evicting what was there before.
  void Insert(KeyType key, ValueType value) {
    const auto hash = hash_(key) % entries_.size();

    entries_[hash].key = std::move(key);
    entries_[hash].value = std::move(value);
  }

  // Returns an element from the cache, or nullptr if the entry has expired or
  // does not exist.
  const ValueType* Get(const KeyType& key) {
    const auto hash = hash_(key) % entries_.size();

    if (entries_[hash].key != key) return nullptr;

    return &entries_[hash].value;
  }

 private:
  struct Entry {
    KeyType key;
    ValueType value;
  };

  std::vector<Entry> entries_;

  std::hash<KeyType> hash_;
};

}  // namespace ev

#endif  // !BASE_CACHE_H_
