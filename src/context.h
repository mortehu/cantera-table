#ifndef STORAGE_CA_TABLE_CONTEXT_H_
#define STORAGE_CA_TABLE_CONTEXT_H_ 1

#include <cstdint>
#include <experimental/optional>
#include <unordered_set>

namespace cantera {
namespace table {
namespace internal {

class Context {
 public:
  static Context* Get() noexcept {
    if (context_ == nullptr) context_ = new Context();
    return context_;
  }

  std::size_t GetNestingLevel() const noexcept { return nesting_; }
  void EnterNestingLevel() noexcept { ++nesting_; }
  void LeaveNestingLevel() noexcept { --nesting_; }

  bool UseFilter() const noexcept {
    return nesting_ == 1 && filter_;
  }

  void SetFilter(std::unordered_set<std::uint64_t>&& filter) noexcept {
    filter_ = std::move(filter);
  }

  const std::unordered_set<std::uint64_t> & GetFilter() const noexcept {
    return filter_.value();
  }

 private:
  std::size_t nesting_ = 0;
  std::experimental::optional<std::unordered_set<std::uint64_t>> filter_;

  static thread_local Context* context_;
};

class ContextNestingLevelGuard {
 public:
  ContextNestingLevelGuard() noexcept { Context::Get()->EnterNestingLevel(); }
  ~ContextNestingLevelGuard() noexcept { Context::Get()->LeaveNestingLevel(); }
};

}  // namespace internal
}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_SELECT_H_
