#ifndef STORAGE_CA_TABLE_KEYWORDS_H_
#define STORAGE_CA_TABLE_KEYWORDS_H_ 1

#include <string>
#include <memory>
#include <vector>

#include "src/ca-table.h"

namespace re2 {
class RE2;
}  // namespace re2

namespace cantera {
namespace table {

// Holds metadata about index keywords.
class Keywords {
 public:
  struct Filter {
    Filter() = default;
    Filter(Filter&&) = default;
    Filter& operator=(Filter&&) = default;

    std::string prefix;
    std::unique_ptr<re2::RE2> regex;
  };

  static Keywords& GetInstance();

  bool IsEphemeral(const string_view& keyword) const;

  bool IsTimestamped(const string_view& keyword) const;

 private:
  Keywords();

  // Keyword prefixes for ephemeral keywords, i.e. keywords whose values can
  // change every date.
  std::vector<Filter> ephemeral_;

  // Keyword prefixes for timestamped keywords, i.e. keywords whose score
  // values indicate a date.
  std::vector<Filter> timestamped_;
};

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_KEYWORDS_H_
