#ifndef STORAGE_CA_TABLE_KEYWORDS_H_
#define STORAGE_CA_TABLE_KEYWORDS_H_ 1

#include "base/stringref.h"

#include <string>
#include <vector>

namespace ca_table {

// Holds metadata about index keywords.
class Keywords {
 public:
  static Keywords& GetInstance();

  bool IsEphemeral(const ev::StringRef& keyword) const;

  bool IsTimestamped(const ev::StringRef& keyword) const;

 private:
  Keywords();

  // Keyword prefixes for ephemeral keywords, i.e. keywords whose values can
  // change every date.
  std::vector<std::string> ephemeral_;

  // Keyword prefixes for timestamped keywords, i.e. keywords whose score
  // values indicate a date.
  std::vector<std::string> timestamped_;
};

}  // namespace ca_table

#endif  // !STORAGE_CA_TABLE_KEYWORDS_H_
