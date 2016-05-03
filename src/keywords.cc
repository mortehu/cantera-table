#if HAVE_CONFIG_H
#include "config.h"
#endif

#include "src/keywords.h"

#include <algorithm>

#include <kj/debug.h>
#include <re2/re2.h>
#include <yaml-cpp/yaml.h>

#include "src/util.h"

namespace cantera {
namespace table {

using namespace internal;

auto Convert(const YAML::Node& node) {
  KJ_REQUIRE(node.IsSequence());

  std::vector<Keywords::Filter> result;
  for (const auto& value : node) {
    KJ_REQUIRE(value.IsScalar(), value.Type());

    const auto expression = value.as<std::string>();
    KJ_REQUIRE(!expression.empty());

    Keywords::Filter filter;
    if (expression.size() > 2 && expression.front() == '/' && expression.back() == '/') {
      // Expression is a regular expression.
      filter.regex = std::make_unique<re2::RE2>(expression.substr(1, expression.size() - 2));

      // Extract shared prefix of regular expression's matches.
      std::string first_key, last_key;
      KJ_REQUIRE(filter.regex->PossibleMatchRange(&first_key, &last_key, 64));
      for (size_t i = 0; i < first_key.size() && i < last_key.size() && first_key[i] == last_key[i]; ++i)
        filter.prefix.push_back(first_key[i]);
    } else {
      // Expression is a prefix pattern.
      filter.prefix = expression;
    }

    result.emplace_back(std::move(filter));
  }

  return result;
}

Keywords::Keywords() {
  auto config =
      YAML::LoadFile(DATAROOTDIR "/san-francisco/config/keywords.yaml");
  if (!config) return;

  ephemeral_ = Convert(config["ephemeral"]);
  timestamped_ = Convert(config["timestamped"]);
}

Keywords& Keywords::GetInstance() {
  static Keywords instance;
  return instance;
}

bool Keywords::IsEphemeral(const string_view& keyword) const {
  for (const auto& filter : ephemeral_) {
    if (HasPrefix(keyword, filter.prefix)) {
      if (filter.regex) {
        if (!RE2::FullMatch(re2::StringPiece(keyword.data(), keyword.size()), *filter.regex)) continue;
      }

      return true;
    }
  }

  return false;
}

bool Keywords::IsTimestamped(const string_view& keyword) const {
  for (const auto& filter : timestamped_) {
    if (HasPrefix(keyword, filter.prefix)) {
      if (filter.regex) {
        if (!RE2::FullMatch(re2::StringPiece(keyword.data(), keyword.size()), *filter.regex)) continue;
      }

      return true;
    }
  }

  return false;
}

}  // namespace table
}  // namespace cantera
