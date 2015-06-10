#include "storage/ca-table/keywords.h"

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <algorithm>

#include "base/string.h"

#include <kj/debug.h>
#include <yaml-cpp/yaml.h>

namespace YAML {

template <>
struct convert<std::vector<std::string, std::allocator<std::string>>> {
  // If the provided node is a sequence, reads the contained strings into the
  // provided vector.
  static bool decode(const Node& node, std::vector<std::string>& output) {
    if (!node.IsSequence()) return false;

    for (const auto& value : node) {
      if (!value.IsScalar()) return false;

      output.emplace_back(value.as<std::string>());
    }

    std::sort(output.begin(), output.end());

    return true;
  }
};

}  // namespace YAML

namespace ca_table {

Keywords::Keywords() {
  auto config =
      YAML::LoadFile(DATAROOTDIR "/san-francisco/config/keywords.yaml");
  if (!config) return;

  ephemeral_ = std::move(config["ephemeral"].as<std::vector<std::string>>());
  timestamped_ =
      std::move(config["timestamped"].as<std::vector<std::string>>());
}

Keywords& Keywords::GetInstance() {
  static Keywords instance;
  return instance;
}

bool Keywords::IsEphemeral(const ev::StringRef& keyword) const {
  for (const auto& prefix : ephemeral_) {
    if (HasPrefix(keyword, prefix)) return true;
  }

  return false;
}

bool Keywords::IsTimestamped(const ev::StringRef& keyword) const {
  for (const auto& prefix : timestamped_) {
    if (HasPrefix(keyword, prefix)) return true;
  }

  return false;
}

}  // namespace ca_table
