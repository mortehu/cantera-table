#ifndef BASE_ESCAPE_H_
#define BASE_ESCAPE_H_ 1

#include <string>
#include <vector>

#include "base/stringref.h"

namespace ev {

std::string DecodeURIComponent(const StringRef& input);

std::vector<std::pair<std::string, std::string>> DecodeURIQuery(
    const StringRef& input);

// Converts a string to JSON format.
void ToJSON(const ev::StringRef& input, std::string& output);

}  // namespace ev

#endif  // !BASE_ESCAPE_H_
