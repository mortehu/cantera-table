#include "base/escape.h"

#include <cctype>
#include <vector>

namespace ev {

namespace {
static const unsigned char kHexHelper[26] = {0, 10, 11, 12, 13, 14, 15, 0, 0,
                                             0, 0,  0,  0,  0,  0,  0,  0, 1,
                                             2, 3,  4,  5,  6,  7,  8,  9};
}  // namespace

std::string DecodeURIComponent(const StringRef& input) {
  std::string result;

  int escape = 0;
  unsigned char decode_buffer = 0;

  for (char ch : input) {
    switch (escape) {
      case 0:
        if (ch == '%') {
          escape = 1;
        } else {
          result.push_back(ch);
        }
        break;

      case 1:
        decode_buffer = kHexHelper[ch & 0x1f] << 4;
        escape = 2;
        break;

      case 2:
        ch = decode_buffer | kHexHelper[ch & 0x1f];
        escape = 0;
        result.push_back(ch);
        break;
    }
  }

  return result;
}

std::vector<std::pair<std::string, std::string>> DecodeURIQuery(
    const StringRef& input) {
  std::vector<std::pair<std::string, std::string>> result;

  std::string key, value;
  bool in_key = true;
  int escape = 0;
  unsigned char decode_buffer = 0;

  for (char ch : input) {
    switch (escape) {
      case 0:

        if (ch == '%') {
          escape = 1;
          continue;
        }

        if (ch == '&') {
          if (!key.empty()) {
            if (in_key) {
              result.emplace_back(std::move(key), std::string());
            } else {
              result.emplace_back(std::move(key), std::move(value));
              value.clear();
            }
            key.clear();
          }
          in_key = true;

          continue;
        }

        if (in_key && ch == '=') {
          in_key = false;

          continue;
        }

        if (in_key)
          key.push_back(ch);
        else
          value.push_back(ch);

        break;

      case 1:

        decode_buffer = kHexHelper[ch & 0x1f] << 4;
        escape = 2;

        break;

      case 2:

        ch = decode_buffer | kHexHelper[ch & 0x1f];
        escape = 0;

        if (in_key)
          key.push_back(ch);
        else
          value.push_back(ch);

        break;
    }
  }

  if (!key.empty()) result.emplace_back(std::move(key), std::move(value));

  return result;
}

void ToJSON(const ev::StringRef& input, std::string& output) {
  static const char kHexDigit[] = "0123456789abcdef";

  output.push_back('"');

  for (auto sch : input) {
    auto ch = static_cast<uint8_t>(sch);

    switch (ch) {
      case '\\':
        break;
      case '"':
        ch = '"';
        break;
      case '\a':
        ch = 'a';
        break;
      case '\b':
        ch = 'b';
        break;
      case '\t':
        ch = 't';
        break;
      case '\n':
        ch = 'n';
        break;
      case '\v':
        ch = 'v';
        break;
      case '\f':
        ch = 'f';
        break;
      case '\r':
        ch = 'r';
        break;

      default:

        if (ch < ' ') {
          output.push_back('\\');
          output.push_back('u');
          output.push_back('0');
          output.push_back('0');
          output.push_back(kHexDigit[ch >> 4]);
          output.push_back(kHexDigit[ch & 0xf]);
        } else {
          output.push_back(ch);
        }

        continue;
    }

    output.push_back('\\');
    output.push_back(ch);
  }

  output.push_back('"');
}

}  // namespace ev
