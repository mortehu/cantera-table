#include "base/base64.h"

#include <kj/debug.h>

namespace ev {

namespace {

unsigned char kBase64DecodeMap[] = {
    0x3e, 0xff, 0xff, 0xff, 0x3f, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a,
    0x3b, 0x3c, 0x3d, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01,
    0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
    0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b,
    0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33};

}  // namespace

const char kBase64Chars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const char kBase64WebSafeChars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

void ToBase64(const ev::StringRef& input, std::string& output,
              const char* alphabet) {
  if (input.empty()) return;

  auto orig_output_size = output.size();
  output.reserve(output.size() + input.size() * 4 / 3 + 12);

  auto in = reinterpret_cast<const uint8_t*>(input.data());
  auto remaining = input.size();
  unsigned int i_bits = 0;
  int i_shift = 0;

  while (remaining) {
    // Consume one byte.
    i_bits = (i_bits << 8) + *in++;
    --remaining;
    i_shift += 8;

    // Output 6 bits at a time.  Keep going if last input byte has been
    // consumed.
    do {
      output.push_back(alphabet[(i_bits << 6 >> i_shift) & 0x3f]);
      i_shift -= 6;
    } while (i_shift > 6 || (!remaining && i_shift > 0));
  }

  // Apply padding needed by some decoders.
  while ((output.size() - orig_output_size) & 3) output.push_back('=');
}

unsigned char* Base64ToBinary(const ev::StringRef& input,
                              unsigned char* output) {
  unsigned int v = 0, o = 0;

  for (auto i = input.begin(); i != input.end() && *i != '='; ++i) {
    if (std::isspace(*i)) continue;

    const auto index = *i - 43;

    KJ_REQUIRE(index < ARRAY_SIZE(kBase64DecodeMap));
    KJ_REQUIRE(kBase64DecodeMap[index] != 0xff);

    v = (v << 6) + kBase64DecodeMap[index];

    if (o & 3) *output++ = v >> (6 - 2 * (o & 3));
    ++o;
  }

  return output;
}

}  // namespace ev
