#include "base/stringref.h"

namespace ev {

extern const char kBase64Chars[];
extern const char kBase64WebSafeChars[];

void ToBase64(const ev::StringRef& input, std::string& output,
              const char* alphabet = kBase64Chars);

unsigned char* Base64ToBinary(const ev::StringRef& input, unsigned char* output);

}  // namespace ev
