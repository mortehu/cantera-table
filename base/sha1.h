#ifndef BASE_SHA1_H_
#define BASE_SHA1_H_ 1

#include <cstddef>
#include <cstdint>
#include <string>

#include <openssl/sha.h>

#include "base/stringref.h"

namespace ev {

class SHA1 {
 public:
  // Constructs a 40 character hexadecimal string.
  static std::string ToASCII(const uint8_t* sha1);

  static void Digest(const void* data, size_t size, uint8_t* digest) {
    SHA1 sha1;
    sha1.Add(data, size);
    sha1.Finish(digest);
  }

  static void Digest(const StringRef& buffer, uint8_t* digest) {
    Digest(buffer.data(), buffer.size(), digest);
  }

  SHA1();

  void Add(const void* data, size_t size);

  void Finish(uint8_t* digest);

 private:
  SHA_CTX sha_ctx_;
};

}  // namespace ev

#endif /* !BASE_SHA1_H_ */
