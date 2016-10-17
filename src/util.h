#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <experimental/string_view>

#include <fcntl.h>
#include <unistd.h>

#include <kj/debug.h>
#include <kj/io.h>

namespace cantera {

using string_view = std::experimental::string_view;

namespace table {
namespace internal {

kj::AutoCloseFd OpenFile(const char* path, int flags, int mode = 0666);

// Creates an unnamed temporary file in the given directory.
kj::AutoCloseFd AnonTemporaryFile(const char* path = nullptr,
                                  int mode = S_IRUSR | S_IWUSR);

size_t ReadWithOffset(int fd, void* dest, size_t size_min, size_t size_max,
                      off_t offset);

inline void ReadWithOffset(int fd, void* dest, size_t size, off_t offset) {
  ReadWithOffset(fd, dest, size, size, offset);
}

// A temporary file in a given directory.
class TemporaryFile : public kj::AutoCloseFd {
 public:
  TemporaryFile() {}

  TemporaryFile(const char* path, int flags, mode_t mode, bool unlink = true) {
    Make(path, flags, mode);
    if (unlink) Unlink();
  }

  ~TemporaryFile() noexcept;

  void Make(const char* path, int flags, mode_t mode);

  void Unlink() {
#if !defined(O_TMPFILE)
    if (!temp_path_.empty()) {
      KJ_SYSCALL(unlink(temp_path_.data()), temp_path_);
      Reset();
    }
#endif
  }

  void Close() { kj::AutoCloseFd::operator=(nullptr); }

 protected:
#if !defined(O_TMPFILE)
  void Reset() { temp_path_.clear(); }

  std::string temp_path_;
#endif
};

// A temporary file in a given directory that can be made persistent if needed.
class PendingFile : public TemporaryFile {
 public:
  PendingFile(const char* path, int flags, mode_t mode);

  void Finish();

 private:
  std::string path_;

#if !defined(O_TMPFILE)
  mode_t mode_;
#endif
};

// Our own hash algorithm, used instead of std::hash for predictable result.
uint64_t Hash(const string_view& key);

// Returns true iff lhs < rhs, assuming both strings are UTF-8 encoded.
inline bool CompareUTF8(const string_view& lhs, const string_view& rhs) {
  return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(),
                                      rhs.end(), [](char lhs, char rhs) {
    return static_cast<uint8_t>(lhs) < static_cast<uint8_t>(rhs);
  });
}

inline uint64_t StringToUInt64(const char* string) {
  static_assert(sizeof(unsigned long) == sizeof(uint64_t), "");
  KJ_REQUIRE(*string != 0);
  char* endptr = nullptr;
  errno = 0;
  const auto value = std::strtoul(string, &endptr, 0);
  KJ_REQUIRE(*endptr == 0, "unexpected character in numeric string", string);
  if (errno != 0) {
    KJ_FAIL_SYSCALL("strtoul", errno, string);
  }
  return value;
}

inline std::string StringPrintf(const char* format, ...) {
  va_list args;
  char* buf;

  va_start(args, format);

  KJ_SYSCALL(vasprintf(&buf, format, args));

  std::string result(buf);
  free(buf);

  return result;
}

inline bool HasPrefix(const string_view& haystack,
                      const string_view& needle) {
  if (haystack.size() < needle.size()) return false;
  return std::equal(needle.begin(), needle.end(), haystack.begin());
}

// Returns a string value that can be losslessly converted back to a float.
inline std::string FloatToString(const float v) {
  if (!v) return "0";

  if ((v >= 1e-6 || v <= -1e-6) && v < 1e9 && v > -1e9) {
    for (int prec = 0; prec < 9; ++prec) {
      auto result = StringPrintf("%.*f", prec, v);
      auto test_v = std::strtof(result.c_str(), nullptr);
      if (test_v == v) return result;
    }
  }

  return StringPrintf("%.9g", v);
}

// Returns a string value that can be losslessly converted back to a double.
inline std::string DoubleToString(const double v) {
  if (!v) return "0";

  if ((v >= 1e-6 || v <= -1e-6) && v < 1e17 && v > -1e17) {
    for (int prec = 0; prec < 17; ++prec) {
      auto result = StringPrintf("%.*f", prec, v);
      auto test_v = std::strtod(result.c_str(), nullptr);
      if (test_v == v) return result;
    }
  }

  return StringPrintf("%.17g", v);
}

inline std::string DecodeURIComponent(const string_view& input) {
  static const unsigned char kHexHelper[26] = {0, 10, 11, 12, 13, 14, 15, 0, 0,
    0, 0,  0,  0,  0,  0,  0,  0, 1,
    2, 3,  4,  5,  6,  7,  8,  9};

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

// Encodes `input` as a JSON string, and appends the result to `output`.
inline void ToJSON(const string_view& input, std::string& output) {
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

}  // namespace internal
}  // namespace table
}  // namespace cantera
