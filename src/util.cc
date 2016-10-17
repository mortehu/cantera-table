#if HAVE_CONFIG_H
#include "config.h"
#endif

#include "src/util.h"

#include <cstring>
#include <memory>
#include <random>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <kj/debug.h>

namespace cantera {
namespace table {
namespace internal {

/*****************************************************************************/

kj::AutoCloseFd OpenFile(const char* path, int flags, int mode) {
  const auto fd = open(path, flags, mode);
  if (fd == -1) KJ_FAIL_SYSCALL("open", errno, path, flags, mode);
  return kj::AutoCloseFd(fd);
}

kj::AutoCloseFd AnonTemporaryFile(const char* path, int mode) {
  TemporaryFile file(path, mode);
  file.Unlink();
  return std::move(file);
}

/*****************************************************************************/

TemporaryFile::~TemporaryFile() noexcept {
#if !defined(O_TMPFILE)
  try {
    Unlink();
  } catch (...) {
    KJ_LOG(ERROR, "failed to remove a temporary file", temp_path_);
  }
#endif
  try {
    Close();
  } catch (...) {
  }
}

void TemporaryFile::Make(const char* path, int mode) {
  if (path == nullptr) {
    path = std::getenv("TMPDIR");
#ifdef P_tmpdir
    if (path == nullptr) path = P_tmpdir;
#endif
    if (path == nullptr) path = "/tmp";
  }

#ifdef O_TMPFILE
  kj::AutoCloseFd fd = OpenFile(path, O_TMPFILE | O_RDWR, mode);
  kj::AutoCloseFd::operator=(std::move(fd));
#else
  static const char suffix[] = "/ca-table.tmp.XXXXXX";

  size_t len = std::strlen(path);
  auto pathname = std::make_unique<char[]>(len + sizeof(suffix));
  std::memcpy(pathname.get(), path, len);
  std::memcpy(pathname.get() + len, suffix, sizeof(suffix));

  int fd;
  KJ_SYSCALL(fd = mkstemp(pathname.get()), pathname.get());
  kj::AutoCloseFd::operator=(kj::AutoCloseFd(fd));

#ifdef PARANOID
  try {
    temp_path_ = pathname.get();
  } catch (...) {
    unlink(pathname.get());
  }
#else
  temp_path_ = pathname.get();
#endif
#endif
}

/*****************************************************************************/

#if defined(O_TMPFILE)
static void MakeRandomString(char* output, size_t length) {
  struct timeval now;
  gettimeofday(&now, nullptr);
  std::minstd_rand rng;
  rng.seed(now.tv_sec * UINT64_C(1'000'000) + now.tv_usec);

  static const char kLetters[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  std::uniform_int_distribution<uint8_t> rng_dist(0, strlen(kLetters) - 1);

  while (length--) *output++ = kLetters[rng_dist(rng)];
}
#endif

PendingFile::PendingFile(const char* path, int mode)
#if !defined(O_TMPFILE)
    : path_(path), mode_(mode) {
#else
    : path_(path) {
#endif
  std::string base(".");
  if (const char* last_slash = std::strrchr(path, '/')) {
    KJ_REQUIRE(path != last_slash);
    base = std::string(path, last_slash - 1);
  }

  Make(base.data(), mode);
}

void PendingFile::Finish() {
  KJ_CONTEXT(path_);
#if defined(O_TMPFILE)
  char temp_path[32];
  snprintf(temp_path, sizeof(temp_path), "/proc/self/fd/%d", get());
  auto ret =
      linkat(AT_FDCWD, temp_path, AT_FDCWD, path_.data(), AT_SYMLINK_FOLLOW);
  if (ret == 0) return;
  if (errno != EEXIST) {
    const auto linkat_errno = errno;
    KJ_SYSCALL(access("/proc", X_OK), "/proc is not available");
    KJ_FAIL_SYSCALL("linkat", linkat_errno, temp_path);
  }

  // Target already exists, so we need an intermediate filename to atomically
  // replace with rename().
  std::string intermediate_path = path_;
  intermediate_path += ".XXXXXX";

  static const size_t kMaxAttempts = 62 * 62 * 62;

  for (size_t i = 0; i < kMaxAttempts; ++i) {
    MakeRandomString(&intermediate_path[intermediate_path.size() - 6], 6);

    ret = linkat(AT_FDCWD, temp_path, AT_FDCWD, intermediate_path.c_str(),
                 AT_SYMLINK_FOLLOW);

    if (ret == 0) {
      KJ_SYSCALL(
          renameat(AT_FDCWD, intermediate_path.data(), AT_FDCWD, path_.data()),
          intermediate_path, path_);
      return;
    }

    if (errno != EEXIST)
      KJ_FAIL_SYSCALL("linkat", errno, intermediate_path, path_);
  }

  KJ_FAIL_REQUIRE("all temporary file creation attempts failed", kMaxAttempts);
#else
  KJ_REQUIRE(!temp_path_.empty(), "file has already been renamed or removed");

  auto mask = umask(0);
  umask(mask);
  if ((mode_ & ~mask) != ((S_IRUSR | S_IWUSR) & ~mask))
    KJ_SYSCALL(fchmod(get(), mode_ & ~mask), temp_path_);

  KJ_SYSCALL(rename(temp_path_.data(), path_.data()), temp_path_, path_);
#endif

  Reset();
}

/*****************************************************************************/

size_t ReadWithOffset(int fd, void* dest, size_t size_min, size_t size_max,
                      off_t offset) {
  size_t result = 0;

  while (size_max > 0) {
    ssize_t ret;
    KJ_SYSCALL(ret = pread(fd, dest, size_max, offset));
    if (ret == 0) break;
    result += ret;
    size_max -= ret;
    offset += ret;
    dest = reinterpret_cast<char*>(dest) + ret;
  }

  KJ_REQUIRE(result >= size_min, "unexpectedly reached end of file", offset, result, size_min);

  return result;
}

uint64_t Hash(const string_view& key) {
  static const auto mul = (UINT64_C(0xc6a4a793) << 32) + UINT64_C(0x5bd1e995);

  static_assert(sizeof(uint64_t) == 8, "uint64_t must be 8 bytes");

  auto shift_mix = [](uint64_t v) -> uint64_t { return v ^ (v >> 47); };

  auto len = key.size();
  auto buf = key.data();

  const auto len_aligned = len & ~7;
  const auto end = buf + len_aligned;
  uint64_t hash = 0xc70f6907 ^ (len * mul);
  auto p = buf;

  for (; p != end; p += 8) {
    uint64_t tmp;
    memcpy(&tmp, p, sizeof(uint64_t));
    const auto data = shift_mix(tmp * mul) * mul;
    hash ^= data;
    hash *= mul;
  }

  len &= 7;

  if (len) {
    p += len;
    uint64_t data = 0;
    while (len--) data = (data << 8) + static_cast<unsigned char>(*--p);

    hash ^= data;
    hash *= mul;
  }

  hash = shift_mix(hash) * mul;
  hash = shift_mix(hash);

  return hash;
}

}  // namespace internal
}  // namespace table
}  // namespace cantera
