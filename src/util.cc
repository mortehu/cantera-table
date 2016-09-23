#if HAVE_CONFIG_H
#include "config.h"
#endif

#include "src/util.h"

#include <cstring>
#include <memory>
#include <random>

#include <fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include <kj/debug.h>

namespace cantera {
namespace table {
namespace internal {

namespace {

void MakeRandomString(char* output, size_t length) {
  struct timeval now;
  gettimeofday(&now, nullptr);
  std::minstd_rand rng;
  rng.seed(now.tv_sec * UINT64_C(1'000'000) + now.tv_usec);

  static const char kLetters[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  std::uniform_int_distribution<uint8_t> rng_dist(0, strlen(kLetters) - 1);

  while (length--) *output++ = kLetters[rng_dist(rng)];
}

}  // namespace

kj::AutoCloseFd OpenFile(const char* path, int flags, int mode) {
  const auto fd = open(path, flags, mode);
  if (fd == -1) KJ_FAIL_SYSCALL("open", errno, path, flags, mode);
  return kj::AutoCloseFd(fd);
}

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

kj::AutoCloseFd AnonTemporaryFile(const char* path, int mode) {
#ifdef O_TMPFILE
  return OpenFile(path, O_TMPFILE | O_RDWR, mode);
#else
  size_t pathlen = std::strlen(path);
  static const char suffix[] = "/ca-table-XXXXXX";
  std::unique_ptr<char[]> pathname(new char[pathlen + sizeof(suffix)]);
  std::memcpy((void *)(pathname.get()), path, pathlen);
  std::memcpy((void *)(pathname.get() + pathlen), suffix, sizeof(suffix));

  int fd;
  KJ_SYSCALL(fd = ::mkstemp(pathname.get()), pathname.get());

  kj::AutoCloseFd acfd(fd);
  KJ_SYSCALL(unlink(pathname.get()), pathname.get());

  return acfd;
#endif
}

void LinkAnonTemporaryFile(int fd, const char* path) {
  LinkAnonTemporaryFile(AT_FDCWD, fd, path);
}

void LinkAnonTemporaryFile(int dir_fd, int fd, const char* path) {
  KJ_CONTEXT(path);
  char temp_path[32];
  snprintf(temp_path, sizeof(temp_path), "/proc/self/fd/%d", fd);
  auto ret = linkat(AT_FDCWD, temp_path, dir_fd, path, AT_SYMLINK_FOLLOW);
  if (ret == 0) return;
  if (errno != EEXIST) {
    const auto linkat_errno = errno;
    KJ_SYSCALL(access("/proc", X_OK), "/proc is not available");
    KJ_FAIL_SYSCALL("linkat", linkat_errno, temp_path);
  }

  // Target already exists, so we need an intermediate filename to atomically
  // replace with rename().
  std::string intermediate_path = path;
  intermediate_path += ".XXXXXX";

  static const size_t kMaxAttempts = 62 * 62 * 62;

  for (size_t i = 0; i < kMaxAttempts; ++i) {
    MakeRandomString(&intermediate_path[intermediate_path.size() - 6], 6);

    ret = linkat(AT_FDCWD, temp_path, dir_fd, intermediate_path.c_str(),
                 AT_SYMLINK_FOLLOW);

    if (ret == 0) {
      KJ_SYSCALL(renameat(dir_fd, intermediate_path.c_str(), dir_fd, path),
                 intermediate_path, path);
      return;
    }

    if (errno != EEXIST) {
      KJ_FAIL_SYSCALL("linkat", errno, intermediate_path, path);
    }
  }

  KJ_FAIL_REQUIRE("all temporary file creation attempts failed", kMaxAttempts);
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
