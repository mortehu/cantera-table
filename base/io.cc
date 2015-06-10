#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "base/io.h"

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <kj/io.h>

#include "base/file.h"

namespace ev {

namespace {

bool StatIsRotational(struct stat sb) {
  char device_path[64];  // Will hold any /sys/dev/block/%u:%u
  KJ_SYSCALL(sprintf(device_path, "/sys/dev/block/%u:%u", major(sb.st_dev),
                     minor(sb.st_dev)));

  kj::AutoCloseFd dirfd(OpenFile(device_path, O_RDONLY | O_DIRECTORY));

  size_t max_iter = 10;
  while (max_iter--) {
    kj::AutoCloseFd fd(openat(dirfd.get(), "queue/rotational", O_RDONLY));

    if (fd == nullptr) {
      if (errno != ENOENT) KJ_FAIL_SYSCALL("open", errno);
      dirfd = OpenFile(dirfd.get(), "..", O_RDONLY | O_DIRECTORY);
      continue;
    }

    char buf;
    KJ_SYSCALL(read(fd.get(), &buf, 1));

    return buf == '1';
  }

  KJ_FAIL_REQUIRE("queue/rotational not found");
}

}  // namespace

bool PathIsRotational(int fd) {
  struct stat sb;
  KJ_SYSCALL(fstat(fd, &sb));
  return StatIsRotational(sb);
}

bool PathIsRotational(const char* path) {
  struct stat sb;
  KJ_SYSCALL(stat(path, &sb));
  return StatIsRotational(sb);
}

}  // namespace ev
