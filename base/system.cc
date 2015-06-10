#include "base/system.h"

#include <cerrno>

#include <unistd.h>
#include <linux/limits.h>

#include <kj/debug.h>

#include "base/error.h"

namespace ev {

std::string HostName() {
  char buf[128];

  int ret;
  KJ_SYSCALL(ret = gethostname(buf, sizeof(buf)));
  KJ_REQUIRE(static_cast<size_t>(ret) + 1 < sizeof(buf), ret);

  return buf;
}

std::string ReadLink(const char* path) {
  char buf[PATH_MAX];

  ssize_t ret = readlink(path, buf, sizeof(buf));
  if (ret < 0) EV_FAIL_SYSCALL("readlink", path);
  KJ_REQUIRE(static_cast<size_t>(ret) + 1 < sizeof(buf), ret);

  KJ_SYSCALL(ret = readlink(path, buf, sizeof(buf)));
  buf[ret] = 0;

  return buf;
}

}  // namespace ev
