#include "base/socket.h"

#include <utility>

#include <fcntl.h>
#include <linux/tcp.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <kj/debug.h>

namespace ev {

std::vector<kj::AutoCloseFd> tcp_listen(const char* address,
                                        const char* service) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;

  struct addrinfo* addrs = nullptr;
  auto ret = getaddrinfo(address, service, &hints, &addrs);

  if (ret)
    KJ_FAIL_REQUIRE("getaddrinfo failed", address, service, gai_strerror(ret));

  std::vector<kj::AutoCloseFd> result;

  for (auto addr = addrs; addr; addr = addr->ai_next) {
    kj::AutoCloseFd listen_fd(socket(
        addr->ai_family, addr->ai_socktype | SOCK_CLOEXEC, addr->ai_protocol));

    if (listen_fd == nullptr) KJ_FAIL_SYSCALL("socket", errno);

    int one = 1;
    KJ_SYSCALL(setsockopt(listen_fd.get(), SOL_SOCKET, SO_REUSEADDR, &one,
                          sizeof(one)));
    KJ_SYSCALL(setsockopt(listen_fd.get(), IPPROTO_TCP, TCP_NODELAY, &one,
                          sizeof(one)));

    KJ_SYSCALL(bind(listen_fd.get(), addr->ai_addr, addr->ai_addrlen));

    KJ_SYSCALL(listen(listen_fd.get(), 8));
    result.emplace_back(std::move(listen_fd));
  }

  freeaddrinfo(addrs);

  return result;
}

std::pair<kj::AutoCloseFd, kj::AutoCloseFd> pipe(bool close_on_exec) {
  int pipe_fds[2];
  KJ_SYSCALL(::pipe2(pipe_fds, close_on_exec ? O_CLOEXEC : 0));
  return std::pair<kj::AutoCloseFd, kj::AutoCloseFd>{
      kj::AutoCloseFd(pipe_fds[0]), kj::AutoCloseFd(pipe_fds[1])};
}

}  // namespace ev
