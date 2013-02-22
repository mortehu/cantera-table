#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <err.h>
#include <getopt.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sysexits.h>
#include <unistd.h>

#include "ca-table.h"

#define LOCK_FILENAME "lock"

static int print_version;
static int print_help;
static int keep_running;

static struct option long_options[] =
{
    { "keep-running",   no_argument,  &keep_running,     1 },
    { "version",        no_argument,  &print_version, 1 },
    { "help",           no_argument,  &print_help,    1 },
    { 0, 0, 0, 0 }
};

/* We allocate these with constant size to avoid heap fragmentation, even if
 * the data may vary in length */
struct lock
{
  char data[CA_LOCK_MESSAGE_SIZE];
  struct lock *next;

  int fd;
};

static struct lock *first_lock;
static struct lock **last_lock_pointer = &first_lock;

static struct lock *first_pending_lock;
static struct lock **last_pending_lock_pointer = &first_pending_lock;

static struct lock *lock_buffer;

static size_t client_count;

static void
process_pending_locks (void)
{
  unsigned char lock_granted = 0x00;
  struct msghdr msghdr;
  struct iovec iov;

  struct lock *i;

  if (!first_pending_lock)
    return;

  if (first_lock)
    {
      if (first_lock->data[0] == CA_LOCK_GLOBAL)
        return;

      if (first_pending_lock->data[0] == CA_LOCK_GLOBAL)
        return;

      for (i = first_lock; i; i = i->next)
        {
          /* Are the locks for different tables?  */
          if (i->data[1] != first_pending_lock->data[1]
              || memcmp (&i->data[4], &first_pending_lock->data[4],
                         i->data[1]))
            continue;

          /* XXX: Add support for value range locks */

          /* Are the locks for different columns?  If so, we don't know if the
           * ranges are mutually exclusive, and must assume they are not.  */

          return;
        }
    }

  i = first_pending_lock;
  first_pending_lock = i->next;

  if (!first_pending_lock)
    last_pending_lock_pointer = &first_pending_lock;

  *last_lock_pointer = i;
  i->next = NULL;

  last_lock_pointer = &i->next;

  memset (&msghdr, 0, sizeof (msghdr));
  iov.iov_base = &lock_granted;
  iov.iov_len = 1;
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;

  sendmsg (i->fd, &msghdr, 0);
}

static void
release_locks (int fd)
{
  struct lock *i, **pointer;
  int dirty = 0;

  /* Clear pending locks */

  pointer = &first_pending_lock;

  for (i = first_pending_lock; i; )
    {
      if (i->fd == fd)
        {
          *pointer = i->next;

          free (i);
          i = *pointer;

          dirty = 1;
        }
      else
        {
          pointer = &i->next;
          i = i->next;
        }
    }

  last_pending_lock_pointer = pointer;

  /* Clear held locks */

  pointer = &first_lock;

  for (i = first_lock; i; )
    {
      if (i->fd == fd)
        {
          *pointer = i->next;

          free (i);
          i = *pointer;

          dirty = 1;
        }
      else
        {
          pointer = &i->next;
          i = i->next;
        }
    }

  last_lock_pointer = pointer;

  if (dirty)
    process_pending_locks ();
}

static void
cleanup_and_exit (void)
{
  unlink (LOCK_FILENAME);

  exit (EXIT_SUCCESS);
}

static void
process_client (int fd)
{
  struct msghdr msghdr;
  struct iovec iov;

  /* We don't discard an allocated lock_buffer before we've actually used it */
  if (!lock_buffer
      && !(lock_buffer = malloc (sizeof (*lock_buffer))))
    goto fail;

  memset (&msghdr, 0, sizeof (msghdr));
  iov.iov_base = lock_buffer->data;
  iov.iov_len = sizeof (lock_buffer->data);
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;

  if (0 >= recvmsg (fd, &msghdr, 0))
    goto fail;

  if ((uint8_t) lock_buffer->data[0] == 0xff)
    {
      release_locks (fd);

      return;
    }

  lock_buffer->next = NULL;
  lock_buffer->fd = fd;

  *last_pending_lock_pointer = lock_buffer;
  last_pending_lock_pointer = &lock_buffer->next;

  lock_buffer = NULL;

  process_pending_locks ();

  return;

fail:

  close (fd);

  release_locks (fd);

  if (!--client_count && !keep_running)
    cleanup_and_exit ();
}

static void
sighandler (int signal)
{
  cleanup_and_exit ();
}

int
main (int argc, char **argv)
{
  struct epoll_event ev;
  int i, unixfd, epollfd;

  while ((i = getopt_long (argc, argv, "c:", long_options, 0)) != -1)
    {
      switch (i)
        {
        case 0:

          break;

        case '?':

          errx (EX_USAGE, "Try '%s --help' for more information.", argv[0]);
        }
    }

  if (print_help)
    {
      printf ("Usage: %s [OPTION]...\n"
             "\n"
             "      --keep-running              don't exit when last client disconnects\n"
             "      --help     display this help and exit\n"
             "      --version  display version information and exit\n"
             "\n"
             "Report bugs to <morten.hustveit@gmail.com>\n",
             argv[0]);

      return EXIT_SUCCESS;
    }

  if (print_version)
    {
      fprintf (stdout, "%s\n", PACKAGE_STRING);

      return EXIT_SUCCESS;
    }

  if (optind + 1 != argc)
    errx (EX_USAGE, "Usage: %s [OPTION]... SCHEMA_PATH", argv[0]);

  if (-1 == chdir (argv[optind]))
    errx (EXIT_FAILURE, "Unable to change directory to '%s': %s",
          argv[optind], strerror (errno));

  if (-1 == (unixfd = socket (PF_UNIX, SOCK_STREAM, 0)))
    errx (EXIT_FAILURE, "Failed to create UNIX socket: %s", strerror (errno));

  struct sockaddr_un unixaddr;
  memset (&unixaddr, 0, sizeof (unixaddr));
  unixaddr.sun_family = AF_UNIX;
  strcpy (unixaddr.sun_path, LOCK_FILENAME);

  if (-1 == bind (unixfd, (struct sockaddr*) &unixaddr, sizeof (unixaddr)))
    err (EXIT_FAILURE, "Failed to bind UNIX socket to address");

  signal (SIGINT, sighandler);
  signal (SIGTERM, sighandler);

  if (-1 == listen (unixfd, SOMAXCONN))
    err (EXIT_FAILURE, "Failed to listen on UNIX socket");

  if (-1 == (epollfd = epoll_create1 (EPOLL_CLOEXEC)))
    err (EXIT_FAILURE, "Failed to create epoll descriptor");

  ev.events = EPOLLIN;
  ev.data.fd = unixfd;

  if (-1 == epoll_ctl (epollfd, EPOLL_CTL_ADD, unixfd, &ev))
    err (EXIT_FAILURE, "Failed to add UNIX socket to epoll");

  if (-1 == daemon (1 /* nochdir */, 0))
    err (EXIT_FAILURE, "Failed to become a daemon");

  for (;;)
    {
      int nfds;

      if (-1 == (nfds = epoll_wait (epollfd, &ev, 1, -1)))
        {
          if (errno == EINTR)
            continue;

          break;
        }

      if (!nfds)
        continue;

      if (ev.data.fd == unixfd)
        {
          int fd;

          if (-1 == (fd = accept (unixfd, NULL, NULL)))
            continue;

          ev.events = EPOLLIN;
          ev.data.fd = fd;

          if (-1 == epoll_ctl (epollfd, EPOLL_CTL_ADD, fd, &ev))
            close (fd);
          else
            ++client_count;
        }
      else
        process_client (ev.data.fd);
    }

  return EXIT_FAILURE;
}
