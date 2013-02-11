#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <assert.h>
#include <errno.h>
#include <string.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ca-table.h"

static int lockd_fd = -1;

static int
spawn_lockd (const char *schema_path)
{
  char *program_name, *args[3];
  int status;
  pid_t pid;

  program_name = BINDIR "/ca-table-lockd";

  if (-1 == asprintf (&args[0], PACKAGE ": lock daemon: %s", schema_path))
    {
      ca_set_error ("asprintf failed: %s", strerror (errno));

      return -1;
    }

  args[1] = (char *) schema_path;
  args[2] = NULL;

  if (-1 == (pid = vfork ()))
    return -1;

  if (!pid)
    {
      execve (program_name, args, environ);

      _exit (EXIT_FAILURE);
    }

  free (args[0]);

  while (-1 == waitpid (pid, &status, 0))
    {
      if (errno != EINTR)
        {
          ca_set_error ("Error waiting for lock daemon to start: %s", strerror (errno));

          return -1;
        }
    }

  if (!WIFEXITED (status) || WEXITSTATUS (status))
    {
      ca_set_error ("Lock daemon exited abormally (%d)", status);

      return -1;
    }

  return 0;
}

int
ca_lock_init (const char* schema_path)
{
  struct sockaddr_un unixaddr;

  assert (lockd_fd == -1);

  if (-1 == (lockd_fd = socket (PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)))
    {
      ca_set_error ("Failed to create UNIX socket: %s", strerror (errno));

      return -1;
    }

  memset (&unixaddr, 0, sizeof (unixaddr));
  unixaddr.sun_family = AF_UNIX;
  snprintf (unixaddr.sun_path,
            sizeof (unixaddr.sun_path) - 1,
            "%s/lock", schema_path);

  while (-1 == connect (lockd_fd, (struct sockaddr*) &unixaddr, sizeof (unixaddr)))
    {
      if (errno != ENOENT && errno != ECONNREFUSED)
        {
          ca_set_error ("Failed to connect to lock daemon: %s", strerror (errno));

          close (lockd_fd);
          lockd_fd = -1;

          return -1;
        }

      if (-1 == spawn_lockd (schema_path))
        return -1;
    }

  return 0;
}

int
ca_lock_grab (const char *table_name,
              const char *column_name,
              enum ca_lock_direction direction,
              const void *value,
              size_t size)
{
  char msg[CA_LOCK_MESSAGE_SIZE];
  struct msghdr msghdr;
  struct iovec iov;

  size_t msg_size, msg_fill = 0;
  size_t table_name_length, column_name_length = 0;

  int ret;

  /* Check to see if the requested lock will fit in a message.  If it doesn't,
   * switch to a more general lock */

  if (direction >= CA_LOCK_TABLE)
    {
      table_name_length = strlen (table_name);

      if (direction > CA_LOCK_TABLE)
        {
          column_name_length = strlen (column_name);

          msg_size = 4
                   + table_name_length
                   + column_name_length
                   + size;

          if (msg_size > sizeof (msg))
            direction = CA_LOCK_TABLE;
        }

      if (direction == CA_LOCK_TABLE)
        {
          msg_size = 4 + table_name_length;

          if (msg_size > sizeof (msg))
            direction = CA_LOCK_GLOBAL;
        }
    }

  if (direction == CA_LOCK_GLOBAL)
    msg_size = 1;

  /* Build the lock request message */

  msg[msg_fill++] = direction;

  if (direction >= CA_LOCK_TABLE)
    {
      msg[msg_fill++] = table_name_length;
      msg[msg_fill++] = column_name_length;
      msg[msg_fill++] = size;

      memcpy (&msg[msg_fill], table_name, table_name_length);
      msg_fill += table_name_length;

      if (direction > CA_LOCK_TABLE)
        {
          memcpy (&msg[msg_fill], column_name, column_name_length);
          msg_fill += column_name_length;

          memcpy (&msg[msg_fill], value, size);
          msg_fill += size;
        }
    }

  assert (msg_fill == msg_size);

  /* Send the message */

  memset (&msghdr, 0, sizeof (msghdr));
  iov.iov_base = msg;
  iov.iov_len = msg_fill;
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;

  if (-1 == sendmsg (lockd_fd, &msghdr, 0))
    {
      ca_set_error ("Failed to send lock request: %s", strerror (errno));

      goto fail;
    }

  /* Wait for confirmation */

  memset (&msghdr, 0, sizeof (msghdr));
  iov.iov_base = msg;
  iov.iov_len = sizeof (msg);
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;

  while (0 >= (ret = recvmsg (lockd_fd, &msghdr, 0)))
    {
      if (!ret)
        ca_set_error ("Lost connection to lock daemon");
      else
        {
          if (errno == EINTR)
            continue;

          ca_set_error ("Error receiving lock confirmation: %s", strerror (errno));
        }

      goto fail;
    }

  if (iov.iov_len != 1)
    {
      ca_set_error ("Unexpected message length from lock daemon");

      return -1;
    }

  if (0x00 != msg[0])
    {
      ca_set_error ("Unexpected message content from lock daemon");

      return -1;
    }

  return 0;

fail:

  close (lockd_fd);
  lockd_fd = -1;

  return -1;
}

int
ca_lock_grab_table (const char *table_name)
{
  return ca_lock_grab (table_name, NULL, CA_LOCK_TABLE, NULL, 0);
}

int
ca_lock_grab_global (void)
{
  return ca_lock_grab (NULL, NULL, CA_LOCK_GLOBAL, NULL, 0);
}

int
ca_lock_release (void)
{
  unsigned char release = 0xff;
  struct msghdr msghdr;
  struct iovec iov;

  memset (&msghdr, 0, sizeof (msghdr));
  iov.iov_base = &release;
  iov.iov_len = 1;
  msghdr.msg_iov = &iov;
  msghdr.msg_iovlen = 1;

  if (-1 == sendmsg (lockd_fd, &msghdr, 0))
    {
      ca_set_error ("Failed to release lock: %s", strerror (errno));

      close (lockd_fd);
      lockd_fd = -1;

      return -1;
    }

  return 0;
}
