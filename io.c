#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <err.h>
#include <fcntl.h>
#include <sysexits.h>
#include <unistd.h>

#include "io.h"

void
write_all (int fd, const void *data, size_t size)
{
  const char *cdata = data;
  ssize_t ret = 1;

  while (size)
    {
      if (0 > (ret = write (fd, cdata, size)))
        {
          if (ret == 0)
            errx (EX_IOERR, "write returned 0");

          err (EX_IOERR, "write failed");
        }

      size -= ret;
      cdata += ret;
    }
}
