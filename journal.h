#ifndef JOURNAL_H_
#define JOURNAL_H_ 1

#include <stdio.h>

enum journal_operation
{
  JOURNAL_TRUNCATE = 1,
  JOURNAL_CREATE_FILE = 2
};

void
journal_init (const char *path);

int
journal_file_open (const char *name);

int
journal_file_rewrite (int handle);

off_t
journal_file_size (int handle);

void
journal_file_append (int handle, const void *data, size_t size);

void
journal_file_map (int handle, void **map, size_t *size, int prot, int flags);

void
journal_flush (void);

void
journal_commit (void);

#endif /* !JOURNAL_H_ */
