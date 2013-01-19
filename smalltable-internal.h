
enum TABLE_flags
{
  TABLE_FLAG_ORDERED = 0x0001
};

struct TABLE_header
{
  uint64_t magic;
  uint8_t major_version;
  uint8_t minor_version;
  uint16_t flags;
  uint32_t data_crc32;
  uint64_t index_offset;
};

struct table
{
  char *path;
  char *tmp_path;

  int fd;

  char *buffer;
  size_t buffer_size, buffer_fill;

  uint64_t write_offset;
  char *prev_key;
  uint64_t prev_time;

  uint32_t crc32;
  uint16_t flags;

  struct TABLE_header *header;

  uint64_t *entries;
  size_t entry_alloc, entry_count;

  int no_relative;
};

void
TABLE_flush (struct table *t);

void
TABLE_write (struct table *t, const void *data, size_t size);

void
TABLE_put_integer (struct table *t, uint64_t value);

#define TABLE_putc(t, c) do { if ((t)->buffer_fill == (t)->buffer_size) TABLE_flush ((t)); (t)->buffer[(t)->buffer_fill++] = (c); } while (0)
