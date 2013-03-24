#ifndef TABLE_H_
#define TABLE_H_ 1

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CA_NAMEDATALEN 64

#ifdef __GNUC__
#  define ca_likely(x)       __builtin_expect((x),1)
#  define ca_unlikely(x)     __builtin_expect((x),0)
#  define CA_USE_RESULT      __attribute__((warn_unused_result))
#  define CA_PACKED          __attribute__((packed))
#else
#  define ca_likely(x)       (x)
#  define ca_unlikely(x)     (x)
#  define CA_USE_RESULT
#  define CA_PACKED
#endif

/*****************************************************************************/

const char *
ca_last_error (void);

void
ca_clear_error (void);

void
ca_set_error (const char *format, ...);

/*****************************************************************************/

#define CA_INVALID_XID ((uint64_t) -1)

extern uint64_t ca_xid; /* The current transaction ID */

/*****************************************************************************/

enum ca_type
{
  CA_INVALID            = -1,

  CA_TEXT               =  0,
  CA_UINT64             =  2,
  CA_INT64              =  3,
  CA_NUMERIC            =  4,
  CA_TIMESTAMPTZ        =  5,
  CA_OFFSET_SCORE_ARRAY =  6,
  CA_BOOLEAN            =  7,
  CA_UINT32             =  8,
  CA_INT32              =  9,
  CA_UINT16             = 10,
  CA_INT16              = 11,
  CA_UINT8              = 12,
  CA_INT8               = 13,
  CA_FLOAT4             = 14,
  CA_FLOAT8             = 15,
  CA_VOID               = 16
};

enum ca_type
ca_type_from_string (const char *string);

const char *
ca_type_to_string (enum ca_type type);

/*****************************************************************************/

/* Compression schemes for sorted offset/score pairs */
enum ca_offset_score_type
{
  /* Difference to previous value encoded using groups of 7 bit values, using
   * the byte MSB to indicate continuation.  Scores stored as float4.  */
  CA_OFFSET_SCORE_VARBYTE_FLOAT = 1,

  /* Same as CA_OFFSET_SCORE_VARBYTE_FLOAT, but the score is not stored, and
   * assumed to be zero.  */
  CA_OFFSET_SCORE_VARBYTE_ZERO = 2,

  /* Same as CA_OFFSET_SCORE_VARBYTE_FLOAT, except the score is stored as an
   * unsigned N-bit integer.  At the beginning of the data, a bias is stored
   * for the score values.  */
  CA_OFFSET_SCORE_VARBYTE_U8 = 3,
  CA_OFFSET_SCORE_VARBYTE_U16 = 4,
  CA_OFFSET_SCORE_VARBYTE_U24 = 5,

  CA_OFFSET_SCORE_FLEXI = 6
};

/*****************************************************************************/

enum ca_field_flag
{
  CA_FIELD_NOT_NULL =    0x0001,
  CA_FIELD_PRIMARY_KEY = 0x0002
};

struct ca_field
{
  enum ca_type type;
  char name[CA_NAMEDATALEN];
  uint32_t flags;
};

struct ca_table_declaration
{
  char *path;
  uint32_t field_count;
  struct ca_field *fields;
};

struct ca_offset_score
{
  uint64_t offset;
  float score;
} CA_PACKED;

/*****************************************************************************/

enum ca_table_flag
{
  CA_TABLE_NO_RELATIVE,
  CA_TABLE_NO_FSYNC
};

struct ca_table_backend
{
  void *
  (*open) (const char *path, int flags, mode_t mode);

  int
  (*stat) (void *handle, struct stat *buf);

  int
  (*utime) (void *handle, const struct timeval tv[2]);

  int
  (*sync) (void *handle);

  void
  (*close) (void *handle);

  int
  (*set_flag) (void *handle, enum ca_table_flag flag);

  int
  (*is_sorted) (void *handle);

  int
  (*insert_row) (void *handle, const struct iovec *value, size_t value_count);

  int
  (*seek) (void *handle, off_t offset, int whence);

  int
  (*seek_to_key) (void *handle, const char *key);

  off_t
  (*offset) (void *handle);

  ssize_t
  (*read_row) (void *handle, struct iovec *value);

  int
  (*delete_row) (void *handle);
};

void
ca_table_register_backend (const char *name,
                           const struct ca_table_backend *backend);

struct ca_table_backend *
ca_table_backend (const char *name);

/*****************************************************************************/

struct ca_table;

struct ca_table *
ca_table_open (const char *backend_name,
               const char *path, int flags, mode_t mode) CA_USE_RESULT;

int
ca_table_stat (struct ca_table *table, struct stat *buf);

int
ca_table_utime (struct ca_table *table, const struct timeval tv[2]);

int
ca_table_sync (struct ca_table *table) CA_USE_RESULT;

void
ca_table_close (struct ca_table *table);

int
ca_table_set_flag (struct ca_table *table, enum ca_table_flag flag);

int
ca_table_is_sorted (struct ca_table *table) CA_USE_RESULT;

int
ca_table_insert_row (struct ca_table *table,
                     const struct iovec *value, size_t value_count) CA_USE_RESULT;

int
ca_table_seek (struct ca_table *table, off_t offset, int whence) CA_USE_RESULT;

/**
 * Returns -1 on failure.  Use ca_last_error() to get an error description.
 * Returns 0 if the key was not found.
 * Returns 1 if the key was found.
 */
int
ca_table_seek_to_key (struct ca_table *table, const char *key) CA_USE_RESULT;

off_t
ca_table_offset (struct ca_table *table) CA_USE_RESULT;

ssize_t
ca_table_read_row (struct ca_table *table, struct iovec *value) CA_USE_RESULT;

int
ca_table_delete_row (struct ca_table *table) CA_USE_RESULT;

/*****************************************************************************/

void *
ca_malloc (size_t size) CA_USE_RESULT;

void *
ca_memdup (const void *data, size_t size) CA_USE_RESULT;

char *
ca_strdup (const char *string) CA_USE_RESULT;

int
ca_array_grow (void **array, size_t *alloc, size_t element_size,
               size_t count) CA_USE_RESULT;

#define CA_ARRAY_GROW(array, alloc) \
  ca_array_grow ((void **) (array), alloc, sizeof(**(array)), 16)

#define CA_ARRAY_GROW_N(array, alloc, n) \
  ca_array_grow ((void **) (array), alloc, sizeof(**(array)), (n))

/*****************************************************************************/

struct ca_fifo;

struct ca_fifo *
ca_fifo_create (size_t size);

void
ca_fifo_free (struct ca_fifo *fifo);

void
ca_fifo_put (struct ca_fifo *fifo, const void *data, size_t size);

void
ca_fifo_get (struct ca_fifo *fifo, void *data, size_t size);

/*****************************************************************************/

struct ca_file_buffer;

struct ca_file_buffer *
ca_file_buffer_alloc (int fd);

void
ca_file_buffer_free (struct ca_file_buffer *buffer);

int
ca_file_buffer_write (struct ca_file_buffer *buffer,
                      const void *buf, size_t count);

int
ca_file_buffer_writev (struct ca_file_buffer *buffer,
                       const struct iovec *iov, int count);

int
ca_file_buffer_flush (struct ca_file_buffer *buffer);

/*****************************************************************************/

struct ca_schema;

struct ca_schema *
ca_schema_load (const char *path) CA_USE_RESULT;

void
ca_schema_close (struct ca_schema *schema);

ssize_t
ca_schema_summary_tables (struct ca_schema *schema,
                          struct ca_table ***tables,
                          uint64_t **offsets);

ssize_t
ca_schema_summary_override_tables (struct ca_schema *schema,
                                   struct ca_table ***tables);

ssize_t
ca_schema_index_tables (struct ca_schema *schema,
                        struct ca_table ***tables);

ssize_t
ca_schema_time_series_tables (struct ca_schema *schema,
                              struct ca_table ***tables);

int
ca_schema_sample (struct ca_schema *schema, const char *key);

int
ca_schema_query (struct ca_schema *schema, const char *query,
                 ssize_t limit);

int
ca_schema_query_correlate (struct ca_schema *schema,
                           const char *query_A,
                           const char *query_B);

/*****************************************************************************/

int
ca_table_write_offset_score (struct ca_table *table, const char *key,
                             const struct ca_offset_score *values,
                             size_t count);

/*****************************************************************************/

void
ca_format_integer (uint8_t **output, uint64_t value);

size_t
ca_offset_score_size (const struct ca_offset_score *values, size_t count);

void
ca_format_offset_score (uint8_t **output,
                        const struct ca_offset_score *values, size_t count);

/*****************************************************************************/

uint64_t
ca_parse_integer (const uint8_t **input);

float
ca_parse_float (const uint8_t **input);

const char *
ca_parse_string (const uint8_t **input);

int
ca_parse_offset_score_array (const uint8_t **input,
                             struct ca_offset_score **sample_values,
                             uint32_t *count) CA_USE_RESULT;

/*****************************************************************************/

int
ca_table_sort (struct ca_table *output, struct ca_table *input) CA_USE_RESULT;

typedef int (*ca_merge_callback) (const struct iovec *value, void *opaque);

int
ca_table_merge (struct ca_table **tables, size_t table_count,
                ca_merge_callback callback, void *opaque) CA_USE_RESULT;

/*****************************************************************************/

uint32_t
ca_crc32c (uint32_t input_crc32, const void *input_buffer, size_t length);

/*****************************************************************************/

void
ca_sort_offset_score_by_offset (struct ca_offset_score *data, size_t count);

void
ca_sort_offset_score_by_score (struct ca_offset_score *data, size_t count);

/*****************************************************************************/

struct ca_hashmap_data
{
  char *key;
  union
    {
      void *pointer;
      int64_t integer;
    } value;
};

struct ca_hashmap
{
  struct ca_hashmap_data *nodes;
  size_t size;
  size_t capacity;
};


struct ca_hashmap *
ca_hashmap_create (size_t capacity);

void
ca_hashmap_free (struct ca_hashmap *map);

int
ca_hashmap_insert (struct ca_hashmap *map, const char *key, void *value);

int64_t
ca_hashmap_insert_int (struct ca_hashmap *map, const char *key, int64_t value);

void *
ca_hashmap_get (const struct ca_hashmap *map, const char *key);

int64_t *
ca_hashmap_get_int (const struct ca_hashmap *map, const char *key);

/*****************************************************************************/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !TABLE_H_ */
