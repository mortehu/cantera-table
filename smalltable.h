#ifndef TABLE_H_
#define TABLE_H_ 1

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CA_NAMEDATALEN 64

struct table;

/*****************************************************************************/

enum ca_value_type
{
  CA_TIME_SERIES = 1,
  CA_RELATIVE_TIME_SERIES = 2,
  CA_TABLE_DECLARATION = 3
};

enum ca_field_flag
{
  CA_FIELD_NOT_NULL = 0x00000001
};

struct ca_field
{
  char name[CA_NAMEDATALEN];
  uint32_t flags;
  uint16_t pad0;
  uint8_t pad1;
  uint8_t type; /* enum ca_value_type */
};

struct ca_table_declaration
{
  const char *path;
  uint32_t field_count;
  struct ca_field *fields;
};

struct ca_data
{
  enum ca_value_type type;

  union
    {
      struct
        {
          uint64_t start_time;
          uint32_t interval;
          size_t count;
          const float *values;
        } time_series;

      struct ca_table_declaration table_declaration;
    } v;
};

/*****************************************************************************/

enum table_order
{
  TABLE_ORDER_PHYSICAL,
  TABLE_ORDER_KEY
};

typedef void (*table_iterate_callback) (const char *key,
                                        const void *value, size_t value_size,
                                        void *opaque);

/* Opens a new table for writing.  Once written, a table is read-only */
struct table *
table_create (const char *path);

/* Opens an existing table for reading */
struct table *
table_open (const char *path);

/* Closes a previously opened table */
void
table_close (struct table *t);

int
table_is_sorted (const struct table *t);

/*****************************************************************************/

/* Writes a key/timestamp/float combination to a table */
void
table_write_sample (struct table *t, const char *key, uint64_t sample_time,
                    float sample_value);

/* Writes multiple key/timestamp/float combinations to a table */
void
table_write_samples (struct table *t, const char *key,
                     uint64_t start_time, uint32_t interval,
                     const float *sample_values, size_t count);

void
table_write_table_declaration (struct table *t, const char *key,
                               const struct ca_table_declaration *declaration);

/*****************************************************************************/

void
table_sort (struct table *output, struct table *input);

void
table_iterate (struct table *t, table_iterate_callback callback,
               enum table_order order, void *opaque);

/* Iterate multiple tables at once, in key order.  Useful for merging */
void
table_iterate_multiple (struct table **tables, size_t table_count,
                        table_iterate_callback callback, void *opaque);

const void *
table_lookup (struct table *t, const char *key,
              size_t *size);

void
table_parse_time_series (const uint8_t **input,
                         uint64_t *start_time, uint32_t *interval,
                         const float **sample_values, size_t *count);

/*****************************************************************************/

typedef void (*ca_data_iterate_callback) (struct ca_data *data, void *opaque);

void
ca_data_iterate (const void *data, size_t size,
                 ca_data_iterate_callback callback, void *opaque);

/*****************************************************************************/

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !TABLE_H_ */
