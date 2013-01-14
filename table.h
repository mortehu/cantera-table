#ifndef TABLE_H_
#define TABLE_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

struct table;

enum table_value_type
{
  TABLE_TIME_SERIES = 1,
  TABLE_RELATIVE_TIME_SERIES = 2
};

enum table_order
{
  TABLE_ORDER_PHYSICAL,
  TABLE_ORDER_KEY
};

typedef void (*table_iterate_callback)(const void *value, size_t value_size);

/* Opens a new table for writing.  Once written, a table is read-only */
struct table *
table_create (const char *path);

/* Opens an existing table for reading */
struct table *
table_open (const char *path);

/* Closes a previously opened table */
void
table_close (struct table *t);

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
table_iterate (struct table *t, table_iterate_callback callback,
               enum table_order order);

/* Iterate multiple tables at once, in key order.  Useful for merging */
void
table_iterate_multiple (struct table *tables, size_t table_count,
                        table_iterate_callback callback);

void
table_parse_time_series (const uint8_t **input,
                         uint64_t *start_time, uint32_t *interval,
                         const float **sample_values, size_t *count);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* !TABLE_H_ */
