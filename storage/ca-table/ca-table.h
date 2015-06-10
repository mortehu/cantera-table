#ifndef STORAGE_CA_TABLE_CA_TABLE_H_
#define STORAGE_CA_TABLE_CA_TABLE_H_ 1

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cmath>
#include <functional>
#include <string>
#include <memory>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "base/macros.h"
#include "base/stringref.h"
#include "storage/ca-table/query.h"

struct ca_offset_score;

namespace ca_table {

class Table;

// Escape string for use in tab delimited format.
std::string Escape(const ev::StringRef& str);

class Backend {
 public:
  virtual ~Backend();

  virtual std::unique_ptr<ca_table::Table> Open(const char* path, int flags,
                                                mode_t mode) = 0;
};

void LookupKey(const std::vector<ca_table::Table*>& index_tables,
               const char* key,
               std::function<void(std::vector<ca_offset_score>)>&& callback);

void Query(std::vector<ca_offset_score>& ret_offsets, const char* query,
           const std::vector<ca_table::Table*>& index_tables,
           bool make_headers = false, bool use_max = true);

// Removes from `lhs' every offset contained `rhs', including duplicates.
// Returns the number of elements left in `rhs'.
size_t SubtractOffsets(struct ca_offset_score* lhs, size_t lhs_count,
                       const struct ca_offset_score* rhs, size_t rhs_count);

}  // namespace ca_table

/*****************************************************************************/

/* Compression schemes for sorted offset/score pairs */
enum ca_offset_score_type {
  // Offset, followed by median, followed by other probability bands.
  CA_OFFSET_SCORE_WITH_PREDICTION = 0,

  // Offset followed by score.
  CA_OFFSET_SCORE = 1,

  // Offset interval compressed, quantized using GCD.  Integer scores stored in
  // fixed minimum amount of bytes.  RLE compressed.
  CA_OFFSET_SCORE_FLEXI = 6,
};

/*****************************************************************************/

struct ca_score;

struct ca_offset_score {
  ca_offset_score() {}

  ca_offset_score(uint64_t offset, float score)
      : offset(offset), score(score) {}

  ca_offset_score(uint64_t offset, const ca_score& score);

  bool HasPercentiles() const { return std::isfinite(score_pct5); }

  uint64_t offset = 0;
  float score = 0.0f;

  float score_pct5 = std::numeric_limits<float>::quiet_NaN();
  float score_pct25 = std::numeric_limits<float>::quiet_NaN();
  float score_pct75 = std::numeric_limits<float>::quiet_NaN();
  float score_pct95 = std::numeric_limits<float>::quiet_NaN();
};

struct ca_score {
  ca_score() {}

  ca_score(float score) : score(score) {}

  ca_score(const ca_offset_score& rhs)
      : score(rhs.score),
        score_pct5(rhs.score_pct5),
        score_pct25(rhs.score_pct25),
        score_pct75(rhs.score_pct75),
        score_pct95(rhs.score_pct95) {}

  ca_score& operator=(const ca_offset_score& rhs) {
    score = rhs.score;
    score_pct5 = rhs.score_pct5;
    score_pct25 = rhs.score_pct25;
    score_pct75 = rhs.score_pct75;
    score_pct95 = rhs.score_pct95;
    return *this;
  }

  bool HasPercentiles() const { return std::isfinite(score_pct5); }

  float score = 0.0f;

  float score_pct5 = std::numeric_limits<float>::quiet_NaN();
  float score_pct25 = std::numeric_limits<float>::quiet_NaN();
  float score_pct75 = std::numeric_limits<float>::quiet_NaN();
  float score_pct95 = std::numeric_limits<float>::quiet_NaN();
};

/*****************************************************************************/

enum ca_table_flag { CA_TABLE_NO_RELATIVE, CA_TABLE_NO_FSYNC };

ca_table::Backend* ca_table_backend(const char* name);

/*****************************************************************************/

namespace ca_table {

class Table {
 public:
  Table();

  virtual ~Table();

  virtual void Sync() = 0;

  virtual void SetFlag(enum ca_table_flag flag) = 0;

  virtual int IsSorted() = 0;

  virtual void InsertRow(const struct iovec* value, size_t value_count) = 0;

  void InsertRow(const ev::StringRef& key, const ev::StringRef& value) {
    iovec iv[2];
    iv[0].iov_base = const_cast<char*>(key.data());
    iv[0].iov_len = key.size();
    iv[1].iov_base = const_cast<char*>(value.data());
    iv[1].iov_len = value.size();
    InsertRow(iv, 2);
  }

  // Seeks to the given key.  Returns true if the key was found, false
  // otherwise.
  //
  // If the key was not found, the cursor MAY have moved, but not beyond any
  // alphanumerically larger keys.  This allows using this function to be used
  // for speeding up prefix key searches.
  virtual void Seek(off_t offset, int whence) = 0;

  virtual bool SeekToKey(const ev::StringRef& key) = 0;

  virtual off_t Offset() = 0;

  virtual ssize_t ReadRow(struct iovec* key, struct iovec* value) = 0;

  struct stat st;
};

}  // namespace ca_table

int ca_table_stat(ca_table::Table* table, struct stat* buf);

int ca_table_utime(ca_table::Table* table, const struct timeval tv[2]);

/*****************************************************************************/

struct ca_schema;

struct ca_schema* ca_schema_load(const char* path) EV_USE_RESULT;

void ca_schema_close(struct ca_schema* schema);

int ca_schema_query(struct ca_schema* schema,
                    const struct query_statement& stmt);

void ca_schema_query_correlate(struct ca_schema* schema, const char* query_A,
                               const char* query_B);

/*****************************************************************************/

void ca_table_write_offset_score(ca_table::Table* table,
                                 const ev::StringRef& key,
                                 const struct ca_offset_score* values,
                                 size_t count);

/*****************************************************************************/

void ca_format_integer(uint8_t** output, uint64_t value);

size_t ca_offset_score_size(const struct ca_offset_score* values, size_t count);

size_t ca_format_offset_score(uint8_t* output,
                              const struct ca_offset_score* values,
                              size_t count);

/*****************************************************************************/

uint64_t ca_parse_integer(const uint8_t** input);

float ca_parse_float(const uint8_t** input);

const char* ca_parse_string(const uint8_t** input);

uint64_t ca_offset_score_max_offset(const uint8_t* begin, const uint8_t* end);

void ca_offset_score_parse(ev::StringRef input,
                           std::vector<ca_offset_score>* output);

size_t ca_offset_score_count(const uint8_t* begin, const uint8_t* end);

/*****************************************************************************/

std::unique_ptr<ca_table::Table> ca_table_open(const char* backend_name,
                                               const char* path, int flags,
                                               mode_t mode = 0666);

std::vector<ca_table::Table*> ca_schema_summary_tables(struct ca_schema* schema,
                                                       uint64_t** offsets);

std::vector<ca_table::Table*> ca_schema_summary_override_tables(
    struct ca_schema* schema);

std::vector<ca_table::Table*> ca_schema_index_tables(struct ca_schema* schema);

std::vector<std::pair<ca_table::Table*, std::string>>
ca_schema_time_series_tables(struct ca_schema* schema);

int ca_table_merge(std::vector<std::unique_ptr<ca_table::Table>>& tables,
                   std::function<int(const struct iovec* key,
                                     const struct iovec* value)> callback);

void ca_table_merge(
    std::vector<std::unique_ptr<ca_table::Table>>& tables,
    std::function<void(const std::string& key,
                       std::vector<std::vector<char>>& data)> callback);

#endif  // !STORAGE_CA_TABLE_CA_TABLE_H_
