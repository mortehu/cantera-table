#ifndef STORAGE_CA_TABLE_CA_TABLE_H_
#define STORAGE_CA_TABLE_CA_TABLE_H_ 1

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cmath>
#include <experimental/string_view>
#include <functional>
#include <string>
#include <memory>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <kj/debug.h>

namespace cantera {

using string_view = std::experimental::string_view;

namespace table {

struct ca_offset_score;

class Query;
class Schema;
class Table;

// Escape string for use in tab delimited format.
std::string Escape(const string_view& str);

class Backend {
 public:
  virtual ~Backend();

  virtual std::unique_ptr<Table> Open(const char* path, int flags,
                                                mode_t mode) = 0;
};

void LookupKey(const std::vector<Table*>& index_tables,
               const char* key,
               std::function<void(std::vector<ca_offset_score>)>&& callback);

void ProcessQuery(std::vector<ca_offset_score>& offsets, const Query* query,
                  Schema* schema, bool make_headers = false,
                  bool use_max = true);

void PrintQuery(const Query* query);

// Removes from `lhs' every offset contained `rhs', including duplicates.
// Returns the number of elements left in `rhs'.
size_t SubtractOffsets(struct ca_offset_score* lhs, size_t lhs_count,
                       const struct ca_offset_score* rhs, size_t rhs_count);

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

  // Offset compressed with delta encoding followed by Oroch encoding.
  // Score is stored as is.
  CA_OFFSET_SCORE_DELTA_OROCH_FLOAT = 7,

  // Offset compressed with delta encoding followed by Oroch encoding.
  // Score is converted to int64 and compressed with Oroch encoding.
  CA_OFFSET_SCORE_DELTA_OROCH_OROCH = 8,

  // Single offset/score pair. The offset is encoded as varint, the score
  // either as the original float or as a small absolute integer value.
  CA_OFFSET_SCORE_SINGLE_FLOAT = 9,
  CA_OFFSET_SCORE_SINGLE_POSITIVE_1 = 10,
  CA_OFFSET_SCORE_SINGLE_NEGATIVE_1 = 11,
  CA_OFFSET_SCORE_SINGLE_POSITIVE_2 = 12,
  CA_OFFSET_SCORE_SINGLE_NEGATIVE_2 = 13,
  CA_OFFSET_SCORE_SINGLE_POSITIVE_3 = 14,
  CA_OFFSET_SCORE_SINGLE_NEGATIVE_3 = 15,

  // Nothing at all.
  CA_OFFSET_SCORE_EMPTY = 16,
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

Backend* ca_table_backend(const char* name);

/*****************************************************************************/

class Table {
 public:
  static std::unique_ptr<Table> Open(const char* backend_name, const char* path, int flags, mode_t mode = 0666);

  Table();

  virtual ~Table();

  virtual void Sync() = 0;

  virtual void SetFlag(enum ca_table_flag flag) = 0;

  virtual int IsSorted() = 0;

  virtual void InsertRow(const struct iovec* value, size_t value_count) = 0;

  void InsertRow(const string_view& key, const string_view& value) {
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

  virtual bool SeekToKey(const string_view& key) = 0;

  virtual off_t Offset() = 0;

  virtual bool ReadRow(struct iovec* key, struct iovec* value) = 0;

  inline bool ReadRow(string_view& key, string_view& value) {
    struct iovec k, v;
    if (!ReadRow(&k, &v)) return false;
    key = string_view{reinterpret_cast<const char*>(k.iov_base), k.iov_len};
    value = string_view{reinterpret_cast<const char*>(v.iov_base), v.iov_len};
    return true;
  }

  string_view ReadValue() {
    struct iovec key, value;
    KJ_REQUIRE(ReadRow(&key, &value));
    return {reinterpret_cast<const char*>(value.iov_base), value.iov_len};
  }

  struct stat st;
};

int ca_table_stat(Table* table, struct stat* buf);

int ca_table_utime(Table* table, const struct timeval tv[2]);

/*****************************************************************************/

void ca_schema_query(Schema* schema,
                     const struct query_statement& stmt);

void ca_schema_query_correlate(Schema* schema, const Query* query_A,
                               const Query* query_B);

/*****************************************************************************/

void ca_table_write_offset_score(Table* table,
                                 const string_view& key,
                                 const struct ca_offset_score* values,
                                 size_t count);

/*****************************************************************************/

void ca_format_integer(uint8_t** output, uint64_t value);

size_t ca_offset_score_size(const struct ca_offset_score* values, size_t count);

size_t ca_format_offset_score(uint8_t* output, size_t output_size,
                              const struct ca_offset_score* values,
                              size_t count);

void ca_format_enable_trace(bool enable);

/*****************************************************************************/

uint64_t ca_parse_integer(const uint8_t** input);

float ca_parse_float(const uint8_t** input);

const char* ca_parse_string(const uint8_t** input);

uint64_t ca_offset_score_max_offset(const uint8_t* begin, const uint8_t* end);

void ca_offset_score_parse(string_view input,
                           std::vector<ca_offset_score>* output);

size_t ca_offset_score_count(const uint8_t* begin, const uint8_t* end);

/*****************************************************************************/

int ca_table_merge(std::vector<std::unique_ptr<Table>>& tables,
                   std::function<int(const struct iovec* key,
                                     const struct iovec* value)> callback);

void ca_table_merge(
    std::vector<std::unique_ptr<Table>>& tables,
    std::function<void(const std::string& key,
                       std::vector<std::vector<char>>& data)> callback);

}  // namespace table
}  // namespace cantera

#endif  // !STORAGE_CA_TABLE_CA_TABLE_H_
