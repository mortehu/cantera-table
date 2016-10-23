#ifndef STORAGE_CA_TABLE_CA_TABLE_H_
#define STORAGE_CA_TABLE_CA_TABLE_H_ 1

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <experimental/string_view>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

namespace cantera {

using string_view = std::experimental::string_view;

namespace table {

struct ca_offset_score;

class Query;
class Schema;
class Table;

// Escape string for use in tab delimited format.
std::string Escape(const string_view& str);

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

enum TableCompression : uint8_t {
  // No compression.
  kTableCompressionNone = 0,

  // Backend-specific default compression.
  kTableCompressionDefault = uint8_t(-1),

  // Zstandard compression. https://github.com/facebook/zstd
  kTableCompressionZSTD = 1,

  // Keep this equal to the numerically last compression method.
  kTableCompressionLast = kTableCompressionZSTD
};

class TableOptions {
 public:
  static TableOptions Create() { return TableOptions(); }

  TableOptions& SetFileFlags(int file_flags) {
    file_flags_ = file_flags;
    return *this;
  }

  TableOptions& SetFileMode(mode_t file_mode) {
    file_mode_ = file_mode;
    return *this;
  }

  TableOptions& SetCompression(TableCompression compression) {
    compression_ = compression;
    return *this;
  }

  TableOptions& SetCompressionLevel(uint8_t compression_level) {
    compression_level_ = compression_level;
    return *this;
  }

  TableOptions& SetNoFSync(bool value = true) {
    no_fsync_ = value;
    return *this;
  }

  TableOptions& SetInputUnsorted(bool value = true) {
    input_unsorted_ = value;
    return *this;
  }

  TableOptions& SetOutputSeekable(bool value = true) {
    output_seekable_ = value;
    return *this;
  }

  int GetFileFlags() const { return file_flags_; }
  mode_t GetFileMode() const { return file_mode_; }

  TableCompression GetCompression() const { return compression_; }
  uint8_t GetCompressionLevel() const { return compression_level_; }

  bool GetNoFSync() const { return no_fsync_; }
  bool GetInputUnsorted() const { return input_unsorted_; }
  bool GetOutputSeekable() const { return output_seekable_; }

 private:
  // File creation options.
  int file_flags_ = 0;
  mode_t file_mode_ = 0666;

  // Data compression options.
  TableCompression compression_ = kTableCompressionDefault;
  uint8_t compression_level_ = 0;

  // Miscellaneous flags.
  bool no_fsync_ = false;
  bool input_unsorted_ = false;
  bool output_seekable_ = false;
};

/*****************************************************************************/

class TableBuilder {
 public:
  virtual ~TableBuilder();

  virtual void InsertRow(const string_view& key, const string_view& value) = 0;

  virtual void Sync() = 0;
};

class Table {
 public:
  Table(const struct stat& st);

  virtual ~Table();

  virtual int IsSorted() = 0;

  // Seeks to the first table row.
  virtual void SeekToFirst() = 0;

  // Seeks to the given key.  Returns true if the key was found, false
  // otherwise.
  //
  // If the key was not found, the cursor MAY have moved, but not beyond
  // any alphanumerically larger keys.  This allows this function to be
  // used for speeding up prefix key searches.
  virtual bool SeekToKey(const string_view& key) = 0;

  // Reads one row.  Returns true if a value was read successfully, or
  // false if end of file was reached instead.
  virtual bool ReadRow(string_view& key, string_view& value) = 0;

  // Skips the given number of rows.
  virtual bool Skip(size_t count) = 0;

  const struct stat st;
};

class SeekableTable : public Table {
 public:
  SeekableTable(const struct stat& st);

  virtual off_t Offset() = 0;

  virtual void Seek(off_t offset, int whence) = 0;
};

/*****************************************************************************/

class TableFactory {
 public:
  static std::unique_ptr<TableBuilder> Create(const char* backend_name,
                                              const char* path,
                                              const TableOptions& options);

  static std::unique_ptr<Table> Open(const char* backend_name,
                                     const char* path);

  static std::unique_ptr<SeekableTable> OpenSeekable(const char* backend_name,
                                                     const char* path);
};

/*****************************************************************************/

void ca_schema_query(Schema* schema,
                     const struct query_statement& stmt);

void ca_schema_query_correlate(Schema* schema, const Query* query_A,
                               const Query* query_B);

/*****************************************************************************/

void ca_table_write_offset_score(TableBuilder* table,
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
