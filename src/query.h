#ifndef CA_STORAGE_CA_TABLE_QUERY_H_
#define CA_STORAGE_CA_TABLE_QUERY_H_ 1

#include <cstdint>
#include <memory>

#include <sys/uio.h>

#include <kj/arena.h>

#include "src/schema.h"

namespace cantera {
namespace table {

struct QueryParseContext {
  void* scanner = nullptr;

  kj::Arena arena;

  std::unique_ptr<Schema> schema;
};

enum StatementType {
  kStatementCorrelate,
  kStatementQuery,
  kStatementParse,
  kStatementSelect,
  kStatementSet
};

enum QueryType {
  kQueryKey,  // Search by document key -- don't use index.
  kQueryLeaf,
  kQueryBinaryOperator,
  kQueryUnaryOperator,
};

enum OperatorType {
  // Binary operators.
  kOperatorOr,
  kOperatorAnd,
  kOperatorSubtract,
  kOperatorEQ,
  kOperatorGT,
  kOperatorGE,
  kOperatorLT,
  kOperatorLE,
  kOperatorInRange,
  kOperatorOrderBy,
  kOperatorRandomSample,

  // Unary operators.
  kOperatorMax,
  kOperatorMin,
  kOperatorNegate,
};

struct Query {
  enum QueryType type;
  const char* legacy_query;
  const char* identifier;

  enum OperatorType operator_type;
  const struct Query* lhs;
  const struct Query* rhs;
  double value;
  double value2;
};

template <typename T>
struct LinkedList {
  T value;
  struct LinkedList* next;
};

struct QueryList {
  struct Query* query;
  QueryList* next;
};

// Defines a keyword and a set of thresholds values for partitioning query
// results into groups.
struct ThresholdClause {
  const char* key;
  LinkedList<double>* values;
};

struct query_statement {
  // Set to non-zero to retrieve document keys instead of JSON summaries.
  int keys_only;

  const struct Query* query;

  struct ThresholdClause* thresholds;
  int64_t limit;
  size_t offset;
};

struct query_correlate_statement {
  const struct Query* query_A;
  const struct Query* query_B;
};

enum RuntimeParameter { CA_PARAM_OUTPUT_FORMAT, CA_PARAM_TIME_FORMAT };

enum RuntimeParameterValue {
  /* OUTPUT FORMAT */
  CA_PARAM_VALUE_CSV,
  CA_PARAM_VALUE_JSON
};

struct parse_statement {
  const struct Query* query;
};

struct select_statement {
  QueryList* fields;
  const struct Query* query;
  int with_summaries;
};

struct set_statement {
  RuntimeParameter parameter;
  union {
    RuntimeParameterValue enum_value;
    const char* string_value;
  } v;
};

struct Statement {
  enum StatementType type;

  union {
    struct query_correlate_statement query_correlate;
    struct query_statement query;
    struct parse_statement parse;
    struct select_statement select;
    struct set_statement set;
  } u;

  Statement* next;
};

/*****************************************************************************/

extern char CA_time_format[64];
extern enum RuntimeParameterValue CA_output_format;

/*****************************************************************************/

void CA_parse_script(QueryParseContext* context, FILE* input);

void CA_process_statement(QueryParseContext* context, struct Statement* stmt);

/*****************************************************************************/

void CA_output_char(int ch);

void CA_output_string(const char* string);

void CA_output_json_string(const char* string, size_t length);

void CA_output_float4(float number);

void CA_output_float8(double number);

void CA_output_uint64(uint64_t number);

void CA_output_time_float4(struct iovec* iov);

}  // namespace table
}  // namespace cantera

#endif  // !CA_STORAGE_CA_TABLE_QUERY_H_
