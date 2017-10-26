#include <algorithm>

#include "src/ca-table.h"
#include "src/query.h"
#include "src/schema.h"
#include "src/select.h"
#include "src/util.h"

#include "third_party/evenk/evenk/synch_queue.h"
#include "third_party/evenk/evenk/thread_pool.h"

#if !HAVE_FPUTS_UNLOCKED
# define fputs_unlocked fputs
#endif

template <typename T>
using thread_pool_queue = evenk::synch_queue<T>;

namespace cantera {
namespace table {

std::size_t CountFields(const select_statement& select) {
  std::size_t count = 0;
  for (auto field = select.fields; field; field = field->next)
    count++; 
  return count;
}

void GetFieldValues(std::vector<std::vector<float>> &values, std::size_t field,
		    struct Query* query, Schema* schema,
		    const std::vector<ca_offset_score> &selection) try {
    std::vector<ca_offset_score> field_offsets;

    // TODO: Get rid of this
    static std::mutex mutex;
    {
      std::unique_lock<std::mutex> lock(mutex);
      ProcessQuery(field_offsets, query, schema, false, false);
    }

    std::sort(field_offsets.begin(), field_offsets.end(),
              [](const auto& lhs, const auto& rhs) {
      if (lhs.offset != rhs.offset) return lhs.offset < rhs.offset;
      return lhs.score < rhs.score;
    });

    bool all_zero = true;
    for (const auto& v : field_offsets) {
      if (v.score) {
        all_zero = false;
        break;
      }
    }

    auto vi = field_offsets.begin();

    for (size_t i = 0; i < selection.size(); ++i) {
      const auto offset = selection[i].offset;

      while (vi != field_offsets.end() && vi->offset < offset) ++vi;

      if (vi == field_offsets.end() || vi->offset > offset)
        values[i][field] = std::numeric_limits<float>::quiet_NaN();
      else
        values[i][field] = all_zero ? 1.0f : vi->score;
    }
} catch (std::exception &x) {
  printf("caught an exception: %s\n", x.what());
} catch (...) {
  printf("caught unknown exception\n");
}

void Select(Schema* schema, const select_statement& select) {
  schema->Load();

  const auto& summary_tables = schema->summary_tables;
  KJ_REQUIRE(summary_tables.size() >= 1);

  std::vector<ca_offset_score> selection;
  ProcessQuery(selection, select.query, schema, false, false);

  std::vector<std::vector<float>> values;
  auto n_fields = CountFields(select);
  values.resize(selection.size());
  for (std::size_t i = 0; i < values.size(); ++i)
    values[i].resize(n_fields);

  if (n_fields < 8) {
    std::size_t index = 0;
    for (auto field = select.fields; field; field = field->next, ++index)
      GetFieldValues(values, index, field->query, schema, selection);
  } else {
    evenk::thread_pool<thread_pool_queue> thread_pool(4);

    std::size_t index = 0;
    for (auto field = select.fields; field; field = field->next, ++index) {
     	    thread_pool.submit([&values, index, field, schema, &selection] {
			    GetFieldValues(values, index, field->query, schema, selection);
			    });
    }

    thread_pool.wait();
  }

  for (size_t i = 0; i < selection.size(); ++i) {
    const auto offset = selection[i].offset;

    auto summary_table_idx = summary_tables.size();

    while (--summary_table_idx &&
           std::get<uint64_t>(summary_tables[summary_table_idx]) > offset)
      ;

    summary_tables[summary_table_idx].second->Seek(
        offset - std::get<uint64_t>(summary_tables[summary_table_idx]),
        SEEK_SET);

    string_view key, data;
    KJ_REQUIRE(
        summary_tables[summary_table_idx].second->ReadRow(key, data));

    printf("%.*s", static_cast<int>(key.size()), key.data());

    for (const auto v : values[i]) {
      if (std::isnan(v)) {
        fputs_unlocked(",nan", stdout);
      } else {
        printf(",%.9g", v);
      }
    }

    if (select.with_summaries) {
      printf(",\"");
      for (const auto ch : data) {
        if (ch == '"')
          putchar('"');
        putchar(ch);
      }
      putchar('"');
    }

    putchar_unlocked('\n');
  }
}

}  // namespace table
}  // namespace cantera
