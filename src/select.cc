#include <algorithm>

#include "src/ca-table.h"
#include "src/query.h"
#include "src/schema.h"
#include "src/select.h"
#include "src/util.h"

#if !HAVE_FPUTS_UNLOCKED
# define fputs_unlocked fputs
#endif

namespace cantera {
namespace table {

void Select(Schema* schema, const struct select_statement& select) {
  schema->Load();

  const auto& summary_tables = schema->summary_tables;
  KJ_REQUIRE(summary_tables.size() >= 1);

  std::vector<ca_offset_score> selection;
  ProcessQuery(selection, select.query, schema, false, false);

  std::vector<std::vector<float>> values;
  values.resize(selection.size());

  for (auto field = select.fields; field; field = field->next) {
    std::vector<ca_offset_score> field_offsets;

    ProcessQuery(field_offsets, field->query, schema, false, false);

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

      if (vi == field_offsets.end() || vi->offset > offset) {
        values[i].push_back(std::numeric_limits<float>::quiet_NaN());
        continue;
      }

      values[i].push_back(all_zero ? 1.0f : vi->score);
    }
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
