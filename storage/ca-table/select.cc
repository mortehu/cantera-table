#include "base/string.h"
#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/select.h"

namespace ca_table {

void Select(struct ca_schema* schema, const struct select_statement& select) {
  const auto index_tables = ca_schema_index_tables(schema);

  uint64_t* summary_table_offsets;
  const auto summary_tables =
      ca_schema_summary_tables(schema, &summary_table_offsets);
  KJ_REQUIRE(summary_tables.size() >= 1);

  std::vector<ca_offset_score> selection;
  Query(selection, select.query, index_tables, false, false);

  std::vector<std::vector<float>> values;
  values.resize(selection.size());

  for (auto field = select.fields; field; field = field->next) {
    std::vector<ca_offset_score> field_offsets;
    bool sorted = true;

    LookupKey(index_tables, field->value,
              [&field_offsets, &sorted](auto offsets) {
      if (field_offsets.empty()) {
        field_offsets.swap(offsets);
      } else {
        field_offsets.insert(field_offsets.end(), offsets.begin(),
                             offsets.end());
        sorted = false;
      }
    });

    if (!sorted) {
      std::sort(field_offsets.begin(), field_offsets.end(),
                [](const auto& lhs, const auto& rhs) {
        if (lhs.offset != rhs.offset) return lhs.offset < rhs.offset;
        return lhs.score < rhs.score;
      });
    }

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
           summary_table_offsets[summary_table_idx] > offset)
      ;

    summary_tables[summary_table_idx]->Seek(
        offset - summary_table_offsets[summary_table_idx], SEEK_SET);

    struct iovec key_iov, data_iov;
    KJ_REQUIRE(1 ==
               summary_tables[summary_table_idx]->ReadRow(&key_iov, &data_iov));

    printf("%.*s", static_cast<int>(key_iov.iov_len),
           reinterpret_cast<const char*>(key_iov.iov_base));

    for (const auto v : values[i]) {
      const auto str = ev::FloatToString(v);
      printf(",%.*s", static_cast<int>(str.size()), str.data());
    }

    putchar('\n');
  }
}

}  // namesapce ca_table
