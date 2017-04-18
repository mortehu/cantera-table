/*
    Low-level data formatter for Cantera Table
    Copyright (C) 2013    Morten Hustveit
    Copyright (C) 2013    eVenture Capital Partners II

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <cassert>
#include <cmath>
#include <cstring>

#include <kj/debug.h>

#include "src/ca-table.h"

#define MAX_HEADER_SIZE 64

namespace cantera {
namespace table {

void ca_table_write_offset_score(TableBuilder* table,
                                 const string_view& key,
                                 const struct ca_offset_score* values,
                                 size_t count) {
  auto buffer_alloc = ca_offset_score_size(values, count);
  std::vector<uint8_t> buffer(buffer_alloc);

  auto size =
      ca_format_offset_score(buffer.data(), buffer_alloc, values, count);

  KJ_ASSERT(size <= buffer_alloc, size, buffer_alloc);
  buffer.resize(size);

  string_view buffer_view{reinterpret_cast<const char*>(buffer.data()), buffer.size()};

  table->InsertRow(key, buffer_view);

#ifdef HARDEN
  std::vector<ca_offset_score> tmp;

  ca_offset_score_parse(buffer_view, &tmp);

  assert(tmp.size() == count);

  for (size_t i = 0; i < count; ++i) {
    KJ_REQUIRE(tmp[i].offset == values[i].offset, i, count, values[i].offset,
               tmp[i].offset);

    KJ_REQUIRE(tmp[i].score == values[i].score ||
                   (isnan(tmp[i].score) && isnan(values[i].score)),
               i, count, values[i].score, tmp[i].score);
  }
#endif
}

}  // namespace table
}  // namespace cantera
