/*
    Heap based merge of sorted tables
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

#include <algorithm>

#include <kj/debug.h>

#include "src/ca-table.h"

namespace cantera {
namespace table {

struct CA_merge_heap {
  string_view key;
  string_view value;
  uint32_t table;
};

static void CA_merge_heap_push(struct CA_merge_heap* heap, size_t heap_size,
                               const struct CA_merge_heap* entry) {
  size_t i = heap_size;

  while (i) {
    size_t parent = (i - 1) / 2;

    if (entry->key >= heap[parent].key)
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = *entry;
}

static void CA_merge_heap_replace_top(struct CA_merge_heap* heap,
                                      size_t heap_size,
                                      const struct CA_merge_heap* entry) {
  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  size_t i, child;
  for (i = 1, child = 2; child <= heap_size; i = child, child = i << 1) {
    /* Move the smaller child up the tree */

    if (child + 1 <= heap_size && heap[child + 1].key < heap[child].key)
      ++child;

    heap[i] = heap[child];
  }

  /* Move ancestor chain down into hole, until we can insert the new entry */

  while (i > 1) {
    size_t parent = i >> 1;

    if (entry->key >= heap[parent].key)
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = *entry;
}

static void CA_merge_heap_pop(struct CA_merge_heap* heap, size_t heap_size) {
  size_t i, child;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for (i = 1, child = 2; child <= heap_size; i = child, child = i << 1) {
    /* Move the smaller child up the tree */

    if (child + 1 <= heap_size && heap[child + 1].key < heap[child].key)
      ++child;

    heap[i] = heap[child];
  }

  if (i == heap_size) return;

  /* Hole did not end up at tail: Move ancestor chain down into hole, until we
   * can insert the tail element as an ancestor */

  while (i > 0) {
    size_t parent = i >> 1;

    if (heap[heap_size].key >= heap[parent].key)
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = heap[heap_size];
}

int ca_table_merge(
    std::vector<std::unique_ptr<Table>>& tables,
    std::function<int(const string_view& key, const string_view& value)>
        callback) {
  std::vector<CA_merge_heap> heap(tables.size());
  size_t heap_size = 0;

  for (size_t i = 0; i < tables.size(); ++i) {
    struct CA_merge_heap e;

    e.table = i;

    KJ_REQUIRE(tables[i]->IsSorted());

    if (!tables[i]->ReadRow(e.key, e.value)) continue;

    CA_merge_heap_push(&heap[0], heap_size++, &e);
  }

  while (heap_size) {
    struct CA_merge_heap e = heap[0];
    if (-1 == callback(e.key, e.value)) return -1;

    if (!tables[e.table]->ReadRow(e.key, e.value)) {
      CA_merge_heap_pop(&heap[0], heap_size);
      --heap_size;
      continue;
    }

    CA_merge_heap_replace_top(&heap[0], heap_size, &e);
  }

  return 0;
}

void ca_table_merge(
    std::vector<std::unique_ptr<Table>>& tables,
    std::function<void(const std::string& key,
                       std::vector<std::vector<char>>& data)> callback) {
  std::vector<std::vector<char>> data;
  std::string current_key;

  auto callback_wrapper =
      [&current_key, &data, callback](const string_view& key,
                                      const string_view& value) -> int {
    if (key != current_key) {
      if (!data.empty()) {
        callback(current_key, data);
        data.clear();
      }
      current_key.assign(key.data(), key.size());
    }

    data.emplace_back();
    data.back().assign(value.begin(), value.end());

    return 0;
  };

  KJ_REQUIRE(0 == ca_table_merge(tables, callback_wrapper));

  if (!data.empty()) {
    callback(current_key, data);
  }
}

}  // namespace table
}  // namespace cantera
