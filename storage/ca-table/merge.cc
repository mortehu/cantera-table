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

#include "storage/ca-table/ca-table.h"

struct CA_merge_heap {
  struct iovec key;
  struct iovec value;
  uint32_t table;
};

#define CA_HEAP_KEY_BEGIN(h) reinterpret_cast<const uint8_t*>((h)->key.iov_base)
#define CA_HEAP_KEY_END(h) \
  (reinterpret_cast<const uint8_t*>((h)->key.iov_base) + (h)->key.iov_len)

static void CA_merge_heap_push(struct CA_merge_heap* heap, size_t heap_size,
                               const struct CA_merge_heap* entry) {
  size_t parent, i;

  i = heap_size;

  while (i) {
    parent = (i - 1) / 2;

    if (!std::lexicographical_compare(
            CA_HEAP_KEY_BEGIN(entry), CA_HEAP_KEY_END(entry),
            CA_HEAP_KEY_BEGIN(&heap[parent]), CA_HEAP_KEY_END(&heap[parent])))
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = *entry;
}

static void CA_merge_heap_replace_top(struct CA_merge_heap* heap,
                                      size_t heap_size,
                                      const struct CA_merge_heap* entry) {
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for (i = 1, child = 2; child <= heap_size; i = child, child = i << 1) {
    /* Move the smaller child up the tree */

    if (child + 1 <= heap_size &&
        std::lexicographical_compare(CA_HEAP_KEY_BEGIN(&heap[child + 1]),
                                     CA_HEAP_KEY_END(&heap[child + 1]),
                                     CA_HEAP_KEY_BEGIN(&heap[child]),
                                     CA_HEAP_KEY_END(&heap[child])))
      ++child;

    heap[i] = heap[child];
  }

  /* Move ancestor chain down into hole, until we can insert the new entry */

  while (i > 1) {
    parent = i >> 1;

    if (!std::lexicographical_compare(
            CA_HEAP_KEY_BEGIN(entry), CA_HEAP_KEY_END(entry),
            CA_HEAP_KEY_BEGIN(&heap[parent]), CA_HEAP_KEY_END(&heap[parent])))
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = *entry;
}

static void CA_merge_heap_pop(struct CA_merge_heap* heap, size_t heap_size) {
  size_t i, child, parent;

  /* This helps avoid some '+1's and '-1's later.  Technically illegal */
  --heap;

  /* Move hole down the tree */

  for (i = 1, child = 2; child <= heap_size; i = child, child = i << 1) {
    /* Move the smaller child up the tree */

    if (child + 1 <= heap_size &&
        std::lexicographical_compare(CA_HEAP_KEY_BEGIN(&heap[child + 1]),
                                     CA_HEAP_KEY_END(&heap[child + 1]),
                                     CA_HEAP_KEY_BEGIN(&heap[child]),
                                     CA_HEAP_KEY_END(&heap[child])))
      ++child;

    heap[i] = heap[child];
  }

  if (i == heap_size) return;

  /* Hole did not end up at tail: Move ancestor chain down into hole, until we
   * can insert the tail element as an ancestor */

  while (i > 0) {
    parent = i >> 1;

    if (!std::lexicographical_compare(CA_HEAP_KEY_BEGIN(&heap[heap_size]),
                                      CA_HEAP_KEY_END(&heap[heap_size]),
                                      CA_HEAP_KEY_BEGIN(&heap[parent]),
                                      CA_HEAP_KEY_END(&heap[parent])))
      break;

    heap[i] = heap[parent];
    i = parent;
  }

  heap[i] = heap[heap_size];
}

int ca_table_merge(std::vector<std::unique_ptr<ca_table::Table>>& tables,
                   std::function<int(const struct iovec* key,
                                     const struct iovec* value)> callback) {
  ssize_t ret;

  std::vector<CA_merge_heap> heap(tables.size());
  size_t heap_size = 0;

  for (size_t i = 0; i < tables.size(); ++i) {
    struct CA_merge_heap e;

    e.table = i;

    KJ_REQUIRE(tables[i]->IsSorted());

    if (0 >= (ret = tables[i]->ReadRow(&e.key, &e.value))) {
      if (ret == 0) continue;

      return -1;
    }

    CA_merge_heap_push(&heap[0], heap_size++, &e);
  }

  while (heap_size) {
    struct CA_merge_heap e;

    e = heap[0];

    if (-1 == callback(&e.key, &e.value)) return -1;

    if (0 >= (ret = tables[e.table]->ReadRow(&e.key, &e.value))) {
      if (ret == 0) {
        CA_merge_heap_pop(&heap[0], heap_size);
        --heap_size;

        continue;
      }

      return -1;
    }

    CA_merge_heap_replace_top(&heap[0], heap_size, &e);
  }

  return 0;
}

void ca_table_merge(
    std::vector<std::unique_ptr<ca_table::Table>>& tables,
    std::function<void(const std::string& key,
                       std::vector<std::vector<char>>& data)> callback) {
  std::vector<std::vector<char>> data;
  std::string current_key;

  auto callback_wrapper =
      [&current_key, &data, callback](const struct iovec* key,
                                      const struct iovec* value) -> int {
    std::string new_key(reinterpret_cast<const char*>(key->iov_base),
                        key->iov_len);
    if (new_key != current_key) {
      if (!data.empty()) {
        callback(current_key, data);
      }
      data.clear();
      current_key = new_key;
    }
    data.emplace_back();

    auto value_begin = reinterpret_cast<const char*>(value->iov_base);
    auto value_end = value_begin + value->iov_len;

    data.back().assign(value_begin, value_end);

    return 0;
  };

  KJ_REQUIRE(0 == ca_table_merge(tables, callback_wrapper));

  if (!data.empty()) {
    callback(current_key, data);
  }
}
