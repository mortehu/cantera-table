/*
    Database structure management routines
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

#include "src/schema.h"

#include <cassert>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "src/query.h"

namespace cantera {
namespace table {

namespace {

auto OpenFileStream(const char* path, const char* mode) {
  std::unique_ptr<FILE, decltype(&fclose)> result{fopen(path, mode), fclose};
  if (!result) {
    KJ_FAIL_SYSCALL("fopen", errno, path, mode);
  }
  return result;
}

}  // namespace

Schema::~Schema() {}

Schema::Schema(std::string path) : path_(std::move(path)) {}

void Schema::Load() {
  if (loaded_) return;

  KJ_CONTEXT(path_);

  auto f = OpenFileStream(path_.c_str(), "r");

  char line[4096];
  line[sizeof(line) - 1] = 0;

  int lineno = 0;

  while (NULL != fgets(line, sizeof(line), f.get())) {
    size_t line_length;

    ++lineno;

    KJ_REQUIRE(line[sizeof(line) - 1] == 0, "Line too long");

    line_length = strlen(line);

    // Remove trailing whitespace.
    while (line_length > 0 && std::isspace(line[line_length - 1]))
      line[--line_length] = 0;

    // Skip empty lines, and lines that start with '#'.
    if (!line[0] || line[0] == '#') continue;

    auto table_path = strchr(line, '\t');

    KJ_REQUIRE(table_path != nullptr, "Missing TAB character", lineno);

    *table_path++ = 0;
    auto offset_string = strchr(table_path, '\t');

    uint64_t offset = 0;

    if (offset_string) {
      *offset_string++ = 0;
      char* endptr;
      offset = static_cast<uint64_t>(strtoll(offset_string, &endptr, 0));
      KJ_REQUIRE(*endptr == 0);
    }

    if (!strcmp(line, "summary")) {
      summary_tables.emplace_back(
          offset,TableFactory::OpenSeekable(nullptr, table_path));
    } else if (!strcmp(line, "summary-override")) {
      summary_override_tables.emplace_back(
          TableFactory::Open(nullptr, table_path));
    } else if (!strcmp(line, "index")) {
      index_table_paths_.emplace_back(table_path);
    } else {
      KJ_FAIL_REQUIRE("Unknown table type", line, lineno);
    }
  }

  loaded_ = true;
}

std::vector<std::unique_ptr<Table>>& Schema::IndexTables() {
  Load();

  if (index_table_paths_.size() != index_tables_.size()) {
    for (const auto& path : index_table_paths_) {
      index_tables_.emplace_back(
          TableFactory::Open(nullptr, path.c_str()));
    }
  }

  return index_tables_;
}

}  // namespace table
}  // namespace cantera
