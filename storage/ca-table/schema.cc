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

#include <cassert>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <kj/debug.h>

#include "storage/ca-table/ca-table.h"
#include "storage/ca-table/error.h"
#include "storage/ca-table/query.h"

enum ca_schema_table_type {
  CA_SCHEMA_TABLE_UNKNOWN,
  CA_SCHEMA_TABLE_SUMMARY,
  CA_SCHEMA_TABLE_SUMMARY_OVERRIDE,
  CA_SCHEMA_TABLE_INDEX,
  CA_SCHEMA_TABLE_TIME_SERIES
};

struct ca_schema_table {
  enum ca_schema_table_type type = CA_SCHEMA_TABLE_UNKNOWN;
  std::string path;
  std::string prefix;
  uint64_t offset = 0;
};

struct ca_time_series_table {
  std::unique_ptr<ca_table::Table> table;
  std::string prefix;
};

struct ca_schema {
  std::string path;

  std::vector<ca_schema_table> tables;

  std::vector<std::unique_ptr<ca_table::Table>> summary_tables;
  std::vector<uint64_t> summary_table_offsets;

  std::vector<std::unique_ptr<ca_table::Table>> summary_override_tables;

  std::vector<std::unique_ptr<ca_table::Table>> index_tables;

  std::vector<ca_time_series_table> time_series_tables;
};

static int CA_schema_load(struct ca_schema* schema) {
  FILE* f;
  char line[4096];
  int lineno = 0, result = -1;

  if (!(f = fopen(schema->path.c_str(), "r"))) {
    ca_set_error("Failed to open '%s' for reading: %s", schema->path.c_str(),
                 strerror(errno));

    return -1;
  }

  line[sizeof(line) - 1] = 0;

  while (NULL != fgets(line, sizeof(line), f)) {
    struct ca_schema_table table;
    char* path, *offset_string;
    size_t line_length;

    ++lineno;

    if (line[sizeof(line) - 1]) {
      ca_set_error("%s:%d: Line too long.  Max is %zu", lineno,
                   schema->path.c_str(), sizeof(line) - 1);

      goto fail;
    }

    line_length = strlen(line);

    /* fgets stores the \n, and we might just as well remove all trailing
     * whitespace */
    while (line_length && isspace(line[line_length - 1]))
      line[--line_length] = 0;

    if (!line[0] || line[0] == '#') continue;

    path = strchr(line, '\t');

    if (!path) {
      ca_set_error("%s:%d: Missing TAB character", schema->path.c_str(),
                   lineno);

      goto fail;
    }

    *path++ = 0;
    offset_string = strchr(path, '\t');

    if (!strcmp(line, "summary")) {
      table.type = CA_SCHEMA_TABLE_SUMMARY;
    } else if (!strcmp(line, "summary-override")) {
      table.type = CA_SCHEMA_TABLE_SUMMARY_OVERRIDE;
    } else if (!strcmp(line, "index")) {
      table.type = CA_SCHEMA_TABLE_INDEX;
    } else if (!strcmp(line, "time-series")) {
      table.type = CA_SCHEMA_TABLE_TIME_SERIES;
    } else {
      ca_set_error("%s:%d: Unknown table type \"%s\"", schema->path.c_str(),
                   lineno, line);

      goto fail;
    }

    if (offset_string) {
      *offset_string++ = 0;

      if (table.type == CA_SCHEMA_TABLE_SUMMARY) {
        char* endptr;

        table.offset = (uint64_t)strtoll(offset_string, &endptr, 0);

        if (*endptr) {
          ca_set_error("%s:%d: Expected EOL after offset, got \\x%02x",
                       (unsigned char)*endptr);

          goto fail;
        }
      } else if (table.type == CA_SCHEMA_TABLE_TIME_SERIES) {
        table.prefix = offset_string;
      } else {
        ca_set_error("%s:%d: Unexpected column for table type \"%s\"", line);

        goto fail;
      }
    } else
      table.offset = 0;

    table.path = path;

    schema->tables.emplace_back(table);
  }

  result = 0;

fail:

  fclose(f);

  return result;
}

struct ca_schema* ca_schema_load(const char* path) {
  struct ca_schema* result;

  int ok = 0;

  result = new ca_schema;

  result->path = path;

  if (-1 == CA_schema_load(result)) goto fail;

  ok = 1;

fail:

  if (!ok) {
    ca_schema_close(result);
    result = NULL;
  }

  return result;
}

void ca_schema_close(struct ca_schema* schema) { delete schema; }

std::vector<ca_table::Table*> ca_schema_summary_tables(struct ca_schema* schema,
                                                       uint64_t** offsets) {
  if (schema->summary_tables.empty()) {
    for (size_t i = 0; i < schema->tables.size(); ++i) {
      if (schema->tables[i].type != CA_SCHEMA_TABLE_SUMMARY) continue;

      schema->summary_tables.emplace_back(
          ca_table_open(NULL, schema->tables[i].path.c_str(), O_RDONLY, 0));
      schema->summary_table_offsets.emplace_back(schema->tables[i].offset);
    }
  }

  *offsets = &schema->summary_table_offsets[0];

  std::vector<ca_table::Table*> result;
  result.reserve(schema->summary_tables.size());
  for (auto& table : schema->summary_tables) result.emplace_back(table.get());

  return result;
}

std::vector<ca_table::Table*> ca_schema_summary_override_tables(
    struct ca_schema* schema) {
  if (schema->summary_override_tables.empty()) {
    for (size_t i = 0; i < schema->tables.size(); ++i) {
      if (schema->tables[i].type != CA_SCHEMA_TABLE_SUMMARY_OVERRIDE) continue;

      schema->summary_override_tables.emplace_back(
          ca_table_open(NULL, schema->tables[i].path.c_str(), O_RDONLY, 0));
    }
  }

  std::vector<ca_table::Table*> result;
  result.reserve(schema->summary_override_tables.size());
  for (auto& table : schema->summary_override_tables)
    result.emplace_back(table.get());

  return result;
}

std::vector<ca_table::Table*> ca_schema_index_tables(struct ca_schema* schema) {
  if (schema->index_tables.empty()) {
    for (size_t i = 0; i < schema->tables.size(); ++i) {
      if (schema->tables[i].type != CA_SCHEMA_TABLE_INDEX) continue;

      schema->index_tables.emplace_back(
          ca_table_open(NULL, schema->tables[i].path.c_str(), O_RDONLY, 0));
    }
  }

  std::vector<ca_table::Table*> result;
  result.reserve(schema->index_tables.size());
  for (auto& table : schema->index_tables) result.emplace_back(table.get());

  return result;
}

std::vector<std::pair<ca_table::Table*, std::string>>
ca_schema_time_series_tables(struct ca_schema* schema) {
  if (schema->time_series_tables.empty()) {
    for (size_t i = 0; i < schema->tables.size(); ++i) {
      if (schema->tables[i].type != CA_SCHEMA_TABLE_TIME_SERIES) continue;

      ca_time_series_table ts_table;
      ts_table.table = std::move(
          ca_table_open(NULL, schema->tables[i].path.c_str(), O_RDONLY, 0));

      if (!schema->tables[i].prefix.empty())
        ts_table.prefix = schema->tables[i].prefix;

      schema->time_series_tables.emplace_back(std::move(ts_table));
    }
  }

  std::vector<std::pair<ca_table::Table*, std::string>> result;
  result.reserve(schema->time_series_tables.size());
  for (auto& ts_table : schema->time_series_tables)
    result.emplace_back(ts_table.table.get(), ts_table.prefix);

  return result;
}
