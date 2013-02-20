/*
    Sorting algorithms
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

#include "ca-table.h"
#include "ca-internal.h"

/* This source file is C++ plus STL due to its support for generating
 * specialized sorting algorithms with ease */

/*****************************************************************************/

struct CA_offset_score_compare
{
  bool operator()(const ca_offset_score &lhs,
                  const ca_offset_score &rhs) const
    {
      return lhs.score > rhs.score;
    }
};

void
ca_sort_offset_score (struct ca_offset_score *data, size_t count)
{
  std::sort (data, data + count, CA_offset_score_compare());
}

/*****************************************************************************/

struct CA_float_rank_compare
{
  bool operator()(const CA_float_rank &lhs,
                  const CA_float_rank &rhs) const
    {
      return lhs.value < rhs.value;
    }
};

void
CA_sort_float_rank (struct CA_float_rank *data, size_t count)
{
  std::sort (data, data + count, CA_float_rank_compare());
}
