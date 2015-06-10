/*
    Computes the 160 bit SHA-1 hash of a byte oriented message
    Copyright (C) 2011    Morten Hustveit

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

#include <cstdlib>
#include <cstring>
#include <cstdint>

#include "base/sha1.h"
#include "base/string.h"

namespace ev {

std::string SHA1::ToASCII(const uint8_t* sha1) {
  std::string result;
  result.reserve(40);

  BinaryToHex(sha1, 20, &result);

  return result;
}

SHA1::SHA1() { SHA1_Init(&sha_ctx_); }

void SHA1::Add(const void* data, size_t size) {
  SHA1_Update(&sha_ctx_, data, size);
}

void SHA1::Finish(uint8_t* digest) { SHA1_Final(digest, &sha_ctx_); }

}  // namespace ev
