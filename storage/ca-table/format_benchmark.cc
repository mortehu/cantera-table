#include <cassert>
#include <cstdint>
#include <vector>

#include <sys/time.h>

#include "storage/ca-table/ca-table.h"

int main(int argc, char** argv) {
  std::vector<ca_offset_score> values;

  uint64_t offset = 0;
  for (size_t i = 0; i < 10000000; ++i) {
    ca_offset_score v;
    v.offset = offset;
    v.score = rand() % 0x1000000;
    offset += 86400 * (1 + (rand() % 16));

    values.emplace_back(v);
  }

  auto size = ca_offset_score_size(&values[0], values.size());

  std::vector<uint8_t> encoded(size);

  struct timeval start, end;

  {
    gettimeofday(&start, nullptr);
    auto size = ca_format_offset_score(&encoded[0], &values[0], values.size());
    gettimeofday(&end, nullptr);

    printf("Encode: %.3f\n", (end.tv_sec - start.tv_sec) +
                                 1.0e-6 * (end.tv_usec - start.tv_usec));
    encoded.resize(size);
  }

  {
    std::vector<ca_offset_score> decoded;
    gettimeofday(&start, nullptr);
    ca_offset_score_parse(encoded, &decoded);
    gettimeofday(&end, nullptr);

    assert(std::equal(decoded.begin(), decoded.end(), values.begin(),
                      values.end(), [](auto& lhs, auto& rhs) {
      return lhs.offset == rhs.offset && lhs.score == rhs.score;
    }));
    printf("Decode: %.3f\n", (end.tv_sec - start.tv_sec) +
                                 1.0e-6 * (end.tv_usec - start.tv_usec));
  }
}
