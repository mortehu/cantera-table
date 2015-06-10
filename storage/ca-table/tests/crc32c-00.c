#include <assert.h>
#include <string.h>

#include "storage/ca-table/crc32c.h"

static const struct
{
  uint32_t digest;
  const char *text;
} test_vectors[] =
{
    { 0x00000000, "" },
    { 0xbfe92a83, "23456789" },
    { 0xe3069283, "123456789" },
    { 0xf3dbd4fe, "1234567890" },
    { 0x22620404, "The quick brown fox jumps over the lazy dog" }
};

int main(int argc, char **argv)
{
  size_t i, j;

  for (i = 0; i < sizeof (test_vectors) / sizeof (test_vectors[0]); ++i)
    {
      uint32_t crc32c = 0;

      assert (test_vectors[i].digest == ca_crc32c (0, test_vectors[i].text, strlen (test_vectors[i].text)));

      for (j = 0; j < strlen (test_vectors[i].text); ++j)
        crc32c = ca_crc32c (crc32c, &test_vectors[i].text[j], 1);

      assert (test_vectors[i].digest == crc32c);
    }

  return 0;
}
