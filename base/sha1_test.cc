#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "base/sha1.h"

void RunTests(const char* path) {
  FILE* input;
  size_t size;
  char* line_begin, *line_end;
  char* len = 0, *msg = 0;
  unsigned char* ref_digest = 0;

  const char* hex = "0123456789abcdef";

  unsigned char digest[20];

  unsigned int i;

  input = fopen(path, "r");

  assert(input);

  fseek(input, 0, SEEK_END);
  size = ftell(input);
  fseek(input, 0, SEEK_SET);

  std::unique_ptr<char[]> buffer(new char[size + 1]);

  if (size != fread(buffer.get(), 1, size, input)) assert(!"fread failed");

  fclose(input);

  buffer[size] = 0;

  line_begin = buffer.get();

  while (*line_begin) {
    if (*line_begin && isspace(*line_begin)) ++line_begin;

    line_end = line_begin;

    while (*line_end && *line_end != '\n' && *line_end != '\r') ++line_end;

    if (!*line_end) break;

    *line_end = 0;

    if (!strncmp(line_begin, "Len = ", 6))
      len = line_begin + 6;
    else if (!strncmp(line_begin, "Msg = ", 6))
      msg = line_begin + 6;
    else if (!strncmp(line_begin, "MD = ", 5))
      ref_digest = (unsigned char*)line_begin + 5;

    if (len && msg && ref_digest) {
      auto length = strtol(len, 0, 0);

      assert(!(length % 8));

      length /= 8;

      assert(strlen(msg) >= static_cast<size_t>(length) * 2);
      assert(strspn(msg, hex) == strlen(msg));
      assert(strlen((char*)ref_digest) == 40);

      for (i = 0; i < length; ++i)
        msg[i] = ((strchr(hex, (unsigned char)msg[i * 2]) - hex) << 4) |
                 (strchr(hex, (unsigned char)msg[i * 2 + 1]) - hex);

      for (i = 0; i < 20; ++i)
        ref_digest[i] =
            ((strchr(hex, (unsigned char)ref_digest[i * 2]) - hex) << 4) |
            (strchr(hex, (unsigned char)ref_digest[i * 2 + 1]) - hex);

      ev::SHA1 sha1;
      sha1.Add(msg, length);
      sha1.Finish(digest);

      for (i = 0; i < 20; ++i) assert(ref_digest[i] == digest[i]);

      len = 0;
      msg = 0;
      ref_digest = 0;
    }

    line_begin = line_end + 1;
  }
}

int main(int argc, char** argv) {
  RunTests("base/testdata/SHA1ShortMsg.txt");
  RunTests("base/testdata/SHA1LongMsg.txt");
}
