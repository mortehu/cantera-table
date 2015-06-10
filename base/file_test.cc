#include <fcntl.h>
#include <unistd.h>

#include "base/file.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct FileTest : testing::Test {};

TEST_F(FileTest, MakeTemporaryDirectory) {
  auto temp_path = TemporaryDirectory();
  EXPECT_EQ(0, rmdir(temp_path.c_str()));
}

TEST_F(FileTest, WriteAndReadFile) {
  auto temp_path = TemporaryDirectory();

  auto temp_file = temp_path;
  temp_file.append("/file");

  {
    kj::AutoCloseFd temp_fd(open(temp_file.c_str(), O_WRONLY | O_CREAT, 0777));
    ASSERT_TRUE(temp_fd != nullptr);

    WriteAll(temp_fd.get(), "1234");
  }

  auto read_buffer = ReadFile(temp_file.c_str());
  ASSERT_EQ(4U, read_buffer.size());
  EXPECT_EQ(0, memcmp(read_buffer.begin(), "1234", 4));

  EXPECT_EQ(0, unlink(temp_file.c_str()));
  EXPECT_EQ(0, rmdir(temp_path.c_str()));
}
