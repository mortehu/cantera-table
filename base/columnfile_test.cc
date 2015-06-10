#include <fcntl.h>

#include "base/cat.h"
#include "base/columnfile.h"
#include "base/file.h"
#include "third_party/gtest/gtest.h"

using namespace ev;

struct ColumnFileTest : testing::Test {};

TEST_F(ColumnFileTest, WriteTableToFile) {
  auto tmp_dir = TemporaryDirectory();
  DirectoryTreeRemover rm_tmp(tmp_dir);

  auto tmp_path = ev::cat(tmp_dir, "/test00");
  ColumnFileWriter writer(tmp_path.c_str());

  writer.Put(0, 0, "2000-01-01");
  writer.Put(0, 1, "January");
  writer.Put(0, 2, "First");

  writer.Put(0, 0, "2000-01-02");
  writer.Put(0, 1, "January");
  writer.Put(0, 2, "Second");

  writer.Put(0, 0, "2000-02-02");
  writer.Put(0, 1, "February");
  writer.Put(0, 2, "Second");
  writer.Flush();

  writer.Put(0, 0, "2000-02-03");
  writer.Put(0, 1, "February");
  writer.Put(0, 2, "Third");

  writer.Put(0, 0, "2000-02-03");
  writer.PutNull(0, 1);
  writer.PutNull(0, 2);
  writer.Finalize();

  ColumnFileReader reader(OpenFile(tmp_path.c_str(), O_RDONLY));

  EXPECT_FALSE(reader.End());

  auto row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-01-01", row[0].second.str());
  EXPECT_EQ("January", row[1].second.str());
  EXPECT_EQ("First", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-01-02", row[0].second.str());
  EXPECT_EQ("January", row[1].second.str());
  EXPECT_EQ("Second", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-02-02", row[0].second.str());
  EXPECT_EQ("February", row[1].second.str());
  EXPECT_EQ("Second", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-02-03", row[0].second.str());
  EXPECT_EQ("February", row[1].second.str());
  EXPECT_EQ("Third", row[2].second.str());

  EXPECT_FALSE(reader.End());

  row = reader.GetRow();
  EXPECT_EQ(1U, row.size());
  EXPECT_EQ("2000-02-03", row[0].second.str());

  EXPECT_TRUE(reader.End());
}

TEST_F(ColumnFileTest, WriteTableToString) {
  std::string buffer;

  ColumnFileWriter writer(buffer);
  writer.Put(0, 0, "2000-01-01");
  writer.Put(0, 1, "January");
  writer.Put(0, 2, "First");

  writer.Put(0, 0, "2000-01-02");
  writer.Put(0, 1, "January");
  writer.Put(0, 2, "Second");
  writer.Flush();

  writer.Put(0, 0, "2000-02-02");
  writer.Put(0, 1, "February");
  writer.Put(0, 2, "Second");

  std::string long_string(0xfff, 'x');
  writer.Put(0, 0, "2000-02-03");
  writer.Put(0, 1, "February");
  writer.Put(0, 2, long_string);

  writer.Put(0, 0, "2000-02-03");
  writer.PutNull(0, 1);
  writer.PutNull(0, 2);
  writer.Finalize();

  ColumnFileReader reader(buffer);

  EXPECT_FALSE(reader.End());

  auto row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-01-01", row[0].second.str());
  EXPECT_EQ("January", row[1].second.str());
  EXPECT_EQ("First", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-01-02", row[0].second.str());
  EXPECT_EQ("January", row[1].second.str());
  EXPECT_EQ("Second", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-02-02", row[0].second.str());
  EXPECT_EQ("February", row[1].second.str());
  EXPECT_EQ("Second", row[2].second.str());

  row = reader.GetRow();
  EXPECT_EQ(3U, row.size());
  EXPECT_EQ("2000-02-03", row[0].second.str());
  EXPECT_EQ("February", row[1].second.str());
  EXPECT_EQ(long_string, row[2].second.str());

  EXPECT_FALSE(reader.End());

  row = reader.GetRow();
  EXPECT_EQ(1U, row.size());
  EXPECT_EQ("2000-02-03", row[0].second.str());

  EXPECT_TRUE(reader.End());
}

TEST_F(ColumnFileTest, IntegerCoding) {
  static const uint32_t kTestNumbers[] = {
      0,          0x10U,      0x7fU,       0x80U,      0x100U,    0x1000U,
      0x3fffU,    0x4000U,    0x10000U,    0x100000U,  0x1fffffU, 0x200000U,
      0x1000000U, 0xfffffffU, 0x10000000U, 0xffffffffU};

  for (auto i : kTestNumbers) {
    std::string buffer;
    ColumnFileWriter::PutInt(buffer, i);

    EXPECT_TRUE((static_cast<uint8_t>(buffer[0]) & 0xc0) != 0xc0);

    ev::StringRef read_buffer(buffer);
    auto decoded_int = ColumnFileReader::GetInt(read_buffer);
    EXPECT_EQ(i, decoded_int);
    EXPECT_TRUE(read_buffer.empty());
  }
}
