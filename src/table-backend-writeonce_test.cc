#include <fcntl.h>
#include <unistd.h>

#include "src/ca-table.h"
#include "third_party/gtest/gtest.h"

#include <kj/exception.h>

using namespace cantera::table;

struct WriteOnceTest : testing::Test {
  static constexpr char name_template[] = "/tmp/ca-table-test-XXXXXX";

 public:
  void SetUp() override {
    char name[sizeof(name_template)];
    strcpy(name, name_template);
    ASSERT_NE(mkdtemp(name), nullptr);
    temp_directory_ = name;
  }

  void TearDown() override {
    std::string cmd;
    cmd.append("rm -rf ");
    cmd.append(temp_directory_);
    system(cmd.c_str());
  }

 protected:
  std::string temp_directory_;
};

TEST_F(WriteOnceTest, CanWriteThenRead) {
  auto builder = TableFactory::Create(
      "write-once", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->InsertRow("a", "xxx");
  builder->InsertRow("b", "yyy");
  builder->InsertRow("c", "zzz");
  builder->InsertRow("d", "www");
  builder->Sync();
  builder.reset();

  auto table_handle =
      TableFactory::Open("write-once", (temp_directory_ + "/table_00").c_str());
  EXPECT_TRUE(table_handle->IsSorted());
  EXPECT_TRUE(table_handle->SeekToKey("a"));
  EXPECT_FALSE(table_handle->SeekToKey("D"));
  EXPECT_TRUE(table_handle->SeekToKey("c"));
  EXPECT_FALSE(table_handle->SeekToKey("A"));
  EXPECT_FALSE(table_handle->SeekToKey("C"));
  EXPECT_FALSE(table_handle->SeekToKey("B"));
  EXPECT_TRUE(table_handle->SeekToKey("d"));
  EXPECT_TRUE(table_handle->SeekToKey("b"));
}

TEST_F(WriteOnceTest, CanWriteThenReadMany) {
  auto builder = TableFactory::Create(
      "write-once", (temp_directory_ + "/table_00").c_str(), TableOptions());
  char str[3];
  str[2] = 0;
  for (str[0] = 'a'; str[0] <= 'z'; ++str[0]) {
    for (str[1] = 'a'; str[1] <= 'z'; ++str[1]) {
      builder->InsertRow(str, "xxx");
    }
  }

  builder->Sync();
  builder.reset();

  auto table_handle =
      TableFactory::Open("write-once", (temp_directory_ + "/table_00").c_str());
  EXPECT_TRUE(table_handle->IsSorted());

  for (str[0] = 'a'; str[0] <= 'z'; ++str[0]) {
    for (str[1] = 'a'; str[1] <= 'z'; ++str[1]) {
      EXPECT_EQ(1, table_handle->SeekToKey(str));
    }
  }
}

TEST_F(WriteOnceTest, InsertOutOfOrderThrows) {
  auto builder = TableFactory::Create(
      "write-once", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->InsertRow("a", "xxx");
  builder->InsertRow("b", "yyy");
  builder->InsertRow("c", "zzz");
  ASSERT_THROW(builder->InsertRow("c", "xxx"), kj::Exception);
}

TEST_F(WriteOnceTest, CanWriteThenReadUnsorted) {
  auto builder = TableFactory::Create("write-once",
                                      (temp_directory_ + "/table_00").c_str(),
                                      TableOptions().SetInputUnsorted(true));
  builder->InsertRow("a", "xxx");
  builder->InsertRow("c", "zzz");
  builder->InsertRow("d", "www");
  builder->InsertRow("b", "yyy");
  builder->Sync();
  builder.reset();

  auto table_handle =
      TableFactory::Open("write-once", (temp_directory_ + "/table_00").c_str());
  EXPECT_TRUE(table_handle->IsSorted());
  EXPECT_TRUE(table_handle->SeekToKey("a"));
  EXPECT_FALSE(table_handle->SeekToKey("D"));
  EXPECT_TRUE(table_handle->SeekToKey("c"));
  EXPECT_FALSE(table_handle->SeekToKey("A"));
  EXPECT_FALSE(table_handle->SeekToKey("C"));
  EXPECT_FALSE(table_handle->SeekToKey("B"));
  EXPECT_TRUE(table_handle->SeekToKey("d"));
  EXPECT_TRUE(table_handle->SeekToKey("b"));
}

TEST_F(WriteOnceTest, EmptyTableOK) {
  auto builder = TableFactory::Create(
      "write-once", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->Sync();
  builder.reset();

  auto table_handle =
      TableFactory::Open("write-once", (temp_directory_ + "/table_00").c_str());
}

TEST_F(WriteOnceTest, UnsyncedTableNotWritten) {
  auto table_handle = TableFactory::Create(
      "write-once", (temp_directory_ + "/table_00").c_str(), TableOptions());
  table_handle.reset();

  ASSERT_THROW(
      TableFactory::Open("write-once", (temp_directory_ + "/table_00").c_str()),
      kj::Exception);
}
