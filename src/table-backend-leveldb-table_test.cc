#include <fcntl.h>
#include <unistd.h>

#include "src/ca-table.h"
#include "third_party/gtest/gtest.h"

#include <kj/exception.h>

using namespace cantera::table;

struct LevelDBTest : testing::Test {
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

TEST_F(LevelDBTest, InsertOutOfOrderThrows) {
  auto builder = TableFactory::Create(
      "leveldb-table", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->InsertRow("a", "xxx");
  builder->InsertRow("b", "yyy");
  builder->InsertRow("c", "zzz");
  ASSERT_THROW(builder->InsertRow("c", "xxx"), kj::Exception);
}

TEST_F(LevelDBTest, CanWriteThenRead) {
  auto builder = TableFactory::Create(
      "leveldb-table", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->InsertRow("a", "xxx");
  builder->InsertRow("b", "yyy");
  builder->InsertRow("c", "zzz");
  builder->InsertRow("d", "www");
  builder->Sync();

  auto table_handle = TableFactory::Open(
      "leveldb-table", (temp_directory_ + "/table_00").c_str());
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

TEST_F(LevelDBTest, EmptyTableOK) {
  auto builder = TableFactory::Create(
      "leveldb-table", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder->Sync();

  auto table_handle = TableFactory::Open(
      "leveldb-table", (temp_directory_ + "/table_00").c_str());
}

TEST_F(LevelDBTest, UnsyncedTableNotWritten) {
  auto builder = TableFactory::Create(
      "leveldb-table", (temp_directory_ + "/table_00").c_str(), TableOptions());
  builder.reset();

  ASSERT_THROW(TableFactory::Open("leveldb-table",
                                  (temp_directory_ + "/table_00").c_str()),
               kj::Exception);
}

TEST_F(LevelDBTest, CanWriteThenReadMany) {
  auto builder = TableFactory::Create(
      "leveldb-table", (temp_directory_ + "/table_00").c_str(), TableOptions());
  char str[3];
  str[2] = 0;
  for (str[0] = 'a'; str[0] <= 'z'; ++str[0]) {
    for (str[1] = 'a'; str[1] <= 'z'; ++str[1]) {
      builder->InsertRow(str, "xxx");
    }
  }

  builder->Sync();

  auto table_handle = TableFactory::Open(
      "leveldb-table", (temp_directory_ + "/table_00").c_str());
  EXPECT_TRUE(table_handle->IsSorted());

  for (str[0] = 'a'; str[0] <= 'z'; ++str[0]) {
    for (str[1] = 'a'; str[1] <= 'z'; ++str[1]) {
      EXPECT_TRUE(table_handle->SeekToKey(str));
    }
  }
}
