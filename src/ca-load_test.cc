#include <initializer_list>
#include <vector>

#include <sys/time.h>

#include <kj/debug.h>

#include "src/ca-table.h"
#include "third_party/gtest/gtest.h"

using namespace cantera::table;

struct CaLoadTest : testing::Test {
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
  void Exec(const std::vector<const char*>& args) {
    pid_t child;
    KJ_SYSCALL(child = fork());
    if (!child) {
      execve(args[0], const_cast<char* const*>(&args[0]), nullptr);
      _exit(EXIT_FAILURE);
    }

    int status;
    KJ_SYSCALL(waitpid(child, &status, 0));
    EXPECT_EQ(0, status);
  }

  void Merge(const std::string& output, const std::vector<std::string>& inputs,
             const std::initializer_list<std::string> extra_args = {}) {
    std::vector<const char*> args;
    args.emplace_back("./ca-load");
    args.emplace_back("--merge-mode=pick-one");
    for (const auto& arg : extra_args) args.emplace_back(arg.c_str());
    args.emplace_back(output.c_str());
    for (const auto& input : inputs) args.emplace_back(input.c_str());
    args.emplace_back(nullptr);
    Exec(args);
    ASSERT_EQ(0, access(output.c_str(), R_OK));
  }

  std::string temp_directory_;
};

TEST_F(CaLoadTest, AddsPrefixToSingleTable) {
  auto table_00_path = (temp_directory_ + "/table_00");
  auto table_01_path = (temp_directory_ + "/table_01");

  ca_offset_score v;
  memset(&v, 0, sizeof(v));

  auto table_00 = TableFactory::Create("leveldb-table", table_00_path.c_str(),
                                       TableOptions());
  v.offset = 1;
  v.score = 1.0f;
  ca_table_write_offset_score(table_00.get(), "a", &v, 1);
  table_00->Sync();
  table_00.reset();

  Merge(table_01_path, {table_00_path}, {"--add-key-prefix=foo:"});

  auto table_01 = TableFactory::Open("leveldb-table", table_01_path.c_str());
  EXPECT_FALSE(table_01->SeekToKey("a"));
  EXPECT_TRUE(table_01->SeekToKey("foo:a"));
}

TEST_F(CaLoadTest, AddsPrefixToMergedTables) {
  auto table_00_path = (temp_directory_ + "/table_00");
  auto table_01_path = (temp_directory_ + "/table_01");
  auto table_02_path = (temp_directory_ + "/table_02");

  ca_offset_score v;
  memset(&v, 0, sizeof(v));

  auto table_00 = TableFactory::Create("leveldb-table", table_00_path.c_str(),
                                       TableOptions());
  v.offset = 1;
  v.score = 0.5f;
  ca_table_write_offset_score(table_00.get(), "a", &v, 1);

  v.offset = 3;
  v.score = 6.0f;
  ca_table_write_offset_score(table_00.get(), "b", &v, 1);
  table_00->Sync();
  table_00.reset();

  auto table_01 = TableFactory::Create("leveldb-table", table_01_path.c_str(),
                                       TableOptions());
  v.offset = 2;
  v.score = 4.0f;
  ca_table_write_offset_score(table_01.get(), "a", &v, 1);

  v.offset = 4;
  v.score = 8.0f;
  ca_table_write_offset_score(table_01.get(), "c", &v, 1);
  table_01->Sync();
  table_01.reset();

  Merge(table_02_path, {table_00_path, table_01_path},
        {"--add-key-prefix=foo:"});

  auto table_02 = TableFactory::Open("leveldb-table", table_02_path.c_str());
  EXPECT_FALSE(table_02->SeekToKey("a"));
  EXPECT_FALSE(table_02->SeekToKey("b"));
  EXPECT_FALSE(table_02->SeekToKey("c"));
  EXPECT_TRUE(table_02->SeekToKey("foo:a"));
  EXPECT_TRUE(table_02->SeekToKey("foo:b"));
  EXPECT_TRUE(table_02->SeekToKey("foo:c"));
}
