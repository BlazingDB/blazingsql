#include <gtest/gtest.h>
#include <sys/stat.h>

#include "gdf/library/csv.h"
#include "gdf/library/table.h"
#include "gdf/library/table_group.h"
#include "gdf/library/types.h"


using namespace gdf::library;

struct UtilsTest : public ::testing::Test {
  void SetUp() { rmmInitialize(nullptr); }
};

TEST_F(UtilsTest, TableBuilder) {
  Table t =
      TableBuilder{
          "emps",
          {
              {"x", [](Index i) -> DType<GDF_FLOAT64> { return i / 10.0; }},
              // TODO percy noboa see upgrade to uints
              //{ "y", [](Index i) -> DType<GDF_UINT64> { return i * 1000; } },
              {"y", [](Index i) -> DType<GDF_INT64> { return i * 1000; }},
          }}
          .Build(10);

  for (std::size_t i = 0; i < 10; i++) {
    // TODO percy noboa see upgrade to uints
    // EXPECT_EQ(i * 1000, t[1][i].get<GDF_UINT64>());
    EXPECT_EQ(i * 1000, t[1][i].get<GDF_INT64>());
  }

  for (std::size_t i = 0; i < 10; i++) {
    EXPECT_EQ(i / 10.0, t[0][i].get<GDF_FLOAT64>());
  }
  t.print(std::cout);
}

TEST_F(UtilsTest, FrameFromTableGroup) {
  auto g =
      TableGroupBuilder{
          {"emps",
           {
               {"x", [](Index i) -> DType<GDF_FLOAT64> { return i / 10.0; }},
               //{ "y", [](Index i) -> DType<GDF_UINT64> { return i * 1000; } },
               // TODO percy noboa see upgrade to uints
               {"y", [](Index i) -> DType<GDF_INT64> { return i * 1000; }},
           }},
          {"emps",
           {
               {"x", [](Index i) -> DType<GDF_FLOAT64> { return i / 100.0; }},
               // TODO percy noboa see upgrade to uints
               //{ "y", [](Index i) -> DType<GDF_UINT64> { return i * 10000; }
               //},
               {"y", [](Index i) -> DType<GDF_INT64> { return i * 10000; }},
           }}}
          .Build({10, 20});

  g[0].print(std::cout);
  g[1].print(std::cout);

  BlazingFrame frame = g.ToBlazingFrame();

  // auto hostVector = HostVectorFrom<GDF_UINT64>(frame[1][1]);
  // TODO percy noboa see upgrade to uints
  auto hostVector = HostVectorFrom<GDF_INT64>(frame[1][1]);

  for (std::size_t i = 0; i < 20; i++) {
    EXPECT_EQ(i * 10000, hostVector[i]);
  }
}

TEST_F(UtilsTest, TableFromLiterals) {
  auto t = LiteralTableBuilder{"emps",
                               {
                                   {
                                       "x",
                                       Literals<GDF_FLOAT64>{1, 3, 5, 7, 9},
                                   },
                                   {
                                       "y",
                                       Literals<GDF_INT64>{0, 2, 4, 6, 8},
                                   },
                               }}
               .Build();

  for (std::size_t i = 0; i < 5; i++) {
    EXPECT_EQ(2 * i, t[1][i].get<GDF_INT64>());
    EXPECT_EQ(2 * i + 1.0, t[0][i].get<GDF_FLOAT64>());
  }

  using VTableBuilder =
      gdf::library::TableRowBuilder<int8_t, double, int32_t, int64_t>;
  using DataTuple = VTableBuilder::DataTuple;

  gdf::library::Table table =
      VTableBuilder{
          "emps",
          {"Id", "Weight", "Age", "Name"},
          {
              DataTuple{'a', 180.2, 40, 100L},
              DataTuple{'b', 175.3, 38, 200L},
              DataTuple{'c', 140.3, 27, 300L},
          },
      }
          .Build();

  table.print(std::cout);
}

TEST_F(UtilsTest, FrameFromGdfColumnsCpps) {
  auto t = LiteralTableBuilder{"emps",
                               {
                                   {
                                       "x",
                                       Literals<GDF_FLOAT64>{1, 3, 5, 7, 9},
                                   },
                                   {
                                       "y",
                                       Literals<GDF_INT64>{0, 2, 4, 6, 8},
                                   },
                               }}
               .Build();

  auto u = GdfColumnCppsTableBuilder{"emps", t.ToGdfColumnCpps()}.Build();

  for (std::size_t i = 0; i < 5; i++) {
    EXPECT_EQ(2 * i, u[1][i].get<GDF_INT64>());
    EXPECT_EQ(2 * i + 1.0, u[0][i].get<GDF_FLOAT64>());
  }

  EXPECT_EQ(t, u);
}

TEST_F(UtilsTest, LiteralTableWithNulls) {
  Literals<GDF_FLOAT64>::vector values = {7, 5, 6, 3, 1, 2, 8, 4,
                                          1, 2, 8, 4, 1, 2, 8, 4};
  Literals<GDF_FLOAT64>::valid_vector valids = {0xF0, 0x0F};
  auto t = LiteralTableBuilder(
               "emps",
               {
                   LiteralColumnBuilder{
                       "x",
                       Literals<GDF_FLOAT64>{
                           Literals<GDF_FLOAT64>::vector{values},
                           Literals<GDF_FLOAT64>::valid_vector{valids}},
                   },
               })
               .Build();
  t.print(std::cout);
  auto u = GdfColumnCppsTableBuilder{"emps", t.ToGdfColumnCpps()}.Build();

  for (std::size_t i = 0; i < 16; i++) {
    // std::cout << t[0][i].get<GDF_FLOAT64>() << std::endl;
    EXPECT_EQ(values[i], t[0][i].get<GDF_FLOAT64>());
  }
  const auto &computed_valids = t[0].getValids();
  std::cout << "valids.size: " << valids.size() << std::endl;
  std::cout << "computed_valids.size: " << computed_valids.size() << std::endl;

  for (std::size_t i = 0; i < valids.size(); i++) {
    std::cout << "\t" << (int)valids[i] << " - " << (int)computed_valids[i]
              << std::endl;
    EXPECT_EQ(valids[i], computed_valids[i]);
  }
  EXPECT_EQ(t, u);
}

// TEST(UtilsTest, CSVReaderForCustomerFile)
// {
//   io::CSVReader<8> in("/home/aocsa/blazingdb/tpch/1mb/customer.psv");
//   std::vector<std::string> columnNames = { "c_custkey", "c_nationkey",
//   "c_acctbal" };
//   //  std::vector<std::string> columnTypes =  {GDF_INT32, GDF_INT32,
//   GDF_FLOAT32}; using VTableBuilder = gdf::library::TableRowBuilder<int32_t,
//   int32_t, float>; using DataTuple = VTableBuilder::DataTuple;
//   std::vector<DataTuple> rows;
//   int c_custkey;
//   std::string c_name;
//   std::string c_address;
//   int c_nationkey;
//   std::string c_phone;
//   float c_acctbal;
//   std::string c_mktsegment;
//   std::string c_comment;
//   while (in.read_row(c_custkey, c_name, c_address, c_nationkey, c_phone,
//   c_acctbal, c_mktsegment, c_comment)) {
//     // do stuff with the data
//     rows.push_back(DataTuple{ c_custkey, c_nationkey, c_acctbal });
//   }
//   gdf::library::Table table = VTableBuilder{
//     .name = "customer",
//     .headers = columnNames,
//     .rows = rows,
//   }.Build();
//   table.print(std::cout);
// }
