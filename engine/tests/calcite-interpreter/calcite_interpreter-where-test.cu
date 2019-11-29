#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <DataFrame.h>
#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
#include <Utils.cuh>
#include <blazingdb/io/Util/StringUtil.h>
#include <gdf/library/table_group.h>
#include "../query_test.h"


class TestEnvironment : public testing::Environment {
public:
  virtual ~TestEnvironment() {}
  virtual void SetUp() { rmmInitialize(nullptr); }

  void TearDown() {
    cudaDeviceReset(); // for cuda-memchecking
  }
};

using gdf::library::DType;
using gdf::library::Index;
using gdf::library::TableGroupBuilder;

using RType = DType<GDF_INT32>;

struct calcite_interpreter_TEST : public  query_test {

  gdf::library::TableGroup group;

  calcite_interpreter_TEST()
      : group{TableGroupBuilder{
            {"hr.emps",
             {
                 {"x", [](Index i) -> RType { return i % 2 ? i : 1; }},
                 {"y", [](Index i) -> RType { return i; }},
                 {"z", [](Index) -> RType { return 1; }},
             }},
        }
                  .Build(num_values)} {}

  void SetUp() {
	rmmInitialize(nullptr);
    input_tables = group.ToBlazingFrame();

    input1 = reinterpret_cast<const std::int32_t *>(group[0][0].get(0));
    input2 = reinterpret_cast<const std::int32_t *>(group[0][1].get(0));
    input3 = reinterpret_cast<const std::int32_t *>(group[0][2].get(0));
  }

  void TearDown() {

    for (std::size_t i = 0; i < outputs.size(); i++) {

      // Releasing allocated memory, here we are responsible for that
      // TODO percy rommel: move to integration/end-to-end test
      // GDFRefCounter::getInstance()->free_if_deregistered(outputs[i].get_gdf_column());
    }
  }

  void Check(gdf_column_cpp out_col, int32_t *host_output,
             size_t num_output_values = 0) {

    if (num_output_values == 0)
      num_output_values = out_col.size();

    int32_t *device_output;
    device_output = new int32_t[num_output_values];
    cudaMemcpy(device_output, out_col.data(),
               num_output_values * sizeof(int32_t), cudaMemcpyDeviceToHost);

    for (std::size_t i = 0; i < num_output_values; i++) {
      EXPECT_TRUE(host_output[i] == device_output[i]);
    }
  }

  gdf_column_cpp left;
  gdf_column_cpp right;
  gdf_column_cpp third;

  std::vector<gdf_column_cpp> inputs;

  static const std::size_t num_values = 32;

  const std::int32_t *input1;
  const std::int32_t *input2;
  const std::int32_t *input3;

  std::vector<std::vector<gdf_column_cpp>> input_tables;
  std::vector<std::string> table_names = {"hr.emps"};
  std::vector<std::vector<std::string>> column_names = {{"x", "y", "z"}};

  std::vector<gdf_column_cpp> outputs;
};

TEST_F(calcite_interpreter_TEST, where_greater) {

  { // select y from hr.emps where y > 5
    std::string query = "\
LogicalProject(y=[$1])\n\
  LogicalFilter(condition=[>($1, 5)])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int cur = 0;
    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      if (input2[i] > 5) {
        host_output[cur] = input2[i];
        cur++;
      }
    }

    Check(outputs[0], host_output, cur);
  }
}

TEST_F(calcite_interpreter_TEST, where_equals) {

  { // select z from hr.emps where x = y
    std::string query = "\
LogicalProject(z=[$2])\n\
  LogicalFilter(condition=[=($0, $1)])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int cur = 0;
    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      if (input1[i] == input2[i]) {
        host_output[cur] = input3[i];
        cur++;
      }
    }

    Check(outputs[0], host_output, cur);
  }
}

// ToDo: fix both literals returns invalid_api_call
TEST_F(calcite_interpreter_TEST, DISABLED_processing_project51) {

  { // select z from hr.emps where 1 = 1
    std::string query = "\
LogicalProject(z=[$2])\n\
  LogicalFilter(condition=[=(1, 1)])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      host_output[i] = input3[i];
    }

    Check(outputs[0], host_output);
  }
}

TEST_F(calcite_interpreter_TEST, where_withmod) {

  { // select y from hr.emps where mod(y,5)=0"
    std::string query = "\
LogicalProject(y=[$1])\n\
  LogicalFilter(condition=[=(MOD($1, 5), 0)])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int cur = 0;
    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      if ((input2[i]) % 5 == 0) {
        host_output[cur] = input2[i];
        cur++;
      }
    }

    Check(outputs[0], host_output, cur);
  }
}

TEST_F(calcite_interpreter_TEST, where_equals_and_greater) {

  { // select z from hr.emps where x = y and x > 4
    std::string query = "\
LogicalProject(z=[$2])\n\
  LogicalFilter(condition=[AND(=($1, $0), >($0, 4))])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int cur = 0;
    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      if (input1[i] == input2[i] && input1[i] > 4) {
        host_output[cur] = input3[i];
        cur++;
      }
    }

    Check(outputs[0], host_output, cur);
  }
}

TEST_F(calcite_interpreter_TEST, where_not_equals) {

  { // select x from hr.emps where x <> 1
    std::string query = "\
LogicalProject(x=[$0])\n\
  LogicalFilter(condition=[<>($0, 1)])\n\
    LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    int cur = 0;
    int32_t *host_output = new int32_t[num_values];
    for (std::size_t i = 0; i < num_values; i++) {
      if (input1[i] != 1) {
        host_output[cur] = input1[i];
        cur++;
      }
    }

    Check(outputs[0], host_output, cur);
  }
}

// Output with several columns (all of them)
TEST_F(calcite_interpreter_TEST, where_all_columns_withand) {

  { // select * from hr.emps where x = z AND 4 >= y
    std::string query = "\
LogicalProject(x=[$0], y=[$1], z=[$2])\n\
  LogicalFilter(condition=[AND(=($0, $2), >=(4, $1))])\n\
	LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 3);

    int cur = 0;
    int32_t *host_output1 = new int32_t[num_values];
    int32_t *host_output2 = new int32_t[num_values];
    int32_t *host_output3 = new int32_t[num_values];

    for (std::size_t i = 0; i < num_values; i++) {
      if (input1[i] == input3[i] && 4 >= input2[i]) {
        host_output1[cur] = input1[i];
        host_output2[cur] = input2[i];
        host_output3[cur] = input3[i];
        cur++;
      }
    }

    Check(outputs[0], host_output1, cur);
    Check(outputs[1], host_output2, cur);
    Check(outputs[2], host_output3, cur);
  }
}

// Output with several columns (two of them)
TEST_F(calcite_interpreter_TEST, where_two_columns_withand) {

  { // select x,z from hr.emps where x = 1 AND 4 >= y
    std::string query = "\
LogicalProject(x=[$0], z=[$2])\n\
  LogicalFilter(condition=[AND(=(1, $0), >=(4, $1))])\n\
	LogicalTableScan(table=[[hr, emps]])";

    gdf_error err =
        evaluate_query(input_tables, table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 2);

    int cur = 0;
    int32_t *host_output1 = new int32_t[num_values];
    int32_t *host_output2 = new int32_t[num_values];

    for (std::size_t i = 0; i < num_values; i++) {
      if (input1[i] == 1 && 4 >= input2[i]) {
        host_output1[cur] = input1[i];
        host_output2[cur] = input3[i];
        cur++;
      }
    }

    Check(outputs[0], host_output1, cur);
    Check(outputs[1], host_output2, cur);
  }
}
 