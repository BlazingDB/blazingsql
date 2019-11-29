
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include "Interpreter/interpreter_cpp.h"
#include "Interpreter/interpreter_ops.cuh"

#include <CalciteExpressionParsing.h>
#include <CalciteInterpreter.h>
#include <DataFrame.h>
#include <blazingdb/io/Util/StringUtil.h>
#include <gtest/gtest.h>
#include <GDFColumn.cuh>
#include <GDFCounter.cuh>
//#include <Utils.cuh>

#include "gdf/library/scalar.h"
#include "gdf/library/table.h"
#include "gdf/library/table_group.h"
#include "gdf/library/types.h"
#include "gdf/library/api.h"
#include "cudf/binaryop.hpp"
using namespace gdf::library;

struct EvaluateQueryTest : public ::testing::Test
{
    struct InputTestItem
    {
        std::string query;
        std::string logicalPlan;
        gdf::library::TableGroup tableGroup;
        gdf::library::Table resultTable;
    };

  void SetUp() {
    rmmInitialize(nullptr);
  }


    void CHECK_RESULT(gdf::library::Table &computed_solution,
                      gdf::library::Table &reference_solution)
    {

        computed_solution.print(std::cout);
        reference_solution.print(std::cout);

        for (size_t index = 0; index < reference_solution.size(); index++)
        {
            const auto &reference_column = reference_solution[index];
            const auto &computed_column = computed_solution[index];
            auto a = reference_column.to_string();
            auto b = computed_column.to_string();
            EXPECT_EQ(a, b);
        }
    }
};

// select ($1 + $0) * $2 + $1, $1 + 2  from table
//  + * + $0 $1 $2 $1 , + $1 2

//step 0:   + * + $0 $1 $2 $1 , + $1 2
//step 0:                expr1, expr2

//step 1:  + * $5 $2 $1 , + $1 $2

//step 2:  + $5 $1 , + $1 $2

// Registers are
// 	0		        1     			2		    3   		  4           5 		    6			  n + 3 + 2
// input_col_1, input_col_2, input_col_3, output_col_1, output_col2, processing_1, processing_2 .... processing_n

TEST_F(EvaluateQueryTest, TEST_00)
{

    Table input_table = TableBuilder{
        "emps",
        {
            {"x", [](Index i) -> DType<GDF_INT32> { return i * 2.0; }},
            {"y", [](Index i) -> DType<GDF_INT32> { return i * 10.0; }},
            {"z", [](Index i) -> DType<GDF_INT32> { return i * 20.0; }},
        }}
                            .Build(10);
    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_INT32> { return 0; }},
            {"o2", [](Index i) -> DType<GDF_INT32> { return 0; }},
        }}
                             .Build(10);

    input_table.print(std::cout); 

    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(2);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    // //  + * + $0 $1 $2 $1 , + $1 2
    // //      + $0 $1
    // //  + * $5      $2 $1 , + $1 2
    // //   * $5 $2 
    // //  + $5 $1 , + $1 2
    // //  + $3 , + $1 2
    

    std::vector<column_index_type> left_inputs =  {0, 5, 5,      1};
    std::vector<column_index_type> right_inputs = {1, 2, 1,     -2};
    std::vector<column_index_type> outputs =      {5, 5, 3,      4};

    std::vector<column_index_type> final_output_positions = {3, 4};

    std::vector<gdf_binary_operator_exp> operators = {BLZ_ADD, BLZ_MUL, BLZ_ADD, BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = {BLZ_INVALID_UNARY, BLZ_INVALID_UNARY, BLZ_INVALID_UNARY, BLZ_INVALID_UNARY};

    using I32 = gdf::library::GdfEnumType<GDF_INT32>;

    gdf::library::Scalar<I32> junk_obj;
    junk_obj.setValue(0).setValid(true);
    gdf::library::Scalar<I32> vscalar_obj;
    vscalar_obj.setValue(2).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk, junk, junk, junk};
    std::vector<gdf_scalar> right_scalars = {junk, junk, junk, scalar_val};

    std::vector<column_index_type> new_input_indices = {0, 1, 2};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<int32_t, int32_t>;
    using DataTuple = VTableBuilder::DataTuple;
    auto reference_table = VTableBuilder{
        "output",
        { "a", "b"},
        {
            DataTuple{0,2}, 
            DataTuple{250,	12}, 
            DataTuple{980,	22}, 
            DataTuple{2190,	32}, 
            DataTuple{3880,	42}, 
            DataTuple{6050,	52}, 
            DataTuple{8700,	62}, 
            DataTuple{11830,72}, 
            DataTuple{15440,82}, 
            DataTuple{19530,92},
        }
    }.Build();
    ral_solution_table.print(std::cout);
    EXPECT_EQ(ral_solution_table, reference_table);
}



// select $2 * $0, $0 +  $1   from customer
TEST_F(EvaluateQueryTest, TEST_FLOATS)
{

   auto input_table = gdf::library::LiteralTableBuilder{"customer",
    {
      gdf::library::LiteralColumnBuilder{
        "x",
        Literals<GDF_FLOAT32>{ 1.1, 3.6, 5.3, 7.9, 9.35 },
      },
      gdf::library::LiteralColumnBuilder{
        "y",
        Literals<GDF_FLOAT32>{ 1, 1, 1, 1, 1 },
      }, 
       gdf::library::LiteralColumnBuilder{
        "z",
        Literals<GDF_FLOAT32>{ 0, 2, 4, 6, 8 },
      }, 
    } }.Build();

    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_FLOAT32> { return 0.0; }},
            {"o2", [](Index i) -> DType<GDF_FLOAT32> { return 0.0; }},
        }}.Build(5);

    input_table.print(std::cout); 

    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(2);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    // select $2 * $0, $0 +  $1   from customer
    std::vector<column_index_type> left_inputs =  {2, 0};
    std::vector<column_index_type> right_inputs = {0, 1};
    std::vector<column_index_type> outputs =      {3, 4};

    std::vector<column_index_type> final_output_positions = {3, 4};

    std::vector<gdf_binary_operator_exp> operators = std::initializer_list<gdf_binary_operator_exp>{BLZ_MUL, BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = std::initializer_list<gdf_unary_operator>{BLZ_INVALID_UNARY, BLZ_INVALID_UNARY};

    using I32 = gdf::library::GdfEnumType<GDF_INT32>;

    gdf::library::Scalar<I32> junk_obj;
    junk_obj.setValue(0).setValid(true);
    gdf::library::Scalar<I32> vscalar_obj;
    vscalar_obj.setValue(1).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk, junk};
    std::vector<gdf_scalar> right_scalars = {junk, junk};

    std::vector<column_index_type> new_input_indices = {0, 1, 2};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<float, float>;
    using DataTuple = VTableBuilder::DataTuple;
    auto reference_table = VTableBuilder{
        "output",
        { "a", "b"},
        {
            DataTuple{0, 2.1}, 
            DataTuple{7.2, 4.6}, 
            DataTuple{21.2, 6.3}, 
            DataTuple{47.4, 8.9}, 
            DataTuple{74.8, 10.35},  
        }
    }.Build();
    ral_solution_table.print(std::cout);
    EXPECT_EQ(ral_solution_table, reference_table);
}


// select $0 + 1 from customer
TEST_F(EvaluateQueryTest, TEST_SCALAR_ADD)
{

   auto input_table = gdf::library::LiteralTableBuilder{"customer",
    {
      gdf::library::LiteralColumnBuilder{
        "x",
        Literals<GDF_INT32>{ 1, 3, 5, 7, 9 },
      },
      gdf::library::LiteralColumnBuilder{
        "y",
        Literals<GDF_INT32>{ 1, 1, 1, 1, 1 },
      }, 
       gdf::library::LiteralColumnBuilder{
        "z",
        Literals<GDF_INT32>{ 0, 2, 4, 6, 8 },
      }, 
    } }.Build();

    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_INT32> { return 0; }},
        }}.Build(5);

    input_table.print(std::cout); 

    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(1);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    //output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    // select $0 + 1 from customer
    std::vector<column_index_type> left_inputs =  {0};
    std::vector<column_index_type> right_inputs = {-2};
    std::vector<column_index_type> outputs =      {3};

    std::vector<column_index_type> final_output_positions = {3};

    std::vector<gdf_binary_operator_exp> operators = std::initializer_list<gdf_binary_operator_exp>{BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = std::initializer_list<gdf_unary_operator>{BLZ_INVALID_UNARY};

    using I32 = gdf::library::GdfEnumType<GDF_INT32>;

    gdf::library::Scalar<I32> junk_obj;
    junk_obj.setValue(0).setValid(true);
    gdf::library::Scalar<I32> vscalar_obj;
    vscalar_obj.setValue(1).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk};
    std::vector<gdf_scalar> right_scalars = {scalar_val};

    std::vector<column_index_type> new_input_indices = {0, -1, -1};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<int>;
    using DataTuple = VTableBuilder::DataTuple;
    auto reference_table = VTableBuilder{
        "output",
        { "a"},
        {
            DataTuple{2}, 
            DataTuple{4}, 
            DataTuple{6}, 
            DataTuple{8}, 
            DataTuple{10},  
        }
    }.Build();
    ral_solution_table.print(std::cout);
    EXPECT_EQ(ral_solution_table, reference_table);
}


// select floor($0), sin($1)  from customer
TEST_F(EvaluateQueryTest, TEST_01_Unary_Operators)
{

   auto input_table = gdf::library::LiteralTableBuilder{"customer",
    {
      gdf::library::LiteralColumnBuilder{
        "x",
        Literals<GDF_FLOAT32>{ 1.1, 3.6, 5.3, 7.9, 9.35 },
      },
      gdf::library::LiteralColumnBuilder{
        "y",
        Literals<GDF_FLOAT32>{ 0, 2, 4, 6, 8 },
      }, 
       gdf::library::LiteralColumnBuilder{
        "z",
        Literals<GDF_FLOAT32>{ 0, 2, 4, 6, 8 },
      }, 
    } }.Build();

    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_FLOAT32> { return 0.0; }},
            {"o2", [](Index i) -> DType<GDF_FLOAT32> { return 0.0; }},
        }}.Build(5);

    input_table.print(std::cout); 

    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(2);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    // select floor($0), sin($1)   from customer
    std::vector<column_index_type> left_inputs =  { 0,  1};
    std::vector<column_index_type> right_inputs = {-1, -1};
    std::vector<column_index_type> outputs =      { 3,  4};

    std::vector<column_index_type> final_output_positions = {3, 4};

    std::vector<gdf_binary_operator_exp> operators = {BLZ_INVALID_BINARY, BLZ_INVALID_BINARY};
    std::vector<gdf_unary_operator> unary_operators = {BLZ_FLOOR, BLZ_SIN};

    using I32 = gdf::library::GdfEnumType<GDF_INT32>;

    gdf::library::Scalar<I32> junk_obj;
    junk_obj.setValue(0).setValid(true);
    gdf::library::Scalar<I32> vscalar_obj;
    vscalar_obj.setValue(2).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk, junk};
    std::vector<gdf_scalar> right_scalars = {junk, junk};

    std::vector<column_index_type> new_input_indices = {0, 1, 2};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<float, float>;
    using DataTuple = VTableBuilder::DataTuple;
    auto reference_table = VTableBuilder{
        "output",
        { "a", "b"},
        {
            DataTuple{1,     0}, 
            DataTuple{3,	 0.9092974268}, 
            DataTuple{5,	-0.7568024953}, 
            DataTuple{7,	-0.2794154982}, 
            DataTuple{9,	 0.9893582466},  
        }
    }.Build();
    ral_solution_table.print(std::cout);
    EXPECT_EQ(ral_solution_table, reference_table);
}


// select ($1 + $0) * $2 + $1, sin($1) + 2.33 from table
//  + * + $0 $1 $2 $1 , + sin $1 2.33

//step 0:   + * + $0 $1 $2 $1 , + sin $1 2.33
//step 0:                expr1, expr2

//step 1:  + * $5 $2 $1 , + $1 $2

//step 2:  + $5 $1 , + $1 $2

// Registers are
// 	0		       1     		2		      3     	  4			      5	    		 6 		        n + 3 + 2
// input_col_1, input_col_2, input_col_3, output_col_1, output_col2, processing_1, processing_2 .... processing_n

TEST_F(EvaluateQueryTest, TEST_02)
{

    Table input_table = TableBuilder{
        "emps",
        {
            {"x", [](Index i) -> DType<GDF_FLOAT32> { return (i + 1) * 2.0; }},
            {"y", [](Index i) -> DType<GDF_FLOAT32> { return (i + 1) * 10.0; }},
            {"z", [](Index i) -> DType<GDF_INT32> { return (i + 1) * 20.0; }},
        }}
                            .Build(10);
    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_FLOAT32> { return 0; }},
            {"o2", [](Index i) -> DType<GDF_FLOAT32> { return 0; }},
        }}
                             .Build(10);

    input_table.print(std::cout); 
    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(2);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    std::vector<column_index_type> left_inputs =  {0, 5, 5,      1, 5};
    std::vector<column_index_type> right_inputs = {1, 2, 1,     -1, -2};
    std::vector<column_index_type> outputs =      {5, 5, 3,      5, 4};

    std::vector<column_index_type> final_output_positions = {3, 4};
    // + sin $1 2.33
    std::vector<gdf_binary_operator_exp> operators = {BLZ_ADD, BLZ_MUL, BLZ_ADD, BLZ_INVALID_BINARY, BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = { BLZ_INVALID_UNARY,BLZ_INVALID_UNARY,BLZ_INVALID_UNARY,BLZ_SIN,BLZ_INVALID_UNARY };

    using FP32 = gdf::library::GdfEnumType<GDF_FLOAT32>;

    gdf::library::Scalar<FP32> junk_obj;
    junk_obj.setValue(0.0).setValid(true);
    gdf::library::Scalar<FP32> vscalar_obj;
    vscalar_obj.setValue(2.33).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk, junk, junk, junk, junk};
    std::vector<gdf_scalar> right_scalars = {junk, junk, junk, junk, scalar_val};

    std::vector<column_index_type> new_input_indices = {0, 1, 2};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<float, float>;
    using DataTuple = VTableBuilder::DataTuple;

    auto reference_table = VTableBuilder{
        "output",
        { "a", "b"},
        {
            DataTuple{250.0,	1.78598}, 
            DataTuple{980.0,	3.24295}, 
            DataTuple{2190.0,	1.34197}, 
            DataTuple{3880.0,	3.07511}, 
            DataTuple{6050.0,	2.06763}, 
            DataTuple{8700.0,	2.02519}, 
            DataTuple{11830.0,  3.10389}, 
            DataTuple{15440.0,  1.33611}, 
            DataTuple{19530.0,  3.224},
            DataTuple{24100.0,  1.82363},
        }
    }.Build();
    ral_solution_table.print(std::cout);
    CHECK_RESULT(ral_solution_table, reference_table);
}

// agregation without groupby
// select count($0) + sum($0), (max($0) + min($0))/2 from table /// ...  where ....
//  { + count($0) sum($0) }, { / + max($0) min($0) 2  }
/*TEST_F(EvaluateQueryTest, TEST_04_aggregation_without_groupby)
{

    Table input_table = TableBuilder{
        "emps",
        {
            {"x", [](Index i) -> DType<GDF_INT32> { return i * 2.0; }},
            {"y", [](Index i) -> DType<GDF_INT32> { return i * 10.0; }},
            {"z", [](Index i) -> DType<GDF_INT32> { return i * 20.0; }},
        }}
                            .Build(10);
    Table output_table = TableBuilder{
        "emps",
        {
            {"o1", [](Index i) -> DType<GDF_INT32> { return 0; }},
            {"o2", [](Index i) -> DType<GDF_INT32> { return 0; }},
        }}
                             .Build(10);

    input_table.print(std::cout); 

    std::vector<gdf_column_cpp> input_columns_cpp = input_table.ToGdfColumnCpps();
    std::vector<gdf_column_cpp> output_columns_cpp = output_table.ToGdfColumnCpps();

    std::vector<gdf_column *> output_columns(2);
    output_columns[0] = output_columns_cpp[0].get_gdf_column();
    output_columns[1] = output_columns_cpp[1].get_gdf_column();

    std::vector<gdf_column *> input_columns(3);
    input_columns[0] = input_columns_cpp[0].get_gdf_column();
    input_columns[1] = input_columns_cpp[1].get_gdf_column();
    input_columns[2] = input_columns_cpp[2].get_gdf_column();

    //  { + count($0) sum($0) }, { / + max($0) min($0) 2  }
    std::vector<column_index_type> left_inputs =  { 0, 0, 5};
    std::vector<column_index_type> right_inputs = {-1,-1,-1};
    std::vector<column_index_type> outputs =      { 5, 6, 3};

    std::vector<column_index_type> final_output_positions = {3, 4};

    std::vector<gdf_binary_operator_exp> operators = {GDF_INVALID_BINARY, GDF_INVALID_BINARY, BLZ_ADD};
    std::vector<gdf_unary_operator> unary_operators = {GDF_SUM, GDF_COUNT, BLZ_INVALID_UNARY};

    using I32 = gdf::library::GdfEnumType<GDF_INT32>;

    gdf::library::Scalar<I32> junk_obj;
    junk_obj.setValue(0).setValid(true);
    gdf::library::Scalar<I32> vscalar_obj;
    vscalar_obj.setValue(2).setValid(true);

    gdf_scalar junk = *junk_obj.scalar();
    gdf_scalar scalar_val = *vscalar_obj.scalar();

    std::vector<gdf_scalar> left_scalars = {junk, junk, junk};
    std::vector<gdf_scalar> right_scalars = {junk, junk, junk};

    std::vector<column_index_type> new_input_indices = {0, 1, 2};

    perform_operation(output_columns, input_columns, left_inputs, right_inputs, outputs, final_output_positions, operators, unary_operators, left_scalars, right_scalars, new_input_indices);
    
    auto ral_solution_table = GdfColumnCppsTableBuilder{ "output", output_columns_cpp }.Build();
    using VTableBuilder = gdf::library::TableRowBuilder<int32_t, int32_t>;
    using DataTuple = VTableBuilder::DataTuple;
    auto reference_table = VTableBuilder{
        "output",
        { "a", "b"},
        {
            DataTuple{0,2}, 
            DataTuple{250,	12}, 
            DataTuple{980,	22}, 
            DataTuple{2190,	32}, 
            DataTuple{3880,	42}, 
            DataTuple{6050,	52}, 
            DataTuple{8700,	62}, 
            DataTuple{11830,72}, 
            DataTuple{15440,82}, 
            DataTuple{19530,92},
        }
    }.Build();
    ral_solution_table.print(std::cout);
    EXPECT_EQ(ral_solution_table, reference_table);
}*/
