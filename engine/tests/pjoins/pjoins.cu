#include <cstdlib>
#include <iostream>
#include <vector>
#include <map>
#include <type_traits>
#include <memory>

#include <CalciteInterpreter.h>
#include <GDFColumn.cuh>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "../utils/gdf/library/table.h"
#include "../utils/gdf/library/table_group.h"

// Selects the kind of join operation that is performed
enum struct join_op
{
  INNER,
  LEFT,
  OUTER
};

template<join_op join_operation, 
         typename tuple_of_vectors,
         bool keys_are_unique = false>
struct TestParameters
{
  // The method to use for the join
  const static join_op op{join_operation};

  // The tuple of vectors that determines the number and types of the columns to join
  using multi_column_t = tuple_of_vectors;

  const static bool unique_keys{keys_are_unique};
};



template <class TypeParam>
struct JoinTest : public ::testing::Test {
  void SetUp() {
	  rmmInitialize(nullptr);
  }
};

typedef ::testing::Types<
                          TestParameters< join_op::INNER,  gdf::library::TableRowBuilder<int8_t, double, int32_t, int64_t> >,
                          TestParameters< join_op::LEFT,   gdf::library::TableRowBuilder<int8_t, double, int32_t, int64_t> >
                      > Implementations;

TYPED_TEST_CASE(JoinTest, Implementations);

TYPED_TEST(JoinTest, SimpleTest)
{
  std::cout << TypeParam::unique_keys << std::endl;  
 
  using VTableBuilder = typename TypeParam::multi_column_t;
  using DataTuple = typename VTableBuilder::DataTuple;
  /*
   gdf::library::Table table = 
       VTableBuilder {
         .name = "emps",
         .headers = {"Id", "Weight", "Age", "Name"},
         .rows = {
           DataTuple{'a', 180.2, 40, 100L},
           DataTuple{'b', 175.3, 38, 200L},
           DataTuple{'c', 140.3, 27, 300L},
         },
       }
       .Build();

   table.print(std::cout);

   std::string query = "
   LogicalProject(x=[$0], y=[$1], z=[$2], join_x=[$3], y0=[$4], EXPR$5=[+($0, $4)])\n
     LogicalJoin(condition=[=($3, $0)], joinType=[inner])\n
       LogicalTableScan(table=[[hr, emps]])\n
       LogicalTableScan(table=[[hr, joiner]])";

	 	gdf_error err = evaluate_query(this->input_tables, this->table_names, this->column_names,
	 			query, this->outputs);

   std::vector<result_type> reference_result = this->compute_reference_solution(true);
   std::vector<result_type> gdf_result = this->compute_gdf_result(true);

   ASSERT_EQ(reference_result.size(), gdf_result.size()) << "Size of gdf result does not match reference result\n";

   // Compare the GDF and reference solutions
   for(size_t i = 0; i < reference_result.size(); ++i){
     EXPECT_EQ(reference_result[i], gdf_result[i]);
   }
  */
}
