//#include <gmock/gmock.h>
//#include <gtest/gtest.h>

#include "tests/utilities/column_wrapper.hpp"
#include "tests/utilities/type_lists.hpp"	 // cudf::test::NumericTypes


#include "tests/utilities/BlazingUnitTest.h"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/graph.h"
#include "execution_graph/logic_controllers/taskflow/port.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"

using blazingdb::transport::Node;
using ral::cache::kstatus;

template <typename T>
struct ProjectionTest : public BlazingUnitTest {};


// Create edge cases as empty data

// Additional test cases where the inputs into the CacheMachine are added with a time delay

// Make utilities that will be reusable (for future other kernel tests)

TYPED_TEST_CASE(ProjectionTest, cudf::test::NumericTypes);


//TEST_F
TYPED_TEST(ProjectionTest, MultipleColumnsMultipleRows) {
	
	using T = TypeParam;

	// Data
    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);

	// Context
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options);


	// Projection kernel
	std::size_t kernel_id = 1;
	std::string projectPlan = "LogicalProject(A=[$0])"; 	// LogicalProject(n_nationkey=[$0], n_name=[$1])  
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<ral::cache::kernel> project_kernel = std::make_shared<ral::batch::Projection>(kernel_id, projectPlan, context, graph);


	// CacheMachines
	std::shared_ptr<ral::cache::CacheMachine>  inputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);
	std::shared_ptr<ral::cache::CacheMachine> outputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);

	project_kernel->input_.register_cache("1", inputCacheMachine);
	project_kernel->output_.register_cache("1", outputCacheMachine);

	inputCacheMachine->addToCache(std::move(table));
	inputCacheMachine->finish();

	// the main function
	kstatus process = project_kernel->run();

	std::unique_ptr<ral::frame::BlazingTable> pulled_table = outputCacheMachine->pullFromCache();

	ASSERT_EQ(pulled_table->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, MultipleColumnSingleRow) {
	
	using T = TypeParam;

	// Data
    cudf::test::fixed_width_column_wrapper<T> col1{{4}, {1}};
    cudf::test::strings_column_wrapper col2({"b"}, {1});

    CudfTableView cudf_table_in_view {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B"});
    std::unique_ptr<ral::frame::BlazingTable> table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table), names);


	// Context
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options);


	// Projection kernel
	std::size_t kernel_id = 1;
	std::string projectPlan = "LogicalProject(A=[$0])"; 	// LogicalProject(n_nationkey=[$0], n_name=[$1])  
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<ral::cache::kernel> project_kernel = std::make_shared<ral::batch::Projection>(kernel_id, projectPlan, context, graph);


	// CacheMachines
	std::shared_ptr<ral::cache::CacheMachine>  inputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);
	std::shared_ptr<ral::cache::CacheMachine> outputCacheMachine = std::make_shared<ral::cache::CacheMachine>(context);

	project_kernel->input_.register_cache("1", inputCacheMachine);
	project_kernel->output_.register_cache("1", outputCacheMachine);

	inputCacheMachine->addToCache(std::move(table));
	inputCacheMachine->finish();

	// the main function
	kstatus process = project_kernel->run();

	std::unique_ptr<ral::frame::BlazingTable> pulled_table = outputCacheMachine->pullFromCache();

	EXPECT_EQ(pulled_table->num_rows(), 1);
	ASSERT_EQ(pulled_table->num_columns(), 1);
}

/*
TEST_F(ProjectionTest, MultipleColumnsSingleRow) {


	ASSERT_EQ(1, 1);
}
*/