#include <chrono>
#include <thread>

#include "tests/utilities/column_wrapper.hpp"
#include "tests/utilities/type_lists.hpp"	 // cudf::test::NumericTypes

#include "tests/utilities/BlazingUnitTest.h"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/graph.h"
#include "execution_graph/logic_controllers/taskflow/port.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"

using blazingdb::transport::Node;
using ral::cache::kstatus;
using ral::cache::CacheMachine;
using ral::frame::BlazingTable;
using ral::cache::kernel;

/**
 * Unit Tests for Projection Kernel
 * pipeline:
 * 1. InputCacheMachine hold one (or multi) BlazingTable(s).
 * 2. ProjectKernel call the run() method that Projects some columns according to the plan.
 *    OBS: Number of rows should remain the same after call ProjectKernel.
 * 3. The ProjectKernel ends when all the required columns are inside the OutputCacheMachine.
 *                               ________________________________
 *                              |                                |
 *                              | ProjectKernel                  |
 *                         ---> | plan:                          |--->
 *                        /	    |  LogicalProject(A=[$0],C=[$1]) |    \
 *                       /	    |________________________________|     \
 *    _________________ /                                               \_______________
 *   |   ___________   |                                                |    _______    |
 *   |  | A | B | C |  |                                                |   | A | C |   |
 *   |  |   |   |   |  |                                                |   |   |   |   |
 *   |  |   |   |   |  |                                                |   |   |   |   |
 *   |  |___|___|___|  |                                                |   |___|___|   |
 *   |_________________|                                                |_______________|
 * 
 *    InputCacheMachine                                                 OutputCacheMachine
 * 
 */
template <typename T>
struct ProjectionTest : public BlazingUnitTest {};


// Just creates a Context
std::shared_ptr<Context> make_context() {
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options);

	return context;
}

// Creates a ProjectKernel using a valid `project_plan`
std::shared_ptr<kernel> make_project_kernel(std::string project_plan, std::shared_ptr<Context> context) {
	std::size_t kernel_id = 1;
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<kernel> project_kernel = std::make_shared<ral::batch::Projection>(kernel_id, project_plan, context, graph);

	return project_kernel;
}

// Creates two CacheMachines and register them with the `project_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>> register_kernel_with_cache_machines(
	std::shared_ptr<kernel> project_kernel,
	std::shared_ptr<Context> context) {
	std::shared_ptr<CacheMachine>  inputCacheMachine = std::make_shared<CacheMachine>(context);
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context);
	project_kernel->input_.register_cache("1", inputCacheMachine);
	project_kernel->output_.register_cache("1", outputCacheMachine);

	return std::make_tuple(inputCacheMachine, outputCacheMachine);
}

// Feeds an input cache with time delays
void add_data_to_cache_with_delay(
	std::shared_ptr<CacheMachine> cache_machine,
	std::vector<std::unique_ptr<BlazingTable>> batches,
	std::vector<int> delays_in_ms)
{
	int total_batches = batches.size();
	int total_delays = delays_in_ms.size();

	EXPECT_EQ(total_delays, total_batches);

	for (int i = 0; i < total_batches; ++i) {
		std::this_thread::sleep_for(std::chrono::milliseconds(delays_in_ms[i]));
		cache_machine->addToCache(std::move(batches[i]));
	}

	// default last delay
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	cache_machine->finish();
}


TYPED_TEST_CASE(ProjectionTest, cudf::test::NumericTypes);

TYPED_TEST(ProjectionTest, MultipleColumnsMultipleRowsOneBatchWithoutDelays) {
	
	using T = TypeParam;

	// Data
    cudf::test::fixed_width_column_wrapper<T> col1{{4, 5, 3, 5, 8, 5, 6}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::unique_ptr<BlazingTable> pulled_table = outputCacheMachine->pullFromCache();
	EXPECT_EQ(pulled_table->num_rows(), 7);
	ASSERT_EQ(pulled_table->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, MultipleColumnsSingleRowOneBatchWithoutDelays) {
	
	using T = TypeParam;

	// Data
    cudf::test::fixed_width_column_wrapper<T> col1{{4}, {1}};
    cudf::test::strings_column_wrapper col2({"b"}, {1});

    CudfTableView cudf_table_in_view {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B"});
    std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(B=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::unique_ptr<BlazingTable> pulled_table = outputCacheMachine->pullFromCache();
	EXPECT_EQ(pulled_table->num_rows(), 1);
	ASSERT_EQ(pulled_table->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, MultipleColumnsNoRowsOneBatchWithoutDelays) {

	using T = TypeParam;

	// Empty Data
	std::vector<std::string> names{"A", "B", "C"};
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32}, 
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::FLOAT64} };
	std::unique_ptr<BlazingTable> empty_batch = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(B=[$0], C=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(empty_batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::unique_ptr<BlazingTable> pulled_table = outputCacheMachine->pullFromCache();
	EXPECT_EQ(pulled_table->num_rows(), 0);
	ASSERT_EQ(pulled_table->num_columns(), 2);
}


TYPED_TEST(ProjectionTest, MultipleColumnsMultipleRowsTwoBatchsWithoutDelays) {

	using T = TypeParam;

	// Batch 1
    cudf::test::fixed_width_column_wrapper<T> col1_a{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2_a({"b", "d", "a", "d"}, {1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_a{{10, 40, 70, 5}, {1, 1, 1, 1}};

    CudfTableView cudf_table_in_view_a {{col1_a, col2_a, col3_a}};
    std::unique_ptr<CudfTable> cudf_table_a = std::make_unique<CudfTable>(cudf_table_in_view_a);

    std::vector<std::string> names_a({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch_1 = std::make_unique<BlazingTable>(std::move(cudf_table_a), names_a);

	// Batch 2
    cudf::test::fixed_width_column_wrapper<T> col1_b{{28, 5, 6}, {1, 1, 1}};
    cudf::test::strings_column_wrapper col2_b({"l", "d", "k"}, {1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_b{{2, 10, 11}, {1, 1, 1}};

    CudfTableView cudf_table_in_view_b {{col1_b, col2_b, col3_b}};
    std::unique_ptr<CudfTable> cudf_table_b = std::make_unique<CudfTable>(cudf_table_in_view_b);

    std::vector<std::string> names_b({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch_2 = std::make_unique<BlazingTable>(std::move(cudf_table_b), names_b);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0], B=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delays
	std::vector<int> delays_in_ms {0, 0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_1));
	batches.push_back(std::move(batch_2));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::vector<std::unique_ptr<BlazingTable>> pulled_batches;
	pulled_batches.push_back(outputCacheMachine->pullFromCache());
	pulled_batches.push_back(outputCacheMachine->pullFromCache());
	
	EXPECT_EQ(pulled_batches.size(), 2);
	ASSERT_EQ(pulled_batches[0]->num_columns(), 2);
	ASSERT_EQ(pulled_batches[1]->num_columns(), 2);
}


TYPED_TEST(ProjectionTest, MultipleColumnsMultipleRowsOneBatchWithDelays) {
	
	using T = TypeParam;

	// Data
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"B", "C"});
    std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(C=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine with just one delay
	std::vector<int> delays_in_ms {100};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);
	
	// Assert output
	std::unique_ptr<BlazingTable> pulled_table = outputCacheMachine->pullFromCache();
	EXPECT_EQ(pulled_table->num_rows(), 7);
	ASSERT_EQ(pulled_table->num_columns(), 1);
}


/*
TYPED_TEST(ProjectionTest, MultipleColumnsNoRowsOneBatchWithDelays) {

	// Empty Data
	std::vector<std::string> names{"A", "B", "C"};
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32}, 
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::FLOAT64} };
	std::unique_ptr<BlazingTable> empty_batch = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0], C=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add empty data to the inputCacheMachine with delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(empty_batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::unique_ptr<BlazingTable> pulled_table = outputCacheMachine->pullFromCache();
	EXPECT_EQ(pulled_table->num_rows(), 0);
	ASSERT_EQ(pulled_table->num_columns(), 2);
}
*/

TYPED_TEST(ProjectionTest, MultipleColumnsMultipleRowsTwoBatchsWithDelays) {

	using T = TypeParam;

	// Batch 1
    cudf::test::fixed_width_column_wrapper<T> col1_a{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2_a({"b", "d", "a", "d"}, {1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_a{{10, 40, 70, 5}, {1, 1, 1, 1}};

    CudfTableView cudf_table_in_view_a {{col1_a, col2_a, col3_a}};
    std::unique_ptr<CudfTable> cudf_table_a = std::make_unique<CudfTable>(cudf_table_in_view_a);

    std::vector<std::string> names_a({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch_1 = std::make_unique<BlazingTable>(std::move(cudf_table_a), names_a);

	// Batch 2
    cudf::test::fixed_width_column_wrapper<T> col1_b{{28, 5, 6}, {1, 1, 1}};
    cudf::test::strings_column_wrapper col2_b({"l", "d", "k"}, {1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_b{{2, 10, 11}, {1, 1, 1}};

    CudfTableView cudf_table_in_view_b {{col1_b, col2_b, col3_b}};
    std::unique_ptr<CudfTable> cudf_table_b = std::make_unique<CudfTable>(cudf_table_in_view_b);

    std::vector<std::string> names_b({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch_2 = std::make_unique<BlazingTable>(std::move(cudf_table_b), names_b);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delays
	std::vector<int> delays_in_ms {30, 60};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_1));
	batches.push_back(std::move(batch_2));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function
	kstatus process = project_kernel->run();
	EXPECT_EQ(kstatus::proceed, process);

	// Assert output
	std::vector<std::unique_ptr<BlazingTable>> pulled_batches;
	pulled_batches.push_back(outputCacheMachine->pullFromCache());
	pulled_batches.push_back(outputCacheMachine->pullFromCache());
	
	EXPECT_EQ(pulled_batches.size(), 2);
	ASSERT_EQ(pulled_batches[0]->num_columns(), 1);
	ASSERT_EQ(pulled_batches[1]->num_columns(), 1);
}
