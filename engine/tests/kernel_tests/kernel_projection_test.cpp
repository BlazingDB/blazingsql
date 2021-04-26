#include <spdlog/spdlog.h>
#include "tests/utilities/BlazingUnitTest.h"

#include <chrono>
#include <thread>
//#include <gtest/gtest.h>

#include "cudf_test/column_wrapper.hpp"
#include "cudf_test/type_lists.hpp"	 // cudf::test::NumericTypes

#include "execution_graph/Context.h"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/graph.h"
#include "execution_graph/logic_controllers/taskflow/port.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"
#include "execution_graph/logic_controllers/taskflow/executor.h"

#include "parser/expression_utils.hpp"

using blazingdb::transport::Node;
using ral::cache::kstatus;
using ral::cache::CacheMachine;
using ral::frame::BlazingTable;
using ral::cache::kernel;
using Context = blazingdb::manager::Context;

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
 *                         ---> | plan:                          | --->
 *                        /     |  LogicalProject(A=[$0],C=[$1]) |     \
 *                       /      |________________________________|      \
 *    _________________ /                                                \_______________
 *   |   ___________   |                                                 |    _______    |
 *   |  | A | B | C |  |                                                 |   | A | C |   |
 *   |  |   |   |   |  |                                                 |   |   |   |   |
 *   |  |   |   |   |  |                                                 |   |   |   |   |
 *   |  |___|___|___|  |                                                 |   |___|___|   |
 *   |_________________|                                                 |_______________|
 *
 *    InputCacheMachine                                                 OutputCacheMachine
 *
 */
template <typename T>
struct ProjectionTest : public ::testing::Test {
	virtual void SetUp() override {
		BlazingRMMInitialize("pool_memory_resource", 32*1024*1024, 256*1024*1024);
		float host_memory_quota=0.75; //default value
		blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
		ral::memory::set_allocation_pools(4000000, 10,
										4000000, 10, false,nullptr);
		int executor_threads = 10;
		ral::execution::executor::init_executor(executor_threads, 0.8);
	}

	virtual void TearDown() override {
		ral::memory::empty_pools();
		BlazingRMMFinalize();
	}
};


// Just creates a Context
std::shared_ptr<Context> make_context() {
	std::vector<Node> nodes;
	Node master_node;
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::string current_timestamp;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options, current_timestamp);

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
	std::shared_ptr<CacheMachine>  inputCacheMachine = std::make_shared<CacheMachine>(context, "");
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context, "");
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
	std::this_thread::sleep_for(std::chrono::milliseconds(50));

	cache_machine->finish();
}


TYPED_TEST_CASE(ProjectionTest, cudf::test::NumericTypes);

TYPED_TEST(ProjectionTest, OneBatchFullWithoutDelay) {

	using T = TypeParam;

	// Batch
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

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 7);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, OneBatchOneRowWithoutDelay) {

	using T = TypeParam;

	// Batch
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

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 1);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, OneBatchEmptyWithoutDelay) {

	// Empty Data
	std::vector<std::string> names{"A", "B", "C"};
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::FLOAT64} };
	std::unique_ptr<BlazingTable> batch_empty = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(B=[$0], C=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_empty));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 0);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 2);
}


TYPED_TEST(ProjectionTest, TwoBatchsFullsWithoutDelay) {

	using T = TypeParam;

	std::vector<std::string> names({"A", "B", "C"});

	// Batch 1
    cudf::test::fixed_width_column_wrapper<T> col1_a{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2_a({"b", "d", "a", "d"}, {1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_a{{10, 40, 70, 5}, {1, 1, 1, 1}};
    CudfTableView cudf_table_in_view_a {{col1_a, col2_a, col3_a}};
    std::unique_ptr<CudfTable> cudf_table_a = std::make_unique<CudfTable>(cudf_table_in_view_a);
    std::unique_ptr<BlazingTable> batch_1 = std::make_unique<BlazingTable>(std::move(cudf_table_a), names);

	// Batch 2
    cudf::test::fixed_width_column_wrapper<T> col1_b{{28, 5, 6}, {1, 1, 1}};
    cudf::test::strings_column_wrapper col2_b({"l", "d", "k"}, {1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_b{{2, 10, 11}, {1, 1, 1}};
    CudfTableView cudf_table_in_view_b {{col1_b, col2_b, col3_b}};
    std::unique_ptr<CudfTable> cudf_table_b = std::make_unique<CudfTable>(cudf_table_in_view_b);
    std::unique_ptr<BlazingTable> batch_2 = std::make_unique<BlazingTable>(std::move(cudf_table_b), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0], B=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delays
	std::vector<int> delays_in_ms {0, 0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_1));
	batches.push_back(std::move(batch_2));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 2);
	EXPECT_EQ(batches_pulled[0]->num_rows() + batches_pulled[1]->num_rows(), 7);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 2);
	ASSERT_EQ(batches_pulled[1]->num_columns(), 2);
}


// The second batch should not be added to the InputCacheMachine because
// already another batch was added.
TYPED_TEST(ProjectionTest, TwoBatchsFirstFullSecondEmptyWithoutDelays) {

	using T = TypeParam;

	std::vector<std::string> names({"A", "B"});

	// Batch 1
    cudf::test::fixed_width_column_wrapper<T> col1_a{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2_a({"b", "d", "a", "d"}, {1, 1, 1, 1});
    CudfTableView cudf_table_in_view_1 {{col1_a, col2_a}};
    std::unique_ptr<CudfTable> cudf_table_1 = std::make_unique<CudfTable>(cudf_table_in_view_1);
    std::unique_ptr<BlazingTable> batch_full = std::make_unique<BlazingTable>(std::move(cudf_table_1), names);

	// Batch 2
    std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING} };
	std::unique_ptr<BlazingTable> batch_empty = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(B=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delays
	std::vector<int> delays_in_ms {0, 0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_full));
	batches.push_back(std::move(batch_empty)); // will not be added to the inputCacheMachine
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);
	
	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 4);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, OneBatchFullWithDelay) {

	using T = TypeParam;

	// Batch
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

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine with just one delay
	std::vector<int> delays_in_ms {100};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 7);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
}


TYPED_TEST(ProjectionTest, OneBatchEmptyWithDelay) {

	// Empty Data
	std::vector<std::string> names{"A", "B", "C"};
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::FLOAT64} };
	std::unique_ptr<BlazingTable> batch_empty = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0], C=[$1])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add empty data to the inputCacheMachine with delay
	std::vector<int> delays_in_ms {30};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_empty));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 0);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 2);
}


TYPED_TEST(ProjectionTest, TwoBatchsFullWithDelays) {

	using T = TypeParam;

	std::vector<std::string> names({"A", "B", "C"});

	// Batch 1
    cudf::test::fixed_width_column_wrapper<T> col1_a{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2_a({"b", "d", "a", "d"}, {1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_a{{10, 40, 70, 5}, {1, 1, 1, 1}};

    CudfTableView cudf_table_in_view_a {{col1_a, col2_a, col3_a}};
    std::unique_ptr<CudfTable> cudf_table_1 = std::make_unique<CudfTable>(cudf_table_in_view_a);
    std::unique_ptr<BlazingTable> batch_1 = std::make_unique<BlazingTable>(std::move(cudf_table_1), names);

	// Batch 2
    cudf::test::fixed_width_column_wrapper<T> col1_b{{28, 5, 6}, {1, 1, 1}};
    cudf::test::strings_column_wrapper col2_b({"l", "d", "k"}, {1, 1, 1});
    cudf::test::fixed_width_column_wrapper<T> col3_b{{2, 10, 11}, {1, 1, 1}};

    CudfTableView cudf_table_in_view_b {{col1_b, col2_b, col3_b}};
    std::unique_ptr<CudfTable> cudf_table_2 = std::make_unique<CudfTable>(cudf_table_in_view_b);
    std::unique_ptr<BlazingTable> batch_2 = std::make_unique<BlazingTable>(std::move(cudf_table_2), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add data to the inputCacheMachine without delays
	std::vector<int> delays_in_ms {30, 60};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_1));
	batches.push_back(std::move(batch_2));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 2);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
	ASSERT_EQ(batches_pulled[1]->num_columns(), 1);
}


// In this case only the first empty batch will be added
// The second empty batch will be ignored
TYPED_TEST(ProjectionTest, TwoBatchsEmptyWithDelays) {

	// Empty Data
	std::vector<std::string> names{"A", "B", "C", "D"};
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING},
										 cudf::data_type{cudf::type_id::FLOAT64},
										 cudf::data_type{cudf::type_id::STRING} };
	std::unique_ptr<BlazingTable> batch_empty_1 = ral::frame::createEmptyBlazingTable(types, names);
	std::unique_ptr<BlazingTable> batch_empty_2 = ral::frame::createEmptyBlazingTable(types, names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(B=[$0],C=[$1],D=[$2])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add empty data to the inputCacheMachine with delay
	std::vector<int> delays_in_ms {10, 35};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_empty_1));
	batches.push_back(std::move(batch_empty_2)); // will not be added to the inputCacheMachine
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 0);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 3);
}


// In this case both batchs will be added
TYPED_TEST(ProjectionTest, TwoBatchsFirstEmptySecondFullWithDelays) {

	using T = TypeParam;

	// Empty Data
	std::vector<std::string> names({"A", "B"});

	// Batch 1 - empty
	std::vector<cudf::data_type> types { cudf::data_type{cudf::type_id::INT32},
										 cudf::data_type{cudf::type_id::STRING} };
	std::unique_ptr<BlazingTable> batch_empty = ral::frame::createEmptyBlazingTable(types, names);

	// Batch2
    cudf::test::fixed_width_column_wrapper<T> col1{{14, 25, 3, 5}, {1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d"}, {1, 1, 1, 1});
    CudfTableView cudf_table_in_view_1 {{col1, col2}};
    std::unique_ptr<CudfTable> cudf_table_1 = std::make_unique<CudfTable>(cudf_table_in_view_1);
    std::unique_ptr<BlazingTable> batch_full = std::make_unique<BlazingTable>(std::move(cudf_table_1), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(A=[$0])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// run function
	std::thread run_thread = std::thread([project_kernel](){
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	// Add empty data to the inputCacheMachine with delay
	std::vector<int> delays_in_ms {15, 25};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch_empty));
	batches.push_back(std::move(batch_full));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	run_thread.join();	

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	// This randomly returns 1 or 2 batches depending if the empty table got processed first
	// EXPECT_EQ(batches_pulled[0]->num_rows() + batches_pulled[1]->num_rows(), 4);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);
}


struct ProjectionTest2 : public ::testing::Test {
	virtual void SetUp() override {
		BlazingRMMInitialize("pool_memory_resource", 32*1024*1024, 256*1024*1024);
		float host_memory_quota=0.75; //default value
		blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
		ral::memory::set_allocation_pools(4000000, 10,
										4000000, 10, false,nullptr);
		int executor_threads = 10;
		ral::execution::executor::init_executor(executor_threads, 0.8);
	}

	virtual void TearDown() override {
		ral::memory::empty_pools();
		BlazingRMMFinalize();
	}
};



TEST_F(ProjectionTest2, LARGE_LITERAL) {

	// Batch
	cudf::test::fixed_width_column_wrapper<int16_t> col1{{10, 40, 70, 5, 2, 10, 11}, {1, 1, 1, 1, 1, 1, 1}};
    cudf::test::strings_column_wrapper col2({"b", "d", "a", "d", "l", "d", "k"}, {1, 1, 1, 1, 1, 1, 1});
    cudf::test::fixed_width_column_wrapper<int64_t> col3{{1000, 4000, 7000, 500, 200, 100, 1100}, {1, 1, 1, 1, 1, 1, 1}};

    CudfTableView cudf_table_in_view {{col1, col2, col3}};
    std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(cudf_table_in_view);

    std::vector<std::string> names({"A", "B", "C"});
    std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(l_linenumber=[$0], l_comment=[$1], EXPR$2=[=($2, 100000000000)])", context);

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

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), 7);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 3);
}
