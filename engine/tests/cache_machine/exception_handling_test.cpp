#include <thread>
#include <cudf_test/base_fixture.hpp>
#include "cudf_test/column_wrapper.hpp"

#include "tests/utilities/MemoryConsumer.cuh"
#include "tests/utilities/BlazingUnitTest.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"
#include "execution_graph/logic_controllers/BatchUnionProcessing.h"
#include "execution_graph/logic_controllers/BatchOrderByProcessing.h"
#include "execution_graph/logic_controllers/taskflow/executor.h"

using blazingdb::transport::Node;
using ral::cache::kstatus;
using ral::cache::CacheMachine;
using ral::frame::BlazingTable;
using ral::cache::kernel;
using Context = blazingdb::manager::Context;

struct ExceptionHandlingTest : public ::testing::Test {
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

// Creates a UnionKernel using a valid `project_plan`
std::shared_ptr<kernel> make_union_kernel(std::string project_plan, std::shared_ptr<Context> context) {
	std::size_t kernel_id = 1;
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<kernel> union_kernel = std::make_shared<ral::batch::UnionKernel>(kernel_id, project_plan, context, graph);

	return union_kernel;
}

// Creates a SortKernel using a valid `project_plan`
std::shared_ptr<kernel> make_sort_kernel(std::string project_plan, std::shared_ptr<Context> context) {
	std::size_t kernel_id = 1;
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<kernel> sort_kernel = std::make_shared<ral::batch::SortAndSampleKernel>(kernel_id, project_plan, context, graph);

	return sort_kernel;
}

// Creates two CacheMachines and register them with the `project_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>> register_project_kernel_with_cache_machines(
	std::shared_ptr<kernel> project_kernel,
	std::shared_ptr<Context> context,
	int cache_level_override) {
	std::shared_ptr<CacheMachine>  inputCacheMachine = std::make_shared<CacheMachine>(context, "", true, cache_level_override);
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context, "", true, 0);
	project_kernel->input_.register_cache("1", inputCacheMachine);
	project_kernel->output_.register_cache("1", outputCacheMachine);

	return std::make_tuple(inputCacheMachine, outputCacheMachine);
}

// Creates three CacheMachines and register them with the `union_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>> register_union_kernel_with_cache_machines(
	std::shared_ptr<kernel> union_kernel,
	std::shared_ptr<Context> context,
	int cache_level_override) {
	std::shared_ptr<CacheMachine>  inputCacheMachineA = std::make_shared<CacheMachine>(context, "", true, cache_level_override);
	std::shared_ptr<CacheMachine>  inputCacheMachineB = std::make_shared<CacheMachine>(context, "", true, cache_level_override);
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context, "", true, 0);
	union_kernel->input_.register_cache("input_a", inputCacheMachineA);
	union_kernel->input_.register_cache("input_b", inputCacheMachineB);
	union_kernel->output_.register_cache("1", outputCacheMachine);

	return std::make_tuple(inputCacheMachineA, inputCacheMachineB, outputCacheMachine);
}

// Creates two CacheMachines and register them with the `sort_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>> register_sort_kernel_with_cache_machines(
	std::shared_ptr<kernel> sort_kernel,
	std::shared_ptr<Context> context,
	int cache_level_override) {
	std::shared_ptr<CacheMachine>  inputCacheMachine = std::make_shared<CacheMachine>(context, "", true, cache_level_override);
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context, "", true, 0);
	sort_kernel->input_.register_cache("1", inputCacheMachine);
	sort_kernel->output_.register_cache("1", outputCacheMachine);

	return std::make_tuple(inputCacheMachine, outputCacheMachine);
}

// Feeds an input cache
void add_data_to_cache(
	std::shared_ptr<CacheMachine> cache_machine,
	std::vector<std::unique_ptr<BlazingTable>> batches) {
	int total_batches = batches.size();

	for (int i = 0; i < total_batches; ++i) {
		cache_machine->addToCache(std::move(batches[i]));
	}

	cache_machine->finish();
}

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
	auto sequence = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
	std::vector<TypeParam> data(sequence, sequence + size);
	cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
	return col.release();
}

template<class TypeParam>
std::vector<std::unique_ptr<BlazingTable>> make_table(cudf::size_type size) {
	auto col1 = make_col<TypeParam>(size);
	auto col2 = make_col<TypeParam>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(col1));
	columns.push_back(std::move(col2));

	auto cudf_table = std::make_unique<cudf::table>(std::move(columns));

	std::vector<std::string> names({"A", "B"});
	std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	return batches;
}

TEST_F(ExceptionHandlingTest, cpu_data_fail_on_decache) {

	std::shared_ptr<Context> context = make_context();

	// Projection kernel with a valid expression
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(EXPR$0=[+($0, $1)])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_project_kernel_with_cache_machines(project_kernel, context, 1); //CPU cache

	cudf::size_type size = 16*1024*1024; //this input does not fit on the pool
	auto input = make_table<int32_t>(size);
	add_data_to_cache(inputCacheMachine, std::move(input));
	
	EXPECT_THROW(project_kernel->run(), rmm::bad_alloc);
}

TEST_F(ExceptionHandlingTest, cpu_data_fail_on_process) {

	std::shared_ptr<Context> context = make_context();

	// Projection kernel with invalid column index
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(EXPR$0=[+($0, $2)])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_project_kernel_with_cache_machines(project_kernel, context, 1); //CPU cache

	cudf::size_type size = 1*1024*1024; //this input fits on the pool
	auto input = make_table<int32_t>(size);
	add_data_to_cache(inputCacheMachine, std::move(input));

	EXPECT_THROW(project_kernel->run(), std::runtime_error);
}

TEST_F(ExceptionHandlingTest, gpu_data_fail_on_project) {

	std::shared_ptr<Context> context = make_context();

	// Projection kernel for select sqrt(-1)
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(EXPR$0=[POWER(-1, 0.5:DECIMAL(2, 1))])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_project_kernel_with_cache_machines(project_kernel, context, 0); //GPU cache

	cudf::size_type size = 1*1024*1024; //this input fits on the pool
	auto input = make_table<int32_t>(size);
	add_data_to_cache(inputCacheMachine, std::move(input));

	EXPECT_THROW(project_kernel->run(), std::runtime_error);
}

TEST_F(ExceptionHandlingTest, gpu_data_fail_on_union) {

	std::shared_ptr<Context> context = make_context();

	// Union kernel for (select * from df1) union (select * from df2)
	std::shared_ptr<kernel> union_kernel = make_union_kernel("LogicalUnion(all=[false])", context);

	// register cache machines with the `union_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachineA, inputCacheMachineB, outputCacheMachine;
	std::tie(inputCacheMachineA, inputCacheMachineB, outputCacheMachine) = register_union_kernel_with_cache_machines(union_kernel, context, 0); //GPU cache

	cudf::size_type size = 1*1024*1024; //this input fits on the pool
	auto inputA = make_table<int32_t>(size);
	auto inputB = make_table<int32_t>(size);
	add_data_to_cache(inputCacheMachineA, std::move(inputA));
	add_data_to_cache(inputCacheMachineB, std::move(inputB));

	EXPECT_THROW(union_kernel->run(), std::exception);
}

TEST_F(ExceptionHandlingTest, gpu_data_fail_on_sort) {

	std::shared_ptr<Context> context = make_context();

	// Sort kernel for select * from df1 order by A
	std::shared_ptr<kernel> sort_kernel = make_sort_kernel("Logical_SortAndSample(sort0=[$0], dir0=[ASC])", context);

	// register cache machines with the `sort_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_sort_kernel_with_cache_machines(sort_kernel, context, 0); //GPU cache

	cudf::size_type size = 16*1024*1024; //this input fits on the pool, but not the result
	auto input = make_table<int32_t>(size);
	add_data_to_cache(inputCacheMachine, std::move(input));

	EXPECT_THROW(sort_kernel->run(), rmm::bad_alloc);
}
