#include <thread>
#include <cudf_test/base_fixture.hpp>
#include "cudf_test/column_wrapper.hpp"

#include "tests/utilities/MemoryConsumer.cuh"
#include "tests/utilities/BlazingUnitTest.h"
#include "execution_graph/logic_controllers/BatchProcessing.h"
#include "execution_graph/logic_controllers/taskflow/executor.h"

using blazingdb::transport::Node;
using ral::cache::kstatus;
using ral::cache::CacheMachine;
using ral::frame::BlazingTable;
using ral::cache::kernel;
using Context = blazingdb::manager::Context;

struct ExceptionHandlingTest : public ::testing::Test {
	virtual void SetUp() override {
		BlazingRMMInitialize("cuda_memory_resource");
		float host_memory_quota=0.75; //default value
		blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
		int executor_threads = 10;
		ral::execution::executor::init_executor(executor_threads, 0.8);
	}

	virtual void TearDown() override {
		BlazingRMMFinalize();
	}
};

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

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
	auto sequence = cudf::test::make_counting_transform_iterator(0, [](auto i) { return TypeParam(i); });
	std::vector<TypeParam> data(sequence, sequence + size);
	cudf::test::fixed_width_column_wrapper<TypeParam> col(data.begin(), data.end());
	return col.release();
}

TEST_F(ExceptionHandlingTest, OneBatchFullWithoutDelay) {

	MemoryConsumer memory_consumer;
	memory_consumer.setOptionsPercentage({0.5, 0.3}, 4000ms);

	//Creating a thread to execute our task
	std::thread memory_consumer_thread([&memory_consumer]()
	{
		memory_consumer.run();
	});

	using T = int32_t;

	// Batch
	cudf::size_type size = 16*1024*1024;

	auto col1 = make_col<T>(size);
	auto col2 = make_col<T>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(col1));
	columns.push_back(std::move(col2));

	auto cudf_table = std::make_unique<cudf::table>(std::move(columns));

	std::vector<std::string> names({"A", "B"});
	std::unique_ptr<BlazingTable> batch = std::make_unique<BlazingTable>(std::move(cudf_table), names);

	// Context
	std::shared_ptr<Context> context = make_context();

	// Projection kernel
	std::shared_ptr<kernel> project_kernel = make_project_kernel("LogicalProject(EXPR$0=[+($0, $1)])", context);

	// register cache machines with the `project_kernel`
	std::shared_ptr<CacheMachine> inputCacheMachine, outputCacheMachine;
	std::tie(inputCacheMachine, outputCacheMachine) = register_kernel_with_cache_machines(project_kernel, context);

	// Add data to the inputCacheMachine without delay
	std::vector<int> delays_in_ms {0};
	std::vector<std::unique_ptr<BlazingTable>> batches;
	batches.push_back(std::move(batch));
	add_data_to_cache_with_delay(inputCacheMachine, std::move(batches), delays_in_ms);

	// main function

	try{
		kstatus process = project_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	}catch(std::exception& ex){
		std::cout<<"Caught\n";
	}

	outputCacheMachine->finish();

	// Assert output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();

	ASSERT_EQ(batches_pulled.size(), 1);
	EXPECT_EQ(batches_pulled[0]->num_rows(), size);
	ASSERT_EQ(batches_pulled[0]->num_columns(), 1);

	memory_consumer.stop();
	memory_consumer_thread.join();
}