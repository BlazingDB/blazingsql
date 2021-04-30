#include <spdlog/spdlog.h>
#include "tests/utilities/BlazingUnitTest.h"
#include "utilities/DebuggingUtils.h"
#include "utilities/CommonOperations.h"

#include <chrono>
#include <thread>
//#include <gtest/gtest.h>

#include "cudf_test/column_wrapper.hpp"
#include "cudf_test/type_lists.hpp"	 // cudf::test::NumericTypes
#include <cudf_test/table_utilities.hpp>

#include "execution_graph/Context.h"
#include "execution_graph/logic_controllers/taskflow/kernel.h"
#include "execution_graph/logic_controllers/taskflow/graph.h"
#include "execution_graph/logic_controllers/taskflow/port.h"
#include "execution_graph/logic_controllers/BatchWindowFunctionProcessing.h"
#include "execution_graph/logic_controllers/taskflow/executor.h"
#include "execution_graph/logic_controllers/CacheData.h"
#include "execution_graph/logic_controllers/GPUCacheData.h"

using blazingdb::transport::Node;
using ral::cache::kstatus;
using ral::cache::CacheMachine;
using ral::cache::CacheData;
using ral::frame::BlazingTable;
using ral::cache::kernel;
using Context = blazingdb::manager::Context;


struct WindowOverlapAccumulatorTest : public ::testing::Test {
	virtual void SetUp() override {
		BlazingRMMInitialize();
		float host_memory_quota=0.75; //default value
		blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
		ral::memory::set_allocation_pools(4000000, 10,
										4000000, 10, false, {});
		int executor_threads = 10;
		ral::execution::executor::init_executor(executor_threads, 0.8);
	}

	virtual void TearDown() override {
		ral::memory::empty_pools();
		BlazingRMMFinalize();
	}
};

struct WindowOverlapGeneratorTest : public ::testing::Test {
    virtual void SetUp() override {
        BlazingRMMInitialize();
        float host_memory_quota=0.75; //default value
        blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
        ral::memory::set_allocation_pools(4000000, 10,
                                          4000000, 10, false, {});
        int executor_threads = 10;
        ral::execution::executor::init_executor(executor_threads, 0.8);
    }

    virtual void TearDown() override {
        ral::memory::empty_pools();
        BlazingRMMFinalize();
    }
};

struct WindowOverlapTest : public ::testing::Test {
    virtual void SetUp() override {
        BlazingRMMInitialize();
        float host_memory_quota=0.75; //default value
        blazing_host_memory_resource::getInstance().initialize(host_memory_quota);
        ral::memory::set_allocation_pools(4000000, 10,
                                          4000000, 10, false, {});
        int executor_threads = 10;
        ral::execution::executor::init_executor(executor_threads, 0.8);
    }

    virtual void TearDown() override {
        ral::memory::empty_pools();
        BlazingRMMFinalize();
    }
};

// Just creates a Context
std::shared_ptr<Context> make_context(int num_nodes) {
	std::vector<Node> nodes(num_nodes);
	for (int i = 0; i < num_nodes; i++){
		nodes[i] = Node(std::to_string(i));
	}
	Node master_node("0");
	std::string logicalPlan;
	std::map<std::string, std::string> config_options;
	std::string current_timestamp;
	std::shared_ptr<Context> context = std::make_shared<Context>(0, nodes, master_node, logicalPlan, config_options, current_timestamp);

	return context;
}

// Creates a OverlapAccumulatorKernel using a valid `project_plan`
std::tuple<std::shared_ptr<kernel>, std::shared_ptr<ral::cache::CacheMachine>, std::shared_ptr<ral::cache::CacheMachine>>
		make_overlap_Accumulator_kernel(std::string project_plan, std::shared_ptr<Context> context) {
	std::size_t kernel_id = 1;
	std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
	std::shared_ptr<ral::cache::CacheMachine> input_cache = std::make_shared<CacheMachine>(nullptr, "messages_in", false);
	std::shared_ptr<ral::cache::CacheMachine> output_cache = std::make_shared<CacheMachine>(nullptr, "messages_out", false, ral::cache::CACHE_LEVEL_CPU );
	graph->set_input_and_output_caches(input_cache, output_cache);
	std::shared_ptr<kernel> overlap_accumulator_kernel = std::make_shared<ral::batch::OverlapAccumulatorKernel>(kernel_id, project_plan, context, graph);

	return std::make_tuple(overlap_accumulator_kernel, input_cache, output_cache);
}

// Creates a OverlapGeneratorKernel using a valid `project_plan`
std::tuple<std::shared_ptr<kernel>, std::shared_ptr<ral::cache::CacheMachine>, std::shared_ptr<ral::cache::CacheMachine>>
make_overlap_Generator_kernel(std::string project_plan, std::shared_ptr<Context> context) {
    std::size_t kernel_id = 1;
    std::shared_ptr<ral::cache::graph> graph = std::make_shared<ral::cache::graph>();
    std::shared_ptr<ral::cache::CacheMachine> input_cache = std::make_shared<CacheMachine>(nullptr, "messages_in", false);
    std::shared_ptr<ral::cache::CacheMachine> output_cache = std::make_shared<CacheMachine>(nullptr, "messages_out", false, ral::cache::CACHE_LEVEL_CPU );
    graph->set_input_and_output_caches(input_cache, output_cache);
    std::shared_ptr<kernel> overlap_generator_kernel = std::make_shared<ral::batch::OverlapGeneratorKernel>(kernel_id, project_plan, context, graph);

    return std::make_tuple(overlap_generator_kernel, input_cache, output_cache);
}

// Creates two CacheMachines and register them with the `project_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>> register_kernel_overlap_accumulator_with_cache_machines(
	std::shared_ptr<kernel> overlap_accumulator_kernel,
	std::shared_ptr<Context> context) {
	std::shared_ptr<CacheMachine>  batchesCacheMachine = std::make_shared<CacheMachine>(context, "batches");
	std::shared_ptr<CacheMachine>  precedingCacheMachine = std::make_shared<CacheMachine>(context, "preceding_overlaps");
	std::shared_ptr<CacheMachine>  followingCacheMachine = std::make_shared<CacheMachine>(context, "following_overlaps");
	std::shared_ptr<CacheMachine> outputCacheMachine = std::make_shared<CacheMachine>(context, "1");
    overlap_accumulator_kernel->input_.register_cache("batches", batchesCacheMachine);
    overlap_accumulator_kernel->input_.register_cache("preceding_overlaps", precedingCacheMachine);
    overlap_accumulator_kernel->input_.register_cache("following_overlaps", followingCacheMachine);
    overlap_accumulator_kernel->output_.register_cache("1", outputCacheMachine);

	return std::make_tuple(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine);	
}

// Creates two CacheMachines and register them with the `project_kernel`
std::tuple<std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>, std::shared_ptr<CacheMachine>>
register_kernel_overlap_generator_with_cache_machines(
        std::shared_ptr<kernel> overlap_generator_kernel,
        std::shared_ptr<Context> context) {
    std::shared_ptr<CacheMachine> batchesCacheMachine = std::make_shared<CacheMachine>(context, "batches");
    std::shared_ptr<CacheMachine> precedingCacheMachine = std::make_shared<CacheMachine>(context, "preceding_overlaps");
    std::shared_ptr<CacheMachine> followingCacheMachine = std::make_shared<CacheMachine>(context, "following_overlaps");
    std::shared_ptr<CacheMachine> inputCacheMachine = std::make_shared<CacheMachine>(context, "1");
    overlap_generator_kernel->input_.register_cache("1", inputCacheMachine);
    overlap_generator_kernel->output_.register_cache("batches", batchesCacheMachine);
    overlap_generator_kernel->output_.register_cache("preceding_overlaps", precedingCacheMachine);
    overlap_generator_kernel->output_.register_cache("following_overlaps", followingCacheMachine);

    return std::make_tuple(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, inputCacheMachine);
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

// this function takes one big vector and breaks it up into batch_sizes to generate a vector of batches and its corresponding preceding_overlaps and following_overlaps in a way similar to how 
// the kernel which feeds OverlapAccumulatorKernel would do
std::tuple<std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>> break_up_full_data(
		CudfTableView full_data_cudf_view, int preceding_value, int following_value, std::vector<cudf::size_type> batch_sizes, std::vector<std::string> names){
	
	std::vector<cudf::size_type> split_indexes = batch_sizes;
	std::partial_sum(split_indexes.begin(), split_indexes.end(), split_indexes.begin());
	split_indexes.erase(split_indexes.begin() + split_indexes.size() - 1);

	auto split_views = cudf::split(full_data_cudf_view, split_indexes);
	
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::vector<std::unique_ptr<BlazingTable>> batch_tables;
	// make batches
	for (std::size_t i = 0; i < split_views.size(); i++){
		auto cudf_table = std::make_unique<CudfTable>(split_views[i]);
		auto blz_table = std::make_unique<BlazingTable>(std::move(cudf_table), names);
		// ral::utilities::print_blazing_table_view(blz_table->toBlazingTableView(), "batch" + std::to_string(i));
		batch_tables.push_back(std::move(blz_table));
	}
	// make preceding overlaps
	// preceding_overlaps[n] goes with batch[n+1]
	for (std::size_t i = 0; i < split_views.size() - 1; i++){
		std::unique_ptr<CudfTable> cudf_table;
		if (preceding_value > batch_tables[i]->num_rows()){
			cudf_table = std::make_unique<CudfTable>(batch_tables[i]->view());
		} else {
            cudf::size_type split_value = preceding_value < batch_tables[i]->num_rows() ? batch_tables[i]->num_rows() - preceding_value : 0;
			std::vector<cudf::size_type> presceding_split_index = {split_value};
			auto preceding_split_views = cudf::split(batch_tables[i]->view(), presceding_split_index);
			cudf_table = std::make_unique<CudfTable>(preceding_split_views[1]);
		}
		auto blz_table = std::make_unique<BlazingTable>(std::move(cudf_table), names);
		// ral::utilities::print_blazing_table_view(blz_table->toBlazingTableView(), "preceding" + std::to_string(i));
		std::string overlap_status = preceding_value > batch_tables[i]->num_rows() ? ral::batch::INCOMPLETE_OVERLAP_STATUS : ral::batch::DONE_OVERLAP_STATUS;
		ral::cache::MetadataDictionary metadata;
		metadata.add_value(ral::cache::OVERLAP_STATUS, overlap_status);
		preceding_overlaps.push_back(std::make_unique<ral::cache::GPUCacheData>(std::move(blz_table),metadata));
	}
	// make following overlaps
	// following_overlaps[n] goes with batch[n] but there is no following_overlaps[batch.size()-1]
	for (std::size_t i = 1; i < split_views.size(); i++){
		std::unique_ptr<CudfTable> cudf_table;
		if (following_value > batch_tables[i]->num_rows()){
			cudf_table = std::make_unique<CudfTable>(batch_tables[i]->view());
		} else {
			std::vector<cudf::size_type> following_split_index = {following_value};
			auto following_split_views = cudf::split(batch_tables[i]->view(), following_split_index);
			cudf_table = std::make_unique<CudfTable>(following_split_views[0]);
		}	
		auto blz_table = std::make_unique<BlazingTable>(std::move(cudf_table), names);
		// ral::utilities::print_blazing_table_view(blz_table->toBlazingTableView(), "following" + std::to_string(i));
		std::string overlap_status = following_value > batch_tables[i]->num_rows() ? ral::batch::INCOMPLETE_OVERLAP_STATUS : ral::batch::DONE_OVERLAP_STATUS;
		ral::cache::MetadataDictionary metadata;
		metadata.add_value(ral::cache::OVERLAP_STATUS, overlap_status);
		following_overlaps.push_back(std::make_unique<ral::cache::GPUCacheData>(std::move(blz_table),metadata));
	}
	// package the batches as CacheDatas
	for (std::size_t i = 0; i < split_views.size(); i++){
		batches.push_back(std::make_unique<ral::cache::GPUCacheData>(std::move(batch_tables[i])));
	}

	return std::make_tuple(std::move(preceding_overlaps), std::move(batches), std::move(following_overlaps));
}

// this function takes one big vector and breaks it up into batch_sizes to generate a vector of batches and its corresponding preceding_overlaps and following_overlaps in a way similar to how 
// the kernel which feeds OverlapAccumulatorKernel would do, similar to break_up_full_data. But it assumes a multi node setup, and therefore all the batches in batch_sizes are not assumed to belong
// to the self_node_index node. It assumes that one or both of the edge batches belong to other nodes. It uses this assumption to produce previous_node_overlap and next_node_overlap
std::tuple<std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>, 
		std::unique_ptr<CacheData>, std::unique_ptr<CacheData>> break_up_full_data_multinode(
		CudfTableView full_data_cudf_view, int preceding_value, int following_value, std::vector<cudf::size_type> batch_sizes, std::vector<std::string> names, 
		int total_nodes, int self_node_index){

	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	if (total_nodes == 1){
		return std::make_tuple(std::move(preceding_overlaps),std::move(batches),std::move(following_overlaps),std::move(previous_node_overlap),std::move(next_node_overlap));
	} else if (self_node_index == 0) {
		batches.erase(batches.begin() + batches.size() - 1);
		next_node_overlap = std::move(following_overlaps.back());
		following_overlaps.erase(following_overlaps.begin() + following_overlaps.size() - 1);
		preceding_overlaps.erase(preceding_overlaps.begin() + preceding_overlaps.size() - 1);		
	} else if (self_node_index == total_nodes - 1) {
		batches.erase(batches.begin());
		previous_node_overlap = std::move(preceding_overlaps[0]);
		following_overlaps.erase(following_overlaps.begin());
		preceding_overlaps.erase(preceding_overlaps.begin());
	} else {
		batches.erase(batches.begin() + batches.size() - 1);
		next_node_overlap = std::move(following_overlaps.back());
		following_overlaps.erase(following_overlaps.begin() + following_overlaps.size() - 1);
		preceding_overlaps.erase(preceding_overlaps.begin() + preceding_overlaps.size() - 1);	

		batches.erase(batches.begin());
		previous_node_overlap = std::move(preceding_overlaps[0]);
		following_overlaps.erase(following_overlaps.begin());
		preceding_overlaps.erase(preceding_overlaps.begin());
	}
	return std::make_tuple(std::move(preceding_overlaps),std::move(batches),std::move(following_overlaps),std::move(previous_node_overlap),std::move(next_node_overlap)); 
}

std::vector<std::unique_ptr<BlazingTable>> make_expected_accumulator_output(
		CudfTableView full_data_cudf_view, int preceding_value, int following_value, std::vector<cudf::size_type> batch_sizes, std::vector<std::string> names){
	
	std::vector<cudf::size_type> split_indexes = batch_sizes;
	std::partial_sum(split_indexes.begin(), split_indexes.end(), split_indexes.begin());
	split_indexes.erase(split_indexes.begin() + split_indexes.size() - 1);

	std::vector<std::unique_ptr<BlazingTable>> out_batches;
	for (std::size_t i = 0; i < batch_sizes.size(); i++){
		if (i == 0){
            cudf::size_type split_value = split_indexes[i] + following_value > full_data_cudf_view.num_rows() ? full_data_cudf_view.num_rows() : split_indexes[i] + following_value;
			std::vector<cudf::size_type> out_split_index = {split_value};
			auto out_split_views = cudf::split(full_data_cudf_view, out_split_index);
			auto cudf_table = std::make_unique<CudfTable>(out_split_views[0]);
			out_batches.push_back(std::make_unique<BlazingTable>(std::move(cudf_table), names));
		} else if (i == batch_sizes.size() - 1){
            cudf::size_type split_value = full_data_cudf_view.num_rows() - batch_sizes[batch_sizes.size() - 1] - preceding_value > 0 ? full_data_cudf_view.num_rows() - batch_sizes[batch_sizes.size() - 1] - preceding_value : 0;
			std::vector<cudf::size_type> out_split_index = {split_value};
			auto out_split_views = cudf::split(full_data_cudf_view, out_split_index);
			auto cudf_table = std::make_unique<CudfTable>(out_split_views[1]);
			out_batches.push_back(std::make_unique<BlazingTable>(std::move(cudf_table), names));
		} else {
            cudf::size_type split_value1 = split_indexes[i - 1] - preceding_value > 0 ? split_indexes[i - 1] - preceding_value : 0;
            cudf::size_type split_value2 = split_indexes[i] + following_value > full_data_cudf_view.num_rows() ? full_data_cudf_view.num_rows() : split_indexes[i] + following_value;
			std::vector<cudf::size_type> out_split_index = {split_value1, split_value2};
			auto out_split_views = cudf::split(full_data_cudf_view, out_split_index);
			auto cudf_table = std::make_unique<CudfTable>(out_split_views[1]);
			out_batches.push_back(std::make_unique<BlazingTable>(std::move(cudf_table), names));
		}		
	}
	return std::move(out_batches);
}

std::vector<std::unique_ptr<BlazingTable>> make_expected_generator_output(
        CudfTableView full_data_cudf_view, std::vector<cudf::size_type> batch_sizes, std::vector<std::string> names){

    std::vector<cudf::size_type> split_indexes = batch_sizes;
    std::partial_sum(split_indexes.begin(), split_indexes.end(), split_indexes.begin());
    split_indexes.erase(split_indexes.begin() + split_indexes.size() - 1);

    auto split_views = cudf::split(full_data_cudf_view, split_indexes);
    std::vector<std::unique_ptr<BlazingTable>> batch_tables;

    for (std::size_t i = 0; i < split_views.size(); i++){
        auto cudf_table = std::make_unique<CudfTable>(split_views[i]);
        auto blz_table = std::make_unique<BlazingTable>(std::move(cudf_table), names);
        batch_tables.push_back(std::move(blz_table));
    }
    return batch_tables;
}

TEST_F(WindowOverlapAccumulatorTest, BasicSingleNode) {

	size_t size = 100000;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});

	int preceding_value = 50;
	int following_value = 10;
	std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(1);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize("0", "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_cache, output_cache;
	std::tie(overlap_accumulator_kernel, input_cache, output_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();	
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}
}

TEST_F(WindowOverlapAccumulatorTest, BasicMultiNode_FirstNode) {

	int self_node_index = 0;
	int total_nodes = 5;
	size_t size = 100000;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 50;
	int following_value = 10;
	std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_request_response_table = ral::utilities::getLimitedRows(expected_out.back()->toBlazingTableView(), preceding_value, true);
	expected_out.erase(expected_out.begin() + expected_out.size()-1);

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string sender_node_id = std::to_string(self_node_index + 1);
	std::string message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + sender_node_id;

	// create overlap request
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	ral::cache::MetadataDictionary request_metadata;
    request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_REQUEST);
    request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(preceding_value));
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, sender_node_id);
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(0));
    request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	input_message_cache->addToCache(std::move(empty_table), ral::batch::PRECEDING_REQUEST + message_id, true, request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary metadata;
	metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_RESPONSE);
	metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, sender_node_id);
	metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, batches.size() - 1);
	metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	next_node_overlap->setMetadata(metadata);
	input_message_cache->addCacheData(std::move(next_node_overlap), ral::batch::FOLLOWING_RESPONSE + message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	// get and validate request response
	std::unique_ptr<CacheData> following_request = output_message_cache->pullCacheData();
	std::unique_ptr<CacheData> request_response = output_message_cache->pullCacheData();
	ral::cache::MetadataDictionary request_response_metadata = request_response->getMetadata();
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::PRECEDING_RESPONSE);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), sender_node_id);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(0));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> request_response_table = request_response->decache();
	cudf::test::expect_tables_equivalent(expected_request_response_table->view(), request_response_table->view());
}

TEST_F(WindowOverlapAccumulatorTest, BasicMultiNode_LastNode) {

	int self_node_index = 4;
	int total_nodes = 5;
	size_t size = 100000;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 50;
	int following_value = 10;
	std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_request_response_table = ral::utilities::getLimitedRows(expected_out[0]->toBlazingTableView(), following_value, false);
	expected_out.erase(expected_out.begin());

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string sender_node_id = std::to_string(self_node_index - 1);
	std::string message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + sender_node_id;

	// create overlap request
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	ral::cache::MetadataDictionary request_metadata;
    request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_REQUEST);
    request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(following_value));
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, sender_node_id);
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(batches.size() - 1));
    request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	input_message_cache->addToCache(std::move(empty_table), ral::batch::FOLLOWING_REQUEST + message_id, true, request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary metadata;
	metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_RESPONSE);
	metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, sender_node_id);
	metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, 0);
	metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	previous_node_overlap->setMetadata(metadata);
	input_message_cache->addCacheData(std::move(previous_node_overlap), ral::batch::PRECEDING_RESPONSE + message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	// get and validate request response
	std::unique_ptr<CacheData> following_request = output_message_cache->pullCacheData();
	std::unique_ptr<CacheData> request_response = output_message_cache->pullCacheData();
	ral::cache::MetadataDictionary request_response_metadata = request_response->getMetadata();
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::FOLLOWING_RESPONSE);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), sender_node_id);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(batches.size() - 1));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> request_response_table = request_response->decache();
	cudf::test::expect_tables_equivalent(expected_request_response_table->view(), request_response_table->view());
}


TEST_F(WindowOverlapAccumulatorTest, BasicMultiNode_MiddleNode) {

	int self_node_index = 2;
	int total_nodes = 5;
	size_t size = 100000;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 50;
	int following_value = 10;
	std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_following_request_response_table = ral::utilities::getLimitedRows(expected_out[0]->toBlazingTableView(), following_value, false);
	std::unique_ptr<BlazingTable> expected_preceding_request_response_table = ral::utilities::getLimitedRows(expected_out.back()->toBlazingTableView(), preceding_value, true);
	expected_out.erase(expected_out.begin());
	expected_out.erase(expected_out.begin() + expected_out.size()-1);

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string previous_node_id = std::to_string(self_node_index - 1);
	std::string next_node_id = std::to_string(self_node_index + 1);
	std::string previous_node_message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + previous_node_id;
	std::string next_node_message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + next_node_id;

	// create overlap request
	ral::cache::MetadataDictionary following_request_metadata;
    following_request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_REQUEST);
    following_request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(following_value));
    following_request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, next_node_id);
    following_request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(batches.size() - 1));
    following_request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	input_message_cache->addToCache(std::move(empty_table), ral::batch::FOLLOWING_REQUEST + previous_node_message_id, true, following_request_metadata, true);

	ral::cache::MetadataDictionary preceding_request_metadata;
    preceding_request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_REQUEST);
    preceding_request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(preceding_value));
    preceding_request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, previous_node_id);
    preceding_request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(0));
    preceding_request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	input_message_cache->addToCache(std::move(empty_table), ral::batch::PRECEDING_REQUEST + next_node_message_id, true, preceding_request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary preceding_response_metadata;
	preceding_response_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_RESPONSE);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, next_node_id);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, 0);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	previous_node_overlap->setMetadata(preceding_response_metadata);
	input_message_cache->addCacheData(std::move(previous_node_overlap), ral::batch::PRECEDING_RESPONSE + previous_node_message_id, true);

	// create overlap response
	ral::cache::MetadataDictionary following_response_metadata;
	following_response_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_RESPONSE);
	following_response_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, previous_node_id);
	following_response_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	following_response_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, batches.size() - 1);
	following_response_metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	next_node_overlap->setMetadata(following_response_metadata);
	input_message_cache->addCacheData(std::move(next_node_overlap), ral::batch::FOLLOWING_RESPONSE + next_node_message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	// get and validate request response
	std::unique_ptr<CacheData> preceding_request_response, following_request_response;
	ral::cache::MetadataDictionary preceding_request_response_metadata, following_request_response_metadata;

	for (std::size_t i = 0; i < 4; i++){
		std::unique_ptr<CacheData> request = output_message_cache->pullCacheData();
		ral::cache::MetadataDictionary metadata = request->getMetadata();
		if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == ral::batch::PRECEDING_RESPONSE){
			preceding_request_response = std::move(request);
			preceding_request_response_metadata = metadata;
		} else if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == ral::batch::FOLLOWING_RESPONSE){
			following_request_response = std::move(request);
			following_request_response_metadata = metadata;
		}
	}

	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::PRECEDING_RESPONSE);
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), previous_node_id);
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(0));
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> preceding_request_response_table = preceding_request_response->decache();
	cudf::test::expect_tables_equivalent(expected_preceding_request_response_table->view(), preceding_request_response_table->view());

	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::FOLLOWING_RESPONSE);
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), next_node_id);
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(batches.size() - 1));
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> following_request_response_table = following_request_response->decache();
	cudf::test::expect_tables_equivalent(expected_following_request_response_table->view(), following_request_response_table->view());

}

TEST_F(WindowOverlapAccumulatorTest, BigWindowMultiNode_FirstNode) {

	int self_node_index = 0;
	int total_nodes = 5;
	size_t size = 14600;
	// size_t size = 146;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 1500;
	int following_value = 3000;
	std::vector<cudf::size_type> batch_sizes = {1000, 1100, 1200, 1300, 10000}; // need to sum up to size
	// int preceding_value = 15;
	// int following_value = 30;
	// std::vector<cudf::size_type> batch_sizes = {10, 11, 12, 13, 100}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_request_response_table = ral::utilities::getLimitedRows(expected_out.back()->toBlazingTableView(), preceding_value, true);
	expected_out.erase(expected_out.begin() + expected_out.size()-1);

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN " + std::to_string(preceding_value) +
            " PRECEDING AND " + std::to_string(following_value) + " FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string sender_node_id = std::to_string(self_node_index + 1);
	std::string message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + sender_node_id;

	// create overlap request
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	ral::cache::MetadataDictionary request_metadata;
    request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_REQUEST);
    request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(preceding_value));
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, sender_node_id);
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(0));
    request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	input_message_cache->addToCache(std::move(empty_table), ral::batch::PRECEDING_REQUEST + message_id, true, request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary metadata;
	metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_RESPONSE);
	metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, sender_node_id);
	metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, batches.size() - 1);
	metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	next_node_overlap->setMetadata(metadata);
	input_message_cache->addCacheData(std::move(next_node_overlap), ral::batch::FOLLOWING_RESPONSE + message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	// get and validate request response
	std::unique_ptr<CacheData> following_request = output_message_cache->pullCacheData();
	std::unique_ptr<CacheData> request_response = output_message_cache->pullCacheData();
	ral::cache::MetadataDictionary request_response_metadata = request_response->getMetadata();
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::PRECEDING_RESPONSE);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), sender_node_id);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(0));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> request_response_table = request_response->decache();
	cudf::test::expect_tables_equivalent(expected_request_response_table->view(), request_response_table->view());
}

TEST_F(WindowOverlapAccumulatorTest, BigWindowMultiNode_LastNode) {

	int self_node_index = 4;
	int total_nodes = 5;
	size_t size = 14600;
	// size_t size = 146;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 1500;
	int following_value = 3000;
	std::vector<cudf::size_type> batch_sizes = {10000, 1100, 1200, 1300, 1000}; // need to sum up to size
	// int preceding_value = 15;
	// int following_value = 30;
	// std::vector<cudf::size_type> batch_sizes = {10, 11, 12, 13, 100}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_request_response_table = ral::utilities::getLimitedRows(expected_out[0]->toBlazingTableView(), following_value, false);
	expected_out.erase(expected_out.begin());

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN " + std::to_string(preceding_value) +
            " PRECEDING AND " + std::to_string(following_value) + " FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string sender_node_id = std::to_string(self_node_index - 1);
	std::string message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + sender_node_id;

	// create overlap request
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	ral::cache::MetadataDictionary request_metadata;
    request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_REQUEST);
    request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(following_value));
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, sender_node_id);
    request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(batches.size() - 1));
    request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	input_message_cache->addToCache(std::move(empty_table), ral::batch::FOLLOWING_REQUEST + message_id, true, request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary metadata;
	metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_RESPONSE);
	metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, sender_node_id);
	metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, 0);
	metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	previous_node_overlap->setMetadata(metadata);
	input_message_cache->addCacheData(std::move(previous_node_overlap), ral::batch::PRECEDING_RESPONSE + message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	// get and validate request response
	std::unique_ptr<CacheData> request_response = output_message_cache->pullCacheData();
	ral::cache::MetadataDictionary request_response_metadata = request_response->getMetadata();
	if (request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) != ral::batch::FOLLOWING_RESPONSE){
		request_response = output_message_cache->pullCacheData();
		request_response_metadata = request_response->getMetadata();
	}
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::FOLLOWING_RESPONSE);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), sender_node_id);
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(batches.size() - 1));
	EXPECT_EQ(request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> request_response_table = request_response->decache();
	cudf::test::expect_tables_equivalent(expected_request_response_table->view(), request_response_table->view());
}



TEST_F(WindowOverlapAccumulatorTest, BigWindowMultiNode_MiddleNode) {

	int self_node_index = 2;
	int total_nodes = 5;
	size_t size = 14600;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 1500;
	int following_value = 3000;
	std::vector<cudf::size_type> batch_sizes = {5500, 1100, 1200, 1300, 5500}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::unique_ptr<CacheData> previous_node_overlap, next_node_overlap;
	std::tie(preceding_overlaps, batches, following_overlaps, previous_node_overlap, next_node_overlap) = break_up_full_data_multinode(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names, total_nodes, self_node_index);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	std::unique_ptr<BlazingTable> expected_following_request_response_table = ral::utilities::getLimitedRows(expected_out[0]->toBlazingTableView(), following_value, false);
	std::unique_ptr<BlazingTable> expected_preceding_request_response_table = ral::utilities::getLimitedRows(expected_out.back()->toBlazingTableView(), preceding_value, true);
	expected_out.erase(expected_out.begin());
	expected_out.erase(expected_out.begin() + expected_out.size()-1);

    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN " + std::to_string(preceding_value) +
            " PRECEDING AND " + std::to_string(following_value) + " FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();

	// create overlap request and response from neighbor node
	std::string previous_node_id = std::to_string(self_node_index - 1);
	std::string next_node_id = std::to_string(self_node_index + 1);
	std::string previous_node_message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + previous_node_id;
	std::string next_node_message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + next_node_id;

	// create overlap request
	ral::cache::MetadataDictionary following_request_metadata;
    following_request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_REQUEST);
    following_request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(following_value));
    following_request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, next_node_id);
    following_request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(batches.size() - 1));
    following_request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	auto empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	input_message_cache->addToCache(std::move(empty_table), ral::batch::FOLLOWING_REQUEST + previous_node_message_id, true, following_request_metadata, true);

	ral::cache::MetadataDictionary preceding_request_metadata;
    preceding_request_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_REQUEST);
    preceding_request_metadata.add_value(ral::cache::OVERLAP_SIZE, std::to_string(preceding_value));
    preceding_request_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, previous_node_id);
    preceding_request_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, std::to_string(0));
    preceding_request_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, std::to_string(self_node_index));
	empty_table =ral::utilities::create_empty_table(expected_out[0]->toBlazingTableView());
	input_message_cache->addToCache(std::move(empty_table), ral::batch::PRECEDING_REQUEST + next_node_message_id, true, preceding_request_metadata, true);

	// create overlap response
	ral::cache::MetadataDictionary preceding_response_metadata;
	preceding_response_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::PRECEDING_RESPONSE);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, next_node_id);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, 0);
	preceding_response_metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	previous_node_overlap->setMetadata(preceding_response_metadata);
	input_message_cache->addCacheData(std::move(previous_node_overlap), ral::batch::PRECEDING_RESPONSE + previous_node_message_id, true);

	// create overlap response
	ral::cache::MetadataDictionary following_response_metadata;
	following_response_metadata.add_value(ral::cache::OVERLAP_MESSAGE_TYPE, ral::batch::FOLLOWING_RESPONSE);
	following_response_metadata.add_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX, previous_node_id);
	following_response_metadata.add_value(ral::cache::OVERLAP_TARGET_NODE_INDEX, self_node_index);
	following_response_metadata.add_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX, batches.size() - 1);
	following_response_metadata.add_value(ral::cache::OVERLAP_STATUS, ral::batch::DONE_OVERLAP_STATUS);
	next_node_overlap->setMetadata(following_response_metadata);
	input_message_cache->addCacheData(std::move(next_node_overlap), ral::batch::FOLLOWING_RESPONSE + next_node_message_id, true);
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}

	std::string self_node_message_id = std::to_string(context->getContextToken()) + "_" + std::to_string(overlap_accumulator_kernel->get_id()) + "_" + std::to_string(self_node_index);

	// get and validate request response
	std::unique_ptr<CacheData> preceding_request_response, following_request_response;
	ral::cache::MetadataDictionary preceding_request_response_metadata, following_request_response_metadata;

	for (std::size_t i = 0; i < 4; i++){
		std::unique_ptr<CacheData> request = output_message_cache->pullCacheData();
		ral::cache::MetadataDictionary metadata = request->getMetadata();
		if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == ral::batch::PRECEDING_RESPONSE){
			preceding_request_response = std::move(request);
			preceding_request_response_metadata = metadata;
		} else if (metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE) == ral::batch::FOLLOWING_RESPONSE){
			following_request_response = std::move(request);
			following_request_response_metadata = metadata;
		}
	}

	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::PRECEDING_RESPONSE);
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), previous_node_id);
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(0));
	EXPECT_EQ(preceding_request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> preceding_request_response_table = preceding_request_response->decache();
	cudf::test::expect_tables_equivalent(expected_preceding_request_response_table->view(), preceding_request_response_table->view());

	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_MESSAGE_TYPE), ral::batch::FOLLOWING_RESPONSE);
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_SOURCE_NODE_INDEX), std::to_string(self_node_index));
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_NODE_INDEX), next_node_id);
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_TARGET_BATCH_INDEX), std::to_string(batches.size() - 1));
	EXPECT_EQ(following_request_response_metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
	std::unique_ptr<BlazingTable> following_request_response_table = following_request_response->decache();
	cudf::test::expect_tables_equivalent(expected_following_request_response_table->view(), following_request_response_table->view());

}


TEST_F(WindowOverlapAccumulatorTest, BigWindowSingleNode) {

	int self_node_index = 0;
	int total_nodes = 1;
	size_t size = 14600;
	// size_t size = 55;

	// Define full dataset
	auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
	auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
	auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
	auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });
	
    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
	cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);
	
	CudfTableView full_data_cudf_view ({col0, col1, col2});
	// CudfTableView full_data_cudf_view ({col0});

	int preceding_value = 1500;
	int following_value = 3000;
	std::vector<cudf::size_type> batch_sizes = {5500, 1100, 1200, 1300, 5500}; // need to sum up to size
	// int preceding_value = 5;
	// int following_value = 1;
	// std::vector<cudf::size_type> batch_sizes = {20, 10, 25}; // need to sum up to size

	// define how its broken up into batches and overlaps
	std::vector<std::string> names({"A", "B", "C"});
	// std::vector<std::string> names({"A"});
	std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
	std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(
			full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

	std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);
	
    // create and start kernel
	// Context
	std::shared_ptr<Context> context = make_context(total_nodes);

	auto & communicationData = ral::communication::CommunicationData::getInstance();
	communicationData.initialize(std::to_string(self_node_index), "/tmp");

	// overlap kernel
	std::shared_ptr<kernel> overlap_accumulator_kernel;
	std::shared_ptr<ral::cache::CacheMachine> input_message_cache, output_message_cache;
	std::tie(overlap_accumulator_kernel, input_message_cache, output_message_cache) = make_overlap_Accumulator_kernel(
            "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN " + std::to_string(preceding_value) +
            " PRECEDING AND " + std::to_string(following_value) + " FOLLOWING)])", context);

	// register cache machines with the kernel
	std::shared_ptr<CacheMachine> batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine;
	std::tie(batchesCacheMachine, precedingCacheMachine, followingCacheMachine, outputCacheMachine) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel, context);

	// run function
	std::thread run_thread = std::thread([overlap_accumulator_kernel](){
		kstatus process = overlap_accumulator_kernel->run();
		EXPECT_EQ(kstatus::proceed, process);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(10));

	// add data into the CacheMachines
	for (std::size_t i = 0; i < batches.size(); i++) {
		batchesCacheMachine->addCacheData(std::move(batches[i]));
		if (i != 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			precedingCacheMachine->addCacheData(std::move(preceding_overlaps[i - 1]));			
		}
		if (i != batches.size() - 1) {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			followingCacheMachine->addCacheData(std::move(following_overlaps[i]));			
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	batchesCacheMachine->finish();
	precedingCacheMachine->finish();
	followingCacheMachine->finish();
	
	run_thread.join();	
	
	// get and validate output
	auto batches_pulled = outputCacheMachine->pull_all_cache_data();
	EXPECT_EQ(batches_pulled.size(), expected_out.size());
	for (std::size_t i = 0; i < batches_pulled.size(); i++) {
		auto table_out = batches_pulled[i]->decache();
		// ral::utilities::print_blazing_table_view(expected_out[i]->toBlazingTableView(), "expected" + std::to_string(i));
		// ral::utilities::print_blazing_table_view(table_out->toBlazingTableView(), "got" + std::to_string(i));
		cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
	}
}

std::tuple<std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>, std::vector<std::unique_ptr<CacheData>>>
run_overlap_generator_kernel(const std::string& project_plan, std::shared_ptr<Context> context, std::vector<std::unique_ptr<CacheData>>& inputCacheData){

    // overlap Generator kernel
    std::shared_ptr<kernel> overlap_generator_kernel;
    std::shared_ptr<ral::cache::CacheMachine> input_generator_cache, output_generator_cache;
    std::tie(overlap_generator_kernel, input_generator_cache, output_generator_cache) = make_overlap_Generator_kernel(project_plan, context);

    // register cache machines with the kernel
    std::shared_ptr<CacheMachine>   batchesCacheMachineGenerator,
            precedingCacheMachineGenerator,
            followingCacheMachineGenerator,
            inputCacheMachineGenerator;
    std::tie(batchesCacheMachineGenerator,
             precedingCacheMachineGenerator,
             followingCacheMachineGenerator,
             inputCacheMachineGenerator) = register_kernel_overlap_generator_with_cache_machines(
            overlap_generator_kernel,
            context);

    // run function in overlap generator
    std::thread run_thread_generator = std::thread([overlap_generator_kernel]() {
        kstatus process = overlap_generator_kernel->run();
        EXPECT_EQ(kstatus::proceed, process);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // add data into the Generator CacheMachines
    for (std::size_t i = 0; i < inputCacheData.size(); i++) {
        inputCacheMachineGenerator->addCacheData(std::move(inputCacheData[i]));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    inputCacheMachineGenerator->finish();

    run_thread_generator.join();

    return {precedingCacheMachineGenerator->pull_all_cache_data(),
            batchesCacheMachineGenerator->pull_all_cache_data(),
            followingCacheMachineGenerator->pull_all_cache_data()};
}

std::vector<std::unique_ptr<CacheData>>
run_overlap_accumulator_kernel(const std::string& project_plan,
                               std::shared_ptr<Context> context,
                               std::vector<std::unique_ptr<CacheData>>& batchesCacheData,
                               std::vector<std::unique_ptr<CacheData>>& batchesPrecedingCacheData,
                               std::vector<std::unique_ptr<CacheData>>& batchesFollowingCacheData){

    // overlap Accumulator kernel
    std::shared_ptr<kernel> overlap_accumulator_kernel;
    std::shared_ptr<ral::cache::CacheMachine> input_accumulator_cache, output_accumulator_cache;
    std::tie(overlap_accumulator_kernel, input_accumulator_cache, output_accumulator_cache) = make_overlap_Accumulator_kernel(
            project_plan, context);

    // register cache machines with the kernel
    std::shared_ptr<CacheMachine>   batchesCacheMachineAccumulator,
            precedingCacheMachineAccumulator,
            followingCacheMachineAccumulator,
            outputCacheMachineAccumulator;
    std::tie(batchesCacheMachineAccumulator,
             precedingCacheMachineAccumulator,
             followingCacheMachineAccumulator,
             outputCacheMachineAccumulator) = register_kernel_overlap_accumulator_with_cache_machines(
            overlap_accumulator_kernel,
            context);

    // run function in overlap accumulator
    std::thread run_thread_accumulator = std::thread([overlap_accumulator_kernel](){
        kstatus process = overlap_accumulator_kernel->run();
        EXPECT_EQ(kstatus::proceed, process);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // add data into the Accumulator CacheMachines
    for (std::size_t i = 0; i < batchesCacheData.size(); i++) {
        batchesCacheMachineAccumulator->addCacheData(std::move(batchesCacheData[i]));
        if (i != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            precedingCacheMachineAccumulator->addCacheData(std::move(batchesPrecedingCacheData[i - 1]));
        }
        if (i != batchesCacheData.size() - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            followingCacheMachineAccumulator->addCacheData(std::move(batchesFollowingCacheData[i]));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    batchesCacheMachineAccumulator->finish();
    precedingCacheMachineAccumulator->finish();
    followingCacheMachineAccumulator->finish();

    run_thread_accumulator.join();

    // get and validate output
    return outputCacheMachineAccumulator->pull_all_cache_data();
}

TEST_F(WindowOverlapGeneratorTest, BasicSingleNode) {

    size_t size = 100000;

    // Define full dataset
    auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
    auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
    auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
    auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });

    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);

    CudfTableView full_data_cudf_view ({col0, col1, col2});

    int preceding_value = 50;
    int following_value = 10;
    std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size

    // define how its broken up into batches and overlaps
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
    std::vector<std::unique_ptr<CacheData>>& inputCacheData = batches;
    std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

    std::vector<std::unique_ptr<BlazingTable>> expected_batch_out = make_expected_generator_output(full_data_cudf_view,
                                                                                                   batch_sizes, names);

    // create and start kernel
    // Context
    std::shared_ptr<Context> context = make_context(1);

    auto & communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize("0", "/tmp");

    std::vector<std::unique_ptr<CacheData>> batches_preceding_pulled,
                                            batches_pulled,
                                            batches_following_pulled;

    std::string project_plan = "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])";

    std::tie(batches_preceding_pulled,
             batches_pulled,
             batches_following_pulled)
            = run_overlap_generator_kernel(project_plan,
                                           context,
                                           inputCacheData);

    EXPECT_EQ(batches_preceding_pulled.size(), preceding_overlaps.size());
    EXPECT_EQ(batches_pulled.size(),           expected_batch_out.size());
    EXPECT_EQ(batches_following_pulled.size(), following_overlaps.size());

    //Validate metadata
    for (const auto &batch : batches_preceding_pulled){
        ral::cache::MetadataDictionary metadata = batch->getMetadata();
        EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
    }

    for (const auto &batch : batches_following_pulled){
        ral::cache::MetadataDictionary metadata = batch->getMetadata();
        EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
    }

    //Validate the data
    for (std::size_t i = 0; i < batches_pulled.size(); i++) {
        if (i < batches_preceding_pulled.size()) {
            auto table_out_preceding    = batches_preceding_pulled[i]->decache();
            auto expected_out_preceding = preceding_overlaps[i]->decache();

            cudf::test::expect_tables_equivalent(expected_out_preceding->view(), table_out_preceding->view());
        }

        if (i < batches_following_pulled.size()) {
            auto table_out_following    = batches_following_pulled[i]->decache();
            auto expected_out_following = following_overlaps[i]->decache();

            cudf::test::expect_tables_equivalent(expected_out_following->view(), table_out_following->view());
        }

        auto table_out_batches = batches_pulled[i]->decache();
        cudf::test::expect_tables_equivalent(expected_batch_out[i]->view(), table_out_batches->view());
    }
}

TEST_F(WindowOverlapGeneratorTest, BigWindowSingleNode) {

    size_t size = 14600;

    // Define full dataset
    auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
    auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
    auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
    auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });

    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);

    CudfTableView full_data_cudf_view ({col0, col1, col2});
    // CudfTableView full_data_cudf_view ({col0});

    int preceding_value = 1500;
    int following_value = 3000;
    std::vector<cudf::size_type> batch_sizes = {5500, 1100, 1200, 1300, 5500}; // need to sum up to size

    // define how its broken up into batches and overlaps
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
    std::vector<std::unique_ptr<CacheData>>& inputCacheData = batches;
    std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(
            full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

    std::vector<std::unique_ptr<BlazingTable>> expected_batch_out = make_expected_generator_output(full_data_cudf_view,
                                                                                                   batch_sizes, names);

    // create and start kernel
    // Context
    std::shared_ptr<Context> context = make_context(1);

    auto & communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize("0", "/tmp");

    std::vector<std::unique_ptr<CacheData>> batches_preceding_pulled,
            batches_pulled,
            batches_following_pulled;

    std::string project_plan = "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 1500 PRECEDING AND 3000 FOLLOWING)])";

    std::tie(batches_preceding_pulled,
             batches_pulled,
             batches_following_pulled)
            = run_overlap_generator_kernel(project_plan,
                                           context,
                                           inputCacheData);

    EXPECT_EQ(batches_preceding_pulled.size(), preceding_overlaps.size());
    EXPECT_EQ(batches_pulled.size(),           expected_batch_out.size());
    EXPECT_EQ(batches_following_pulled.size(), following_overlaps.size());

    //Validate metadata
    for (std::size_t i = 0; i < batches_preceding_pulled.size(); ++i) {
        ral::cache::MetadataDictionary metadata = batches_preceding_pulled[i]->getMetadata();
        if (i == 0) {
            EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
        } else {
            EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::INCOMPLETE_OVERLAP_STATUS);
        }
    }

    for (std::size_t i = 0; i < batches_following_pulled.size(); ++i) {
        ral::cache::MetadataDictionary metadata = batches_following_pulled[i]->getMetadata();
        if (i == batches_following_pulled.size() - 1) {
            EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::DONE_OVERLAP_STATUS);
        } else {
            EXPECT_EQ(metadata.get_value(ral::cache::OVERLAP_STATUS), ral::batch::INCOMPLETE_OVERLAP_STATUS);
        }
    }

    //Validate the data
    for (std::size_t i = 0; i < batches_pulled.size(); i++) {
        if (i < batches_preceding_pulled.size()) {
            auto table_out_preceding    = batches_preceding_pulled[i]->decache();
            auto expected_out_preceding = preceding_overlaps[i]->decache();

            cudf::test::expect_tables_equivalent(expected_out_preceding->view(), table_out_preceding->view());
        }

        if (i < batches_following_pulled.size()) {
            auto table_out_following    = batches_following_pulled[i]->decache();
            auto expected_out_following = following_overlaps[i]->decache();

            cudf::test::expect_tables_equivalent(expected_out_following->view(), table_out_following->view());
        }

        auto table_out_batches = batches_pulled[i]->decache();
        cudf::test::expect_tables_equivalent(expected_batch_out[i]->view(), table_out_batches->view());
    }
}

TEST_F(WindowOverlapTest, BasicSingleNode) {

    size_t size = 100000;

    // Define full dataset
    auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
    auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
    auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
    auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });

    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);

    CudfTableView full_data_cudf_view ({col0, col1, col2});

    int preceding_value = 50;
    int following_value = 10;
    std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size

    // define how its broken up into batches and overlaps
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
    std::vector<std::unique_ptr<CacheData>>& inputCacheData = batches;

    std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

    std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);

    // create and start kernel
    // Context
    std::shared_ptr<Context> context = make_context(1);

    auto & communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize("0", "/tmp");

    std::vector<std::unique_ptr<CacheData>> batches_preceding_pulled,
                                            batches_pulled,
                                            batches_following_pulled;

    std::string project_plan = "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])";

    std::tie(batches_preceding_pulled,
             batches_pulled,
             batches_following_pulled)
            = run_overlap_generator_kernel(project_plan,
                                           context,
                                           inputCacheData);

    std::vector<std::unique_ptr<CacheData>> last_batches_pulled = run_overlap_accumulator_kernel(project_plan,
                                                                                                 context,
                                                                                                 batches_pulled,
                                                                                                 batches_preceding_pulled,
                                                                                                 batches_following_pulled);

    EXPECT_EQ(last_batches_pulled.size(), expected_out.size());

    for (std::size_t i = 0; i < last_batches_pulled.size(); i++) {
        auto table_out = last_batches_pulled[i]->decache();
        cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
    }
}

TEST_F(WindowOverlapTest, BigWindowSingleNode) {

    size_t size = 14600;

    // Define full dataset
    auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
    auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
    auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
    auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });

    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);

    CudfTableView full_data_cudf_view ({col0, col1, col2});
    // CudfTableView full_data_cudf_view ({col0});

    int preceding_value = 1500;
    int following_value = 3000;
    std::vector<cudf::size_type> batch_sizes = {5500, 1100, 1200, 1300, 5500}; // need to sum up to size

    // define how its broken up into batches and overlaps
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
    std::vector<std::unique_ptr<CacheData>>& inputCacheData = batches;

    std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

    std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);

    // create and start kernel
    // Context
    std::shared_ptr<Context> context = make_context(1);

    auto & communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize("0", "/tmp");

    std::vector<std::unique_ptr<CacheData>> batches_preceding_pulled,
            batches_pulled,
            batches_following_pulled;

    std::string project_plan = "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 1500 PRECEDING AND 3000 FOLLOWING)])";

    std::tie(batches_preceding_pulled,
             batches_pulled,
             batches_following_pulled)
            = run_overlap_generator_kernel(project_plan,
                                           context,
                                           inputCacheData);

    std::vector<std::unique_ptr<CacheData>> last_batches_pulled = run_overlap_accumulator_kernel(project_plan,
                                                                                                 context,
                                                                                                 batches_pulled,
                                                                                                 batches_preceding_pulled,
                                                                                                 batches_following_pulled);

    EXPECT_EQ(last_batches_pulled.size(), expected_out.size());

    for (std::size_t i = 0; i < last_batches_pulled.size(); i++) {
        auto table_out = last_batches_pulled[i]->decache();
        cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
    }
}

TEST_F(WindowOverlapTest, BasicSingleNode2) {

    size_t size = 100000;

    // Define full dataset
    auto iter0 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i);});
    auto iter1 = cudf::detail::make_counting_transform_iterator(1000, [](auto i) { return int32_t(i * 2);});
    auto iter2 = cudf::detail::make_counting_transform_iterator(0, [](auto i) { return int32_t(i % 5);});
    auto valids_iter = cudf::detail::make_counting_transform_iterator(0, [](auto /*i*/) { return true; });

    cudf::test::fixed_width_column_wrapper<int32_t> col0(iter0, iter0 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col1(iter1, iter1 + size, valids_iter);
    cudf::test::fixed_width_column_wrapper<int32_t> col2(iter2, iter2 + size, valids_iter);

    CudfTableView full_data_cudf_view ({col0, col1, col2});

    int preceding_value = 50;
    int following_value = 10;
    std::vector<cudf::size_type> batch_sizes = {5000, 50000, 20000, 15000, 10000}; // need to sum up to size

    // define how its broken up into batches and overlaps
    std::vector<std::string> names({"A", "B", "C"});
    std::vector<std::unique_ptr<CacheData>> preceding_overlaps, batches, following_overlaps;
    std::vector<std::unique_ptr<CacheData>>& inputCacheData = batches;

    std::tie(preceding_overlaps, batches, following_overlaps) = break_up_full_data(full_data_cudf_view, preceding_value, following_value, batch_sizes, names);

    std::vector<std::unique_ptr<BlazingTable>> expected_out = make_expected_accumulator_output(full_data_cudf_view,
                                                                                               preceding_value,
                                                                                               following_value,
                                                                                               batch_sizes, names);

    // create and start kernel
    // Context
    std::shared_ptr<Context> context = make_context(1);

    auto & communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize("0", "/tmp");

    std::string project_plan = "LogicalProject(min_val=[MIN($0) OVER (ORDER BY $1 ROWS BETWEEN 50 PRECEDING AND 10 FOLLOWING)])";

    // overlap Generator kernel
    std::shared_ptr<kernel> overlap_generator_kernel;
    std::shared_ptr<ral::cache::CacheMachine> input_generator_cache, output_generator_cache;
    std::tie(overlap_generator_kernel, input_generator_cache, output_generator_cache) = make_overlap_Generator_kernel(project_plan, context);

    std::shared_ptr<kernel> overlap_accumulator_kernel;
    std::shared_ptr<ral::cache::CacheMachine> input_accumulator_cache, output_accumulator_cache;
    std::tie(overlap_accumulator_kernel, input_accumulator_cache, output_accumulator_cache) = make_overlap_Accumulator_kernel(project_plan, context);


    std::shared_ptr<CacheMachine> batchesCacheMachine   = std::make_shared<CacheMachine>(context, "batches");
    std::shared_ptr<CacheMachine> precedingCacheMachine = std::make_shared<CacheMachine>(context, "preceding_overlaps");
    std::shared_ptr<CacheMachine> followingCacheMachine = std::make_shared<CacheMachine>(context, "following_overlaps");
    std::shared_ptr<CacheMachine> inputCacheMachine     = std::make_shared<CacheMachine>(context, "1");
    std::shared_ptr<CacheMachine> outputCacheMachine    = std::make_shared<CacheMachine>(context, "1");

    overlap_generator_kernel->input_.register_cache("1", inputCacheMachine);
    overlap_generator_kernel->output_.register_cache("batches", batchesCacheMachine);
    overlap_generator_kernel->output_.register_cache("preceding_overlaps", precedingCacheMachine);
    overlap_generator_kernel->output_.register_cache("following_overlaps", followingCacheMachine);

    overlap_accumulator_kernel->input_.register_cache("batches", batchesCacheMachine);
    overlap_accumulator_kernel->input_.register_cache("preceding_overlaps", precedingCacheMachine);
    overlap_accumulator_kernel->input_.register_cache("following_overlaps", followingCacheMachine);
    overlap_accumulator_kernel->output_.register_cache("1", outputCacheMachine);


    // run function in overlap generator
    std::thread run_thread_generator = std::thread([overlap_generator_kernel]() {
        kstatus process = overlap_generator_kernel->run();
        EXPECT_EQ(kstatus::proceed, process);
    });

    // run function in overlap accumulator
    std::thread run_thread_accumulator = std::thread([overlap_accumulator_kernel](){
        kstatus process = overlap_accumulator_kernel->run();
        EXPECT_EQ(kstatus::proceed, process);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // add data into the Generator CacheMachines
    for (std::size_t i = 0; i < inputCacheData.size(); i++) {
        inputCacheMachine->addCacheData(std::move(inputCacheData[i]));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    inputCacheMachine->finish();

	std::this_thread::sleep_for(std::chrono::milliseconds(500));

    batchesCacheMachine->finish();
    precedingCacheMachine->finish();
    followingCacheMachine->finish();

	std::this_thread::sleep_for(std::chrono::milliseconds(500));
    outputCacheMachine->finish();

    run_thread_generator.join();
    run_thread_accumulator.join();

    // get and validate output
    auto last_batches_pulled = outputCacheMachine->pull_all_cache_data();

    EXPECT_EQ(last_batches_pulled.size(), expected_out.size());

    for (std::size_t i = 0; i < last_batches_pulled.size(); i++) {
        auto table_out = last_batches_pulled[i]->decache();
        cudf::test::expect_tables_equivalent(expected_out[i]->view(), table_out->view());
    }
}