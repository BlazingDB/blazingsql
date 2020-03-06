
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <src/execution_graph/logic_controllers/LogicalFilter.h>
#include <src/from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <src/utilities/DebuggingUtils.h>
#include <stack>
#include <thrust/sequence.h>


using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;

struct CacheMachineTest : public cudf::test::BaseFixture {
	CacheMachineTest() {}

	~CacheMachineTest() {}
};

template<class TypeParam>
std::unique_ptr<cudf::column> make_col(cudf::size_type size) {
	thrust::device_vector<TypeParam> d_integers(size);
	thrust::sequence( thrust::device, d_integers.begin(), d_integers.end());
	cudf::mask_state state = cudf::mask_state::ALL_VALID;

	auto integers = cudf::make_numeric_column(cudf::data_type{cudf::experimental::type_to_id<TypeParam>()}, size, state);
	auto integers_view = integers->mutable_view();
	cudaMemcpy( integers_view.data<TypeParam>(), d_integers.data().get(), size * sizeof(TypeParam), cudaMemcpyDeviceToDevice );
	return integers;
}

std::unique_ptr<ral::frame::BlazingTable> build_custom_table() {
	cudf::size_type size = 10;

	auto num_column_1 = make_col<int32_t>(size);
	auto num_column_2 = make_col<int64_t>(size);
	auto num_column_3 = make_col<float>(size);
	auto num_column_4 = make_col<double>(size);

	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	columns.push_back(std::move(num_column_2));
	columns.push_back(std::move(num_column_3));
	columns.push_back(std::move(num_column_4));

	cudf::test::strings_column_wrapper col2({"d", "e", "a", "d", "k", "d", "l", "a", "b", "c"}, {1, 0, 1, 1, 1, 1, 1, 1, 0, 1});

	std::unique_ptr<cudf::column> str_col = std::make_unique<cudf::column>(std::move(col2));
	columns.push_back(std::move(str_col));

	std::vector<std::string> column_names = {"INT64", "INT32", "FLOAT64", "FLOAT32", "STRING"};

	auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));
}


std::unique_ptr<ral::frame::BlazingTable>  build_custom_one_column_table() {
	cudf::size_type size = 10;

	auto num_column_1 = make_col<int32_t>(size);
	std::vector<std::unique_ptr<cudf::column>> columns;
	columns.push_back(std::move(num_column_1));
	std::vector<std::string> column_names = {"INT64"};

	auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(ral::frame::BlazingTable(std::move(table), column_names));

}

TEST_F(CacheMachineTest, CacheMachineTest) {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	ral::cache::CacheMachine cacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes);

	for(int i = 0; i < 10; ++i) {
		auto table = build_custom_table();
		std::cout << ">> " << i << "|" << table->sizeInBytes() << std::endl;
		cacheMachine.addToCache(std::move(table));
		if(i % 5 == 0) {
			auto cacheTable = cacheMachine.pullFromCache();
		}
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

std::shared_ptr<ral::cache::CacheMachine> createSourceCacheMachine() {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	auto source = std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
	auto table = build_custom_table();
	source->addToCache(std::move(table));
	return source;
}

std::shared_ptr<ral::cache::CacheMachine> createSinkCacheMachine() {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	return std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
}

TEST_F(CacheMachineTest, FilterTest) {
	using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & table,
		const std::string & query_part,
		blazingdb::manager::experimental::Context * context);

	std::shared_ptr<ral::cache::CacheMachine> cacheSource = createSourceCacheMachine();
	std::shared_ptr<ral::cache::CacheMachine> cacheSink = createSinkCacheMachine();
	ProcessorFunctor * process_project = &ral::processor::process_filter;
	std::string queryString = "BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])";
	blazingdb::manager::experimental::Context * context = nullptr;
	int numWorkers = 1;
	ral::cache::ProcessMachine<ProcessorFunctor> processor(
		cacheSource, cacheSink, process_project, queryString, context, numWorkers);

	std::cout << ">> processor.run()\n";
	cacheSource->finish();
	processor.run();
	std::cout << "<<> processor.run()\n";
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

/*TEST_F(CacheMachineTest, ProjectTest) {
	using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(const ral::frame::BlazingTableView & table,
		const std::string & query_part,
		blazingdb::manager::experimental::Context * context);

	std::shared_ptr<ral::cache::CacheMachine> cacheSource = createSourceCacheMachine();
	std::shared_ptr<ral::cache::CacheMachine> cacheSink = createSinkCacheMachine();
	ProcessorFunctor * process_project = &ral::processor::process_project;
	std::string queryString = "LogicalProject(INT64=[$0], INT32=[$1], FLOAT64=[$2])";
	blazingdb::manager::experimental::Context * context = nullptr;
	int numWorkers = 1;
	ral::cache::ProcessMachine<ProcessorFunctor> processor(
		cacheSource, cacheSink, process_project, queryString, context, numWorkers);

	std::cout << ">> processor.run()\n";
	cacheSource->finish();
	processor.run();
	std::cout << "<<> processor.run()\n";
	std::this_thread::sleep_for(std::chrono::seconds(1));
}*/

std::shared_ptr<ral::cache::CacheMachine> createSourceCacheMachineOneColumn() {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	auto source = std::make_shared<ral::cache::CacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
	auto table = build_custom_one_column_table();
	source->addToCache(std::move(table));
	return source;
}


TEST_F(CacheMachineTest, LogicalJoinTest) {
	using ProcessorFunctor = std::unique_ptr<ral::frame::BlazingTable>(blazingdb::manager::experimental::Context *,
		const ral::frame::BlazingTableView &,
		const ral::frame::BlazingTableView &,
		const std::string &);

	std::shared_ptr<ral::cache::CacheMachine> cacheLeftSource = createSourceCacheMachineOneColumn();
	std::shared_ptr<ral::cache::CacheMachine> cacheRightSource = createSourceCacheMachineOneColumn();
	std::shared_ptr<ral::cache::CacheMachine> cacheSink = createSinkCacheMachine();
	ProcessorFunctor * process_project = &ral::processor::process_logical_join;

	std::string queryString = "LogicalJoin(condition=[=($1, $0)], joinType=[inner])";
	using blazingdb::manager::experimental::Context;
	using blazingdb::transport::experimental::Address;
	using blazingdb::transport::experimental::Node;
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	int numWorkers = 1;

	ral::cache::ProcessMachine<ProcessorFunctor> processor(
		cacheLeftSource, cacheRightSource, cacheSink, process_project, queryString, &queryContext, numWorkers);

	std::cout << ">> processor.run()\n";
	cacheLeftSource->finish();
	cacheRightSource->finish();
	processor.run();
	std::cout << "<<> processor.run()\n";
	std::this_thread::sleep_for(std::chrono::seconds(1));
}
