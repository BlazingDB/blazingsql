#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/random_generator.cuh"
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/TaskFlowProcessor.h>
#include <src/from_cudf/cpp_tests/utilities/base_fixture.hpp>
#include <bmr/initializer.h>
#include "generators/file_generator.h"

using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
namespace ral {
namespace cache {

struct GraphProcessorTest  : public ::testing::Test  {

	GraphProcessorTest() {}

	void SetUp() {
		rmmInitialize(nullptr);
		BlazingRMMInitialize(nullptr);
	}


	~GraphProcessorTest() {}
};

TEST_F(GraphProcessorTest, JoinTest) {
	GeneratorKernel a(10), b(10);

	std::string expression = "LogicalJoin(condition=[=($1, $0)], joinType=[inner])";
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");
	JoinKernel s(expression, queryContext);
	PrinterKernel print;
	ral::cache::graph g;
	try {
		g += a >> s["input_a"];
		g += b >> s["input_b"];
		g += s >> print;
		g.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

// select $0 from a inner join b on a.$0 = b.$0 where a.$0 < 5 and where b.$0 < 5
TEST_F(GraphProcessorTest, ComplexTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");

	GeneratorKernel a(10), b(10);
	FilterKernel filterA("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", queryContext);
	FilterKernel filterB("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $0)], joinType=[inner])", queryContext);
	ProjectKernel project("LogicalProject(INT64=[$0])", queryContext);

	PrinterKernel print;
	ral::cache::graph m;
	try {
		m += a >> filterA;
		m += b >> filterB;
		m += filterA >> join["input_a"];
		m += filterB >> join["input_b"];
		m += join >> project;
		m += project >> print;
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}


// sql: select c_custkey, c_nationkey, c_acctbal from orders as o inner join customer as c on o.o_custkey = c.c_custkey
// where o.o_orderkey < 100

//# LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])
//#   LogicalFilter(condition=[<($0, 100)])
//#     LogicalJoin(condition=[=($1, $9)], joinType=[inner])
//#       LogicalTableScan(table=[[main, orders]])
//#       LogicalTableScan(table=[[main, customer]])

//# DEBUG: com.blazingdb.calcite.application.RelationalAlgebraGenerator - optimized
//# LogicalProject(c_custkey=[$1], c_nationkey=[$2], c_acctbal=[$3])
//#   LogicalJoin(condition=[=($0, $1)], joinType=[inner])
//#     LogicalProject(o_custkey=[$1])
//#       BindableTableScan(table=[[main, orders]], filters=[[<($0, 100)]], projects=[[0, 1]], aliases=[[$f0,
//o_custkey]]) #     BindableTableScan(table=[[main, customer]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey,
//c_acctbal]])
TEST_F(GraphProcessorTest, IOTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");

	auto orders_input = blazingdb::test::CreateCsvOrderTableProvider();
	auto customer_input = blazingdb::test::CreateCsvCustomerTableProvider();

	ral::io::data_loader orders_loader(orders_input.first, orders_input.second);
	ral::io::Schema orders_schema;
	orders_loader.get_schema(orders_schema, {});

	ral::io::data_loader customer_loader(customer_input.first, customer_input.second);
	ral::io::Schema customer_schema;
	customer_loader.get_schema(customer_schema, {});

	TableScanKernel order_generator(orders_loader, orders_schema, queryContext);
	TableScanKernel customer_generator(customer_loader, customer_schema, queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 100)])", queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $9)], joinType=[inner])", queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])", queryContext);

	PrinterKernel print;
	ral::cache::graph m;
	try {
		cache_settings concatenating_machine1{CacheType::CONCATENATING};
		cache_settings concatenating_machine2{CacheType::CONCATENATING};
		m += link(order_generator, join["input_a"], concatenating_machine1);
		m += link(customer_generator, join["input_b"], concatenating_machine2);
		m += join >> filter;
		m += filter >> project;
		m += project >> print;

		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

// select c_custkey, c_nationkey from customer where c_custkey < 10 order by c_nationkey, c_custkey

// LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])
// LogicalProject(c_custkey=[$0], c_nationkey=[$3])
// LogicalFilter(condition=[<($0, 10)])
// LogicalTableScan(table=[[main, customer]])
TEST_F(GraphProcessorTest, SortTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");

	auto customer_input = blazingdb::test::CreateCsvCustomerTableProvider();

	ral::io::data_loader customer_loader(customer_input.first, customer_input.second);
	ral::io::Schema customer_schema;
	customer_loader.get_schema(customer_schema, {});

	TableScanKernel customer_generator(customer_loader, customer_schema, queryContext);

	SortKernel order_by("LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 25)])", queryContext);
	PrinterKernel print;
	ral::cache::graph m;
	try {
		m += customer_generator >> filter;
		m += filter >> project;
		m += project >> order_by;
		m += order_by >> print;
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

TEST_F(GraphProcessorTest, SortSamplePartitionTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");

	auto customer_input = blazingdb::test::CreateCsvCustomerTableProvider();

	ral::io::data_loader customer_loader(customer_input.first, customer_input.second);
	ral::io::Schema customer_schema;
	customer_loader.get_schema(customer_schema, {});

	TableScanKernel customer_generator(customer_loader, customer_schema, queryContext);

	SortAndSampleKernel sort_and_sample("Logical_SortAndSample(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	PartitionKernel partition("LogicalPartition(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	MergeStreamKernel merge("LogicalMerge(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 25)])", queryContext);
	PrinterKernel print;
	ral::cache::graph m;
	try {
		auto cache_machine_config =
			cache_settings{.type = CacheType::FOR_EACH, .num_partitions = queryContext->getTotalNodes()};
		m += customer_generator >> filter;
		m += filter >> project;
		m += project >> sort_and_sample;
		m += sort_and_sample["output_a"] >> partition["input_a"];
		m += sort_and_sample["output_b"] >> partition["input_b"];
		m += link(partition, merge, cache_machine_config);
		m += link(merge, print, cache_settings{.type = CacheType::CONCATENATING});
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

}  // namespace cache
}  // namespace ral
