#include "execution_graph/logic_controllers/LogicalProject.h"
#include "utilities/random_generator.cuh"
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cudf/cudf.h>
#include <cudf/io/functions.hpp>
#include <cudf/types.hpp>
#include <execution_graph/logic_controllers/TaskFlowProcessor.h>
#include <src/from_cudf/cpp_tests/utilities/base_fixture.hpp>

namespace ral {
namespace cache {
	
struct GraphProcessorTest : public cudf::test::BaseFixture {
	GraphProcessorTest() {}

	~GraphProcessorTest() {}
}; 

std::shared_ptr<ral::cache::CacheMachine> create_cache_concatenating_machine() {
	unsigned long long gpuMemory = 1024;
	std::vector<unsigned long long> memoryPerCache = {INT_MAX};
	std::vector<ral::cache::CacheDataType> cachePolicyTypes = {ral::cache::CacheDataType::LOCAL_FILE};
	return std::make_shared<ral::cache::ConcatenatingCacheMachine>(gpuMemory, memoryPerCache, cachePolicyTypes);
}

TEST_F(GraphProcessorTest, JoinWorkFlowTest) {
	GeneratorKernel a(10), b(10);

	std::string expression = "LogicalJoin(condition=[=($1, $0)], joinType=[inner])";
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};
	JoinKernel s(expression, &queryContext);
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
TEST_F(GraphProcessorTest, ComplexWorkFlowTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	GeneratorKernel a(10), b(10);
	FilterKernel filterA("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", &queryContext);
	FilterKernel filterB("BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]])", &queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $0)], joinType=[inner])", &queryContext);
	ProjectKernel project("LogicalProject(INT64=[$0])", &queryContext);

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



//sql: select c_custkey, c_nationkey, c_acctbal from orders as o inner join customer as c on o.o_custkey = c.c_custkey where o.o_orderkey < 100

//# LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])
//#   LogicalFilter(condition=[<($0, 100)])
//#     LogicalJoin(condition=[=($1, $9)], joinType=[inner])
//#       LogicalTableScan(table=[[main, orders]])
//#       LogicalTableScan(table=[[main, customer]])

//# DEBUG: com.blazingdb.calcite.application.RelationalAlgebraGenerator - optimized
//# LogicalProject(c_custkey=[$1], c_nationkey=[$2], c_acctbal=[$3])
//#   LogicalJoin(condition=[=($0, $1)], joinType=[inner])
//#     LogicalProject(o_custkey=[$1])
//#       BindableTableScan(table=[[main, orders]], filters=[[<($0, 100)]], projects=[[0, 1]], aliases=[[$f0, o_custkey]])
//#     BindableTableScan(table=[[main, customer]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])
TEST_F(GraphProcessorTest, IOWorkFlowTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string folder_path = "/home/aocsa/tpch/100MB2Part/tpch/";
	int n_files = 1;
	std::vector<std::string> order_path_list;
	std::vector<std::string> customer_path_list;
	for (int index = 0; index < n_files; index++){
		auto filepath = folder_path + "orders_" + std::to_string(index) + "_0.parquet";
		order_path_list.push_back(filepath);
		filepath = folder_path + "customer_" + std::to_string(index) + "_0.parquet";
		customer_path_list.push_back(filepath);
	}

	TableScanKernel order_generator(order_path_list);
	TableScanKernel customer_generator(customer_path_list);
	FilterKernel filter("LogicalFilter(condition=[<($0, 100)])", &queryContext);
	JoinKernel join("LogicalJoin(condition=[=($1, $9)], joinType=[inner])", &queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])", &queryContext);

	PrinterKernel print;
	ral::cache::graph m;
	try {
		auto concatenating_machine_1 = create_cache_concatenating_machine();
		auto concatenating_machine_2 = create_cache_concatenating_machine();
		m += link(order_generator, join["input_a"], concatenating_machine_1);
		m += link(customer_generator, join["input_b"], concatenating_machine_2);
		m += join >> filter;
		m += filter >> project;
		m += project >> print;

		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

//select c_custkey, c_nationkey from customer where c_custkey < 10 order by c_nationkey, c_custkey

//LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])
//LogicalProject(c_custkey=[$0], c_nationkey=[$3])
//LogicalFilter(condition=[<($0, 10)])
//LogicalTableScan(table=[[main, customer]])
TEST_F(GraphProcessorTest, SortWorkFlowTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string folder_path = "/home/aocsa/tpch/100MB2Part/tpch/";
	int n_files = 1;
	std::vector<std::string> customer_path_list;
	for (int index = 0; index < n_files; index++) {
		auto filepath = folder_path + "customer_" + std::to_string(index) + "_0.parquet";
		customer_path_list.push_back(filepath);
	}
	TableScanKernel customer_generator(customer_path_list);
	SortKernel order_by("LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", &queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", &queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 10)])", &queryContext);
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

}  // namespace cache
}  // namespace ral
