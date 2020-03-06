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
#include <src/io/data_parser/CSVParser.h>
#include <src/io/data_parser/ParquetParser.h>
#include <src/io/data_provider/UriDataProvider.h>

using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
namespace ral {
namespace cache {

struct GraphProcessorTest : public cudf::test::BaseFixture {
	using data_provider_pair =
		std::pair<std::shared_ptr<ral::io::csv_parser>, std::shared_ptr<ral::io::uri_data_provider>>;

	data_provider_pair CreateCustomerTableProvider(int index = 0) {
		const std::string content =
		R"(1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e
		2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref
		3|Customer#000000003|MG9kdTD2WBHm|1|11-719-748-3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov
		4|Customer#000000004|XxVSJsLAGtn|4|14-128-190-5944|2866.83|MACHINERY| requests. final, regular ideas sleep final accou
		5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-750-942-6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole accor
		6|Customer#000000006|sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn|20|30-114-968-4951|7638.57|AUTOMOBILE|tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious
		7|Customer#000000007|TcGe5gaZNgVePxU5kRrvXBfkasDTea|18|28-190-982-9759|9561.95|AUTOMOBILE|ainst the ironic, express theodolites. express, even pinto beans among the exp
		8|Customer#000000008|I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5|17|27-147-574-9335|6819.74|BUILDING|among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide
		9|Customer#000000009|xKiAFTjUsCuxfeleNqefumTrjS|8|18-338-906-3675|8324.07|FURNITURE|r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl
		10|Customer#000000010|6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2|5|15-741-346-9870|2753.54|HOUSEHOLD|es regular deposits haggle. fur
		11|Customer#000000011|PkWS 3HlXqwTuzrKg633BEi|23|33-464-151-3439|-272.6|BUILDING|ckages. requests sleep slyly. quickly even pinto beans promise above the slyly regular pinto beans.
		12|Customer#000000012|9PWKuhzT4Zr1Q|13|23-791-276-1263|3396.49|HOUSEHOLD| to the carefully final braids. blithely regular requests nag. ironic theodolites boost quickly along
		13|Customer#000000013|nsXQu0oVjD7PM659uC3SRSp|3|13-761-547-5974|3857.34|BUILDING|ounts sleep carefully after the close frays. carefully bold notornis use ironic requests. blithely
		14|Customer#000000014|KXkletMlL2JQEA |1|11-845-129-3851|5266.3|FURNITURE|, ironic packages across the unus
		15|Customer#000000015|YtWggXoOLdwdo7b0y,BZaGUQMLJMX1Y,EC,6Dn|23|33-687-542-7601|2788.52|HOUSEHOLD| platelets. regular deposits detect asymptotes. blithely unusual packages nag slyly at the fluf
		16|Customer#000000016|cYiaeMLZSMAOQ2 d0W,|10|20-781-609-3107|4681.03|FURNITURE|kly silent courts. thinly regular theodolites sleep fluffily after
		17|Customer#000000017|izrh 6jdqtp2eqdtbkswDD8SG4SzXruMfIXyR7|2|12-970-682-3487|6.34|AUTOMOBILE|packages wake! blithely even pint
		18|Customer#000000018|3txGO AiuFux3zT0Z9NYaFRnZt|6|16-155-215-1315|5494.43|BUILDING|s sleep. carefully even instructions nag furiously alongside of t
		19|Customer#000000019|uc,3bHIx84H,wdrmLOjVsiqXCq2tr|18|28-396-526-5053|8914.71|HOUSEHOLD| nag. furiously careful packages are slyly at the accounts. furiously regular in
		20|Customer#000000020|JrPk8Pqplj4Ne|22|32-957-234-8742|7603.4|FURNITURE|g alongside of the special excuses-- fluffily enticing packages wake)";

		std::string filename = "/tmp/customer_" + std::to_string(index) + ".psv";
		std::ofstream outfile(filename, std::ofstream::out);
		outfile << content << std::endl;
		outfile.close();

		std::vector<std::pair<std::string, std::string>> customer_map = {
			{"c_custkey", "int64"},
			{"c_name", "str"},
			{"c_address", "str"},
			{"c_nationkey", "int64"},
			{"c_phone", "str"},
			{"c_acctbal", "float64"},
			{"c_mktsegment", "str"},
			{"c_comment", "str"}};
		std::vector<std::string> col_names;
		std::vector<std::string> dtypes;
		for(auto pair : customer_map) {
			col_names.push_back(pair.first);
			dtypes.push_back(pair.second);
		}
		cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
		in_args.names = col_names;
		in_args.dtype = dtypes;
		in_args.delimiter = '|';
		in_args.header = -1;

		std::vector<Uri> uris;
		uris.push_back(Uri{filename});

		auto parser = std::make_shared<ral::io::csv_parser>(in_args);
		auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
		return std::make_pair(parser, provider);
	}

	data_provider_pair CreateOrderTableProvider(int index = 0) {
		const std::string content =
		R"(1|3691|O|194029.55|1996-01-02T00:00:00.000Z|5-LOW|Clerk#000000951|0|nstructions sleep furiously among
		2|7801|O|60951.63|1996-12-01T00:00:00.000Z|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot
		3|12332|F|247296.05|1993-10-14T00:00:00.000Z|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos
		4|13678|O|53829.87|1995-10-11T00:00:00.000Z|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro
		5|4450|F|139660.54|1994-07-30T00:00:00.000Z|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly
		6|5563|F|65843.52|1992-02-21T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia
		7|3914|O|231037.28|1996-01-10T00:00:00.000Z|2-HIGH|Clerk#000000470|0|ly special requests
		32|13006|O|166802.63|1995-07-16T00:00:00.000Z|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep
		33|6697|F|118518.56|1993-10-27T00:00:00.000Z|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request
		34|6101|O|75662.77|1998-07-21T00:00:00.000Z|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe
		35|12760|O|192885.43|1995-10-23T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000259|0|zzle. carefully enticing deposits nag furio
		36|11527|O|72196.43|1995-11-03T00:00:00.000Z|1-URGENT|Clerk#000000358|0| quick packages are blithely. slyly silent accounts wake qu
		37|8612|F|156440.15|1992-06-03T00:00:00.000Z|3-MEDIUM|Clerk#000000456|0|kly regular pinto beans. carefully unusual waters cajole never
		38|12484|O|64695.26|1996-08-21T00:00:00.000Z|4-NOT SPECIFIED|Clerk#000000604|0|haggle blithely. furiously express ideas haggle blithely furiously regular re
		39|8177|O|307811.89|1996-09-20T00:00:00.000Z|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir
		64|3212|F|30616.9|1994-07-16T00:00:00.000Z|3-MEDIUM|Clerk#000000661|0|wake fluffily. sometimes ironic pinto beans about the dolphin
		65|1627|P|99763.79|1995-03-18T00:00:00.000Z|1-URGENT|Clerk#000000632|0|ular requests are blithely pending orbits-- even requests against the deposit)";

		std::string filename = "/tmp/orders_" + std::to_string(index) + ".psv";
		std::ofstream outfile(filename, std::ofstream::out);
		outfile << content << std::endl;
		outfile.close();

		std::vector<std::pair<std::string, std::string>> orders_map = {
			{"o_orderkey", "int64"},
			{"o_custkey", "int64"},
			{"o_orderstatus", "str"},
			{"o_totalprice", "float64"},
			{"o_orderdatetime64", "str"},
			{"o_orderpriority", "str"},
			{"o_clerk", "str"},
			{"o_shippriority", "str"},
			{"o_comment", "str"}};

		std::vector<std::string> col_names;
		std::vector<std::string> dtypes;
		for(auto pair : orders_map) {
			col_names.push_back(pair.first);
			dtypes.push_back(pair.second);
		}
		cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
		in_args.names = col_names;
		in_args.dtype = dtypes;
		in_args.delimiter = '|';
		in_args.header = -1;

		std::vector<Uri> uris;
		uris.push_back(Uri{filename});

		auto parser = std::make_shared<ral::io::csv_parser>(in_args);
		auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
		return std::make_pair(parser, provider);
	}

	GraphProcessorTest() {}

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

	auto orders_input = this->CreateOrderTableProvider();
	auto customer_input = this->CreateCustomerTableProvider();

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

	auto customer_input = this->CreateCustomerTableProvider();

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

	auto customer_input = this->CreateCustomerTableProvider();

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

TEST_F(GraphProcessorTest, GroupSamplePartitionTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");

	auto customer_input = this->CreateCustomerTableProvider();

	ral::io::data_loader customer_loader(customer_input.first, customer_input.second);
	ral::io::Schema customer_schema;
	customer_loader.get_schema(customer_schema, {});

	TableScanKernel customer_generator(customer_loader, customer_schema, queryContext);

	AggregateAndSampleKernel aggregate_and_sample("Logical_AggregateAndSample(group=[{}], EXPR$0=[SUM($1)])", queryContext);
	AggregatePartitionKernel partition("LogicalAggregatePartition(group=[{}], EXPR$0=[SUM($1)])", queryContext);
	AggregateMergeStreamKernel merge("LogicalAggregateMerge(group=[{}], EXPR$0=[SUM($1)])", queryContext);
	ProjectKernel project("LogicalProject(c_nationkey=[$3], c_custkey=[$0])", queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 30)])", queryContext);
	PrinterKernel print;
	ral::cache::graph m;
	try {
		auto cache_machine_config =
			cache_settings{.type = CacheType::FOR_EACH, .num_partitions = queryContext->getTotalNodes()};
		m += customer_generator >> filter;
		m += filter >> project;
		m += project >> aggregate_and_sample;
		m += aggregate_and_sample["output_a"] >> partition["input_a"];
		m += aggregate_and_sample["output_b"] >> partition["input_b"];
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
