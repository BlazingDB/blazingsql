
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "io/DataLoader.h"
#include <boost/property_tree/json_parser.hpp>
#include <tests/utilities/base_fixture.hpp>
#include <src/io/data_parser/CSVParser.h>
#include <src/io/data_parser/ParquetParser.h>
#include <src/io/data_provider/UriDataProvider.h>
#include <execution_graph/logic_controllers/PhysicalPlanGenerator.h>
#include "../BlazingUnitTest.h"
#include "generators/file_generator.h"

using blazingdb::manager::Context;
using blazingdb::transport::Address;
using blazingdb::transport::Node;
struct Batching : public BlazingUnitTest {
	Batching() {

	}
	~Batching() {}
};

namespace ral {
namespace batch {

TEST_F(Batching, SimpleQuery) {

	std::string json = R"(
	{
		'expr': 'LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2])',
		'children': [
			{
				'expr': 'LogicalFilter(condition=[<($0, 10)])',
				'children': [
					{
						'expr': 'LogicalTableScan(table=[[main, nation]])',
						'children': []
					}
				]
			}
		]
	}
	)";

	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "", std::map<std::string, std::string>());

	auto n_batches = 5;
	std::shared_ptr<ral::io::parquet_parser> parser;
	std::shared_ptr<ral::io::uri_data_provider> provider;
	ral::io::Schema schema;
	std::tie(parser, provider, schema) = blazingdb::test::CreateParquetNationTableProvider(queryContext.get(), n_batches);
	
	ral::io::data_loader loader(parser, provider);

	tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	Print print;
	std::tuple<std::shared_ptr<ral::cache::graph>,std::size_t> graph_tuple = tree.build_batch_graph(json);
	std::shared_ptr<ral::cache::graph> graph = std::get<0>(graph_tuple);
	try {
		ral::cache::cache_settings simple_cache_config{.type = ral::cache::CacheType::SIMPLE};
		*graph += link(graph->get_last_kernel(), print, simple_cache_config);
		graph->execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

TEST_F(Batching, BindableQuery) {

	std::string json = R"(
	{
		'expr': 'BindableTableScan(table=[[main, nation]], filters=[[<($0, 10)]], projects=[[0, 1, 2]], aliases=[[n_nationkey, n_name, n_regionkey]])',
		'children': []
	}
	)";

	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "", std::map<std::string, std::string>());

	auto n_batches = 5;
	std::shared_ptr<ral::io::parquet_parser> parser;
	std::shared_ptr<ral::io::uri_data_provider> provider;
	ral::io::Schema schema;
	std::tie(parser, provider, schema) = blazingdb::test::CreateParquetNationTableProvider(queryContext.get(), n_batches);

	ral::io::data_loader loader(parser, provider);

	tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	Print print;
	std::tuple<std::shared_ptr<ral::cache::graph>,std::size_t> graph_tuple = tree.build_batch_graph(json);
	std::shared_ptr<ral::cache::graph> graph = std::get<0>(graph_tuple);	
	try {
		ral::cache::cache_settings simple_cache_config{.type = ral::cache::CacheType::SIMPLE};
		*graph += link(graph->get_last_kernel(), print, simple_cache_config);
		graph->execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

TEST_F(Batching, SortSamplePartitionTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "", std::map<std::string, std::string>());

	auto n_batches = 5;
	std::shared_ptr<ral::io::parquet_parser> parser;
	std::shared_ptr<ral::io::uri_data_provider> provider;
	ral::io::Schema schema;
	std::tie(parser, provider, schema) = blazingdb::test::CreateParquetOrderTableProvider(queryContext.get(), n_batches);

	ral::io::data_loader loader(parser, provider);

	std::shared_ptr<ral::cache::graph> query_graph = std::make_shared<ral::cache::graph>();
	TableScan customer_generator(0,"", loader, schema, queryContext, query_graph);

	SortAndSampleKernel sort_and_sample(1, "Logical_SortAndSample(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, query_graph);
	PartitionSingleNodeKernel partition(2, "LogicalPartition(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, query_graph);
	MergeStreamKernel merge(3, "LogicalMerge(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, query_graph);

	Projection project(4, "LogicalProject(c_custkey=[$0], c_nationkey=[$3])", queryContext, query_graph);
	Filter filter(5, "LogicalFilter(condition=[<($0, 100)])", queryContext, query_graph);
	Print print;
	ral::cache::graph m;
	try {
		auto cache_machine_config =
			ral::cache::cache_settings{.type = ral::cache::CacheType::FOR_EACH, .num_partitions = 32};
		m += customer_generator >> filter;
		m += filter >> project;
		m += project >> sort_and_sample;
		m += sort_and_sample["output_a"] >> partition["input_a"];
		m += sort_and_sample["output_b"] >> partition["input_b"];
		m += link(partition, merge, cache_machine_config);
		m += link(merge, print, ral::cache::cache_settings{.type = ral::cache::CacheType::CONCATENATING});
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

}  // namespace batch
}  // namespace ral
