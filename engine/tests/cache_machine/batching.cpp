
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
	contextNodes.push_back(Node(address, ""));
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
	std::shared_ptr<ral::cache::graph> graph = tree.build_batch_graph(json);
	try {
		ral::cache::cache_settings simple_cache_config{.type = ral::cache::CacheType::SIMPLE};
		graph->addPair(kpair(graph->get_last_kernel(), print, simple_cache_config));
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
	contextNodes.push_back(Node(address, ""));
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
	std::shared_ptr<ral::cache::graph> graph = tree.build_batch_graph(json);
	try {
		ral::cache::cache_settings simple_cache_config{.type = ral::cache::CacheType::SIMPLE};
		graph->addPair(kpair(graph->get_last_kernel(), print, simple_cache_config));
		graph->execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

TEST_F(Batching, SortSamplePartitionTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address, ""));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "", std::map<std::string, std::string>());

	auto n_batches = 5;
	std::shared_ptr<ral::io::parquet_parser> parser;
	std::shared_ptr<ral::io::uri_data_provider> provider;
	ral::io::Schema schema;
	std::tie(parser, provider, schema) = blazingdb::test::CreateParquetOrderTableProvider(queryContext.get(), n_batches);

	ral::io::data_loader loader(parser, provider);

	TableScan customer_generator("", loader, schema, queryContext, nullptr);

	SortAndSampleKernel sort_and_sample("Logical_SortAndSample(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, nullptr);
	PartitionSingleNodeKernel partition("LogicalPartition(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, nullptr);
	MergeStreamKernel merge("LogicalMerge(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])", queryContext, nullptr);

	Projection project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", queryContext, nullptr);
	Filter filter("LogicalFilter(condition=[<($0, 100)])", queryContext, nullptr);
	Print print;
	ral::cache::graph m;
	try {
		auto cache_machine_config =
			ral::cache::cache_settings{.type = ral::cache::CacheType::FOR_EACH, .num_partitions = 32};
		m->addPair(customer_generator >> filter);
		m->addPair(filter >> project);
		m->addPair(project >> sort_and_sample);
		m->addPair(kpair(&sort_and_sample, "output_a", &partition, "input_a"));
		m->addPair(kpair(&sort_and_sample, "output_b", &partition, "input_b"));
		m->addPair(kpair(partition, merge, cache_machine_config));
		m->addPair(kpair(merge, print, ral::cache::cache_settings{.type = ral::cache::CacheType::CONCATENATING}));
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}

}  // namespace batch
}  // namespace ral
