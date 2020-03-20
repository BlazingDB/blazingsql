
#include "execution_graph/logic_controllers/LogicalProject.h"
#include "execution_graph/logic_controllers/CacheMachine.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/random_generator.cuh"
#include <Util/StringUtil.h>
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
#include <execution_graph/logic_controllers/BatchProcessing.h>

using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
struct Batching : public cudf::test::BaseFixture {
	 

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
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

	std::vector<Uri> uris;
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/nation_0_0.parquet"});
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/nation_0_0.parquet"});
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/nation_0_0.parquet"});

	ral::io::Schema tableSchema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
	std::vector<int> row_groups{0};
 	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						   {row_groups, row_groups, row_groups});//because 3 files
	tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	Print print;  
	auto graph = tree.build_batch_graph(json);
	try {
		ral::cache::cache_settings simple_cache_config{.type = ral::cache::CacheType::SIMPLE};
		graph += link(graph.get_last_kernel(), print, simple_cache_config);
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

TEST_F(Batching, SortSamplePartitionTest) {
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

	std::vector<Uri> uris;
	uris.push_back(Uri{"/home/jeanpierre/Desktop/nvmefiles/blazingdb/data/tpch100/tpch/customer_0_0.parquet"});
	uris.push_back(Uri{"/home/jeanpierre/Desktop/nvmefiles/blazingdb/data/tpch100/tpch/customer_0_0.parquet"});
	uris.push_back(Uri{"/home/jeanpierre/Desktop/nvmefiles/blazingdb/data/tpch100/tpch/customer_0_0.parquet"});

	ral::io::Schema tableSchema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
	std::vector<int> row_groups{0};
 	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						   {row_groups, row_groups, row_groups});//because 3 files

	TableScan customer_generator(loader, schema, queryContext);

	SortAndSampleSingleNodeKernel sort_and_sample("Logical_SortAndSample(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	PartitionSingleNodeKernel partition("LogicalPartition(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	MergeStreamKernel merge("LogicalMerge(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", queryContext);
	Projection project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", queryContext);
	Filter filter("LogicalFilter(condition=[<($0, 100)])", queryContext);
	Print print;
	ral::cache::graph m;
	try {
		auto cache_machine_config =
			ral::cache::cache_settings{.type = ral::cache::CacheType::FOR_EACH, .num_partitions = 11};
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
