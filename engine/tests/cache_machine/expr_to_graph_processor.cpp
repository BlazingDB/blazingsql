
#include "execution_graph/logic_controllers/LogicalProject.h"
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


using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
struct ExprToGraphProcessor : public cudf::test::BaseFixture {
	ExprToGraphProcessor() {}

	~ExprToGraphProcessor() {}
}; 

namespace ral {
namespace cache {
//LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], n_comment=[$3])
//LogicalFilter(condition=[<($0, 10)])
//LogicalTableScan(table=[[main, nation]])

TEST_F(ExprToGraphProcessor, FromJsonInput) {
	std::string json = R"(
	{
		'expr': 'LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], n_comment=[$3])',
		'children': [
			{
				'expr': 'LogicalFilter(condition=[<($0, 5)])',
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
	const std::string content =
		R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
		1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
		2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
		3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
		4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
		5|ETHIOPIA|0|ven packages wake quickly. regu
		6|FRANCE|3|refully final requests. regular, ironi
		7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
		8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
		9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
		10|IRAN|4|efully alongside of the slyly final dependencies)";

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string filename = "/tmp/nation.psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	in_args.dtype = { "int32", "int64", "int32", "int64"};
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;

	uris.push_back(Uri{filename});
	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(schema, {});

	parser::expr_tree_processor tree{
		.root = {},
		.context = &queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	PrinterKernel print;

	auto graph = tree.build_graph(json);
	try {
		graph += graph.get_last_kernel() >> print;
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}


TEST_F(ExprToGraphProcessor, FromJsonInputOptimized) {
	std::string json = R"(
	{
		'expr': 'BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]], projects=[[0, 2]], aliases=[[n_nationkey, n_regionkey]])',
		'children': []
	}
	)";
	const std::string content =
		R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
		1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
		2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
		3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
		4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
		5|ETHIOPIA|0|ven packages wake quickly. regu
		6|FRANCE|3|refully final requests. regular, ironi
		7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
		8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
		9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
		10|IRAN|4|efully alongside of the slyly final dependencies)";

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string filename = "/tmp/nation.psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	in_args.dtype = { "int32", "int64", "int32", "int64"};
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;

	uris.push_back(Uri{filename});
	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(schema, {});

	parser::expr_tree_processor tree{
		.root = {},
		.context = &queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	PrinterKernel print;

	auto graph = tree.build_graph(json);
	try {
		graph += graph.get_last_kernel() >> print;
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}


TEST_F(ExprToGraphProcessor, FromJsonInputAggregation) {
	std::string json = R"(
	{
		'expr': 'LogicalAggregate(group=[{0}])',
		'children': [
			{
				'expr': 'LogicalProject(n_regionkey=[$1])',
				'children': [
					{
						'expr': 'BindableTableScan(table=[[main, nation]], filters=[[<($0, 5)]], projects=[[0, 2]], aliases=[[$f0, n_regionkey]])',
						'children': []
					}
				]
			}

		]
	}
	)";
	const std::string content =
		R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
		1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
		2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
		3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
		4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
		5|ETHIOPIA|0|ven packages wake quickly. regu
		6|FRANCE|3|refully final requests. regular, ironi
		7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
		8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
		9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
		10|IRAN|4|efully alongside of the slyly final dependencies)";

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	Context queryContext{ctxToken, contextNodes, contextNodes[0], ""};

	std::string filename = "/tmp/nation.psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	in_args.dtype = { "int32", "int64", "int32", "int64"};
	in_args.delimiter = '|';
	in_args.header = -1;

	std::vector<Uri> uris;

	uris.push_back(Uri{filename});
	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::csv_parser>(in_args);
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(schema, {});

	parser::expr_tree_processor tree{
		.root = {},
		.context = &queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	PrinterKernel print;

	auto graph = tree.build_graph(json);
	try {
		graph += graph.get_last_kernel() >> print;
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}


}  // namespace cache
}  // namespace ral
