
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


using blazingdb::manager::experimental::Context;
using blazingdb::transport::experimental::Address;
using blazingdb::transport::experimental::Node;
struct ExprToGraphProcessor : public cudf::test::BaseFixture {
	const std::string filename = "/tmp/nation.psv";

	ExprToGraphProcessor() {
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

		std::ofstream outfile(filename, std::ofstream::out);
		outfile << content << std::endl;
		outfile.close();

	}

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
	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

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
		.context = queryContext,
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
	std::replace( json.begin(), json.end(), '\'', '\"');
	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

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
		.context = queryContext,
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
	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

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
		.context = queryContext,
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

/*
TEST_F(ExprToGraphProcessor, TimerIssue) {
	auto count = 20;
    std::vector<std::thread> thread_pool;

	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	in_args.dtype = { "int32", "str", "int32", "str"};
	in_args.delimiter = '|';
	in_args.header = -1;
	cudf_io::read_parquet_args in_pargs{cudf_io::source_info{"/home/aocsa/tpch/100MB2Part/tpch/customer_0_0.parquet"}};

	auto result = cudf_io::read_parquet(in_pargs);

	CodeTimer loop_timer;
    for (size_t i = 0; i < count; i++)
    {
        CodeTimer timer;
		auto result = cudf_io::read_parquet(in_pargs);
        std::cout << "id: " << i << std::endl;
        timer.display();
    }
	std::cout << "****loop_timer*****" << std::endl;
	loop_timer.display();

	loop_timer.reset();
    for (size_t i = 0; i < count; i++)
    {
        thread_pool.emplace_back(std::thread([i, in_args, in_pargs](){
            CodeTimer timer;
            std::cout << "id: " << i << std::endl;
			auto result = cudf_io::read_parquet(in_pargs);
            timer.display();
        }));
    }
    for(auto &t : thread_pool) {
        t.join();
    }
	std::cout << "****loop_timer*****" << std::endl;
	loop_timer.display();
}

TEST_F(ExprToGraphProcessor, DeadLockIssue) {

	std::string json = R"({
		'expr': 'LogicalSort(sort0=[$1], dir0=[ASC])',
		'children': [
			{
			'expr': 'BindableTableScan(table=[[main, customer]], projects=[[0, 3]], aliases=[[c_custkey, c_nationkey]])',
			'children': []
			}
		]
		})";

	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

	std::vector<Uri> uris;
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/customer_0_0.parquet"}); 

	ral::io::Schema tableSchema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});
 
 	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						   { std::vector<int>{0}});

	parser::expr_tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"customer"}
	};
	PrinterKernel print;

	auto graph = tree.build_graph(json);
	try {
		graph += link(graph.get_last_kernel(), print, ral::cache::cache_settings{.type = ral::cache::CacheType::CONCATENATING});
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

// TEST_02
// select n1.n_nationkey as n1key, n2.n_nationkey as n2key, n1.n_nationkey + n2.n_nationkey
// from nation as n1 full
// outer join nation as n2
// on n1.n_nationkey = n2.n_nationkey + 6 where n1.n_nationkey < 10
TEST_F(ExprToGraphProcessor, JoinIssue) {
	std::string json = R"(
	{
	  'expr': 'LogicalProject(n1key=[$0], n2key=[$1], EXPR$2=[+($0, $1)])',
	  'children': [
		{
		  'expr': 'LogicalFilter(condition=[<($0, 10)])',
		  'children': [
			{
			  'expr': 'LogicalProject(n1key=[$0], n2key=[$4])',
			  'children': [
				{
				  'expr': 'LogicalJoin(condition=[=($0, $8)], joinType=[full])',
				  'children': [
					{
					  'expr': 'LogicalTableScan(table=[[main, nation]])',
					  'children': []
					},
					{
					  'expr': 'LogicalProject(n_nationkey=[$0], n_name=[$1], n_regionkey=[$2], n_comment=[$3], $f4=[+($0, 6)])',
					  'children': [
						{
						  'expr': 'BindableTableScan(table=[[main, nation]], aliases=[[n_nationkey, n_name, n_regionkey, n_comment, $f4]])',
						  'children': []
						}
					  ]
					}
				  ]
				}
			  ]
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
	ral::io::Schema tableSchema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(tableSchema, {});

	ral::io::Schema schema(tableSchema.get_names(),
						  tableSchema.get_calcite_to_file_indices(),
						  tableSchema.get_dtypes(),
						  tableSchema.get_in_file(),
						   { std::vector<int>{0}});
	parser::expr_tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	PrinterKernel print;

//	auto graph = tree.build_graph(json);
//	try {
//		graph += graph.get_last_kernel() >> print;
//		graph.show();
//		graph.execute();
//	} catch(std::exception & ex) {
//		std::cout << ex.what() << "\n";
//	}
	tree.execute_plan(json);
}*/

/*
//union issue
// (select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1,2, 3 limit 10)
//		union all
//(select l_shipdate, l_orderkey, l_linestatus from lineitem where l_linenumber = 1 order by 1, 3 desc, 2 limit 10)
//
//LogicalUnion(all=[true])
//	LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC], fetch=[10])
//		LogicalProject(l_shipdate=[$3], l_orderkey=[$0], l_linestatus=[$2])
//			BindableTableScan(table=[[main, lineitem]], filters=[[=($1, 1)]], projects=[[0, 3, 9, 10]], aliases=[[l_orderkey, $f1, l_linestatus, l_shipdate]])
//	LogicalSort(sort0=[$0], sort1=[$2], sort2=[$1], dir0=[ASC], dir1=[DESC], dir2=[ASC], fetch=[10])
//		LogicalProject(l_shipdate=[$3], l_orderkey=[$0], l_linestatus=[$2])
//			BindableTableScan(table=[[main, lineitem]], filters=[[=($1, 1)]], projects=[[0, 3, 9, 10]], aliases=[[l_orderkey, $f1, l_linestatus, l_shipdate]])
TEST_F(ExprToGraphProcessor, UnionTest) {
	std::string json = R"(
	{
	  'expr': 'LogicalUnion(all=[true])',
	  'children': [
		{
		  'expr': 'LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC], fetch=[10])',
		  'children': [
			{
			  'expr': 'LogicalProject(l_shipdate=[$3], l_orderkey=[$0], l_linestatus=[$2])',
			  'children': [
				{
				  'expr': 'BindableTableScan(table=[[main, lineitem]], filters=[[=($1, 1)]], projects=[[0, 3, 9, 10]], aliases=[[l_orderkey, $f1, l_linestatus, l_shipdate]])',
				  'children': []
				}
			  ]
			}
		  ]
		},
		{
		  'expr': 'LogicalSort(sort0=[$0], sort1=[$2], sort2=[$1], dir0=[ASC], dir1=[DESC], dir2=[ASC], fetch=[10])',
		  'children': [
			{
			  'expr': 'LogicalProject(l_shipdate=[$3], l_orderkey=[$0], l_linestatus=[$2])',
			  'children': [
				{
				  'expr': 'BindableTableScan(table=[[main, lineitem]], filters=[[=($1, 1)]], projects=[[0, 3, 9, 10]], aliases=[[l_orderkey, $f1, l_linestatus, l_shipdate]])',
				  'children': []
				}
			  ]
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
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/lineitem_0_0.parquet"});
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/lineitem_1_0.parquet"});

	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(schema, {});

	ral::io::Schema schema2;
	auto parser2 = std::make_shared<ral::io::parquet_parser>();
	auto provider2 = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader2(parser2, provider2);
	loader2.get_schema(schema2, {});


	parser::expr_tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"lineitem"}
	};
	PrinterKernel print;

	auto graph = tree.build_graph(json);
	try {
		graph += link(graph.get_last_kernel(), print, ral::cache::cache_settings{.type = ral::cache::CacheType::CONCATENATING});
		graph.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
}

// 	   	0 -> 4 -> 3
// 	0 -> 6 -> 5 -> 3 -> 2 -> 1

TEST_F(ExprToGraphProcessor, JoinTest) {
	std::string json = R"(
	{
	  'expr': 'LogicalAggregate(group=[{}], n1key=[COUNT($0)], n2key=[COUNT($1)], cstar=[COUNT()])',
	  'children': [
		{
		  'expr': 'LogicalProject(n_nationkey=[$0], n_nationkey0=[$1])',
		  'children': [
			{
			  'expr': 'LogicalJoin(condition=[=($0, $2)], joinType=[full])',
			  'children': [
				{
				  'expr': 'BindableTableScan(table=[[main, nation]], projects=[[0]], aliases=[[n_nationkey]])',
				  'children': []
				},
				{
				  'expr': 'LogicalProject(n_nationkey=[$0], $f4=[+($0, 6)])',
				  'children': [
					{
					  'expr': 'BindableTableScan(table=[[main, nation]], projects=[[0]], aliases=[[n_nationkey, $f4]])',
					  'children': []
					}
				  ]
				}
			  ]
			}
		  ]
		}
	  ]
	})";

	std::replace( json.begin(), json.end(), '\'', '\"');

	std::vector<Node> contextNodes;
	auto address = Address::TCP("127.0.0.1", 8089, 0);
	contextNodes.push_back(Node(address));
	uint32_t ctxToken = 123;
	auto queryContext = std::make_shared<Context>(ctxToken, contextNodes, contextNodes[0], "");;

	std::vector<Uri> uris;
	uris.push_back(Uri{"/home/aocsa/tpch/100MB2Part/tpch/nation_0_0.parquet"});

	ral::io::Schema schema;
	auto parser = std::make_shared<ral::io::parquet_parser>();
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	ral::io::data_loader loader(parser, provider);
	loader.get_schema(schema, {});

	parser::expr_tree_processor tree{
		.root = {},
		.context = queryContext,
		.input_loaders = {loader},
		.schemas = {schema},
		.table_names = {"nation"}
	};
	PrinterKernel print;

	auto output = tree.execute_plan(json);
	ral::utilities::print_blazing_table_view(output->toBlazingTableView());

}*/

}  // namespace cache
}  // namespace ral
