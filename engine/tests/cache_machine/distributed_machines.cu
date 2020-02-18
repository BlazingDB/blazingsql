
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

struct DistributedMachinesTest : public cudf::test::BaseFixture {
	DistributedMachinesTest() {}

	~DistributedMachinesTest() {}
}; 

namespace ral {
namespace cache {
namespace parser{

struct expr_tree_processor {
	struct node {
		std::string expr;               // expr
		int level;                      // level
		std::shared_ptr<kernel>            kernel_unit;
		std::vector<std::shared_ptr<node>> children;  // children nodes
	} root;
	blazingdb::manager::experimental::Context * context;

	expr_tree_processor(std::string json) {
		try {
			std::replace( json.begin(), json.end(), '\'', '\"');
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);

		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}
	}
	void expr_tree_from_json(boost::property_tree::ptree const& node, expr_tree_processor::node * root_ptr, int level) {
		auto expr = node.get<std::string>("expr", "");
		for(int i = 0; i < level*2 ; ++i) {
			std::cout << " ";
		}
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = get_kernel(expr);
		std::cout << expr << std::endl;
		for (auto &child : node.get_child("children"))
		{
			auto child_node_ptr = std::make_shared<expr_tree_processor::node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1);
		}
	}
	std::shared_ptr<kernel>  get_kernel(std::string expr) {
		if (expr.find("LogicalProject") != std::string::npos)
			return std::make_shared<ProjectKernel>(expr, this->context);
		if (expr.find("LogicalFilter") != std::string::npos)
			return std::make_shared<FilterKernel>(expr, this->context);
		if (expr.find("LogicalJoin") != std::string::npos)
			return std::make_shared<JoinKernel>(expr, this->context);
		if (expr.find("LogicalProject") != std::string::npos)
			return std::make_shared<ProjectKernel>(expr, this->context);
//		if (expr.find("LogicalTableScan") != std::string::npos)
//			TODO: include use the data_provider apis
//			return std::make_shared<TableScanKernel>(expr, this->context);

		return nullptr;
	}
};

} // namespace parser


TEST_F(DistributedMachinesTest, FromJsonInput) {
	std::string json = R"(
	{
		'expr': 'LogicalProject(c_custkey=[$9], c_nationkey=[$12], c_acctbal=[$14])',
		'children': [
			{
				'expr': 'LogicalFilter(condition=[<($0, 100)])',
				'children': [
					{
						'expr': 'LogicalJoin(condition=[=($1, $9)], joinType=[inner])',
						'children':
						[
							{
								'expr': 'LogicalTableScan(table=[[main, orders]])',
								'children' : []
							},
							{
								'expr':  'LogicalTableScan(table=[[main, customer]])',
								'children' : []
							}

						]
					}
				]
			}
		]
	}
	)";
	parser::expr_tree_processor tree(json);
}

TEST_F(DistributedMachinesTest, SortSamplePartitionWorkFlowTest) {
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
	SortKernel sort("LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])", &queryContext);
	ProjectKernel project("LogicalProject(c_custkey=[$0], c_nationkey=[$3])", &queryContext);
	FilterKernel filter("LogicalFilter(condition=[<($0, 10)])", &queryContext);
	PrinterKernel print;
	ral::cache::graph m;
	try {
		m += customer_generator >> filter;
		m += filter >> project;
		m += project >> sort;
		m += sort >> print;
		m.execute();
	} catch(std::exception & ex) {
		std::cout << ex.what() << "\n";
	}
	std::this_thread::sleep_for(std::chrono::seconds(1));
}


}  // namespace cache
}  // namespace ral
