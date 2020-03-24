#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "BatchProcessing.h"
#include "BatchOrderByProcessing.h"
#include "BatchAggregationProcessing.h"
#include "io/DataLoader.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
 
struct tree_processor {
	struct node {
		std::string expr;               // expr
		int level;                      // level
		std::shared_ptr<kernel>            kernel_unit;
		std::vector<std::shared_ptr<node>> children;  // children nodes

	} root;
	std::shared_ptr<Context> context;
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;
	const bool transform_operators_bigger_than_gpu = false;

	std::shared_ptr<kernel> make_kernel(std::string expr) {
		std::shared_ptr<kernel> k;
		auto kernel_context = this->context->clone();
		if ( is_project(expr) ) {
			k = std::make_shared<Projection>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ProjectKernel);
		} else if ( is_filter(expr) ) {
			k = std::make_shared<Filter>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::FilterKernel);
		}	else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<TableScan>(*loader, schema, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::TableScanKernel);
		} else if (is_bindable_scan(expr)) {
//			size_t table_index = get_table_index(table_names, extract_table_name(expr));
//			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
//			auto schema = this->schemas[table_index];
//			k = std::make_shared<BindableTableScanKernel>(expr, *loader, schema, kernel_context);
//			kernel_context->setKernelId(k->get_id());
//			k->set_type_id(kernel_type::BindableTableScanKernel);
		} else if (is_single_node_partition(expr)) {
			k = std::make_shared<PartitionSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionSingleNodeKernel);
		} else if (is_single_node_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleSingleNodeKernel);
		} else if (is_merge(expr)) {
			k = std::make_shared<MergeStreamKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeStreamKernel);
		}
		
		return k;
	}
	void expr_tree_from_json(boost::property_tree::ptree const& p_tree, node * root_ptr, int level) {
		auto expr = p_tree.get<std::string>("expr", "");
		// for(int i = 0; i < level*2 ; ++i) {
		// 	std::cout << " ";
		// }
		// std::cout << expr << std::endl;
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = make_kernel(expr);
		for (auto &child : p_tree.get_child("children")) {
			auto child_node_ptr = std::make_shared<node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1);
		}
	}

	ral::cache::graph build_batch_graph(std::string json) {
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			expr_tree_from_json(p_tree, &this->root, 0);

		} catch (std::exception & e) {
			std::cerr << e.what() <<  std::endl;
		}


		ral::cache::graph graph;
		if (this->root.kernel_unit != nullptr) {
			graph.add_node(this->root.kernel_unit.get()); // register first node
			visit(graph, &this->root, this->root.children);
		}
		return graph;
	}

	void visit(ral::cache::graph& graph, node * parent, std::vector<std::shared_ptr<node>>& children) {
		for (size_t index = 0; index < children.size(); index++) {
			auto& child  =  children[index];
			visit(graph, child.get(), child->children);
			std::string port_name = "input";

			if (children.size() > 1) {
				char index_char = 'a' + index;
				port_name = std::string("input_");
				port_name.push_back(index_char);
				graph +=  *child->kernel_unit >> (*parent->kernel_unit)[port_name];
			} else {
				auto a = child->kernel_unit->get_type_id();
				auto b = parent->kernel_unit->get_type_id();
				graph +=  *child->kernel_unit >> (*parent->kernel_unit);
			}
		}
	}

};
 

} // namespace batch
} // namespace ral
