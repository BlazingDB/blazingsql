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
using ral::cache::cache_settings;
using ral::cache::CacheType;
 
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
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<BindableTableScan>(expr, *loader, schema, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::BindableTableScanKernel);
		} else if ( is_sort(expr) ) {
			k = std::make_shared<ral::cache::SortKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortKernel);
		}  else if (is_single_node_partition(expr)) {
			k = std::make_shared<PartitionSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionSingleNodeKernel);
		} else if (is_single_node_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleSingleNodeKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleSingleNodeKernel);
		} else if (is_partition(expr)) {
			k = std::make_shared<PartitionKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionKernel);
		} else if (is_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleKernel);
		}  else if (is_merge(expr)) {
			k = std::make_shared<MergeStreamKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeStreamKernel);
		}  else if (is_aggregate(expr)) {
			k = std::make_shared<ral::cache::AggregateKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::AggregateKernel);
		}  else if (is_compute_aggregate(expr)) {
			k = std::make_shared<ComputeAggregateKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ComputeAggregateKernel);
		}  else if (is_distribute_aggregate(expr)) {
			k = std::make_shared<DistributeAggregateKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::DistributeAggregateKernel);
		}  else if (is_merge_aggregate(expr)) {
			k = std::make_shared<MergeAggregateKernel>(expr, kernel_context);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeAggregateKernel);
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

	void transform_operator_bigger_than_gpu(node* parent, node * current) {
		if (current->kernel_unit->get_type_id() == kernel_type::SortKernel) {
			auto merge_expr = current->expr;
			auto partition_expr = current->expr;
			auto sort_and_sample_expr = current->expr;

			if (this->context->getTotalNodes() == 1) {
				StringUtil::findAndReplaceAll(merge_expr, LOGICAL_SORT_TEXT, LOGICAL_MERGE_TEXT);
				StringUtil::findAndReplaceAll(partition_expr, LOGICAL_SORT_TEXT, LOGICAL_SINGLE_NODE_PARTITION_TEXT);
				StringUtil::findAndReplaceAll(sort_and_sample_expr, LOGICAL_SORT_TEXT, LOGICAL_SINGLE_NODE_SORT_AND_SAMPLE_TEXT);
			}	else {
				StringUtil::findAndReplaceAll(merge_expr, LOGICAL_SORT_TEXT, LOGICAL_MERGE_TEXT);
				StringUtil::findAndReplaceAll(partition_expr, LOGICAL_SORT_TEXT, LOGICAL_PARTITION_TEXT);
				StringUtil::findAndReplaceAll(sort_and_sample_expr, LOGICAL_SORT_TEXT, LOGICAL_SORT_AND_SAMPLE_TEXT);
			}

			auto merge = std::make_shared<node>();
			merge->expr = merge_expr;
			merge->level = current->level;
			merge->kernel_unit = make_kernel(merge_expr);

			auto partition = std::make_shared<node>();
			partition->expr = partition_expr;
			partition->level = current->level;
			partition->kernel_unit = make_kernel(partition_expr);
			merge->children.push_back(partition);

			auto ssample = std::make_shared<node>();
			ssample->expr = sort_and_sample_expr;
			ssample->level = current->level;
			ssample->kernel_unit = make_kernel(sort_and_sample_expr);
			partition->children.push_back(ssample);

			ssample->children = current->children;

			if (parent == nullptr) { // root is sort
				// new root
				current->expr = merge->expr;
				current->kernel_unit = merge->kernel_unit;
				current->children = merge->children;
			} else {
				auto it = std::find_if(parent->children.begin(), parent->children.end(), [current] (std::shared_ptr<node> tmp) {
					return tmp->kernel_unit->get_id() == current->kernel_unit->get_id();
				});
				parent->children.erase( it );
				parent->children.push_back(merge);
			}
		} else if (current->kernel_unit->get_type_id() == kernel_type::AggregateKernel) {
			auto merge_aggregate_expr = current->expr;
			auto distribute_aggregate_expr = current->expr;
			auto compute_aggregate_expr = current->expr;

			if (this->context->getTotalNodes() == 1) {
				StringUtil::findAndReplaceAll(merge_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_MERGE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(compute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_COMPUTE_AGGREGATE_TEXT);
			}	else {
				StringUtil::findAndReplaceAll(merge_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_MERGE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(distribute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_DISTRIBUTE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(compute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_COMPUTE_AGGREGATE_TEXT);
			}

			auto compute = std::make_shared<node>();
			compute->expr = compute_aggregate_expr;
			compute->level = current->level;
			compute->kernel_unit = make_kernel(compute_aggregate_expr);
			compute->children = current->children;

			auto merge = std::make_shared<node>();
			merge->expr = merge_aggregate_expr;
			merge->level = current->level;
			merge->kernel_unit = make_kernel(merge_aggregate_expr);

			if (this->context->getTotalNodes() == 1) {
				merge->children.push_back(compute);
			} else {
				auto distribute = std::make_shared<node>();
				distribute->expr = distribute_aggregate_expr;
				distribute->level = current->level;
				distribute->kernel_unit = make_kernel(distribute_aggregate_expr);
				distribute->children.push_back(compute);
				merge->children.push_back(distribute);
			}			

			if (parent == nullptr) { // root is sort
				// new root
				current->expr = merge->expr;
				current->kernel_unit = merge->kernel_unit;
				current->children = merge->children;
			} else {
				auto it = std::find_if(parent->children.begin(), parent->children.end(), [current] (std::shared_ptr<node> tmp) {
					return tmp->kernel_unit->get_id() == current->kernel_unit->get_id();
				});
				parent->children.erase( it );
				parent->children.push_back(merge);
			}	
		
		
		}	else if (current) {
			for (auto& child : current->children) {
				transform_operator_bigger_than_gpu(current, child.get());
			}
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

		transform_operator_bigger_than_gpu(nullptr, &this->root);

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
				auto child_kernel_type = child->kernel_unit->get_type_id();
				auto parent_kernel_type = parent->kernel_unit->get_type_id();
				if ((child_kernel_type == kernel_type::SortAndSampleKernel &&	parent_kernel_type == kernel_type::PartitionKernel)
						|| (child_kernel_type == kernel_type::SortAndSampleSingleNodeKernel &&	parent_kernel_type == kernel_type::PartitionSingleNodeKernel)) {
					graph += (*(child->kernel_unit))["output_a"] >> (*(parent->kernel_unit))["input_a"];
					graph += (*(child->kernel_unit))["output_b"] >> (*(parent->kernel_unit))["input_b"];
				} else if ((child_kernel_type == kernel_type::PartitionKernel && parent_kernel_type == kernel_type::MergeStreamKernel)
									|| (child_kernel_type == kernel_type::PartitionSingleNodeKernel && parent_kernel_type == kernel_type::MergeStreamKernel)) {
					auto cache_machine_config =	cache_settings{.type = CacheType::FOR_EACH, .num_partitions = 10};
					graph += link(*child->kernel_unit, *parent->kernel_unit, cache_machine_config);
				} else {
					graph +=  *child->kernel_unit >> (*parent->kernel_unit);
				}	
			}
		}
	}

};
 

} // namespace batch
} // namespace ral
