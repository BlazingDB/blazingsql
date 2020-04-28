#pragma once

#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "BatchProcessing.h"
#include "BatchOrderByProcessing.h"
#include "BatchAggregationProcessing.h"
#include "BatchJoinProcessing.h"
#include "BatchUnionProcessing.h"
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

	std::shared_ptr<kernel> make_kernel(std::string expr, std::shared_ptr<ral::cache::graph> query_graph) {
		std::shared_ptr<kernel> k;
		auto kernel_context = this->context->clone();
		if ( is_project(expr) ) {
			k = std::make_shared<Projection>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ProjectKernel);
		} else if ( is_filter(expr) ) {
			k = std::make_shared<Filter>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::FilterKernel);
		} else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<TableScan>(*loader, schema, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::TableScanKernel);
		} else if (is_bindable_scan(expr)) {
			size_t table_index = get_table_index(table_names, extract_table_name(expr));
			auto loader = this->input_loaders[table_index].clone(); // NOTE: this is required if the same loader is used next time
			auto schema = this->schemas[table_index];
			k = std::make_shared<BindableTableScan>(expr, *loader, schema, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::BindableTableScanKernel);
		}  else if (is_single_node_partition(expr)) {
			k = std::make_shared<PartitionSingleNodeKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionSingleNodeKernel);
		} else if (is_single_node_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleSingleNodeKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleSingleNodeKernel);
		} else if (is_partition(expr)) {
			k = std::make_shared<PartitionKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartitionKernel);
		} else if (is_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::SortAndSampleKernel);
		} else if (is_merge(expr)) {
			k = std::make_shared<MergeStreamKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeStreamKernel);
		} else if (is_limit(expr)) {
			k = std::make_shared<LimitKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::LimitKernel);
		}  else if (is_compute_aggregate(expr)) {
			k = std::make_shared<ComputeAggregateKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::ComputeAggregateKernel);
		}  else if (is_distribute_aggregate(expr)) {
			k = std::make_shared<DistributeAggregateKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::DistributeAggregateKernel);
		}  else if (is_merge_aggregate(expr)) {
			k = std::make_shared<MergeAggregateKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::MergeAggregateKernel);
		}  else if (is_pairwise_join(expr)) {
			k = std::make_shared<PartwiseJoin>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::PartwiseJoinKernel);
		} else if (is_join_partition(expr)) {
			k = std::make_shared<JoinPartitionKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::JoinPartitionKernel);
		} else if (is_union(expr)) {
			k = std::make_shared<UnionKernel>(expr, kernel_context, query_graph);
			kernel_context->setKernelId(k->get_id());
			k->set_type_id(kernel_type::UnionKernel);
		}
		k->expr = expr;
		return k;
	}
	void expr_tree_from_json(boost::property_tree::ptree const& p_tree, node * root_ptr, int level, std::shared_ptr<ral::cache::graph> query_graph) {
		auto expr = p_tree.get<std::string>("expr", "");
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = make_kernel(expr, query_graph);
		for (auto &child : p_tree.get_child("children")) {
			auto child_node_ptr = std::make_shared<node>();
			root_ptr->children.push_back(child_node_ptr);
			expr_tree_from_json(child.second, child_node_ptr.get(), level + 1, query_graph);
		}
	} 

	boost::property_tree::ptree create_array_tree(boost::property_tree::ptree child){
		boost::property_tree::ptree children;
		children.push_back(std::make_pair("", child));
		return children;
	}

	void transform_json_tree(boost::property_tree::ptree &p_tree) {
		std::string expr = p_tree.get<std::string>("expr", "");
		if (is_sort(expr)){
			auto limit_expr = expr;
			auto merge_expr = expr;
			auto partition_expr = expr;
			auto sort_and_sample_expr = expr;
			if(ral::operators::experimental::has_limit_only(expr)){
				StringUtil::findAndReplaceAll(limit_expr, LOGICAL_SORT_TEXT, LOGICAL_LIMIT_TEXT);

				p_tree.put("expr", limit_expr);
			} else {
				if (this->context->getTotalNodes() == 1) {
					StringUtil::findAndReplaceAll(limit_expr, LOGICAL_SORT_TEXT, LOGICAL_LIMIT_TEXT);
					StringUtil::findAndReplaceAll(merge_expr, LOGICAL_SORT_TEXT, LOGICAL_MERGE_TEXT);
					StringUtil::findAndReplaceAll(partition_expr, LOGICAL_SORT_TEXT, LOGICAL_SINGLE_NODE_PARTITION_TEXT);
					StringUtil::findAndReplaceAll(sort_and_sample_expr, LOGICAL_SORT_TEXT, LOGICAL_SINGLE_NODE_SORT_AND_SAMPLE_TEXT);
				}	else {
					StringUtil::findAndReplaceAll(limit_expr, LOGICAL_SORT_TEXT, LOGICAL_LIMIT_TEXT);
					StringUtil::findAndReplaceAll(merge_expr, LOGICAL_SORT_TEXT, LOGICAL_MERGE_TEXT);
					StringUtil::findAndReplaceAll(partition_expr, LOGICAL_SORT_TEXT, LOGICAL_PARTITION_TEXT);
					StringUtil::findAndReplaceAll(sort_and_sample_expr, LOGICAL_SORT_TEXT, LOGICAL_SORT_AND_SAMPLE_TEXT);
				}
				boost::property_tree::ptree sample_tree;
				sample_tree.put("expr", sort_and_sample_expr);
				sample_tree.add_child("children", p_tree.get_child("children"));
				boost::property_tree::ptree partition_tree;
				partition_tree.put("expr", partition_expr);
				partition_tree.add_child("children", create_array_tree(sample_tree));
				boost::property_tree::ptree merge_tree;
				merge_tree.put("expr", merge_expr);
				merge_tree.add_child("children", create_array_tree(partition_tree)); 

				p_tree.put("expr", limit_expr);
				p_tree.put_child("children", create_array_tree(merge_tree));
			}
		} 
		else if (is_aggregate(expr)) {
			auto merge_aggregate_expr = expr;
			auto distribute_aggregate_expr = expr;
			auto compute_aggregate_expr = expr;

			if (this->context->getTotalNodes() == 1) {
				StringUtil::findAndReplaceAll(merge_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_MERGE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(compute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_COMPUTE_AGGREGATE_TEXT);

				boost::property_tree::ptree agg_tree;
				agg_tree.put("expr", compute_aggregate_expr);
				agg_tree.add_child("children", p_tree.get_child("children")); 

				p_tree.clear();

				p_tree.put("expr", merge_aggregate_expr);
				p_tree.put_child("children", create_array_tree(agg_tree));
			}	else {
				StringUtil::findAndReplaceAll(merge_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_MERGE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(distribute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_DISTRIBUTE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(compute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_COMPUTE_AGGREGATE_TEXT);

				boost::property_tree::ptree compute_aggregate_tree;
				compute_aggregate_tree.put("expr", compute_aggregate_expr);
				compute_aggregate_tree.add_child("children", p_tree.get_child("children"));
			
				boost::property_tree::ptree distribute_aggregate_tree;
				distribute_aggregate_tree.put("expr", distribute_aggregate_expr);
				distribute_aggregate_tree.add_child("children", create_array_tree(compute_aggregate_tree)); 

				p_tree.clear();

				p_tree.put("expr", merge_aggregate_expr);
				p_tree.put_child("children", create_array_tree(distribute_aggregate_tree));
			}
		} else if (is_join(expr)) {
			if (this->context->getTotalNodes() == 1) {
				// PartwiseJoin
				auto pairwise_expr = expr;
				StringUtil::findAndReplaceAll(pairwise_expr, LOGICAL_JOIN_TEXT, LOGICAL_PARTWISE_JOIN_TEXT);
				p_tree.put("expr", pairwise_expr);
			} else {
				auto pairwise_expr = expr;
				auto join_partition_expr = expr;

				StringUtil::findAndReplaceAll(pairwise_expr, LOGICAL_JOIN_TEXT, LOGICAL_PARTWISE_JOIN_TEXT);
				StringUtil::findAndReplaceAll(join_partition_expr, LOGICAL_JOIN_TEXT, LOGICAL_JOIN_PARTITION_TEXT);

				boost::property_tree::ptree join_partition_tree;
				join_partition_tree.put("expr", join_partition_expr);
				join_partition_tree.add_child("children", p_tree.get_child("children")); 

				p_tree.clear();

				p_tree.put("expr", pairwise_expr);
				p_tree.put_child("children", create_array_tree(join_partition_tree));
			}
		}
		for (auto &child : p_tree.get_child("children")) {
			transform_json_tree(child.second);
		}
	}

	void print_tree(node* p_tree, int level = 0) {
		auto expr = p_tree->expr;
		for(int i = 0; i < level*2 ; ++i) {
			std::cout << " ";
		}
		std::cout << std::to_string((int)p_tree->kernel_unit->get_type_id()) + "_" + std::to_string(p_tree->kernel_unit->get_id())
							<< "  |  " << expr << std::endl;
		for (auto &child : p_tree->children) {
			print_tree(child.get(), level + 1);
		}
	} 
	
	std::shared_ptr<ral::cache::graph> build_batch_graph(std::string json) {
		auto query_graph = std::make_shared<ral::cache::graph>();
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			transform_json_tree(p_tree);

			expr_tree_from_json(p_tree, &this->root, 0, query_graph);
		} catch (std::exception & e) {
			std::cerr << "property_tree:" << e.what() <<  std::endl;
			throw e;
		}

		printf("==============================================================\n");
		print_tree(&this->root);
		printf("==============================================================\n");

		if (this->root.kernel_unit != nullptr) {
			query_graph->add_node(this->root.kernel_unit.get()); // register first node
			visit(*query_graph, &this->root, this->root.children);
		}
		return query_graph;
	}

	void visit(ral::cache::graph& query_graph, node * parent, std::vector<std::shared_ptr<node>>& children) {
		for (size_t index = 0; index < children.size(); index++) {
			auto& child  =  children[index];
			visit(query_graph, child.get(), child->children);
			std::string port_name = "input";

			if (children.size() > 1) {
				char index_char = 'a' + index;
				port_name = std::string("input_");
				port_name.push_back(index_char);
				query_graph +=  *child->kernel_unit >> (*parent->kernel_unit)[port_name];
			} else {
				auto child_kernel_type = child->kernel_unit->get_type_id();
				auto parent_kernel_type = parent->kernel_unit->get_type_id();
				if ((child_kernel_type == kernel_type::JoinPartitionKernel && parent_kernel_type == kernel_type::PartwiseJoinKernel)
					    || (child_kernel_type == kernel_type::SortAndSampleKernel &&	parent_kernel_type == kernel_type::PartitionKernel)
						|| (child_kernel_type == kernel_type::SortAndSampleSingleNodeKernel &&	parent_kernel_type == kernel_type::PartitionSingleNodeKernel)) {
					query_graph += (*(child->kernel_unit))["output_a"] >> (*(parent->kernel_unit))["input_a"];
					query_graph += (*(child->kernel_unit))["output_b"] >> (*(parent->kernel_unit))["input_b"];
				} else if ((child_kernel_type == kernel_type::PartitionKernel && parent_kernel_type == kernel_type::MergeStreamKernel)
									|| (child_kernel_type == kernel_type::PartitionSingleNodeKernel && parent_kernel_type == kernel_type::MergeStreamKernel)) {
					
					const int MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE = 8; // WSM TODO make this dynamic or configurable. ALso used in generate_distributed_partition_plan function
					auto cache_machine_config =	cache_settings{.type = CacheType::FOR_EACH, .num_partitions = MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE};
					query_graph += link(*child->kernel_unit, *parent->kernel_unit, cache_machine_config);

				} else {
					query_graph +=  *child->kernel_unit >> (*parent->kernel_unit);
				}	
			}
		}
	}

};
 

} // namespace batch
} // namespace ral
