#pragma once

#include "LogicPrimitives.h"
#include "BatchOrderByProcessing.h"
#include "BatchAggregationProcessing.h"
#include "BatchJoinProcessing.h"
#include "BatchUnionProcessing.h"
#include "BatchWindowFunctionProcessing.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "parser/expression_utils.hpp"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <Util/StringUtil.h>

using namespace fmt::literals;

namespace ral {
namespace batch {

using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;
using ral::cache::cache_settings;
using ral::cache::CacheType;

struct node {
	std::string expr;               // expr
	int level;                      // level
	std::shared_ptr<kernel>            kernel_unit;
	std::vector<std::shared_ptr<node>> children;  // children nodes
};
struct tree_processor {
	
	node root;
	std::shared_ptr<Context> context;
	std::vector<ral::io::data_loader> input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;
	std::vector<std::string> table_scans;
	const bool transform_operators_bigger_than_gpu = false;

	tree_processor(	node root,
	std::shared_ptr<Context> context,
	std::vector<ral::io::data_loader> input_loaders,
	std::vector<ral::io::Schema> schemas,
	std::vector<std::string> table_names,
	std::vector<std::string> table_scans,
	const bool transform_operators_bigger_than_gpu) : root(root),context(context),
	input_loaders(input_loaders),schemas(schemas),table_names(table_names),
	table_scans(table_scans), transform_operators_bigger_than_gpu(transform_operators_bigger_than_gpu){

	}

	std::shared_ptr<kernel> make_kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<ral::cache::graph> query_graph) {
		std::shared_ptr<kernel> k;
		auto kernel_context = this->context->clone();
		this->context->incrementQueryStep();
		if ( is_project(expr) ) {
			k = std::make_shared<Projection>(kernel_id,expr, kernel_context, query_graph);
		} else if ( is_filter(expr) ) {
			k = std::make_shared<Filter>(kernel_id,expr, kernel_context, query_graph);

		} else if ( is_logical_scan(expr) ) {
			size_t table_index = get_table_index(table_scans, expr);
			k = std::make_shared<TableScan>(kernel_id, expr, this->input_loaders[table_index].get_provider()->clone(),this->input_loaders[table_index].get_parser(), this->schemas[table_index], kernel_context, query_graph);
			// lets erase the input_loaders and corresponding table_name and table_scan so that if we have a repeated table_scan, we dont reuse it
			input_loaders.erase(input_loaders.begin() + table_index);
			table_names.erase(table_names.begin() + table_index);
			table_scans.erase(table_scans.begin() + table_index);
			schemas.erase(schemas.begin() + table_index);

		} else if (is_bindable_scan(expr)) {
			size_t table_index = get_table_index(table_scans, expr);
			k = std::make_shared<BindableTableScan>(kernel_id, expr, this->input_loaders[table_index].get_provider()->clone(),this->input_loaders[table_index].get_parser(), this->schemas[table_index], kernel_context, query_graph);
			// lets erase the input_loaders and corresponding table_name and table_scan so that if we have a repeated table_scan, we dont reuse it
			input_loaders.erase(input_loaders.begin() + table_index);
			table_names.erase(table_names.begin() + table_index);
			table_scans.erase(table_scans.begin() + table_index);
			schemas.erase(schemas.begin() + table_index);

		}  else if (is_single_node_partition(expr)) {
			k = std::make_shared<PartitionSingleNodeKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_partition(expr)) {
			k = std::make_shared<PartitionKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_sort_and_sample(expr)) {
			k = std::make_shared<SortAndSampleKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_only_sort(expr)) {
			k = std::make_shared<SortKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_concat_partitions_by_keys(expr)) {
			k = std::make_shared<ConcatPartitionsByKeysKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_split_by_keys(expr)) {
			k = std::make_shared<SplitByKeysKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_window_compute(expr)) {
			k = std::make_shared<ComputeWindowKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_merge(expr)) {
			k = std::make_shared<MergeStreamKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_limit(expr)) {
			k = std::make_shared<LimitKernel>(kernel_id,expr, kernel_context, query_graph);

		}  else if (is_compute_aggregate(expr)) {
			k = std::make_shared<ComputeAggregateKernel>(kernel_id,expr, kernel_context, query_graph);

		}  else if (is_distribute_aggregate(expr)) {
			k = std::make_shared<DistributeAggregateKernel>(kernel_id,expr, kernel_context, query_graph);

		}  else if (is_merge_aggregate(expr)) {
			k = std::make_shared<MergeAggregateKernel>(kernel_id,expr, kernel_context, query_graph);

		}  else if (is_pairwise_join(expr)) {
			k = std::make_shared<PartwiseJoin>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_join_partition(expr)) {
			k = std::make_shared<JoinPartitionKernel>(kernel_id,expr, kernel_context, query_graph);

		} else if (is_union(expr)) {
			k = std::make_shared<UnionKernel>(kernel_id,expr, kernel_context, query_graph);

		} else {
			RAL_FAIL("Invalid or unsupported expression: '" + expr + "' in the logical plan");
		}
		return k;
	}

	std::size_t expr_tree_from_json(std::size_t kernel_id, boost::property_tree::ptree const& p_tree, node * root_ptr, int level, std::shared_ptr<ral::cache::graph> query_graph) {
		auto expr = p_tree.get<std::string>("expr", "");
		root_ptr->expr = expr;
		root_ptr->level = level;
		root_ptr->kernel_unit = make_kernel(kernel_id, expr, query_graph);
		kernel_id++;
		for (auto &child : p_tree.get_child("children")) {
			auto child_node_ptr = std::make_shared<node>();
			root_ptr->children.push_back(child_node_ptr);
			kernel_id = expr_tree_from_json(kernel_id, child.second, child_node_ptr.get(), level + 1, query_graph);
		}
		return kernel_id;
	}

	boost::property_tree::ptree create_array_tree(boost::property_tree::ptree child){
		boost::property_tree::ptree children;
		children.push_back(std::make_pair("", child));
		return children;
	}

	void transform_json_tree(boost::property_tree::ptree &p_tree) {
		std::string expr = p_tree.get<std::string>("expr", "");
		if (is_sort(expr)){
			std::string limit_expr = expr;
			std::string merge_expr = expr;
			std::string partition_expr = expr;
			std::string sort_and_sample_expr = expr;

			if(ral::operators::has_limit_only(expr)){
				StringUtil::findAndReplaceAll(limit_expr, LOGICAL_SORT_TEXT, LOGICAL_LIMIT_TEXT);

				p_tree.put("expr", limit_expr);
			} else {
				if (this->context->getTotalNodes() == 1) {
					StringUtil::findAndReplaceAll(limit_expr, LOGICAL_SORT_TEXT, LOGICAL_LIMIT_TEXT);
					StringUtil::findAndReplaceAll(merge_expr, LOGICAL_SORT_TEXT, LOGICAL_MERGE_TEXT);
					StringUtil::findAndReplaceAll(partition_expr, LOGICAL_SORT_TEXT, LOGICAL_SINGLE_NODE_PARTITION_TEXT);
					StringUtil::findAndReplaceAll(sort_and_sample_expr, LOGICAL_SORT_TEXT, LOGICAL_SORT_AND_SAMPLE_TEXT);
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
			std::string merge_aggregate_expr = expr;
			std::string distribute_aggregate_expr = expr;
			std::string compute_aggregate_expr = expr;

			if (this->context->getTotalNodes() == 1) {
				StringUtil::findAndReplaceAll(merge_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_MERGE_AGGREGATE_TEXT);
				StringUtil::findAndReplaceAll(compute_aggregate_expr, LOGICAL_AGGREGATE_TEXT, LOGICAL_COMPUTE_AGGREGATE_TEXT);

				boost::property_tree::ptree agg_tree;
				agg_tree.put("expr", compute_aggregate_expr);
				agg_tree.add_child("children", p_tree.get_child("children"));

				p_tree.clear();

				p_tree.put("expr", merge_aggregate_expr);
				p_tree.put_child("children", create_array_tree(agg_tree));
			} else {
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
		}
		else if (is_join(expr)) {
			if (this->context->getTotalNodes() == 1) {
				// PartwiseJoin
				std::string pairwise_expr = expr;
				StringUtil::findAndReplaceAll(pairwise_expr, LOGICAL_JOIN_TEXT, LOGICAL_PARTWISE_JOIN_TEXT);
				p_tree.put("expr", pairwise_expr);
			} else {
				std::string pairwise_expr = expr;
				std::string join_partition_expr = expr;

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
		// special case when the query contains UNION
		else if (is_union(expr) && get_named_expression(expr, "all") == "false") {
			expr = "LogicalUnion(all=[true])";

			// when UNION, we want to do a group by on all columns without aggregations.
			std::string merge_aggregate_expr = LOGICAL_MERGE_AGGREGATE_TEXT + "(group=[{*}])";
			std::string compute_aggregate_expr = LOGICAL_COMPUTE_AGGREGATE_TEXT + "(group=[{*}])";
			std::string distribute_aggregate_expr = LOGICAL_DISTRIBUTE_AGGREGATE_TEXT + "(group=[{*}])";

			if (this->context->getTotalNodes() == 1) {

				boost::property_tree::ptree root_union_tree;
				root_union_tree.put("expr", expr);
				root_union_tree.add_child("children", p_tree.get_child("children"));

				boost::property_tree::ptree agg_union_tree;
				agg_union_tree.put("expr", compute_aggregate_expr);
				agg_union_tree.add_child("children", create_array_tree(root_union_tree));

				p_tree.clear();

				p_tree.put("expr", merge_aggregate_expr);
				p_tree.put_child("children", create_array_tree(agg_union_tree));

			} else {

				boost::property_tree::ptree root_union_tree;
				root_union_tree.put("expr", expr);
				root_union_tree.add_child("children", p_tree.get_child("children"));

				boost::property_tree::ptree compute_aggregate_tree;
				compute_aggregate_tree.put("expr", compute_aggregate_expr);
				compute_aggregate_tree.add_child("children", create_array_tree(root_union_tree));

				boost::property_tree::ptree distribute_aggregate_tree;
				distribute_aggregate_tree.put("expr", distribute_aggregate_expr);
				distribute_aggregate_tree.add_child("children", create_array_tree(compute_aggregate_tree));

				p_tree.clear();

				p_tree.put("expr", merge_aggregate_expr);
				p_tree.put_child("children", create_array_tree(distribute_aggregate_tree));
			}

		}
		/*
		There are 5 different scenarios to keep in mind:
			Window has a PARTITION BY clause
			Window has a ORDER BY clause using ROWS
			Window has a ORDER BY clause using RANGE
			Window has a PARTITION BY and ORDER BY clause using ROWS
			Window has a PARTITION BY and ORDER BY clause using RANGE
		*/
		else if (is_window(expr)) {
			// When the Window function contains the `partition by` clause
			if (is_partitioned(expr)) {
				std::string concat_partitions_by_key_expr = expr;
				std::string split_by_keys = expr;
				std::string sort_expr = expr;

				StringUtil::findAndReplaceAll(concat_partitions_by_key_expr, LOGICAL_WINDOW_TEXT, LOGICAL_CONCAT_PARTITIONS_BY_KEY_TEXT);
				StringUtil::findAndReplaceAll(split_by_keys, LOGICAL_WINDOW_TEXT, LOGICAL_SPLIT_BY_KEYS_TEXT);
				StringUtil::findAndReplaceAll(sort_expr, LOGICAL_WINDOW_TEXT, LOGICAL_ONLY_SORT_TEXT);

				boost::property_tree::ptree sort_tree;
				sort_tree.put("expr", sort_expr);
				sort_tree.add_child("children", p_tree.get_child("children"));

				boost::property_tree::ptree split_by_keys_tree;
				split_by_keys_tree.put("expr", split_by_keys);
				split_by_keys_tree.add_child("children", create_array_tree(sort_tree));

				p_tree.clear();

				p_tree.put("expr", concat_partitions_by_key_expr);
				p_tree.put_child("children", create_array_tree(split_by_keys_tree));

				// in case the Window function also contains an `order by` clause
				if (expr.find("order by") != std::string::npos) {
					sort_tree.clear();
					sort_tree.put("expr", sort_expr);
					sort_tree.put_child("children", create_array_tree(p_tree));

					p_tree = sort_tree;
				}
			} 
			// TODO: This could be improved, maybe we should just use the SortAndSampleKernel, PartitionKernel and MergeStreamKernel
			// FOr now AVOID Window Functions without PARTITION BY clause
			// When the Window function just contains the `order by` clause
			else {
				std::string sort_expr = expr;
				StringUtil::findAndReplaceAll(sort_expr, LOGICAL_WINDOW_TEXT, LOGICAL_ONLY_SORT_TEXT);

				p_tree.put("expr", sort_expr);
			}

			// Add the main Window function
			std::string window_expr = expr;

			StringUtil::findAndReplaceAll(window_expr, LOGICAL_WINDOW_TEXT, LOGICAL_COMPUTE_WINDOW_TEXT);

			boost::property_tree::ptree compute_window_tree;
			compute_window_tree.put("expr", window_expr);
			compute_window_tree.put_child("children", create_array_tree(p_tree));

			p_tree = compute_window_tree;
		}

		for (auto &child : p_tree.get_child("children")) {
			transform_json_tree(child.second);
		}
	}

	std::string to_string() {
		return to_string(&this->root, 0);
	}

	std::string to_string(node* p_tree, int level) {
		std::string str;

		for(int i = 0; i < level * 2; ++i) {
			str += " ";
		}

		str += "[" + std::to_string((int)p_tree->kernel_unit->get_type_id()) + "_" + std::to_string(p_tree->kernel_unit->get_id()) + "] "
				+ p_tree->expr;

		for (auto &child : p_tree->children) {
			str += "\n" + to_string(child.get(), level + 1);
		}

		return str;
	}

	std::tuple<std::shared_ptr<ral::cache::graph>,std::size_t> build_batch_graph(std::string json) {
		auto query_graph = std::make_shared<ral::cache::graph>();
		std::size_t max_kernel_id = 0;
		try {
			std::istringstream input(json);
			boost::property_tree::ptree p_tree;
			boost::property_tree::read_json(input, p_tree);
			transform_json_tree(p_tree);
			max_kernel_id = expr_tree_from_json(0, p_tree, &this->root, 0, query_graph);
		} catch (std::exception & e) {
			std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
			logger->error("|||{info}|||||", "info"_a="In build_batch_graph. What: {}"_format(e.what()));
			throw;
		}

		if (this->root.kernel_unit != nullptr) {
			query_graph->add_node(this->root.kernel_unit); // register first node
			visit(*query_graph, &this->root, this->root.children);
		}
		return std::make_tuple(query_graph, max_kernel_id);
	}

	void visit(ral::cache::graph& query_graph, node * parent, std::vector<std::shared_ptr<node>>& children) {
		for (size_t index = 0; index < children.size(); index++) {
			auto& child  =  children[index];
			visit(query_graph, child.get(), child->children);
			std::string port_name = "input";

			std::map<std::string, std::string> config_options = context->getConfigOptions();
			std::size_t join_partition_size_thresh = 400000000; // 400 MB
			auto it = config_options.find("JOIN_PARTITION_SIZE_THRESHOLD");
			if (it != config_options.end()){
				join_partition_size_thresh = std::stoull(config_options["JOIN_PARTITION_SIZE_THRESHOLD"]);
			}
			
			auto child_kernel_type = child->kernel_unit->get_type_id();
			auto parent_kernel_type = parent->kernel_unit->get_type_id();
			if (children.size() > 1) {
				char index_char = 'a' + index;
				port_name = std::string("input_");
				port_name.push_back(index_char);

				if (parent_kernel_type == kernel_type::PartwiseJoinKernel) {				

					auto join_type = static_cast<PartwiseJoin*>(parent->kernel_unit.get())->get_join_type();
					bool left_concat_all = join_type == ral::batch::RIGHT_JOIN || join_type == ral::batch::OUTER_JOIN || join_type == ral::batch::CROSS_JOIN;
					bool right_concat_all = join_type == ral::batch::LEFT_JOIN || join_type == ral::batch::OUTER_JOIN || join_type == ral::batch::CROSS_JOIN;
					bool concat_all = index == 0 ? left_concat_all : right_concat_all;
					cache_settings join_cache_machine_config = cache_settings{.type = CacheType::CONCATENATING, .num_partitions = 1, .context = context->clone(),
						.concat_cache_num_bytes = join_partition_size_thresh, .concat_all = concat_all};
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, parent->kernel_unit, port_name, join_cache_machine_config));					

				} else {
					cache_settings cache_machine_config;
					cache_machine_config.context = context->clone();

					query_graph.addPair(ral::cache::kpair(child->kernel_unit, parent->kernel_unit, port_name, cache_machine_config));
				}
			} else {
				
				if (child_kernel_type == kernel_type::JoinPartitionKernel && parent_kernel_type == kernel_type::PartwiseJoinKernel) {

					auto join_type = static_cast<PartwiseJoin*>(parent->kernel_unit.get())->get_join_type();
					bool left_concat_all = join_type == ral::batch::RIGHT_JOIN || join_type == ral::batch::OUTER_JOIN || join_type == ral::batch::CROSS_JOIN;
					bool right_concat_all = join_type == ral::batch::LEFT_JOIN || join_type == ral::batch::OUTER_JOIN || join_type == ral::batch::CROSS_JOIN;
					cache_settings left_cache_machine_config = cache_settings{.type = CacheType::CONCATENATING, .num_partitions = 1, .context = context->clone(),
						.concat_cache_num_bytes = join_partition_size_thresh, .concat_all = left_concat_all};
					cache_settings right_cache_machine_config = cache_settings{.type = CacheType::CONCATENATING, .num_partitions = 1, .context = context->clone(),
						.concat_cache_num_bytes = join_partition_size_thresh, .concat_all = right_concat_all};
					
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, "output_a", parent->kernel_unit, "input_a", left_cache_machine_config));
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, "output_b", parent->kernel_unit, "input_b", right_cache_machine_config));

				} else if ((child_kernel_type == kernel_type::SortAndSampleKernel && parent_kernel_type == kernel_type::PartitionKernel)
							|| (child_kernel_type == kernel_type::SortAndSampleKernel && parent_kernel_type == kernel_type::PartitionSingleNodeKernel)) {

					cache_settings cache_machine_config;
					cache_machine_config.context = context->clone();

					query_graph.addPair(ral::cache::kpair(child->kernel_unit, "output_a", parent->kernel_unit, "input_a", cache_machine_config));
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, "output_b", parent->kernel_unit, "input_b", cache_machine_config));

				} else if ((child_kernel_type == kernel_type::PartitionKernel && parent_kernel_type == kernel_type::MergeStreamKernel)
							|| (child_kernel_type == kernel_type::PartitionSingleNodeKernel && parent_kernel_type == kernel_type::MergeStreamKernel)) {

					int max_num_order_by_partitions_per_node = 8;
					it = config_options.find("MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE");
					if (it != config_options.end()){
						max_num_order_by_partitions_per_node = std::stoi(config_options["MAX_NUM_ORDER_BY_PARTITIONS_PER_NODE"]);
					}
					ral::cache::cache_settings cache_machine_config;
					cache_machine_config.type = ral::cache::CacheType::FOR_EACH;
					cache_machine_config.num_partitions = max_num_order_by_partitions_per_node;
					cache_machine_config.context = context->clone();
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, parent->kernel_unit, cache_machine_config));
					
				} else if(child_kernel_type == kernel_type::TableScanKernel || child_kernel_type == kernel_type::BindableTableScanKernel) {
					std::size_t concat_cache_num_bytes = 400000000; // 400 MB
					config_options = context->getConfigOptions();
					it = config_options.find("MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE");
					if (it != config_options.end()){
						concat_cache_num_bytes = std::stoull(config_options["MAX_DATA_LOAD_CONCAT_CACHE_BYTE_SIZE"]);
					}
					
					cache_settings cache_machine_config = cache_settings{.type = CacheType::CONCATENATING, .num_partitions = 1, .context = context->clone(),
						.concat_cache_num_bytes = concat_cache_num_bytes, .concat_all = false};
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, parent->kernel_unit, cache_machine_config));
				} else {
					cache_settings cache_machine_config;
					cache_machine_config.context = context->clone();
					query_graph.addPair(ral::cache::kpair(child->kernel_unit, parent->kernel_unit, cache_machine_config));
				}
			}
		}
	}
};


} // namespace batch
} // namespace ral
