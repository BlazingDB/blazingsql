#include <stack>

#include <cudf/table/table_view.hpp>
#include <cudf/hashing.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>

#include "LogicalFilter.h"
#include "LogicalProject.h"
#include "../../CalciteExpressionParsing.h"
#include <blazingdb/io/Library/Logging/Logger.h>

#include "blazingdb/transport/Node.h"

#include "distribution/primitives.h"
#include "communication/CommunicationData.h"
#include "utilities/CommonOperations.h"
#include "utilities/DebuggingUtils.h"
#include "utilities/StringUtils.h"
#include "Utils.cuh"

#include "../../Interpreter/interpreter_cpp.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"

namespace ral {
namespace processor {
namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

} // namespace

bool is_logical_filter(const std::string & query_part) {
  return query_part.find(LOGICAL_FILTER) != std::string::npos;
}

std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView & boolValues){
  auto filteredTable = cudf::experimental::apply_boolean_mask(
    table.view(),boolValues);
  return std::make_unique<ral::frame::BlazingTable>(std::move(
    filteredTable),table.names());
}

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  const ral::frame::BlazingTableView & table_view,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context) {

	if(table_view.num_rows() == 0) {
		return std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(table_view.view()), table_view.names());
	}
	
  std::string conditional_expression = get_named_expression(query_part, "condition");
	if(conditional_expression.empty()) {
		conditional_expression = get_named_expression(query_part, "filters");
	}

  std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = evaluate_expressions(table_view.toBlazingColumns(), {conditional_expression});

  RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression does not evaluate to a boolean mask");

  return applyBooleanFilter(table_view, evaluated_table[0]->view());
}


  namespace{
    typedef std::pair<blazingdb::transport::experimental::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
    typedef std::pair<blazingdb::transport::experimental::Node, ral::frame::BlazingTableView > NodeColumnView;
  }

  void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices) {
	// TODO: right now this only works for equijoins
	// since this is all that is implemented at the time

	// TODO: for this to work properly we can only do multi column join
	// when we have ands, when we have hors we hvae to perform the joisn seperately then
	// do a unique merge of the indices

	// right now with pred push down the join codnition takes the filters as the second argument to condition

	std::string clean_expression = clean_calcite_expression(condition);
	int operator_count = 0;
	std::stack<std::string> operand;
	// NOTE percy c.cordoba here we dont need to call fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp
	// after
	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			if(token == "=") {
				// so far only equijoins are supported in libgdf
				operator_count++;
			} else if(token != "AND") {
				throw std::runtime_error("In evaluate_join function: unsupported non-equijoins operator");
			}
		} else {
			operand.push(token);
		}
	}

	columnIndices.resize(2 * operator_count);
	for(size_t i = 0; i < operator_count; i++) {
		int right_index = get_index(operand.top());
		operand.pop();
		int left_index = get_index(operand.top());
		operand.pop();

		if(right_index < left_index) {
			std::swap(left_index, right_index);
		}

		columnIndices[2 * i] = left_index;
		columnIndices[2 * i + 1] = right_index;
	}
}

  std::unique_ptr<ral::frame::BlazingTable> process_distribution_table(
  	const ral::frame::BlazingTableView & table,
    std::vector<int> & columnIndices,
    blazingdb::manager::experimental::Context * context) {

    std::vector<NodeColumnView > partitions;
    std::unique_ptr<CudfTable> hashed_data;
    if (table.num_rows() > 0){    
      //TODO: CACHE_POINT (ask felipe)
      std::vector<cudf::size_type> columns_to_hash;
      std::transform(columnIndices.begin(), columnIndices.end(), std::back_inserter(columns_to_hash), [](int index) { return (cudf::size_type)index; });
      
      std::vector<cudf::size_type> hased_data_offsets;
      std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(table.view(),
              columns_to_hash, context->getTotalNodes());

      // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
      std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
      std::vector<CudfTableView> partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);
      
      for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
          partitions.emplace_back(
            std::make_pair(context->getNode(nodeIndex), ral::frame::BlazingTableView(partitioned[nodeIndex], table.names())));
      }
    } else {
      for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
        partitions.emplace_back(
          std::make_pair(context->getNode(nodeIndex), ral::frame::BlazingTableView(table.view(), table.names())));
      }
    }

  	context->incrementQuerySubstep();
    ral::distribution::experimental::distributePartitions(context, partitions);
    std::vector<NodeColumn> remote_node_columns = ral::distribution::experimental::collectPartitions(context);

    std::vector<ral::frame::BlazingTableView> partitions_to_concat;
    for (int i = 0; i < remote_node_columns.size(); i++){
      partitions_to_concat.emplace_back(remote_node_columns[i].second->toBlazingTableView());
    }
    bool found_self_partition = false;
    for (auto partition : partitions){
      if (partition.first == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
        partitions_to_concat.emplace_back(partition.second);
        found_self_partition = true;
        break;
      }
    }
    assert(found_self_partition);
    return ral::utilities::experimental::concatTables(partitions_to_concat);
  }


  std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> > process_hash_based_distribution(
  	const ral::frame::BlazingTableView & left,
    const ral::frame::BlazingTableView & right,
    const std::string & query,
    blazingdb::manager::experimental::Context * context) {

    std::vector<int> globalColumnIndices;
  	parseJoinConditionToColumnIndices(get_named_expression(query, "condition"), globalColumnIndices);

    std::vector<int> localIndicesLeft;
    std::vector<int> localIndicesRight;

    std::for_each(globalColumnIndices.begin(), globalColumnIndices.end(), [&](int i) {
      if(i < left.view().num_columns()){
        localIndicesLeft.push_back(i);
      }else{
        localIndicesRight.push_back(i - left.view().num_columns());
      }
    });

    return std::make_pair(
      std::move(process_distribution_table(left, localIndicesLeft,context)),
      std::move(process_distribution_table(right, localIndicesRight, context))
    );
  }

  std::unique_ptr<ral::frame::BlazingTable> process_logical_join(blazingdb::manager::experimental::Context * context,
        const ral::frame::BlazingTableView & table_left,
        const ral::frame::BlazingTableView & table_right,
        const std::string & expression){
      
    if(context->getTotalNodes() <= 1) { // if single node
        return processJoin(table_left, table_right, expression);
    } else {
        std::string join_type = get_named_expression(expression, "joinType");
        if(join_type == INNER_JOIN) { // TODO WSM: We can actually scatter the right table for a left outer join as well
          std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> > distributed_tables = process_optimal_inner_join_distribution(
              table_left, table_right, expression, context);

          return processJoin(distributed_tables.first->toBlazingTableView(), distributed_tables.second->toBlazingTableView(), expression);

        } else {
          std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> > distributed_tables = process_hash_based_distribution(
              table_left, table_right, expression, context);
          
          Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context->getContextToken()),
            std::to_string(context->getQueryStep()),
            std::to_string(context->getQuerySubstep()),
            "join process_distribution hash based distribution"));

          return processJoin(distributed_tables.first->toBlazingTableView(), distributed_tables.second->toBlazingTableView(), expression);
        }
    }
  }

std::unique_ptr<ral::frame::BlazingTable> process_join(const ral::frame::BlazingTableView & table_left,
													   const ral::frame::BlazingTableView & table_right,
													   const std::string & expression,
													   blazingdb::manager::experimental::Context * context) {
	return process_logical_join(context, table_left, table_right, expression);
}


// This function can either do a small table scatter distribution or regular hash based. 
  std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> >  process_optimal_inner_join_distribution(
    const ral::frame::BlazingTableView & left,
    const ral::frame::BlazingTableView & right,
    const std::string & query,
    blazingdb::manager::experimental::Context * context) {

  	// First lets find out if we are joining against a small table. If so, we will want to replicate that small table
  	
  	int self_node_idx = context->getNodeIndex(ral::communication::experimental::CommunicationData::getInstance().getSelfNode());

  	context->incrementQuerySubstep();
  	ral::distribution::experimental::distributeLeftRightTableSizeBytes(context, left, right);
  	std::vector<int64_t> nodes_num_bytes_left;
  	std::vector<int64_t> nodes_num_bytes_right;
  	ral::distribution::experimental::collectLeftRightTableSizeBytes(context, nodes_num_bytes_left, nodes_num_bytes_right);
  	nodes_num_bytes_left[self_node_idx] = ral::utilities::experimental::get_table_size_bytes(left);
  	nodes_num_bytes_right[self_node_idx] = ral::utilities::experimental::get_table_size_bytes(right);

    int64_t total_bytes_left = std::accumulate(nodes_num_bytes_left.begin(), nodes_num_bytes_left.end(), int64_t(0));
  	int64_t total_bytes_right = std::accumulate(nodes_num_bytes_right.begin(), nodes_num_bytes_right.end(), int64_t(0));

   	int num_nodes = context->getTotalNodes();

  	bool scatter_left = false;
  	bool scatter_right = false;
  	int64_t estimate_regular_distribution = (total_bytes_left + total_bytes_right) * (num_nodes - 1) / num_nodes;
  	int64_t estimate_scatter_left = (total_bytes_left) * (num_nodes - 1);
  	int64_t estimate_scatter_right = (total_bytes_right) * (num_nodes - 1);
  	int64_t MAX_SCATTER_MEM_OVERHEAD = 500000000;  // 500Mb  how much extra memory consumption per node are we ok with
  												  // WSM TODO get this value from config

   	if(estimate_scatter_left < estimate_regular_distribution ||
  		estimate_scatter_right < estimate_regular_distribution) {
  		if(estimate_scatter_left < estimate_scatter_right &&
  			total_bytes_left < MAX_SCATTER_MEM_OVERHEAD) {
  			scatter_left = true;
  		} else if(estimate_scatter_right < estimate_scatter_left &&
  				  total_bytes_right < MAX_SCATTER_MEM_OVERHEAD) {
  			scatter_right = true;
  		}
  	}

  	if(scatter_left || scatter_right) {
  		context->incrementQuerySubstep();
  		int num_to_collect = 0;
  		if(scatter_left) {
  			Library::Logging::Logger().logTrace(
  				ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  					std::to_string(context->getQueryStep()),
  					std::to_string(context->getQuerySubstep()),
  					"join process_distribution scatter_left bytes: " + std::to_string(total_bytes_left)));
  			if(left.num_rows() > 0) {
  				ral::distribution::experimental::scatterData(context, left);
  			}
  			for(size_t i = 0; i < nodes_num_bytes_left.size(); i++) {
  				if(i != self_node_idx && nodes_num_bytes_left[i] > 0) {
  					num_to_collect++;
  				}
  			}
  		} else {
  			Library::Logging::Logger().logTrace(
  				ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  					std::to_string(context->getQueryStep()),
  					std::to_string(context->getQuerySubstep()),
  					"join process_distribution scatter_right bytes: " + std::to_string(total_bytes_right)));
  			if(right.num_rows() > 0) {  // this node has data on the right to scatter
  				ral::distribution::experimental::scatterData(context, right);
  			}
  			for(size_t i = 0; i < nodes_num_bytes_right.size(); i++) {
  				if(i != self_node_idx && nodes_num_bytes_right[i] > 0) {
  					num_to_collect++;
  				}
  			}
  		}

      std::unique_ptr<ral::frame::BlazingTable> cluster_shared_table;
      if(num_to_collect > 0) {
  			std::vector<NodeColumn> collected_partitions =
  				  ral::distribution::experimental::collectSomePartitions(context, num_to_collect);
        std::vector<ral::frame::BlazingTableView> partitions_to_concat;
        for (int i = 0; i < collected_partitions.size(); i++){
          partitions_to_concat.emplace_back(collected_partitions[i].second->toBlazingTableView());
        }
        if (scatter_left && left.num_rows() > 0){
          partitions_to_concat.push_back(left);
        } else if (scatter_right && right.num_rows() > 0){
          partitions_to_concat.push_back(right);
        }
        cluster_shared_table = ral::utilities::experimental::concatTables(partitions_to_concat);
      } else {
        if (scatter_left && left.num_rows() >= 0){
          cluster_shared_table = std::make_unique<ral::frame::BlazingTable>(left.view(), left.names());
        } else if (scatter_right && right.num_rows() >= 0){
          cluster_shared_table = std::make_unique<ral::frame::BlazingTable>(right.view(), right.names());
        }  			
  		}

      Library::Logging::Logger().logTrace(
  				ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  					std::to_string(context->getQueryStep()),
  					std::to_string(context->getQuerySubstep()),
  					"join process_distribution scatter complete"));

  		if(scatter_left) {
        std::unique_ptr<ral::frame::BlazingTable> right_table = std::make_unique<ral::frame::BlazingTable>(right.view(), right.names());
  			return std::make_pair(std::move(cluster_shared_table), std::move(right_table));
  		} else {
  			std::unique_ptr<ral::frame::BlazingTable> left_table = std::make_unique<ral::frame::BlazingTable>(left.view(), left.names());
  			return std::make_pair(std::move(left_table), std::move(cluster_shared_table));
  		}  		

  	} else {
  		Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  			std::to_string(context->getQueryStep()),
  			std::to_string(context->getQuerySubstep()),
  			"join process_distribution hash based distribution"));
  		return process_hash_based_distribution(left, right, query, context);
  	}
  }

bool check_if_has_nulls(CudfTableView const& input, std::vector<cudf::size_type> const& keys){
  auto keys_view = input.select(keys);
  if (keys_view.num_columns() != 0 && keys_view.num_rows() != 0 && cudf::has_nulls(keys_view)) {
      return true;
  }

  return false;
}

std::unique_ptr<ral::frame::BlazingTable> processJoin(
  const ral::frame::BlazingTableView & table_left_in,
  const ral::frame::BlazingTableView & table_right_in,
  const std::string & expression){

  std::string condition = get_named_expression(expression, "condition");
  std::string join_type = get_named_expression(expression, "joinType");

  std::vector<int> column_indices;
  parseJoinConditionToColumnIndices(condition, column_indices);

  std::vector<cudf::size_type> left_column_indices;
  std::vector<cudf::size_type> right_column_indices;
  for(int i = 0; i < column_indices.size();i++){
    if(column_indices[i] >= table_left_in.view().num_columns()){
      right_column_indices.push_back(column_indices[i] - table_left_in.view().num_columns());
    }else{
      left_column_indices.push_back(column_indices[i]);
    }
  }

  // Here lets make sure that the columns being joined are of the same type so that we can join them properly
  std::vector<std::unique_ptr<ral::frame::BlazingColumn>> left_columns = table_left_in.toBlazingColumns();
  std::vector<std::unique_ptr<ral::frame::BlazingColumn>> right_columns = table_right_in.toBlazingColumns();
  for (size_t i = 0; i < left_column_indices.size(); i++){
    if (left_columns[left_column_indices[i]]->view().type().id() != right_columns[right_column_indices[i]]->view().type().id()){
      std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns_to_normalize;
      columns_to_normalize.emplace_back(std::move(left_columns[left_column_indices[i]]));
      columns_to_normalize.emplace_back(std::move(right_columns[right_column_indices[i]]));
      std::vector<std::unique_ptr<ral::frame::BlazingColumn>> normalized_columns = ral::utilities::experimental::normalizeColumnTypes(std::move(columns_to_normalize));
      left_columns[left_column_indices[i]] = std::move(normalized_columns[0]);
      right_columns[right_column_indices[i]] = std::move(normalized_columns[1]);
    }
  }
  std::unique_ptr<ral::frame::BlazingTable> left_blazing_table = std::make_unique<ral::frame::BlazingTable>(std::move(left_columns), table_left_in.names());
  std::unique_ptr<ral::frame::BlazingTable> right_blazing_table = std::make_unique<ral::frame::BlazingTable>(std::move(right_columns), table_right_in.names());
  ral::frame::BlazingTableView table_left = left_blazing_table->toBlazingTableView();
  ral::frame::BlazingTableView table_right = right_blazing_table->toBlazingTableView();

  std::vector<std::pair<cudf::size_type, cudf::size_type>> columns_in_common;

  std::unique_ptr<CudfTable> result_table;
  std::vector<std::string> result_names = table_left.names();
  std::vector<std::string> right_names = table_right.names();
  result_names.insert(result_names.end(), right_names.begin(), right_names.end());

  // need to validate that tables are not empty
  if (table_left.num_columns() == 0 || table_right.num_columns() == 0){
    RAL_FAIL("Tables need at least one column to join on");
  }

  if(join_type == INNER_JOIN) {
    //Removing nulls on key columns before joining
    std::unique_ptr<CudfTable> table_left_dropna;
    std::unique_ptr<CudfTable> table_right_dropna;

    bool has_nulls_left = check_if_has_nulls(table_left.view(), left_column_indices);
    bool has_nulls_right = check_if_has_nulls(table_right.view(), right_column_indices);

    if(has_nulls_left){
      table_left_dropna = cudf::experimental::drop_nulls(table_left.view(), left_column_indices);
    }
    if(has_nulls_right){
      table_right_dropna = cudf::experimental::drop_nulls(table_right.view(), right_column_indices);
    }

    result_table = cudf::experimental::inner_join(
      has_nulls_left ? table_left_dropna->view() : table_left.view(),
      has_nulls_right ? table_right_dropna->view() : table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);
  } else if(join_type == LEFT_JOIN) {
    //Removing nulls on right key columns before joining
    std::unique_ptr<CudfTable> table_right_dropna;

    bool has_nulls_right = check_if_has_nulls(table_right.view(), right_column_indices);

    if(has_nulls_right){
      table_right_dropna = cudf::experimental::drop_nulls(table_right.view(), right_column_indices);
    }

    result_table = cudf::experimental::left_join(
      table_left.view(),
      has_nulls_right ? table_right_dropna->view() : table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);
  } else if(join_type == OUTER_JOIN) {
    result_table = cudf::experimental::full_join(
      table_left.view(),
      table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);
  } else {
    RAL_FAIL("Unsupported join operator");
  }

  return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), result_names);
}

} // namespace processor
} // namespace ral
