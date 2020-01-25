#include <cudf/table/table_view.hpp>
#include <cudf/hashing.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>

#include "LogicalFilter.h"
#include "../../CalciteExpressionParsing.h"
#include "../../JoinProcessor.h"



#include "blazingdb/transport/Node.h"

#include "distribution/primitives.h"
#include "communication/CommunicationData.h"
#include "utilities/CommonOperations.h"

#include "../../Interpreter/interpreter_cpp.h"

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

std::unique_ptr<cudf::column> evaluate_expression(
  const cudf::table_view & table,
  const std::string & expression,
  cudf::data_type output_type) {
  using interops::column_index_type;

  if(is_var_column(expression)) {
    // special case when there is nothing to evaluate in the condition expression i.e. LogicalFilter(condition=[$16])
    cudf::size_type index = get_index(expression);
    auto col_view = table.column(index);

    RAL_EXPECTS(col_view.type() == output_type, "Expression evaluates to incorrect type");

    return std::make_unique<cudf::column>(col_view);
  }

  std::string clean_expression = clean_calcite_expression(expression);
  std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
  fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(table, tokens);
  
  // Keep track of which columns are used in the expression
  std::vector<bool> col_used_in_expression(table.num_columns(), false);
  for(const auto & token : tokens) {
    if(is_var_column(token)) {
      cudf::size_type index = get_index(token);
      col_used_in_expression[index] = true;
    }
  }

  // Get the needed columns indices in order and keep track of the mapped indices
  std::vector<cudf::size_type> input_col_indices;
  std::map<column_index_type, column_index_type> col_idx_map;
  for(size_t i = 0; i < col_used_in_expression.size(); i++) {
    if(col_used_in_expression[i]) {
      col_idx_map.insert({i, col_idx_map.size()});
      input_col_indices.push_back(i);
    }
  }

  cudf::table_view filtered_table = table.select(input_col_indices);
  std::vector<column_index_type> left_inputs;
  std::vector<column_index_type> right_inputs;
  std::vector<column_index_type> outputs;
  std::vector<column_index_type> final_output_positions = {filtered_table.num_columns()};
  std::vector<interops::operator_type> operators;
  std::vector<std::unique_ptr<cudf::scalar>> left_scalars;
  std::vector<std::unique_ptr<cudf::scalar>> right_scalars;
  
  interops::add_expression_to_interpreter_plan(tokens,
                                              filtered_table,
                                              col_idx_map,
                                              0,
                                              1,
                                              left_inputs,
                                              right_inputs,
                                              outputs,
                                              final_output_positions,
                                              operators,
                                              left_scalars,
                                              right_scalars);

  auto ret = cudf::make_fixed_width_column(output_type, table.num_rows(), cudf::mask_state::UNINITIALIZED); 
  cudf::mutable_table_view ret_view {{ret->mutable_view()}};
  interops::perform_interpreter_operation(ret_view,
                                          filtered_table,
                                          left_inputs,
                                          right_inputs,
                                          outputs,
                                          final_output_positions,
                                          operators,
                                          left_scalars,
                                          right_scalars);

  return ret;
}

std::unique_ptr<ral::frame::BlazingTable> process_filter(
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::experimental::Context * context) {

	cudf::table_view table_view = table.view();
	if(table_view.num_rows() == 0) {
		return std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(table_view), table.names());
	}
	
  std::string conditional_expression = get_named_expression(query_part, "condition");
	if(conditional_expression.empty()) {
		conditional_expression = get_named_expression(query_part, "filters");
	}

  auto bool_mask = evaluate_expression(table_view, conditional_expression, cudf::data_type{cudf::type_id::BOOL8});

  return applyBooleanFilter(table, *bool_mask);
}

std::vector<std::unique_ptr<ral::frame::BlazingTable> > hashPartition(
  const ral::frame::BlazingTableView & table,
  std::vector<cudf::size_type> const& columns_to_hash,
  int numPartitions){

  auto result = hash_partition(table.view(),
  columns_to_hash,
  numPartitions);

  std::vector<std::unique_ptr<ral::frame::BlazingTable> > partitionedTables;

  for(auto & partition : result){
    partitionedTables.push_back(
      std::make_unique<ral::frame::BlazingTable>(
        std::move(partition),
        table.names())
      );
  }
    return partitionedTables;

}


  namespace{
    typedef std::pair<blazingdb::transport::experimental::Node, std::unique_ptr<ral::frame::BlazingTable> > NodeColumn;
    typedef std::pair<blazingdb::transport::experimental::Node, ral::frame::BlazingTableView > NodeColumnView;
  }

  std::unique_ptr<ral::frame::BlazingTable> process_distribution_table(
  	const ral::frame::BlazingTableView & table,
    std::vector<int> & columnIndices,
    blazingdb::manager::experimental::Context * context) {

    //TODO: CACHE_POINT (ask felipe)
    std::vector<std::unique_ptr<ral::frame::BlazingTable> > partitionedData = hashPartition(
      table,
      columnIndices,
      context->getTotalNodes());

    std::vector<NodeColumnView > partitions;
    for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
      partitions.push_back(
        std::make_pair(context->getNode(nodeIndex),
         partitionedData[nodeIndex]->toBlazingTableView()));
    }

  	context->incrementQuerySubstep();
    //TODO: uncomment when implemented
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
        std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> > distributed_tables = process_hash_based_distribution(
            table_left, table_right, expression, context);

        return processJoin(distributed_tables.first->toBlazingTableView(), distributed_tables.second->toBlazingTableView(), expression);
    }
  }



// This function can either do a small table scatter distribution or regular hash based. 
  std::pair<std::unique_ptr<ral::frame::BlazingTable>, std::unique_ptr<ral::frame::BlazingTable> >  process_distribution(
    const ral::frame::BlazingTableView & left,
    const ral::frame::BlazingTableView & right,
    const std::string & query,
    blazingdb::manager::experimental::Context * context) {

  	// First lets find out if we are joining against a small table. If so, we will want to replicate that small table
  	
  // 	int self_node_idx = context->getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode());

  // 	context->incrementQuerySubstep();
  // 	cudf::size_type local_num_rows_left = left.view().num_rows();
  // 	cudf::size_type local_num_rows_right = right.view().num_rows();
  // 	ral::distribution::distributeLeftRightNumRows(context, local_num_rows_left, local_num_rows_right);
  // 	std::vector<cudf::size_type> nodes_num_rows_left;
  // 	std::vector<cudf::size_type> nodes_num_rows_right;
  // 	ral::distribution::collectLeftRightNumRows(context, nodes_num_rows_left, nodes_num_rows_right);
  // 	nodes_num_rows_left[self_node_idx] = local_num_rows_left;
  // 	nodes_num_rows_right[self_node_idx] = local_num_rows_right;

  // 	cudf::size_type total_rows_left = std::accumulate(nodes_num_rows_left.begin(), nodes_num_rows_left.end(), 0);
  // 	cudf::size_type total_rows_right = std::accumulate(nodes_num_rows_right.begin(), nodes_num_rows_right.end(), 0);

  // 	size_t row_width_bytes_left = 0;
  // 	size_t row_width_bytes_right = 0;
  // 	for(auto & column : tables[0]) {
  // 		// TODO percy cudf0.12 port to cudf::column custring
  // //		if(column.dtype() == GDF_STRING_CATEGORY)
  // //			row_width_bytes_left += ral::traits::get_dtype_size_in_bytes(column.dtype()) *
  // //									5;  // WSM TODO. Here i am adding a fudge factor trying to account for the fact that
  // //										// strings can occupy a lot more. We needa better way to easily figure out how
  // //										// mmuch space a string column actually occupies
  // //		else
  // //			row_width_bytes_left += ral::traits::get_dtype_size_in_bytes(column.dtype());
  // 	}
  // 	for(auto & column : tables[1]) {
  // 		// TODO percy cudf0.12 port to cudf::column custring
  // //		if(column.dtype() == GDF_STRING_CATEGORY)
  // //			row_width_bytes_right += ral::traits::get_dtype_size_in_bytes(column.dtype()) *
  // //									 5;  // WSM TODO. Here i am adding a fudge factor trying to account for the fact
  // //										 // that strings can occupy a lot more. We needa better way to easily figure out
  // //										 // how mmuch space a string column actually occupies
  // //		else
  // //			row_width_bytes_right += ral::traits::get_dtype_size_in_bytes(column.dtype());
  // 	}
  // 	int num_nodes = context->getTotalNodes();

  // 	bool scatter_left = false;
  // 	bool scatter_right = false;
  // 	size_t estimate_regular_distribution =
  // 		(total_rows_left * row_width_bytes_left + total_rows_right * row_width_bytes_right) * (num_nodes - 1) /
  // 		num_nodes;
  // 	size_t estimate_scatter_left = (total_rows_left * row_width_bytes_left) * (num_nodes - 1);
  // 	size_t estimate_scatter_right = (total_rows_right * row_width_bytes_right) * (num_nodes - 1);
  // 	size_t MAX_SCATTER_MEM_OVERHEAD = 500000000;  // 500Mb  how much extra memory consumption per node are we ok with
  // 												  // WSM TODO get this value from config
  // 	if(estimate_scatter_left < estimate_regular_distribution ||
  // 		estimate_scatter_right < estimate_regular_distribution) {
  // 		if(estimate_scatter_left < estimate_scatter_right &&
  // 			total_rows_left * row_width_bytes_left < MAX_SCATTER_MEM_OVERHEAD) {
  // 			scatter_left = true;
  // 		} else if(estimate_scatter_right < estimate_scatter_left &&
  // 				  total_rows_right * row_width_bytes_right < MAX_SCATTER_MEM_OVERHEAD) {
  // 			scatter_right = true;
  // 		}
  // 	}

  // 	if(scatter_left || scatter_right) {
  // 		context->incrementQuerySubstep();
  // 		int num_to_collect = 0;
  // 		std::vector<gdf_column_cpp> data_to_scatter;
  // 		if(scatter_left) {
  // 			Library::Logging::Logger().logTrace(
  // 				ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  // 					std::to_string(context->getQueryStep()),
  // 					std::to_string(context->getQuerySubstep()),
  // 					"join process_distribution scatter_left"));
  // 			data_to_scatter = frame.get_table(0);
  // 			if(local_num_rows_left > 0) {
  // 				ral::distribution::scatterData(*context, data_to_scatter);
  // 			}
  // 			for(size_t i = 0; i < nodes_num_rows_left.size(); i++) {
  // 				if(i != self_node_idx && nodes_num_rows_left[i] > 0) {
  // 					num_to_collect++;
  // 				}
  // 			}
  // 		} else {
  // 			Library::Logging::Logger().logTrace(
  // 				ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  // 					std::to_string(context->getQueryStep()),
  // 					std::to_string(context->getQuerySubstep()),
  // 					"join process_distribution scatter_right"));
  // 			data_to_scatter = frame.get_table(1);
  // 			if(local_num_rows_right > 0) {  // this node has data on the right to scatter
  // 				ral::distribution::experimental::scatterData(context, data_to_scatter);
  // 			}
  // 			for(size_t i = 0; i < nodes_num_rows_right.size(); i++) {
  // 				if(i != self_node_idx && nodes_num_rows_right[i] > 0) {
  // 					num_to_collect++;
  // 				}
  // 			}
  // 		}

  // 		blazing_frame join_frame;
  // 		std::vector<gdf_column_cpp> cluster_shared_table;
  // 		if(num_to_collect > 0) {
  // 			std::vector<NodeColumns> collected_partitions =
  // 				ral::distribution::collectSomePartitions(*context, num_to_collect);
  // 			cluster_shared_table = concat_columns(data_to_scatter, collected_partitions);
  // 		} else {
  // 			cluster_shared_table = data_to_scatter;
  // 		}

  // 		if(scatter_left) {
  // 			join_frame.add_table(cluster_shared_table);
  // 			join_frame.add_table(tables[1]);
  // 		} else {
  // 			join_frame.add_table(tables[0]);
  // 			join_frame.add_table(cluster_shared_table);
  // 		}
  // 		return join_frame;

  // 	} else {
  // 		Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  // 			std::to_string(context->getQueryStep()),
  // 			std::to_string(context->getQuerySubstep()),
  // 			"join process_distribution hash based distribution"));
  // 		return process_hash_based_distribution(frame, query);
  // 	}
  //   return process_hash_based_distribution(left, right, query, context);
  }


std::unique_ptr<ral::frame::BlazingTable> processJoin(
  const ral::frame::BlazingTableView & table_left,
  const ral::frame::BlazingTableView & table_right,
  const std::string & expression){

  std::string condition = get_named_expression(expression, "condition");
  std::string join_type = get_named_expression(expression, "joinType");

  std::vector<int> column_indices;
  parseJoinConditionToColumnIndices(condition, column_indices);

  std::vector<cudf::size_type> left_column_indices;
  std::vector<cudf::size_type> right_column_indices;
  for(int i = 0; i < column_indices.size();i++){
    if(column_indices[i] >= table_left.view().num_columns()){
      right_column_indices.push_back(column_indices[i] - table_left.view().num_columns());
    }else{
      left_column_indices.push_back(column_indices[i]);
    }
  }

  std::vector<std::pair<cudf::size_type, cudf::size_type>> columns_in_common(left_column_indices.size());
  for(int i = 0; i < left_column_indices.size(); i++){
    columns_in_common[i].first = left_column_indices[i];
    columns_in_common[i].second = right_column_indices[i];
  }

  if(join_type == INNER_JOIN) {
    auto result = cudf::experimental::inner_join(
      table_left.view(),
      table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);

  } else if(join_type == LEFT_JOIN) {
    auto result = cudf::experimental::left_join(
      table_left.view(),
      table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);

  } else if(join_type == OUTER_JOIN) {
    auto result = cudf::experimental::full_join(
      table_left.view(),
      table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);

  } else {
    throw std::runtime_error("In evaluate_join function: unsupported join operator, " + join_type);
  }
}

} // namespace processor
} // namespace ral
