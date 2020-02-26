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

#include "blazingdb/transport/Node.h"

#include "distribution/primitives.h"
#include "communication/CommunicationData.h"
#include "utilities/CommonOperations.h"
#include "utilities/DebuggingUtils.h"
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
    result_table = cudf::experimental::inner_join(
      table_left.view(),
      table_right.view(),
      left_column_indices,
      right_column_indices,
      columns_in_common);
  } else if(join_type == LEFT_JOIN) {
    result_table = cudf::experimental::left_join(
      table_left.view(),
      table_right.view(),
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
