#include <cudf/table/table_view.hpp>
#include <cudf/hashing.hpp>
#include <cudf/join.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/copying.hpp>

#include "LogicalFilter.h"
#include "../../CalciteExpressionParsing.h"
#include "../../JoinProcessor.h"
#include "../../Interpreter/interpreter_cpp.h"

namespace ral {
namespace processor {
namespace {

const std::string LOGICAL_FILTER = "LogicalFilter";

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

std::unique_ptr<cudf::column> boolean_mask_from_expression(
  const cudf::table_view & table,
  const std::string & expression) {
  using interops::column_index_type;

  if(is_var_column(expression)) {
    // special case when there is nothing to evaluate in the condition expression i.e. LogicalFilter(condition=[$16])
    cudf::size_type index = get_index(expression);
    auto col_view = table.column(index);

    RAL_EXPECTS(col_view.type().id() == cudf::type_id::BOOL8, "Column must be of type boolean");

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
      col_idx_map[i] = col_idx_map.size();
      input_col_indices.push_back(i);
    }
  }

  cudf::table_view filtered_table = table.select(input_col_indices);
  std::vector<column_index_type> left_inputs;
  std::vector<column_index_type> right_inputs;
  std::vector<column_index_type> outputs;
  std::vector<column_index_type> final_output_positions = {filtered_table.num_columns()};
  std::vector<gdf_binary_operator_exp> operators;
  std::vector<gdf_unary_operator> unary_operators;
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
                                              unary_operators,
                                              left_scalars,
                                              right_scalars);

  auto ret = cudf::make_numeric_column(cudf::data_type{cudf::type_id::BOOL8}, table.num_rows(), cudf::mask_state::UNINITIALIZED);
  cudf::mutable_table_view ret_view {{ret->mutable_view()}};
  interops::perform_interpreter_operation(ret_view,
                                          filtered_table,
                                          left_inputs,
                                          right_inputs,
                                          outputs,
                                          final_output_positions,
                                          operators,
                                          unary_operators,
                                          left_scalars,
                                          right_scalars);

  return ret;
}

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
  const ral::frame::BlazingTableView & table,
  const std::string & query_part,
  blazingdb::manager::Context * context) {

	cudf::table_view table_view = table.view();
	if(table_view.num_rows() == 0) {
		return std::make_unique<ral::frame::BlazingTable>(cudf::experimental::empty_like(table_view), table.names());
	}
	
  std::string conditional_expression = get_named_expression(query_part, "condition");
	if(conditional_expression.empty()) {
		conditional_expression = get_named_expression(query_part, "filters");
	}

  auto bool_mask = boolean_mask_from_expression(table_view, conditional_expression);

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
