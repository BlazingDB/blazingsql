
#include "LogicalFilter.h"
#include "cudf/stream_compaction.hpp"
#include "cudf/hashing.hpp"
#include "cudf/join.hpp"
#include "../../CalciteExpressionParsing.h"
#include "../../JoinProcessor.h"
namespace ral{

namespace processor{

  const std::string INNER_JOIN = "inner";
  const std::string LEFT_JOIN = "left";
  const std::string RIGHT_JOIN = "right";
  const std::string OUTER_JOIN = "full";

std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
  const ral::frame::BlazingTableView & table,
  const CudfColumnView boolValues){
    auto filteredTable = cudf::experimental::apply_boolean_mask(
      table.view(),boolValues);
    return std::make_unique<ral::frame::BlazingTable>(std::move(
      filteredTable),table.names());
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


}

}
