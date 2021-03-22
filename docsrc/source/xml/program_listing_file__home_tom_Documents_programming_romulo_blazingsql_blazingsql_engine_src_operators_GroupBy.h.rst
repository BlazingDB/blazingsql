
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_operators_GroupBy.h:

Program Listing for File GroupBy.h
==================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_operators_GroupBy.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/operators/GroupBy.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <execution_graph//Context.h>
   #include <string>
   #include <vector>
   #include <tuple>
   
   #include "execution_graph/logic_controllers/LogicPrimitives.h"
   #include <cudf/aggregation.hpp>
   #include <cudf/groupby.hpp>
   #include <cudf/detail/aggregation/aggregation.hpp>
   
   #include <cudf/column/column_view.hpp>
   
   enum AggregateKind{
       SUM,
       SUM0,
       MEAN,
       MIN,
       MAX,
       COUNT_VALID,
       COUNT_ALL,
       COUNT_DISTINCT,
       ROW_NUMBER,
       LAG,
       LEAD
   };
   
   namespace ral {
   namespace operators {
   
       // offset param is needed for `LAG` and `LEAD` aggs
       std::unique_ptr<cudf::aggregation> makeCudfAggregation(AggregateKind input, int offset = 0);
   
       AggregateKind get_aggregation_operation(std::string expression_in);
   
       std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
           parseGroupByExpression(const std::string & queryString, std::size_t num_cols);
   
       std::tuple<std::vector<int>, std::vector<std::string>, std::vector<AggregateKind>, std::vector<std::string>> 
           modGroupByParametersPostComputeAggregations(const std::vector<int> & group_column_indices, 
           const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & merging_column_names);
   
       std::unique_ptr<ral::frame::BlazingTable> compute_groupby_without_aggregations(
           const ral::frame::BlazingTableView & table, const std::vector<int> & group_column_indices);
   
       std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_without_groupby(
           const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions, 
           const std::vector<AggregateKind> & aggregation_types, const std::vector<std::string> & aggregation_column_assigned_aliases);
   
       std::unique_ptr<ral::frame::BlazingTable> compute_aggregations_with_groupby(
           const ral::frame::BlazingTableView & table, const std::vector<std::string> & aggregation_input_expressions, const std::vector<AggregateKind> & aggregation_types,
           const std::vector<std::string> & aggregation_column_assigned_aliases, const std::vector<int> & group_column_indices);
   
   }  // namespace operators
   }  // namespace ral
