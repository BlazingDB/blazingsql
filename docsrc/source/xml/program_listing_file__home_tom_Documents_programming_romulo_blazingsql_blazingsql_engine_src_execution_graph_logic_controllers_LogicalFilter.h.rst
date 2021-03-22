
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_LogicalFilter.h:

Program Listing for File LogicalFilter.h
========================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_LogicalFilter.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/LogicalFilter.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <execution_graph/Context.h>
   #include "LogicPrimitives.h"
   
   namespace ral{
   namespace processor{
   
   bool is_logical_filter(const std::string & query_part);
   
   std::unique_ptr<ral::frame::BlazingTable> applyBooleanFilter(
     const ral::frame::BlazingTableView & table,
     const CudfColumnView & boolValues);
   
   std::unique_ptr<ral::frame::BlazingTable> process_filter(
     const ral::frame::BlazingTableView & table,
     const std::string & query_part,
     blazingdb::manager::Context * context);
   
   bool check_if_has_nulls(CudfTableView const& input, std::vector<cudf::size_type> const& keys);
   
   std::unique_ptr<ral::frame::BlazingTable> process_distribution_table(
       const ral::frame::BlazingTableView & table,
       std::vector<int> & columnIndices,
       blazingdb::manager::Context * context);
   
   } // namespace processor
   } // namespace ral
