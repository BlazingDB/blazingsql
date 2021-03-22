
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchWindowFunctionProcessing.h:

Program Listing for File BatchWindowFunctionProcessing.h
========================================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchWindowFunctionProcessing.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BatchWindowFunctionProcessing.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "BatchProcessing.h"
   #include "operators/OrderBy.h"
   #include "operators/GroupBy.h"
   
   namespace ral {
   namespace batch {
   
   //using ral::cache::distributing_kernel;
   using ral::cache::kstatus;
   using ral::cache::kernel;
   using ral::cache::kernel_type;
   
   class ComputeWindowKernel : public kernel {
   public:
       ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::unique_ptr<CudfColumn> compute_column_from_window_function(
           cudf::table_view input_cudf_view,
           cudf::column_view input_col_view,
           std::size_t pos, int & agg_param_count);
   
       std::string kernel_name() { return "ComputeWindow";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   
   private:
       // LogicalComputeWindow(min_keys=[MIN($0) OVER (PARTITION BY $1)], lag_col=[LAG($0, 5) OVER (PARTITION BY $1)], n_name=[$2])
       std::vector<int> column_indices_partitioned;   // column indices to be partitioned: [1]
       std::vector<int> column_indices_to_agg;        // column indices to be agg: [0, 0]
       std::vector<int> agg_param_values;             // due to LAG or LEAD: [5]
       int preceding_value;           // X PRECEDING
       int following_value;               // Y FOLLOWING
       std::string frame_type;                        // ROWS or RANGE
       std::vector<std::string> type_aggs_as_str;     // ["MIN", "LAG"]
       std::vector<AggregateKind> aggs_wind_func;     // [AggregateKind::MIN, AggregateKind::LAG]
   };
   
   } // namespace batch
   } // namespace ral
