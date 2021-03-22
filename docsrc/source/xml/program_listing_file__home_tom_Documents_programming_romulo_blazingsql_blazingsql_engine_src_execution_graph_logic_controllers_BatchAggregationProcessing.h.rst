
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchAggregationProcessing.h:

Program Listing for File BatchAggregationProcessing.h
=====================================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchAggregationProcessing.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BatchAggregationProcessing.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "BatchProcessing.h"
   #include "taskflow/distributing_kernel.h"
   #include "operators/GroupBy.h"
   
   namespace ral {
   namespace batch {
   using ral::cache::distributing_kernel;
   using ral::cache::kstatus;
   using ral::cache::kernel;
   using ral::cache::kernel_type;
   using namespace fmt::literals;
   
   class ComputeAggregateKernel : public kernel {
   public:
       ComputeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "ComputeAggregate";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       virtual kstatus run();
   
       std::pair<bool, uint64_t> get_estimated_output_num_rows();
   
   private:
       std::vector<AggregateKind> aggregation_types;
       std::vector<int> group_column_indices;
       std::vector<std::string> aggregation_input_expressions;
       std::vector<std::string> aggregation_column_assigned_aliases;
   };
   
   class DistributeAggregateKernel : public distributing_kernel {
   public:
       DistributeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "DistributeAggregate";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       virtual kstatus run();
   
   private:
       std::vector<int> group_column_indices;
       std::vector<std::string> aggregation_input_expressions, aggregation_column_assigned_aliases; // not used in this kernel
       std::vector<AggregateKind> aggregation_types; // not used in this kernel
       std::vector<cudf::size_type> columns_to_hash;
       bool set_empty_part_for_non_master_node = false; // this is only for aggregation without group by
   };
   
   class MergeAggregateKernel : public kernel {
   public:
       MergeAggregateKernel(std::size_t kernel_id, const std::string & queryString, std::shared_ptr<Context> context, std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "MergeAggregate";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       virtual kstatus run();
   
   private:
   
   };
   
   } // namespace batch
   } // namespace ral
