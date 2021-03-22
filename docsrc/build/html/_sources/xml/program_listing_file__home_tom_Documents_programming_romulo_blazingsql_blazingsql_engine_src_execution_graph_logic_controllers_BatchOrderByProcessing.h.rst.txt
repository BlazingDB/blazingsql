
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchOrderByProcessing.h:

Program Listing for File BatchOrderByProcessing.h
=================================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BatchOrderByProcessing.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BatchOrderByProcessing.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "BatchProcessing.h"
   #include "communication/CommunicationData.h"
   #include "operators/OrderBy.h"
   #include "taskflow/distributing_kernel.h"
   
   namespace ral {
   namespace batch {
   using ral::cache::distributing_kernel;
   using ral::cache::kstatus;
   using ral::cache::kernel;
   using ral::cache::kernel_type;
   using namespace fmt::literals;
   
   
   class PartitionSingleNodeKernel : public kernel {
   public:
       PartitionSingleNodeKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "PartitionSingleNode";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   
   private:
       std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
   };
   
   class SortAndSampleKernel : public distributing_kernel {
   
   std::size_t SAMPLES_MESSAGE_TRACKER_IDX = 0;
   std::size_t PARTITION_PLAN_MESSAGE_TRACKER_IDX = 1;
   
   public:
       SortAndSampleKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "SortAndSample";}
   
       bool all_node_samples_are_available();
   
       void make_partition_plan_task();
   
       void compute_partition_plan(
           std::vector<std::unique_ptr<ral::frame::BlazingTable>> inputSamples);
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   
   private:
       std::vector<std::unique_ptr<ral::frame::BlazingTable>> samplesTables;
       std::atomic<bool> get_samples;
       std::atomic<bool> already_computed_partition_plan;
       std::mutex samples_mutex;
       std::size_t population_sampled = 0;
       std::size_t max_order_by_samples = 10000;
       std::size_t total_num_rows_for_sampling = 0;
       std::size_t total_bytes_for_sampling = 0;   
   };
   
   class PartitionKernel : public distributing_kernel {
   public:
       PartitionKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "Partition";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   
   private:
       std::unique_ptr<ral::frame::BlazingTable> partitionPlan;
       std::vector<cudf::order> sortOrderTypes;
       std::vector<int> sortColIndices;
       int num_partitions_per_node;
   };
   
   class MergeStreamKernel : public kernel {
   public:
       MergeStreamKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "MergeStream";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   };
   
   
   class LimitKernel : public distributing_kernel {
   public:
       LimitKernel(std::size_t kernel_id, const std::string & queryString,
           std::shared_ptr<Context> context,
           std::shared_ptr<ral::cache::graph> query_graph);
   
       std::string kernel_name() { return "Limit";}
   
       ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args) override;
   
       kstatus run() override;
   
   private:
       std::atomic<int64_t> rows_limit;
   };
   
   } // namespace batch
   } // namespace ral
