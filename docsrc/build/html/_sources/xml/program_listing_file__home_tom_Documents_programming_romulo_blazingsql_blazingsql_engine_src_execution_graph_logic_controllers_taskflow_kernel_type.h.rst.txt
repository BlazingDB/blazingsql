
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kernel_type.h:

Program Listing for File kernel_type.h
======================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kernel_type.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/taskflow/kernel_type.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <string>
   
   namespace ral {
   namespace cache {
   
   enum class kernel_type {
       ProjectKernel,
       FilterKernel,
       UnionKernel,
       MergeStreamKernel,
       PartitionKernel,
       SortAndSampleKernel,
       ComputeWindowKernel,
       PartitionSingleNodeKernel,
       LimitKernel,
       ComputeAggregateKernel,
       DistributeAggregateKernel,
       MergeAggregateKernel,
       TableScanKernel,
       BindableTableScanKernel,
       PartwiseJoinKernel,
       JoinPartitionKernel,
       OutputKernel,
       PrintKernel,
       GenerateKernel,
   };
   
   std::string get_kernel_type_name(kernel_type type);
   }  // namespace cache
   }  // namespace ral
