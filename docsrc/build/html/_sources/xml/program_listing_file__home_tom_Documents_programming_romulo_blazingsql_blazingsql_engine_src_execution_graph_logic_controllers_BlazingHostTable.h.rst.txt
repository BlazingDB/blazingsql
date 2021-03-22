
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingHostTable.h:

Program Listing for File BlazingHostTable.h
===========================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_BlazingHostTable.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/BlazingHostTable.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <vector>
   #include <string>
   #include <memory>
   #include "cudf/types.hpp"
   #include "transport/ColumnTransport.h"
   #include "bmr/BufferProvider.h"
   #include "LogicPrimitives.h"
   
   namespace ral {
   namespace frame {
   
   using ColumnTransport = blazingdb::transport::ColumnTransport;
   class BlazingTable;  // forward declaration
   
   class BlazingHostTable {
   public:
   
       BlazingHostTable(const std::vector<ColumnTransport> &columns_offsets,
           std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
           std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations);
   
       ~BlazingHostTable();
   
       std::vector<cudf::data_type> get_schema() const;
   
       std::vector<std::string> names() const;
   
       void set_names(std::vector<std::string> names);
   
       cudf::size_type num_rows() const ;
   
       cudf::size_type num_columns() const ;
   
       std::size_t sizeInBytes() ;
   
       void setPartitionId(const size_t &part_id) ;
   
       size_t get_part_id() ;
   
       const std::vector<ColumnTransport> & get_columns_offsets() const ;
   
       std::unique_ptr<BlazingTable> get_gpu_table() const;
   
       std::vector<ral::memory::blazing_allocation_chunk> get_raw_buffers() const;
   
       const std::vector<ral::memory::blazing_chunked_column_info> &  get_blazing_chunked_column_infos() const;
   
   private:
       std::vector<ColumnTransport> columns_offsets;
       std::vector<ral::memory::blazing_chunked_column_info> chunked_column_infos;
       std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> allocations;
   
       
       size_t part_id;
   };
   
   }  // namespace frame
   }  // namespace ral
