
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_metadata_orc_metadata.h:

Program Listing for File orc_metadata.h
=======================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_metadata_orc_metadata.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/data_parser/metadata/orc_metadata.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #ifndef ORC_METADATA_H_
   #define ORC_METADATA_H_
           
   #include "common_metadata.h"
   #include <cudf/io/orc_metadata.hpp>
   
   cudf::type_id to_dtype(cudf::io::statistics_type stat_type);
   
   void set_min_max(
       std::vector<std::vector<int64_t>> & minmax_metadata_table,
       cudf::io::column_statistics & statistic,
       int col_index);
   
   std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
       std::vector<cudf::io::parsed_orc_statistics> & statistics,
       size_t total_stripes, int metadata_offset);
   
   #endif  // ORC_METADATA_H_
