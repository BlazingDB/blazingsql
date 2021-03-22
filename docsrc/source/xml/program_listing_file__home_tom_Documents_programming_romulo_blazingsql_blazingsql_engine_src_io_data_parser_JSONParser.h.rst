
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_JSONParser.h:

Program Listing for File JSONParser.h
=====================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_JSONParser.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/data_parser/JSONParser.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <arrow/io/interfaces.h>
   #include <cudf/io/datasource.hpp>
   #include <cudf/io/json.hpp>
   #include <memory>
   #include <vector>
   
   #include "DataParser.h"
   
   namespace ral {
   namespace io {
   
   class json_parser : public data_parser {
   public:
       json_parser(std::map<std::string, std::string> args_map);
   
       virtual ~json_parser();
   
       std::unique_ptr<ral::frame::BlazingTable> parse_batch(ral::io::data_handle handle,
           const Schema & schema,
           std::vector<int> column_indices,
           std::vector<cudf::size_type> row_groups) override;
   
       void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);
   
       DataType type() const override { return DataType::JSON; }
   
   private:
       std::map<std::string, std::string> args_map;
   };
   
   } /* namespace io */
   } /* namespace ral */
