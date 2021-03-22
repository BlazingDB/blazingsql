
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_DataLoader.h:

Program Listing for File DataLoader.h
=====================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_DataLoader.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/DataLoader.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   /*
    * dataloader.h
    *
    *  Created on: Nov 29, 2018
    *      Author: felipe
    */
   
   #pragma once
   
   #include <FileSystem/Uri.h>
   #include "data_parser/DataParser.h"
   #include "data_provider/DataProvider.h"
   #include <arrow/io/interfaces.h>
   #include <execution_graph/Context.h>
   #include <vector>
   
   #include <memory>
   
   
   namespace ral {
   
   namespace io {
   
   namespace {
   using blazingdb::manager::Context;
   }  // namespace
   
   class data_loader {
   public:
       data_loader(std::shared_ptr<data_parser> parser, std::shared_ptr<data_provider> provider);
       data_loader(const data_loader& ) = default;
       std::shared_ptr<data_loader> clone();
   
       virtual ~data_loader();
   
       void get_schema(Schema & schema, std::vector<std::pair<std::string, cudf::type_id>> non_file_columns);
   
       std::unique_ptr<ral::frame::BlazingTable> get_metadata(int offset);
   
       std::shared_ptr<data_provider> get_provider() {
           return provider;
       }
       std::shared_ptr<data_parser> get_parser() {
           return parser;
       }
   
   private:
       std::shared_ptr<data_provider> provider;
       std::shared_ptr<data_parser> parser;
   };
   
   
   } /* namespace io */
   } /* namespace ral */
