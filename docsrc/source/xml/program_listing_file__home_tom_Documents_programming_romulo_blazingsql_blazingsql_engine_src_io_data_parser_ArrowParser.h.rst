
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_ArrowParser.h:

Program Listing for File ArrowParser.h
======================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_parser_ArrowParser.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/data_parser/ArrowParser.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   
   
   #ifndef ARROWPARSER_H_
   #define ARROWPARSER_H_
   
   #include "DataParser.h"
   #include <vector>
   #include <memory>
   #include "arrow/io/interfaces.h"
   
   //#include "cudf.h"
   #include "io/io.h"
   #include <arrow/table.h>
   
   namespace ral {
   namespace io {
   
   class arrow_parser : public data_parser {
   public:
       arrow_parser( std::shared_ptr< arrow::Table > table);
   
       virtual ~arrow_parser();
   
       void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file,
               ral::io::Schema & schema);
   
       DataType type() const override { return DataType::ARROW; }
   
   private:
       std::shared_ptr< arrow::Table > table;
   };
   
   } /* namespace io */
   } /* namespace ral */
   
   #endif /* ARROWPARSER_H_ */
