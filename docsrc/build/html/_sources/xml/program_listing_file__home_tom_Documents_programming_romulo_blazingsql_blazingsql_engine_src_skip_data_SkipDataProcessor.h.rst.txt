
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_skip_data_SkipDataProcessor.h:

Program Listing for File SkipDataProcessor.h
============================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_skip_data_SkipDataProcessor.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/skip_data/SkipDataProcessor.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   
   #ifndef SKIPDATAPROCESSOR_H_
   #define SKIPDATAPROCESSOR_H_
   
   #include <iostream>
   #include <string>
   #include "parser/expression_tree.hpp"
   #include "execution_graph/logic_controllers/LogicPrimitives.h"
   
   namespace ral {
   namespace skip_data {
   
   // For unit testing
   void drop_value(ral::parser::parse_tree& tree, const std::string & value);
   bool apply_skip_data_rules(ral::parser::parse_tree& tree);
   
   std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> process_skipdata_for_table(
       const ral::frame::BlazingTableView & metadata_view, const std::vector<std::string> & names, std::string table_scan);
   
   } // namespace skip_data
   } // namespace ral
   
   
   #endif //SKIPDATAPROCESSOR_H_
