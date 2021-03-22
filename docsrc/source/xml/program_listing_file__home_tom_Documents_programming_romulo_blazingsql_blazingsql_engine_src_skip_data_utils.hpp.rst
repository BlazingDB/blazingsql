
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_skip_data_utils.hpp:

Program Listing for File utils.hpp
==================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_skip_data_utils.hpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/skip_data/utils.hpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   #include <string>
   #include <vector>
   
   namespace ral {
   namespace skip_data {
   
   bool is_unsupported_binary_op(const std::string &test);
   
   bool is_exclusion_unary_op(const std::string &test);
   
   int get_id(const std::string &s);
   
   std::vector<std::string> split(const std::string &str,
                                  const std::string &delim = " ");
   } // namespace skip_data
   } // namespace ral
