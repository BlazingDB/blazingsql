
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_CalciteExpressionParsing.h:

Program Listing for File CalciteExpressionParsing.h
===================================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_CalciteExpressionParsing.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/CalciteExpressionParsing.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "cudf/types.hpp"
   #include <string>
   #include <vector>
   
   bool is_type_float(cudf::type_id type);
   bool is_type_integer(cudf::type_id type);
   bool is_type_bool(cudf::type_id type) ;
   bool is_type_timestamp(cudf::type_id type);
   bool is_type_string(cudf::type_id type);
   
   cudf::size_type get_index(const std::string & operand_string);
   
   // interprets the expression and if is n-ary and logical, then returns their corresponding binary version
   std::string expand_if_logical_op(std::string expression);
   
   std::string clean_calcite_expression(const std::string & expression);
   
   std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression);
   
   std::string get_aggregation_operation_string(std::string operator_expression);
   
   std::string get_string_between_outer_parentheses(std::string operator_string);
   
   std::unique_ptr<cudf::scalar> get_max_integer_scalar(cudf::data_type type);
   
   std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string, cudf::data_type type, bool strings_have_quotes = true);
   
   int count_string_occurrence(std::string haystack, std::string needle);
