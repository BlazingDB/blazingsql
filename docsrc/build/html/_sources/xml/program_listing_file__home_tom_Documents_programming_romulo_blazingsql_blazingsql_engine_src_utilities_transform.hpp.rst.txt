
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_utilities_transform.hpp:

Program Listing for File transform.hpp
======================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_utilities_transform.hpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/utilities/transform.hpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <cudf/column/column_view.hpp>
   
   namespace ral {
   namespace utilities {
   
   void transform_length_to_end(cudf::mutable_column_view& length, const cudf::column_view & start);
   
   void transform_start_to_zero_based_indexing(cudf::mutable_column_view& start);
   
   }  // namespace utilities
   }  // namespace ral
