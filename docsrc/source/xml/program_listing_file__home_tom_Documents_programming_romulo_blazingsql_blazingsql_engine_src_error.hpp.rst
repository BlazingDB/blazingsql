
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_error.hpp:

Program Listing for File error.hpp
==================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_error.hpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/error.hpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <iostream>
   #include <vector>
   #include <cudf/utilities/error.hpp>
   #include <exception>
   
   #define STRINGIFY_DETAIL(x) #x
   #define RAL_STRINGIFY(x) STRINGIFY_DETAIL(x)
   
   #define RAL_EXPECTS(cond, reason)                            \
     (!!(cond))                                                 \
         ? static_cast<void>(0)                                 \
         : throw cudf::logic_error("Ral failure at: " __FILE__ \
                                   ":" RAL_STRINGIFY(__LINE__) ": " reason)
   
   #define RAL_FAIL(reason)                              \
     throw cudf::logic_error("Ral failure at: " __FILE__ \
                             ":" CUDF_STRINGIFY(__LINE__) ": " reason)
   
   
   
   struct BlazingMissingMetadataException : public std::exception
   {
   
     BlazingMissingMetadataException(std::string key) : key{key} {}
     virtual ~BlazingMissingMetadataException() {}
     const char * what () const throw ()
       {
           return ("Missing metadata, could not find key " + key).c_str();
       }
     private:
       std::string key;
   };
   
   enum _error
   {
     E_SUCCESS = 0,
     E_EXCEPTION
   };
   
   typedef int error_code_t;
