
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_ColumnTransport.h:

Program Listing for File ColumnTransport.h
==========================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_ColumnTransport.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/transport/ColumnTransport.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <cstdint>
   
   namespace blazingdb {
   namespace transport {
   
   struct ColumnTransport {
     struct MetaData {
       int32_t dtype{};
       int32_t size{};
       int32_t null_count{};
       char col_name[128]{};
     };
     MetaData metadata{};
     int data{};  // position del buffer? / (-1) no hay buffer
     int valid{};
     int strings_data{};
     int strings_offsets{};
     int strings_nullmask{};
     int strings_data_size{0};
     int strings_offsets_size{0};
   
     std::size_t size_in_bytes{0};
   };
   
   }  // namespace transport
   }  // namespace blazingdb
