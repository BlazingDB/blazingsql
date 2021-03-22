
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_DataType.h:

Program Listing for File DataType.h
===================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_DataType.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/DataType.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #ifndef BLAZING_RAL_DATA_TYPE_H_
   #define BLAZING_RAL_DATA_TYPE_H_
   
   namespace ral {
   namespace io {
   
   typedef enum { UNDEFINED = 999, PARQUET = 0, ORC = 1, CSV = 2, JSON = 3, CUDF = 4, DASK_CUDF = 5, ARROW = 6 } DataType;
   
   } /* namespace io */
   } /* namespace ral */
   
   #endif /* BLAZING_RAL_DATA_TYPE_H_ */
