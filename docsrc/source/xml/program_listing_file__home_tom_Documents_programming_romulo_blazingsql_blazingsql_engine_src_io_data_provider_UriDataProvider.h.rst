
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_provider_UriDataProvider.h:

Program Listing for File UriDataProvider.h
==========================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_io_data_provider_UriDataProvider.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/io/data_provider/UriDataProvider.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   /*
    * uridataprovider.h
    *
    *  Created on: Nov 29, 2018
    *      Author: felipe
    */
   
   #ifndef URIDATAPROVIDER_H_
   #define URIDATAPROVIDER_H_
   
   #include "DataProvider.h"
   #include <arrow/io/interfaces.h>
   #include <blazingdb/io/FileSystem/Uri.h>
   #include <vector>
   
   #include <memory>
   
   
   namespace ral {
   namespace io {
   class uri_data_provider : public data_provider {
   public:
       uri_data_provider(std::vector<Uri> uris,
           std::vector<std::map<std::string, std::string>> uri_values,
           bool ignore_missing_paths = false);
       uri_data_provider(std::vector<Uri> uris, 
           bool ignore_missing_paths = false);
   
       std::shared_ptr<data_provider> clone() override; 
   
       virtual ~uri_data_provider();
       bool has_next();
       void reset();
       data_handle get_next(bool open_file = true);
       
       
       std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true);
   
       void close_file_handles();
   
       size_t get_num_handles();
   
   private:
       std::vector<Uri> file_uris;
       size_t current_file;
       std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> opened_files;
       // TODO: we should really be either handling exceptions up the call stack or
       // storing something more elegant than just a string with an error message
       std::vector<std::string> errors;
   
       std::vector<std::map<std::string,  std::string>> uri_values;
       std::vector<Uri> directory_uris;
       size_t directory_current_file;
       bool ignore_missing_paths;
   };
   
   } /* namespace io */
   } /* namespace ral */
   
   #endif /* URIDATAPROVIDER_H_ */
