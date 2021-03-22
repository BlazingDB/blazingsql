
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationData.h:

Program Listing for File CommunicationData.h
============================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationData.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/communication/CommunicationData.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <ucp/api/ucp.h>
   #include <transport/Node.h>
   #include <memory>
   #include <string>
   #include <map>
   
   namespace ral {
   namespace communication {
   
   class CommunicationData {
   public:
       static CommunicationData & getInstance();
   
       void initialize(const std::string & worker_id, const std::string& cache_directory);
   
       const blazingdb::transport::Node & getSelfNode();
   
       std::string get_cache_directory();
   
       CommunicationData(CommunicationData &&) = delete;
       CommunicationData(const CommunicationData &) = delete;
       CommunicationData & operator=(CommunicationData &&) = delete;
       CommunicationData & operator=(const CommunicationData &) = delete;
   
   private:
       CommunicationData();
   
       blazingdb::transport::Node _selfNode;
       std::string _cache_directory;
   };
   
   }  // namespace communication
   }  // namespace ral
