
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationData.cpp:

Program Listing for File CommunicationData.cpp
==============================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationData.cpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/communication/CommunicationData.cpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #include "CommunicationData.h"
   
   namespace ral {
   namespace communication {
   
   CommunicationData::CommunicationData() {}
   
   CommunicationData & CommunicationData::getInstance() {
       static CommunicationData communicationData;
       return communicationData;
   }
   
   void CommunicationData::initialize(const std::string & worker_id, const std::string& cache_directory) {
       _selfNode = blazingdb::transport::Node( worker_id);
       _cache_directory = cache_directory;
   }
   
   const blazingdb::transport::Node & CommunicationData::getSelfNode() { return _selfNode; }
   
   std::string CommunicationData::get_cache_directory() { return _cache_directory; }
   
   }  // namespace communication
   }  // namespace ral
