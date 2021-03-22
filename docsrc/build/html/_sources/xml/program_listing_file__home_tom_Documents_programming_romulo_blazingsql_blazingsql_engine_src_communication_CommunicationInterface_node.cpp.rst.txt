
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationInterface_node.cpp:

Program Listing for File node.cpp
=================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationInterface_node.cpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/communication/CommunicationInterface/node.cpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #include "node.hpp"
   
   namespace comm{
   
   node::node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker)
       : _idx{idx}, _id{id}, _ucp_ep{ucp_ep}, _ucp_worker{ucp_worker} {}
   
   
   node::node(int idx, std::string id, std::string ip, int port)
       : _idx{idx}, _id{id}, _ip{ip}, _port{port} {}
   
   int node::index() const { return _idx; }
   
   std::string node::id() const { return _id; };
   
   ucp_ep_h node::get_ucp_endpoint() const { return _ucp_ep; }
   
   std::string node::ip() const { return _ip; }
   
   int node::port() const { return _port; }
   
   ucp_worker_h node::get_ucp_worker() const { return _ucp_worker; }
   
   } // namespace comm
