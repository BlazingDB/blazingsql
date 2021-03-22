
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationInterface_node.hpp:

Program Listing for File node.hpp
=================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_communication_CommunicationInterface_node.hpp>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/communication/CommunicationInterface/node.hpp``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <string>
   #include <ucp/api/ucp.h>
   
   namespace comm {
   
   class node {
   public:
     node(int idx, std::string id, ucp_ep_h ucp_ep, ucp_worker_h ucp_worker);
     node(int idx, std::string id, std::string ip, int port);
   
     int index() const;
     std::string id() const;
   
     ucp_ep_h get_ucp_endpoint() const;
     ucp_worker_h get_ucp_worker() const;
     int port() const;
     std::string ip() const;
   protected:
     int _idx;
     std::string _id;
     std::string _ip;
     int _port;
     ucp_ep_h _ucp_ep;
     ucp_worker_h _ucp_worker;
   };
   
   }  // namespace comm
