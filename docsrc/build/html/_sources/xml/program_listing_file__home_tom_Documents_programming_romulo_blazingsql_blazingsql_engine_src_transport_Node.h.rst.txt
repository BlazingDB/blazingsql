
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_Node.h:

Program Listing for File Node.h
===============================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_Node.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/transport/Node.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <memory>
   #include <string>
   
   namespace blazingdb {
   namespace transport {
   
   class Node {
   public:
     // TODO define clear constructors
     Node();
     Node( const std::string& id, bool isAvailable = true);
   
     bool operator==(const Node& rhs) const;
     bool operator!=(const Node& rhs) const;
   
   
     std::string id() const;
   
     bool isAvailable() const;
   
     void setAvailable(bool available);
   
     void print() const;
   
   
   protected:
   
     std::string id_;
     bool isAvailable_{false};
   };
   
   }  // namespace transport
   }  // namespace blazingdb
