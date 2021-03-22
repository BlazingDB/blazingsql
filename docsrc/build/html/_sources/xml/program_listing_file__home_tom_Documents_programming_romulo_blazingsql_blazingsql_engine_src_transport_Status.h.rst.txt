
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_Status.h:

Program Listing for File Status.h
=================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_transport_Status.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/transport/Status.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <string>
   
   namespace blazingdb {
   namespace transport {
   
   class Status {
   public:
     Status(bool ok = false) : ok_{ok} {}
     inline bool IsOk() const noexcept { return ok_; }
   
   private:
     bool ok_{false};
   };
   
   }  // namespace transport
   }  // namespace blazingdb
