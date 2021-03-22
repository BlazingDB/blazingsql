
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_bmr_MemoryMonitor.h:

Program Listing for File MemoryMonitor.h
========================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_bmr_MemoryMonitor.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/bmr/MemoryMonitor.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <condition_variable>
   #include <mutex>
   #include <chrono>
   #include "ExceptionHandling/BlazingThread.h"
   #include <map>
   
   class BlazingMemoryResource;
   namespace ral {
   namespace  batch{
       class tree_processor;
       class node;
   } //namespace batch
   
   class MemoryMonitor {
   
       public:
           MemoryMonitor(std::shared_ptr<ral::batch::tree_processor> tree, std::map<std::string, std::string> config_options);
           void start();
           void finalize();
       private:
           bool finished;
           std::mutex finished_lock;
           std::condition_variable condition;
           std::shared_ptr<ral::batch::tree_processor> tree;
           std::chrono::milliseconds period;
           BlazingMemoryResource* resource;
           BlazingThread monitor_thread;
   
           bool need_to_free_memory();
           void downgradeCaches(ral::batch::node* starting_node);
   };
   
   }  // namespace ral
