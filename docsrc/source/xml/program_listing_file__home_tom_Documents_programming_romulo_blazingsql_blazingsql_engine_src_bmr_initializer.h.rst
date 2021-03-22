
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_bmr_initializer.h:

Program Listing for File initializer.h
======================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_bmr_initializer.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/bmr/initializer.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once 
   
   #include <bmr/BlazingMemoryResource.h>
   
   // Shutdown memory manager.
   static void BlazingRMMFinalize()
   {
       blazing_device_memory_resource::getInstance().finalize();   
   }
   
   static void BlazingRMMInitialize(std::string allocation_mode = "managed_memory_resource",
                                           std::size_t initial_pool_size = 0,
                                           std::size_t maximum_pool_size = 0,
                                           std::string allocator_logging_file = "",
                                           float device_mem_resouce_consumption_thresh = 0.95)
   {
     blazing_device_memory_resource::getInstance().initialize(
         allocation_mode, initial_pool_size, maximum_pool_size, allocator_logging_file, device_mem_resouce_consumption_thresh);    
   }
