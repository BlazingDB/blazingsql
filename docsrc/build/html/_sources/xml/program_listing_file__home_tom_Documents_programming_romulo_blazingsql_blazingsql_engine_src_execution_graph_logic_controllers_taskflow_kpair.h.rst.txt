
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kpair.h:

Program Listing for File kpair.h
================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kpair.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/taskflow/kpair.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "port.h"
   #include "kernel.h"
   
   namespace ral {
   namespace cache {
   
   enum class CacheType {SIMPLE, CONCATENATING, FOR_EACH };
   
   struct cache_settings {
       CacheType type = CacheType::SIMPLE;
       int num_partitions = 1;
       std::shared_ptr<Context> context;
       std::size_t concat_cache_num_bytes = 400000000;
       bool concat_all = false; 
   };
   
   using kernel_pair = std::pair<kernel *, std::string>;
   
   class kpair {
   public:
       kpair(std::shared_ptr<kernel> a, std::shared_ptr<kernel> b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
           src = a;
           dst = b;
       }
       kpair(std::shared_ptr<kernel> a, std::shared_ptr<kernel> b, const std::string & port_b, const cache_settings & config = cache_settings{}) : cache_machine_config(config) {
           src = a;
           dst = b;
           dst_port_name = port_b;
       }
       kpair(std::shared_ptr<kernel> a, const std::string & port_a, std::shared_ptr<kernel> b, const std::string & port_b, const cache_settings & config = cache_settings{})
           : cache_machine_config(config) {
           src = a;
           src_port_name = port_a;
           dst = b;
           dst_port_name = port_b;
       }
   
       kpair(const kpair &) = default;
       kpair(kpair &&) = default;
   
       bool has_custom_source() const { return not src_port_name.empty(); }
       bool has_custom_target() const { return not dst_port_name.empty(); }
   
       std::shared_ptr<kernel> src;
       std::shared_ptr<kernel> dst;
       const cache_settings cache_machine_config;
       std::string src_port_name;
       std::string dst_port_name;
   };
   
   }  // namespace cache
   }  // namespace ral
