
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kernel.h:

Program Listing for File kernel.h
=================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_kernel.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/taskflow/kernel.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "kernel_type.h"
   #include "port.h"
   #include "graph.h"
   
   namespace ral {
   namespace execution{
   
   enum task_status{
       SUCCESS,
       RETRY,
       FAIL
   };
   
   struct task_result{
       task_status status;
       std::string what;
       std::vector<std::unique_ptr<ral::frame::BlazingTable> > inputs;
   };
   
   }
   }
   
   namespace ral {
   namespace cache {
   class kernel;
   class graph;
   using kernel_pair = std::pair<kernel *, std::string>;
   
   class kernel {
   public:
       kernel(std::size_t kernel_id, std::string expr, std::shared_ptr<Context> context, kernel_type kernel_type_id);
   
       void set_parent(size_t id) { parent_id_ = id; }
   
       bool has_parent() const { return parent_id_ != -1; }
   
       virtual ~kernel() {}
   
       virtual kstatus run() = 0;
   
   
       kernel_pair operator[](const std::string & portname) { return std::make_pair(this, portname); }
   
       std::int32_t get_id() const { return (kernel_id); }
   
       kernel_type get_type_id() const { return kernel_type_id; }
   
       void set_type_id(kernel_type kernel_type_id_) { kernel_type_id = kernel_type_id_; }
   
       std::shared_ptr<ral::cache::CacheMachine> input_cache();
   
       std::shared_ptr<ral::cache::CacheMachine> output_cache(std::string cache_id = "");
   
       bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingTable> table, std::string cache_id = "",bool always_add = false);
   
       bool add_to_output_cache(std::unique_ptr<ral::cache::CacheData> cache_data, std::string cache_id = "", bool always_add = false);
   
       bool add_to_output_cache(std::unique_ptr<ral::frame::BlazingHostTable> host_table, std::string cache_id = "");
   
       Context * get_context() const {
           return context.get();
       }
   
       std::string get_message_id(){
           return std::to_string((int)this->get_type_id()) + "_" + std::to_string(this->get_id());
       }
   
       bool input_all_finished() {
           return this->input_.all_finished();
       }
   
       uint64_t total_input_rows_added() {
           return this->input_.total_rows_added();
       }
   
       bool input_cache_finished(const std::string & port_name) {
           return this->input_.is_finished(port_name);
       }
   
       uint64_t input_cache_num_rows_added(const std::string & port_name) {
           return this->input_.get_num_rows_added(port_name);
       }
   
       virtual std::pair<bool, uint64_t> get_estimated_output_num_rows();
   
       ral::execution::task_result process(std::vector<std::unique_ptr<ral::frame::BlazingTable > >  inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           cudaStream_t stream, const std::map<std::string, std::string>& args);
   
       virtual ral::execution::task_result do_process(std::vector<std::unique_ptr<ral::frame::BlazingTable> > /*inputs*/,
           std::shared_ptr<ral::cache::CacheMachine> /*output*/,
           cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/){
               return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
       }
   
       std::size_t estimate_output_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);
   
       std::size_t estimate_operating_bytes(const std::vector<std::unique_ptr<ral::cache::CacheData > > & inputs);
   
       virtual std::string kernel_name() { return "base_kernel"; }
   
       void notify_complete(size_t task_id);
       void notify_fail(size_t task_id);
       void add_task(size_t task_id);
       bool finished_tasks(){
           return tasks.empty();
       }
   protected:
       std::set<size_t> tasks;
       std::mutex kernel_mutex;
       std::condition_variable kernel_cv;
       std::atomic<std::size_t> total_input_bytes_processed;
   
   public:
       std::string expression; 
       port input_{this}; 
       port output_{this}; 
       const std::size_t kernel_id; 
       std::int32_t parent_id_; 
       bool execution_done = false; 
       kernel_type kernel_type_id; 
       std::shared_ptr<graph> query_graph; 
       std::shared_ptr<Context> context; 
       bool has_limit_; 
       int64_t limit_rows_; 
       std::shared_ptr<spdlog::logger> logger;
   };
   
   
   }  // namespace cache
   }  // namespace ral
