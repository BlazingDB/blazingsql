
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_executor.h:

Program Listing for File executor.h
===================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_taskflow_executor.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/taskflow/executor.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include "kernel.h"
   #include "execution_graph/logic_controllers/CacheMachine.h"
   #include "ExceptionHandling/BlazingThread.h"
   #include "utilities/ctpl_stl.h"
   
   namespace ral {
   namespace execution{
   
   
   class priority {
   public:
   
   private:
       size_t priority_num_query; //can be used to prioritize one query over another
       size_t priority_num_kernel; //can be
   };
   
   class executor;
   
   class task {
   public:
   
       task(
       std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
       std::shared_ptr<ral::cache::CacheMachine> output,
       size_t task_id,
       ral::cache::kernel * kernel, size_t attempts_limit,
       const std::map<std::string, std::string>& args, size_t attempts = 0);
   
       void run(cudaStream_t stream, executor * executor);
       void complete();
       void fail();
       std::size_t task_memory_needed();
   
       std::vector<std::unique_ptr<ral::cache::CacheData > > release_inputs();
   
       void set_inputs(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs);
   
   protected:
       std::vector<std::unique_ptr<ral::cache::CacheData > > inputs;
       std::shared_ptr<ral::cache::CacheMachine> output;
       size_t task_id;
       ral::cache::kernel * kernel;
       size_t attempts = 0;
       size_t attempts_limit;
       std::map<std::string, std::string> args;
   
   public:
       std::shared_ptr<spdlog::logger> task_logger;
   };
   
   
   class executor{
   public:
       static executor * get_instance(){
           if(_instance == nullptr){
               throw std::runtime_error("Executor not initialized.");
           }
           return _instance;
       }
   
       static void init_executor(int num_threads, double processing_memory_limit_threshold){
           if(!_instance){
               _instance = new executor(num_threads, processing_memory_limit_threshold);
               _instance->task_id_counter = 0;
               _instance->active_tasks_counter = 0;
               auto thread = std::thread([/*_instance*/]{
                   _instance->execute();
               });
               thread.detach();
           }
       }
   
       void execute();
       std::exception_ptr last_exception();
       bool has_exception();
   
       size_t add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           ral::cache::kernel * kernel, const std::map<std::string, std::string>& args = {});
   
       void add_task(std::vector<std::unique_ptr<ral::cache::CacheData > > inputs,
           std::shared_ptr<ral::cache::CacheMachine> output,
           ral::cache::kernel * kernel,
           size_t attempts,
           size_t task_id, const std::map<std::string, std::string>& args = {});
   
       void add_task(std::unique_ptr<task> task);
   
       std::unique_ptr<task> remove_task_from_back();
   
       void notify_memory_safety_cv(){
           memory_safety_cv.notify_all();
       }
   
   private:
       executor(int num_threads, double processing_memory_limit_threshold);
       ctpl::thread_pool<BlazingThread> pool;
       std::vector<cudaStream_t> streams; //one stream per thread
       ral::cache::WaitingQueue< std::unique_ptr<task> > task_queue;
       int shutdown = 0;
       static executor * _instance;
       std::atomic<int> task_id_counter;
       size_t attempts_limit = 10;
   
       std::mutex exception_holder_mutex;
       std::queue<std::exception_ptr> exception_holder; 
       BlazingMemoryResource* resource;
       std::size_t processing_memory_limit;
       std::atomic<int> active_tasks_counter;
       std::mutex memory_safety_mutex;
       std::condition_variable memory_safety_cv;
   };
   
   
   } // namespace execution
   } // namespace ral
