
.. _program_listing_file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_CacheData.h:

Program Listing for File CacheData.h
====================================

|exhale_lsh| :ref:`Return to documentation for file <file__home_tom_Documents_programming_romulo_blazingsql_blazingsql_engine_src_execution_graph_logic_controllers_CacheData.h>` (``/home/tom/Documents/programming/romulo_blazingsql/blazingsql/engine/src/execution_graph/logic_controllers/CacheData.h``)

.. |exhale_lsh| unicode:: U+021B0 .. UPWARDS ARROW WITH TIP LEFTWARDS

.. code-block:: cpp

   #pragma once
   
   #include <atomic>
   #include <deque>
   #include <memory>
   #include <condition_variable>
   #include <mutex>
   #include <string>
   #include <vector>
   #include <map>
   
   #include <spdlog/spdlog.h>
   #include "cudf/types.hpp"
   #include "error.hpp"
   #include "CodeTimer.h"
   #include <execution_graph/logic_controllers/LogicPrimitives.h>
   #include <execution_graph/Context.h>
   #include <bmr/BlazingMemoryResource.h>
   #include "communication/CommunicationData.h"
   #include <exception>
   #include "io/data_provider/DataProvider.h"
   #include "io/data_parser/DataParser.h"
   
   #include "communication/messages/GPUComponentMessage.h"
   
   using namespace std::chrono_literals;
   
   namespace ral {
   namespace cache {
   
   using Context = blazingdb::manager::Context;
   using namespace fmt::literals;
   
   
   enum class CacheDataType { GPU, CPU, LOCAL_FILE, IO_FILE, CONCATENATING, PINNED };
   
   const std::string KERNEL_ID_METADATA_LABEL = "kernel_id"; 
   const std::string RAL_ID_METADATA_LABEL = "ral_id"; 
   const std::string QUERY_ID_METADATA_LABEL = "query_id"; 
   const std::string CACHE_ID_METADATA_LABEL = "cache_id";  
   const std::string ADD_TO_SPECIFIC_CACHE_METADATA_LABEL = "add_to_specific_cache";  
   const std::string SENDER_WORKER_ID_METADATA_LABEL = "sender_worker_id"; 
   const std::string WORKER_IDS_METADATA_LABEL = "worker_ids"; 
   const std::string TOTAL_TABLE_ROWS_METADATA_LABEL = "total_table_rows"; 
   const std::string JOIN_LEFT_BYTES_METADATA_LABEL = "join_left_bytes_metadata_label"; 
   const std::string JOIN_RIGHT_BYTES_METADATA_LABEL = "join_right_bytes_metadata_label"; 
   const std::string AVG_BYTES_PER_ROW_METADATA_LABEL = "avg_bytes_per_row"; 
   const std::string MESSAGE_ID = "message_id"; 
   const std::string PARTITION_COUNT = "partition_count"; 
   const std::string UNIQUE_MESSAGE_ID = "unique_message_id"; 
   class MetadataDictionary{
   public:
   
       void add_value(std::string key, std::string value){
           this->values[key] = value;
       }
   
       void add_value(std::string key, int value){
           this->values[key] = std::to_string(value);
       }
   
       int get_kernel_id(){
           if( this->values.find(KERNEL_ID_METADATA_LABEL) == this->values.end()){
               throw BlazingMissingMetadataException(KERNEL_ID_METADATA_LABEL);
           }
           return std::stoi(values[KERNEL_ID_METADATA_LABEL]);
       }
   
       void print(){
           for(auto elem : this->values)
           {
              std::cout << elem.first << " " << elem.second<< "\n";
           }
       }
       std::map<std::string,std::string> get_values() const {
           return this->values;
       }
   
       void set_values(std::map<std::string,std::string> new_values){
           this->values= new_values;
       }
   
       bool has_value(std::string key){
           auto it = this->values.find(key);
           return it != this->values.end();
       }
   
       std::string get_value(std::string key);
   
       void set_value(std::string key, std::string value);
   
   private:
       std::map<std::string,std::string> values; 
   };
   
   class CacheData {
   public:
   
       CacheData(CacheDataType cache_type, std::vector<std::string> col_names, std::vector<cudf::data_type> schema, size_t n_rows)
           : cache_type(cache_type), col_names(col_names), schema(schema), n_rows(n_rows)
       {
       }
   
       CacheData()
       {
       }
       virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;
   
       virtual size_t sizeInBytes() const = 0;
   
       virtual void set_names(const std::vector<std::string> & names) = 0;
   
       virtual ~CacheData() {}
   
       std::vector<std::string> names() const {
           return col_names;
       }
   
       std::vector<cudf::data_type> get_schema() const {
           return schema;
       }
   
       size_t num_columns() const {
           return col_names.size();
       }
   
       size_t num_rows() const {
           return n_rows;
       }
   
       CacheDataType get_type() const {
           return cache_type;
       }
   
       MetadataDictionary getMetadata(){
           return this->metadata;
       }
   
       static std::unique_ptr<CacheData> downgradeCacheData(std::unique_ptr<CacheData> cacheData, std::string id, std::shared_ptr<Context> ctx);
   
   protected:
       CacheDataType cache_type; 
       std::vector<std::string> col_names; 
       std::vector<cudf::data_type> schema; 
       size_t n_rows; 
       MetadataDictionary metadata; 
   };
   
   class GPUCacheData : public CacheData {
   public:
       GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table)
           : CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {}
   
       GPUCacheData(std::unique_ptr<ral::frame::BlazingTable> table, const MetadataDictionary & metadata)
           : CacheData(CacheDataType::GPU,table->names(), table->get_schema(), table->num_rows()),  data{std::move(table)} {
               this->metadata = metadata;
           }
       
       std::unique_ptr<ral::frame::BlazingTable> decache() override { return std::move(data); }
   
       size_t sizeInBytes() const override { return data->sizeInBytes(); }
   
       void set_names(const std::vector<std::string> & names) override {
           data->setNames(names);
       }
   
       virtual ~GPUCacheData() {}
   
       ral::frame::BlazingTableView getTableView(){
           return this->data->toBlazingTableView();
       }
   
       void set_data(std::unique_ptr<ral::frame::BlazingTable> table ){
           this->data = std::move(table);
       }
   protected:
       std::unique_ptr<ral::frame::BlazingTable> data; 
   };
   
   
    class CPUCacheData : public CacheData {
    public:
       CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table, bool use_pinned = false);
   
       CPUCacheData(std::unique_ptr<ral::frame::BlazingTable> gpu_table,const MetadataDictionary & metadata, bool use_pinned = false);
   
       CPUCacheData(const std::vector<blazingdb::transport::ColumnTransport> & column_transports,
                   std::vector<ral::memory::blazing_chunked_column_info> && chunked_column_infos,
                   std::vector<std::unique_ptr<ral::memory::blazing_allocation_chunk>> && allocations,
                   const MetadataDictionary & metadata);
   
       CPUCacheData(std::unique_ptr<ral::frame::BlazingHostTable> host_table);
   
       std::unique_ptr<ral::frame::BlazingTable> decache() override {
           return std::move(host_table->get_gpu_table());
       }
   
       std::unique_ptr<ral::frame::BlazingHostTable> releaseHostTable() {
           return std::move(host_table);
       }
   
       size_t sizeInBytes() const override { return host_table->sizeInBytes(); }
   
       void set_names(const std::vector<std::string> & names) override
       {
           host_table->set_names(names);
       }
   
       virtual ~CPUCacheData() {}
   
   protected:
       std::unique_ptr<ral::frame::BlazingHostTable> host_table; 
    };
   
   
   class CacheDataLocalFile : public CacheData {
   public:
   
       CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table, std::string orc_files_path, std::string ctx_token);
   
       std::unique_ptr<ral::frame::BlazingTable> decache() override;
   
       size_t sizeInBytes() const override;
       size_t fileSizeInBytes() const;
   
       void set_names(const std::vector<std::string> & names) override {
           this->col_names = names;
       }
   
       virtual ~CacheDataLocalFile() {}
   
       std::string filePath() const { return filePath_; }
   
   private:
       std::vector<std::string> col_names; 
       std::string filePath_; 
       size_t size_in_bytes; 
   };
   
   
   class CacheDataIO : public CacheData {
   public:
   
        CacheDataIO(ral::io::data_handle handle,
           std::shared_ptr<ral::io::data_parser> parser,
           ral::io::Schema schema,
           ral::io::Schema file_schema,
           std::vector<int> row_group_ids,
           std::vector<int> projections
            );
   
       std::unique_ptr<ral::frame::BlazingTable> decache() override;
   
       size_t sizeInBytes() const override;
   
       void set_names(const std::vector<std::string> & names) override {
           this->schema.set_names(names);
       }
   
       virtual ~CacheDataIO() {}
   
   
   private:
       ral::io::data_handle handle;
       std::shared_ptr<ral::io::data_parser> parser;
       ral::io::Schema schema;
       ral::io::Schema file_schema;
       std::vector<int> row_group_ids;
       std::vector<int> projections;
   };
   
   class ConcatCacheData : public CacheData {
   public:
       ConcatCacheData(std::vector<std::unique_ptr<CacheData>> cache_datas, const std::vector<std::string>& col_names, const std::vector<cudf::data_type>& schema);
   
       std::unique_ptr<ral::frame::BlazingTable> decache() override;
   
       size_t sizeInBytes() const override;
   
       void set_names(const std::vector<std::string> & names) override;
   
       std::vector<std::unique_ptr<CacheData>> releaseCacheDatas();
   
       virtual ~ConcatCacheData() {}
   
   protected:
       std::vector<std::unique_ptr<CacheData>> _cache_datas;
   };
   
   
   class message { //TODO: the cache_data object can store its id. This is not needed.
   public:
       message(std::unique_ptr<CacheData> content, std::string message_id = "")
           : data(std::move(content)), message_id(message_id)
       {
           assert(data != nullptr);
       }
   
       ~message() = default;
   
       std::string get_message_id() const { return (message_id); }
   
       CacheData& get_data() const { return *data; }
   
       std::unique_ptr<CacheData> release_data() { return std::move(data); }
   
   protected:
       std::unique_ptr<CacheData> data;
       const std::string message_id;
   };
   
   }  // namespace cache
   
   
   } // namespace ral
