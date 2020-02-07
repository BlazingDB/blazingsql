

#include "LogicPrimitives.h"
#include <sys/stat.h>


#include <random>
namespace ral{

namespace frame{


BlazingTable::BlazingTable(
  std::unique_ptr<CudfTable> table,
  std::vector<std::string> columnNames)
  : table(std::move(table)), columnNames(columnNames){

}

CudfTableView BlazingTable::view() const{
  return this->table->view();
}

std::vector<std::string> BlazingTable::names() const{
  return this->columnNames;
}

BlazingTableView BlazingTable::toBlazingTableView() const{
  return BlazingTableView(this->table->view(), this->columnNames);
}


BlazingTableView::BlazingTableView(){

}

BlazingTableView::BlazingTableView(
  CudfTableView table,
  std::vector<std::string> columnNames)
  : table(table), columnNames(columnNames){

}

CudfTableView BlazingTableView::view() const{
  return this->table;
}

std::vector<std::string> BlazingTableView::names() const{
  return this->columnNames;
}

std::unique_ptr<BlazingTable> BlazingTableView::clone() const {
  std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(this->table);
  return std::make_unique<BlazingTable>(std::move(cudfTable), this->columnNames);
}


} // namespace frame

namespace cache{

  std::string randomString(std::size_t length)
  {
      const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

      std::random_device random_device;
      std::mt19937 generator(random_device());
      std::uniform_int_distribution<> distribution(0, characters.size() - 1);

      std::string random_string;

      for (std::size_t i = 0; i < length; ++i)
      {
          random_string += characters[distribution(generator)];
      }

      return random_string;
  }

  unsigned long long CacheDataLocalFile::sizeInBytes(){
    struct stat st;

    if(stat(this->filePath_.c_str(),&st)==0)
        return (st.st_size);
    else
        throw;
  }

  std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache(){
    cudf_io::read_orc_args in_args{cudf_io::source_info{this->filePath_}};
    auto result = cudf_io::read_orc(in_args);
    return std::make_unique<ral::frame::BlazingTable>(
      std::move(result.tbl),
      this->names);
  }

  CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table) {
    //TODO: make this configurable
    this->filePath_ ="/tmp/.blazing-temp-" + randomString(64) + ".orc";
    this->names = table->names();
    std::cout << "CacheDataLocalFile: " << this->filePath_ << std::endl;
    cudf_io::table_metadata metadata;
    for (auto name : table->names()) {
        metadata.column_names.emplace_back(name);
    }
    cudf_io::write_orc_args out_args(
      cudf_io::sink_info{this->filePath_},
      table->view(), &metadata);

    cudf_io::write_orc(out_args);
  }


  bool CacheMachine::finished(){
      if(gpuData.size() > 0){
        return false;
      }
      for(int cacheIndex = 1; cacheIndex < cache.size(); cacheIndex++){
        if(cache.size() > 0){
          return false;
        }
      }
      return this->_finished;
    }

 void CacheMachine::finish(){
   this->_finished = true;
 }

  std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache(){
    {
      std::lock_guard<std::mutex> lock(cacheMutex);
      if(gpuData.size() > 0){
        auto data = std::move(gpuData.front());
        gpuData.pop();
        usedMemory[0] -= data->sizeInBytes();
        return std::move(data); //blocks  until it can do this, can return nullptr

      }
    }

    //a different option would be to use cv.notify here
    for(int cacheIndex = 1; cacheIndex < memoryPerCache.size(); cacheIndex++){
      {
        std::lock_guard<std::mutex> lock(cacheMutex);
        if(cache[cacheIndex]->size() > 0){
          auto data = std::move(cache[cacheIndex]->front());
          cache[cacheIndex]->pop();
          usedMemory[cacheIndex] -= data->sizeInBytes();
          return std::move(data->decache());
        }
      }
    }
    //there are no chunks
    //make condition variable  and Wait
    //for now hack it by calling pullFromCache again
    if(_finished){
      return nullptr;
    }
    // TODO: Fix or ask @felipe latter
    //    return pullFromCache();
  }

  void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table){
    for(int cacheIndex = 0; cacheIndex < memoryPerCache.size(); cacheIndex++){
      if(usedMemory[cacheIndex] <= (memoryPerCache[cacheIndex] + table->sizeInBytes())){
        usedMemory[cacheIndex]+= table->sizeInBytes();
        if(cacheIndex == 0){
            std::lock_guard<std::mutex> lock(cacheMutex);
            gpuData.push(std::move(table));
        }else{
            std::thread t([ table = std::move(table),this,cacheIndex] () mutable {
              if(this->cachePolicyTypes[cacheIndex] == LOCAL_FILE){
                std::lock_guard<std::mutex> lock(cacheMutex);
                auto item = std::make_unique<CacheDataLocalFile>(std::move(table));
                this->cache[cacheIndex]->push(std::move(item));
                //NOTE: Wait don't kill the main process until the last thread is finished!
              }
          });
          t.detach();
        }
        break;
      }
    }

  }

  WaitingCacheMachine::WaitingCacheMachine(unsigned long long gpuMemory, std::vector<unsigned long long> memoryPerCache, std::vector<CacheDataType> cachePolicyTypes) :
  CacheMachine(gpuMemory,memoryPerCache,cachePolicyTypes){

  }

  CacheMachine::~CacheMachine(){

  }

  CacheMachine::CacheMachine(unsigned long long gpuMemory, std::vector<unsigned long long> memoryPerCache_, std::vector<CacheDataType> cachePolicyTypes_)
  : _finished(false){

    cache.resize(cachePolicyTypes_.size() + 1);
    for(size_t i = 0; i < cache.size(); i++) {
        cache[i] = std::make_unique< std::queue<std::unique_ptr<CacheData>> >();
    }
    this->memoryPerCache.push_back(gpuMemory);
    for( auto mem : memoryPerCache_){
        this->memoryPerCache.push_back(mem);
    }

    usedMemory.resize(cache.size(),0UL);
    this->cachePolicyTypes.push_back(GPU);
    for(auto policy : cachePolicyTypes_){
      this->cachePolicyTypes.push_back(policy);
    }


  }


} // namespace cache

} // namespace ral
