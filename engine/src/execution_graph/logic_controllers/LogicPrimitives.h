
#pragma once

#include "cudf/column/column_view.hpp"
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include <memory>
#include <string>
#include <vector>
#include <blazingdb/manager/Context.h> 
#include <cudf/io/functions.hpp>
#include <mutex>
#include <queue>
#include <thread>
#include <src/from_cudf/cpp_tests/utilities/column_utilities.hpp>

typedef cudf::experimental::table CudfTable;
typedef cudf::table_view CudfTableView;
typedef cudf::column CudfColumn;
typedef cudf::column_view CudfColumnView;
namespace cudf_io = cudf::experimental::io;

namespace ral {

namespace frame {

class BlazingTable;
class BlazingTableView;

class BlazingTable {
public:
	BlazingTable(std::unique_ptr<CudfTable> table, std::vector<std::string> columnNames);
	BlazingTable(BlazingTable &&) = default;
	BlazingTable & operator=(BlazingTable const &) = delete;
	BlazingTable & operator=(BlazingTable &&) = delete;

	CudfTableView view() const;
	cudf::size_type num_columns() const { return table->num_columns(); }
	cudf::size_type num_rows() const { return table->num_rows(); }
	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> &names) { this->columnNames = names; }

	BlazingTableView toBlazingTableView() const;

	operator bool() const { return table != nullptr; }

	std::unique_ptr<CudfTable> releaseCudfTable() { return std::move(table);}

	unsigned long long sizeInBytes(){
        unsigned long long total_size = 0UL;
        for(cudf::size_type i = 0; i < this->table->num_columns(); ++i) {
            const cudf::column_view &column = this->table->get_column(i);
			if (column.type().id() == cudf::type_id::STRING) {
                auto num_children = column.num_children();
                assert(num_children == 2);

                auto offsets_column = column.child(0);
                auto chars_column = column.child(1);

                total_size += chars_column.size();
                cudf::data_type offset_dtype (cudf::type_id::INT32);
                total_size += offsets_column.size() * cudf::size_of(offset_dtype);
                if (column.has_nulls()) {
                    total_size += cudf::bitmask_allocation_size_bytes(column.size());
                }
            } else {
                total_size += column.size() * cudf::size_of(column.type());
                if (column.has_nulls()) {
                    total_size += cudf::bitmask_allocation_size_bytes(column.size());
                }
			}
        }
		return total_size;
	}
private:
	std::vector<std::string> columnNames;
	std::unique_ptr<CudfTable> table;
};


class BlazingTableView {
public:
	BlazingTableView();
	BlazingTableView(CudfTableView table, std::vector<std::string> columnNames);
	BlazingTableView(BlazingTableView const &) = default;
	BlazingTableView(BlazingTableView &&) = default;

	BlazingTableView & operator=(BlazingTableView const &) = default;
	BlazingTableView & operator=(BlazingTableView &&) = default;

	CudfTableView view() const;

	cudf::column_view const & column(cudf::size_type column_index) const { return table.column(column_index); }

	std::vector<std::string> names() const;
	// set columnNames
	void setNames(const std::vector<std::string> &names) { this->columnNames = names; }

	cudf::size_type num_columns() const { return table.num_columns(); }

	cudf::size_type num_rows() const { return table.num_rows(); }

	std::unique_ptr<BlazingTable> clone() const;

private:
	std::vector<std::string> columnNames;
	CudfTableView table;
};

typedef std::pair<std::unique_ptr<ral::frame::BlazingTable>, ral::frame::BlazingTableView> TableViewPair;

}  // namespace frame

namespace cache{

	enum CacheDataType{
			GPU,
			CPU,
			LOCAL_FILE,
			IO_FILE
		};



class CacheData{
public:


	virtual std::unique_ptr<ral::frame::BlazingTable> decache() = 0;
	virtual unsigned long long sizeInBytes() = 0;
	virtual ~CacheData(){

	}
protected:
	std::vector<std::string> names;

};

class CacheDataLocalFile : public CacheData{
public:
  std::unique_ptr<ral::frame::BlazingTable> decache() override;

  CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table);
	unsigned long long sizeInBytes() override;
  virtual ~CacheDataLocalFile(){

  }
  std::string filePath() const {
    return filePath_;
  }
private:
  std::string filePath_;
  //ideally would be a writeable file
};


class CacheMachine{
public:
	CacheMachine(unsigned long long gpuMemory, std::vector<unsigned long long> memoryPerCache, std::vector<CacheDataType> cachePolicyTypes);
	virtual std::unique_ptr<ral::frame::BlazingTable> pullFromCache();
//  void eject(int partitionNumber);
//  void reprocess(int partitionNumber);
	virtual ~CacheMachine();
	void addToCache(std::unique_ptr<ral::frame::BlazingTable> table);
//  void reduceCache(int cacheIndex, unsigned long long reductionAmount);
//  void incrementCache(int cacheIndex, unsigned long long incrementAmount);
//TODO: figure out how we tell it which cache to prefer

  bool finished();
	void finish();
protected:
	std::mutex cacheMutex;
	std::vector<std::unique_ptr<std::queue<std::unique_ptr<CacheData> > > > cache;
  std::queue<std::unique_ptr<ral::frame::BlazingTable> > gpuData;
//  std::map<int,std::unique_ptr<CacheData> > operatingData;

	std::vector<CacheDataType> cachePolicyTypes;
  std::vector<unsigned long long> memoryPerCache;
  std::vector<unsigned long long> usedMemory;
  bool _finished;
};

static std::string type_string(cudf::data_type dtype) {
    using namespace cudf;

    switch (dtype.id()) {
        case BOOL8: return "BOOL8";
        case INT8:  return "INT8";
        case INT16: return "INT16";
        case INT32: return "INT32";
        case INT64: return "INT64";
        case FLOAT32: return "FLOAT32";
        case FLOAT64: return "FLOAT64";
        case STRING:  return "STRING";
        case TIMESTAMP_DAYS: return "TIMESTAMP_DAYS";
        case TIMESTAMP_SECONDS: return "TIMESTAMP_SECONDS";
        case TIMESTAMP_MILLISECONDS: return "TIMESTAMP_MILLISECONDS";
        case TIMESTAMP_MICROSECONDS: return "TIMESTAMP_MICROSECONDS";
        case TIMESTAMP_NANOSECONDS: return "TIMESTAMP_NANOSECONDS";
        default: return "Unsupported type_id";
    }
}

static void print_blazing_table_view(ral::frame::BlazingTableView table_view, const std::string table_name = ""){
    std::cout<<"Table: "<<table_name<<std::endl;
    std::cout<<"\t"<<"Num Rows: "<<table_view.num_rows()<<std::endl;
    std::cout<<"\t"<<"Num Columns: "<<table_view.num_columns()<<std::endl;
    for(size_t col_idx=0; col_idx<table_view.num_columns(); col_idx++){
        std::string col_string;
        if (table_view.num_rows() > 0){
            col_string = cudf::test::to_string(table_view.column(col_idx), "|");
        }
        std::cout<<"\t"<<table_view.names().at(col_idx)<<" ("<<"type: "<<type_string(table_view.column(col_idx).type())<<"): "<<col_string<<std::endl;
    }
}

class WaitingCacheMachine : public CacheMachine{
public:
	WaitingCacheMachine(unsigned long long gpuMemory, std::vector<unsigned long long> memoryPerCache, std::vector<CacheDataType> cachePolicyTypes);
	~WaitingCacheMachine(){

	}
//	TODO:
//	std::unique_ptr<ral::frame::BlazingTable> pullFromCache();
};

  class WorkerThread{
  public:

    WorkerThread() {}
    virtual ~WorkerThread() {
    }
    virtual bool process() = 0;

  protected:
    blazingdb::manager::experimental::Context * context;
    std::string queryString;
    bool _paused;
};

	template <typename Processor>
	class SingleSourceWorkerThread : public WorkerThread{
	public:
		SingleSourceWorkerThread(
			std::shared_ptr<CacheMachine> cacheSource,
			std::shared_ptr<CacheMachine> cacheSink,
			std::string queryString,
      Processor * processor,
			blazingdb::manager::experimental::Context * context)
			    :  source(cacheSource), sink(cacheSink), WorkerThread()
			 {
				this->context = context;
				this->queryString = queryString;
				this->_paused = false;
				this->processor = processor;
		}

		//returns true when theres nothing left to process
		bool process() override {

      if(_paused || source->finished()){
        return false;
      }
      auto input = source->pullFromCache();
      if(input == nullptr){
        return true;
      }
      //TODO: fix context Type
      print_blazing_table_view(input->toBlazingTableView());
      auto output = this->processor(input->toBlazingTableView(), queryString, nullptr);
      std::cout << ">>> process filter ()\n";
      print_blazing_table_view(output->toBlazingTableView());
      sink->addToCache(std::move(output));
      std::cout << "<<< process filter ()\n";
      // TODO: Fix or ask @felipe latter
      // return process();
		}
		void resume(){
			_paused = false;
		}
		void pause(){
			_paused = true;
		}
		virtual ~SingleSourceWorkerThread() {

		}
	private:
		std::shared_ptr<CacheMachine> source;
		std::shared_ptr<CacheMachine> sink;
    Processor * processor;
	};
//
////Has two sources, waits until at least one is complte before proceeding
template <typename Processor>
class DoubleSourceWaitingWorkerThread : public WorkerThread {
	public:
		DoubleSourceWaitingWorkerThread(
			std::shared_ptr<CacheMachine> cacheSourceOne,
			std::shared_ptr<CacheMachine> cacheSourceTwo,
			std::shared_ptr<CacheMachine> cacheSink,
			std::string queryString,
      Processor * processor,
			blazingdb::manager::experimental::Context * context): sourceOne(cacheSourceOne), sourceTwo(cacheSourceTwo), sink(cacheSink)
    {
				this->context = context;
				this->queryString = queryString;
				this->_paused = false;
        this->processor = processor;
		}

		//returns true when theres nothing left to process
		bool process() override {
//TODO : complete this threads
//			std::thread left([]{
//
//			});
//
//			std::thread right([]{
//
//			});

			//USE a cv here to notify when they finish or what?
			if(_paused || sourceOne->finished()){
				return false;
			}
			auto inputOne = sourceOne->pullFromCache();
			if(inputOne == nullptr){
				return false;
			}

            if(_paused || sourceTwo->finished()){
				return false;
			}
			auto inputTwo = sourceTwo->pullFromCache();
			if(inputTwo == nullptr){
				return false;
			}
      // TODO: change blazingdb::manager::experimental::ContextType
			auto output = this->processor(this->context, inputOne->toBlazingTableView(), inputTwo->toBlazingTableView(), queryString);
			sink->addToCache(std::move(output));
			//  TODO: ask @felipe recursive call
          // process();
         return true;
		}

		void resume(){
			_paused = false;
		}

		void pause(){
			_paused = true;
		}

		virtual ~DoubleSourceWaitingWorkerThread() {

    }

	private:
		std::shared_ptr<CacheMachine> sourceOne;
    std::shared_ptr<CacheMachine> sourceTwo;
		std::shared_ptr<CacheMachine> sink;
    Processor * processor;
	};


	template <typename Processor>
  class ProcessMachine{
  public:
    ProcessMachine(
      std::shared_ptr<CacheMachine> cacheSource,
      std::shared_ptr<CacheMachine> cacheSink,
      Processor* processor, std::string queryString,
      blazingdb::manager::experimental::Context * context,
      int numWorkers)
    : source(cacheSource), sink(cacheSink),
      context(context), queryString(queryString), numWorkers(numWorkers){
				for(int i = 0; i < numWorkers; i++){
            auto thread = std::make_unique<SingleSourceWorkerThread< Processor > >(cacheSource, cacheSink, queryString, processor, context  );
				    workerThreads.emplace_back(std::move(thread));
				}
    }
    ProcessMachine(
      std::shared_ptr<CacheMachine> cacheSourceOne,
      std::shared_ptr<CacheMachine> cacheSourceTwo,
      std::shared_ptr<CacheMachine> cacheSink,
      Processor* processor, std::string queryString,
      blazingdb::manager::experimental::Context * context,
      int numWorkers)
    :   sink(cacheSink),
        context(context), queryString(queryString), numWorkers(numWorkers){
				for(int i = 0; i < numWorkers; i++){
            auto thread = std::make_unique<DoubleSourceWaitingWorkerThread< Processor > >(cacheSourceOne, cacheSourceTwo, cacheSink, queryString, processor, context  );
				    workerThreads.emplace_back(std::move(thread));
				}
    }
    void run();
    void adjustWorkerCount(int numWorkers);
  private:
		std::shared_ptr<CacheMachine> source;
		std::shared_ptr<CacheMachine> sink;
    std::vector<std::unique_ptr<WorkerThread> > workerThreads;
    int numWorkers;
		blazingdb::manager::experimental::Context * context;
    std::string queryString;
  };

  template <typename Processor>
  void ProcessMachine<Processor>::run(){
    std::vector<std::thread> threads;
    for(int threadIndex = 0; threadIndex < numWorkers; threadIndex++){
        std::thread t([this,threadIndex]{
                this->workerThreads[threadIndex]->process();
        });
        threads.push_back(std::move(t));
    }
    for(auto & thread : threads){
        thread.join();
    }
  } 


} // namespace cache

}  // namespace ral
