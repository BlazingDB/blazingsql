

#include "LogicPrimitives.h"
#include <sys/stat.h>


#include <random>
#include <src/utilities/CommonOperations.h>

#include "cudf/column/column_factories.hpp"

namespace ral {

namespace frame{

BlazingTable::BlazingTable(std::vector<std::unique_ptr<BlazingColumn>> columns, const std::vector<std::string> & columnNames)
	: columns(std::move(columns)), columnNames(columnNames) {}


BlazingTable::BlazingTable(	std::unique_ptr<CudfTable> table, const std::vector<std::string> & columnNames){

	std::vector<std::unique_ptr<CudfColumn>> columns_in = table->release();
	for (size_t i = 0; i < columns_in.size(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnOwner>(std::move(columns_in[i])));
	}
	this->columnNames = columnNames;
}

BlazingTable::BlazingTable(const CudfTableView & table, const std::vector<std::string> & columnNames){
	for (size_t i = 0; i < table.num_columns(); i++){
		columns.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	this->columnNames = columnNames;
}

CudfTableView BlazingTable::view() const{
	std::vector<CudfColumnView> column_views(columns.size());
	for (size_t i = 0; i < columns.size(); i++){
		column_views[i] = columns[i]->view();
	}
	return CudfTableView(column_views);
}

std::vector<std::string> BlazingTable::names() const{
	return this->columnNames;
}

BlazingTableView BlazingTable::toBlazingTableView() const{
	return BlazingTableView(this->view(), this->columnNames);
}

std::unique_ptr<CudfTable> BlazingTable::releaseCudfTable() {
	std::vector<std::unique_ptr<CudfColumn>> columns_out;
	for (size_t i = 0; i < columns.size(); i++){
		columns_out.emplace_back(std::move(columns[i]->release()));
	}
	return std::make_unique<CudfTable>(std::move(columns_out));
}

std::vector<std::unique_ptr<BlazingColumn>> BlazingTable::releaseBlazingColumns() {
	return std::move(columns);
}


unsigned long long BlazingTable::sizeInBytes()
{
	unsigned long long total_size = 0UL;
// TODO: Fix this
// 	for(cudf::size_type i = 0; i < this->table->num_columns(); ++i) {
// 		const cudf::column_view & column = this->table->get_column(i);
// 		if(column.type().id() == cudf::type_id::STRING) {
// 			auto num_children = column.num_children();
// 			if(num_children == 2) {
// 				auto offsets_column = column.child(0);
// 				auto chars_column = column.child(1);

// 				total_size += chars_column.size();
// 				cudf::data_type offset_dtype(cudf::type_id::INT32);
// 				total_size += offsets_column.size() * cudf::size_of(offset_dtype);
// 				if(column.has_nulls()) {
// 					total_size += cudf::bitmask_allocation_size_bytes(column.size());
// 				}
// 			} else {
// //					std::cerr << "string column with no children\n";
// 			};
// 		} else {
// 			total_size += column.size() * cudf::size_of(column.type());
// 			if(column.has_nulls()) {
// 				total_size += cudf::bitmask_allocation_size_bytes(column.size());
// 			}
// 		}
// 	}
	return total_size;
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

std::vector<std::unique_ptr<BlazingColumn>> BlazingTableView::toBlazingColumns() const{
	return cudfTableViewToBlazingColumns(this->table);
}

std::vector<std::string> BlazingTableView::names() const{
	return this->columnNames;
}

std::unique_ptr<BlazingTable> BlazingTableView::clone() const {
	std::unique_ptr<CudfTable> cudfTable = std::make_unique<CudfTable>(this->table);
	return std::make_unique<BlazingTable>(std::move(cudfTable), this->columnNames);
}

std::unique_ptr<ral::frame::BlazingTable> createEmptyBlazingTable(std::vector<cudf::type_id> column_types,
																  std::vector<std::string> column_names) {
	std::vector< std::unique_ptr<cudf::column> > empty_columns;
	empty_columns.resize(column_types.size());
	for(int i = 0; i < column_types.size(); ++i) {
		cudf::type_id col_type = column_types[i];
		cudf::data_type dtype(col_type);
		std::unique_ptr<cudf::column> empty_column = cudf::make_empty_column(dtype);
		empty_columns[i] = std::move(empty_column);
	}

	std::unique_ptr<CudfTable> cudf_table = std::make_unique<CudfTable>(std::move(empty_columns));
	return std::make_unique<BlazingTable>(std::move(cudf_table), column_names);
}

std::vector<std::unique_ptr<BlazingColumn>> cudfTableViewToBlazingColumns(const CudfTableView & table){
	std::vector<std::unique_ptr<BlazingColumn>> columns_out;
	for (size_t i = 0; i < table.num_columns(); i++){
		columns_out.emplace_back(std::make_unique<BlazingColumnView>(table.column(i)));
	}
	return columns_out;
}

} // end namespace frame

namespace cache {


std::string randomString(std::size_t length) {
	const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	std::random_device random_device;
	std::mt19937 generator(random_device());
	std::uniform_int_distribution<> distribution(0, characters.size() - 1);

	std::string random_string;

	for(std::size_t i = 0; i < length; ++i) {
		random_string += characters[distribution(generator)];
	}

	return random_string;
}

unsigned long long CacheDataLocalFile::sizeInBytes() {
	struct stat st;

	if(stat(this->filePath_.c_str(), &st) == 0)
		return (st.st_size);
	else
		throw;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataLocalFile::decache() {
	cudf_io::read_orc_args in_args{cudf_io::source_info{this->filePath_}};
	auto result = cudf_io::read_orc(in_args);
	return std::make_unique<ral::frame::BlazingTable>(std::move(result.tbl), this->names);
}

CacheDataLocalFile::CacheDataLocalFile(std::unique_ptr<ral::frame::BlazingTable> table) {
	// TODO: make this configurable
	this->filePath_ = "/tmp/.blazing-temp-" + randomString(64) + ".orc";
	this->names = table->names();
	std::cout << "CacheDataLocalFile: " << this->filePath_ << std::endl;
	cudf_io::table_metadata metadata;
	for(auto name : table->names()) {
		metadata.column_names.emplace_back(name);
	}
	cudf_io::write_orc_args out_args(cudf_io::sink_info{this->filePath_}, table->view(), &metadata);

	cudf_io::write_orc(out_args);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

CacheMachine::CacheMachine(unsigned long long gpuMemory,
	std::vector<unsigned long long> memoryPerCache_,
	std::vector<CacheDataType> cachePolicyTypes_)
	: _finished(false) {

  	waitingCache = std::make_unique<WaitingQueue<CacheData>>(this->_finished);

	this->memoryPerCache.push_back(gpuMemory);
	for(auto mem : memoryPerCache_) {
		this->memoryPerCache.push_back(mem);
	}

	this->usedMemory.resize(cachePolicyTypes_.size() + 1, 0UL);
	this->cachePolicyTypes.push_back(GPU);
	for(auto policy : cachePolicyTypes_) {
		this->cachePolicyTypes.push_back(policy);
	}
}

CacheMachine::~CacheMachine() {}


void CacheMachine::finish() {
	this->_finished = true;
	this->waitingCache->notify();
}

void CacheMachine::addToCache(std::unique_ptr<ral::frame::BlazingTable> table) {
	for(int cacheIndex = 0; cacheIndex < memoryPerCache.size(); cacheIndex++) {
		if(usedMemory[cacheIndex] <= (memoryPerCache[cacheIndex] + table->sizeInBytes())) {
			usedMemory[cacheIndex] += table->sizeInBytes();
			if(cacheIndex == 0) {
        auto cache_data = std::make_unique<GPUCacheData>(std::move(table));
        std::unique_ptr<message<CacheData>> item =
          std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
        this->waitingCache->put(std::move(item));

      } else {
				std::thread t([table = std::move(table), this, cacheIndex]() mutable {
					if(this->cachePolicyTypes[cacheIndex] == LOCAL_FILE) {
						auto cache_data = std::make_unique<CacheDataLocalFile>(std::move(table));
						std::unique_ptr<message<CacheData>> item =
							std::make_unique<message<CacheData>>(std::move(cache_data), cacheIndex);
						this->waitingCache->put(std::move(item));
						// NOTE: Wait don't kill the main process until the last thread is finished!
					}
				});
				t.detach();
			}
			break;
		}
	}
}

bool CacheMachine::is_finished() {
	if(not waitingCache->empty()) {
		return false;
	}
	return this->_finished;
}


std::unique_ptr<ral::frame::BlazingTable> CacheMachine::pullFromCache() {
  std::unique_ptr<message<CacheData>> message_data = waitingCache->pop_or_wait();
  if (message_data == nullptr) {
  	return nullptr;
  }
  auto cache_data = message_data->releaseData();
  auto cache_index = message_data->cacheIndex();
  usedMemory[cache_index] -= cache_data->sizeInBytes();
  return std::move(cache_data->decache());
}


ConcatenatingCacheMachine::ConcatenatingCacheMachine(unsigned long long gpuMemory,
                                         std::vector<unsigned long long> memoryPerCache,
                                         std::vector<CacheDataType> cachePolicyTypes_)
  : CacheMachine(gpuMemory, memoryPerCache, cachePolicyTypes_)
{
}

std::unique_ptr<ral::frame::BlazingTable> ConcatenatingCacheMachine::pullFromCache() {
  std::vector<std::unique_ptr<ral::frame::BlazingTable>> holder_samples;
  std::vector<ral::frame::BlazingTableView> samples;
  auto all_messages_data = waitingCache->get_all_or_wait();
  for (auto& message_data : all_messages_data) {
    auto cache_data = message_data->releaseData();
    auto cache_index = message_data->cacheIndex();
    usedMemory[cache_index] -= cache_data->sizeInBytes();
    auto tmp_frame = cache_data->decache();
    samples.emplace_back(tmp_frame->toBlazingTableView());
    holder_samples.emplace_back(std::move(tmp_frame));
  }
  return ral::utilities::experimental::concatTables(samples);
}


}  // namespace cache

}  // namespace ral
