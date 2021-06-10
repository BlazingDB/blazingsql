
#include "DataLoader.h"

#include <numeric>

#include "utilities/CommonOperations.h"
#include "utilities/CodeTimer.h"
#include <blazingdb/io/Library/Logging/Logger.h>
#include "ExceptionHandling/BlazingThread.h"
#include <cudf/filling.hpp>
#include <cudf/column/column_factories.hpp>
#include "parser/CalciteExpressionParsing.h"
#include "execution_kernels/LogicalFilter.h"

#include <spdlog/spdlog.h>
using namespace fmt::literals;
namespace ral {
// TODO: namespace frame should be remove from here
namespace io {


data_loader::data_loader(std::shared_ptr<data_parser> _parser, std::shared_ptr<data_provider> _data_provider)
	: provider(_data_provider), parser(_parser) {}

std::shared_ptr<data_loader> data_loader::clone() {
	auto cloned_provider = this->provider->clone();
	return std::make_shared<data_loader>(this->parser, cloned_provider);
}

data_loader::~data_loader() {}

void data_loader::get_schema(Schema & schema, std::vector<std::pair<std::string, cudf::type_id>> non_file_columns) {
	bool got_schema = false;
	while (!got_schema && this->provider->has_next()){
		data_handle handle = this->provider->get_next();
		if (handle.file_handle != nullptr){
			this->parser->parse_schema(handle, schema);
			if (schema.get_num_columns() > 0){
				got_schema = true;
				schema.add_file(handle.uri.toString(true));
			}
		}
	}
	if (!got_schema){
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		std::string log_detail = "ERROR: Could not get schema";
		if(logger){
		    logger->error("|||{info}|||||","info"_a=log_detail);
		}
	}
		
	bool open_file = false;
	while (this->provider->has_next()){
		std::vector<data_handle> handles = this->provider->get_some(64, open_file);
		for(auto handle : handles) {
			schema.add_file(handle.uri.toString(true));
		}
	}

	for(auto extra_column : non_file_columns) {
		schema.add_column(extra_column.first, extra_column.second, 0, false);
	}
	this->provider->reset();
}

std::unique_ptr<ral::frame::BlazingTable> data_loader::get_metadata(int offset,
	std::map<std::string, std::string> args_map)
{
	std::size_t NUM_FILES_AT_A_TIME = 64;
	std::vector<std::unique_ptr<ral::frame::BlazingTable>> metadata_batches;
	while(this->provider->has_next()){
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
		std::vector<data_handle> handles = this->provider->get_some(NUM_FILES_AT_A_TIME);
		for(auto handle : handles) {
			files.push_back(handle.file_handle);
		}
		metadata_batches.emplace_back(this->parser->get_metadata(handles, offset, args_map));
		offset += files.size();
		this->provider->close_file_handles();
	}
	this->provider->reset();
	if (metadata_batches.size() == 1){
		return std::move(metadata_batches[0]);
	} else {
		if(ral::utilities::checkIfConcatenatingStringsWillOverflow(metadata_batches)) {
            std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
            if(logger){
                logger->warn("|||{info}|||||",
						"info"_a="In data_loader::get_metadata Concatenating will overflow strings length");
            }
		}

		return ral::utilities::concatTables(std::move(metadata_batches));
	}
}

} /* namespace io */
} /* namespace ral */
