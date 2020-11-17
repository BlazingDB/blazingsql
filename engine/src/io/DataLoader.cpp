
#include "DataLoader.h"

#include <numeric>

#include "utilities/CommonOperations.h"

#include <CodeTimer.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/concurrency/BlazingThread.h"
#include <cudf/filling.hpp>
#include <cudf/column/column_factories.hpp>
#include "CalciteExpressionParsing.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"

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


std::unique_ptr<ral::frame::BlazingTable> data_loader::load_batch(
	const std::vector<int> & column_indices_in,
	const Schema & schema,
	data_handle file_data_handle,
	size_t file_index,
	std::vector<cudf::size_type> row_group_ids) {

/*	std::vector<int> column_indices = column_indices_in;
	if (column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}
	
	// this is the fileSchema we want to send to parse_batch since they only the columns and row_groups we want to get from a file or gdf as opposed to hive
	auto fileSchema = schema.fileSchema(file_index); 

	if (schema.all_in_file()){
		std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(file_data_handle.fileHandle, fileSchema, column_indices, row_group_ids);
		return std::move(loaded_table);
	} else {
		std::vector<int> column_indices_in_file;  // column indices that are from files
		for (int i = 0; i < column_indices.size(); i++){
			if(schema.get_in_file()[column_indices[i]]) {
				column_indices_in_file.push_back(column_indices[i]);
			}
		}
		
		std::vector<std::unique_ptr<cudf::column>> all_columns(column_indices.size());
		std::vector<std::unique_ptr<cudf::column>> file_columns;
		std::vector<std::string> names;
		cudf::size_type num_rows;
		if (column_indices_in_file.size() > 0){
			std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse_batch(file_data_handle.fileHandle, fileSchema, column_indices_in_file, row_group_ids);
			names = current_blazing_table->names();
			std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
			num_rows = current_table->num_rows();
			file_columns = current_table->release();
		
		} else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
			std::vector<int> temp_column_indices = {0};
			std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(file_data_handle.fileHandle, fileSchema, temp_column_indices, row_group_ids);
			num_rows = loaded_table->num_rows();
		}

		int in_file_column_counter = 0;
		for(int i = 0; i < column_indices.size(); i++) {
			int col_ind = column_indices[i];
			if(!schema.get_in_file()[col_ind]) {
				std::string name = schema.get_name(col_ind);
				names.push_back(name);
				cudf::type_id type = schema.get_dtype(col_ind);
				std::string literal_str = file_data_handle.column_values[name];
				std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf::data_type{type},false);
				all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
			} else {
				all_columns[i] = std::move(file_columns[in_file_column_counter]);
				in_file_column_counter++;
			}
		}
		auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
		return std::move(std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names));
	}
	*/
}


void data_loader::get_schema(Schema & schema, std::vector<std::pair<std::string, cudf::type_id>> non_file_columns) {
	bool got_schema = false;
	while (!got_schema && this->provider->has_next()){
		data_handle handle = this->provider->get_next();
		if (handle.file_handle != nullptr){
			this->parser->parse_schema(handle.file_handle, schema);
			if (schema.get_num_columns() > 0){
				got_schema = true;
				schema.add_file(handle.uri.toString(true));
			}
		}
	}
	if (!got_schema){
		auto logger = spdlog::get("batch_logger");
		std::string log_detail = "ERROR: Could not get schema";
		logger->error("|||{info}|||||","info"_a=log_detail);
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

std::unique_ptr<ral::frame::BlazingTable> data_loader::get_metadata(int offset) {

	std::size_t NUM_FILES_AT_A_TIME = 64;
	std::vector<std::unique_ptr<ral::frame::BlazingTable>> metadata_batches;
	std::vector<ral::frame::BlazingTableView> metadata_batche_views;
	while(this->provider->has_next()){
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
		std::vector<data_handle> handles = this->provider->get_some(NUM_FILES_AT_A_TIME);
		for(auto handle : handles) {
			files.push_back(handle.file_handle);
		}
		metadata_batches.emplace_back(this->parser->get_metadata(files,  offset));
		metadata_batche_views.emplace_back(metadata_batches.back()->toBlazingTableView());
		offset += files.size();
		this->provider->close_file_handles();
	}
	this->provider->reset();
	if (metadata_batches.size() == 1){
		return std::move(metadata_batches[0]);
	} else {
		if( ral::utilities::checkIfConcatenatingStringsWillOverflow(metadata_batche_views) ) {
			auto logger = spdlog::get("batch_logger");
			logger->warn("|||{info}|||||",
						"info"_a="In data_loader::get_metadata Concatenating will overflow strings length");
		}

		return ral::utilities::concatTables(metadata_batche_views);
	}
}

} /* namespace io */
} /* namespace ral */
