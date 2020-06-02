
#include "DataLoader.h"

#include <numeric>

#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"
#include <CodeTimer.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "blazingdb/concurrency/BlazingThread.h"
#include <cudf/filling.hpp>
#include <cudf/column/column_factories.hpp>
#include "CalciteExpressionParsing.h"
#include "execution_graph/logic_controllers/LogicalFilter.h"
namespace ral {
// TODO: namespace frame should be remove from here
namespace io {

namespace {
using blazingdb::manager::Context;
}  // namespace

data_loader::data_loader(std::shared_ptr<data_parser> _parser, std::shared_ptr<data_provider> _data_provider)
	: provider(_data_provider), parser(_parser) {}

std::shared_ptr<data_loader> data_loader::clone() {
	auto cloned_provider = this->provider->clone();
	return std::make_shared<data_loader>(this->parser, cloned_provider);
}

data_loader::~data_loader() {}


std::unique_ptr<ral::frame::BlazingTable> data_loader::load_data(
	Context * context,
	const std::vector<size_t> & column_indices_in,
	const Schema & schema,
  std::string filterString) {

	std::vector<size_t> column_indices = column_indices_in;
	if(column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(schema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<data_handle> files;

	// iterates through files and parses them into columns
	this->provider->reset();
	while(this->provider->has_next()) {
		auto handle = this->provider->get_next();
		if (handle.is_valid())
			files.emplace_back(std::move(handle));
	}
	size_t num_files = files.size();

	std::string loadMsg = "DataLoader going to load " + std::to_string(files.size()) + " files";
	Library::Logging::Logger().logTrace(ral::utilities::buildLogString(std::to_string(context->getContextToken()),
  			std::to_string(context->getQueryStep()), std::to_string(context->getQuerySubstep()), loadMsg));

	std::vector< std::unique_ptr<ral::frame::BlazingTable> > blazingTable_per_file;
	blazingTable_per_file.resize(num_files);

	// TODO NOTE percy c.gonzales rommel fix our concurrent reads here (better use of thread)
	// make sure cudf supports concurrent reads
	int MAX_NUM_LOADING_THREADS = 8;
	std::vector<std::vector<data_handle>> file_sets(num_files < MAX_NUM_LOADING_THREADS ? num_files : MAX_NUM_LOADING_THREADS);
	for (int i = 0; i < num_files; i++){
		file_sets[i % MAX_NUM_LOADING_THREADS].emplace_back(std::move(files[i]));
    }

	std::vector<BlazingThread> threads;

	for(int file_set_index = 0; file_set_index < file_sets.size(); file_set_index++) {
		threads.push_back(BlazingThread([&, file_set_index]() {
			for (int file_in_set = 0; file_in_set < file_sets[file_set_index].size(); file_in_set++) {
				int file_index = file_in_set * MAX_NUM_LOADING_THREADS + file_set_index;

				if (file_sets[file_set_index][file_in_set].fileHandle != nullptr) {

					Schema fileSchema = schema.fileSchema(file_index);

					std::vector<size_t> column_indices_in_file;
					for (int i = 0; i < column_indices.size(); i++){
						if(schema.get_in_file()[column_indices[i]]) {
							column_indices_in_file.push_back(column_indices[i]);
						}
					}

					if (schema.all_in_file()){
						std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse(file_sets[file_set_index][file_in_set].fileHandle, fileSchema, column_indices_in_file);

						if(filterString != ""){
							blazingTable_per_file[file_index] = std::move(ral::processor::process_filter(loaded_table->toBlazingTableView(), filterString, context));
						}
						else{
							blazingTable_per_file[file_index] =  std::move(loaded_table);
						}
					} else {
						std::vector<std::unique_ptr<cudf::column>> all_columns(column_indices.size());
						std::vector<std::unique_ptr<cudf::column>> file_columns;
						std::vector<std::string> names;
						cudf::size_type num_rows;
						if (column_indices_in_file.size() > 0){
							std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse(file_sets[file_set_index][file_in_set].fileHandle, fileSchema, column_indices_in_file);
							names = current_blazing_table->names();
							std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
							num_rows = current_table->num_rows();
							file_columns = current_table->release();
						} else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
							std::vector<size_t> temp_column_indices = {0};
							std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse(file_sets[file_set_index][file_in_set].fileHandle, fileSchema, temp_column_indices);
							num_rows = loaded_table->num_rows();
						}

						int in_file_column_counter = 0;
						for(int i = 0; i < column_indices.size(); i++) {
							int col_ind = column_indices[i];
							if(!schema.get_in_file()[col_ind]) {
								std::string name = schema.get_name(col_ind);
								names.push_back(name);
								cudf::type_id type = schema.get_dtype(col_ind);
								std::string literal_str = file_sets[file_set_index][file_in_set].column_values[name];
								std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf::data_type{type});
								all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
							} else {
								all_columns[i] = std::move(file_columns[in_file_column_counter]);
								in_file_column_counter++;
							}
						}
						auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
						if(filterString != ""){
							auto temp = std::move(std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names));
							blazingTable_per_file[file_index] = std::move(ral::processor::process_filter(temp->toBlazingTableView(), filterString, context));
						}else{
							blazingTable_per_file[file_index] = std::move(std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names));
						}
					}
				} else {
					Library::Logging::Logger().logError(ral::utilities::buildLogString(
						"", "", "", "ERROR: Was unable to open " + file_sets[file_set_index][file_in_set].uri.toString()));
				}
			}
		}));
	}
	std::for_each(threads.begin(), threads.end(), [](BlazingThread & this_thread) { this_thread.join(); });

	// checking if any errors occurred
	std::vector<std::string> provider_errors = this->provider->get_errors();
	if(provider_errors.size() != 0) {
		for(size_t error_index = 0; error_index < provider_errors.size(); error_index++) {
			Library::Logging::Logger().logError(
				ral::utilities::buildLogString("", "", "", "ERROR: " + provider_errors[error_index]));
		}
	}

	this->provider->reset();

	size_t num_columns;

	if(num_files > 0) {
		num_columns = blazingTable_per_file[0]->num_columns();
	}

	if(num_files == 0 || num_columns == 0) {
		// GDFParse is parsed here
		std::unique_ptr<ral::frame::BlazingTable> parsed_table = parser->parse(nullptr, schema, column_indices);
		bool if_null_empty_load = (parsed_table == nullptr || parsed_table->num_columns() == 0);
		if (if_null_empty_load) {
			parsed_table = schema.makeEmptyBlazingTable(column_indices);
		}else if(filterString != ""){
			return std::move(ral::processor::process_filter(parsed_table->toBlazingTableView(), filterString, context));
		}

		return std::move(parsed_table);
	}

	if(num_files == 1) {  // we have only one file so we can just return the columns we parsed from that file
		return std::move(blazingTable_per_file[0]);

	} else {  // we have more than one file so we need to concatenate

		std::vector<ral::frame::BlazingTableView> table_views;
		for (int i = 0; i < blazingTable_per_file.size(); i++){
			if(blazingTable_per_file[i]->num_rows() > 0){
				table_views.push_back(std::move(blazingTable_per_file[i]->toBlazingTableView()));
			}
			else{
				auto empty_table = schema.makeEmptyBlazingTable(column_indices);
				table_views.push_back(std::move(empty_table->toBlazingTableView()));
			}
		}

		return ral::utilities::concatTables(table_views);
	}
}


std::unique_ptr<ral::frame::BlazingTable> data_loader::load_batch(
	Context * context,
	const std::vector<size_t> & column_indices_in,
	const Schema & schema,
	data_handle file_data_handle,
	size_t file_index,
	std::vector<cudf::size_type> row_group_ids) {

	auto fileSchema = schema.fileSchema(file_index);

	std::vector<size_t> column_indices = column_indices_in;
	if (column_indices.size() == 0) {  // including all columns by default
		column_indices.resize(fileSchema.get_num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	if (schema.all_in_file()){
		std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(file_data_handle.fileHandle, fileSchema, column_indices, row_group_ids);
		return std::move(loaded_table);
	} else {
		std::vector<size_t> column_indices_in_file;  // column indices that are from files
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
			std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse(file_data_handle.fileHandle, fileSchema, column_indices_in_file);
			names = current_blazing_table->names();
			std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
			num_rows = current_table->num_rows();
			file_columns = current_table->release();
		} else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
			std::vector<size_t> temp_column_indices = {0};
			std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse(file_data_handle.fileHandle, fileSchema, temp_column_indices);
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
				std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf::data_type{type});
				all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
			} else {
				all_columns[i] = std::move(file_columns[in_file_column_counter]);
				in_file_column_counter++;
			}
		}
		auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
		return std::move(std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names));
	}
}


void data_loader::get_schema(Schema & schema, std::vector<std::pair<std::string, cudf::type_id>> non_file_columns) {
	bool got_schema = false;
	while (!got_schema && this->provider->has_next()){
		data_handle handle = this->provider->get_next();
		if (handle.fileHandle != nullptr){
			this->parser->parse_schema(handle.fileHandle, schema);
			if (schema.get_num_columns() > 0){
				got_schema = true;
				schema.add_file(handle.uri.toString(true));
			}
		}
	}
	if (!got_schema){
		std::cout<<"ERROR: Could not get schema"<<std::endl;
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
			files.push_back(handle.fileHandle);
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
		return ral::utilities::concatTables(metadata_batche_views);
	}
}

} /* namespace io */
} /* namespace ral */
