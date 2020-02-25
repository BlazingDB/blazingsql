
#include "DataLoader.h"
#include "Traits/RuntimeTraits.h"
#include "config/GPUManager.cuh"
#include "cudf/legacy/filling.hpp"
#include "rmm/thrust_rmm_allocator.h"
#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"
#include <CodeTimer.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <thread>
#include <cudf/filling.hpp>
#include "gdf_wrapper.cuh"

namespace ral {
// TODO: namespace frame should be remove from here
namespace io {

namespace {
using blazingdb::manager::experimental::Context;
}  // namespace

data_loader::data_loader(std::shared_ptr<data_parser> _parser, std::shared_ptr<data_provider> _data_provider)
	: provider(_data_provider), parser(_parser) {}


data_loader::~data_loader() {}


std::unique_ptr<ral::frame::BlazingTable> data_loader::load_data(
	Context * context,
	const std::vector<size_t> & column_indices,
	const Schema & schema) {

	static CodeTimer timer;
	timer.reset();

	std::vector<std::string> user_readable_file_handles;
	std::vector<data_handle> files;

	// iterates through files and parses them into columns
	while(this->provider->has_next()) {
		// a file handle that we can use in case errors occur to tell the user which file had parsing issues
		user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
		files.push_back(this->provider->get_next());
	}
	size_t num_files = files.size();

	std::vector< std::unique_ptr<ral::frame::BlazingTable> > blazingTable_per_file;
	blazingTable_per_file.resize(num_files);

	// TODO NOTE percy c.gonzales rommel fix our concurrent reads here (better use of thread)
	// make sure cudf supports concurrent reads
	int MAX_NUM_LOADING_THREADS = 8;
	std::vector<std::vector<data_handle>> file_sets(num_files < MAX_NUM_LOADING_THREADS ? num_files : MAX_NUM_LOADING_THREADS);
	for (int i = 0; i < num_files; i++){
		file_sets[i % MAX_NUM_LOADING_THREADS].emplace_back(std::move(files[i]));
    }

	std::vector<std::thread> threads;

	for(int file_set_index = 0; file_set_index < file_sets.size(); file_set_index++) {
		threads.push_back(std::thread([&, file_set_index]() {
			for (int file_in_set = 0; file_in_set < file_sets[file_set_index].size(); file_in_set++) {
				int file_index = file_in_set * MAX_NUM_LOADING_THREADS + file_set_index;
				
				if (file_sets[file_set_index][file_in_set].fileHandle != nullptr) {								
					// TODO william alex percy skipdata cudf0.12
					Schema fileSchema = schema.fileSchema(file_index);

					// // TODO: @alex, Super important to support HIVE!! tricky!!!
					std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse(file_sets[file_set_index][file_in_set].fileHandle, user_readable_file_handles[file_index], fileSchema, column_indices);
					// {

						// std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = std::move(loaded_table.first);
						// std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
						// auto num_rows = current_table->num_rows();
						// std::vector<std::unique_ptr<cudf::column>> current_columns = current_table->release();
						// for(int i = 0; i < schema.get_num_columns(); i++) {
						// 	if(!schema.get_in_file()[i]) {
						// 		std::cout<<"creating column!"<<std::endl;
						// 		std::string name = schema.get_name(i);
						// 		if(files[file_index].is_column_string[name]) {
						// // 			std::string string_value = files[file_index].string_values[name];
						// // 			NVCategory * category = repeated_string_category(string_value, num_rows);
						// // 			gdf_column_cpp column;
						// // 			column.create_gdf_column(category, num_rows, name);
						// // 			converted_data.push_back(column);
						// 		} else {
						// 			auto scalar = files[file_index].column_values[name];
						// 			size_t width_per_value = cudf::size_of(scalar->type());
						// 			auto buffer_size = width_per_value * num_rows;
						// 			rmm::device_buffer gpu_buffer(buffer_size);
						// 			auto scalar_column = std::make_unique<cudf::column>(scalar->type(), num_rows, std::move(gpu_buffer));

						// 			cudf::experimental::fill(scalar_column->mutable_view(), cudf::size_type{0}, cudf::size_type{num_rows}, *scalar);
						// 			current_columns.emplace_back(std::move(scalar_column));
						// 		}
						// 		std::cout<<"created column!"<<std::endl;
						// 	}
						// }
						// auto unique_table = std::make_unique<cudf::experimental::table>(std::move(current_columns));
						// auto new_blazing_table = std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), current_blazing_table->names());
						// tableViewPairs_per_file[file_index] = std::make_pair(std::move(new_blazing_table), new_blazing_table->toBlazingTableView());
					// } 
					{
						blazingTable_per_file[file_index] =  std::move(loaded_table);
					}
				} else {
					Library::Logging::Logger().logError(ral::utilities::buildLogString(
						"", "", "", "ERROR: Was unable to open " + user_readable_file_handles[file_index]));
				}
			}
		}));
	}
	std::for_each(threads.begin(), threads.end(), [](std::thread & this_thread) { this_thread.join(); });

	Library::Logging::Logger().logInfo(timer.logDuration(*context, "data_loader::load_data part 1 parse"));
	timer.reset();

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
		std::unique_ptr<ral::frame::BlazingTable> parsed_table = parser->parse(nullptr, "", schema, column_indices);
		bool if_null_empty_load = (parsed_table == nullptr || parsed_table->num_columns() == 0);
		if (if_null_empty_load) {
			std::vector<std::string> select_names(column_indices.size());
			std::vector<cudf::type_id> select_types(column_indices.size());
			if (column_indices.empty()) {
				select_types = schema.get_dtypes();
				select_names = schema.get_names();
			} else {
				for (int i = 0; i < column_indices.size(); i++){
					select_names[i] = schema.get_names()[column_indices[i]];
					select_types[i] = schema.get_dtypes()[column_indices[i]];
				}
			}

			parsed_table = ral::frame::createEmptyBlazingTable(select_types, select_names);
		}
		return std::move(parsed_table);
	}

	Library::Logging::Logger().logInfo(timer.logDuration(*context, "data_loader::load_data part 2 concat"));
	timer.reset();

	if(num_files == 1) {  // we have only one file so we can just return the columns we parsed from that file
		return std::move(blazingTable_per_file[0]);

	} else {  // we have more than one file so we need to concatenate

		std::vector<ral::frame::BlazingTableView> table_views;
		for (int i = 0; i < blazingTable_per_file.size(); i++)
			table_views.push_back(std::move(blazingTable_per_file[i]->toBlazingTableView()));
		
		return ral::utilities::experimental::concatTables(table_views);		
	}	
}


void data_loader::get_schema(Schema & schema, std::vector<std::pair<std::string, gdf_dtype>> non_file_columns) {
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
	bool firstIteration = true;
	std::vector<data_handle> handles = this->provider->get_all();
	for(auto handle : handles) {
		files.push_back(handle.fileHandle);
	}
	this->parser->parse_schema(files, schema);

	for(auto handle : handles) {
		schema.add_file(handle.uri.toString(true));
	}

	for(auto extra_column : non_file_columns) {
		schema.add_column(extra_column.first, to_type_id(extra_column.second), 0, false);
	}
	this->provider->reset();
}

std::unique_ptr<ral::frame::BlazingTable> data_loader::get_metadata(int offset) {
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;

	bool firstIteration = true;
	std::vector<data_handle> handles = this->provider->get_all();
	for(auto handle : handles) {
		files.push_back(handle.fileHandle);
	}
	return this->parser->get_metadata(files,  offset);
}

} /* namespace io */
} /* namespace ral */
