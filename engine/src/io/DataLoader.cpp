
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

namespace ral {
// TODO: namespace frame should be remove from here
namespace io {

namespace {
using blazingdb::manager::Context;
}  // namespace

data_loader::data_loader(std::shared_ptr<data_parser> _parser, std::shared_ptr<data_provider> _data_provider)
	: provider(_data_provider), parser(_parser) {}


data_loader::~data_loader() {}


void data_loader::load_data(const Context & context,
	std::vector<gdf_column_cpp> & columns,
	const std::vector<size_t> & column_indices,
	const Schema & schema) {
	static CodeTimer timer;
	timer.reset();

	std::vector<std::vector<gdf_column_cpp>> columns_per_file;  // stores all of the columns parsed from each file
	std::vector<std::string> user_readable_file_handles;
	std::vector<data_handle> files;

	// iterates through files and parses them into columns
	while(this->provider->has_next()) {
		// std::cout<<"pushing back files!"<<std::endl;
		// a file handle that we can use in case errors occur to tell the user which file had parsing issues
		user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
		files.push_back(this->provider->get_next());
	}
	// std::cout<<"pushed back"<<std::endl;

	columns_per_file.resize(files.size());
	// TODO NOTE percy c.gonzales rommel fix our concurrent reads here (better use of thread)
	// make sure cudf supports concurrent reads
	std::vector<std::thread> threads;

	for(int file_index = 0; file_index < files.size(); file_index++) {
		threads.push_back(std::thread([&, file_index]() {
			// std::cout<<"starting file thread"<<std::endl;
			std::vector<gdf_column_cpp> converted_data;
			// std::cout<<"converted data"<<std::endl;

			if(files[file_index].fileHandle != nullptr) {
				// std::cout<<"get num columns==>"<<schema.get_num_columns()<<std::endl;
				// std::cout<<"file is "<< user_readable_file_handles[file_index]<<" with uri
				// "<<files[file_index].uri.getPath().toString()<<std::endl;
				Schema fileSchema = schema.fileSchema(file_index);
				parser->parse(files[file_index].fileHandle,
					user_readable_file_handles[file_index],
					converted_data,
					fileSchema,
					column_indices);
				// std::cout<<"parsed file got "<<converted_data.size()<<" columns!"<<std::endl;
				for(int i = 0; i < schema.get_num_columns(); i++) {
					if(!schema.get_in_file()[i]) {
						// std::cout<<"creating column!"<<std::endl;
						auto num_rows = converted_data[0].get_gdf_column()->size();
						std::string name = schema.get_name(i);
						if(files[file_index].is_column_string[name]) {
							// TODO percy cudf0.12 port to cudf::column and custrings
//							std::string string_value = files[file_index].string_values[name];
//							NVCategory * category = repeated_string_category(string_value, num_rows);
//							gdf_column_cpp column;
//							column.create_gdf_column(category, num_rows, name);
//							converted_data.push_back(column);
						} else {

							// TODO percy cudf0.12 implement proper scalar support
							//std::unique_ptr<cudf::scalar> scalar = files[file_index].column_values[name];

							// TODO percy cudf0.12 implement proper scalar support
							//gdf_column_cpp column;
							//column.create_gdf_column(to_type_id(scalar.dtype),
							//	num_rows,
							//	nullptr,
							//	ral::traits::get_dtype_size_in_bytes(to_type_id(scalar.dtype)),
							//	name);
							//cudf::fill(column.get_gdf_column(), scalar, 0, num_rows);
							//converted_data.push_back(column);

						}
						// std::cout<<"created column!"<<std::endl;
					}
				}

				columns_per_file[file_index] = converted_data;
			} else {
				Library::Logging::Logger().logError(ral::utilities::buildLogString(
					"", "", "", "ERROR: Was unable to open " + user_readable_file_handles[file_index]));
			}
		}));
	}

	std::for_each(threads.begin(), threads.end(), [](std::thread & this_thread) { this_thread.join(); });
	// std::cout<<"finished loading!"<<std::endl;
	Library::Logging::Logger().logInfo(timer.logDuration(context, "data_loader::load_data part 1 parse"));
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

	size_t num_columns, num_files = columns_per_file.size();

	if(num_files > 0)
		num_columns = columns_per_file[0].size();

	if(num_files == 0 || num_columns == 0) {  // we got no data

		parser->parse(nullptr, "", columns, schema, column_indices);
		return;
	}
	// std::cout<<"reset provider num cols is "<<num_columns<<std::endl;
	// be replacing no longer needed gdf_column_cpp with this dummy column we can
	// make columns go out of scope while still preserving the size of the vector
	gdf_column_cpp dummy_column;


	if(num_files == 1) {  // we have only one file so we can just return the columns we parsed from that file
		columns = columns_per_file[0];

	} else {  // we have more than one file so we need to concatenate
			  // std::cout<<"concatting!"<<std::endl;
		columns = ral::utilities::concatTables(columns_per_file);
		// std::cout<<"concatted!"<<std::endl;
	}

	Library::Logging::Logger().logInfo(timer.logDuration(context, "data_loader::load_data part 2 concat"));
	timer.reset();
}

std::unique_ptr<ral::frame::BlazingTable> data_loader::load_data(const Context & context,
		const std::vector<size_t> & column_indices,
		const Schema & schema) 
{
	static CodeTimer timer;
	timer.reset();

// 	std::vector<std::vector<gdf_column_cpp>> columns_per_file;  // stores all of the columns parsed from each file
	std::vector<std::string> user_readable_file_handles;
	std::vector<data_handle> files;

	// iterates through files and parses them into columns
	while(this->provider->has_next()) {
		// std::cout<<"pushing back files!"<<std::endl;
		// a file handle that we can use in case errors occur to tell the user which file had parsing issues
		user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
		files.push_back(this->provider->get_next());
	}
	std::unique_ptr<ral::frame::BlazingTable> *columns_per_file = new std::unique_ptr<ral::frame::BlazingTable>[files.size()];

	// columns_per_file.resize(files.size());

	// TODO NOTE percy c.gonzales rommel fix our concurrent reads here (better use of thread)
	// make sure cudf supports concurrent reads
	std::vector<std::thread> threads;

	for(int file_index = 0; file_index < files.size(); file_index++) {
		threads.push_back(std::thread([&, file_index]() {
			// std::cout<<"starting file thread"<<std::endl;
			// std::cout<<"converted data"<<std::endl;

			if(files[file_index].fileHandle != nullptr) {
				// std::cout<<"get num columns==>"<<schema.get_num_columns()<<std::endl;
				// std::cout<<"file is "<< user_readable_file_handles[file_index]<<" with uri
				// "<<files[file_index].uri.getPath().toString()<<std::endl;
				Schema fileSchema = schema.fileSchema();
				std::unique_ptr<ral::frame::BlazingTable> converted_data = parser->parse(files[file_index].fileHandle,
					user_readable_file_handles[file_index],
					fileSchema,
					column_indices);
				// std::cout<<"parsed file got "<<converted_data.size()<<" columns!"<<std::endl;
				for(int i = 0; i < schema.get_num_columns(); i++) {
					if(!schema.get_in_file()[i]) {
						// std::cout<<"creating column!"<<std::endl;
						auto num_rows = converted_data->num_rows();
						std::string name = schema.get_name(i);
						if(files[file_index].is_column_string[name]) {
							// TODO percy cudf0.12 port to cudf::column and custrings
//							std::string string_value = files[file_index].string_values[name];
//							NVCategory * category = repeated_string_category(string_value, num_rows);
//							gdf_column_cpp column;
//							column.create_gdf_column(category, num_rows, name);
//							converted_data.push_back(column);
						} else {

							// TODO percy cudf0.12 implement proper scalar support
							//std::unique_ptr<cudf::scalar> scalar = files[file_index].column_values[name];

							// TODO percy cudf0.12 implement proper scalar support
							//gdf_column_cpp column;
							//column.create_gdf_column(to_type_id(scalar.dtype),
							//	num_rows,
							//	nullptr,
							//	ral::traits::get_dtype_size_in_bytes(to_type_id(scalar.dtype)),
							//	name);
							//cudf::fill(column.get_gdf_column(), scalar, 0, num_rows);
							//converted_data.push_back(column);

						}
						// std::cout<<"created column!"<<std::endl;
					}
				}
				// TODO: tricky!!! 
				columns_per_file[file_index] = std::move(converted_data);
			} else {
				Library::Logging::Logger().logError(ral::utilities::buildLogString(
					"", "", "", "ERROR: Was unable to open " + user_readable_file_handles[file_index]));
			}
		}));
	}

	std::for_each(threads.begin(), threads.end(), [](std::thread & this_thread) { this_thread.join(); });
	// std::cout<<"finished loading!"<<std::endl;
	Library::Logging::Logger().logInfo(timer.logDuration(context, "data_loader::load_data part 1 parse"));
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
	size_t num_files = files.size();

	if(num_files > 0)
		num_columns = columns_per_file[0]->num_columns();

	if(num_files == 0 || num_columns == 0) {  // we got no data

		// TODO: empty table!
		// parser->parse(nullptr, "", columns, schema, column_indices);
		return nullptr;
	}

	// std::cout<<"reset provider num cols is "<<num_columns<<std::endl;
	// be replacing no longer needed gdf_column_cpp with this dummy column we can
	// make columns go out of scope while still preserving the size of the vector


	Library::Logging::Logger().logInfo(timer.logDuration(context, "data_loader::load_data part 2 concat"));
	timer.reset();

	if(num_files == 1) {  // we have only one file so we can just return the columns we parsed from that file
		return std::move(columns_per_file[0]); // ->clone()

	} else {  // we have more than one file so we need to concatenate
			  // std::cout<<"concatting!"<<std::endl;
		
		// columns = ral::utilities::concatTables(columns_per_file);
		// std::cout<<"concatted!"<<std::endl;
	}
	return nullptr;
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

void data_loader::get_metadata(Metadata & metadata, std::vector<std::pair<std::string, gdf_dtype>> non_file_columns) {
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
	// std::vector<std::string> user_readable_file_handles;

	bool firstIteration = true;
	std::vector<data_handle> handles = this->provider->get_all();
	for(auto handle : handles) {
		files.push_back(handle.fileHandle);
		// user_readable_file_handles.push_back(handle.uri.toString());
	}
	if (this->parser->get_metadata(files,  metadata) == false) {
		throw std::runtime_error("No metadata for this data file");
	}
	//TODO, non_file_columns hive feature, @percy
	// ... 

}

} /* namespace io */
} /* namespace ral */
