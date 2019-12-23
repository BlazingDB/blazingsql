
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
#include <arrow/status.h>

namespace ral {
namespace io {

namespace {
using blazingdb::manager::Context;

class RandomAccessFileForSchema : public arrow::io::RandomAccessFile {
public:
	explicit RandomAccessFileForSchema(std::shared_ptr<arrow::io::RandomAccessFile> & file) : file_{file} {}

	arrow::Status Close() final {
		// NOTE: Do nothing to avoid close the file after read schema.
    // This class only acts for the schema creation process.
		return arrow::Status::OK();
	}

  arrow::Status Tell(std::int64_t * position) const final { return file_->Tell(position); }

  bool closed() const final { return file_->closed(); }

  arrow::Status Seek(std::int64_t position) final { return file_->Seek(position); }

  arrow::Status Read(std::int64_t nbytes, std::int64_t * bytes_read, void * out) final {
    return file_->Read(nbytes, bytes_read, out);
  }

  arrow::Status Read(std::int64_t nbytes, std::shared_ptr<arrow::Buffer> * out) final {
    return file_->Read(nbytes, out);
  }

  arrow::Status GetSize(std::int64_t * size) final { return file_->GetSize(size); }

private:
	std::shared_ptr<arrow::io::RandomAccessFile> file_;
};
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
	std::vector<data_handle> * files;

	std::vector<data_handle> * data_handles = provider->data_handles();

	// TODO: Now, we are using the environment variable only for testing purposes.
	const char * fsCacheRaw = std::getenv("BLAZINGSQL_FS_CACHE");
	bool fsCacheEnabled = true;
	if(nullptr != fsCacheRaw) {
		const std::string fsCacheValue = fsCacheRaw;
		if("OFF" == fsCacheValue) {
			fsCacheEnabled = false;
		} else {
			if("ON" != fsCacheValue) {
				throw std::runtime_error("Invalid value for BLAZINGSQL_FS_CACHE (ON or OFF): " + fsCacheValue);
			}
		}

		if(nullptr != data_handles && !fsCacheEnabled) {
			std::cout << "no effect for fs cache" << std::endl;
		}
	}

	std::vector<data_handle> nfiles;
	if(nullptr != data_handles && fsCacheEnabled) {
		files = data_handles;
		while(this->provider->has_next()) {
			user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
			provider->step();
		}
	} else {
		// iterates through files and parses them into columns
		while(this->provider->has_next()) {
			// std::cout<<"pushing back files!"<<std::endl;
			// a file handle that we can use in case errors occur to tell the user which file had parsing issues
			user_readable_file_handles.push_back(this->provider->get_current_user_readable_file_handle());
			nfiles.push_back(this->provider->get_next());
		}
		// std::cout<<"pushed back"<<std::endl;
		files = &nfiles;
	}

	columns_per_file.resize(files->size());
	// TODO NOTE percy c.gonzales rommel fix our concurrent reads here (better use of thread)
	// make sure cudf supports concurrent reads
	std::vector<std::thread> threads;

	for(int file_index = 0; file_index < files->size(); file_index++) {
		threads.push_back(std::thread([&, file_index]() {
			// std::cout<<"starting file thread"<<std::endl;
			ral::config::GPUManager::getInstance().setDevice();
			std::vector<gdf_column_cpp> converted_data;
			// std::cout<<"converted data"<<std::endl;

			if((*files)[file_index].fileHandle != nullptr) {
				// std::cout<<"get num columns==>"<<schema.get_num_columns()<<std::endl;
				// std::cout<<"file is "<< user_readable_file_handles[file_index]<<" with uri
				// "<<files[file_index].uri.getPath().toString()<<std::endl;
				Schema fileSchema = schema.fileSchema();
				(*files)[file_index].fileHandle->Seek(0);
				std::shared_ptr<arrow::io::RandomAccessFile> fileForParse =
					std::make_shared<RandomAccessFileForSchema>((*files)[file_index].fileHandle);
				parser->parse(fileForParse,
					user_readable_file_handles[file_index],
					converted_data,
					fileSchema,
					column_indices);
				// std::cout<<"parsed file got "<<converted_data.size()<<" columns!"<<std::endl;
				for(int i = 0; i < schema.get_num_columns(); i++) {
					if(!schema.get_in_file()[i]) {
						// std::cout<<"creating column!"<<std::endl;
						auto num_rows = converted_data[0].size();
						std::string name = schema.get_name(i);
						if((*files)[file_index].is_column_string[name]) {
							std::string string_value = (*files)[file_index].string_values[name];
							NVCategory * category = repeated_string_category(string_value, num_rows);
							gdf_column_cpp column;
							column.create_gdf_column(category, num_rows, name);
							converted_data.push_back(column);
						} else {
							gdf_scalar scalar = (*files)[file_index].column_values[name];

							gdf_column_cpp column;
							column.create_gdf_column(scalar.dtype,
								gdf_dtype_extra_info{TIME_UNIT_ms},
								num_rows,
								nullptr,
								ral::traits::get_dtype_size_in_bytes(scalar.dtype),
								name);
							cudf::fill(column.get_gdf_column(), scalar, 0, num_rows);
							converted_data.push_back(column);
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

std::vector<data_handle> * data_loader::get_schema(
	Schema & schema, std::vector<std::pair<std::string, gdf_dtype>> non_file_columns) {
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
	std::vector<data_handle> handles = this->provider->get_all();

	for(auto handle : handles) {
		files.push_back(handle.fileHandle);
	}

	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> filesForSchema;
	filesForSchema.reserve(files.size());
	std::transform(files.begin(),
		files.end(),
		std::back_inserter(filesForSchema),
		[](std::shared_ptr<arrow::io::RandomAccessFile> & file) {
			return std::make_shared<RandomAccessFileForSchema>(file);
		});

	this->parser->parse_schema(filesForSchema, schema);

	std::for_each(
		files.begin(), files.end(), [](std::shared_ptr<arrow::io::RandomAccessFile> & file) { file->Seek(0); });

	for(auto handle : handles) {
		schema.add_file(handle.uri.toString(true));
	}

	for(auto extra_column : non_file_columns) {
		schema.add_column(extra_column.first, extra_column.second, 0, false);
	}

	const char * fsCacheRaw = std::getenv("BLAZINGSQL_FS_CACHE");
	bool fsCacheEnabled = true;
	if(nullptr != fsCacheRaw) {
		const std::string fsCacheValue = fsCacheRaw;
		if("OFF" == fsCacheValue) {
			fsCacheEnabled = false;
		} else {
			if("ON" != fsCacheValue) {
				throw std::runtime_error("Invalid value for BLAZINGSQL_FS_CACHE (ON or OFF): " + fsCacheValue);
			}
		}
	}

	// This pointers are released in the end of runquery (see engine.cpp)
	std::vector<data_handle> * data_handles = nullptr;
	if(fsCacheEnabled) {
		data_handles = new std::vector<data_handle>;
		*data_handles = std::move(handles);
		data_handles_bucket.push_back(data_handles);
	}

	return data_handles;
}

std::vector<std::vector<data_handle> *> data_loader::data_handles_bucket;

} /* namespace io */
} /* namespace ral */
