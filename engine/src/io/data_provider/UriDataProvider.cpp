/*
 * uridataprovider.cpp
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 *
 * 2019 Percy Camilo Triveño Aucahuasi <percy@blazingsql.com>
 */

#include "UriDataProvider.h"
#include "Config/BlazingContext.h"
#include "arrow/status.h"
#include <blazingdb/io/Util/StringUtil.h>

namespace ral {
namespace io {

uri_data_provider::uri_data_provider(std::vector<Uri> uris, bool ignore_missing_paths)
	: data_provider(), file_uris(uris), uri_values({}), opened_files({}),
	  current_file(0), errors({}), directory_uris({}), directory_current_file(0), ignore_missing_paths(ignore_missing_paths) {}

uri_data_provider::uri_data_provider(std::vector<Uri> uris,
	std::vector<std::map<std::string, std::string>> uri_values,
	bool ignore_missing_paths)
	: data_provider(), file_uris(uris), uri_values(uri_values), 
	opened_files({}), current_file(0), errors({}), directory_uris({}),
	  directory_current_file(0), ignore_missing_paths(ignore_missing_paths) {
	// thanks to c++11 we no longer have anything interesting to do here :)
}

std::shared_ptr<data_provider> uri_data_provider::clone() {
	return std::make_shared<uri_data_provider>(this->file_uris, this->uri_values);
}

uri_data_provider::~uri_data_provider() {
	// TODO: when a shared_ptr to a randomaccessfile goes out of scope does it close files automatically?
	// in case it doesnt we can close that here
	this->close_file_handles(); 
}

bool uri_data_provider::has_next() { return this->current_file < this->file_uris.size(); }

void uri_data_provider::reset() {
	this->current_file = 0;
	this->directory_current_file = 0;
}

/**
 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
 */
std::vector<data_handle> uri_data_provider::get_some(std::size_t num_files, bool open_file){
	std::size_t count = 0;
	std::vector<data_handle> file_handles;
	while(this->has_next() && count < num_files) {
		auto handle = this->get_next(open_file);
		if (handle.is_valid())
			file_handles.emplace_back(std::move(handle));
		count++;
	}
	return file_handles;
}


data_handle uri_data_provider::get_next(bool open_file) {
	// TODO: Take a look at this later, just calling this function to ensure
	// the uri is in a valid state otherwise throw an exception
	// because openReadable doens't  validate it and just return a nullptr

	if(this->directory_uris.size() > 0 && this->directory_current_file < this->directory_uris.size()) {
		std::shared_ptr<arrow::io::RandomAccessFile> file = open_file ? 
			BlazingContext::getInstance()->getFileSystemManager()->openReadable(
				this->directory_uris[this->directory_current_file]) : nullptr;

		data_handle handle;
		handle.uri = this->directory_uris[this->directory_current_file];
		if(this->uri_values.size() != 0) {
			if(this->uri_values.size() > this->current_file) {
				handle.column_values = this->uri_values[this->current_file];
			} else {
				std::cout<<"ERROR: Out of range error. this->uri_values.size() is "<<this->uri_values.size()<<" this->current_file is "<<this->current_file<<std::endl;
			}
		}

		if (open_file){
			this->opened_files.push_back(file);
		}

		this->directory_current_file++;
		if(this->directory_current_file >= directory_uris.size()) {
			this->directory_uris = {};
			this->current_file++;
		}

		handle.fileHandle = file;
		return handle;
	} else if (this->current_file < this->file_uris.size()) {
		FileStatus fileStatus;
		auto current_uri = this->file_uris[this->current_file];
		const bool hasWildcard = current_uri.getPath().hasWildcard();
		Uri target_uri = current_uri;

		try {
			auto fs_manager = BlazingContext::getInstance()->getFileSystemManager();

			if(hasWildcard) {
				const Path final_path = current_uri.getPath().getParentPath();
				target_uri = Uri(current_uri.getScheme(), current_uri.getAuthority(), final_path);
			}

			if(fs_manager && fs_manager->exists(target_uri)) {
				fileStatus = BlazingContext::getInstance()->getFileSystemManager()->getFileStatus(target_uri);
			} else if (!ignore_missing_paths){
				throw std::runtime_error(
					"Path '" + target_uri.toString() +
					"' does not exist. File or directory paths are expected to be in one of the following formats: " +
					"For local file paths: '/folder0/folder1/fileName.extension'    " +
					"For local file paths with wildcard: '/folder0/folder1/*fileName*.*'    " +
					"For local directory paths: '/folder0/folder1/'    " +
					"For s3 file paths: 's3://registeredFileSystemName/folder0/folder1/fileName.extension'    " +
					"For s3 file paths with wildcard: '/folder0/folder1/*fileName*.*'    " +
					"For s3 directory paths: 's3://registeredFileSystemName/folder0/folder1/'    " +
					"For gs file paths: 'gs://registeredFileSystemName/folder0/folder1/fileName.extension'    " +
					"For gs file paths with wildcard: '/folder0/folder1/*fileName*.*'    " +
					"For gs directory paths: 'gs://registeredFileSystemName/folder0/folder1/'    " +
					"For HDFS file paths: 'hdfs://registeredFileSystemName/folder0/folder1/fileName.extension'    " +
					"For HDFS file paths with wildcard: '/folder0/folder1/*fileName*.*'    " +
					"For HDFS directory paths: 'hdfs://registeredFileSystemName/folder0/folder1/'");
			} 
		} catch(const std::exception & e) {
			std::cerr << e.what() << std::endl;
			throw;
		} catch(...) {
			throw;
		}

		if(fileStatus.isDirectory()) {
			if(hasWildcard) {
				const std::string wildcard = current_uri.getPath().getResourceName();

				this->directory_uris =
					BlazingContext::getInstance()->getFileSystemManager()->list(target_uri, wildcard);

			} else {
				this->directory_uris = BlazingContext::getInstance()->getFileSystemManager()->list(target_uri);
			}

			std::string ender = ".crc";
			std::vector<Uri> new_uris;
			for(int i = 0; i < this->directory_uris.size(); i++) {
				std::string fileName = this->directory_uris[i].getPath().toString();

				if(!StringUtil::endsWith(fileName, ender)) { 
					new_uris.push_back(this->directory_uris[i]);
				}
			}
			this->directory_uris = new_uris;

			this->directory_current_file = 0;
			return get_next(open_file);

		} else if(fileStatus.isFile()) {
			std::shared_ptr<arrow::io::RandomAccessFile> file = open_file ? 
				BlazingContext::getInstance()->getFileSystemManager()->openReadable(current_uri) : nullptr;

			if (open_file){
				this->opened_files.push_back(file);
			}
			data_handle handle;
			handle.uri = current_uri;
			handle.fileHandle = file;
			if(this->uri_values.size() != 0) {
				if(this->uri_values.size() > this->current_file) {
					handle.column_values = this->uri_values[this->current_file];
				} else {
					std::cout<<"ERROR: Out of range error. this->uri_values.size() is "<<this->uri_values.size()<<" this->current_file is "<<this->current_file<<std::endl;
				}
			}

			this->current_file++;
			return handle;
		} else {
			// this is a file we cannot parse apparently
			this->current_file++;
			return get_next(open_file);
		}
	} else {
		data_handle empty_handle;
		return empty_handle;
	}
}

/**
 * Closes currently open set of file handles maintained by the provider
*/
void uri_data_provider::close_file_handles() {
	for(size_t file_index = 0; file_index < this->opened_files.size(); file_index++) {
		// TODO: perhaps consider capturing status here and complainig if it fails
		this->opened_files[file_index]->Close();
		//
	}
	this->opened_files.resize(0);
}

std::vector<std::string> uri_data_provider::get_errors() { return this->errors; }

} /* namespace io */
} /* namespace ral */
