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
#include "ExceptionHandling/BlazingException.h"
#include "arrow/status.h"
#include <blazingdb/io/Util/StringUtil.h>
#include <iostream>

namespace ral {
namespace io {

uri_data_provider::uri_data_provider(std::vector<Uri> uris)
	: data_provider(), file_uris(uris), uri_values({}), opened_files({}),
	  current_file(0), errors({}), directory_uris({}), directory_current_file(0) {}

// TODO percy cudf0.12 implement proper scalar support, TODO, finish this to support HIVE with partitions, @alex 
uri_data_provider::uri_data_provider(std::vector<Uri> uris,
	std::vector<std::map<std::string, std::string>> uri_values)
	: data_provider(), file_uris(uris), uri_values(uri_values), 
	opened_files({}), current_file(0), errors({}), directory_uris({}),
	  directory_current_file(0) {
	// thanks to c++11 we no longer have anything interesting to do here :)
}

std::shared_ptr<data_provider> uri_data_provider::clone() {
	return std::make_shared<uri_data_provider>(this->file_uris, this->uri_values);
}

uri_data_provider::~uri_data_provider() {
	// TODO: when a shared_ptr to a randomaccessfile goes out of scope does it close files automatically?
	// in case it doesnt we can close that here
	for(size_t file_index = 0; file_index < this->opened_files.size(); file_index++) {
		// TODO: perhaps consider capturing status here and complainig if it fails
		this->opened_files[file_index]->Close();
		//
	}
}

std::string uri_data_provider::get_current_user_readable_file_handle() {
	if(directory_uris.size() == 0) {
		return this->file_uris[this->current_file].toString();
	} else {
		return this->directory_uris[this->directory_current_file].toString();
	}
}

bool uri_data_provider::has_next() { return this->current_file < this->file_uris.size(); }

void uri_data_provider::reset() {
	this->current_file = 0;
	this->directory_current_file = 0;
}

std::vector<data_handle> uri_data_provider::get_all() {
	std::vector<data_handle> file_handles;
	while(this->has_next()) {
		file_handles.push_back(this->get_next());
	}

	return file_handles;
}

data_handle uri_data_provider::get_next() {
	// TODO: Take a look at this later, just calling this function to ensure
	// the uri is in a valid state otherwise throw an exception
	// because openReadable doens't  validate it and just return a nullptr

	if(this->directory_uris.size() > 0 && this->directory_current_file < this->directory_uris.size()) {
		auto fileStatus = BlazingContext::getInstance()->getFileSystemManager()->getFileStatus(
			this->directory_uris[this->directory_current_file]);

		std::shared_ptr<arrow::io::RandomAccessFile> file =
			BlazingContext::getInstance()->getFileSystemManager()->openReadable(
				this->directory_uris[this->directory_current_file]);

		data_handle handle;
		handle.uri = this->directory_uris[this->directory_current_file];
		if(this->uri_values.size() != 0) {
			if(this->uri_values.size() > this->current_file) {
				handle.column_values = this->uri_values[this->current_file];
			} else {
				std::cout<<"ERROR: Out of range error. this->uri_values.size() is "<<this->uri_values.size()<<" this->current_file is "<<this->current_file<<std::endl;
			}
		}

		this->opened_files.push_back(file);

		this->directory_current_file++;
		if(this->directory_current_file >= directory_uris.size()) {
			this->directory_uris = {};
			this->current_file++;
		}

		handle.fileHandle = file;
		return handle;
	} else {
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
			} else {
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
			return get_next();

		} else if(fileStatus.isFile()) {
			std::shared_ptr<arrow::io::RandomAccessFile> file =
				BlazingContext::getInstance()->getFileSystemManager()->openReadable(current_uri);

			this->opened_files.push_back(file);
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
			return get_next();
		}
	}
}

data_handle uri_data_provider::get_first() {
	this->reset();
	data_handle handle = this->get_next();
	this->reset();
	this->directory_uris = {};
	return handle;
}

std::vector<std::string> uri_data_provider::get_errors() { return this->errors; }

} /* namespace io */
} /* namespace ral */
