/*
 * uridataprovider.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef URIDATAPROVIDER_H_
#define URIDATAPROVIDER_H_

#include "DataProvider.h"
#include <arrow/io/interfaces.h>
#include <blazingdb/io/FileSystem/Uri.h>
#include <vector>

#include <memory>


namespace ral {
namespace io {
/**
 * can generate a series of randomaccessfiles from uris that are provided
 * when it goes out of scope it will close any files it opened
 * this last point is debatable in terms of if this is the desired functionality
 */
class uri_data_provider : public data_provider {
public:
	uri_data_provider(std::vector<Uri> uris,
		std::vector<std::map<std::string, std::string>> uri_values,
		bool ignore_missing_paths = false);
	uri_data_provider(std::vector<Uri> uris, 
		bool ignore_missing_paths = false);

	std::shared_ptr<data_provider> clone() override; 

	virtual ~uri_data_provider();
	/**
	 * tells us if there are more files in the list of uris to be provided
	 */
	bool has_next();
	/**
	 *  Resets current_file to 0
	 */
	void reset();
	/**
	 * gets a randomaccessfile to the uri at file_uris[current_file] and advances current_file by 1
	 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
	 */
	data_handle get_next(bool open_file = true);
	
	/**
	 * returns any errors that were encountered when opening arrow::io::RandomAccessFile
	 */
	std::vector<std::string> get_errors();
	
	/**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
	 */
	std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true);

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	void close_file_handles();


private:
	/**
	 * stores the list of uris that will be used by the provider
	 */
	std::vector<Uri> file_uris;
	/**
	 * stores an index to the current file being used
	 */
	size_t current_file;
	/**
	 * stores the files that were opened by the provider to be closed when it goes out of scope
	 */
	std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> opened_files;
	// TODO: we should really be either handling exceptions up the call stack or
	// storing something more elegant than just a string with an error message
	/**
	 * stores any errors that occured while trying to open these uris
	 */
	std::vector<std::string> errors;

	/**
	 * Stores strings which represent scalar values which map to a file or directory, for example in hive
	 * partitioned tables where the files themselves wont have the partition columns
	 * this gives us a way of passing those in
	 */
	std::vector<std::map<std::string,  std::string>> uri_values;
	std::vector<Uri> directory_uris;
	size_t directory_current_file;
	bool ignore_missing_paths;
};

} /* namespace io */
} /* namespace ral */

#endif /* URIDATAPROVIDER_H_ */
