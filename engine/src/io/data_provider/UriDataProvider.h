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
		std::vector<std::map<std::string, std::string>> uri_values);
	uri_data_provider(std::vector<Uri> uris);

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
	 */
	data_handle get_next();
	/**
	 * gets a randomaccessfile to the uri at file_uris[0]
	 */
	data_handle get_first();

	/**
	 * returns any errors that were encountered when opening arrow::io::RandomAccessFile
	 */
	std::vector<std::string> get_errors();
	/**
	 * returns a string that the user should be able to use to identify which file is being referred to in error
	 * messages
	 */
	std::string get_current_user_readable_file_handle();
	/**
	 * returns all of the file handles
	 */
	std::vector<data_handle> get_all();

	/**
	 * returns the current file index
	 */
	size_t get_file_index();

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
};

} /* namespace io */
} /* namespace ral */

#endif /* URIDATAPROVIDER_H_ */
