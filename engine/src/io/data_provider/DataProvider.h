/*
 * DataProvider.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DATAPROVIDER_H_
#define DATAPROVIDER_H_

#include <arrow/io/interfaces.h>
#include <cudf/cudf.h>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/scalar/scalar_device_view.cuh>

#include <map>
#include <memory>
#include <vector>

#include <blazingdb/io/FileSystem/Uri.h>

namespace ral {
namespace io {

struct data_handle {
	std::shared_ptr<arrow::io::RandomAccessFile> fileHandle;
	std::map<std::string, std::string> column_values;  // allows us to add hive values
	Uri uri;										  // in case the data was loaded from a file

	bool is_valid(){
		return fileHandle != nullptr || !uri.isEmpty();
	}
};

/**
 * A class we can use which will be the base for all of our data providers
 */
class data_provider {
public:

	virtual std::shared_ptr<data_provider> clone() = 0; 

	/**
	 * tells us if this provider can generate more arrow::io::RandomAccessFile instances
	 */
	virtual bool has_next() = 0;

	/**
	 *  Resets file read count to 0 for file based DataProvider
	 */
	virtual void reset() = 0;

	/**
	 * gets us the next arrow::io::RandomAccessFile
	 */
	virtual data_handle get_next(bool open_file = true) = 0;
	/**
	 * gets any errors that occured while opening the files
	 */
	virtual std::vector<std::string> get_errors() = 0;

	/**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 */
	virtual std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true) = 0;

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	virtual void close_file_handles() = 0;

private:
};

} /* namespace io */
} /* namespace ral */

#endif /* DATAPROVIDER_H_ */
