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
 * can generate a series of data_handles from gdfs that are provided
 * when it goes out of scope it will close release any gdfs
 */
class gdf_data_provider : public data_provider {
public:

	gdf_data_provider(std::vector<ral::frame::BlazingTableView> table_views);

	std::shared_ptr<data_provider> clone() override; 

	virtual ~gdf_data_provider();
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
	 * returns an empty vector. Used for compatiblity with apis that open files.
	 */
	std::vector<std::string> get_errors();
	
	/**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
	 */
	std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true);

	/**
	 * Does nothing. Used for compatiblity with apis that open files.
	 */
	void close_file_handles();


private:
	
	std::vector<ral::frame::BlazingTableView> table_views;
	/**
	 * stores an index to the current file being used
	 */
	size_t current_file;
	
};

} /* namespace io */
} /* namespace ral */

#endif /* URIDATAPROVIDER_H_ */
