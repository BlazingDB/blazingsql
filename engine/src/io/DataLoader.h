/*
 * dataloader.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#pragma once

#include <FileSystem/Uri.h>

#include "GDFColumn.cuh"
#include "data_parser/DataParser.h"
#include "data_provider/DataProvider.h"
#include <arrow/io/interfaces.h>
#include <blazingdb/manager/Context.h>
#include <vector>

#include <memory>
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>
#include <nvstrings/ipc_transfer.h>

// TODO includes from BlazingTable
//#include <memory>
//#include <vector>
#include <string>
#include "cudf/table/table.hpp"
#include "cudf/table/table_view.hpp"
#include "cudf/column/column_view.hpp"

namespace ral {

// TODO all this namespace frame should be erased from here
namespace frame {
	typedef cudf::experimental::table CudfTable;
	typedef cudf::table_view CudfTableView;
	typedef cudf::column_view CudfColumnView;
class BlazingTable{
  	public:
  		BlazingTable(std::unique_ptr<CudfTable> table,std::vector<std::string> columnNames);
  		CudfTableView view() const;
  		std::vector<std::string> names() const;
  	private:
  		std::vector<std::string> columnNames;
  		std::unique_ptr<CudfTable> table;
};

class BlazingTableView{
  	public:
		BlazingTableView(CudfTableView table,std::vector<std::string> columnNames);
		CudfTableView view() const;
		std::vector<std::string> names() const;
  	private:
		std::vector<std::string> columnNames;
		CudfTableView table;
};

} // namespace frame
namespace io {

namespace {
using blazingdb::manager::Context;
}  // namespace

/**
 * class is used for loading data from some kind of file type using some kind of file provider
 * in our case we will be using blazing-io to read and write files but a local version could also be made
 */

class data_loader {
public:
	data_loader(std::shared_ptr<data_parser> parser, std::shared_ptr<data_provider> provider);
	data_loader() : provider(nullptr), parser(nullptr) {}
	virtual ~data_loader();

	/**
	 * loads data into a vector of gdf_column_cpp
	 * @param columns a vector to receive our output should be of size 0 when it is coming in and it will be allocated
	 * by this function
	 * @param include_column the different files we can read from can have more columns than we actual want to read,
	 * this lest us filter some of them out
	 */

	void load_data(const Context & context,
		std::vector<gdf_column_cpp> & columns,
		const std::vector<size_t> & column_indices,
		const Schema & schema);
	void get_schema(Schema & schema, std::vector<std::pair<std::string, gdf_dtype>> non_file_columns);

private:
	/**
	 * DataProviders are able to serve up one or more arrow::io::RandomAccessFile objects
	 */
	std::shared_ptr<data_provider> provider;
	/**
	 * parsers are able to parse arrow::io::RandomAccessFile objects of a specific file type and convert them into
	 * gdf_column_cpp
	 */
	std::shared_ptr<data_parser> parser;
};


} /* namespace io */
} /* namespace ral */
