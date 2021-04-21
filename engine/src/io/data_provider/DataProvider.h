/*
 * DataProvider.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DATAPROVIDER_H_
#define DATAPROVIDER_H_

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <arrow/io/interfaces.h>

#pragma GCC diagnostic ignored "-Wunknown-pragmas"
//#include <cudf/cudf.h>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/scalar/scalar_device_view.cuh>
#pragma GCC diagnostic pop

#include <map>
#include <memory>
#include <vector>

#include <blazingdb/io/FileSystem/Uri.h>
#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include <arrow/table.h>

#ifdef MYSQL_SUPPORT
#include "jdbc/cppconn/resultset.h"
#endif

#ifdef SQLITE_SUPPORT
#include <sqlite3.h>
#endif

#ifdef POSTGRESQL_SUPPORT
#include <libpq-fe.h>
#endif

namespace ral {
namespace io {

struct sql_datasource {
  std::string table;
  std::string query;
  std::vector<std::string> column_names;
  std::vector<std::string> column_types; // always uppercase
  std::vector<size_t> column_bytes;
  size_t row_count;
#ifdef MYSQL_SUPPORT
  std::shared_ptr<sql::ResultSet> mysql_resultset = nullptr;
#endif
#ifdef SQLITE_SUPPORT
  std::shared_ptr<sqlite3_stmt> sqlite_statement = nullptr;
#endif
#ifdef POSTGRESQL_SUPPORT
  std::shared_ptr<PGresult> postgresql_result = nullptr;
#endif
  // TODO percy c.gonzales add other backends here
};

struct data_handle {
	std::shared_ptr<arrow::io::RandomAccessFile> file_handle;
	std::map<std::string, std::string> column_values;  // allows us to add hive values
	Uri uri;										  // in case the data was loaded from a file
	frame::BlazingTableView table_view;
	std::shared_ptr<arrow::Table> arrow_table;
	sql_datasource sql_handle;
	data_handle(){}

	data_handle(
		std::shared_ptr<arrow::io::RandomAccessFile> file_handle,
		std::map<std::string, std::string> column_values,
		Uri uri,
		frame::BlazingTableView table_view)
	: file_handle(file_handle), column_values(column_values), uri(uri), table_view(table_view) { }

	data_handle(
		std::shared_ptr<arrow::io::RandomAccessFile> file_handle,
		std::map<std::string, std::string> column_values,
		Uri uri,
		std::shared_ptr<arrow::Table> arrow_table)
	: file_handle(file_handle), column_values(column_values), uri(uri), arrow_table(arrow_table) { }

	bool is_valid(){
		return file_handle != nullptr || !uri.isEmpty() ;
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
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files,
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 */
	virtual std::vector<data_handle> get_some(std::size_t num_files, bool open_file = true) = 0;

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	virtual void close_file_handles() = 0;

	/**
	 * Get the number of data_handles that will be provided.
	 */
	virtual size_t get_num_handles() = 0;
};

} /* namespace io */
} /* namespace ral */

#endif /* DATAPROVIDER_H_ */
