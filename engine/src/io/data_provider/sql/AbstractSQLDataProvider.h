/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef ABSTRACTSQLDATAPROVIDER_H_
#define ABSTRACTSQLDATAPROVIDER_H_

#include "io/data_provider/DataProvider.h"

namespace ral {
namespace io {

struct sql_info {
  std::string host;
  size_t port;
  std::string user;
  std::string password;
  std::string schema; // aka database name
  std::string table;
  std::string table_filter;
  size_t table_batch_size;
};

/**
 * serves as base abstract class for all sql providers
 * can generate a series of resultsets from databases that are provided
 * when it goes out of scope it will close any resultset it opened
 * this last point is debatable in terms of if this is the desired functionality
 */
class abstractsql_data_provider : public data_provider {
public:
  abstractsql_data_provider(const sql_info &sql);

  virtual ~abstractsql_data_provider();

  /**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
	 */
	std::vector<data_handle> get_some(std::size_t batch_count, bool = true) override;

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	void close_file_handles() override;

  bool is_sql() const override { return true; }

  /**
	 * Set the column indices (aka projections) that will use to select the table
	 */
  void set_column_indices(std::vector<int> column_indices) { this->column_indices = column_indices; }

protected:
  // returns SELECT ... FROM
  std::string build_select_from() const;

  // returns LIMIT ... OFFSET
  std::string build_limit_offset(size_t offset) const;

protected:
  sql_info sql;
  std::pair<std::string, std::string> query_parts;
  std::vector<int> column_indices;
  std::vector<std::string> column_names;
  std::vector<std::string> column_types;
  std::vector<size_t> column_bytes;
};

} /* namespace io */
} /* namespace ral */

#endif /* ABSTRACTSQLDATAPROVIDER_H_ */
