/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef ABSTRACTSQLDATAPROVIDER_H_
#define ABSTRACTSQLDATAPROVIDER_H_

#include "io/data_provider/DataProvider.h"

namespace ral {
namespace io {

// TODO percy this connection logic belong to the parsers
struct sql_connection {
  std::string host;
  size_t port;
  std::string user;
  std::string password;
  std::string schema; // aka database name
};

/**
 * can generate a series of randomaccessfiles from uris that are provided
 * when it goes out of scope it will close any files it opened
 * this last point is debatable in terms of if this is the desired functionality
 */
class abstractsql_data_provider : public data_provider {
public:
  static const size_t DETAULT_BATCH_SIZE_HINT = 100;

  // in case the table is not partitioned we'll use the batch_size_hint to fetch data from the table
  abstractsql_data_provider(const sql_connection &sql_conn,
                            const std::string &table,
                            size_t batch_size_hint = abstractsql_data_provider::DETAULT_BATCH_SIZE_HINT);

  virtual ~abstractsql_data_provider();

  /**
	 *  Resets batch position to 0
	 */
	void reset();

  /**
	 * Tries to get up to num_files data_handles. We use this instead of a get_all() because if there are too many files, 
	 * trying to get too many file handles will cause a crash. Using get_some() forces breaking up the process of getting file_handles.
	 * open_file = true will actually open the file and return a std::shared_ptr<arrow::io::RandomAccessFile>. If its false it will return a nullptr
	 */
	std::vector<data_handle> get_some(std::size_t batch_count, bool open_file = true);

	/**
	 * Closes currently open set of file handles maintained by the provider
	 */
	void close_file_handles();

protected:
  // TODO percy docs

  /**
	 * stores the limit ... that were opened by the provider to be closed when it goes out of scope
	 */
  sql_connection sql_conn;

  /**
	 * stores the limit ... that were opened by the provider to be closed when it goes out of scope
	 */
  std::string table;

  /**
	 * stores the limit ... that were opened by the provider to be closed when it goes out of scope
	 */
	size_t batch_size_hint;

  /**
	 * stores the limit ... that were opened by the provider to be closed when it goes out of scope
	 */
  size_t batch_position;
};

} /* namespace io */
} /* namespace ral */

#endif /* ABSTRACTSQLDATAPROVIDER_H_ */
