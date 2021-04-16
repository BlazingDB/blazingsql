/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#ifndef MYSQLDATAPROVIDER_H_
#define MYSQLDATAPROVIDER_H_

#include "AbstractSQLDataProvider.h"

#include <jdbc/cppconn/connection.h>

namespace ral {
namespace io {

/**
 * can generate a series of randomaccessfiles from uris that are provided
 * when it goes out of scope it will close any files it opened
 * this last point is debatable in terms of if this is the desired functionality
 */
class mysql_data_provider : public abstractsql_data_provider {
public:
	mysql_data_provider(const sql_info &sql,
                      size_t total_number_of_nodes,
                      size_t self_node_idx);

  virtual ~mysql_data_provider();

	std::shared_ptr<data_provider> clone() override; 

  /**
	 * tells us if this provider can generate more sql resultsets
	 */
	bool has_next() override;

	/**
	 *  Resets file read count to 0 for file based DataProvider
	 */
	void reset() override;

	/**
	 * gets us the next arrow::io::RandomAccessFile
	 * if open_file is false will not run te query and just returns a data_handle
	 * with columns info
	 */
	data_handle get_next(bool open_file = true) override;

	/**
	 * Get the number of data_handles that will be provided.
	 */ 
	size_t get_num_handles() override;

protected:
  std::unique_ptr<ral::parser::node_transformer> get_predicate_transformer() const override;

private:
  std::unique_ptr<sql::Connection> mysql_connection;
  size_t estimated_table_row_count;
  size_t batch_index;
  bool table_fetch_completed;
};

} /* namespace io */
} /* namespace ral */

#endif /* MYSQLDATAPROVIDER_H_ */
