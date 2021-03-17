/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _MYSQLSQLPARSER_H_
#define _MYSQLSQLPARSER_H_

#include "io/data_parser/DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/datasource.hpp>

namespace ral {
namespace io {

class mysql_parser : public data_parser {
public:
	mysql_parser();
	virtual ~mysql_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata( 
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files,
		int offset);
  // TODO percy add sql data types to the enum
	DataType type() const override { return DataType::PARQUET; }
};

} /* namespace io */
} /* namespace ral */

#endif /* _MYSQLSQLPARSER_H_ */
