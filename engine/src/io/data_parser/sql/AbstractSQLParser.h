/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _ABSTRACTSQLPARSER_H_
#define _ABSTRACTSQLPARSER_H_

#include "io/data_parser/DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class abstractsql_parser : public data_parser {
public:
	abstractsql_parser(DataType sql_datatype);
	virtual ~abstractsql_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata( 
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files,
		int offset);

	DataType type() const override { return this->sql_datatype; }

private:
  DataType sql_datatype;
};

} /* namespace io */
} /* namespace ral */

#endif /* _ABSTRACTSQLPARSER_H_ */
