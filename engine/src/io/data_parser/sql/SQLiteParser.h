/*
 * Copyright 2021 BlazingDB, Inc.
 *     Copyright 2021 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _SQLITEPARSER_H_
#define _SQLITEPARSER_H_

#include "io/data_parser/DataParser.h"

namespace ral {
namespace io {

class sqlite_parser : public data_parser {
public:
	sqlite_parser();
	virtual ~sqlite_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups) override;

	void parse_schema(ral::io::data_handle handle, Schema & schema) override;

	std::unique_ptr<ral::frame::BlazingTable> get_metadata(
		std::vector<ral::io::data_handle> handles,
		int offset) override;

	DataType type() const override { return DataType::PARQUET; }
};

} /* namespace io */
} /* namespace ral */

#endif /* _SQLITEPARSER_H_ */
