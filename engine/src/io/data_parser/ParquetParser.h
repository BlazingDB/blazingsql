/*
 * ParquetParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef PARQUETPARSER_H_
#define PARQUETPARSER_H_

#include "../data_provider/DataProvider.h"
#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/datasource.hpp>

namespace ral {
namespace io {

class parquet_parser : public data_parser {
public:
	parquet_parser();
	virtual ~parquet_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, int offset);

	DataType type() const override { return DataType::PARQUET; }
};

} /* namespace io */
} /* namespace ral */

#endif /* PARQUETPARSER_H_ */
