/*
 * ParquetParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef PARQUETPARSER_H_
#define PARQUETPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class parquet_parser : public data_parser {
public:
	parquet_parser();
	virtual ~parquet_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, int offset);

};

} /* namespace io */
} /* namespace ral */

#endif /* PARQUETPARSER_H_ */
