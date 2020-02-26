/*
 * CSVParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef CSVPARSER_H_
#define CSVPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <cudf/legacy/io_types.hpp>
#include <memory>
#include <vector>

#include <cudf/io/functions.hpp>

namespace ral {
namespace io {

namespace cudf_io = cudf::experimental::io;

class csv_parser : public data_parser {
public:
	csv_parser(cudf::experimental::io::read_csv_args new_csv_arg);

	virtual ~csv_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema);

private:
	cudf_io::read_csv_args csv_args{cudf_io::source_info("")};
};

} /* namespace io */
} /* namespace ral */

#endif /* CSVPARSER_H_ */
