/*
 * CSVParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef CSVPARSER_H_
#define CSVPARSER_H_

#include "DataParser.h"
#include "GDFColumn.cuh"
#include "arrow/io/interfaces.h"
#include <cudf/legacy/io_types.hpp>
#include <memory>
#include <vector>

#include <cudf/io/functions.hpp>

namespace ral {
namespace io {

class csv_parser : public data_parser {
public:
	// DEPRECATED don't use this constructor
	csv_parser(cudf::csv_read_arg csv_arg);

	csv_parser(cudf::experimental::io::read_csv_args new_csv_arg);

	virtual ~csv_parser();

	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		std::vector<gdf_column_cpp> & columns_out,
		const Schema & schema,
		std::vector<size_t> column_indices_requested);

	std::unique_ptr<ral::frame::BlazingTable> parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema);

private:
	// DEPRECATED use csv_args
	cudf::csv_read_arg csv_arg{cudf::source_info{""}};
	cudf::experimental::io::read_csv_args csv_args{cudf::experimental::io::source_info("")};
};

} /* namespace io */
} /* namespace ral */

#endif /* CSVPARSER_H_ */
