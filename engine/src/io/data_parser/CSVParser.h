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
#include <memory>
#include <vector>

#include <cudf/io/datasource.hpp>
#include <cudf/io/csv.hpp>

namespace ral {
namespace io {

class csv_parser : public data_parser {
public:
	csv_parser(cudf::io::csv_reader_options args);

	virtual ~csv_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema);

private:
	cudf::io::csv_reader_options csv_args;
};

} /* namespace io */
} /* namespace ral */

#endif /* CSVPARSER_H_ */
