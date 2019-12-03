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

namespace ral {
namespace io {

class csv_parser : public data_parser {
public:
	csv_parser(cudf::csv_read_arg csv_arg);

	virtual ~csv_parser();

	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		std::vector<gdf_column_cpp> & columns_out,
		const Schema & schema,
		std::vector<size_t> column_indices_requested);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema);

private:
	cudf::csv_read_arg csv_arg{cudf::source_info{""}};
};

} /* namespace io */
} /* namespace ral */

#endif /* CSVPARSER_H_ */
