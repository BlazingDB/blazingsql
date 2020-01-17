/*
 * jsonParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef jsonPARSER_H_
#define jsonPARSER_H_

#include "DataParser.h"
#include "GDFColumn.cuh"
#include "arrow/io/interfaces.h"
#include "cudf/legacy/io_types.hpp"
#include <memory>
#include <vector>

#include <cudf/io/functions.hpp>

namespace ral {
namespace io {

class json_parser : public data_parser {
public:
	json_parser(cudf::json_read_arg args);

	virtual ~json_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, Schema & schema);

private:
	cudf::json_read_arg args;
};
} /* namespace io */
} /* namespace ral */

#endif /* jsonPARSER_H_ */
