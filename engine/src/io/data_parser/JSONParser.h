#pragma once

#include <arrow/io/interfaces.h>
#include <cudf/io/functions.hpp>
#include <memory>
#include <vector>

#include "DataParser.h"

namespace ral {
namespace io {

class json_parser : public data_parser {
public:
	json_parser(cudf::experimental::io::read_json_args args);

	virtual ~json_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

private:
	cudf::experimental::io::read_json_args args;
};

} /* namespace io */
} /* namespace ral */
