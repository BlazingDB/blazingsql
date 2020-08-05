#pragma once

#include <arrow/io/interfaces.h>
#include <cudf/io/functions.hpp>
#include <cudf/io/datasource.hpp>
#include <memory>
#include <vector>

#include "DataParser.h"

namespace ral {
namespace io {

class json_parser : public data_parser {
public:
	json_parser(cudf::io::read_json_args args);

	virtual ~json_parser();

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

private:
	cudf::io::read_json_args args;
};

} /* namespace io */
} /* namespace ral */
