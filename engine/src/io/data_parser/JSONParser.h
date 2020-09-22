#pragma once

#include <arrow/io/interfaces.h>
#include <cudf/io/datasource.hpp>
#include <cudf/io/json.hpp>
#include <memory>
#include <vector>

#include "DataParser.h"

namespace ral {
namespace io {

class json_parser : public data_parser {
public:
	json_parser(cudf::io::json_reader_options args);

	virtual ~json_parser();

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

private:
	cudf::io::json_reader_options args;
};

} /* namespace io */
} /* namespace ral */
