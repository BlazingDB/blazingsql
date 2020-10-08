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
	json_parser(std::map<std::string, std::string> args_map);

	virtual ~json_parser();

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

private:
	std::map<std::string, std::string> args_map;
};

} /* namespace io */
} /* namespace ral */
