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

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups) override;

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

	DataType type() const override { return DataType::JSON; }

private:
	std::map<std::string, std::string> args_map;
};

} /* namespace io */
} /* namespace ral */
