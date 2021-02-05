#ifndef ORCPARSER_H_
#define ORCPARSER_H_

#include "DataParser.h"

#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/datasource.hpp>
#include <cudf/io/orc.hpp>

namespace ral {
namespace io {

class orc_parser : public data_parser {
public:
	orc_parser(std::map<std::string, std::string> args_map);

	virtual ~orc_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

	std::unique_ptr<ral::frame::BlazingTable> get_metadata(
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files,
		int offset);

	DataType type() const override { return DataType::ORC; }

private:
	std::map<std::string, std::string> args_map;
};

} /* namespace io */
} /* namespace ral */

#endif /* ORCPARSER_H_ */
