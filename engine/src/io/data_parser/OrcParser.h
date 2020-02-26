#ifndef ORCPARSER_H_
#define ORCPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/functions.hpp>

namespace ral {
namespace io {

namespace cudf_io = cudf::experimental::io;

class orc_parser : public data_parser {
public:
	orc_parser(cudf::experimental::io::read_orc_args orc_args);

	virtual ~orc_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, Schema & schema);

private:
	cudf::experimental::io::read_orc_args orc_args{cudf_io::source_info("")};
};

} /* namespace io */
} /* namespace ral */

#endif /* ORCPARSER_H_ */
