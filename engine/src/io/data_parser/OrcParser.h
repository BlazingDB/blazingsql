#ifndef ORCPARSER_H_
#define ORCPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/functions.hpp>
#include <cudf/io/datasource.hpp>

namespace ral {
namespace io {

namespace cudf_io = cudf::io;

class orc_parser : public data_parser {
public:
	orc_parser(cudf::io::read_orc_args orc_args);

	virtual ~orc_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<size_t> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, Schema & schema);

private:
	cudf::io::read_orc_args orc_args{cudf_io::source_info("")};
};

} /* namespace io */
} /* namespace ral */

#endif /* ORCPARSER_H_ */
