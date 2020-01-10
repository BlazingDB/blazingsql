#ifndef ORCPARSER_H_
#define ORCPARSER_H_

#include "DataParser.h"
#include "GDFColumn.cuh"
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
	//orc_parser(cudf::orc_read_arg orc_args);
	virtual ~orc_parser();

	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		std::vector<gdf_column_cpp> & columns_out,
		const Schema & schema,
		std::vector<size_t> column_indices_requested);

	std::unique_ptr<ral::frame::BlazingTable> parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, Schema & schema);

private:
	// DEPRECATED use orc_args
	//cudf::orc_read_arg orc_arg{cudf::source_info{""}};
	cudf_io::read_orc_args orc_args{cudf_io::source_info("")};
};

} /* namespace io */
} /* namespace ral */

#endif /* ORCPARSER_H_ */
