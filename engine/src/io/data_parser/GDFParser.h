

#ifndef GDFPARSER_H_
#define GDFPARSER_H_

#include "DataParser.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>
#include "cudf.h"

#include <cudf/io/functions.hpp>

#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace io {

class gdf_parser : public data_parser {
public:
	gdf_parser(frame::BlazingTableView blazingTableView);

	virtual ~gdf_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<std::size_t> column_indices);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema);

private:
	frame::BlazingTableView blazingTableView_;
};

} /* namespace io */
} /* namespace ral */

#endif /* GDFPARSER_H_ */
