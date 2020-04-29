

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
	gdf_parser(std::vector<frame::BlazingTableView> blazingTableViews);

	size_t get_num_partitions();

	virtual ~gdf_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<std::size_t> column_indices);

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const Schema & schema,
		std::vector<size_t> column_indices,
		cudf::size_type partition);

	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema);

private:
	std::vector<frame::BlazingTableView> blazingTableViews_;
};

} /* namespace io */
} /* namespace ral */

#endif /* GDFPARSER_H_ */
