

#ifndef ARROWPARSER_H_
#define ARROWPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"

//#include "cudf.h"
#include "io/io.h"
#include <arrow/table.h>

namespace ral {
namespace io {

class arrow_parser : public data_parser {
public:
	arrow_parser();

	virtual ~arrow_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(ral::io::data_handle /*handle*/,
			ral::io::Schema & schema);

	DataType type() const override { return DataType::ARROW; }

private:
	//std::shared_ptr< arrow::Table > table;
};

} /* namespace io */
} /* namespace ral */

#endif /* ARROWPARSER_H_ */
