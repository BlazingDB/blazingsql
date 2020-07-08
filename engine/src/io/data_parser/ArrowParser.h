

#ifndef ARROWPARSER_H_
#define ARROWPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"

//#include "cudf.h"
#include "io/io.h"
#include <arrow/table.h>

#include <cudf/io/functions.hpp>

namespace ral {
namespace io {

class arrow_parser : public data_parser {
public:
	arrow_parser( std::shared_ptr< arrow::Table > table);

	virtual ~arrow_parser();
	
	void parse_schema(std::shared_ptr<arrow::io::RandomAccessFile> file,
			ral::io::Schema & schema);

private:
	std::shared_ptr< arrow::Table > table;
};

} /* namespace io */
} /* namespace ral */

#endif /* ARROWPARSER_H_ */
