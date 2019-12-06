

#ifndef GDFPARSER_H_
#define GDFPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"

#include "GDFColumn.cuh"
#include "cudf.h"
#include "../../include/io/io.h"
#include <arrow/table.h>

namespace ral {
namespace io {

class arrow_parser: public data_parser {
public:
	arrow_parser( std::shared_ptr< arrow::Table > table);
	
	virtual ~arrow_parser();


	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
			const std::string & user_readable_file_handle,
			std::vector<gdf_column_cpp> & columns_out,
			const Schema & schema,
			std::vector<size_t> column_indices_requested);


	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
			ral::io::Schema & schema);

;

private:
	std::shared_ptr< arrow::Table > table;

};

} /* namespace io */
} /* namespace ral */

#endif /* GDFPARSER_H_ */
