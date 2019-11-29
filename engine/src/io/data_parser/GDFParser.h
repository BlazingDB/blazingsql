

#ifndef GDFPARSER_H_
#define GDFPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"

#include "GDFColumn.cuh"
#include "cudf.h"
#include "../../include/io/io.h"

namespace ral {
namespace io {

class gdf_parser: public data_parser {
public:
	gdf_parser(TableSchema tableSchemas);

	virtual ~gdf_parser();


	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
			const std::string & user_readable_file_handle,
			std::vector<gdf_column_cpp> & columns_out,
			const Schema & schema,
			std::vector<size_t> column_indices_requested);


	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
			ral::io::Schema & schema);

;

private:
	TableSchema tableSchema;
};

} /* namespace io */
} /* namespace ral */

#endif /* GDFPARSER_H_ */
