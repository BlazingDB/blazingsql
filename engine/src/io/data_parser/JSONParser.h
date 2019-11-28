/*
 * jsonParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef jsonPARSER_H_
#define jsonPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"
#include "GDFColumn.cuh"
#include "cudf/legacy/io_types.hpp"

namespace ral {
namespace io {

class json_parser: public data_parser {
public:
	json_parser(cudf::json_read_arg args);

	virtual ~json_parser();
	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
				const std::string & user_readable_file_handle,
				std::vector<gdf_column_cpp> & columns_out,
				const Schema & schema,
				std::vector<size_t> column_indices_requested);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
			Schema & schema);

private:
	cudf::json_read_arg args;
};
} /* namespace io */
} /* namespace ral */

#endif /* jsonPARSER_H_ */
