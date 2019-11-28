/*
 * ParquetParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef PARQUETPARSER_H_
#define PARQUETPARSER_H_

#include "DataParser.h"
#include <vector>
#include <memory>
#include "arrow/io/interfaces.h"
#include "GDFColumn.cuh"

namespace ral {
namespace io {

class parquet_parser: public data_parser {
public:
	parquet_parser();
	virtual ~parquet_parser();
	void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
				const std::string & user_readable_file_handle,
				std::vector<gdf_column_cpp> & columns_out,
				const Schema & schema,
				std::vector<size_t> column_indices_requested);

	void parse_schema(std::vector<std::shared_ptr<arrow::io::RandomAccessFile> > files,
			Schema & schema);


};

} /* namespace io */
} /* namespace ral */

#endif /* PARQUETPARSER_H_ */
