/*
 * DataParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DATAPARSER_H_
#define DATAPARSER_H_

#include "../Metadata.h"
#include "../Schema.h"
#include "GDFColumn.cuh"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class data_parser {
public:
	/**
	 * columns should be the full size of the schema, if for example, some of the columns
	 * are not going to be parsed, we will still want a gdf_column_cpp of size 0
	 * in there so we can preserve column index like access e.g. $3 $1 from the logical plan
	 */
	// Deprecated
	virtual void parse(std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		std::vector<gdf_column_cpp> & columns,
		const Schema & schema,
		std::vector<size_t> column_indices) = 0;

	virtual std::unique_ptr<ral::frame::BlazingTable> parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices) {
			return nullptr;
	}


	virtual void parse_schema(
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Schema & schema) = 0;

	virtual bool get_metadata(std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files, ral::io::Metadata & metadata) {
		return false;
	}
};

} /* namespace io */
} /* namespace ral */

class DataParser {
public:
	DataParser();
	virtual ~DataParser();
};
#endif /* DATAPARSER_H_ */
