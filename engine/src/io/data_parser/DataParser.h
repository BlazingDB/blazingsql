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
#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class data_parser {
public:

	virtual ral::frame::TableViewPair parse(
		std::shared_ptr<arrow::io::RandomAccessFile> file,
		const std::string & user_readable_file_handle,
		const Schema & schema,
		std::vector<size_t> column_indices) {
			return std::make_pair(nullptr, ral::frame::BlazingTableView()); // TODO cordova ask ALexander why is not a pure virtual function as before
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
