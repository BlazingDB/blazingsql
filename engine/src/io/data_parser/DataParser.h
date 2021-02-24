/*
 * DataParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef DATAPARSER_H_
#define DATAPARSER_H_

#include "../Schema.h"
#include "../DataType.h"
#include "../data_provider/DataProvider.h"

#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

namespace ral {
namespace io {

class data_parser {
public:

	virtual std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle /*handle*/,
		const Schema & /*schema*/,
		std::vector<int> /*column_indices*/,
		std::vector<cudf::size_type> /*row_groups*/) {
		return nullptr; // TODO cordova ask ALexander why is not a pure virtual function as before
	}

	virtual size_t get_num_partitions() {
		return 0;
	}

	virtual void parse_schema(
		std::shared_ptr<arrow::io::RandomAccessFile> file, ral::io::Schema & schema) = 0;

	virtual std::unique_ptr<ral::frame::BlazingTable> get_metadata(
		std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> /*files*/,
		int /*offset*/) {
		return nullptr;
	}

	virtual DataType type() const { return 	DataType::UNDEFINED; }
};

} /* namespace io */
} /* namespace ral */

class DataParser {
public:
	DataParser();
	virtual ~DataParser();
};
#endif /* DATAPARSER_H_ */
