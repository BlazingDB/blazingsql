/*
 * CSVParser.h
 *
 *  Created on: Nov 29, 2018
 *      Author: felipe
 */

#ifndef CSVPARSER_H_
#define CSVPARSER_H_

#include "DataParser.h"
#include "../data_provider/DataProvider.h"
#include "arrow/io/interfaces.h"
#include <memory>
#include <vector>

#include <cudf/io/datasource.hpp>
#include <cudf/io/csv.hpp>

namespace ral {
namespace io {

class csv_parser : public data_parser {
public:
	csv_parser(std::map<std::string, std::string> args_map);

	virtual ~csv_parser();

	std::unique_ptr<ral::frame::BlazingTable> parse_batch(
		ral::io::data_handle handle,
		const Schema & schema,
		std::vector<int> column_indices,
		std::vector<cudf::size_type> row_groups);

	void parse_schema(ral::io::data_handle handle, ral::io::Schema & schema);

	size_t max_bytes_chunk_size() const;

	DataType type() const override { return DataType::CSV; }

private:
	std::map<std::string, std::string> args_map;
};

} /* namespace io */
} /* namespace ral */

#endif /* CSVPARSER_H_ */
