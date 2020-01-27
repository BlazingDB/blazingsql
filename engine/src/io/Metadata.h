/*
 * Schema.h
 *
 *  Created on: Apr 26, 2019
 *      Author: felipe
 */

#ifndef BLAZING_RAL_METADATA_H_
#define BLAZING_RAL_METADATA_H_

#include <cudf/cudf.h>
#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"

namespace ral {
namespace io {

class Metadata {
public:
	Metadata(std::vector<CudfColumnView> metadata, std::pair<int, int> offset)
		: metadata_{metadata}, offset_{offset}
	{
	}

	std::vector<CudfColumnView> get_columns() {
		return metadata_;
	}

	int offset(){
		return offset_.first;
	}

	int offset_size(){
		return offset_.first;
	}
public:
	//	[T_1(min), T_1(max), T_2(min), T_2(max), ... T_n(min), T_n(max), file_path_index, row_group]
	// pair-wise elements are min, evens
	// impar-wise elements are max, odds
	// file_path_index
	// row_group
	std::vector<CudfColumnView> metadata_; // for all files in that node!
	std::pair<int, int> offset_;
};

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_METADATA_H_ */
