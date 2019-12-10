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


namespace ral {
namespace io {

class Metadata {
public:
	Metadata(std::vector<gdf_column*> metadata)
		: metadata_{metadata}
	{
	}

public:
	//	[T_1(min), T_1(max), T_2(min), T_2(max), ... T_n(min), T_n(max), file_path_index, row_group]
	// pair-wise elements are min, evens
	// impar-wise elements are max, odds
	// file_path_index
	// row_group
	std::vector<gdf_column*> metadata_; // for all files in that node!
};

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_METADATA_H_ */
