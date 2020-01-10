/*
 * QueryState.h
 *
 *  Created on: Aug 5, 2018
 *      Author: felipe
 */

#ifndef QUERYSTATE_H_
#define QUERYSTATE_H_

#include "gdf_wrapper/gdf_wrapper.cuh"
#include <vector>

class QueryState {
public:
	QueryState();
	virtual ~QueryState();

private:
	std::vector<std::vector<cudf::column *>> data_frame;
	std::vector<size_t> cummulative_sum_sizes;
	// for every join
};

#endif /* QUERYSTATE_H_ */
