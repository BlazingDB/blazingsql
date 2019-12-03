/*
 * JoinProcessor.h
 *
 *  Created on: Aug 7, 2018
 *      Author: felipe
 */

#ifndef JOINPROCESSOR_H_
#define JOINPROCESSOR_H_

#include "DataFrame.h"
#include "gdf_wrapper/gdf_wrapper.cuh"

void parseJoinConditionToColumnIndices(const std::string & condition, std::vector<int> & columnIndices);

void evaluate_join(std::string condition,
	std::string join_type,
	blazing_frame data_frame,
	gdf_column * left_indices,
	gdf_column * right_indices);


#endif /* JOINPROCESSOR_H_ */
