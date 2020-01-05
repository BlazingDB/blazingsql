/*
 * ColumnManipulation.cuh
 *
 *  Created on: Aug 9, 2018
 *      Author: felipe
 */

#ifndef COLUMNMANIPULATION_CUH_
#define COLUMNMANIPULATION_CUH_

#include "gdf_wrapper/gdf_wrapper.cuh"

#include <cudf/column/column.hpp>

//TODO: in theory  we want to get rid of this
// we should be using permutation iterators when we can

void materialize_column(cudf::column * input,
		cudf::column * output,
		cudf::column * row_indeces);

#endif /* COLUMNMANIPULATION_CUH_ */
