/*
 * GDFColumn.cu
 *
 *  Created on: Sep 12, 2018
 *	  Author: rqc
 */

#include <arrow/util/bit_util.h>

//readme:  use bit-utils to compute valid.size in a standard way
// see https://github.com/apache/arrow/blob/e34057c4b4be8c7abf3537dd4998b5b38919ba73/cpp/src/arrow/ipc/writer.cc#L66

#include <cudf.h>
#include "GDFColumn.cuh"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include "cuDF/Allocator.h"
#include "cuio/parquet/util/bit_util.cuh"
#include "Traits/RuntimeTraits.h"

#include <cudf/legacy/bitmask.hpp>

