#ifndef GDF_UTILS_H
#define GDF_UTILS_H

#include "cudf/cudf.h"
#include "cudf/utilities/error.hpp"
#include "error_utils.h"
#include "miscellany.hpp"
#include <cuda_runtime_api.h>
#include <vector>

#include "cudf/types.hpp"
typedef cudf::size_type gdf_index_type;

inline bool gdf_is_valid(const cudf::valid_type * valid, gdf_index_type pos) {
	if(valid)
		return (valid[pos / GDF_VALID_BITSIZE] >> (pos % GDF_VALID_BITSIZE)) & 1;
	else
		return true;
}

// Buffers are padded to 64-byte boundaries (for SIMD) static
constexpr int32_t kArrowAlignment = 64;

// Tensors are padded to 64-byte boundaries static
constexpr int32_t kTensorAlignment = 64;

// Align on 8-byte boundaries in IPC static
constexpr int32_t kArrowIpcAlignment = 8;

// todo, enable arrow ipc utils, and remove this method
static inline int64_t PaddedLength(int64_t nbytes, int32_t alignment = kArrowAlignment) {
	return ((nbytes + alignment - 1) / alignment) * alignment;
}

/**
 * Calculates the size in bytes of a validity indicator pseudo-column for a given column's size.
 *
 * @note Actually, this is the size in bytes of a column of bits, where the individual
 * bit-container elements are of the same size as `cudf::valid_type`.
 *
 * @param[in] column_size the number of elements, i.e. the number of bits to be available
 * for use, in the column
 * @return the number of bytes necessary to make available for the validity indicator pseudo-column
 */
inline cudf::size_type get_number_of_bytes_for_valid(cudf::size_type column_size) {
	// return gdf::util::div_rounding_up_safe(column_size, GDF_VALID_BITSIZE);
	return (column_size + (cudf::size_type) GDF_VALID_BITSIZE - 1) / ((cudf::size_type) GDF_VALID_BITSIZE);
}

/* --------------------------------------------------------------------------*/
/**
 * @brief Flatten AOS info from gdf_columns into SOA.
 *
 * @Param[in] cols Host-side array of gdf_columns
 * @Param[in] ncols # columns
 * @Param[out] d_cols Pointer to device array of columns
 * @Param[out] d_types Device array of column types
 *
 * @Returns GDF_SUCCESS upon successful completion
 */
/* ----------------------------------------------------------------------------*/
inline gdf_error soa_col_info(gdf_column * cols, size_t ncols, void ** d_cols, int * d_types) {
	std::vector<void *> v_cols(ncols, nullptr);
	std::vector<int> v_types(ncols, 0);
	for(size_t i = 0; i < ncols; ++i) {
		v_cols[i] = cols[i].data;
		v_types[i] = cols[i].dtype;
	}

	void ** h_cols = v_cols.data();
	int * h_types = v_types.data();
	CUDA_TRY(cudaMemcpy(d_cols, h_cols, ncols * sizeof(void *), cudaMemcpyHostToDevice));  // TODO: add streams
	CUDA_TRY(cudaMemcpy(d_types, h_types, ncols * sizeof(int), cudaMemcpyHostToDevice));   // TODO: add streams

	return GDF_SUCCESS;
}

/* --------------------------------------------------------------------------*/
/**
 * @brief Flatten AOS info from gdf_columns into SOA.
 *
 * @Param[in] cols Host-side array of pointers to gdf_columns
 * @Param[in] ncols # columns
 * @Param[out] d_cols Pointer to device array of columns
 * @Param[out] d_valids Pointer to device array of cudf::valid_type for each column
 * @Param[out] d_types Device array of column types
 *
 * @Returns GDF_SUCCESS upon successful completion
 */
/* ----------------------------------------------------------------------------*/
inline gdf_error soa_col_info(
	gdf_column ** cols, size_t ncols, void ** d_cols, cudf::valid_type ** d_valids, int * d_types) {
	std::vector<void *> v_cols(ncols, nullptr);
	std::vector<cudf::valid_type *> v_valids(ncols, nullptr);
	std::vector<int> v_types(ncols, 0);
	for(size_t i = 0; i < ncols; ++i) {
		v_cols[i] = cols[i]->data;
		v_valids[i] = cols[i]->valid;
		v_types[i] = cols[i]->dtype;
	}

	void ** h_cols = v_cols.data();
	cudf::valid_type ** h_valids = v_valids.data();
	int * h_types = v_types.data();
	CUDA_TRY(cudaMemcpy(d_cols, h_cols, ncols * sizeof(void *), cudaMemcpyHostToDevice));  // TODO: add streams
	CUDA_TRY(
		cudaMemcpy(d_valids, h_valids, ncols * sizeof(cudf::valid_type *), cudaMemcpyHostToDevice));  // TODO: add streams
	CUDA_TRY(cudaMemcpy(d_types, h_types, ncols * sizeof(int), cudaMemcpyHostToDevice));			// TODO: add streams

	return GDF_SUCCESS;
}

#endif  // GDF_UTILS_H
