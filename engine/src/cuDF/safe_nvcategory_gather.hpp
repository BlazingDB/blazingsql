#ifndef CUDF_NVCATEGORY_GATHER_HPP_
#define CUDF_NVCATEGORY_GATHER_HPP_

#include <cudf/legacy/table.hpp>
#include <cudf/utilities/legacy/nvcategory_util.hpp>
#include <nvstrings/NVCategory.h>

namespace ral {

// TODO(cudf): We need to check output size because nvcategory_gather
// doesn't create a empty NVCategory for output gdf column, so it could
// produce crashes
inline gdf_error safe_nvcategory_gather_for_string_category(gdf_column * column, NVCategory * nv_category) {
	gdf_error error = nvcategory_gather(column, nv_category);

	if(0 == column->size && column->dtype_info.category == nullptr) {
		column->dtype_info.category = NVCategory::create_from_array(nullptr, 0);
	}

	return error;
}

inline gdf_error safe_nvcategory_gather_for_string_category(gdf_column * column, void * category) {
	return safe_nvcategory_gather_for_string_category(column, static_cast<NVCategory *>(category));
}

inline void init_string_category_if_null(cudf::table & table) {
	for(auto && c : table) {
		if(c->dtype == GDF_STRING_CATEGORY && 0 == c->size && c->dtype_info.category == nullptr) {
			c->dtype_info.category = NVCategory::create_from_array(nullptr, 0);
		}
	}
}

inline void init_string_category_if_null(gdf_column * column) {
	if(column->dtype == GDF_STRING_CATEGORY && 0 == column->size && column->dtype_info.category == nullptr) {
		column->dtype_info.category = NVCategory::create_from_array(nullptr, 0);
	}
}

inline void truncate_nvcategory(gdf_column * column, cudf::size_type new_size) {
	assert(new_size <= column->size);
	if(new_size == column->size) {
		return;
	}

	NVCategory * nvCategory = static_cast<NVCategory *>(column->dtype_info.category);
	NVCategory * newNvCategory = nvCategory->gather(static_cast<nv_category_index_type *>(column->data), new_size);
	NVCategory::destroy(nvCategory);
	column->dtype_info.category = newNvCategory;
	newNvCategory->get_values(static_cast<nv_category_index_type *>(column->data));
	column->size = new_size;
}


inline void gather_and_remap_nvcategory(cudf::table & table) {
	for(auto && c : table) {
		if(c->dtype == GDF_STRING_CATEGORY && c->size > 0 && c->dtype_info.category != nullptr) {
			NVCategory * nvcategory = static_cast<NVCategory *>(c->dtype_info.category);
			NVCategory * newnvcategory =
				nvcategory->gather_and_remap(static_cast<nv_category_index_type *>(c->data), c->size);
			newnvcategory->get_values(static_cast<nv_category_index_type *>(c->data));
			c->dtype_info.category = newnvcategory;
			NVCategory::destroy(nvcategory);
		}
	}
}

}  // namespace ral

#endif
