/*
 * GDFColumn.h
 *
 *  Created on: Sep 12, 2018
 *      Author: rqc
 */

#ifndef GDFCOLUMN_H_
#define GDFCOLUMN_H_

#include "gdf_wrapper/gdf_wrapper.cuh"
#include "GDFCounter.cuh"
#include "Utils.cuh"
#include "Types.h"
#include <string>

#include <cudf/column/column.hpp>

class gdf_column_cpp
{
	private:
		cudf::valid_type * allocate_valid();


	//	gdf_column_cpp(void* _data, cudf::valid_type* _valid, cudf::type_id _dtype, size_t _size, cudf::size_type _null_count, const std::string &column_name = "");
	public:

	void set_name(std::string name);

    std::string name() const;

	gdf_column_cpp();
	void create_gdf_column(cudf::column * column, bool registerColumn = true);

	gdf_column_cpp(const gdf_column_cpp& col);

	void operator=(const gdf_column_cpp& col);

	gdf_column_cpp clone(std::string name = "", bool registerColumn = true);

	cudf::column* get_gdf_column() const;

	void create_gdf_column(NVCategory* category, size_t num_values, std::string column_name);

	void create_gdf_column(NVStrings* strings, size_t num_values, std::string column_name);

	void create_gdf_column(cudf::type_id type, size_t num_values, void * input_data, size_t width_per_value, const std::string &column_name = "", bool allocate_valid_buffer = false);

	void create_gdf_column(cudf::type_id type, size_t num_values, void * input_data, cudf::valid_type * host_valid, size_t width_per_value, const std::string &column_name = "");

	void create_gdf_column(const std::unique_ptr<cudf::scalar> & scalar, const std::string &column_name);

	void create_empty(const cudf::type_id dtype, const std::string &column_name);

	void create_empty(const cudf::type_id dtype);

	void allocate_like(const gdf_column_cpp& other);

	gdf_error gdf_column_view(cudf::column *column, void *data, cudf::valid_type *valid, cudf::size_type size, cudf::type_id dtype);

	~gdf_column_cpp();

private:
    cudf::column *column;
    std::string column_name;
};

#endif /* GDFCOLUMN_H_ */
