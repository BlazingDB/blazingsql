#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"

#include "CalciteExpressionParsing.h"
#include "Traits/RuntimeTraits.h"
#include "cudf/legacy/unary.hpp"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/legacy/column.hpp>

namespace ral {
namespace utilities {

std::vector<cudf::column_view> concatTables(const std::vector<std::vector<cudf::column_view>> & tables) {
	assert(tables.size() > 0);

	cudf::size_type num_columns = 0;
	cudf::size_type output_row_size = 0;
	cudf::size_type num_tables_with_data = 0;
	std::vector<std::vector<cudf::column_view>> columnsToConcatArray;
	for(size_t i = 0; i < tables.size(); i++) {
		if(tables[i].size() > 0) {
			const std::vector<cudf::column_view> & table = tables[i];
			if(num_columns == 0) {
				num_columns = table.size();
				columnsToConcatArray.resize(num_columns);
			} else {
				if(table.size() != num_columns) {
					Library::Logging::Logger().logError(
						buildLogString("", "", "", "ERROR: tables being concatenated are not the same size"));
				}
				assert(table.size() == num_columns);
			}

			cudf::size_type table_rows = table[0].size();
			if(table_rows == 0) {
				continue;
			}

			num_tables_with_data++;
			output_row_size += table_rows;
			for(cudf::size_type j = 0; j < num_columns; j++) {
				columnsToConcatArray[j].push_back(table[j]);
			}
		}
	}

	if(output_row_size == 0) {  // if no table has data, lets try to return one that at least has column definitions
		for(size_t i = 0; i < tables.size(); i++) {
			if(tables[i].size() > 0) {
				return tables[i];
			}
		}
		return std::vector<cudf::column_view>();
	}
	if(num_tables_with_data == 1) {  // if only one table has data, lets return it
		for(size_t i = 0; i < tables.size(); i++) {
			if(tables[i].size() > 0 && tables[i][0].size() > 0) {
				return tables[i];
			}
		}
	}

	std::vector<cudf::column_view> output_table(num_columns);
	for(size_t i = 0; i < num_columns; i++) {
		columnsToConcatArray[i] = normalizeColumnTypes(columnsToConcatArray[i]);

		std::vector<cudf::column_view> raw_columns_to_concat(columnsToConcatArray[i].size());
		for(size_t j = 0; j < columnsToConcatArray[i].size(); j++) {
			raw_columns_to_concat[j] = columnsToConcatArray[i][j];
		}

		if(std::any_of(raw_columns_to_concat.cbegin(), raw_columns_to_concat.cend(), [](cudf::column_view c) {
				return c.has_nulls();
		   })) {
						
			cudf::data_type dtype = raw_columns_to_concat[0].type();
			cudf::size_type size = output_row_size;
			rmm::device_buffer data = rmm::device_buffer();
			rmm::device_buffer null_mask = rmm::device_buffer();
			cudf::size_type null_count = cudf::UNKNOWN_NULL_COUNT;
			                                                           
			cudf::column new_col(dtype, size, data, null_mask, null_count);
			cudf::column_view new_view = new_col;
			output_table[i] = new_view;
		} else {
			output_table[i].create_gdf_column(raw_columns_to_concat[0]->dtype,
				raw_columns_to_concat[0]->dtype_info,
				output_row_size,
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(raw_columns_to_concat[0]->dtype),
				std::string(raw_columns_to_concat[0]->col_name));
		}

		CUDF_CALL(gdf_column_concat(
			output_table[i].get_gdf_column(), raw_columns_to_concat.data(), raw_columns_to_concat.size()));
	}

	return output_table;
}

std::vector<gdf_column_cpp> normalizeColumnTypes(std::vector<gdf_column_cpp> columns) {
	if(columns.size() < 2) {
		return columns;
	}

	gdf_dtype common_type = columns[0].dtype();
	gdf_dtype_extra_info common_info = columns[0].dtype_info();
	for(size_t j = 1; j < columns.size(); j++) {
		gdf_dtype type_out;
		gdf_dtype_extra_info info_out;
		get_common_type(common_type, common_info, columns[j].dtype(), columns[j].dtype_info(), type_out, info_out);

		if(type_out == GDF_invalid) {
			throw std::runtime_error("In normalizeColumnTypes function: no common type between " +
									 std::to_string(common_type) + " and " + std::to_string(columns[j].dtype()));
		}

		common_type = type_out;
		common_info = info_out;
	}

	std::vector<gdf_column_cpp> columns_out(columns.size());
	for(size_t j = 0; j < columns.size(); j++) {
		if(common_type == GDF_TIMESTAMP) {
			if(columns[j].dtype() == common_type && columns[j].dtype_info().time_unit == common_info.time_unit) {
				columns_out[j] = columns[j];
			} else {
				Library::Logging::Logger().logWarn(buildLogString("",
					"",
					"",
					"WARNING: normalizeColumnTypes casting " + std::to_string(columns[j].get_gdf_column()->dtype) +
						" to " + std::to_string(common_type)));
				gdf_column raw_column_out = cudf::cast(*(columns[j].get_gdf_column()), common_type, common_info);
				gdf_column * temp_raw_column = new gdf_column{};
				*temp_raw_column = raw_column_out;
				columns_out[j].create_gdf_column(temp_raw_column);
				columns_out[j].set_name(columns[j].name());
			}
		} else if(columns[j].dtype() == common_type) {
			columns_out[j] = columns[j];
		} else {
			Library::Logging::Logger().logWarn(buildLogString("",
				"",
				"",
				"WARNING: normalizeColumnTypes casting " + std::to_string(columns[j].get_gdf_column()->dtype) + " to " +
					std::to_string(common_type)));
			gdf_column raw_column_out = cudf::cast(*(columns[j].get_gdf_column()), common_type);
			gdf_column * temp_raw_column = new gdf_column{};
			*temp_raw_column = raw_column_out;
			columns_out[j].create_gdf_column(temp_raw_column);
			columns_out[j].set_name(columns[j].name());
		}
	}
	return columns_out;
}

}  // namespace utilities
}  // namespace ral
