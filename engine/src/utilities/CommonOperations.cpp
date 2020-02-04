#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"

#include "CalciteExpressionParsing.h"
#include "Traits/RuntimeTraits.h"
#include "cudf/legacy/unary.hpp"
#include <algorithm>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/legacy/column.hpp>
#include <cudf/copying.hpp>
#include <numeric>

namespace ral {
namespace utilities {


std::vector<gdf_column_cpp> concatTables(const std::vector<std::vector<gdf_column_cpp>> & tables) {
	assert(tables.size() > 0);

	cudf::size_type num_columns = 0;
	cudf::size_type output_row_size = 0;
	cudf::size_type num_tables_with_data = 0;
	std::vector<std::vector<gdf_column_cpp>> columnsToConcatArray;
	for(size_t i = 0; i < tables.size(); i++) {
		if(tables[i].size() > 0) {
			const std::vector<gdf_column_cpp> & table = tables[i];
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

			cudf::size_type table_rows = table[0].get_gdf_column()->size();
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
		return std::vector<gdf_column_cpp>();
	}
	if(num_tables_with_data == 1) {  // if only one table has data, lets return it
		for(size_t i = 0; i < tables.size(); i++) {
			if(tables[i].size() > 0 && tables[i][0].get_gdf_column()->size() > 0) {
				return tables[i];
			}
		}
	}

	std::vector<gdf_column_cpp> output_table(num_columns);
	for(size_t i = 0; i < num_columns; i++) {
		columnsToConcatArray[i] = normalizeColumnTypes(columnsToConcatArray[i]);

		std::vector<cudf::column *> raw_columns_to_concat(columnsToConcatArray[i].size());
		for(size_t j = 0; j < columnsToConcatArray[i].size(); j++) {
			raw_columns_to_concat[j] = columnsToConcatArray[i][j].get_gdf_column();
		}

		if(std::any_of(raw_columns_to_concat.cbegin(), raw_columns_to_concat.cend(), [](const cudf::column * c) {
			   return c->has_nulls();
		   })) {
			output_table[i].create_gdf_column(raw_columns_to_concat[0]->type().id(),
				output_row_size,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(raw_columns_to_concat[0]->type().id()),
				std::string(columnsToConcatArray[i][0].name()));
		} else {
			output_table[i].create_gdf_column(raw_columns_to_concat[0]->type().id(),
				output_row_size,
				nullptr,
				nullptr,
				ral::traits::get_dtype_size_in_bytes(raw_columns_to_concat[0]->type().id()),
				std::string(columnsToConcatArray[i][0].name()));
		}

		// TODO percy cudf0.12 port to cudf::column
//		CUDF_CALL(gdf_column_concat(
//			output_table[i].get_gdf_column(), raw_columns_to_concat.data(), raw_columns_to_concat.size()));
	}

	return output_table;
}

std::vector<gdf_column_cpp> normalizeColumnTypes(std::vector<gdf_column_cpp> columns) {
	if(columns.size() < 2) {
		return columns;
	}

	cudf::type_id common_type = columns[0].get_gdf_column()->type().id();
	for(size_t j = 1; j < columns.size(); j++) {
		cudf::type_id type_out;
		get_common_type(common_type, columns[j].get_gdf_column()->type().id(), type_out);

		// TODO percy cudf0.12 was invalid here, should we consider empty?
		if(type_out == cudf::type_id::EMPTY) {
			throw std::runtime_error("In normalizeColumnTypes function: no common type between " +
									 std::to_string(common_type) + " and " + std::to_string(columns[j].get_gdf_column()->type().id()));
		}

		common_type = type_out;
	}

	std::vector<gdf_column_cpp> columns_out(columns.size());
	for(size_t j = 0; j < columns.size(); j++) {
		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		if(common_type == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			if(columns[j].get_gdf_column()->type().id() == common_type) {
				columns_out[j] = columns[j];
			} else {
				Library::Logging::Logger().logWarn(buildLogString("",
					"",
					"",
					"WARNING: normalizeColumnTypes casting " + std::to_string(columns[j].get_gdf_column()->type().id()) +
						" to " + std::to_string(common_type)));

				// TODO percy cudf0.12 port to cudf::column				
//				cudf::column raw_column_out = cudf::cast(*(columns[j].get_gdf_column()), to_gdf_type(common_type));
//				cudf::column * temp_raw_column = new cudf::column();
//				*temp_raw_column = raw_column_out;
//				columns_out[j].create_gdf_column(temp_raw_column);
//				columns_out[j].set_name(columns[j].name());
			}
		} else if(columns[j].get_gdf_column()->type().id() == common_type) {
			columns_out[j] = columns[j];
		} else {
			Library::Logging::Logger().logWarn(buildLogString("",
				"",
				"",
				"WARNING: normalizeColumnTypes casting " + std::to_string(columns[j].get_gdf_column()->type().id()) + " to " +
					std::to_string(common_type)));

			// TODO percy cudf0.12 port to cudf::column				
//			cudf::column raw_column_out = cudf::cast(*(columns[j].get_gdf_column()), to_gdf_type(common_type));
//			cudf::column * temp_raw_column = new cudf::column();
//			*temp_raw_column = raw_column_out;
//			columns_out[j].create_gdf_column(temp_raw_column);
//			columns_out[j].set_name(columns[j].name());
		}
	}
	return columns_out;
}

}  // namespace utilities
}  // namespace ral

namespace ral {
namespace utilities {
namespace experimental {


std::unique_ptr<BlazingTable> concatTables(const std::vector<BlazingTableView> & tables) {
	assert(tables.size() > 0);

	std::vector<std::unique_ptr<CudfTable>> temp_holder;
	std::vector<std::string> names;
	std::vector<CudfTableView> table_views_to_concat;
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i].names().size() > 0){ // lets make sure we get the names from a table that is not empty
			names = tables[i].names();
		}
		if(tables[i].view().num_columns() > 0) { // lets make sure we are trying to concatenate tables that are not empty
			if (tables[i].view().column(0).offset() > 0){  // WSM this is because a bug in cudf https://github.com/rapidsai/cudf/issues/4055
				temp_holder.emplace_back(std::make_unique<CudfTable>(CudfTable(tables[i].view())));
          		table_views_to_concat.emplace_back(temp_holder.back()->view());
			} else {
				table_views_to_concat.push_back(tables[i].view());
			}
		}
	}
	// TODO want to integrate data type normalization.
	// Data type normalization means that only some columns from a table would get normalized,
	// so we would need to manage the lifecycle of only a new columns that get allocated

	std::unique_ptr<CudfTable> concatenated_tables = cudf::experimental::concatenate(table_views_to_concat);
	return std::make_unique<BlazingTable>(std::move(concatenated_tables), names);
}

std::unique_ptr<cudf::column> make_empty_column(cudf::data_type type) {
  return std::make_unique<cudf::column>(type, 0, rmm::device_buffer{});
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const std::vector<std::string> &column_names, 
	const std::vector<cudf::type_id> &dtypes, std::vector<size_t> column_indices) {
	
	if (column_indices.size() == 0){
		column_indices.resize(column_names.size());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		columns[idx] =  make_empty_column(cudf::data_type(dtypes[idx]));
	}
	auto table = std::make_unique<cudf::experimental::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), column_names);
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table) {

	std::unique_ptr<CudfTable> empty = cudf::experimental::empty_like(table.view());
	return std::make_unique<ral::frame::BlazingTable>(std::move(empty), table.names());	
}

}  // namespace experimental
}  // namespace utilities
}  // namespace ral
