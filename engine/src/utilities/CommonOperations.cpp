#include "utilities/CommonOperations.h"
#include "utilities/StringUtils.h"
#include "error.hpp"

#include "CalciteExpressionParsing.h"
#include <cudf/filling.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <cudf/copying.hpp>
#include <cudf/unary.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <numeric>

namespace ral {
namespace utilities {

bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<BlazingTable>> & tables) {
	std::vector<ral::frame::BlazingTableView> tables_to_concat(tables.size());
	for (std::size_t i = 0; i < tables.size(); i++){
		tables_to_concat[i] = tables[i]->toBlazingTableView();
	}

	return checkIfConcatenatingStringsWillOverflow(tables_to_concat);
}

bool checkIfConcatenatingStringsWillOverflow(const std::vector<BlazingTableView> & tables) {
	if( tables.size() == 0 ) {
		return false;
	}

	// Lets only look at tables that not empty
	std::vector<size_t> non_empty_index;
	for(size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
		if (tables[table_idx].num_columns() > 0){
			non_empty_index.push_back(table_idx);
		}
	}
	if( non_empty_index.size() == 0 ) {
		return false;
	}


	for(size_t col_idx = 0; col_idx < tables[non_empty_index[0]].get_schema().size(); col_idx++) {
		if(tables[non_empty_index[0]].get_schema()[col_idx].id() == cudf::type_id::STRING) {
			std::size_t total_bytes_size = 0;
			std::size_t total_offset_count = 0;

			for(size_t i = 0; i < non_empty_index.size(); i++) {
				size_t table_idx = non_empty_index[i];

				// Column i-th from the next tables are expected to have the same string data type
				assert( tables[table_idx].get_schema()[col_idx].id() == cudf::type_id::STRING );

				auto & column = tables[table_idx].column(col_idx);
				auto num_children = column.num_children();
				if(num_children == 2) {

					auto offsets_column = column.child(0);
					auto chars_column = column.child(1);

					// Similarly to cudf, we focus only on the byte number of chars and the offsets count
					total_bytes_size += chars_column.size();
					total_offset_count += offsets_column.size() + 1;

					if( total_bytes_size > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max()) ||
						total_offset_count > static_cast<std::size_t>(std::numeric_limits<cudf::size_type>::max())) {
						return true;
					}
				}
			}
		}
	}

	return false;
}

std::unique_ptr<BlazingTable> concatTables(const std::vector<BlazingTableView> & tables) {
	assert(tables.size() >= 0);

	std::vector<std::string> names;
	std::vector<CudfTableView> table_views_to_concat;
	for(size_t i = 0; i < tables.size(); i++) {
		if (tables[i].names().size() > 0){ // lets make sure we get the names from a table that is not empty
			names = tables[i].names();
		}
		if(tables[i].view().num_columns() > 0) { // lets make sure we are trying to concatenate tables that are not empty
			table_views_to_concat.push_back(tables[i].view());
		}
	}
	// TODO want to integrate data type normalization.
	// Data type normalization means that only some columns from a table would get normalized,
	// so we would need to manage the lifecycle of only a new columns that get allocated

	size_t empty_count = 0;
	for(size_t i = 0; i < table_views_to_concat.size(); i++) {
		if (table_views_to_concat[i].num_rows() == 0){
			++empty_count;
		}		
	}

 	// All tables are empty so we just need to return the 1st one
	if (empty_count == table_views_to_concat.size()) {
		return std::make_unique<ral::frame::BlazingTable>(table_views_to_concat[0], names);
	}

	std::unique_ptr<CudfTable> concatenated_tables = cudf::concatenate(table_views_to_concat);
	return std::make_unique<BlazingTable>(std::move(concatenated_tables), names);
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const std::vector<std::string> &column_names,
	const std::vector<cudf::data_type> &dtypes, std::vector<size_t> column_indices) {

	if (column_indices.size() == 0){
		column_indices.resize(column_names.size());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	}

	std::vector<std::unique_ptr<cudf::column>> columns(column_indices.size());

	for (auto idx : column_indices) {
		columns[idx] =  make_empty_column(dtypes[idx]);
	}
	auto table = std::make_unique<cudf::table>(std::move(columns));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), column_names);
}

std::unique_ptr<cudf::table> create_empty_table(const std::vector<cudf::type_id> &dtypes) {
	std::vector<std::unique_ptr<cudf::column>> columns(dtypes.size());
	for (size_t idx =0; idx < dtypes.size(); idx++) {
		columns[idx] =  make_empty_column(cudf::data_type(dtypes[idx]));
	}
	return std::make_unique<cudf::table>(std::move(columns));
}

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table) {

	std::unique_ptr<CudfTable> empty = cudf::empty_like(table.view());
	return std::make_unique<ral::frame::BlazingTable>(std::move(empty), table.names());
}


std::vector<cudf::data_type> get_common_types(const std::vector<cudf::data_type> & types1, const std::vector<cudf::data_type> & types2, bool strict){
	RAL_EXPECTS(types1.size() == types2.size(), "In get_common_types: Mismatched number of columns");
	std::vector<cudf::data_type> common_types(types1.size());
	for(size_t j = 0; j < common_types.size(); j++) {
		common_types[j] = get_common_type(types1[j], types2[j], strict);
	}
	return common_types;
}


cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict) {
	if(type1 == type2) {
		return type1;
	} else if((is_type_float(type1.id()) && is_type_float(type2.id())) || (is_type_integer(type1.id()) && is_type_integer(type2.id()))) {
		return (cudf::size_of(type1) >= cudf::size_of(type2))	? type1	: type2;
	} else if(is_type_timestamp(type1.id()) && is_type_timestamp(type2.id())) {
		// if they are both datetime, return the highest resolution either has
		static constexpr std::array<cudf::data_type, 5> datetime_types = {
			cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS},
			cudf::data_type{cudf::type_id::TIMESTAMP_DAYS}
		};

		for (auto datetime_type : datetime_types){
			if(type1 == datetime_type || type2 == datetime_type)
				return datetime_type;
		}
	}
	if (strict) {
		RAL_FAIL("No common type between " + std::to_string(static_cast<int32_t>(type1.id())) + " and " + std::to_string(static_cast<int32_t>(type2.id())));
	} else {
		if(is_type_float(type1.id()) && is_type_integer(type2.id())) {
			return type1;
		} else if (is_type_float(type2.id()) && is_type_integer(type1.id())) {
			return type2;
		} else if (is_type_bool(type1.id()) && (is_type_integer(type2.id()) || is_type_float(type2.id()) || is_type_string(type2.id()) )){
			return type2;
		} else if (is_type_bool(type2.id()) && (is_type_integer(type1.id()) || is_type_float(type1.id()) || is_type_string(type1.id()) )){
			return type1;
		}
	}
}

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types, 
		std::vector<cudf::size_type> column_indices) {
	
	if (column_indices.size() == 0){
		RAL_EXPECTS(table->num_columns() == types.size(), "In normalize_types: table->num_columns() != types.size()");
		column_indices.resize(table->num_columns());
		std::iota(column_indices.begin(), column_indices.end(), 0);
	} else {
		RAL_EXPECTS(column_indices.size() == types.size(), "In normalize_types: column_indices.size() != types.size()");
	}
	std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns = table->releaseBlazingColumns();
	for (size_t i = 0; i < column_indices.size(); i++){
		if (!(columns[column_indices[i]]->view().type() == types[i])){
			std::unique_ptr<CudfColumn> casted = cudf::cast(columns[column_indices[i]]->view(), types[i]);
			columns[column_indices[i]] = std::make_unique<ral::frame::BlazingColumnOwner>(std::move(casted));			
		}
	}
	table = std::make_unique<ral::frame::BlazingTable>(std::move(columns), table->names());	
}


int64_t get_table_size_bytes(const ral::frame::BlazingTableView & table){
	if (table.num_rows() == 0){
		return 0;
	} else {
		int64_t bytes = 0;
		CudfTableView table_view = table.view();
		for(size_t i = 0; i < table_view.num_columns(); i++) {
			if(table_view.column(i).type().id() == cudf::type_id::STRING){
				cudf::strings_column_view str_col_view{table_view.column(i)};
				auto offsets_column = str_col_view.offsets();
				auto chars_column = str_col_view.chars();
				bytes += (int64_t)(chars_column.size());
				bytes += (int64_t)(offsets_column.size()) * (int64_t)(sizeof(int32_t));
			} else {
				bytes += (int64_t)(cudf::size_of(table_view.column(i).type()) * (int64_t)(table_view.num_rows()));
			}
		}
		return bytes;
	}
}

}  // namespace utilities
}  // namespace ral
