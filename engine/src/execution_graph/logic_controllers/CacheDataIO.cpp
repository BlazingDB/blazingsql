#include "CacheDataIO.h"
#include "CalciteExpressionParsing.h"

namespace ral {
namespace cache {

CacheDataIO::CacheDataIO(ral::io::data_handle handle,
	std::shared_ptr<ral::io::data_parser> parser,
	ral::io::Schema schema,
	ral::io::Schema file_schema,
	std::vector<int> row_group_ids,
	std::vector<int> projections)
	: CacheData(CacheDataType::IO_FILE, schema.get_names(), schema.get_data_types(), 1),
	handle(handle), parser(parser), schema(schema),
	file_schema(file_schema), row_group_ids(row_group_ids),
	projections(projections)
	{

	}

size_t CacheDataIO::sizeInBytes() const{
	return 0;
}

std::unique_ptr<ral::frame::BlazingTable> CacheDataIO::decache(){
	if (schema.all_in_file()){
		std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(handle, file_schema, projections, row_group_ids);
		return loaded_table;
	} else {
		std::vector<int> column_indices_in_file;  // column indices that are from files
		for (auto projection_idx : projections){
			if(schema.get_in_file()[projection_idx]) {
				column_indices_in_file.push_back(projection_idx);
			}
		}

		std::vector<std::unique_ptr<cudf::column>> all_columns(projections.size());
		std::vector<std::unique_ptr<cudf::column>> file_columns;
		std::vector<std::string> names;
		cudf::size_type num_rows;
		if (column_indices_in_file.size() > 0){
			std::unique_ptr<ral::frame::BlazingTable> current_blazing_table = parser->parse_batch(handle, file_schema, column_indices_in_file, row_group_ids);
			names = current_blazing_table->names();
			std::unique_ptr<CudfTable> current_table = current_blazing_table->releaseCudfTable();
			num_rows = current_table->num_rows();
			file_columns = current_table->release();

		} else { // all tables we are "loading" are from hive partitions, so we dont know how many rows we need unless we load something to get the number of rows
			std::vector<int> temp_column_indices = {0};
			std::unique_ptr<ral::frame::BlazingTable> loaded_table = parser->parse_batch(handle, file_schema, temp_column_indices, row_group_ids);
			num_rows = loaded_table->num_rows();
		}

		int in_file_column_counter = 0;
		for(std::size_t i = 0; i < projections.size(); i++) {
			int col_ind = projections[i];
			if(!schema.get_in_file()[col_ind]) {
				std::string name = schema.get_name(col_ind);
				names.push_back(name);
				cudf::type_id type = schema.get_dtype(col_ind);
				std::string literal_str = handle.column_values[name];
				std::unique_ptr<cudf::scalar> scalar = get_scalar_from_string(literal_str, cudf::data_type{type},false);
				all_columns[i] = cudf::make_column_from_scalar(*scalar, num_rows);
			} else {
				all_columns[i] = std::move(file_columns[in_file_column_counter]);
				in_file_column_counter++;
			}
		}
		auto unique_table = std::make_unique<cudf::table>(std::move(all_columns));
		return std::make_unique<ral::frame::BlazingTable>(std::move(unique_table), names);
	}
}

void CacheDataIO::set_names(const std::vector<std::string> & names) {
	this->schema.set_names(names);
}


} // namespace cache
} // namespace ral