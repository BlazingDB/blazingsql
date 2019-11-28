/*
 * Schema.cpp
 *
 *  Created on: Apr 26, 2019
 *      Author: felipe
 */

#include "Schema.h"

namespace ral {
namespace io {



std::string convert_dtype_to_string(const gdf_dtype & dtype) {

	if(dtype == GDF_STRING)			return "str";
	if(dtype == GDF_DATE64)			return "date64";
	if(dtype == GDF_DATE32)			return "date32";
	if(dtype == GDF_TIMESTAMP)		return "timestamp";
	if(dtype == GDF_CATEGORY)		return "category";
	if(dtype == GDF_FLOAT32)		return "float32";
	if(dtype == GDF_FLOAT64)		return "float64";
	if(dtype == GDF_INT16)			return "short";
	if(dtype == GDF_INT32)			return "int32";
	if(dtype == GDF_INT64)			return "int64";
	if(dtype == GDF_BOOL8)			return "bool";

	return "str";
}

Schema::Schema(	std::vector<std::string> names,
		std::vector<size_t> calcite_to_file_indices,
		std::vector<gdf_dtype> types,
		std::vector<gdf_time_unit> time_units,
		std::vector<size_t> num_row_groups) : names(names),calcite_to_file_indices(calcite_to_file_indices), types(types), time_units(time_units), num_row_groups(num_row_groups) {
	// TODO Auto-generated constructor stub

	in_file.resize(names.size(),true);
}

Schema::Schema(	std::vector<std::string> names,
			std::vector<size_t> calcite_to_file_indices,
			std::vector<gdf_dtype> types,
			std::vector<size_t> num_row_groups,
			std::vector<gdf_time_unit> time_units,
			std::vector<bool> in_file) : names(names),calcite_to_file_indices(calcite_to_file_indices), types(types), num_row_groups(num_row_groups), time_units(time_units), in_file(in_file) {

			if(in_file.size() != names.size()){
					this->in_file.resize(names.size(),true);
			}
}

Schema::Schema(	std::vector<std::string> names,
			std::vector<gdf_dtype> types, std::vector<gdf_time_unit> time_units) : names(names),calcite_to_file_indices({}), types(types), time_units(time_units), num_row_groups({}) {

	in_file.resize(names.size(),true);
}

Schema::Schema() : names({}),calcite_to_file_indices({}), types({}), num_row_groups({}) {

}



Schema::~Schema() {
	// TODO Auto-generated destructor stub
}

std::vector<std::string> Schema::get_names() const{
	return this->names;
}

std::vector<std::string> Schema::get_types() const{
	std::vector<std::string> string_types;
	for(int i = 0; i < this->types.size(); i++){
		string_types.push_back(convert_dtype_to_string(this->types[i]));
	}
	return string_types;
}

std::vector<gdf_dtype> Schema::get_dtypes() const{
	return this->types;
}

std::vector<gdf_time_unit> Schema::get_time_units() const{
	return this->time_units;
}

std::string Schema::get_name(size_t schema_index) const{
	return this->names[schema_index];
}

std::string Schema::get_type(size_t schema_index) const{
	return convert_dtype_to_string(this->types[schema_index]);
}

size_t Schema::get_file_index(size_t schema_index) const{
	if(this->calcite_to_file_indices.size() == 0){
		return schema_index;
	}
	return this->calcite_to_file_indices[schema_index];
}

size_t Schema::get_num_row_groups(size_t file_index) const{
	return this->num_row_groups[file_index];
}

size_t Schema::get_num_columns() const {
	return this->names.size();
}

std::vector<bool> Schema::get_in_file() const {
	return this->in_file;
}
void Schema::add_column(gdf_column_cpp column,size_t file_index){
	this->names.push_back(column.name());
	this->types.push_back(column.dtype());
	this->time_units.push_back(column.dtype_info().time_unit);
	this->calcite_to_file_indices.push_back(file_index);
	this->in_file.push_back(true);
}

void Schema::add_column(std::string name, gdf_dtype type, size_t file_index, bool is_in_file, gdf_time_unit time_unit){
	this->names.push_back(name);
	this->types.push_back(type);
	this->time_units.push_back(time_unit);
	this->calcite_to_file_indices.push_back(file_index);
	this->in_file.push_back(is_in_file);
}

Schema Schema::fileSchema() const{
	Schema schema;
	//std::cout<<"in_file size "<<this->in_file.size()<<std::endl;
	for(int i = 0; i < this->names.size(); i++){
		size_t file_index = this->calcite_to_file_indices.size() == 0 ? i : this->calcite_to_file_indices[i];
		if(this->in_file[i]){
			schema.add_column(this->names[i],this->types[i],file_index);
		}

	}
		return schema;
}

} /* namespace io */
} /* namespace ral */
