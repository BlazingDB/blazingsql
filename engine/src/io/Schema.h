/*
 * Schema.h
 *
 *  Created on: Apr 26, 2019
 *      Author: felipe
 */

#ifndef BLAZING_RAL_SCHEMA_H_
#define BLAZING_RAL_SCHEMA_H_


#include <cudf/cudf.h>
#include <string>
#include <vector>


#include "cudf/column/column_view.hpp"
#include "execution_graph/logic_controllers/LogicPrimitives.h"
namespace ral {

namespace io {

/**
 * I did not want to write this and its very dangerous
 * but the cudf::io::csv::reader_options (what a name) currently requires a char * input
 *I have no idea why
 */
std::string convert_dtype_to_string(const cudf::type_id & dtype);

class Schema {
public:
	Schema();

	Schema(std::vector<std::string> names,
		std::vector<size_t> calcite_to_file_indices,
		std::vector<cudf::type_id> types,
		std::vector<std::vector<int>> row_groups_ids = {}
		);

	Schema(std::vector<std::string> names,
		std::vector<size_t> calcite_to_file_indices,
		std::vector<cudf::type_id> types,
		std::vector<bool> in_file,
		std::vector<std::vector<int>> row_groups_ids = {});

	Schema(std::vector<std::string> names, std::vector<cudf::type_id> types);

	Schema(const Schema& ) = default;

	Schema& operator = (const Schema& ) = default;

	virtual ~Schema();

	std::vector<std::string> get_names() const;
	std::vector<std::string> get_types() const;
	std::vector<std::string> get_files() const;
	std::vector<bool> get_in_file() const;
	bool all_in_file() const;
	std::vector<cudf::type_id> get_dtypes() const;
	cudf::type_id get_dtype(size_t schema_index) const;
	std::string get_name(size_t schema_index) const;
	std::vector<size_t> get_calcite_to_file_indices() const { return this->calcite_to_file_indices; }
	Schema fileSchema(size_t current_file_index) const;
	
	size_t get_num_columns() const;

	std::vector<int> get_rowgroup_ids(size_t file_index) const { 
		if (this->row_groups_ids.size() > file_index){
			return this->row_groups_ids.at(file_index);
		} else {
			//if no metadata read, return a rowgroup/stripe representing all stripes/rowgroups
			return std::vector<int>{-1};
		}
	}
	
	void add_file(std::string file);

	void add_column(std::string name,
		cudf::type_id type,
		size_t file_index,
		bool is_in_file = true);

	std::unique_ptr<ral::frame::BlazingTable> makeEmptyBlazingTable(const std::vector<size_t> & column_indices) const;

	inline bool operator==(const Schema & rhs) const {
		return (this->names == rhs.names) && (this->types == rhs.types);
	}

	inline bool operator!=(const Schema & rhs) { return !(*this == rhs); }

private:
	std::vector<std::string> names;
	std::vector<size_t> calcite_to_file_indices;  // maps calcite columns to our columns
	std::vector<cudf::type_id> types;
	std::vector<bool> in_file;
	std::vector<std::string> files;
	
	std::vector<std::vector<int>> row_groups_ids;
};

} /* namespace io */
} /* namespace ral */

#endif /* BLAZING_RAL_SCHEMA_H_ */
