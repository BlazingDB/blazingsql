/*
 * DataFrame.h
 *
 *  Created on: Aug 7, 2018
 *      Author: felipe
 */

#ifndef DATAFRAME_H_
#define DATAFRAME_H_


#include "Utils.cuh"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include <GDFColumn.cuh>
#include <vector>

#include "cuDF/utilities/column_utilities.hpp"

typedef struct blazing_frame {
public:
	// @todo: constructor copia, operator =
	blazing_frame() : columns{} {}

	blazing_frame(const blazing_frame & other) : columns{other.columns} {}

	blazing_frame(blazing_frame && other) : columns{std::move(other.columns)} {}

	blazing_frame & operator=(const blazing_frame & other) {
		this->columns = other.columns;
		return *this;
	}

	blazing_frame & operator=(blazing_frame && other) {
		this->columns = std::move(other.columns);
		return *this;
	}

	cudf::size_type get_num_rows_in_table(int table_index) {
		if(table_index >= this->columns.size())
			return 0;
		else if(this->columns[table_index].size() == 0)
			return 0;
		else
			return this->columns[table_index][0].get_gdf_column()->size();
	}


	gdf_column_cpp & get_column(int column_index) {
		int cur_count = 0;
		for(std::size_t i = 0; i < columns.size(); i++) {
			if(column_index < cur_count + static_cast<int>(columns[i].size())) {
				return columns[i][column_index - cur_count];
			}
			cur_count += columns[i].size();
		}

		assert(false);
	}

	std::vector<std::vector<gdf_column_cpp>> & get_columns() { return columns; }

	std::vector<gdf_column_cpp> get_table(int table_index) { return columns[table_index]; }

	void add_table(std::vector<gdf_column_cpp> & columns_to_add) {
		columns.push_back(columns_to_add);
		if(columns_to_add.size() > 0) {
			// fill row_indeces with 0 to n
		}
	}

	void add_table(std::vector<gdf_column_cpp> && column_array) { columns.emplace_back(std::move(column_array)); }

	void set_column(size_t column_index, gdf_column_cpp column) {
		size_t cur_count = 0;
		for(std::size_t i = 0; i < columns.size(); i++) {
			if(column_index < cur_count + columns[i].size()) {
				columns[i][column_index - cur_count] = column;
				return;
			}

			cur_count += columns[i].size();
		}
	}

	void consolidate_tables() {
		std::vector<gdf_column_cpp> new_tables;
		for(std::size_t table_index = 0; table_index < columns.size(); table_index++) {
			new_tables.insert(new_tables.end(), columns[table_index].begin(), columns[table_index].end());
		}
		this->columns.resize(1);
		this->columns[0] = new_tables;
	}

	void add_column(gdf_column_cpp column_to_add, int table_index = 0) {
		columns[table_index].push_back(column_to_add);
	}

	void remove_table(size_t table_index) { columns.erase(columns.begin() + table_index); }

	void swap_table(std::vector<gdf_column_cpp> columns_to_add, size_t index) { columns[index] = columns_to_add; }

	size_t get_size_column(int table_index = 0) { return columns[table_index].size(); }

	size_t get_width() { return get_size_columns(); }

	size_t get_size_columns() {
		size_t size_columns = 0;
		for(std::size_t i = 0; i < columns.size(); i++) {
			size_columns += columns[i].size();
		}

		return size_columns;
	}

	void resize_num_columns(int new_column_size) {
		for(std::size_t i = 0; i < columns.size(); i++) {
			columns[i].resize(new_column_size);
		}
	}

	void clear() { this->columns.resize(0); }

	void empty_columns() {
		for(std::size_t i = 0; i < columns.size(); i++) {
			for(std::size_t j = 0; j < columns[i].size(); j++) {
				// TODO percy cudf0.12 port to cudf::column with auto ptrs
				//columns[i][j].resize(0);
			}
		}
	}

	void print(std::string title) {
		std::cout << "---> " << title << std::endl;
		for(std::size_t table_index = 0; table_index < columns.size(); table_index++) {
			std::cout << "Table: " << table_index << "\n";
			for(std::size_t column_index = 0; column_index < columns[table_index].size(); column_index++)
				cudf::test::print(*columns[table_index][column_index].get_gdf_column());
		}
	}

	// This function goes over all columns in the data frame and makes sure that no two columns are actually pointing to
	// the same data, and if so, clones the data so that they are all pointing to unique data pointers
	void deduplicate() {
		std::map<void *, std::pair<int, int>>
			dataPtrs;  // keys are the pointers, value is the table and column index it came from
		for(std::size_t table_index = 0; table_index < columns.size(); table_index++) {
			for(std::size_t column_index = 0; column_index < columns[table_index].size(); column_index++) {
				if(columns[table_index][column_index].get_gdf_column()->type().id() != cudf::type_id::EMPTY) {
					// TODO percy cudf0.12 port to cudf::column with auto ptrs
//					auto it = dataPtrs.find(columns[table_index][column_index].data());
//					if(it != dataPtrs.end()) {  // found a duplicate
//						columns[table_index][column_index] = columns[table_index][column_index].clone();
//					}
//					dataPtrs[columns[table_index][column_index].get_gdf_column()->data] =
//						std::make_pair(table_index, column_index);
				}
			}
		}
	}

	blazing_frame clone() {
		blazing_frame new_frame;
		for(auto table : this->get_columns()) {
			std::vector<gdf_column_cpp> table_columns;
			for(auto column : table) {
				table_columns.push_back(column.clone());
			}
			new_frame.add_table(table_columns);
		}
		return new_frame;
	}

private:
	std::vector<std::vector<gdf_column_cpp>> columns;
	// std::vector<gdf_column *> row_indeces; //per table row indexes used for materializing
} blazing_frame;


#endif /* DATAFRAME_H_ */
