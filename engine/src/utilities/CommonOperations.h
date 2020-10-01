#pragma once

#include <string>
#include <vector>
#include "execution_graph/logic_controllers/LogicPrimitives.h"
#include "cudf/column/column_factories.hpp"

namespace ral {
namespace utilities {

using namespace ral::frame;

bool checkIfConcatenatingStringsWillOverflow(const std::vector<BlazingTableView> & tables);
bool checkIfConcatenatingStringsWillOverflow(const std::vector<std::unique_ptr<BlazingTable>> & tables);

std::unique_ptr<BlazingTable> concatTables(const std::vector<BlazingTableView> & tables);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const std::vector<std::string> &column_names,
	const std::vector<cudf::data_type> &dtypes, std::vector<size_t> column_indices = std::vector<size_t>());

std::unique_ptr<cudf::table> create_empty_table(const std::vector<cudf::type_id> &dtypes);

std::unique_ptr<ral::frame::BlazingTable> create_empty_table(const BlazingTableView & table);

cudf::data_type get_common_type(cudf::data_type type1, cudf::data_type type2, bool strict);

std::vector<cudf::data_type> get_common_types(const std::vector<cudf::data_type> & types1, const std::vector<cudf::data_type> & types2, bool strict);

void normalize_types(std::unique_ptr<ral::frame::BlazingTable> & table,  const std::vector<cudf::data_type> & types,
	 		std::vector<cudf::size_type> column_indices = std::vector<cudf::size_type>() );

int64_t get_table_size_bytes(const ral::frame::BlazingTableView & table);


// This is only for numerics
template<typename T>
std::unique_ptr<cudf::column> vector_to_column(std::vector<T> vect, cudf::data_type type){
	return std::make_unique<cudf::column>(type, vect.size(), rmm::device_buffer{vect.data(), vect.size() * sizeof(T)});	
}

// This is only for numerics
template<typename T>
std::vector<T> vector_to_column(cudf::column_view column){
	std::vector<T> host_data(column.size());
  	CUDA_TRY(cudaMemcpy(host_data.data(), column.data<T>(), column.size() * sizeof(T), cudaMemcpyDeviceToHost));
	return host_data;
}








}  // namespace utilities
}  // namespace ral
