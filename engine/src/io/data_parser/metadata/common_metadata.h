#ifndef METADATA_H_
#define METADATA_H_

#include "execution_graph/logic_controllers/LogicPrimitives.h"

std::unique_ptr<ral::frame::BlazingTable> makeMetadataFromCols(std::vector<std::string> col_names);

std::unique_ptr<cudf::column> make_cudf_column_from(
	cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size);

std::unique_ptr<cudf::column> make_empty_column(cudf::data_type type);

std::basic_string<char> get_typed_vector_content(
	cudf::type_id dtype, std::vector<int64_t> &vector);

#endif	// METADATA_H_
