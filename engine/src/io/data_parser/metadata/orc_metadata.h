#ifndef ORC_METADATA_H_
#define ORC_METADATA_H_
        
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include <cudf/io/orc_metadata.hpp>

std::unique_ptr<ral::frame::BlazingTable> makeMetadataORCTable(std::vector<std::string> col_names);

cudf::type_id to_dtype(cudf::io::statistics_type stat_type);

void set_min_max_string(
	std::vector<std::vector<std::string>> & minmax_string_metadata_table,
	cudf::io::column_statistics & statistic,
    int col_index);

void set_min_max(
	std::vector<std::vector<int64_t>> & minmax_metadata_table,
	cudf::io::column_statistics & statistic,
    int col_index);

std::basic_string<char> get_typed_vector_str_content_(cudf::type_id dtype, std::vector<std::string> & vector);

std::basic_string<char> get_typed_vector_content_(cudf::type_id dtype, std::vector<int64_t> & vector);

std::unique_ptr<cudf::column> make_cudf_column_from_orc(
    cudf::data_type dtype,
    std::basic_string<char> &vector,
    unsigned long column_size);

std::vector<int64_t> get_all_values_in_the_same_col( 
	std::vector<std::vector<std::int64_t>> & min_max,
	std::size_t index);

std::vector<std::string> get_all_str_values_in_the_same_col(
	std::vector<std::vector<std::string>> & minmax_string,
	std::size_t index);

std::unique_ptr<ral::frame::BlazingTable> get_minmax_orc_metadata(
    std::vector<cudf::io::parsed_orc_statistics> & statistics,
    size_t total_stripes, int metadata_offset);

#endif	// ORC_METADATA_H_
