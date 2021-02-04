#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_

#include "orc_metadata.h"
//#include "ExceptionHandling/BlazingThread.h"
#include "utilities/CommonOperations.h"
//#include <cudf/column/column_factories.hpp>


// TODO: this function is the same as the parquet_metadata::makeMetadataTable
// we just want one of them
std::unique_ptr<ral::frame::BlazingTable> makeMetadataORCTable(std::vector<std::string> col_names) {
	const int ncols = col_names.size();
	std::vector<std::string> metadata_col_names;
	// + 2: due to file_handle_index and stripe_index
	metadata_col_names.resize(ncols * 2 + 2);

	int metadata_col_index = -1;
	for (int colIndex = 0; colIndex < ncols; ++colIndex){
		std::string col_name = col_names[colIndex];
		std::string col_name_min = "min_" + std::to_string(colIndex) + "_" + col_name;
		std::string col_name_max = "max_" + std::to_string(colIndex)  + "_" + col_name;

		metadata_col_names[++metadata_col_index] = col_name_min;
		metadata_col_names[++metadata_col_index] = col_name_max;
	}

	metadata_col_names[++metadata_col_index] = "file_handle_index";
	metadata_col_names[++metadata_col_index] = "row_group_index"; // stripe_index

	std::vector<std::unique_ptr<cudf::column>> minmax_metadata_gdf_table;
	minmax_metadata_gdf_table.resize(metadata_col_names.size());
	for (std::size_t i = 0; i < metadata_col_names.size(); ++i) {
		std::vector<int32_t> temp{(int32_t)-1};
		std::unique_ptr<cudf::column> expected_col = ral::utilities::vector_to_column(temp, cudf::data_type(cudf::type_id::INT32));
		minmax_metadata_gdf_table[i] = std::move(expected_col);
	}

	auto cudf_metadata_table = std::make_unique<cudf::table>(std::move(minmax_metadata_gdf_table));
	auto metadata_table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_metadata_table), metadata_col_names);

	return metadata_table;
}

cudf::type_id to_dtype(cudf::io::statistics_type stat_type) {
	if (stat_type == cudf::io::statistics_type::INT) {
		return cudf::type_id::INT32;
	} else if (stat_type == cudf::io::statistics_type::STRING) {
		return cudf::type_id::STRING;
	} else if (stat_type == cudf::io::statistics_type::DOUBLE) {
		return cudf::type_id::FLOAT64;
	} else if (stat_type == cudf::io::statistics_type::BUCKET) {
		return cudf::type_id::BOOL8;
	} else if (stat_type == cudf::io::statistics_type::TIMESTAMP) {
		return cudf::type_id::TIMESTAMP_SECONDS;
	} else {
		return cudf::type_id::EMPTY;
	}
}

void set_min_max_string(
	std::vector<std::string> & minmax_string_metadata_table,
	cudf::io::column_statistics & statistic, int col_index) {

	auto type_stat = statistic.type_specific_stats<cudf::io::string_statistics>();
	std::string min = *type_stat->minimum();
	std::string max = *type_stat->maximum();
	minmax_string_metadata_table[col_index] = min;
	minmax_string_metadata_table[col_index + 1] = max;
}

void set_min_max(
	std::vector<int64_t> & minmax_metadata_table,
	cudf::io::column_statistics & statistic, int col_index) {
	// TODO: support for more dtypes
	if (statistic.type() == cudf::io::statistics_type::INT) {
		auto type_stat = statistic.type_specific_stats<cudf::io::integer_statistics>();
		auto min = type_stat->has_minimum() ? *type_stat->minimum() : std::numeric_limits<int32_t>::min();
		auto max = type_stat->has_maximum() ? *type_stat->maximum() : std::numeric_limits<int32_t>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (statistic.type() == cudf::io::statistics_type::DOUBLE) {
		auto type_stat = statistic.type_specific_stats<cudf::io::double_statistics>();
		auto min = type_stat->has_minimum() ? *type_stat->minimum() : std::numeric_limits<double>::min();
		auto max = type_stat->has_maximum() ? *type_stat->maximum() : std::numeric_limits<double>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (statistic.type() == cudf::io::statistics_type::BUCKET) {
		auto type_stat = statistic.type_specific_stats<cudf::io::bucket_statistics>();
		auto min = std::numeric_limits<bool>::min();
		auto max = std::numeric_limits<bool>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (statistic.type() == cudf::io::statistics_type::TIMESTAMP) {
		auto type_stat = statistic.type_specific_stats<cudf::io::timestamp_statistics>();
		auto min = type_stat->has_minimum() ? *type_stat->minimum() : std::numeric_limits<int64_t>::min();
		auto max = type_stat->has_maximum() ? *type_stat->maximum() : std::numeric_limits<int64_t>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else {
		throw std::runtime_error("Invalid statistic type in set_min_max");
	}
}

std::basic_string<char> get_typed_vector_str_content_(cudf::type_id dtype, std::vector<std::string> & vector) {
	std::basic_string<char> output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(char));
	return output;
}

std::basic_string<char> get_typed_vector_content_(cudf::type_id dtype, std::vector<int64_t> & vector) {
  	std::basic_string<char> output;
  	switch (dtype) {
	case cudf::type_id::INT8:{
			std::vector<char> typed_v(vector.begin(), vector.end());
			output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(char));
			break;
		}
	case cudf::type_id::INT16: {
		std::vector<int16_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int16_t));
		break;
	}
	case cudf::type_id::INT32:{
		std::vector<int32_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int32_t));
		break;
	}
	case cudf::type_id::INT64: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	case cudf::type_id::FLOAT64: {
		double* casted_metadata = reinterpret_cast<double*>(&(vector[0]));
		output = std::basic_string<char>((char *)casted_metadata, vector.size() * sizeof(double));
		break;
	}
	case cudf::type_id::BOOL8: {
		std::vector<int8_t> typed_v(vector.begin(), vector.end());
		output = std::basic_string<char>((char *)typed_v.data(), typed_v.size() * sizeof(int8_t));
		break;
	}
	case cudf::type_id::TIMESTAMP_SECONDS: {
		output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(int64_t));
		break;
	}
	default: {
		// default return type since we're throwing an exception.
		std::cerr << "Invalid gdf_dtype in create_host_column" << std::endl;
		throw std::runtime_error("Invalid gdf_dtype in create_host_column");
	}
  }
  return output;
}

std::unique_ptr<cudf::column> make_cudf_column_from_orc(cudf::data_type dtype, std::basic_string<char> &vector, unsigned long column_size) {
	size_t width_per_value = cudf::size_of(dtype); // TODO: issue when column is string
	if (vector.size() != 0) {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(vector.data(), buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, std::move(gpu_buffer));
	} else {
		auto buffer_size = width_per_value * column_size;
		rmm::device_buffer gpu_buffer(buffer_size);
		return std::make_unique<cudf::column>(dtype, column_size, buffer_size);
	}
}

std::vector<int64_t> get_all_values_in_the_same_col( 
	std::vector<std::vector<std::int64_t>> & min_max,
	std::size_t index) {
	std::vector<std::int64_t> output_v;
	for (std::size_t col_index; col_index < min_max.size(); ++col_index) {
		output_v.push_back(min_max[col_index][index]);
	}

	return output_v;
}

std::vector<std::string> get_all_str_values_in_the_same_col(
	std::vector<std::vector<std::string>> & minmax_string,
	std::size_t index) {
	std::vector<std::string> output_v;
	for (std::size_t col_index; col_index < minmax_string.size(); ++col_index) {
		output_v.push_back(minmax_string[col_index][index]);
	}

	return output_v;
}

std::unique_ptr<ral::frame::BlazingTable> get_minmax_orc_metadata(
    std::vector<cudf::io::parsed_orc_statistics> & orc_statistics,
    size_t total_stripes, int metadata_offset) {

	// if no parsed_orc_statistics
	if (orc_statistics.size() == 0) {
		return nullptr;
	}

	std::vector<std::string> metadata_names;
	std::vector<cudf::data_type> metadata_dtypes;
	std::vector<std::size_t> columns_with_metadata;
	std::vector<std::size_t> columns_with_string_metadata;

	// NOTE: we must try to use and load always an orc that contains at least one stripe
	int valid_orc_reader = -1;

	for (size_t i = 0; i < orc_statistics.size(); ++i) {
		if (orc_statistics[i].stripes_stats.size() == 0) {
			continue;
		}

		valid_orc_reader = i;
		break;
	}

	if (valid_orc_reader == -1) {
		// An additional `col_0` is always appended at the beginning
		std::vector<std::string> col_names = orc_statistics[0].column_names;
		col_names.erase(col_names.begin());

		return makeMetadataORCTable(col_names);
	}

	// Getting metadata and stats from the whole orc file
	// we are not filling data here, just getting the general Schema for the output table
	std::vector<cudf::io::column_statistics> & file_metadata = orc_statistics[valid_orc_reader].file_stats;
	std::vector<std::string> col_names = orc_statistics[valid_orc_reader].column_names;

	int num_stripes = orc_statistics[valid_orc_reader].stripes_stats.size();

	if (num_stripes > 0) {
		for (std::size_t colIndex = 0; colIndex < file_metadata.size(); colIndex++) {
			cudf::data_type dtype = cudf::data_type(to_dtype(file_metadata[colIndex].type())) ;
			if (file_metadata[colIndex].type() != cudf::io::statistics_type::NONE) {
				std::string col_name_min = "min_" + std::to_string(colIndex) + "_" + col_names[colIndex];
				metadata_names.push_back(col_name_min);
				metadata_dtypes.push_back(dtype);
				std::string col_name_max = "max_" + std::to_string(colIndex)  + "_" + col_names[colIndex];
				metadata_names.push_back(col_name_max);
				metadata_dtypes.push_back(dtype);

				if (file_metadata[colIndex].type() == cudf::io::statistics_type::STRING) {
					columns_with_string_metadata.push_back(colIndex);
				} 
				else columns_with_metadata.push_back(colIndex);
				
			}
		}
		
		metadata_dtypes.push_back(cudf::data_type{cudf::type_id::INT32});
		metadata_names.push_back("file_handle_index");
		metadata_dtypes.push_back(cudf::data_type{cudf::type_id::INT32});
		metadata_names.push_back("row_group_index"); // stripe_index
	}

	std::size_t total_cols_with_metadata = columns_with_string_metadata.size() + columns_with_metadata.size();
	// now we want to get min & max values (string in a separate matrix)
	std::vector<std::vector<int64_t>> minmax_metadata(total_stripes);
	std::vector<std::vector<std::string>> minmax_string_metadata(total_stripes);
	std::size_t file_str_count = 0;
	std::size_t file_not_str_count = 0;

	for (std::size_t file_index = 0; file_index < orc_statistics.size(); file_index++) {
		std::vector<std::vector<cudf::io::column_statistics>> & all_stats = orc_statistics[file_index].stripes_stats;
		std::size_t num_stripes = all_stats.size();
		if (num_stripes > 0) {
			std::vector<int64_t> this_minmax_metadata(columns_with_metadata.size() * 2 + 2);
			std::vector<std::string> this_minmax_metadata_string(columns_with_string_metadata.size() * 2);
			for (std::size_t stripe_index = 0; stripe_index < num_stripes; stripe_index++) {
				std::vector<cudf::io::column_statistics> & statistics_per_stripe = all_stats[stripe_index];
				// we are handling two separated minmax_metadas
				std::size_t string_count = 0;
				std::size_t not_string_count = 0;
				// due to the default first column `col_0`
				for (std::size_t col_count = 0; col_count < total_cols_with_metadata + 1; col_count++) {
					if (statistics_per_stripe[col_count].type() != cudf::io::statistics_type::NONE) {
						// when there is no string columns
						if (columns_with_string_metadata.size() == 0) {
							set_min_max(this_minmax_metadata, statistics_per_stripe[col_count], (col_count - 1) * 2);
						} else {
							if (statistics_per_stripe[col_count].type() == cudf::io::statistics_type::STRING) {
								set_min_max_string(this_minmax_metadata_string, statistics_per_stripe[col_count], string_count * 2);
								string_count++;
							} 
							else {
								set_min_max(this_minmax_metadata, statistics_per_stripe[col_count], not_string_count * 2);
								not_string_count++;
							}
						}
					}
				}

				this_minmax_metadata[this_minmax_metadata.size() - 2] = metadata_offset + file_index;
				this_minmax_metadata[this_minmax_metadata.size() - 1] = stripe_index;

				if (this_minmax_metadata.size() > 0) {
					minmax_metadata[file_not_str_count].insert(
						minmax_metadata[file_not_str_count].end(), this_minmax_metadata.begin(), this_minmax_metadata.end());
					file_not_str_count++;
				}
				if (this_minmax_metadata_string.size() > 0) {
					minmax_string_metadata[file_str_count].insert(
						minmax_string_metadata[file_str_count].end(), this_minmax_metadata_string.begin(), this_minmax_metadata_string.end());
					file_str_count++;
				}
			}
		}
	}

	std::vector<std::unique_ptr<cudf::column>> minmax_metadata_gdf_table(metadata_names.size());
	// we are handling two separated minmax_metadas
	std::size_t string_count = 0;
	std::size_t not_string_count = 0;
	std::basic_string<char> content;
	for (std::size_t index = 0; index < metadata_names.size(); index++) {
		cudf::data_type dtype = metadata_dtypes[index];
		if (dtype == cudf::data_type{cudf::type_id::STRING}) {
			std::vector<std::string> vector = get_all_str_values_in_the_same_col(minmax_string_metadata, string_count);
			string_count++;
			content = get_typed_vector_str_content_(dtype.id(), vector); // TODO review if this is handled correctecly
			dtype = cudf::data_type{cudf::type_id::INT32}; // TODO: avoid this ? Issue when no set up to int32
		} else {
			std::vector<int64_t> vector = get_all_values_in_the_same_col(minmax_metadata, not_string_count);
			not_string_count++;
			content = get_typed_vector_content_(dtype.id(), vector);
			//std::unique_ptr<cudf::column> expected_col2 = ral::utilities::vector_to_column(vector, dtype);
			//minmax_metadata_gdf_table[index] = std::move(expected_col2);
		}
		minmax_metadata_gdf_table[index] = make_cudf_column_from_orc(dtype, content, total_stripes);
	}

	auto table = std::make_unique<cudf::table>(std::move(minmax_metadata_gdf_table));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), metadata_names);
}

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_
