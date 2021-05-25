#ifndef BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_
#define BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_

#include "orc_metadata.h"
#include "utilities/CommonOperations.h"

#include <cudf/column/column_factories.hpp>
#include <cudf/detail/utilities/vector_factories.hpp>
#include <cudf/io/orc_metadata.hpp>

#include <numeric>

std::basic_string<char> get_typed_vector_str_content(cudf::type_id dtype, std::vector<std::string> & vector) {
	std::basic_string<char> output = std::basic_string<char>((char *)vector.data(), vector.size() * sizeof(char));
	return output;
}

std::vector<int64_t> get_all_values_in_the_same_col( 
	std::vector<std::vector<std::int64_t>> & min_max,
	std::size_t index) {
	std::vector<std::int64_t> output_v;
	for (std::size_t col_index=0; col_index < min_max.size(); ++col_index) {
		output_v.push_back(min_max[col_index][index]);
	}

	return output_v;
}

std::vector<std::string> get_all_str_values_in_the_same_col(
	std::vector<std::vector<std::string>> & minmax_string,
	std::size_t index) {
	std::vector<std::string> output_v;
	for (std::size_t col_index=0; col_index < minmax_string.size(); ++col_index) {
		output_v.push_back(minmax_string[col_index][index]);
	}

	return output_v;
}

bool type_statistic_valid(cudf::io::column_statistics cs) {
	if ( std::holds_alternative<cudf::io::no_statistics>(cs.type_specific_stats) ||
		 std::holds_alternative<cudf::io::decimal_statistics>(cs.type_specific_stats) ||
		 std::holds_alternative<cudf::io::binary_statistics>(cs.type_specific_stats) ) {
		return false;
	}

	return true;
}

cudf::type_id statistic_to_dtype(cudf::io::column_statistics cs) {
	auto aa = cs.type_specific_stats;
	if (std::holds_alternative<cudf::io::integer_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::INT32;
	} else if (std::holds_alternative<cudf::io::string_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::STRING;
	} else if (std::holds_alternative<cudf::io::double_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::FLOAT64;
	} else if (std::holds_alternative<cudf::io::bucket_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::BOOL8;
	} else if (std::holds_alternative<cudf::io::timestamp_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::TIMESTAMP_NANOSECONDS;
	} else if (std::holds_alternative<cudf::io::date_statistics>(cs.type_specific_stats)) {
		return cudf::type_id::TIMESTAMP_DAYS;
	// TODO: Not support for decimal type for now
	//} else if (std::holds_alternative<cudf::io::decimal_statistics>(cs.type_specific_stats) {
	//	return cudf::type_id::DECIMAL64;
	} else {
		return cudf::type_id::EMPTY;
	}
}

std::pair< std::vector<char>, std::vector<cudf::size_type> > concat_strings(
	std::vector<std::string> & vector) {
	std::vector<char> chars;
	std::vector<cudf::size_type> offsets(1, 0); // the first offset value must be 0

	for (std::size_t i = 0; i < vector.size(); i++) {
		offsets.push_back(vector[i].size());
        chars.insert(chars.end(), vector[i].begin(), vector[i].end());   
    }

	offsets[offsets.size() - 1] = chars.size();

	return std::make_pair(chars, offsets);
}

void set_min_max_string(
	std::vector<std::string> & minmax_string_metadata_table,
	cudf::io::column_statistics & statistic, int col_index) {

	auto& ts = std::get<cudf::io::string_statistics>(statistic.type_specific_stats);
	std::string min = *ts.minimum;
	std::string max = *ts.maximum;
	minmax_string_metadata_table[col_index] = min;
	minmax_string_metadata_table[col_index + 1] = max;
}

void set_min_max(
	std::vector<int64_t> & minmax_metadata_table,
	cudf::io::column_statistics & statistic, int col_index) {
	// TODO: support for more dtypes
	if (std::holds_alternative<cudf::io::integer_statistics>(statistic.type_specific_stats)) {
		auto& ts = std::get<cudf::io::integer_statistics>(statistic.type_specific_stats);
		auto min = ts.minimum ? *ts.minimum : std::numeric_limits<int32_t>::min();
		auto max = ts.maximum ? *ts.maximum : std::numeric_limits<int32_t>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (std::holds_alternative<cudf::io::double_statistics>(statistic.type_specific_stats)) {
		int64_t dummy = 0;
		minmax_metadata_table[col_index] = dummy;
		minmax_metadata_table[col_index + 1] = dummy;
		auto& ts = std::get<cudf::io::double_statistics>(statistic.type_specific_stats);
		double min = ts.minimum ? *ts.minimum : std::numeric_limits<double>::min();
		double max = ts.maximum ? *ts.maximum : std::numeric_limits<double>::max();
		// here we want to reinterpret cast minmax_metadata_table to be double so that we can just use this same vector as if they were double
		size_t current_row_index = minmax_metadata_table.size() - 1;
		double* casted_metadata_min = reinterpret_cast<double*>(&(minmax_metadata_table[col_index]));
		double* casted_metadata_max = reinterpret_cast<double*>(&(minmax_metadata_table[col_index + 1]));
		casted_metadata_min[0] = min;
		casted_metadata_max[0] = max;
	} else if (std::holds_alternative<cudf::io::bucket_statistics>(statistic.type_specific_stats)) {
		auto& ts = std::get<cudf::io::bucket_statistics>(statistic.type_specific_stats);
		auto min = std::numeric_limits<bool>::min();
		auto max = std::numeric_limits<bool>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (std::holds_alternative<cudf::io::timestamp_statistics>(statistic.type_specific_stats)) {
		auto& ts = std::get<cudf::io::timestamp_statistics>(statistic.type_specific_stats);
		auto min = ts.minimum ? *ts.minimum : std::numeric_limits<int64_t>::min();
		auto max = ts.maximum ? *ts.maximum : std::numeric_limits<int64_t>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (std::holds_alternative<cudf::io::date_statistics>(statistic.type_specific_stats)) {
		auto& ts = std::get<cudf::io::date_statistics>(statistic.type_specific_stats);
		auto min = ts.minimum ? *ts.minimum : std::numeric_limits<int32_t>::min();
		auto max = ts.maximum ? *ts.maximum : std::numeric_limits<int32_t>::max();
		minmax_metadata_table[col_index] = min;
		minmax_metadata_table[col_index + 1] = max;
	} else if (std::holds_alternative<cudf::io::decimal_statistics>(statistic.type_specific_stats)) {
		throw std::runtime_error("ERROR: currently not supported statistic type for DECIMAL in ORC set_min_max");
	} else if (std::holds_alternative<cudf::io::binary_statistics>(statistic.type_specific_stats)) {
		throw std::runtime_error("ERROR: currently not supported statistic type for BINARY in ORC set_min_max");
	} else {
		throw std::runtime_error("ERROR: not supported statistic type for NONE in ORC set_min_max");
	}
}

bool is_decimal_or_empty_dtype(cudf::type_id type_id) {
	if (type_id == cudf::type_id::DECIMAL32 || type_id == cudf::type_id::DECIMAL64 || type_id == cudf::type_id::EMPTY) {
		return true;
	}

	return false;
}

std::unique_ptr<ral::frame::BlazingTable> get_minmax_metadata(
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

		return make_dummy_metadata_table_from_col_names(col_names);
	}

	// Getting metadata and stats from the whole orc file
	// we are not filling data here, just getting the general Schema for the output table
	std::vector<cudf::io::column_statistics> & file_metadata = orc_statistics[valid_orc_reader].file_stats;
	std::vector<std::string> col_names = orc_statistics[valid_orc_reader].column_names;

	int num_stripes = orc_statistics[valid_orc_reader].stripes_stats.size();

	if (num_stripes > 0) {
		for (std::size_t colIndex = 0; colIndex < file_metadata.size(); colIndex++) {
			cudf::data_type dtype = cudf::data_type(statistic_to_dtype(file_metadata[colIndex]));
			if ( type_statistic_valid(file_metadata[colIndex]) ) {
				// -1: to match with the project columns when calling skipdata
				std::string col_name_min = "min_" + std::to_string(colIndex - 1) + "_" + col_names[colIndex];
				metadata_names.push_back(col_name_min);
				metadata_dtypes.push_back(dtype);
				std::string col_name_max = "max_" + std::to_string(colIndex - 1) + "_" + col_names[colIndex];
				metadata_names.push_back(col_name_max);
				metadata_dtypes.push_back(dtype);

				if (std::holds_alternative<cudf::io::string_statistics>(file_metadata[colIndex].type_specific_stats)) {
					columns_with_string_metadata.push_back(colIndex);
				} 
				else columns_with_metadata.push_back(colIndex);
			}
		}
		
		metadata_dtypes.push_back(cudf::data_type{cudf::type_id::INT32});
		metadata_names.push_back("file_handle_index");
		metadata_dtypes.push_back(cudf::data_type{cudf::type_id::INT32});
		metadata_names.push_back("row_group_index");  // stripe_index in case of ORC
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
					if ( !std::holds_alternative<cudf::io::no_statistics>(statistics_per_stripe[col_count].type_specific_stats) ) {
						// when there is no string columns
						if (columns_with_string_metadata.size() == 0) {
							set_min_max(this_minmax_metadata, statistics_per_stripe[col_count], (col_count - 1) * 2);
						} else {
							if ( std::holds_alternative<cudf::io::string_statistics>(statistics_per_stripe[col_count].type_specific_stats)) {
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
	for (std::size_t index = 0; index < metadata_names.size(); index++) {
		cudf::data_type dtype = metadata_dtypes[index];
		if (is_decimal_or_empty_dtype(dtype.id())) {
			continue;
		} // we need to handle `strings` in a different way
		else if (dtype == cudf::data_type{cudf::type_id::STRING}) {
			std::vector<std::string> vector_str = get_all_str_values_in_the_same_col(minmax_string_metadata, string_count);
			string_count++;
			std::pair<std::vector<char>, std::vector<cudf::size_type>> result_pair = concat_strings(vector_str);
			auto d_chars = cudf::detail::make_device_uvector_sync(result_pair.first);
			auto d_offsets = cudf::detail::make_device_uvector_sync(result_pair.second);
			std::unique_ptr<cudf::column> col = cudf::make_strings_column(d_chars, d_offsets, {}, 0);
			minmax_metadata_gdf_table[index] = std::move(col);
		} else {
			std::vector<int64_t> vector = get_all_values_in_the_same_col(minmax_metadata, not_string_count);
			not_string_count++;
			std::basic_string<char> content = get_typed_vector_content(dtype.id(), vector);
			minmax_metadata_gdf_table[index] = make_cudf_column_from_vector(dtype, content, total_stripes);
		}
	}

	auto table = std::make_unique<cudf::table>(std::move(minmax_metadata_gdf_table));
	return std::make_unique<ral::frame::BlazingTable>(std::move(table), metadata_names);
}

#endif	// BLAZINGDB_RAL_SRC_IO_DATA_PARSER_METADATA_ORC_METADATA_CPP_H_
