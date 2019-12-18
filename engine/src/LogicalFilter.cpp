/*
 * LogicalFilter.cpp
 *
 *  Created on: Jul 20, 2018
 *      Author: felipe
 */

#include <deque>
#include <iostream>
#include <regex>
#include <vector>

#include "LogicalFilter.h"

#include "CalciteExpressionParsing.h"
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>

#include "CodeTimer.h"
#include "Traits/RuntimeTraits.h"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include <blazingdb/io/Library/Logging/Logger.h>

#include "Interpreter/interpreter_cpp.h"
#include "cudf/legacy/binaryop.hpp"
#include <cudf/utilities/legacy/nvcategory_util.hpp>

typedef struct {
	std::string token;
	column_index_type position;
} operand_position;

column_index_type get_first_open_position(std::vector<bool> & open_positions, column_index_type start_position) {
	for(column_index_type index = start_position; index < open_positions.size(); index++) {
		if(open_positions[index]) {
			open_positions[index] = false;
			return index;
		}
	}
	return -1;
}

gdf_column_cpp handle_cast_from_string(gdf_unary_operator operation, gdf_column * input_col) {
	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
	NVStrings * nv_strings =
		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

	gdf_dtype cast_type = get_output_type(GDF_STRING_CATEGORY, operation);

	// TODO Percy Rommel Jean Pierre improve timestamp resolution
	gdf_dtype_extra_info extra_info;
	extra_info.category = nullptr;
	extra_info.time_unit =
		(cast_type == GDF_TIMESTAMP ? TIME_UNIT_ms : TIME_UNIT_NONE);  // TODO this should not be hardcoded

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(
		cast_type, extra_info, input_col->size, nullptr, ral::traits::get_dtype_size_in_bytes(cast_type));

	switch(cast_type) {
	case GDF_INT32: nv_strings->stoi(static_cast<int *>(new_input_col.data())); break;
	case GDF_INT64: nv_strings->stol(static_cast<long *>(new_input_col.data())); break;
	case GDF_FLOAT32: nv_strings->stof(static_cast<float *>(new_input_col.data())); break;
	case GDF_FLOAT64: nv_strings->stod(static_cast<double *>(new_input_col.data())); break;
	case GDF_DATE32:
		nv_strings->timestamp2long("%Y-%m-%d", NVStrings::days, static_cast<unsigned long *>(new_input_col.data()));
		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_NONE;
		break;
	case GDF_DATE64:
		nv_strings->timestamp2long("%Y-%m-%d", NVStrings::ms, static_cast<unsigned long *>(new_input_col.data()));
		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_NONE;
		break;
	case GDF_TIMESTAMP:
		// TODO: Should know when use TIME_UNIT_ns
		nv_strings->timestamp2long(
			"%Y-%m-%dT%H:%M:%SZ", NVStrings::ms, static_cast<unsigned long *>(new_input_col.data()));
		new_input_col.get_gdf_column()->dtype_info.time_unit = TIME_UNIT_ms;
		break;
	default: assert(false);
	}

	NVStrings::destroy(nv_strings);

	if(input_col->null_count) {
		new_input_col.allocate_set_valid();
		CheckCudaErrors(cudaMemcpy(new_input_col.valid(),
			input_col->valid,
			gdf_valid_allocation_size(new_input_col.size()),
			cudaMemcpyDefault));
		new_input_col.get_gdf_column()->null_count = input_col->null_count;
	}

	return new_input_col;
}

gdf_column_cpp handle_cast_to_string(gdf_column * input_col) {
	NVStrings * nv_strings = nullptr;

	if(input_col->size > 0) {
		switch(input_col->dtype) {
		case GDF_INT32:
			nv_strings = NVStrings::itos(static_cast<int *>(input_col->data), input_col->size, input_col->valid);
			break;
		case GDF_INT64:
			nv_strings = NVStrings::ltos(static_cast<long *>(input_col->data), input_col->size, input_col->valid);
			break;
		case GDF_FLOAT32:
			nv_strings = NVStrings::ftos(static_cast<float *>(input_col->data), input_col->size, input_col->valid);
			break;
		case GDF_FLOAT64:
			nv_strings = NVStrings::dtos(static_cast<double *>(input_col->data), input_col->size, input_col->valid);
			break;
		case GDF_DATE32:
		case GDF_DATE64:
			nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
				input_col->size,
				NVStrings::days,
				"%Y-%m-%d",
				input_col->valid);
			break;
		case GDF_TIMESTAMP:
			if(input_col->dtype_info.time_unit == TIME_UNIT_ns) {
				nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
					input_col->size,
					NVStrings::ns,
					"%Y-%m-%dT%H:%M:%SZ",
					input_col->valid);
			} else {
				nv_strings = NVStrings::long2timestamp(static_cast<unsigned long *>(input_col->data),
					input_col->size,
					NVStrings::ms,
					"%Y-%m-%dT%H:%M:%SZ",
					input_col->valid);
			}
			break;
		default: assert(false);
		}
	}

	NVCategory * nv_category =
		nv_strings ? NVCategory::create_from_strings(*nv_strings) : NVCategory::create_from_array(nullptr, 0);
	NVStrings::destroy(nv_strings);

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(nv_category, nv_category->size(), "");

	return new_input_col;
}

int insert_string_into_column_nvcategory(gdf_column * col, const std::string & str) {
	const char * str_arr[] = {str.c_str()};
	NVStrings * temp_string = NVStrings::create_from_array(str_arr, 1);
	NVCategory * old_category = static_cast<NVCategory *>(col->dtype_info.category);
	NVCategory * new_category = old_category->add_strings(*temp_string);
	col->dtype_info.category = new_category;

	NVStrings::destroy(temp_string);
	NVCategory::destroy(old_category);

	CheckCudaErrors(cudaMemcpyAsync(
		col->data, new_category->values_cptr(), sizeof(gdf_nvstring_category) * col->size, cudaMemcpyDeviceToDevice));

	return new_category->get_value(str.c_str());
}

std::string like_expression_to_regex_str(const std::string & like_exp) {
	if(like_exp.empty()) {
		return like_exp;
	}

	bool match_start = like_exp[0] != '%';
	bool match_end = like_exp[like_exp.size() - 1] != '%';

	std::string re = like_exp;
	static const std::regex any_string_re{R"(([^\\]?|\\{2})%)"};
	re = std::regex_replace(re, any_string_re, "$1(?:.*?)");

	static const std::regex any_char_re{R"(([^\\]?|\\{2})_)"};
	re = std::regex_replace(re, any_char_re, "$1(?:.)");

	return (match_start ? "^" : "") + re + (match_end ? "$" : "");
}

gdf_column_cpp handle_match_regex(gdf_column * input_col, const std::string & re) {
	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
	NVStrings * nv_strings =
		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(GDF_BOOL8,
		gdf_dtype_extra_info{TIME_UNIT_NONE, nullptr},
		input_col->size,
		nullptr,
		nullptr,
		ral::traits::get_dtype_size_in_bytes(GDF_BOOL8));

	nv_strings->contains_re(re.c_str(), static_cast<bool *>(new_input_col.data()));

	NVStrings::destroy(nv_strings);

	return new_input_col;
}

gdf_column_cpp handle_substring(gdf_column * input_col, const std::string & str_params) {
	size_t pos = str_params.find(":");
	int start = std::max(std::stoi(str_params.substr(0, pos)), 1) - 1;
	int end = pos != std::string::npos ? start + std::stoi(str_params.substr(pos + 1)) : -1;

	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
	NVStrings * nv_strings =
		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

	NVStrings * new_strings = nv_strings->slice(start, end);
	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(new_category, new_category->size(), "");

	NVStrings::destroy(nv_strings);
	NVStrings::destroy(new_strings);

	return new_input_col;
}

gdf_column_cpp handle_concat_str_literal(gdf_column * input_col, const std::string & str, bool prefix = false) {
	std::vector<const char *> str_vec{(size_t) input_col->size, str.c_str()};
	NVStrings * temp_strings = NVStrings::create_from_array(str_vec.data(), str_vec.size());

	NVCategory * nv_category = static_cast<NVCategory *>(input_col->dtype_info.category);
	NVStrings * nv_strings =
		nv_category->gather_strings(static_cast<nv_category_index_type *>(input_col->data), input_col->size);

	NVStrings * new_strings = prefix ? temp_strings->cat(nv_strings, "") : nv_strings->cat(temp_strings, "");
	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(new_category, new_category->size(), "");

	NVStrings::destroy(temp_strings);
	NVStrings::destroy(nv_strings);
	NVStrings::destroy(new_strings);

	return new_input_col;
}

gdf_column_cpp handle_concat_str_col(gdf_column * left_input_col, gdf_column * right_input_col) {
	NVCategory * left_nv_category = static_cast<NVCategory *>(left_input_col->dtype_info.category);
	NVStrings * left_nv_strings = left_nv_category->gather_strings(
		static_cast<nv_category_index_type *>(left_input_col->data), left_input_col->size);

	NVCategory * right_nv_category = static_cast<NVCategory *>(right_input_col->dtype_info.category);
	NVStrings * right_nv_strings = right_nv_category->gather_strings(
		static_cast<nv_category_index_type *>(right_input_col->data), right_input_col->size);

	NVStrings * new_strings = left_nv_strings->cat(right_nv_strings, "");
	NVCategory * new_category = NVCategory::create_from_strings(*new_strings);

	gdf_column_cpp new_input_col;
	new_input_col.create_gdf_column(new_category, new_category->size(), "");

	NVStrings::destroy(left_nv_strings);
	NVStrings::destroy(right_nv_strings);
	NVStrings::destroy(new_strings);

	return new_input_col;
}

/**
 * Creates a physical plan for the expression that can be added to the total plan
 */
void add_expression_to_plan(blazing_frame & inputs,
	std::vector<gdf_column *> & input_columns,
	std::string expression,
	column_index_type expression_position,
	column_index_type num_outputs,
	column_index_type num_inputs,
	std::vector<column_index_type> & left_inputs,
	std::vector<column_index_type> & right_inputs,
	std::vector<column_index_type> & outputs,

	std::vector<gdf_binary_operator_exp> & operators,
	std::vector<gdf_unary_operator> & unary_operators,

	std::vector<gdf_scalar> & left_scalars,
	std::vector<gdf_scalar> & right_scalars,
	std::vector<column_index_type> & new_input_indices,

	std::vector<column_index_type> & final_output_positions,
	gdf_column * output_column) {
	column_index_type start_processing_position = num_inputs + num_outputs;

	std::string clean_expression = clean_calcite_expression(expression);

	std::deque<operand_position> operand_stack;
	gdf_scalar dummy_scalar;

	std::vector<column_index_type> src_str_col_map(512, -1);  // Used to find the original input nvcategory
	std::vector<bool> processing_space_free(
		512, true);  // a place to stare whether or not a processing space is occupied at any point in time
	for(size_t i = 0; i < start_processing_position; i++) {
		processing_space_free[i] = false;
	}
	// pretend they are like registers and we need to know how many registers we need to evaluate this expression

	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(inputs, tokens);

	for(size_t token_ind = 0; token_ind < tokens.size(); token_ind++) {
		std::string token = tokens[token_ind];

		if(is_operator_token(token)) {
			column_index_type src_str_col_idx = -1;  // column input index for GDF_STRING_CATEGORY operands
			bool new_input_col_added = false;

			if(is_binary_operator_token(token)) {
				std::string left_operand = operand_stack.back().token;
				if(!is_literal(left_operand)) {
					if(operand_stack.back().position >= start_processing_position) {
						processing_space_free[operand_stack.back().position] = true;
					}
				}
				operand_stack.pop_back();
				std::string right_operand = operand_stack.back().token;
				if(!is_literal(right_operand)) {
					if(operand_stack.back().position >= start_processing_position) {
						processing_space_free[operand_stack.back().position] = true;
					}
				}
				operand_stack.pop_back();

				gdf_binary_operator_exp operation = get_binary_operation(token);
				operators.push_back(operation);
				unary_operators.push_back(BLZ_INVALID_UNARY);

				if(is_literal(left_operand) && is_literal(right_operand)) {
					// both are literal have to deduce types, nuts
					// TODO: this is not working yet becuase we have to deduce the types..
					//					gdf_scalar left =
					// get_scalar_from_string(left_operand,inputs.get_column(right_index).dtype());
					//					left_scalars.push_back(left);
					//					gdf_scalar right =
					// get_scalar_from_string(right_operand,inputs.get_column(right_index).dtype());
					//					right_scalars.push_back(left);

					left_inputs.push_back(SCALAR_INDEX);  //
				} else if(is_literal(left_operand) && !is_string(left_operand)) {
					size_t right_index = get_index(right_operand);
					// TODO: remove get_type_from_string dirty fix
					// gdf_scalar right =
					// get_scalar_from_string(left_operand,inputs.get_column(get_index(right_operand)).dtype());

					// NOTE percy this is a literal so the extra info it doesnt matters
					gdf_dtype_extra_info extra_info;
					// TODO percy jp c.cordoba need more testing here in case used with other ops
					if(right_index < num_inputs) {
						extra_info = input_columns[right_index]->dtype_info;
					}

					gdf_scalar left =
						get_scalar_from_string(left_operand, get_type_from_string(left_operand), extra_info);
					left_scalars.push_back(left);
					right_scalars.push_back(dummy_scalar);

					left_inputs.push_back(left.is_valid ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					right_inputs.push_back(right_index);
				} else if(is_literal(right_operand) && !is_string(right_operand)) {
					size_t left_index = get_index(left_operand);
					// TODO: remove get_type_from_string dirty fix
					// gdf_scalar right =
					// get_scalar_from_string(right_operand,inputs.get_column(get_index(left_operand)).dtype());

					// NOTE percy this is a literal so the extra info it doesnt matters
					gdf_dtype_extra_info extra_info;
					// TODO percy jp c.cordoba need more testing here in case used with other ops
					if(left_index < num_inputs) {
						extra_info = input_columns[left_index]->dtype_info;
					}

					gdf_scalar right =
						get_scalar_from_string(right_operand, get_type_from_string(right_operand), extra_info);
					right_scalars.push_back(right);
					left_scalars.push_back(dummy_scalar);

					right_inputs.push_back(right.is_valid ? SCALAR_INDEX : SCALAR_NULL_INDEX);
					left_inputs.push_back(left_index);
				} else if(is_string(left_operand) || is_string(right_operand)) {
					std::string literal_operand = is_string(left_operand)
													  ? left_operand.substr(1, left_operand.size() - 2)
													  : right_operand.substr(1, right_operand.size() - 2);
					size_t left_index = is_string(left_operand) ? get_index(right_operand) : get_index(left_operand);
					column_index_type mapped_index = src_str_col_map[left_index];
					assert(mapped_index != -1);
					gdf_column * left_column = input_columns[mapped_index];

					if(operation == BLZ_STR_LIKE) {
						std::string regex = like_expression_to_regex_str(literal_operand);
						gdf_column_cpp new_input_col = handle_match_regex(left_column, regex);

						inputs.add_column(new_input_col);
						input_columns.push_back(new_input_col.get_gdf_column());

						left_index = num_inputs;
						new_input_col_added = true;

						right_scalars.push_back(dummy_scalar);
						left_scalars.push_back(dummy_scalar);
						right_inputs.push_back(SCALAR_NULL_INDEX);
						left_inputs.push_back(left_index);
					} else if(operation == BLZ_STR_SUBSTRING) {
						gdf_column_cpp new_input_col = handle_substring(left_column, literal_operand);

						inputs.add_column(new_input_col);
						input_columns.push_back(new_input_col.get_gdf_column());

						src_str_col_map[num_inputs] = num_inputs;
						src_str_col_idx = num_inputs;

						left_index = num_inputs;
						new_input_col_added = true;

						right_scalars.push_back(dummy_scalar);
						left_scalars.push_back(dummy_scalar);
						right_inputs.push_back(SCALAR_NULL_INDEX);
						left_inputs.push_back(left_index);
					} else if(operation == BLZ_STR_CONCAT) {
						gdf_column_cpp new_input_col =
							handle_concat_str_literal(left_column, literal_operand, is_string(left_operand));

						inputs.add_column(new_input_col);
						input_columns.push_back(new_input_col.get_gdf_column());

						src_str_col_map[num_inputs] = num_inputs;
						src_str_col_idx = num_inputs;

						left_index = num_inputs;
						new_input_col_added = true;

						right_scalars.push_back(dummy_scalar);
						left_scalars.push_back(dummy_scalar);
						right_inputs.push_back(SCALAR_NULL_INDEX);
						left_inputs.push_back(left_index);
					} else {
						int idx_position = static_cast<NVCategory *>(left_column->dtype_info.category)
											   ->get_value(literal_operand.c_str());
						if(idx_position == -1) {
							idx_position = insert_string_into_column_nvcategory(left_column, literal_operand);
						}
						assert(idx_position != -1);

						src_str_col_idx = mapped_index;

						gdf_data data;
						data.si32 = idx_position;
						gdf_scalar right = {data, GDF_INT32, true};
						right_scalars.push_back(right);
						left_scalars.push_back(dummy_scalar);
						right_inputs.push_back(right.is_valid ? SCALAR_INDEX : SCALAR_NULL_INDEX);
						left_inputs.push_back(left_index);
					}
				} else {
					size_t left_index = get_index(left_operand);
					size_t right_index = get_index(right_operand);

					column_index_type mapped_left_index = src_str_col_map[left_index];
					column_index_type mapped_right_index = src_str_col_map[right_index];
					src_str_col_idx = mapped_left_index >= 0 ? mapped_left_index : mapped_right_index;
					if(mapped_left_index >= 0 && mapped_right_index >= 0 && mapped_left_index != mapped_right_index) {
						gdf_column * left_column = input_columns[mapped_left_index];
						gdf_column * right_column = input_columns[mapped_right_index];

						if(operation == BLZ_STR_CONCAT) {
							gdf_column_cpp new_input_col = handle_concat_str_col(left_column, right_column);

							inputs.add_column(new_input_col);
							input_columns.push_back(new_input_col.get_gdf_column());

							src_str_col_map[num_inputs] = num_inputs;
							src_str_col_idx = num_inputs;

							left_index = num_inputs;
							new_input_col_added = true;
						} else {
							gdf_column * process_columns[2] = {left_column, right_column};
							gdf_column * output_columns[2] = {left_column, right_column};

							CUDF_CALL(sync_column_categories(process_columns, output_columns, 2));
						}
					}

					left_inputs.push_back(left_index);
					right_inputs.push_back(right_index);

					left_scalars.push_back(dummy_scalar);
					right_scalars.push_back(dummy_scalar);
				}
			} else if(is_unary_operator_token(token)) {
				std::string left_operand = operand_stack.back().token;
				if(!is_literal(left_operand)) {
					if(operand_stack.back().position >= start_processing_position) {
						processing_space_free[operand_stack.back().position] = true;
					}
				}
				operand_stack.pop_back();

				gdf_unary_operator operation = get_unary_operation(token);

				if(is_literal(left_operand)) {
					// TODO to be implemented
				} else {
					size_t left_index = get_index(left_operand);
					column_index_type mapped_left_index = src_str_col_map[left_index];

					if(operation == BLZ_CAST_VARCHAR) {
						if(left_index < num_inputs) {
							gdf_column * left_column = input_columns[left_index];
							if(left_column->dtype != GDF_STRING_CATEGORY) {
								gdf_column_cpp new_input_col = handle_cast_to_string(left_column);

								inputs.add_column(new_input_col);
								input_columns.push_back(new_input_col.get_gdf_column());

								src_str_col_map[num_inputs] = num_inputs;
								src_str_col_idx = num_inputs;

								left_index = num_inputs;
								new_input_col_added = true;
							} else {
								src_str_col_idx = src_str_col_map[left_index];
							}
						} else {
							throw std::runtime_error("Cast to String from intermediate results is not supported yet");
						}
					} else if(operation == BLZ_CAST_INTEGER || operation == BLZ_CAST_BIGINT ||
							  operation == BLZ_CAST_FLOAT || operation == BLZ_CAST_DOUBLE ||
							  operation == BLZ_CAST_DATE || operation == BLZ_CAST_TIMESTAMP) {
						if(mapped_left_index >= 0) {
							gdf_column * left_column = input_columns[mapped_left_index];
							gdf_column_cpp new_input_col = handle_cast_from_string(operation, left_column);

							inputs.add_column(new_input_col);
							input_columns.push_back(new_input_col.get_gdf_column());

							left_index = num_inputs;
							new_input_col_added = true;
						}
					}

					operators.push_back(BLZ_INVALID_BINARY);
					unary_operators.push_back(operation);

					left_inputs.push_back(left_index);
					right_inputs.push_back(-1);

					left_scalars.push_back(dummy_scalar);
					right_scalars.push_back(dummy_scalar);
				}
			}

			if(new_input_col_added) {
				new_input_indices.push_back(num_inputs);

				// Update plan to avoid collision between indices
				for(size_t i = 0; i < left_inputs.size() - 1; i++) {
					if(left_inputs[i] >= num_inputs) {
						left_inputs[i]++;
					}
					if(right_inputs[i] >= num_inputs) {
						right_inputs[i]++;
					}
				}
				auto max_iter = std::max_element(outputs.begin(), outputs.end());
				for(column_index_type i = (max_iter != outputs.end() ? *max_iter : -1); i > num_inputs; i--) {
					src_str_col_map[i + 1] = src_str_col_map[i];
					src_str_col_map[i] = -1;
				}
				for(size_t i = 0; i < outputs.size(); i++) {
					if(outputs[i] >= num_inputs) {
						outputs[i]++;
					}
				}
				for(size_t i = 0; i < final_output_positions.size(); i++) {
					if(final_output_positions[i] >= num_inputs) {
						final_output_positions[i]++;
					}
				}
				for(size_t i = 0; i < operand_stack.size(); i++) {
					column_index_type position = operand_stack[i].position;
					if(position >= num_inputs) {
						position++;
						operand_stack[i] = {"$" + std::to_string(position), position};
					}
				}

				num_inputs++;
				start_processing_position++;
				for(column_index_type i = start_processing_position; i < start_processing_position + outputs.size();
					i++) {
					processing_space_free[i] = false;
				}
			}

			if(token_ind == tokens.size() - 1) {  // last one
				// write to final output
				outputs.push_back(expression_position + num_inputs);
				if(output_column && output_column->dtype == GDF_STRING_CATEGORY) {
					assert(src_str_col_idx != -1);
					NVCategory::destroy(static_cast<NVCategory *>(output_column->dtype_info.category));
					gdf_column * src_col = input_columns[src_str_col_idx];
					output_column->dtype_info.category =
						static_cast<NVCategory *>(src_col->dtype_info.category)->copy();
				}
			} else {
				// write to temp output
				column_index_type output_position =
					get_first_open_position(processing_space_free, start_processing_position);
				outputs.push_back(output_position);
				// push back onto stack
				operand_stack.push_back({"$" + std::to_string(output_position), output_position});
				src_str_col_map[output_position] = src_str_col_idx;
			}
		} else {
			if(is_literal(token) || is_string(token)) {
				operand_stack.push_back({token, SCALAR_INDEX});
			} else {
				column_index_type mapped_idx = new_input_indices[get_index(token)];
				operand_stack.push_back({"$" + std::to_string(mapped_idx), mapped_idx});
				src_str_col_map[mapped_idx] =
					(input_columns[mapped_idx]->dtype == GDF_STRING_CATEGORY ? mapped_idx : -1);
			}
		}
	}
}


// processing in reverse we never need to have more than TWO spaces to work in
void evaluate_expression(blazing_frame & inputs, const std::string & expression, gdf_column_cpp & output) {
	// make temp a column of size 8 bytes so it can accomodate the largest possible size

	size_t num_columns = inputs.get_size_column();

	// special case when there is nothing to evaluate in the condition expression i.e. LogicalFilter(condition=[$16])
	if(expression[0] == '$') {
		size_t index = get_index(expression);
		if(index >= 0) {
			output = inputs.get_column(index).clone();
			return;
		}
	}

	std::string clean_expression = clean_calcite_expression(expression);

	std::vector<column_index_type> final_output_positions(1);
	std::vector<gdf_column *> output_columns(1);
	output_columns[0] = output.get_gdf_column();
	std::vector<gdf_column *> input_columns;

	std::vector<gdf_dtype> output_type_expressions(
		1);  // contains output types for columns that are expressions, if they are not expressions we skip over it
	output_type_expressions[0] = output.dtype();

	std::vector<bool> input_used_in_expression(inputs.get_size_column(), false);
	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(inputs, tokens);

	for(std::string token : tokens) {
		if(!is_operator_token(token) && !is_literal(token) && !is_string(token)) {
			size_t index = get_index(token);
			input_used_in_expression[index] = true;
		}
	}

	std::vector<column_index_type> left_inputs;
	std::vector<column_index_type> right_inputs;
	std::vector<column_index_type> outputs;

	std::vector<gdf_binary_operator_exp> operators;
	std::vector<gdf_unary_operator> unary_operators;


	std::vector<gdf_scalar> left_scalars;
	std::vector<gdf_scalar> right_scalars;

	std::vector<column_index_type> new_column_indices(input_used_in_expression.size());
	size_t input_columns_used = 0;
	for(int i = 0; i < input_used_in_expression.size(); i++) {
		if(input_used_in_expression[i]) {
			new_column_indices[i] = input_columns_used;
			input_columns.push_back(inputs.get_column(i).get_gdf_column());
			input_columns_used++;

		} else {
			new_column_indices[i] = -1;  // won't be uesd anyway
		}
	}

	final_output_positions[0] = input_columns_used;


	add_expression_to_plan(inputs,
		input_columns,
		expression,
		0,
		1,
		input_columns_used,
		left_inputs,
		right_inputs,
		outputs,
		operators,
		unary_operators,
		left_scalars,
		right_scalars,
		new_column_indices,
		final_output_positions);


	perform_operation(output_columns,
		input_columns,
		left_inputs,
		right_inputs,
		outputs,
		final_output_positions,
		operators,
		unary_operators,
		left_scalars,
		right_scalars,
		new_column_indices);

	// Remove any temp column added by add_expression_to_plan
	inputs.resize_num_columns(num_columns);

	output.update_null_count();
}
