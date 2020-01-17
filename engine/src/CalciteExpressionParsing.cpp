#include <algorithm>
#include <cudf.h>
#include <cudf/table/table_view.hpp>
#include <iomanip>
#include <limits>
#include <map>
#include <regex>
#include <sstream>
#include <stack>

#include <blazingdb/io/Util/StringUtil.h>

#include "CalciteExpressionParsing.h"
#include "DataFrame.h"
#include "Traits/RuntimeTraits.h"
#include "cudf/legacy/binaryop.hpp"
#include "from_cudf/cpp_src/io/csv/legacy/datetime_parser.hpp"
#include "parser/expression_tree.hpp"
#include "utilities/scalar_timestamp_parser.hpp"

bool is_type_signed(cudf::type_id type) {
	return (cudf::type_id::INT8 == type || cudf::type_id::BOOL8 == type || cudf::type_id::INT16 == type ||
			cudf::type_id::INT32 == type || cudf::type_id::INT64 == type || cudf::type_id::FLOAT32 == type ||
			cudf::type_id::FLOAT64 == type || cudf::type_id::TIMESTAMP_DAYS == type ||
			cudf::type_id::TIMESTAMP_SECONDS == type || cudf::type_id::TIMESTAMP_MILLISECONDS == type ||
			cudf::type_id::TIMESTAMP_MICROSECONDS == type || cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}

bool is_type_float(cudf::type_id type) { return (cudf::type_id::FLOAT32 == type || cudf::type_id::FLOAT64 == type); }

bool is_type_integer(cudf::type_id type) {
	return (cudf::type_id::INT8 == type || cudf::type_id::INT16 == type || cudf::type_id::INT32 == type ||
			cudf::type_id::INT64 == type);
}

bool is_date_type(cudf::type_id type) {
	return (cudf::type_id::TIMESTAMP_DAYS == type || cudf::type_id::TIMESTAMP_SECONDS == type ||
			cudf::type_id::TIMESTAMP_MILLISECONDS == type || cudf::type_id::TIMESTAMP_MICROSECONDS == type ||
			cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}

// TODO percy noboa see upgrade to uints
bool is_numeric_type(cudf::type_id type) {
	// return is_type_signed(type) || is_type_unsigned_numeric(type);
	return is_type_signed(type);
}

cudf::type_id get_next_biggest_type(cudf::type_id type) {
	if(type == cudf::type_id::INT8 || type == cudf::type_id::BOOL8) {
		return cudf::type_id::INT16;
	} else if(type == cudf::type_id::INT16) {
		return cudf::type_id::INT32;
	} else if(type == cudf::type_id::INT32) {
		return cudf::type_id::INT64;
	} else if(type == cudf::type_id::FLOAT32) {
		return cudf::type_id::FLOAT64;
	} else {
		return type;
	}
}


// TODO all these return types need to be revisited later. Right now we have issues with some aggregators that only
// support returning the same input type. Also pygdf does not currently support unsigned types (for example count should
// return and unsigned type)
cudf::type_id get_aggregation_output_type(cudf::type_id input_type, gdf_agg_op aggregation, bool have_groupby) {
	if(aggregation == GDF_COUNT) {
		return cudf::type_id::INT64;
	} else if(aggregation == GDF_SUM) {
		if(have_groupby)
			return input_type;  // current group by function can only handle this
		else {
			// we can assume it is numeric based on the oepration
			// to be safe we should enlarge to the greatest integer or float representation
			return is_type_float(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
		}
	} else if(aggregation == GDF_MIN) {
		return input_type;
	} else if(aggregation == GDF_MAX) {
		return input_type;
	} else if(aggregation == GDF_AVG) {
		return cudf::type_id::FLOAT64;
	} else if(aggregation == GDF_COUNT_DISTINCT) {
		return cudf::type_id::INT64;
	} else {
		// TODO percy cudf0.12 was invalid here, is ok to return EMPTY?
		return cudf::type_id::EMPTY;
	}
}

bool is_exponential_operator(gdf_binary_operator_exp operation) { return operation == BLZ_POW; }

bool is_null_check_operator(gdf_unary_operator operation) {
	return (operation == BLZ_IS_NULL || operation == BLZ_IS_NOT_NULL);
}

bool is_arithmetic_operation(gdf_binary_operator_exp operation) {
	return (operation == BLZ_ADD || operation == BLZ_SUB || operation == BLZ_MUL || operation == BLZ_DIV ||
			operation == BLZ_MOD);
}

bool is_logical_operation(gdf_binary_operator_exp operation) {
	return (operation == BLZ_EQUAL || operation == BLZ_NOT_EQUAL || operation == BLZ_GREATER ||
			operation == BLZ_GREATER_EQUAL || operation == BLZ_LESS || operation == BLZ_LESS_EQUAL ||
			operation == BLZ_LOGICAL_OR);
}

bool is_trig_operation(gdf_unary_operator operation) {
	return (operation == BLZ_SIN || operation == BLZ_COS || operation == BLZ_ASIN || operation == BLZ_ACOS ||
			operation == BLZ_TAN || operation == BLZ_COTAN || operation == BLZ_ATAN);
}

cudf::type_id get_signed_type_from_unsigned(cudf::type_id type) {
	return type;
	// TODO felipe percy noboa see upgrade to uints
	//	if(type == GDF_UINT8){
	//		return GDF_INT16;
	//	}else if(type == GDF_UINT16){
	//		return GDF_INT32;
	//	}else if(type == GDF_UINT32){
	//		return GDF_INT64;
	//	}else if(type == GDF_UINT64){
	//		return GDF_INT64;
	//	}else{
	//		return GDF_INT64;
	//	}
}

cudf::type_id get_output_type(cudf::type_id input_left_type, gdf_unary_operator operation) {
	if(operation == BLZ_CAST_INTEGER) {
		return cudf::type_id::INT32;
	} else if(operation == BLZ_CAST_BIGINT) {
		return cudf::type_id::INT64;
	} else if(operation == BLZ_CAST_FLOAT) {
		return cudf::type_id::FLOAT32;
	} else if(operation == BLZ_CAST_DOUBLE) {
		return cudf::type_id::FLOAT64;
	} else if(operation == BLZ_CAST_DATE) {
		return cudf::type_id::TIMESTAMP_SECONDS;
	} else if(operation == BLZ_CAST_TIMESTAMP) {
		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		return cudf::type_id::TIMESTAMP_MILLISECONDS;
	} else if(operation == BLZ_CAST_VARCHAR) {
		return cudf::type_id::CATEGORY;
	} else if(is_date_type(input_left_type)) {
		return cudf::type_id::INT16;
	} else if(is_trig_operation(operation) || operation == BLZ_LOG || operation == BLZ_LN) {
		if(input_left_type == cudf::type_id::FLOAT32 || input_left_type == cudf::type_id::FLOAT64) {
			return input_left_type;
		} else {
			return cudf::type_id::FLOAT64;
		}
	} else if(is_null_check_operator(operation)) {
		return cudf::type_id::BOOL8;  // TODO: change to bools
	} else {
		return input_left_type;
	}
}

// todo: get_output_type: add support to coalesce and date operations!
cudf::type_id get_output_type(
	cudf::type_id input_left_type, cudf::type_id input_right_type, gdf_binary_operator_exp operation) {
	if(is_arithmetic_operation(operation)) {
		if(is_type_float(input_left_type) || is_type_float(input_right_type)) {
			// the output shoudl be ther largest float type
			if(is_type_float(input_left_type) && is_type_float(input_right_type)) {
				return (ral::traits::get_dtype_size_in_bytes(input_left_type) >=
						   ral::traits::get_dtype_size_in_bytes(input_right_type))
						   ? input_left_type
						   : input_right_type;
			} else if(is_type_float(input_left_type)) {
				return input_left_type;
			} else {
				return input_right_type;
			}
		}

		// ok so now we know we have now floating points left
		// so only things to worry about now are
		// if both are signed or unsigned, use largest type

		if((is_type_signed(input_left_type) && is_type_signed(input_right_type)) ||
			(!is_type_signed(input_left_type) && !is_type_signed(input_right_type))) {
			return (ral::traits::get_dtype_size_in_bytes(input_left_type) >=
					   ral::traits::get_dtype_size_in_bytes(input_right_type))
					   ? input_left_type
					   : input_right_type;
		}

		// now we know one is signed and the other isnt signed, if signed is larger we can just use signed version, if
		// unsigned is larger we have to use the signed version one step up e.g. an unsigned int32 requires and int64 to
		// represent all its numbers, unsigned int64 we are just screwed :)
		if(is_type_signed(input_left_type)) {
			// left signed
			// right unsigned
			if(ral::traits::get_dtype_size_in_bytes(input_left_type) >
				ral::traits::get_dtype_size_in_bytes(input_right_type)) {
				// great the left can represent the right
				return input_left_type;
			} else {
				// right type cannot be represented by left so we need to get a signed type big enough to represent the
				// unsigned right
				return get_signed_type_from_unsigned(input_right_type);
			}
		} else {
			// right signed
			// left unsigned
			if(ral::traits::get_dtype_size_in_bytes(input_left_type) <
				ral::traits::get_dtype_size_in_bytes(input_right_type)) {
				return input_right_type;
			} else {
				return get_signed_type_from_unsigned(input_left_type);
			}
		}

		// convert to largest type
		// if signed and unsigned convert to signed, upgrade unsigned if possible to determine size requirements
	} else if(is_logical_operation(operation)) {
		return cudf::type_id::BOOL8;
	} else if(is_exponential_operator(operation)) {
		// assume biggest type unsigned if left is unsigned, signed if left is signed

		if(is_type_float(input_left_type) || is_type_float(input_right_type)) {
			return cudf::type_id::FLOAT64;
			//		}else if(is_type_signed(input_left_type)){
			//			return GDF_INT64;
		} else {
			// TODO felipe percy noboa see upgrade to uints
			// return GDF_UINT64;
			return cudf::type_id::INT64;
		}
	} else if(operation == BLZ_MAGIC_IF_NOT) {
		return input_right_type;
	} else if(operation == BLZ_FIRST_NON_MAGIC) {
		if(is_numeric_type(input_left_type) && is_numeric_type(input_right_type)) {
			if(is_type_float(input_left_type) && !is_type_float(input_right_type)) {
				return input_left_type;
			} else if(!is_type_float(input_left_type) && is_type_float(input_right_type)) {
				return input_right_type;
			}
		}
		return (ral::traits::get_dtype_size_in_bytes(input_left_type) >=
				   ral::traits::get_dtype_size_in_bytes(input_right_type))
				   ? input_left_type
				   : input_right_type;
	} else if(operation == BLZ_STR_LIKE) {
		return cudf::type_id::BOOL8;
	} else if(operation == BLZ_STR_SUBSTRING || operation == BLZ_STR_CONCAT) {
		return cudf::type_id::CATEGORY;
	} else {
		// TODO percy cudf0.12 was invalid here, is correct to use empty?
		return cudf::type_id::EMPTY;
	}
}

void get_common_type(cudf::type_id type1, cudf::type_id type2, cudf::type_id & type_out) {
	// TODO percy cudf0.12 was invalid here, should we return empty?
	type_out = cudf::type_id::EMPTY;
	if(type1 == type2) {
		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		if(type1 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			type_out = type1;
		} else {
			type_out = type1;
		}
	} else if((is_type_float(type1) && is_type_float(type2)) || (is_type_integer(type1) && is_type_integer(type2))) {
		type_out = (ral::traits::get_dtype_size_in_bytes(type1) >= ral::traits::get_dtype_size_in_bytes(type2)) ? type1
																												: type2;
	} else if(type1 == cudf::type_id::TIMESTAMP_SECONDS || type1 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
		if(type2 == cudf::type_id::TIMESTAMP_MILLISECONDS || type2 == cudf::type_id::TIMESTAMP_SECONDS) {
			type_out = (ral::traits::get_dtype_size_in_bytes(type1) >= ral::traits::get_dtype_size_in_bytes(type2))
						   ? type1
						   : type2;
			// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
		} else if(type2 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			if(type1 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
				type_out = cudf::type_id::TIMESTAMP_MILLISECONDS;
			} else {
				type_out = cudf::type_id::TIMESTAMP_MILLISECONDS;
			}
		} else {
			// No common type, datetime type and non-datetime type are not compatible
		}
	} else if(type1 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
		if(type2 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			type_out = cudf::type_id::TIMESTAMP_MILLISECONDS;
		} else if(type2 == cudf::type_id::TIMESTAMP_MILLISECONDS) {
			type_out = cudf::type_id::TIMESTAMP_MILLISECONDS;
		} else {
			// No common type
		}
	} else if((type1 == cudf::type_id::STRING || type1 == cudf::type_id::CATEGORY) &&
			  (type2 == cudf::type_id::STRING || type2 == cudf::type_id::CATEGORY)) {
		type_out = cudf::type_id::CATEGORY;
	} else {
		// No common type
	}
}

cudf::type_id infer_dtype_from_literal(const std::string & token) {
	if(is_null(token)) {
		// TODO percy cudf0.12 was invalid here, should we return empty?
		return cudf::type_id::EMPTY;
	} else if(is_bool(token)) {
		return cudf::type_id::BOOL8;
	} else if(is_number(token)) {
		if(token.find_first_of(".eE") != std::string::npos) {
			double parsed_double = std::stod(token);
			float casted_float = static_cast<float>(parsed_double);
			return parsed_double == casted_float ? cudf::type_id::FLOAT32 : cudf::type_id::FLOAT64;
		} else {
			int64_t parsed_int64 = std::stoll(token);
			return parsed_int64 > std::numeric_limits<int32_t>::max() ||
						   parsed_int64 < std::numeric_limits<int32_t>::min()
					   ? cudf::type_id::INT64
					   : parsed_int64 > std::numeric_limits<int16_t>::max() ||
								 parsed_int64 < std::numeric_limits<int16_t>::min()
							 ? cudf::type_id::INT32
							 : parsed_int64 > std::numeric_limits<int8_t>::max() ||
									   parsed_int64 < std::numeric_limits<int8_t>::min()
								   ? cudf::type_id::INT16
								   : cudf::type_id::INT8;
		}
	} else if(is_date(token)) {
		return cudf::type_id::TIMESTAMP_DAYS;
	} else if(is_timestamp(token)) {
		return cudf::type_id::TIMESTAMP_MILLISECONDS;
	} else if(is_string(token)) {
		return cudf::type_id::CATEGORY;
	}

	RAL_FAIL("Invalid literal string");
}

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string) {
	cudf::type_id type_id = infer_dtype_from_literal(scalar_string);
	cudf::data_type type{type_id};

	if(type_id == cudf::type_id::EMPTY) {
		return nullptr;
	}
	if(type_id == cudf::type_id::BOOL8) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = cudf::experimental::bool8;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(scalar_string == "true"));
		return ret;
	}
	if(type_id == cudf::type_id::INT8) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = int8_t;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::INT16) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = int16_t;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::INT32) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = int32_t;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoi(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::INT64) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = int64_t;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stoll(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::FLOAT32) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = float;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stof(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::FLOAT64) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = double;
		using ScalarType = cudf::experimental::scalar_type_t<T>;
		static_cast<ScalarType *>(ret.get())->set_value(static_cast<T>(std::stod(scalar_string)));
		return ret;
	}
	if(type_id == cudf::type_id::TIMESTAMP_DAYS) {
		return strings::str_to_timestamp_scalar(scalar_string, type, "%Y-%m-%d");
	}
	if(type_id == cudf::type_id::TIMESTAMP_MILLISECONDS) {
		return strings::str_to_timestamp_scalar(scalar_string, type, "%Y-%m-%d %H:%M:%S");
	}

	assert(false);
}

// must pass in temp type as invalid if you are not setting it to something to begin with
cudf::type_id get_output_type_expression(blazing_frame * input, cudf::type_id * max_temp_type, std::string expression) {
	std::string clean_expression = clean_calcite_expression(expression);

	// TODO percy cudf0.12 was invalid here, should we consider empty?
	if(*max_temp_type == cudf::type_id::EMPTY) {
		*max_temp_type = cudf::type_id::INT8;
	}

	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	// TODO: percy fix this cudf 0.12 migration
	// fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(*input, tokens);

	std::stack<cudf::type_id> operands;
	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			if(is_binary_operator_token(token)) {
				if(operands.size() < 2)
					throw std::runtime_error(
						"In function get_output_type_expression, the operator cannot be processed on less than one or "
						"zero elements");

				cudf::type_id left_operand = operands.top();
				operands.pop();
				cudf::type_id right_operand = operands.top();
				operands.pop();

				// TODO percy cudf0.12 was invalid here, should we consider empty?
				if(left_operand == cudf::type_id::EMPTY) {
					if(right_operand == cudf::type_id::EMPTY) {
						throw std::runtime_error("In get_output_type_expression function: invalid operands");
					} else {
						left_operand = right_operand;
					}
				} else {
					if(right_operand == cudf::type_id::EMPTY) {
						right_operand = left_operand;
					}
				}
				gdf_binary_operator_exp operation = get_binary_operation(token);
				operands.push(get_output_type(left_operand, right_operand, operation));
				if(ral::traits::get_dtype_size_in_bytes(operands.top()) >
					ral::traits::get_dtype_size_in_bytes(*max_temp_type)) {
					*max_temp_type = operands.top();
				}
			} else if(is_unary_operator_token(token)) {
				cudf::type_id left_operand = operands.top();
				operands.pop();

				gdf_unary_operator operation = get_unary_operation(token);

				operands.push(get_output_type(left_operand, operation));
				if(ral::traits::get_dtype_size_in_bytes(operands.top()) >
					ral::traits::get_dtype_size_in_bytes(*max_temp_type)) {
					*max_temp_type = operands.top();
				}
			} else {
				throw std::runtime_error(
					"In get_output_type_expression function: unsupported operator token, " + token);
			}

		} else {
			if(is_literal(token)) {
				operands.push(infer_dtype_from_literal(token));
			} else {
				operands.push(input->get_column(get_index(token)).get_gdf_column()->type().id());
			}
		}
	}
	return operands.top();
}

gdf_agg_op get_aggregation_operation(std::string operator_string) {
	operator_string = operator_string.substr(
		operator_string.find("=[") + 2, (operator_string.find("]") - (operator_string.find("=[") + 2)));

	// remove expression
	operator_string = operator_string.substr(0, operator_string.find("("));
	if(operator_string == "SUM") {
		return GDF_SUM;
	} else if(operator_string == "AVG") {
		return GDF_AVG;
	} else if(operator_string == "MIN") {
		return GDF_MIN;
	} else if(operator_string == "MAX") {
		return GDF_MAX;
	} else if(operator_string == "COUNT") {
		return GDF_COUNT;
	} else if(operator_string == "COUNT_DISTINCT") {
		return GDF_COUNT_DISTINCT;
	}

	throw std::runtime_error(
		"In get_aggregation_operation function: aggregation type not supported, " + operator_string);
}


gdf_unary_operator get_unary_operation(std::string operator_string) {
	if(gdf_unary_operator_map.find(operator_string) != gdf_unary_operator_map.end())
		return gdf_unary_operator_map[operator_string];

	throw std::runtime_error("In get_unary_operation function: unsupported operator, " + operator_string);
}

gdf_binary_operator_exp get_binary_operation(std::string operator_string) {
	if(gdf_binary_operator_map.find(operator_string) != gdf_binary_operator_map.end())
		return gdf_binary_operator_map[operator_string];

	throw std::runtime_error("In get_binary_operation function: unsupported operator, " + operator_string);
}

std::vector<std::string> get_tokens_in_reverse_order(const std::string & expression) {
	std::vector<std::string> tokens = StringUtil::splitNotInQuotes(expression, " ");
	std::reverse(tokens.begin(), tokens.end());
	return tokens;
}

// TODO percy dirty hack ... fix this approach for timestamps
// out arg: tokens will be modified in case need a fix due timestamp
void fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(
	const cudf::table_view & inputs, std::vector<std::string> & tokens) {
	bool has_timestamp = false;
	for(auto && c : inputs) {
		if(is_date_type(c.type().id())) {
			has_timestamp = true;
			break;
		}
	}

	if(has_timestamp) {
		bool coms = false;
		for(int i = 0; i < tokens.size(); ++i) {
			auto tok = tokens.at(i);

			// static const std::regex re("'([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})'");
			// coms = std::regex_match(tok, re);

			if(StringUtil::contains(tok, "'")) {
				coms = true;
				break;
			}
		}

		if(coms) {
			for(int i = 0; i < tokens.size(); ++i) {
				auto tok = tokens.at(i);

				if(!StringUtil::contains(tok, "'"))
					continue;

				tok.erase(0, 1);
				tok.erase(tok.size() - 1, 1);

				if(is_timestamp(tok)) {
					tokens[i].erase(0, 1);
					tokens[i].erase(tokens.at(i).size() - 1, 1);
				}
			}
		}

		int idx_date = -1;
		size_t tokens_size = tokens.size();
		size_t count_occ = 0;
		for(int i = 0; i < tokens_size - count_occ; ++i) {
			auto tok = tokens.at(i);
			if(is_date(tok)) {
				idx_date = i;

				if(idx_date > 0 && is_hour(tokens[idx_date - 1])) {
					std::string date = tokens[idx_date];
					std::string ts_part = tokens[idx_date - 1];
					std::string ts = date + " " + ts_part;
					tokens.erase(tokens.begin() + idx_date);
					tokens[idx_date - 1] = ts;
					++count_occ;
				}
			}
		}
	}
}

cudf::size_type get_index(const std::string & operand_string) {
	if(operand_string.empty()) {
		return 0;
	}
	std::string cleaned_expression = clean_calcite_expression(operand_string);
	return std::stoi(is_literal(cleaned_expression) ? cleaned_expression
													: cleaned_expression.substr(1, cleaned_expression.size() - 1));
}

std::string aggregator_to_string(gdf_agg_op aggregation) {
	if(aggregation == GDF_COUNT) {
		return "count";
	} else if(aggregation == GDF_SUM) {
		return "sum";
	} else if(aggregation == GDF_MIN) {
		return "min";
	} else if(aggregation == GDF_MAX) {
		return "max";
	} else if(aggregation == GDF_AVG) {
		return "avg";
	} else if(aggregation == GDF_COUNT_DISTINCT) {
		return "count_distinct";
	} else {
		return "";  // FIXME: is really necessary?
	}
}

// interprets the expression and if is n-ary and logical, then returns their corresponding binary version
std::string expand_if_logical_op(std::string expression) {
	std::string output = expression;
	int start_pos = 0;

	while(start_pos < expression.size()) {
		std::vector<bool> is_quoted_vector = StringUtil::generateQuotedVector(expression);

		int first_and = StringUtil::findFirstNotInQuotes(
			expression, "AND(", start_pos, is_quoted_vector);  // returns -1 if not found
		int first_or = StringUtil::findFirstNotInQuotes(
			expression, "OR(", start_pos, is_quoted_vector);  // returns -1 if not found

		int first = -1;
		std::string op = "";
		if(first_and >= 0) {
			if(first_or >= 0 && first_or < first_and) {
				first = first_or;
				op = "OR(";
			} else {
				first = first_and;
				op = "AND(";
			}
		} else {
			first = first_or;
			op = "OR(";
		}

		if(first >= 0) {
			int expression_start = first + op.size() - 1;
			int expression_end = find_closing_char(expression, expression_start);

			std::string rest = expression.substr(expression_start + 1, expression_end - (expression_start + 1));
			// the trim flag is false because trimming the expressions cause malformmed ones
			std::vector<std::string> processed = get_expressions_from_expression_list(rest, false);

			if(processed.size() == 2) {  // is already binary
				start_pos = expression_start;
				continue;
			} else {
				start_pos = first;
			}

			output = expression.substr(0, first);
			for(size_t I = 0; I < processed.size() - 1; I++) {
				output += op;
				start_pos += op.size();
			}

			output += processed[0] + ",";
			for(size_t I = 1; I < processed.size() - 1; I++) {
				output += processed[I] + "),";
			}
			output += processed[processed.size() - 1] + ")";

			if(expression_end < expression.size() - 1) {
				output += expression.substr(expression_end + 1);
			}
			expression = output;
		} else {
			return output;
		}
	}

	return output;
}

std::string replace_calcite_regex(const std::string & expression) {
	std::string ret = expression;

	static const std::regex count_re{R""(COUNT\(DISTINCT (\W\(.+?\)|.+)\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, count_re, "COUNT_DISTINCT($1)");

	static const std::regex char_collate_re{
		R""((?:\(\d+\))? CHARACTER SET ".+?" COLLATE ".+?")"", std::regex_constants::icase};
	ret = std::regex_replace(ret, char_collate_re, "");

	static const std::regex timestamp_re{R""(TIMESTAMP\(\d+\))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, timestamp_re, "TIMESTAMP");

	static const std::regex number_implicit_cast_re{
		R""((\d):(?:DECIMAL\(\d+, \d+\)|INTEGER|BIGINT|FLOAT|DOUBLE))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, number_implicit_cast_re, "$1");

	static const std::regex null_implicit_cast_re{
		R""(null:(?:DECIMAL\(\d+, \d+\)|INTEGER|BIGINT|FLOAT|DOUBLE))"", std::regex_constants::icase};
	ret = std::regex_replace(ret, null_implicit_cast_re, "null");

	static const std::regex varchar_implicit_cast_re{R""(':VARCHAR)"", std::regex_constants::icase};
	ret = std::regex_replace(ret, varchar_implicit_cast_re, "'");

	StringUtil::findAndReplaceAll(ret, "IS NOT NULL", "IS_NOT_NULL");
	StringUtil::findAndReplaceAll(ret, "IS NULL", "IS_NULL");
	StringUtil::findAndReplaceAll(ret, " NOT NULL", "");

	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(YEAR), ", "BL_YEAR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MONTH), ", "BL_MONTH(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(DAY), ", "BL_DAY(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(HOUR), ", "BL_HOUR(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(MINUTE), ", "BL_MINUTE(");
	StringUtil::findAndReplaceAll(ret, "EXTRACT(FLAG(SECOND), ", "BL_SECOND(");
	StringUtil::findAndReplaceAll(ret, "FLOOR(", "BL_FLOUR(");

	StringUtil::findAndReplaceAll(ret, "/INT(", "/(");

	return ret;
}

std::string clean_calcite_expression(const std::string & expression) {
	std::string clean_expression = replace_calcite_regex(expression);

	ral::parser::parse_tree tree;
	tree.build(clean_expression);
	tree.transform_to_custom_op();
	clean_expression = tree.rebuildExpression();

	clean_expression = expand_if_logical_op(clean_expression);

	std::string new_string = "";
	new_string.reserve(clean_expression.size());

	for(int i = 0; i < clean_expression.size(); i++) {
		if(clean_expression[i] == '(') {
			new_string.push_back(' ');

		} else if(clean_expression[i] != ',' && clean_expression[i] != ')') {
			new_string.push_back(clean_expression.at(i));
		}
	}

	return new_string;
}

std::string get_string_between_outer_parentheses(std::string input_string) {
	int start_pos, end_pos;
	start_pos = input_string.find("(");
	end_pos = input_string.rfind(")");
	if(start_pos == input_string.npos || end_pos == input_string.npos || end_pos < start_pos) {
		return "";
	}
	start_pos++;
	// end_pos--;

	return input_string.substr(start_pos, end_pos - start_pos);
}

int find_closing_char(const std::string & expression, int start) {
	char openChar = expression[start];

	char closeChar = openChar;
	if(openChar == '(') {
		closeChar = ')';
	} else if(openChar == '[') {
		closeChar = ']';
	} else {
		// TODO throw error
		return -1;
	}

	int curInd = start + 1;
	int closePos = curInd;
	int depth = 1;
	bool inQuotes = false;

	while(curInd < expression.size()) {
		if(inQuotes) {
			if(expression[curInd] == '\'') {
				if(!(curInd + 1 < expression.size() &&
					   expression[curInd + 1] ==
						   '\'')) {  // if we are in quotes and we get a double single quotes, that is an escaped quotes
					inQuotes = false;
				}
			}
		} else {
			if(expression[curInd] == '\'') {
				inQuotes = true;
			} else if(expression[curInd] == openChar) {
				depth++;
			} else if(expression[curInd] == closeChar) {
				depth--;
				if(depth == 0) {
					return curInd;
				}
			}
		}
		curInd++;
	}
	// TODO throw error
	return -1;
}

// takes a comma delimited list of expressions and splits it into separate expressions
std::vector<std::string> get_expressions_from_expression_list(std::string & combined_expression, bool trim) {
	combined_expression = replace_calcite_regex(combined_expression);

	std::vector<std::string> expressions;

	int curInd = 0;
	int curStart = 0;
	bool inQuotes = false;
	int parenthesisDepth = 0;
	int sqBraketsDepth = 0;
	while(curInd < combined_expression.size()) {
		if(inQuotes) {
			if(combined_expression[curInd] == '\'') {
				if(!(curInd + 1 < combined_expression.size() &&
					   combined_expression[curInd + 1] ==
						   '\'')) {  // if we are in quotes and we get a double single quotes, that is an escaped quotes
					inQuotes = false;
				}
			}
		} else {
			if(combined_expression[curInd] == '\'') {
				inQuotes = true;
			} else if(combined_expression[curInd] == '(') {
				parenthesisDepth++;
			} else if(combined_expression[curInd] == ')') {
				parenthesisDepth--;
			} else if(combined_expression[curInd] == '[') {
				sqBraketsDepth++;
			} else if(combined_expression[curInd] == ']') {
				sqBraketsDepth--;
			} else if(combined_expression[curInd] == ',' && parenthesisDepth == 0 && sqBraketsDepth == 0) {
				std::string exp = combined_expression.substr(curStart, curInd - curStart);

				if(trim)
					expressions.push_back(StringUtil::ltrim(exp));
				else
					expressions.push_back(exp);

				curStart = curInd + 1;
			}
		}
		curInd++;
	}

	if(curStart < combined_expression.size() && curInd <= combined_expression.size()) {
		std::string exp = combined_expression.substr(curStart, curInd - curStart);

		if(trim)
			expressions.push_back(StringUtil::trim(exp));
		else
			expressions.push_back(exp);
	}

	return expressions;
}

bool contains_evaluation(std::string expression) { return expression.find("(") != std::string::npos; }
