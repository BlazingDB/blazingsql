#include <algorithm>
#include <cudf.h>
#include <cudf/table/table_view.hpp>
#include <iomanip>
#include <map>
#include <regex>

#include <blazingdb/io/Util/StringUtil.h>
#include "Utils.cuh"

#include "CalciteExpressionParsing.h"
#include "Traits/RuntimeTraits.h"
#include "cudf/legacy/binaryop.hpp"
#include <cudf/scalar/scalar_factories.hpp>
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
cudf::type_id get_aggregation_output_type(cudf::type_id input_type, AggregateKind aggregation, bool have_groupby) {
	if(aggregation == AggregateKind::COUNT_VALID || aggregation == AggregateKind::COUNT_ALL) {
		return cudf::type_id::INT64;
	} else if(aggregation == AggregateKind::SUM || aggregation == AggregateKind::SUM0) {
		if(have_groupby)
			return input_type;  // current group by function can only handle this
		else {
			// we can assume it is numeric based on the oepration
			// to be safe we should enlarge to the greatest integer or float representation
			return is_type_float(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
		}
	} else if(aggregation == AggregateKind::MIN) {
		return input_type;
	} else if(aggregation == AggregateKind::MAX) {
		return input_type;
	} else if(aggregation == AggregateKind::MEAN) {
		return cudf::type_id::FLOAT64;
	// TODO percy cudf0.12 aggregation pass flag for COUNT_DISTINCT cases
//	} else if(aggregation == GDF_COUNT_DISTINCT) {
//		return cudf::type_id::INT64;
	} else {
		throw std::runtime_error(
			"In get_aggregation_output_type function: aggregation type not supported: " + aggregation);
	}
}

cudf::type_id get_aggregation_output_type(cudf::type_id input_type, const std::string & aggregation) {
	if(aggregation == "COUNT") {
		return cudf::type_id::INT64;
	} else if(aggregation == "SUM" || aggregation == "$SUM0") {
		return is_type_float(input_type) ? cudf::type_id::FLOAT64 : cudf::type_id::INT64;
	} else if(aggregation == "MIN") {
		return input_type;
	} else if(aggregation == "MAX") {
		return input_type;
	} else if(aggregation == "AVG") {
		return cudf::type_id::FLOAT64;
	} else {
		throw std::runtime_error(
			"In get_aggregation_output_type function: aggregation type not supported: " + aggregation);
	}
}

cudf::type_id get_common_type(cudf::type_id type1, cudf::type_id type2) {
	
	if(type1 == type2) {
		return type1;		
	} else if((is_type_float(type1) && is_type_float(type2)) || (is_type_integer(type1) && is_type_integer(type2))) {
		return (ral::traits::get_dtype_size_in_bytes(type1) >= ral::traits::get_dtype_size_in_bytes(type2)) ? type1
																												: type2;
	} else if(is_date_type(type1) && is_date_type(type2)) { // if they are both datetime, return the highest resolution either has
		std::vector<cudf::type_id> datetime_types = {cudf::type_id::TIMESTAMP_NANOSECONDS, cudf::type_id::TIMESTAMP_MICROSECONDS,
						cudf::type_id::TIMESTAMP_MILLISECONDS, cudf::type_id::TIMESTAMP_SECONDS, cudf::type_id::TIMESTAMP_DAYS};
		for (auto datetime_type : datetime_types){
			if(type1 == datetime_type || type2 == datetime_type)
				return datetime_type;	
		}		
	} else if((type1 == cudf::type_id::STRING) &&
			  (type2 == cudf::type_id::STRING)) {
		return cudf::type_id::STRING;
	} 
	return cudf::type_id::EMPTY;	
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
		return cudf::type_id::TIMESTAMP_NANOSECONDS;
	} else if(is_string(token)) {
		return cudf::type_id::STRING;
	}

	RAL_FAIL("Invalid literal string");
}

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string) {
	cudf::type_id type_id = infer_dtype_from_literal(scalar_string);
	return get_scalar_from_string(scalar_string, type_id);
}

std::unique_ptr<cudf::scalar> get_scalar_from_string(const std::string & scalar_string, const cudf::type_id & type_id) {

	cudf::data_type type{type_id};

	if (type_id == cudf::type_id::EMPTY) {
		return nullptr;
	}
	if(type_id == cudf::type_id::BOOL8) {
		auto ret = cudf::make_numeric_scalar(type);
		using T = bool;
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
	if(type_id == cudf::type_id::TIMESTAMP_SECONDS || type_id == cudf::type_id::TIMESTAMP_MILLISECONDS 
		|| type_id == cudf::type_id::TIMESTAMP_MICROSECONDS || type_id == cudf::type_id::TIMESTAMP_NANOSECONDS) {
		if (scalar_string.find(":") != std::string::npos){
			return strings::str_to_timestamp_scalar(scalar_string, type, "%Y-%m-%d %H:%M:%S");
		} else {
			return strings::str_to_timestamp_scalar(scalar_string, type, "%Y-%m-%d");
		}
		
	}
	if(type_id == cudf::type_id::STRING)	{
		auto str_scalar = cudf::make_string_scalar(scalar_string.substr(1, scalar_string.length() - 2));
		str_scalar->set_valid(true); // https://github.com/rapidsai/cudf/issues/4085
		return str_scalar;
	}

	assert(false);
}

// must pass in temp type as invalid if you are not setting it to something to begin with
cudf::type_id get_output_type_expression(const cudf::table_view & table, std::string expression) {
	std::string clean_expression = clean_calcite_expression(expression);

	// TODO percy cudf0.12 was invalid here, should we consider empty?
	cudf::type_id max_temp_type = cudf::type_id::INT8;

	std::vector<std::string> tokens = get_tokens_in_reverse_order(clean_expression);
	fix_tokens_after_call_get_tokens_in_reverse_order_for_timestamp(table, tokens);

	std::stack<cudf::type_id> operands;
	for(std::string token : tokens) {
		if(is_operator_token(token)) {
			interops::operator_type operation = map_to_operator_type(token);
			if(is_binary_operator(operation)) {
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

				operands.push(get_output_type(left_operand, right_operand, operation));
				if(ral::traits::get_dtype_size_in_bytes(operands.top()) >
					ral::traits::get_dtype_size_in_bytes(max_temp_type)) {
					max_temp_type = operands.top();
				}
			} else if(is_unary_operator(operation)) {
				cudf::type_id left_operand = operands.top();
				operands.pop();

				operands.push(get_output_type(left_operand, operation));
				if(ral::traits::get_dtype_size_in_bytes(operands.top()) >
					ral::traits::get_dtype_size_in_bytes(max_temp_type)) {
					max_temp_type = operands.top();
				}
			}
		} else {
			if(is_literal(token)) {
				operands.push(infer_dtype_from_literal(token));
			} else {
				operands.push(table.column(get_index(token)).type().id());
			}
		}
	}
	return operands.top();
}

std::string get_aggregation_operation_string(std::string operator_string) {

	// lets check to see if its a full expression. If its not, we assume its the aggregator, so lets return that
	if (operator_string.find("=[") == std::string::npos && operator_string.find("(") == std::string::npos)
		return operator_string;

	operator_string = operator_string.substr(
		operator_string.find("=[") + 2, (operator_string.find("]") - (operator_string.find("=[") + 2)));

	// remove expression
	return operator_string.substr(0, operator_string.find("("));
}

AggregateKind get_aggregation_operation(std::string expression_in) {

	std::string operator_string = get_aggregation_operation_string(expression_in);
	std::string expression = get_string_between_outer_parentheses(expression_in);
	if (expression == "" && operator_string == "COUNT"){
		return AggregateKind::COUNT_ALL;
	} else if(operator_string == "SUM") {
		return AggregateKind::SUM;
	} else if(operator_string == "$SUM0") {
		return AggregateKind::SUM0;
	} else if(operator_string == "AVG") {
		return AggregateKind::MEAN;
	} else if(operator_string == "MIN") {
		return AggregateKind::MIN;
	} else if(operator_string == "MAX") {
		return AggregateKind::MAX;
	} else if(operator_string == "COUNT") {
		return AggregateKind::COUNT_VALID;
	}

	throw std::runtime_error(
		"In get_aggregation_operation function: aggregation type not supported, " + operator_string);
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
	assert(is_var_column(operand_string) || is_literal(operand_string));

	return std::stoi(is_literal(operand_string) ? operand_string : operand_string.substr(1, operand_string.size() - 1));
}

std::string aggregator_to_string(AggregateKind aggregation) {
	if(aggregation == AggregateKind::COUNT_VALID || aggregation == AggregateKind::COUNT_ALL) {
		return "count";
	} else if(aggregation == AggregateKind::SUM) {
		return "sum";
	} else if(aggregation == AggregateKind::SUM0) {
		return "sum0";
	} else if(aggregation == AggregateKind::MIN) {
		return "min";
	} else if(aggregation == AggregateKind::MAX) {
		return "max";
	} else if(aggregation == AggregateKind::MEAN) {
		return "avg";
	// TODO percy cudf0.12 aggregation pass flag for COUNT_DISTINCT cases
//	} else if(aggregation == GDF_COUNT_DISTINCT) {
//		return "count_distinct";
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
		int first_or = -1;

		std::string floor_str = "FLOOR";
		if (StringUtil::contains(expression, floor_str) == false) {
			first_or = StringUtil::findFirstNotInQuotes(expression, "OR(", start_pos, is_quoted_vector);  // returns -1 if not found
		}

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

bool contains_evaluation(std::string expression) { return expression.find("(") != std::string::npos; }


int count_string_occurrence(std::string haystack, std::string needle) {
	int position = haystack.find(needle, 0);
	int count = 0;
	while(position != std::string::npos) {
		count++;
		position = haystack.find(needle, position + needle.size());
	}

	return count;
}
