#pragma once

#include <cassert>
#include <cuda_runtime.h>
#include <cudf/column/column_device_view.cuh>
#include <cudf/datetime.hpp>
#include <cudf/scalar/scalar_device_view.cuh>
#include <cudf/strings/string_view.cuh>
#include <cudf/table/table_device_view.cuh>
#include <cudf/types.hpp>
#include <cudf/utilities/bit.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <limits>
#include <numeric>
#include <simt/chrono>
#include <type_traits>

#include <curand_kernel.h>

#include "interpreter_cpp.h"
#include "error.hpp"

namespace interops {


typedef int64_t temp_gdf_valid_type;  // until its an int32 in cudf
typedef int16_t column_index_type;

template <typename T>
CUDA_DEVICE_CALLABLE T getMagicNumber() {
	return T{};
}

template <>
CUDA_DEVICE_CALLABLE int64_t getMagicNumber<int64_t>() {
	return std::numeric_limits<int64_t>::max() - 13ll;
}

template <>
CUDA_DEVICE_CALLABLE double getMagicNumber<double>() {
	return 1.7976931348623123e+308;
}

__global__ void setup_rand_kernel(curandState *state, unsigned long long seed )
{
    int id = threadIdx.x + blockIdx.x * blockDim.x;
    /* Each thread gets same seed, a different sequence
       number, no offset */
    curand_init(seed, id, 0, &state[id]);
}




using cudf::datetime::detail::datetime_component;
template <typename Timestamp, datetime_component Component>
struct extract_component_operator {
	static_assert(cudf::is_timestamp<Timestamp>(), "");

	CUDA_DEVICE_CALLABLE int16_t operator()(Timestamp const ts) const {
		using namespace simt::std::chrono;

		auto days_since_epoch = floor<days>(ts);

		auto time_since_midnight = ts - days_since_epoch;

		if(time_since_midnight.count() < 0) {
			time_since_midnight += days(1);
		}

		auto hrs_ = duration_cast<hours>(time_since_midnight);
		auto mins_ = duration_cast<minutes>(time_since_midnight - hrs_);
		auto secs_ = duration_cast<seconds>(time_since_midnight - hrs_ - mins_);

		switch(Component) {
		case datetime_component::YEAR: return static_cast<int>(year_month_day(days_since_epoch).year());
		case datetime_component::MONTH: return static_cast<unsigned>(year_month_day(days_since_epoch).month());
		case datetime_component::DAY: return static_cast<unsigned>(year_month_day(days_since_epoch).day());
		case datetime_component::WEEKDAY: return year_month_weekday(days_since_epoch).weekday().iso_encoding();
		case datetime_component::HOUR: return hrs_.count();
		case datetime_component::MINUTE: return mins_.count();
		case datetime_component::SECOND: return secs_.count();
		default: return 0;
		}
	}
};

template <datetime_component Component>
struct launch_extract_component {
	template <typename Element, std::enable_if_t<!cudf::is_timestamp<Element>()> * = nullptr>
	CUDA_DEVICE_CALLABLE int16_t operator()(int64_t val) {
		assert(false);
		return 0;
	}

	template <typename Timestamp, std::enable_if_t<cudf::is_timestamp<Timestamp>()> * = nullptr>
	CUDA_DEVICE_CALLABLE int16_t operator()(int64_t val) {
		return extract_component_operator<Timestamp, Component>{}(Timestamp{static_cast<typename Timestamp::rep>(val)});
	}
};

struct cast_to_timestamp_ns {
	template <typename Element, std::enable_if_t<!cudf::is_timestamp<Element>()> * = nullptr>
	CUDA_DEVICE_CALLABLE cudf::timestamp_ns operator()(int64_t val) {
		assert(false);
		return cudf::timestamp_ns{};
	}

	template <typename Timestamp, std::enable_if_t<cudf::is_timestamp<Timestamp>()> * = nullptr>
	CUDA_DEVICE_CALLABLE cudf::timestamp_ns operator()(int64_t val) {
		return cudf::timestamp_ns{Timestamp{static_cast<typename Timestamp::rep>(val)}};
	}
};

CUDA_DEVICE_CALLABLE bool is_float_type(cudf::type_id type) {
	return (cudf::type_id::FLOAT32 == type || cudf::type_id::FLOAT64 == type);
}

CUDA_DEVICE_CALLABLE bool is_timestamp_type(cudf::type_id type) {
	return (cudf::type_id::TIMESTAMP_DAYS == type || cudf::type_id::TIMESTAMP_SECONDS == type ||
			cudf::type_id::TIMESTAMP_MILLISECONDS == type || cudf::type_id::TIMESTAMP_MICROSECONDS == type ||
			cudf::type_id::TIMESTAMP_NANOSECONDS == type);
}

CUDA_DEVICE_CALLABLE bool is_string_type(cudf::type_id type) {
	return (cudf::type_id::STRING == type);
}

/**
 * every element that is stored in the local buffer is 8 bytes in local, so signed ints are cast to int64, unsigned to
 * uint64, and floating points are all doubles
 */
class InterpreterFunctor {
public:
	~InterpreterFunctor() = default;
	InterpreterFunctor(InterpreterFunctor && other) = default;
	InterpreterFunctor(InterpreterFunctor const & other) = default;
	InterpreterFunctor & operator=(InterpreterFunctor const & other) = delete;
	InterpreterFunctor & operator=(InterpreterFunctor && other) = delete;

	InterpreterFunctor(cudf::mutable_table_device_view out_table,
		cudf::table_device_view table,
		cudf::size_type num_operations,
		const column_index_type * left_input_positions,
		const column_index_type * right_input_positions,
		const column_index_type * output_positions,
		const column_index_type * final_output_positions,
		const cudf::type_id * input_types_left,
		const cudf::type_id * input_types_right,
		const operator_type * operations,
		cudf::detail::scalar_device_view_base ** scalars_left,
		cudf::detail::scalar_device_view_base ** scalars_right,
		void * temp_valids_in_buffer,
		void * temp_valids_out_buffer)
		: out_table{out_table},
			table{table},
			num_operations{num_operations},
			left_input_positions{left_input_positions},
			right_input_positions{right_input_positions},
			output_positions{output_positions},
			final_output_positions{final_output_positions},
			input_types_left{input_types_left},
			input_types_right{input_types_right},
			operations{operations},
			scalars_left{scalars_left},
			scalars_right{scalars_right},
		  temp_valids_in_buffer{static_cast<cudf::bitmask_type *>(temp_valids_in_buffer)},
		  temp_valids_out_buffer{static_cast<cudf::bitmask_type *>(temp_valids_out_buffer)} {

	}

	CUDA_DEVICE_CALLABLE void operator()(
		cudf::size_type row_index, int64_t total_buffer[], cudf::size_type size, curandState & state) {
		cudf::bitmask_type * valids_in_buffer =
			temp_valids_in_buffer + (blockIdx.x * blockDim.x + threadIdx.x) * table.num_columns();
		cudf::bitmask_type * valids_out_buffer =
			temp_valids_out_buffer + (blockIdx.x * blockDim.x + threadIdx.x) * out_table.num_columns();

		for(cudf::size_type column_index = 0; column_index < table.num_columns(); column_index++) {
			read_valid_data(column_index, valids_in_buffer, row_index);
		}

		// NOTE: Currently interops does not support plans with an input or output index greater than 63
		// This is a limitation by using an uint64_t to store all the valids in the plan
		uint64_t cur_row_valids;
		for(cudf::size_type row = 0; row < 32 && row_index + row < size; row++) {
			// load current row valids and data
			for(cudf::size_type column_index = 0; column_index < table.num_columns(); column_index++) {
				setColumnValid(cur_row_valids, column_index, getColumnValid(valids_in_buffer[column_index], row));
				read_data(column_index, total_buffer, row_index + row);
			}

			for(int16_t op_index = 0; op_index < num_operations; op_index++) {
				process_operator(op_index, total_buffer, row_index + row, cur_row_valids,state );
			}

			// copy data and row valids into buffer
			for(cudf::size_type column_index = 0; column_index < out_table.num_columns(); column_index++) {
				write_data(column_index, final_output_positions[column_index], total_buffer, row_index + row);
				setColumnValid(valids_out_buffer[column_index],	row, getColumnValid(cur_row_valids, this->final_output_positions[column_index]));
			}
		}

		// copy row valids into global
		for(cudf::size_type column_index = 0; column_index < out_table.num_columns(); column_index++) {
			write_valid_data(column_index, valids_out_buffer[column_index], row_index);
		}
	}

private:

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 * @param position the position in the local buffer where this data needs to be written
	 */
	CUDA_DEVICE_CALLABLE void get_data_from_buffer(int64_t * data, int64_t * buffer, int position) {
		*data = *(buffer + (position * blockDim.x + threadIdx.x));
	}

	CUDA_DEVICE_CALLABLE void get_data_from_buffer(double * data, int64_t * buffer, int position) {
		*data = __longlong_as_double(*(buffer + (position * blockDim.x + threadIdx.x)));
	}

	CUDA_DEVICE_CALLABLE void store_data_in_buffer(int64_t data, int64_t * buffer, int position) {
		*(buffer + (position * blockDim.x + threadIdx.x)) = data;
	}

	CUDA_DEVICE_CALLABLE void store_data_in_buffer(double data, int64_t * buffer, int position) {
		*(buffer + (position * blockDim.x + threadIdx.x)) = __double_as_longlong(data);
	}

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 */
	struct device_ptr_read_into_buffer {
		template <typename ColType, std::enable_if_t<std::is_integral<ColType>::value> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::table_device_view& table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer) {
			*(buffer + (col_index * blockDim.x + threadIdx.x)) = static_cast<int64_t>(table.column(col_index).element<ColType>(row));
		}

		template <typename ColType, std::enable_if_t<cudf::is_fixed_point<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::table_device_view& table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer) {
			//TODO: implement fixed point
			//*(buffer + (col_index * blockDim.x + threadIdx.x)) = static_cast<int64_t>(table.column(col_index).element<ColType>(row));
		}

		template <typename ColType, std::enable_if_t<std::is_floating_point<ColType>::value> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::table_device_view& table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer) {
			*(buffer + (col_index * blockDim.x + threadIdx.x)) = __double_as_longlong(static_cast<double>(table.column(col_index).element<ColType>(row)));
		}

		template <typename ColType, std::enable_if_t<cudf::is_timestamp<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::table_device_view& table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer) {
			*(buffer + (col_index * blockDim.x + threadIdx.x)) = static_cast<int64_t>(table.column(col_index).element<ColType>(row).time_since_epoch().count());
		}

		template <typename ColType, std::enable_if_t<cudf::is_compound<ColType>() or cudf::is_duration<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::table_device_view& table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer) {
		}
	};

	CUDA_DEVICE_CALLABLE void read_data(cudf::size_type cur_column, int64_t * buffer, cudf::size_type row_index) {
		cudf::type_dispatcher(table.column(cur_column).type(),
																				device_ptr_read_into_buffer{},
																				table,
																				cur_column,
																				row_index,
																				buffer);
	}

	/**
	 * @param buffer the local buffer which storse the information that is to be processed
	 */
	struct device_ptr_write_from_buffer {
		template <typename ColType, std::enable_if_t<std::is_integral<ColType>::value> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::mutable_table_device_view & out_table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer,
																					int position) {
			out_table.column(col_index).element<ColType>(row) =	static_cast<ColType>(*(buffer + (position * blockDim.x + threadIdx.x)));
		}


		template <typename ColType, std::enable_if_t<cudf::is_fixed_point<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::mutable_table_device_view & out_table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer,
																					int position) {
			//TODO: implement fixed point
			//out_table.column(col_index).element<ColType>(row) =	static_cast<ColType>(*(buffer + (position * blockDim.x + threadIdx.x)));
		}


		template <typename ColType, std::enable_if_t<std::is_floating_point<ColType>::value> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::mutable_table_device_view & out_table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer,
																					int position) {
			out_table.column(col_index).element<ColType>(row) =	static_cast<ColType>(__longlong_as_double(*(buffer + (position * blockDim.x + threadIdx.x))));
		}

		template <typename ColType, std::enable_if_t<cudf::is_timestamp<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::mutable_table_device_view & out_table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer,
																					int position) {
			out_table.column(col_index).element<ColType>(row) =	static_cast<typename ColType::rep>(*(buffer + (position * blockDim.x + threadIdx.x)));
		}

		template <typename ColType, std::enable_if_t<cudf::is_compound<ColType>() or cudf::is_duration<ColType>()> * = nullptr>
		CUDA_DEVICE_CALLABLE void operator() (cudf::mutable_table_device_view & out_table,
																					cudf::size_type col_index,
																					cudf::size_type row,
																					int64_t * buffer,
																					int position) {
		}
	};

	CUDA_DEVICE_CALLABLE void write_data(cudf::size_type cur_column, int cur_buffer, int64_t * buffer, cudf::size_type row_index) {
		cudf::type_dispatcher(out_table.column(cur_column).type(),
																				device_ptr_write_from_buffer{},
																				out_table,
																				cur_column,
																				row_index,
																				buffer,
																				cur_buffer);
	}

	CUDA_DEVICE_CALLABLE void read_valid_data(cudf::size_type column_idx, cudf::bitmask_type * buffer, cudf::size_type row_index) {
		const cudf::bitmask_type * valid_in = table.column(column_idx).null_mask();
		if(valid_in != nullptr) {
			buffer[column_idx] = valid_in[cudf::word_index(row_index)];
		} else {
			buffer[column_idx] = 0xffffffff;
		}
	}

	CUDA_DEVICE_CALLABLE void write_valid_data(cudf::size_type column_idx, cudf::bitmask_type valid_data, cudf::size_type row_index) {
		if(out_table.column(column_idx).nullable()) {
			cudf::bitmask_type * valid_out = out_table.column(column_idx).null_mask();
			valid_out[cudf::word_index(row_index)] = valid_data;
		}
	}

	CUDA_DEVICE_CALLABLE bool getColumnValid(uint64_t row_valid, int bit_idx) {
		assert(bit_idx < sizeof(uint64_t)*8);
		return (row_valid >> bit_idx) & uint64_t{1};
	}

	CUDA_DEVICE_CALLABLE void setColumnValid(cudf::bitmask_type & row_valid, int bit_idx, bool value) {
		assert(bit_idx < sizeof(cudf::bitmask_type)*8);
		row_valid ^= ((-value) ^ row_valid) & (cudf::bitmask_type{1} << bit_idx);
	}

	CUDA_DEVICE_CALLABLE void setColumnValid(uint64_t & row_valid, int bit_idx, bool value) {
		assert(bit_idx < sizeof(uint64_t)*8);
		row_valid ^= ((-value) ^ row_valid) & (uint64_t{1} << bit_idx);
	}

	template <typename LeftType>
	CUDA_DEVICE_CALLABLE LeftType get_scalar_value(cudf::detail::scalar_device_view_base * scalar_ptr) {
		switch (scalar_ptr->type().id())
		{
		case cudf::type_id::BOOL8:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<bool>*>(scalar_ptr)->value());
		case cudf::type_id::INT8:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<int8_t>*>(scalar_ptr)->value());
		case cudf::type_id::INT16:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<int16_t>*>(scalar_ptr)->value());
		case cudf::type_id::INT32:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<int32_t>*>(scalar_ptr)->value());
		case cudf::type_id::INT64:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<int64_t>*>(scalar_ptr)->value());
		case cudf::type_id::FLOAT32:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<float>*>(scalar_ptr)->value());
		case cudf::type_id::FLOAT64:
			return static_cast<LeftType>(static_cast<cudf::numeric_scalar_device_view<double>*>(scalar_ptr)->value());
		case cudf::type_id::TIMESTAMP_DAYS:
			return static_cast<LeftType>(static_cast<cudf::timestamp_scalar_device_view<cudf::timestamp_D>*>(scalar_ptr)->value().time_since_epoch().count());
		case cudf::type_id::TIMESTAMP_SECONDS:
			return static_cast<LeftType>(static_cast<cudf::timestamp_scalar_device_view<cudf::timestamp_s>*>(scalar_ptr)->value().time_since_epoch().count());
		case cudf::type_id::TIMESTAMP_MILLISECONDS:
			return static_cast<LeftType>(static_cast<cudf::timestamp_scalar_device_view<cudf::timestamp_ms>*>(scalar_ptr)->value().time_since_epoch().count());
		case cudf::type_id::TIMESTAMP_MICROSECONDS:
			return static_cast<LeftType>(static_cast<cudf::timestamp_scalar_device_view<cudf::timestamp_us>*>(scalar_ptr)->value().time_since_epoch().count());
		case cudf::type_id::TIMESTAMP_NANOSECONDS:
			return static_cast<LeftType>(static_cast<cudf::timestamp_scalar_device_view<cudf::timestamp_ns>*>(scalar_ptr)->value().time_since_epoch().count());
		default:
			return LeftType{};
		}
	}

	CUDA_DEVICE_CALLABLE void process_operator(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, uint64_t & row_valids, curandState & state) {
		cudf::type_id type = input_types_left[op_index];
		if(is_float_type(type)) {
			process_operator_1<double>(op_index, buffer, row_index, row_valids,state);
		} else {
			process_operator_1<int64_t>(op_index, buffer, row_index, row_valids,state);
		}
	}

	template <typename LeftType>
	CUDA_DEVICE_CALLABLE void process_operator_1(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, uint64_t & row_valids, curandState & state) {
		cudf::type_id type = input_types_right[op_index];
		if(is_float_type(type)) {
			process_operator_2<LeftType, double>(op_index, buffer, row_index, row_valids,state);
		} else {
			process_operator_2<LeftType, int64_t>(op_index, buffer, row_index, row_valids,state);
		}
	}

	template <typename LeftType, typename RightType>
	CUDA_DEVICE_CALLABLE void process_operator_2(
		size_t op_index, int64_t * buffer, cudf::size_type row_index, uint64_t & row_valids, curandState & state) {
		column_index_type left_position = left_input_positions[op_index];
		column_index_type right_position = right_input_positions[op_index];
		column_index_type output_position = output_positions[op_index];
		operator_type oper = operations[op_index];

		if(right_position != UNARY_INDEX && right_position != NULLARY_INDEX) {
			// It's a binary operation

			cudf::type_id left_type_id = input_types_left[op_index];
			LeftType left_value;
			cudf::string_view left_str_view;
			bool left_valid;
			if(left_position >= 0) {
				if (is_string_type(left_type_id)) {
					// string values always come from the table input,
					// intermediate string result not supported
					left_str_view = table.column(left_position).element<cudf::string_view>(row_index);
				} else {
					get_data_from_buffer(&left_value, buffer, left_position);
				}
				left_valid = getColumnValid(row_valids, left_position);
			} else if(left_position == SCALAR_INDEX) {
				if (is_string_type(left_type_id)) {
					left_str_view = static_cast<cudf::string_scalar_device_view*>(scalars_left[op_index])->value();
				} else {
					left_value = get_scalar_value<LeftType>(scalars_left[op_index]);
				}
				left_valid = true;
			} else { // if(left_position == SCALAR_NULL_INDEX)
				left_valid = false;
			}

			cudf::type_id right_type_id = input_types_right[op_index];
			RightType right_value;
			cudf::string_view right_str_view;
			bool right_valid;
			if(right_position >= 0) {
				if (is_string_type(right_type_id)) {
					// string values always come from the table input,
					// intermediate string result not supported
					right_str_view = table.column(right_position).element<cudf::string_view>(row_index);
				} else {
					get_data_from_buffer(&right_value, buffer, right_position);
				}
				right_valid = getColumnValid(row_valids, right_position);
			}	else if(right_position == SCALAR_INDEX) {
				if (is_string_type(right_type_id)) {
					right_str_view = static_cast<cudf::string_scalar_device_view*>(scalars_right[op_index])->value();
				} else {
					right_value = get_scalar_value<RightType>(scalars_right[op_index]);
				}
				right_valid = true;
			} else { // if(right_position == SCALAR_NULL_INDEX)
				right_valid = false;
			}

			if(oper == operator_type::BLZ_MAGIC_IF_NOT) {
				if(left_valid && left_value) {
					store_data_in_buffer(right_value, buffer, output_position);
					setColumnValid(row_valids, output_position, right_valid);
				} else {
					// we want to indicate to first_non_magic to use the second value
					store_data_in_buffer(getMagicNumber<RightType>(), buffer, output_position);
				}
			} else if(oper == operator_type::BLZ_FIRST_NON_MAGIC) {
				if(left_value == getMagicNumber<LeftType>()) {
					store_data_in_buffer(right_value, buffer, output_position);
					setColumnValid(row_valids, output_position, right_valid);
				} else {
					store_data_in_buffer(left_value, buffer, output_position);
					setColumnValid(row_valids, output_position, left_valid);
				}
			} else if(oper == operator_type::BLZ_LOGICAL_OR) {
				if(left_valid && right_valid) {
					store_data_in_buffer(static_cast<int64_t>(left_value || right_value), buffer, output_position);
					setColumnValid(row_valids, output_position, true);
				} else if(left_valid) {
					store_data_in_buffer(left_value, buffer, output_position);
					setColumnValid(row_valids, output_position, !!left_value);
				} else if(right_valid) {
					store_data_in_buffer(right_value, buffer, output_position);
					setColumnValid(row_valids, output_position, !!right_valid);
				}	else {
					setColumnValid(row_valids, output_position, false);
				}
			} else if (left_valid && right_valid) {
				if(oper == operator_type::BLZ_LOGICAL_AND) {
					store_data_in_buffer(static_cast<int64_t>(left_value && right_value), buffer, output_position);
				} else if(oper == operator_type::BLZ_ADD) {
					store_data_in_buffer(left_value + right_value, buffer, output_position);
				} else if(oper == operator_type::BLZ_SUB) {
					store_data_in_buffer(left_value - right_value, buffer, output_position);
				} else if(oper == operator_type::BLZ_MUL) {
					store_data_in_buffer(left_value * right_value, buffer, output_position);
				} else if(oper == operator_type::BLZ_DIV) {
					store_data_in_buffer(left_value / right_value, buffer, output_position);
				} else if(oper == operator_type::BLZ_MOD) {
					if (!is_float_type(left_type_id) && !is_float_type(right_type_id))	{
						store_data_in_buffer(
						static_cast<int64_t>(left_value) % static_cast<int64_t>(right_value), buffer, output_position);
					}	else {
						store_data_in_buffer(
						fmod(static_cast<double>(left_value), static_cast<double>(right_value)), buffer, output_position);
					}
				} else if(oper == operator_type::BLZ_POW) {
					store_data_in_buffer(pow(static_cast<double>(left_value), static_cast<double>(right_value)), buffer, output_position);
				} else if(oper == operator_type::BLZ_ROUND) {
					double factor = pow(10, right_value);
					store_data_in_buffer(round(static_cast<double>(left_value) * factor) / factor, buffer, output_position);
				} else if(oper == operator_type::BLZ_EQUAL) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view == right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts == right_ts;
					} else {
						computed = left_value == right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_NOT_EQUAL) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view != right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts != right_ts;
					} else {
						computed = left_value != right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_LESS) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view < right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts < right_ts;
					} else {
						computed = left_value < right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_GREATER) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view > right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts > right_ts;
					} else {
						computed = left_value > right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_LESS_EQUAL) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view <= right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts <= right_ts;
					} else {
						computed = left_value <= right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_GREATER_EQUAL) {
					int64_t computed;
					if (is_string_type(left_type_id) && is_string_type(right_type_id)) {
						computed = left_str_view >= right_str_view;
					} else if(is_timestamp_type(left_type_id) && is_timestamp_type(right_type_id)) {
						cudf::timestamp_ns left_ts = cudf::type_dispatcher(cudf::data_type{left_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(left_value));
						cudf::timestamp_ns right_ts = cudf::type_dispatcher(cudf::data_type{right_type_id}, cast_to_timestamp_ns{}, static_cast<int64_t>(right_value));
						computed = left_ts >= right_ts;
					} else {
						computed = left_value >= right_value;
					}
					store_data_in_buffer(computed, buffer, output_position);
				}

				if(oper == operator_type::BLZ_DIV && right_value == 0) //if div by zero = null
					setColumnValid(row_valids, output_position, false);
				else
					setColumnValid(row_valids, output_position, true);
			} else {
				setColumnValid(row_valids, output_position, false);
			}
		} else if( right_position != NULLARY_INDEX) {
			// It's a unary operation, scalar inputs are not allowed
			assert(left_position >= 0);

			cudf::type_id left_type_id = input_types_left[op_index];
			LeftType left_value;
			cudf::string_view left_str_view;
			if (is_string_type(left_type_id)) {
					// string values always come from the table input,
					// intermediate string result not supported
					left_str_view = table.column(left_position).element<cudf::string_view>(row_index);
			} else {
				get_data_from_buffer(&left_value, buffer, left_position);
			}
			bool left_valid = getColumnValid(row_valids, left_position);

			if(oper == operator_type::BLZ_IS_NULL) {
				store_data_in_buffer(static_cast<int64_t>(!left_valid), buffer, output_position);
			} else if(oper == operator_type::BLZ_IS_NOT_NULL) {
				store_data_in_buffer(static_cast<int64_t>(left_valid), buffer, output_position);
			}	else if (left_valid) {
				if(oper == operator_type::BLZ_FLOOR) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(floor(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_CEIL) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(ceil(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_SIN) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(sin(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_COS) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(cos(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_ASIN) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(asin(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_ACOS) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(acos(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_TAN) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(tan(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_COTAN) {
					double val = static_cast<double>(left_value);
					double sin_, cos_;
					sincos(val, &sin_, &cos_);
					store_data_in_buffer(cos_ / sin_, buffer, output_position);
				} else if(oper == operator_type::BLZ_ATAN) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(atan(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_ABS) {
					if (is_float_type(left_type_id)){
						double val = static_cast<double>(left_value);
						store_data_in_buffer(fabs(val), buffer, output_position);
					} else {
						int64_t val = static_cast<int64_t>(left_value);
						store_data_in_buffer(abs(val), buffer, output_position);
					}
				} else if(oper == operator_type::BLZ_NOT) {
					store_data_in_buffer(static_cast<int64_t>(!left_value), buffer, output_position);
				} else if(oper == operator_type::BLZ_LN) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(log(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_LOG) {
					double val = static_cast<double>(left_value);
					store_data_in_buffer(log10(val), buffer, output_position);
				} else if(oper == operator_type::BLZ_YEAR) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::YEAR>{},	static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_MONTH) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::MONTH>{}, static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_DAY) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::DAY>{}, static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_HOUR) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::HOUR>{},	static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_MINUTE) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::MINUTE>{}, static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_SECOND) {
					int64_t computed = cudf::type_dispatcher(cudf::data_type{left_type_id},
						launch_extract_component<datetime_component::SECOND>{}, static_cast<int64_t>(left_value));
					store_data_in_buffer(computed, buffer, output_position);
				} else if(oper == operator_type::BLZ_CAST_TINYINT || oper == operator_type::BLZ_CAST_SMALLINT || oper == operator_type::BLZ_CAST_INTEGER || oper == operator_type::BLZ_CAST_BIGINT) {
					store_data_in_buffer(static_cast<int64_t>(left_value), buffer, output_position);
				} else if(oper == operator_type::BLZ_CAST_FLOAT || oper == operator_type::BLZ_CAST_DOUBLE) {
					store_data_in_buffer(static_cast<double>(left_value), buffer, output_position);
				} else if(oper == operator_type::BLZ_CAST_DATE) {
					cudf::timestamp_D computed;
					switch (left_type_id)
					{
					case cudf::type_id::INT8:
					case cudf::type_id::INT16:
					case cudf::type_id::INT32:
					case cudf::type_id::INT64:
					case cudf::type_id::FLOAT32:
					case cudf::type_id::FLOAT64:
					case cudf::type_id::TIMESTAMP_DAYS:
						computed = cudf::timestamp_D{static_cast<cudf::timestamp_D::rep>(left_value)};
						break;
					case cudf::type_id::TIMESTAMP_SECONDS:
						// computed = cudf::timestamp_D{cudf::timestamp_s{static_cast<cudf::timestamp_s::rep>(left_value)}};
						computed = cudf::timestamp_D{static_cast<cudf::timestamp_s::rep>(left_value)};
						break;
					case cudf::type_id::TIMESTAMP_MILLISECONDS:
						// computed = cudf::timestamp_D{cudf::timestamp_ms{static_cast<cudf::timestamp_ms::rep>(left_value)}};
						computed = cudf::timestamp_D{static_cast<cudf::timestamp_ms::rep>(left_value)};
						break;
					case cudf::type_id::TIMESTAMP_MICROSECONDS:
						// computed = cudf::timestamp_D{cudf::timestamp_us{static_cast<cudf::timestamp_us::rep>(left_value)}};
						computed = cudf::timestamp_D{static_cast<cudf::timestamp_us::rep>(left_value)};
						break;
					case cudf::type_id::TIMESTAMP_NANOSECONDS:
						// computed = cudf::timestamp_D{cudf::timestamp_ns{static_cast<cudf::timestamp_ns::rep>(left_value)}};
						computed = cudf::timestamp_D{static_cast<cudf::timestamp_ns::rep>(left_value)};
						break;
					default:
						// should not reach here, invalid conversion
						assert(false);
						break;
					}
					store_data_in_buffer(static_cast<int64_t>(computed.time_since_epoch().count()), buffer, output_position);
				} else if(oper == operator_type::BLZ_CAST_TIMESTAMP) {
					cudf::timestamp_ns computed;
					switch (left_type_id)
					{
					case cudf::type_id::INT8:
					case cudf::type_id::INT16:
					case cudf::type_id::INT32:
					case cudf::type_id::INT64:
					case cudf::type_id::FLOAT32:
					case cudf::type_id::FLOAT64:
					case cudf::type_id::TIMESTAMP_NANOSECONDS:
						computed = cudf::timestamp_ns{static_cast<cudf::timestamp_ns::rep>(left_value)};
						break;
					case cudf::type_id::TIMESTAMP_DAYS:
						computed = cudf::timestamp_ns{cudf::timestamp_D{static_cast<cudf::timestamp_D::rep>(left_value)}};
						break;
					case cudf::type_id::TIMESTAMP_SECONDS:
						computed = cudf::timestamp_ns{cudf::timestamp_s{static_cast<cudf::timestamp_s::rep>(left_value)}};
						break;
					case cudf::type_id::TIMESTAMP_MILLISECONDS:
						computed = cudf::timestamp_ns{cudf::timestamp_ms{static_cast<cudf::timestamp_ms::rep>(left_value)}};
						break;
					case cudf::type_id::TIMESTAMP_MICROSECONDS:
						computed = cudf::timestamp_ns{cudf::timestamp_us{static_cast<cudf::timestamp_us::rep>(left_value)}};
						break;
					default:
						// should not reach here, invalid conversion
						assert(false);
						break;
					}
					store_data_in_buffer(static_cast<int64_t>(computed.time_since_epoch().count()), buffer, output_position);
				} else if(oper == operator_type::BLZ_CHAR_LENGTH) {
					int64_t computed = left_str_view.length();
					store_data_in_buffer(computed, buffer, output_position);
				}
			}

			bool out_valid = (oper == operator_type::BLZ_IS_NULL || oper == operator_type::BLZ_IS_NOT_NULL) ? true : left_valid;
			setColumnValid(row_valids, output_position, out_valid);
		}else{
			if(oper == operator_type::BLZ_RAND) {
				double out = curand_uniform_double(&state);
				store_data_in_buffer(out, buffer, output_position);
			}
			setColumnValid(row_valids, output_position, true);
		}
	}

private:
	cudf::mutable_table_device_view out_table;
	cudf::table_device_view table;

	cudf::size_type num_operations;

	const column_index_type * left_input_positions;
	const column_index_type * right_input_positions;
	const column_index_type * output_positions;
	const column_index_type * final_output_positions;  // should be same size as output_data, e.g. num_outputs

	const cudf::type_id * input_types_left;
	const cudf::type_id * input_types_right;

	const operator_type * operations;

	cudf::detail::scalar_device_view_base ** scalars_left;
	cudf::detail::scalar_device_view_base ** scalars_right;

	cudf::bitmask_type * temp_valids_in_buffer;
	cudf::bitmask_type * temp_valids_out_buffer;
};


__global__ void transformKernel(InterpreterFunctor op, cudf::size_type size, curandState *state) {
	extern __shared__ int64_t total_buffer[];

    int id = threadIdx.x + blockIdx.x * blockDim.x;
    

    curandState localState = state[id];
 

	for(cudf::size_type i = (blockIdx.x * blockDim.x + threadIdx.x) * 32; i < size; i += blockDim.x * gridDim.x * 32) {
		op(i, total_buffer, size, localState);
	}
	state[id] = localState;
}

} // namespace interops
