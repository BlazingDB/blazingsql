
#include <algorithm>
#include <iostream>
#include <numeric>
#include <vector>
#include <limits>
#include <map>
#include <type_traits>
#include <thrust/reduce.h>
#include <thrust/device_vector.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include "utilities/StringUtils.h"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include <chrono>
#include <cuda_runtime.h>
#include "../Utils.cuh"
#include "helper_cuda.h"
#include "cudf/legacy/binaryop.hpp"
#include "CalciteExpressionParsing.h"

typedef int64_t temp_gdf_valid_type; //until its an int32 in cudf

__host__ __device__ __forceinline__
bool gdf_is_valid_32(const temp_gdf_valid_type *valid, gdf_index_type pos) {
	if ( valid )
		return (valid[pos / 64] >> (pos % 64)) & 1;
	else
		return true;
}

template<typename T>
__device__ __forceinline__
T getMagicNumber() {
	return T{};
}

template <>
__device__ __forceinline__
int64_t getMagicNumber<int64_t>() {
	return std::numeric_limits<int64_t>::max() - 13ll;
}

template <>
__device__ __forceinline__
double getMagicNumber<double>() {
	return 1.7976931348623123e+308;
}

/*
template<typename T>
__device__ __forceinline__ T t(int thread_id,
                                                      int logical_index)
{
    return logical_index * THREADBLOCK_SIZE + thread_id;
}
 */

/*
 * Things to consider
 *
 * shared memory is limited in size and we actually aren't using it the way its meant which is to coordinate access between threads
 * we just use it as a lower latency place to access memory than from device memory, we should see how fast it is when we just use
 * normal memory for the buffer instead of shared
 *
 *
 */


/*
 * TODO find a way to include date time operations from CUDF instead of maintaing it here
 * irequires that the operators can be templated
 */
__constant__
const int64_t units_per_day = 86400000;
__constant__
const int64_t units_per_hour = 3600000;
__constant__
const int64_t units_per_minute = 60000;
__constant__
const int64_t units_per_second = 1000;

static __device__
int64_t extract_year_op(int64_t unixTime){

			const int z = ((unixTime >= 0 ? unixTime : unixTime - (units_per_day - 1)) / units_per_day) + 719468;
			const int era = (z >= 0 ? z : z - 146096) / 146097;
			const unsigned doe = static_cast<unsigned>(z - era * 146097);
			const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
			const int y = static_cast<int>(yoe) + era * 400;
			const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
			const unsigned mp = (5*doy + 2)/153;
			const unsigned m = mp + (mp < 10 ? 3 : -9);
			if (m <= 2)
				return y + 1;
			else
				return y;
}

static __device__
int64_t extract_month_op(int64_t unixTime){
	const int z = ((unixTime >= 0 ? unixTime : unixTime - (units_per_day - 1)) / units_per_day) + 719468;
	const int era = (z >= 0 ? z : z - 146096) / 146097;
	const unsigned doe = static_cast<unsigned>(z - era * 146097);
	const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
	const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
	const unsigned mp = (5*doy + 2)/153;
	return mp + (mp < 10 ? 3 : -9);
}

static __device__
int64_t extract_day_op(int64_t unixTime){
	const int z = ((unixTime >= 0 ? unixTime : unixTime - (units_per_day - 1)) / units_per_day) + 719468;
	const int era = (z >= 0 ? z : z - 146096) / 146097;
	const unsigned doe = static_cast<unsigned>(z - era * 146097);
	const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
	const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
	const unsigned mp = (5*doy + 2)/153;
	return doy - (153*mp+2)/5 + 1;
}

static __device__
int64_t extract_hour_op(int64_t unixTime){
	return unixTime >= 0 ? ((unixTime % units_per_day)/units_per_hour) : ((units_per_day + (unixTime % units_per_day))/units_per_hour);
}

static __device__
int64_t extract_minute_op(int64_t unixTime){
	return unixTime >= 0 ? ((unixTime % units_per_hour)/units_per_minute) :  ((units_per_hour + (unixTime % units_per_hour))/units_per_minute);
}

static __device__
int64_t extract_second_op(int64_t unixTime){
	return unixTime >= 0 ? ((unixTime % units_per_minute)/units_per_second) : ((units_per_minute + (unixTime % units_per_minute))/units_per_second);
}

static __device__
int64_t extract_year_op_32(int64_t unixDate){
	const int z = unixDate + 719468;
	const int era = (z >= 0 ? z : z - 146096) / 146097;
	const unsigned doe = static_cast<unsigned>(z - era * 146097);
	const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
	const int y = static_cast<int>(yoe) + era * 400;
	const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
	const unsigned mp = (5*doy + 2)/153;
	const unsigned m = mp + (mp < 10 ? 3 : -9);
	if (m <= 2)
		return y + 1;
	else
		return y;
}

static __device__
int64_t extract_month_op_32(int64_t unixDate){
	const int z = unixDate + 719468;
			const int era = (z >= 0 ? z : z - 146096) / 146097;
			const unsigned doe = static_cast<unsigned>(z - era * 146097);
			const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
			const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
			const unsigned mp = (5*doy + 2)/153;

			return mp + (mp < 10 ? 3 : -9);
}


static __device__
int64_t extract_day_op_32(int64_t unixDate){
	const int z = unixDate + 719468;
	const int era = (z >= 0 ? z : z - 146096) / 146097;
	const unsigned doe = static_cast<unsigned>(z - era * 146097);
	const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
	const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
	const unsigned mp = (5*doy + 2)/153;
	return doy - (153*mp+2)/5 + 1;
}


static int64_t scale_to_64_bit_return_bytes(gdf_scalar input){
	gdf_dtype cur_type = input.dtype;

	int64_t data_return;
	if(cur_type == GDF_INT8 || cur_type == GDF_BOOL8) data_return = input.data.si08;
	else if(cur_type == GDF_INT16) data_return = input.data.si16;
	else if(cur_type == GDF_INT32 || cur_type == GDF_STRING_CATEGORY) data_return = input.data.si32;
	else if(cur_type == GDF_INT64) data_return = input.data.si64;
	else if(cur_type == GDF_DATE32) data_return = input.data.dt32;
	else if(cur_type == GDF_DATE64) data_return = input.data.dt64;
	else if(cur_type == GDF_TIMESTAMP) data_return = input.data.tmst;
	else if(cur_type == GDF_FLOAT32){
		double * data_return_ptr = (double *) &data_return;
		*data_return_ptr = input.data.fp32;
	}
	else if(cur_type == GDF_FLOAT64){
		double * data_return_ptr = (double *) &data_return;
		*data_return_ptr = input.data.fp64;
	}
	else {
		Library::Logging::Logger().logError(ral::utilities::buildLogString("", "", "", "ERROR: data type not found in scale_to_64_bit_return_bytes"));
		data_return = 0;
	}

	return data_return;
}

static __device__ __host__ __forceinline__
bool isInt(gdf_dtype type){
	return (type == GDF_INT32) ||
			(type == GDF_INT64) ||
			(type == GDF_INT16) ||
			(type == GDF_INT8) ||
			(type == GDF_BOOL8) ||
			(type == GDF_DATE32) ||
			(type == GDF_DATE64) ||
			(type == GDF_TIMESTAMP) ||
			(type == GDF_STRING_CATEGORY);
}

static __device__ __forceinline__
bool isDate32(gdf_dtype type){
	return type == GDF_DATE32;
}

static __device__ __forceinline__
bool isFloat(gdf_dtype type){
	return (type == GDF_FLOAT64) ||
			(type == GDF_FLOAT32);
}

static __device__ __forceinline__
bool isUnsignedInt(gdf_dtype type){
	return false;
	/* Unsigned types are not currently supported in cudf
	return (type == GDF_UINT32) ||
			(type == GDF_UINT64) ||
			(type == GDF_UINT16) ||
			(type == GDF_UINT8);*/
}




typedef short column_index_type;

/**
 * every element that is stored in the local buffer is 8 bytes in local, so signed ints are cast to int64, unsigned to uint64, and floating points
 * are all doubles
 */

template <typename IndexT>
class InterpreterFunctor {
public:
		size_t num_columns;
		short num_final_outputs;
private:
	void  **column_data; //these are device side pointers to the device pointer found in gdf_column.data
	void ** output_data;
	temp_gdf_valid_type ** valid_ptrs; //device,
	temp_gdf_valid_type ** valid_ptrs_out;

	gdf_dtype * input_column_types;
	size_t num_rows;
	column_index_type *  left_input_positions; //device fuck it we are using -2 for scalars
	column_index_type * right_input_positions; //device , -1 for unary, -2 for scalars!, fuck it one more, -3 for null scalars
	column_index_type * output_positions; //device
	column_index_type * final_output_positions; //should be same size as output_data, e.g. num_outputs

	short num_operations;
	gdf_dtype * input_types_left; //device
	gdf_dtype * input_types_right; //device
	gdf_dtype * output_types; //device
	gdf_dtype * final_output_types; //size
	gdf_binary_operator_exp * binary_operations; //device
	gdf_unary_operator * unary_operations; //device
	int64_t * scalars_left; //if these scalars are not of invalid type we use them instead of input positions
	int64_t * scalars_right;
	cudaStream_t stream;

	int BufferSize;
	int ThreadBlockSize;
	short maxPosition;

	gdf_size_type * null_counts_inputs;
	gdf_size_type * null_counts_outputs;


	template<typename LocalStorageType, typename BufferType>
	__device__ __forceinline__
	LocalStorageType get_data_from_buffer(
			BufferType * buffer, //the local buffer which storse the information that is to be processed
			column_index_type position) //the position in the local buffer where this data needs to be written
	{
		//columns
		//	return *col_data;
		//	printf("data %d",*((LocalStorageType *) (buffer + position)));


		//lets chang eposition to not be in bytes

		return *((LocalStorageType *) (buffer + ((position * ThreadBlockSize) + threadIdx.x)));

		//return (col_data[row]);
	}

	template<typename LocalStorageType, typename BufferType>
	__device__ __forceinline__
	void store_data_in_buffer(
			LocalStorageType data,
			BufferType * buffer,
			column_index_type position){
		*((LocalStorageType *) (buffer + ((position * ThreadBlockSize) + threadIdx.x))) = data;
	}


	template<typename ColType, typename LocalStorageType, typename BufferType>
	__device__ __forceinline__
	void device_ptr_read_into_buffer(column_index_type col_index,
			const IndexT row,
			const void * const * columns,
			BufferType * buffer, //the local buffer which storse the information that is to be processed
			column_index_type position){
		const ColType* col_data = static_cast<const ColType*>((columns[col_index]));
		//	return *col_data;
		*((LocalStorageType *) (buffer + ((position * ThreadBlockSize) + threadIdx.x))) = (LocalStorageType) __ldg(((ColType *) &col_data[row]));
	}

	template<typename ColType, typename LocalStorageType, typename BufferType>
	__device__ __forceinline__
	void device_ptr_write_from_buffer(
			const IndexT row,
			void * columns,
			BufferType * buffer, //the local buffer which storse the information that is to be processed
			column_index_type position){
		const LocalStorageType col_data = *((LocalStorageType *) (buffer + ((position * ThreadBlockSize) + threadIdx.x)));
		((ColType *) columns)[row] = (ColType) col_data;
	}




	template<typename BufferType>
	__device__
	__forceinline__ void write_data(column_index_type cur_column, column_index_type cur_buffer,  BufferType * buffer,const size_t & row_index){
		gdf_dtype cur_type = this->final_output_types[cur_column];
		if(cur_type == GDF_INT8 || cur_type == GDF_BOOL8){
			device_ptr_write_from_buffer<int8_t,int64_t>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}else if(cur_type == GDF_INT16){
			device_ptr_write_from_buffer<int16_t,int64_t>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}else if(cur_type == GDF_INT32 ||
				cur_type == GDF_DATE32 ||
				cur_type == GDF_STRING_CATEGORY){
			device_ptr_write_from_buffer<int32_t,int64_t>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}else if(cur_type == GDF_INT64 ||
				cur_type == GDF_DATE64 ||
				cur_type == GDF_TIMESTAMP){
			device_ptr_write_from_buffer<int64_t,int64_t>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}else if(cur_type == GDF_FLOAT32){
			device_ptr_write_from_buffer<float,double>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}else if(cur_type == GDF_FLOAT64){
			device_ptr_write_from_buffer<double,double>(

					row_index,
					this->output_data[cur_column],
					buffer,
					cur_buffer);


		}

	}

	//TODO: a clever person would make this something that gets passed into this function so that we can do more clever things than just read
	//data from some grumpy old buffer, like read in from another one of these that does read from a boring buffer, or from some perumtation iterartor
	//hmm in fact if it could read permuted data you could avoi dmaterializing intermeidate filter steps
	template<typename BufferType>
	__device__
	__forceinline__ void read_data(column_index_type cur_column,  BufferType * buffer,const size_t & row_index){
		gdf_dtype cur_type = this->input_column_types[cur_column];

		//printf("cur_type: %d\n", cur_type);

		if(cur_type == GDF_INT8 || cur_type == GDF_BOOL8){
			device_ptr_read_into_buffer<int8_t,int64_t>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}else if(cur_type == GDF_INT16){
			device_ptr_read_into_buffer<int16_t,int64_t>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}else if(cur_type == GDF_INT32 ||
				cur_type == GDF_DATE32 ||
				cur_type == GDF_STRING_CATEGORY){
			device_ptr_read_into_buffer<int32_t,int64_t>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}else if(cur_type == GDF_INT64 ||
				cur_type == GDF_DATE64 ||
				cur_type == GDF_TIMESTAMP){
			device_ptr_read_into_buffer<int64_t,int64_t>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}else if(cur_type == GDF_FLOAT32){
			device_ptr_read_into_buffer<float,double>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}else if(cur_type == GDF_FLOAT64){
			device_ptr_read_into_buffer<double,double>(
					cur_column,
					row_index,
					this->column_data,
					buffer,
					cur_column);


		}
	}


	__device__
	__forceinline__ void read_valid_data(column_index_type cur_column, int64_t * buffer,const size_t & row_index){
		int64_t * valid_in = (int64_t *) this->valid_ptrs[cur_column];
		buffer[cur_column] = valid_in[row_index / 64];
	}


	__device__
	__forceinline__ void write_valid_data(column_index_type out_index, int64_t valid_data, const size_t & row_index){

		int64_t * valid_out = (int64_t *) this->valid_ptrs_out[out_index];
		valid_out[row_index / 64] = valid_data;

	}

	template<typename InputType, typename OutputType, typename std::enable_if<std::is_floating_point<InputType>::value>::type* = nullptr>
	__device__
	__forceinline__ OutputType cast_op(InputType value){
		return (OutputType)round(value);
	}

	template<typename InputType, typename OutputType, typename std::enable_if<!std::is_floating_point<InputType>::value>::type* = nullptr>
	__device__
	__forceinline__ OutputType cast_op(InputType value){
		return (OutputType)value;
	}

	/*
	__device__
	__forceinline__ void read_permuted_data(column_index_type cur_column,  int64_t * buffer,const size_t & row_index){
		//put permuted data here
	}
	 */
	template<typename BufferType>
	__device__
	__forceinline__ void process_operator(size_t op_index,  BufferType * buffer, const IndexT &row_index,int64_t & row_valids){
		gdf_dtype type = this->input_types_left[op_index];
		if(isFloat(type)){
			process_operator_1<double>(op_index,buffer, row_index,row_valids);
		}else {
			process_operator_1<int64_t>(op_index,buffer, row_index,row_valids);
		}
	}


	template<typename LeftType, typename BufferType>
	__device__
	__forceinline__ void process_operator_1(size_t op_index,  BufferType * buffer, const IndexT &row_index,int64_t & row_valids){
		gdf_dtype type = this->input_types_right[op_index];
		if(isFloat(type)){
			process_operator_2<LeftType,double>(op_index,buffer, row_index,row_valids);
		}else {
			process_operator_2<LeftType,int64_t>(op_index,buffer, row_index,row_valids);
		}
	}

	template<typename LeftType, typename RightType, typename BufferType>
	__device__
	__forceinline__ void process_operator_2(size_t op_index,  BufferType * buffer, const IndexT &row_index,int64_t & row_valids){
		gdf_dtype type = this->output_types[op_index];
		if(isFloat(type)){
			process_operator_3<LeftType,RightType,double>(op_index,buffer, row_index,row_valids);
		}else {
			process_operator_3<LeftType,RightType,int64_t>(op_index,buffer, row_index,row_valids);
		}
	}


	__device__
	__forceinline__ bool getColumnValid(const int64_t & row_valid, const column_index_type & cur_column){
		return  (row_valid >> cur_column) & 1;
	}
	//allows us to avoid branch divergence on setting bits on and off
	__device__//TODO: careful you might have to cast bool value to int64_t for this to actually work
	__forceinline__ void setColumnValid(int64_t & row_valid, const column_index_type & cur_column,bool value){
			if (cur_column > 63){
				printf("====> setColumnValid() cur_column: %d blockIdx.x: %d threadIdx.x: %d\n", cur_column, blockIdx.x, threadIdx.x);
			}

			row_valid ^= ((-value) ^ row_valid) & (1UL << cur_column);
		}



	template<typename LeftType, typename RightType, typename OutputTypeOperator, typename BufferType>
	__device__
	__forceinline__ void process_operator_3(size_t op_index,  BufferType * buffer, const IndexT &row_index, int64_t & row_valids){



		column_index_type right_position = this->right_input_positions[op_index];
		column_index_type left_position = this->left_input_positions[op_index];

		bool left_valid;
		bool right_valid;

		column_index_type output_position = this->output_positions[op_index];
		if(right_position != -1){
			//binary op
			LeftType left_value;

			if(left_position == -2){
				left_value = ((LeftType *) this->scalars_left)[op_index];
				left_valid = true;
			}else if(left_position >= 0){
				left_value = get_data_from_buffer<LeftType>(buffer,left_position);
				left_valid = getColumnValid(row_valids,left_position);
			}else if(left_position == -3){
				//left is null do whatever
				left_valid = false;
			}

			RightType right_value;
			if(right_position == -2){
				right_value = ((RightType *) this->scalars_right)[op_index];
				right_valid = true;
			}else if(right_position == -3){
				right_valid = false;
			}else if(right_position >= 0){
				right_value = get_data_from_buffer<RightType>(buffer,right_position);
				right_valid = getColumnValid(row_valids,right_position);
			}
			// uncomment next line to fast compilation
			//			#define  FEW_OPERATORS 1


			if (this->binary_operations[op_index] == GDF_COALESCE) {
				setColumnValid(row_valids,output_position,left_valid || right_valid);
			} else {
				setColumnValid(row_valids,output_position,left_valid && right_valid);
			}

			gdf_binary_operator_exp oper = this->binary_operations[op_index];
			if(oper == BLZ_ADD){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						+ right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_SUB){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						- right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_MUL){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						* right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_DIV || oper == BLZ_FLOOR_DIV){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						/ right_value,
						buffer,
						output_position);
			} else if (oper == BLZ_COALESCE) {
				if ( left_valid ) {
						store_data_in_buffer<OutputTypeOperator>(
							left_value,
							buffer,
							output_position);
				} else {
						store_data_in_buffer<OutputTypeOperator>(
							right_value,
							buffer,
							output_position);
				}
			}

			/*else if(oper == BLZ_TRUE_DIV){
				//TODO: snap this requires understanding of the bitmask
			}*/else if(oper == BLZ_MOD){
				//mod only makes sense with integer inputs
				store_data_in_buffer<OutputTypeOperator>(
						(int64_t) left_value
						% (int64_t) right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_POW){
				//oh god this is where it breaks if we are floats e do one thing
				OutputTypeOperator data = 1;
				if(isFloat((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index])) || isFloat((gdf_dtype) __ldg((int32_t *) &this->input_types_right[op_index]))){
					data = pow((double) left_value,
							(double) right_value);

				}else{
					//there is no pow for ints, so lets just do it...

					LeftType base = left_value;
					//right type is the exponent
					for(int i = 0; i < right_value; i++){
						data *= base;
					}
				}
				store_data_in_buffer<OutputTypeOperator>(
						data,
						buffer,
						output_position);
			}else if(oper == BLZ_EQUAL){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						== right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_NOT_EQUAL){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						!= right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_LESS){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						< right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_GREATER){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						> right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_LESS_EQUAL){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						<= right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_GREATER_EQUAL){
				store_data_in_buffer<OutputTypeOperator>(
						left_value
						>= right_value,
						buffer,
						output_position);
			}else if(oper == BLZ_LOGICAL_OR){
				if(left_valid && right_valid){
					store_data_in_buffer<OutputTypeOperator>(
							left_value
							|| right_value,
							buffer,
							output_position);
				}else if(left_valid){
					store_data_in_buffer<OutputTypeOperator>(
							left_value,
							buffer,
							output_position);
				}else{
					store_data_in_buffer<OutputTypeOperator>(
							right_value,
							buffer,
							output_position);
				}

			}else if(oper == BLZ_MAGIC_IF_NOT){
				if(left_valid && left_value){
					store_data_in_buffer<OutputTypeOperator>(
							right_value,
							buffer,
							output_position);
							setColumnValid(row_valids,output_position,right_valid);
				}else{
						//we want to indicate to first_non_magic to use the second value
						store_data_in_buffer<OutputTypeOperator>(
								getMagicNumber<OutputTypeOperator>(),
								buffer,
								output_position);
				}

			}else if(oper == BLZ_FIRST_NON_MAGIC){
				if(left_value == getMagicNumber<OutputTypeOperator>()){
					store_data_in_buffer<OutputTypeOperator>(
							right_value,
							buffer,
							output_position);
					setColumnValid(row_valids,output_position,right_valid);
				}else{
					store_data_in_buffer<OutputTypeOperator>(
							left_value,
							buffer,
							output_position);
					setColumnValid(row_valids,output_position,left_valid);
				}

			}	else if (oper == BLZ_STR_LIKE
								|| oper == BLZ_STR_SUBSTRING
								|| oper == BLZ_STR_CONCAT){
				store_data_in_buffer<OutputTypeOperator>(
						left_value,
						buffer,
						output_position);
				setColumnValid(row_valids,output_position,left_valid);
			}
		}else{
			//unary op
			gdf_unary_operator oper = this->unary_operations[op_index];

			LeftType left_value;
			bool left_valid;

			if(left_position == -2){
				left_value = ((LeftType *) this->scalars_left)[op_index];
				left_valid = true;
			}else if(left_position >= 0){
				left_value = get_data_from_buffer<LeftType>(buffer,left_position);
				left_valid = getColumnValid(row_valids,left_position);
			}else if(left_position == -3){
				//left is null do whatever
				left_valid = false;
			}
			/*
			 * BLZ_FLOOR,
	BLZ_CEIL,
	BLZ_SIN,
	BLZ_COS,
	BLZ_ASIN,
	BLZ_ACOS,
	BLZ_TAN,
	BLZ_COTAN,
	BLZ_ATAN,
	BLZ_ABS,
	BLZ_NOT,
	BLZ_LN,
	BLZ_LOG,
	BLZ_YEAR,
	BLZ_MONTH,
	BLZ_DAY,
	BLZ_HOUR,
	BLZ_MINUTE,
	BLZ_SECOND,
	BLZ_INVALID_UNARY
			 */


			OutputTypeOperator computed = left_value;

			if(oper == BLZ_FLOOR){
				computed = floor(left_value);
			}else if(oper == BLZ_CEIL){
				computed = ceil(left_value);
			}else if(oper == BLZ_SIN){
				computed = sin(left_value);
			}else if(oper == BLZ_COS){
				computed = cos(left_value);
			}
			else if(oper == BLZ_ASIN){
				computed = asin(left_value);
			}else if(oper == BLZ_ACOS){
				computed = acos(left_value);
			}else if(oper == BLZ_TAN){
				computed = tan(left_value);
			}else if(oper == BLZ_COTAN){
				computed = cos(left_value)/sin(left_value);
			}else if(oper == BLZ_ATAN){
				computed = atan(left_value);
			}else if(oper == BLZ_ABS){
				computed = fabs(left_value);
			}else if(oper == BLZ_NOT){
				computed = ! left_value;
			}else if(oper == BLZ_LN){
				computed = log(left_value);
			}else if(oper == BLZ_LOG){
				computed = log10(left_value);
			}else if(oper == BLZ_YEAR){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = extract_year_op_32(left_value);

				}else{
					//assume date64
					computed = extract_year_op(left_value);
				}
			}else if(oper == BLZ_MONTH){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = extract_month_op_32(left_value);

				}else{

					computed = extract_month_op(left_value);
				}
			}else if(oper == BLZ_DAY){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = extract_day_op_32(left_value);
				}else{
					computed = extract_day_op(left_value);
				}
			}else if(oper == BLZ_HOUR){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = 0;
				}else{
					computed = extract_hour_op(left_value);
				}
			}else if(oper == BLZ_MINUTE){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = 0;
				}else{
					computed = extract_minute_op(left_value);
				}
			}else if(oper == BLZ_SECOND){
				if(isDate32((gdf_dtype) __ldg((int32_t *) &this->input_types_left[op_index]))){
					computed = 0;
				}else{
					computed = extract_second_op(left_value);
				}
			}else if(oper == BLZ_CAST_INTEGER || oper == BLZ_CAST_BIGINT){
				computed = cast_op<LeftType, OutputTypeOperator>(left_value);
			}else if(oper == BLZ_CAST_FLOAT
							 || oper == BLZ_CAST_DOUBLE
							 || oper == BLZ_CAST_DATE
							 || oper == BLZ_CAST_TIMESTAMP
							 || oper == BLZ_CAST_VARCHAR){
				computed = left_value;
			}else if(oper == BLZ_IS_NULL){
				computed = !left_valid;
			}else if(oper == BLZ_IS_NOT_NULL){
				computed = left_valid;
			}

			if(oper == BLZ_IS_NULL || oper == BLZ_IS_NOT_NULL){
					setColumnValid(row_valids,output_position,true);
			 }else{
					setColumnValid(row_valids,output_position,left_valid);
			 }
			store_data_in_buffer<OutputTypeOperator>(
					computed,
					buffer,
					output_position);
		}


	}

public:
	cudaStream_t get_stream(){
		return this->stream;
	}

	void set_stream(cudaStream_t new_stream){
		this->stream = new_stream;
	}

	int get_buffer_size(){
		return BufferSize;
	}

	int get_thread_block_size(){
		return ThreadBlockSize;
	}
	/*
	 * void  **column_data; //these are device side pointers to the device pointer found in gdf_column.data
	void ** output_data;
	temp_gdf_valid_type ** valid_ptrs; //device
	temp_gdf_valid_type ** valid_ptrs_out;
	size_t num_columns;
	gdf_dtype * input_column_types;
	size_t num_rows;
	column_index_type *  left_input_positions; //device
	column_index_type * right_input_positions; //device
	column_index_type * output_positions; //device
	column_index_type * final_output_positions; //should be same size as output_data, e.g. num_outputs
	short num_final_outputs;
	short num_operations;
	gdf_dtype * input_types_left; //device
	gdf_dtype * input_types_right; //device
	gdf_dtype * output_types; //device
	gdf_dtype * final_output_types; //size
	gdf_binary_operator_exp * binary_operations; //device
	gdf_unary_operator * unary_operations;

	 */


	static size_t get_temp_size(short num_inputs,
			short _num_operations,
			short num_final_outputs){
		size_t space = 0;
		space += sizeof(void *) * num_inputs; //space for array of pointers to column data
		space += sizeof(void *) * num_final_outputs;
		space += sizeof(temp_gdf_valid_type *) *num_inputs; //space for array of pointers  to column validity bitmasks
		space += sizeof(temp_gdf_valid_type *) * num_final_outputs;
		space += sizeof(gdf_dtype) * num_inputs; //space for pointers to types for of input_columns
		space += 3 * (sizeof(short) * _num_operations); //space for pointers to indexes to columns e.g. left_input index, right_input index and output_index
		space += 3 * (sizeof(gdf_dtype) * _num_operations); //space for pointers to types for each input_index, e.g. if input_index = 1 then this value should contain column_1 type
		space += (sizeof(short) * num_final_outputs); //space for pointers to indexes to columns e.g. left_input index, right_input index and output_index
		space += (sizeof(gdf_dtype) * num_final_outputs); //space for pointers to types for each input_index, e.g. if input_index = 1 then this value should contain column_1 type
		space += sizeof(gdf_binary_operator_exp) * _num_operations;
		space += sizeof(gdf_unary_operator) * _num_operations;
		space += sizeof(gdf_size_type) * num_inputs; //space for null_counts_inputs
		space += sizeof(gdf_size_type) * num_final_outputs; //space for null count outputs
		space += sizeof(int64_t) * _num_operations * 2; //space for scalar inputs
		return space;
	}

	// DO NOT USE THIS. This is currently not working due to strange race condition
	// void update_columns_null_count(std::vector<gdf_column *> output_columns){
	// 	gdf_size_type * outputs = new gdf_size_type[output_columns.size()];
	// 	CheckCudaErrors(cudaMemcpyAsync(outputs,this->null_counts_outputs,sizeof(gdf_size_type) * output_columns.size(),cudaMemcpyDeviceToHost,this->stream));
	//	CheckCudaErrors(cudaStreamSynchronize(this->stream));
	// 	for(int i = 0; i < output_columns.size(); i++){
	// 		//std::cout<<"outputs["<<i<<"] = "<<outputs[i]<<std::endl;
	// 		output_columns[i]->null_count = outputs[i];
	// 	}
	// 	delete outputs;
	// }

	//does not perform allocations
	InterpreterFunctor(void  ** column_data, //these are device side pointers to the device pointer found in gdf_column.data
			temp_gdf_valid_type ** valid_ptrs, //device
			size_t num_columns,
			size_t num_rows,
			short * left_input_positions, //device
			short * right_input_positions, //device
			short * output_positions, //device
			short num_operations,
			gdf_dtype * input_types_left, //device
			gdf_dtype * input_types_right, //device
			gdf_dtype * output_types_out, //device
			gdf_binary_operator_exp * binary_operations, //device
			gdf_dtype * input_column_types //device
	){
		this->column_data = column_data;
		this->valid_ptrs = valid_ptrs;
		this->num_columns = num_columns;
		this->num_rows = num_rows;
		this->left_input_positions = left_input_positions;
		this->right_input_positions = right_input_positions;
		this->output_positions = output_positions;
		this->num_operations = num_operations;
		this->input_types_left = input_types_left;
		this->input_types_right = input_types_right;
		this->output_types = output_types;
		this->binary_operations = binary_operations;
		this->input_column_types = input_column_types;
	}

	__host__
	virtual ~InterpreterFunctor(){

	}



	//simpler api and requires allocating just one block of temp space with
	// char * to make sure it can be dereferenced at one byte boundaries
	//This whole phase should take about ~ .1 ms, should
	//be using a stream for all this
	InterpreterFunctor(
			std::vector<gdf_column *> columns,
			std::vector<gdf_column *> output_columns,
			short _num_operations,
			std::vector<column_index_type> left_input_positions_vec,
			std::vector<column_index_type> right_input_positions_vec,
			std::vector<column_index_type> output_positions_vec,
			std::vector<column_index_type> final_output_positions_vec,
			std::vector<gdf_binary_operator_exp> operators,
			std::vector<gdf_unary_operator> unary_operators,
			std::vector<gdf_scalar> & left_scalars, //should be same size as operations with most of them filled in with invalid types unless scalar is used in oepration
			std::vector<gdf_scalar> & right_scalars//,
			,cudaStream_t stream,
			char * temp_space,
			int BufferSize, int ThreadBlockSize
			//char * temp_space
	){

		//TODO: you should be able to make this faster by placing alll the memory in one
		//pinned memory buffer then copying that over

		this->BufferSize = BufferSize;
		this->ThreadBlockSize = ThreadBlockSize;
		this->num_final_outputs = final_output_positions_vec.size();
		this->num_operations = _num_operations;
		this->maxPosition = final_output_positions_vec[final_output_positions_vec.size()-1];
		this->stream = stream;
		num_columns = columns.size();
		num_rows = columns[0]->size;

		//added this to class
		//fuck this allocating is easier and i didnt see a significant differnece in tmie when i tried
		//to put this in constant memory

		//		cudaGetSymbolAddress ( (void**)&cur_temp_space, shared_buffer);
		//		cudaGetSymbolAddress ( (void**)&column_data, shared_buffer);

		char * cur_temp_space = temp_space;

		//VERY IMPORTANT!!!!
		//the temporary space MUST be allocated in space from largest to smallest elements, if you don't follow this pattern you end up in
		// a situation where you can makme misaligned accesses to memory.

		column_data = (void **) cur_temp_space;
		cur_temp_space += sizeof(void *) * num_columns;
		output_data = (void **) cur_temp_space;
		cur_temp_space += sizeof(void *) * num_final_outputs;
		scalars_left = (int64_t *) cur_temp_space;
		cur_temp_space += sizeof(int64_t) * num_operations;
		scalars_right = (int64_t *) cur_temp_space;
		cur_temp_space += sizeof(int64_t) * num_operations;
		valid_ptrs = (temp_gdf_valid_type **) cur_temp_space;
		cur_temp_space += sizeof(temp_gdf_valid_type *) * num_columns;
		valid_ptrs_out = (temp_gdf_valid_type **) cur_temp_space;
		cur_temp_space += sizeof(temp_gdf_valid_type *) * num_final_outputs;
		null_counts_inputs = (gdf_size_type *) cur_temp_space;
		cur_temp_space += sizeof(gdf_size_type) * num_columns;
		null_counts_outputs = (gdf_size_type *) cur_temp_space;
		cur_temp_space += sizeof(gdf_size_type) * num_final_outputs;
		input_column_types = (gdf_dtype *) cur_temp_space;
		cur_temp_space += sizeof(gdf_dtype) * num_columns;
		input_types_left = (gdf_dtype *) cur_temp_space;
		cur_temp_space += sizeof(gdf_dtype) * num_operations;
		input_types_right= (gdf_dtype *) cur_temp_space;
		cur_temp_space += sizeof(gdf_dtype) * num_operations;
		output_types = (gdf_dtype *) cur_temp_space;
		cur_temp_space += sizeof(gdf_dtype) * num_operations;
		final_output_types = (gdf_dtype *) cur_temp_space;
		cur_temp_space += sizeof(gdf_dtype) * num_final_outputs;
		binary_operations = (gdf_binary_operator_exp *) cur_temp_space;
		cur_temp_space += sizeof(gdf_binary_operator_exp) * num_operations;
		unary_operations = (gdf_unary_operator *) cur_temp_space;
		cur_temp_space += sizeof(gdf_unary_operator) * num_operations;
		left_input_positions = (short *) cur_temp_space;
		cur_temp_space += sizeof(short) * num_operations;
		right_input_positions = (short *) cur_temp_space;
		cur_temp_space += sizeof(short) * num_operations;
		output_positions = (short *) cur_temp_space;
		cur_temp_space += sizeof(short) * num_operations;
		final_output_positions = (short *) cur_temp_space;
		cur_temp_space += sizeof(short) * num_final_outputs;




		std::vector<void *> host_data_ptrs(num_columns);
		std::vector<temp_gdf_valid_type *> host_valid_ptrs(num_columns);
		std::vector<gdf_size_type> host_null_counts(num_columns);
		std::vector<gdf_size_type> host_null_counts_output(num_final_outputs,0);

		for(std::size_t i = 0; i < num_columns; i++){
			host_data_ptrs[i] = columns[i]->data;
			host_valid_ptrs[i] = (temp_gdf_valid_type *) columns[i]->valid;
			host_null_counts[i] = columns[i]->null_count;
		}

		CheckCudaErrors(cudaMemcpyAsync(this->column_data,&host_data_ptrs[0],sizeof(void *) * num_columns,cudaMemcpyHostToDevice,stream));
		//	CheckCudaErrors(cudaMemcpy(this->column_data,&host_data_ptrs[0],sizeof(void *) * num_columns,cudaMemcpyHostToDevice));
		//	std::cout<<"about to copy host valid"<<error<<std::endl;
		//	CheckCudaErrors(cudaMemcpy(this->valid_ptrs,&host_valid_ptrs[0],sizeof(void *) * num_columns,cudaMemcpyHostToDevice));
		CheckCudaErrors(cudaMemcpyAsync(this->valid_ptrs,&host_valid_ptrs[0],sizeof(void *) * num_columns,cudaMemcpyHostToDevice,stream));
		CheckCudaErrors(cudaMemcpyAsync(this->null_counts_inputs,&host_null_counts[0],sizeof(gdf_size_type) * num_columns,cudaMemcpyHostToDevice,stream));
		CheckCudaErrors(cudaMemcpyAsync(this->null_counts_outputs,&host_null_counts_output[0],sizeof(gdf_size_type) * num_final_outputs,cudaMemcpyHostToDevice,stream));
		//	std::cout<<"copied data and valid"<<error<<std::endlnum_final_outputs


		host_data_ptrs.resize(num_final_outputs);
		host_valid_ptrs.resize(num_final_outputs);

		for(int i = 0; i < num_final_outputs; i++){
			host_data_ptrs[i] = output_columns[i]->data;
			host_valid_ptrs[i] = (temp_gdf_valid_type *) output_columns[i]->valid;
		}
		//	CheckCudaErrors(cudaMemcpy(this->output_data,&host_data_ptrs[0],sizeof(void *) * num_final_outputs,cudaMemcpyHostToDevice));

		CheckCudaErrors(cudaMemcpyAsync(this->output_data,&host_data_ptrs[0],sizeof(void *) * num_final_outputs,cudaMemcpyHostToDevice,stream));

		//	std::cout<<"about to copy host valid"<<error<<std::endl;
		//		CheckCudaErrors(cudaMemcpy(this->valid_ptrs_out,&host_valid_ptrs[0],sizeof(void *) * num_final_outputs,cudaMemcpyHostToDevice));
		CheckCudaErrors(cudaMemcpyAsync(this->valid_ptrs_out,&host_valid_ptrs[0],sizeof(void *) * num_final_outputs,cudaMemcpyHostToDevice,stream));


		//copy over inputs

		std::vector<gdf_dtype> left_input_types_vec(num_operations);
		std::vector<gdf_dtype> right_input_types_vec(num_operations);
		std::vector<gdf_dtype> output_types_vec(num_operations);
		std::vector<gdf_dtype> output_final_types_vec(num_final_outputs);
		std::vector<int64_t> left_scalars_host(num_operations);
		std::vector<int64_t> right_scalars_host(num_operations);




		//stores index to type map so we can retrieve this if we need to
		std::map<size_t,gdf_dtype> output_map_type;

		for(int cur_operation = 0; cur_operation < num_operations; cur_operation++){
			column_index_type left_index = left_input_positions_vec[cur_operation];
			column_index_type right_index = right_input_positions_vec[cur_operation];
			column_index_type output_index = output_positions_vec[cur_operation];

			if( left_index < static_cast<column_index_type>(columns.size()) && left_index >= 0){
				left_input_types_vec[cur_operation] = columns[left_index]->dtype;
			}else{
				if(left_index < 0 ){
					if(left_index == -3){
						//this was a null scalar in left weird but ok
						left_input_types_vec[cur_operation] = left_scalars[cur_operation].dtype;
					}else if(left_index == -2){
						//get scalars type
						if(isInt(left_scalars[cur_operation].dtype)){
							left_input_types_vec[cur_operation] = GDF_INT64;

						}else{
							left_input_types_vec[cur_operation] = GDF_FLOAT64;
						}

						left_scalars_host[cur_operation] = scale_to_64_bit_return_bytes(left_scalars[cur_operation]);
					}
				}else{
					//have to get it from the output that generated it
					left_input_types_vec[cur_operation] = output_map_type[left_index];
				}

			}

			if( right_index < static_cast<column_index_type>(columns.size()) && right_index >= 0){
				right_input_types_vec[cur_operation] = columns[right_index]->dtype;
			}else{
				if(right_index < 0 ){
					if(right_index == -3){
						//this was a null scalar weird but ok
						//TODO: figure out if we have to do anything here, i am sure we will for coalesce
						right_input_types_vec[cur_operation] = right_scalars[cur_operation].dtype;
					}else if(right_index == -2){
						//get scalars type

						if(isInt(right_scalars[cur_operation].dtype)){
							right_input_types_vec[cur_operation] = GDF_INT64;
						}else{
							right_input_types_vec[cur_operation] = GDF_FLOAT64;
						}
						right_scalars_host[cur_operation] = scale_to_64_bit_return_bytes(right_scalars[cur_operation]);
					}else if(right_index == -1){
						//right wont be used its a unary operation
					}
				}else{
					right_input_types_vec[cur_operation] = output_map_type[right_index];
				}


				//		std::cout<<"right type was "<<right_input_types_vec[cur_operation]<<std::endl;
			}

			gdf_dtype type_from_op;
			if(right_index == -1){
				//unary
				type_from_op = get_output_type(left_input_types_vec[cur_operation],
						unary_operators[cur_operation]);

			}else{
				//binary
				type_from_op = get_output_type(left_input_types_vec[cur_operation],
						right_input_types_vec[cur_operation],
						operators[cur_operation]);

			}

			//		std::cout<<"type from op was "<<type_from_op<<std::endl;
			if(is_type_signed(type_from_op) && !(is_type_float(type_from_op))){
				output_types_vec[cur_operation] = GDF_INT64;
			}else if(is_type_float(type_from_op)){
				output_types_vec[cur_operation] = GDF_FLOAT64;
			}
			//		std::cout<<"op will be "<<output_types_vec[cur_operation]<<std::endl;


			output_map_type[output_index] = output_types_vec[cur_operation];

		}


		//put the output final positions
		for(int output_index = 0; output_index < num_final_outputs; output_index++){
			output_final_types_vec[output_index] = output_columns[output_index]->dtype;
		}

		std::vector<gdf_dtype> input_column_types_vec(num_columns);
		for(std::size_t column_index = 0; column_index < columns.size(); column_index++){
			input_column_types_vec[column_index] = columns[column_index]->dtype;
			//		std::cout<<"type was "<<input_column_types_vec[column_index]<<std::endl;
		}


		CheckCudaErrors(cudaMemcpyAsync (left_input_positions,
				&left_input_positions_vec[0],
				left_input_positions_vec.size() * sizeof(short),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (right_input_positions,
				&right_input_positions_vec[0],
				right_input_positions_vec.size() * sizeof(short),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (output_positions,
				&output_positions_vec[0],
				output_positions_vec.size() * sizeof(short),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (final_output_positions,
				&final_output_positions_vec[0],
				final_output_positions_vec.size() * sizeof(short),cudaMemcpyHostToDevice,stream));
		/*
		thrust::copy(left_input_positions_vec.begin(),left_input_positions_vec.end(),thrust::device_pointer_cast(left_input_positions));
		thrust::copy(right_input_positions_vec.begin(),right_input_positions_vec.end(),thrust::device_pointer_cast(right_input_positions));
		thrust::copy(output_positions_vec.begin(),output_positions_vec.end(),thrust::device_pointer_cast(output_positions));
		thrust::copy(final_output_positions_vec.begin(),final_output_positions_vec.end(),thrust::device_pointer_cast(final_output_positions));

			*/

		CheckCudaErrors(cudaMemcpyAsync (binary_operations,
				&operators[0],
				operators.size() * sizeof(gdf_binary_operator_exp),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (unary_operations,
				&unary_operators[0],
				unary_operators.size() * sizeof(gdf_unary_operator),cudaMemcpyHostToDevice,stream));

		//	thrust::copy(operators.begin(),operators.end(),thrust::device_pointer_cast(binary_operations));

		CheckCudaErrors(cudaMemcpyAsync (input_column_types,
				&input_column_types_vec[0],
				input_column_types_vec.size() * sizeof(gdf_dtype),cudaMemcpyHostToDevice,stream));


		//	thrust::copy(input_column_types_vec.begin(), input_column_types_vec.end(), thrust::device_pointer_cast(input_column_types));


		CheckCudaErrors(cudaMemcpyAsync (input_types_left,
				&left_input_types_vec[0],
				left_input_types_vec.size() * sizeof(gdf_dtype),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (input_types_right,
				&right_input_types_vec[0],
				right_input_types_vec.size() * sizeof(gdf_dtype),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (output_types,
				&output_types_vec[0],
				output_types_vec.size() * sizeof(gdf_dtype),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (final_output_types,
				&output_final_types_vec[0],
				output_final_types_vec.size() * sizeof(gdf_dtype),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (scalars_left,
				&left_scalars_host[0],
				left_scalars_host.size() * sizeof(int64_t),cudaMemcpyHostToDevice,stream));

		CheckCudaErrors(cudaMemcpyAsync (scalars_right,
				&right_scalars_host[0],
				right_scalars_host.size() * sizeof(int64_t),cudaMemcpyHostToDevice,stream));
		/*
		thrust::copy(left_input_types_vec.begin(),left_input_types_vec.end(), thrust::device_pointer_cast(input_types_left));
		thrust::copy(right_input_types_vec.begin(),right_input_types_vec.end(), thrust::device_pointer_cast(input_types_right));
		thrust::copy(output_types_vec.begin(),output_types_vec.end(), thrust::device_pointer_cast(output_types));
		thrust::copy(output_final_types_vec.begin(),output_final_types_vec.end(), thrust::device_pointer_cast(final_output_types));
			*/

		//			std::cout<<"copied data!"<<std::endl;
		CheckCudaErrors(cudaStreamSynchronize(stream));
	}

	__device__ __forceinline__ short get_num_outputs(){
		return this->num_final_outputs;
	}

__device__ __forceinline__ void copyRowValidsIntoBuffer(const int64_t & row_valid, int64_t * valids_out_buffer, const gdf_size_type & row_index){
	for(column_index_type out_index = 0; out_index < this->num_final_outputs; out_index++ ){
		if(this->valid_ptrs_out[out_index] != nullptr){

			setColumnValid(valids_out_buffer[out_index],row_index,
				getColumnValid(row_valid,this->final_output_positions[out_index]));
		}

	}
}
__device__ __forceinline__ void copyRowValidsIntoGlobal( int64_t * valids_out_buffer, const gdf_size_type & row_index){
	for(column_index_type out_index = 0; out_index < this->num_final_outputs; out_index++ ){
		if(this->valid_ptrs_out[out_index] != nullptr){

			write_valid_data(out_index, valids_out_buffer[out_index],row_index);

		}

	}
}
__device__ __forceinline__
void load_cur_row_valids(int64_t valids_in_buffer[],gdf_size_type row,int64_t & cur_row_valids, column_index_type num_columns){
	for(column_index_type cur_column = 0; cur_column < num_columns; cur_column++ ){
		setColumnValid(cur_row_valids,cur_column,
			getColumnValid(valids_in_buffer[cur_column],row));
	}
}

	__device__ __forceinline__ void operator()(const IndexT row_index, int64_t total_buffer[], int64_t * valids_in_buffer, int64_t * valids_out_buffer, gdf_size_type size) {
		//		__shared__ char buffer[BufferSize * THREADBLOCK_SIZE];

		int64_t cur_row_valids;
//TODO: enable when we process null counts in this kernel
//		gdf_size_type null_counts[this->num_final_outputs];

		bool is64Thread = blockIdx.x == 0 && threadIdx.x == 64;
		if (is64Thread){
			printf("====> operator() blockIdx: %d threadIdx: %d row_index: %d\n", blockIdx.x, threadIdx.x, row_index);
		}


		for(column_index_type cur_column = 0; cur_column < this->num_columns; cur_column++ ){

			if(this->valid_ptrs[cur_column] == nullptr || this->null_counts_inputs[cur_column] == 0){

				valids_in_buffer[cur_column] = -1;
			}else{
				if (is64Thread) {
					printf("====> operator() -- Inside loop read_valid_data: %d\n", cur_column);
				}
				
				read_valid_data(cur_column,valids_in_buffer, row_index);
			}

		}

		if (is64Thread){
			printf("====> operator() -- after read_valid_data()\n");
		}

		for(gdf_size_type row = 0; row < 64 && row_index + row < size; row++){

			if (is64Thread){
				gdf_size_type temp = (row_index + row);
				printf("====> operator() -- Inside loop -- before -- load_cur_row_valids(): row: %d (row_index+row): %d\n", row, temp);
			}

			load_cur_row_valids(valids_in_buffer,row,cur_row_valids,this->num_columns);

			if (is64Thread){
				printf("====> operator() -- Inside loop -- after -- load_cur_row_valids(): %d\n", row);
			}

			for(short cur_column = 0; cur_column < this->num_columns; cur_column++ ){
				read_data(cur_column,total_buffer, row_index + row);

			}


			for(short op_index = 0; op_index < this->num_operations; op_index++ ){
				process_operator(op_index, total_buffer, row_index + row, cur_row_valids );
			}

			//		#pragma unroll
			for(int out_index = 0; out_index < this->num_final_outputs; out_index++ ){
				write_data(out_index,this->final_output_positions[out_index],total_buffer,row_index + row);
			}

			if (is64Thread){
				printf("====> operator() -- Inside loop -- before -- copyRowValidsIntoBuffer(): %d\n", row);
			}
			copyRowValidsIntoBuffer(cur_row_valids,valids_out_buffer,row);

			if (is64Thread){
				printf("====> operator() -- Inside loop -- after -- copyRowValidsIntoBuffer(): %d\n", row);
			}

			copyRowValidsIntoBuffer(cur_row_valids,valids_out_buffer,row);
		}

		if (is64Thread){
			printf("====> operator() -- before copyRowValidsIntoGlobal\n");
		}

		//write out valids here
		copyRowValidsIntoGlobal( valids_out_buffer, row_index);

		if (is64Thread){
			printf("====> operator() -- after copyRowValidsIntoGlobal\n");
		}

	}


};


/*

void delete_gdf_column(gdf_column col){
	cudaFree(col.data);
	cudaFree(col.valid);
}

gdf_column create_gdf_column(gdf_dtype type, size_t num_values, void * input_data, size_t width_per_value)
{

	cudaError_t error;
	gdf_column column;
	char * data;
	temp_gdf_valid_type * valid_device;

	size_t allocated_size_valid = ((((num_values + 7 ) / 8) + 63 ) / 64) * 64; //so allocations are supposed to be 64byte aligned
	std::cout<<"allocating valid "<<allocated_size_valid<<std::endl;
	error = cudaMalloc( &valid_device, allocated_size_valid);
	std::cout<<"allocated valid "<<allocated_size_valid<<std::endl;
	//	std::cout<<"allocated device valid"<<error<<std::endl;
	cudaMemset(valid_device, (temp_gdf_valid_type)255, allocated_size_valid); //assume all relevant bits are set to on

	size_t allocated_size_data = (((width_per_value * num_values) + 63 )/64) * 64;
	std::cout<<"allocating data "<<allocated_size_data<<std::endl;
	cudaMalloc( &data, allocated_size_data);
	std::cout<<"allocated data "<<allocated_size_data<<std::endl;
	gdf_column_view(&column, (void *) data, valid_device, num_values, type);

	if(input_data != nullptr){

		cudaMemcpy(data, input_data, num_values * width_per_value, cudaMemcpyHostToDevice);
		//		std::cout<<"copied memory"<<error<<std::endl;
	}
	return column;

}
 */


//TODO: consider running valids at the same time as the normal
// operations to increase throughput
template<typename interpreted_operator>
__global__ void transformKernel(interpreted_operator op, gdf_size_type size)
{

	extern __shared__  int64_t  total_buffer[];

	//TODO: this is sort of lazy to do this really nicely we should allocate this space before hand
	//and let every thread get some offset based on the number of elements each thread needs
	int64_t * valids_in_buffer = new int64_t[op.num_columns];
	int64_t * valids_out_buffer = new int64_t[op.num_final_outputs]; //my thinking here is by putting it into a temp buffer that is small
													//we will be able to ensure that most accesses to this buffer happen in cache
													//this way its only every 64 rows that we copy it out to global
	
	if (valids_in_buffer == nullptr) {
		printf("valids_in_buffer is nullptr in blockIdx.x: %d threadIdx.x: %d\n", blockIdx.x, threadIdx.x);
	}
	
	if (valids_out_buffer == nullptr) {
		printf("valids_out_buffer is nullptr in blockIdx.x: %d threadIdx.x: %d\n", blockIdx.x, threadIdx.x);
	}

	for (gdf_size_type i = (blockIdx.x * blockDim.x + threadIdx.x) * 64;
			i < size;
			i += blockDim.x * gridDim.x * 64)
	{
			op(i,total_buffer,valids_in_buffer, valids_out_buffer,size);
	}

	delete[] valids_in_buffer;
	delete[] valids_out_buffer;

	return;
}

/*
class Timer
{
public:
	Timer() : beg_(clock_::now()) {}
	void reset() { beg_ = clock_::now(); }
	int64_t elapsed() const {
		return (std::chrono::high_resolution_clock::now() - beg_).count();
	}
private:
	typedef std::chrono::high_resolution_clock clock_;

	std::chrono::time_point<clock_> beg_;
};


const int THREAD_BLOCK_SIZE = 512;
int main(void)
{


	cudaDeviceProp prop;
	cudaGetDeviceProperties(&prop, 0);


	printf("  (%2d) Multiprocessors, (%3d) CUDA Cores/MP:     %d CUDA Cores\n",
			prop.multiProcessorCount,
			_ConvertSMVer2Cores(prop.major, prop.minor),
			_ConvertSMVer2Cores(prop.major, prop.minor) *
			prop.multiProcessorCount);

	int number_of_sms = prop.multiProcessorCount;


	typedef InterpreterFunctor<size_t,8,THREAD_BLOCK_SIZE> interpreter_functor;
	size_t limit = 10000001; //60000000
	for(size_t num_rows = 10000000; num_rows < limit; num_rows += 10000000){
		std::cout<<std::endl<<std::endl<<std::endl<<"############################################################"<<std::endl;
		std::cout<<"Running with "<<num_rows<<" rows!"<<std::endl;
		std::cout<<"############################################################"<<std::endl;

		std::vector<gdf_column> columns(2);

		//make 2 gdf columns

		int* input_data = new int[num_rows];
		for(int i = 0; i < num_rows; i++){
			input_data[i] = i;
		}

		double* input_data_dbl = new double[num_rows];
		for(int i = 0; i < num_rows; i++){
			input_data_dbl[i] = ((double)i) + .5;
		}

		columns[0] = create_gdf_column(GDF_INT32, num_rows, (void *) input_data, 4);
		columns[1] = create_gdf_column(GDF_FLOAT64, num_rows, (void *) input_data_dbl, 8);

		std::cout<<"Created columns"<<std::endl;



		std::vector<gdf_column> output_columns(2);
		output_columns[0] = create_gdf_column(GDF_FLOAT64, num_rows, nullptr, 8);
		output_columns[1] = create_gdf_column(GDF_FLOAT64, num_rows, nullptr, 8);
		std::cout<<"created op no making a vector of size"<<num_rows<<std::endl;


		Timer timer;

		std::vector<short> left_inputs = { 0 , 2, 1};
		std::vector<short> right_inputs = { 1,  1, 3};
		std::vector<short> outputs { 2, 3, 2 };
		std::vector<short> final_output_positions { 1, 3 };
		std::vector<gdf_binary_operator_exp> operators = { GDF_ADD, GDF_MUL};
		//		char * temp_space;
		size_t num_final_outputs = 2;
		//	size_t temp_space_needed = interpreter_functor::get_temp_size(columns,2,left_inputs,right_inputs,outputs,operators,num_final_outputs);
		//	std::cout<<"Planning phase ==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;
		//	std::cout<<"need this much temp space"<<temp_space_needed<<std::endl;
		//	cudaMalloc(&temp_space,temp_space_needed);
		//	std::cout<<"allocted temp space"<<std::endl;







		interpreter_functor op(columns,output_columns,2,left_inputs,right_inputs,outputs,final_output_positions,operators);


		thrust::counting_iterator<size_t> iota_iter(0);


		std::cout<<"Planning phase ==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;
		//		thrust::for_each(iota_iter, iota_iter + num_rows, op);
		//thrust::transform(thrust::cuda::par,iota_iter, iota_iter + num_rows, thrust::device_pointer_cast((double *)output_column.data), op);
		cudaDeviceSynchronize();
		//		std::cout<<"Total Time==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;


		transformKernel<<<32 * number_of_sms,THREAD_BLOCK_SIZE>>>(op, num_rows);
		cudaDeviceSynchronize();
		std::cout<<"Total Time kernel==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;
		std::cout<<"column 0 from operator"<<std::endl;
		thrust::copy(thrust::device_pointer_cast((double *)output_columns[0].data),
				thrust::device_pointer_cast((double *)output_columns[0].data) + num_rows,
				data_out_host.begin());

		for(int i = 0; i < num_rows; i++){
			if(i == 1 || i == (num_rows - 1)){
				std::cout<<"Row "<<i<<" = "<<data_out_host[i]<<std::endl;
			}
		}

		std::cout<<"column 1 from operator"<<std::endl;
		thrust::copy(thrust::device_pointer_cast((double *)output_columns[1].data),
				thrust::device_pointer_cast((double *)output_columns[1].data) + num_rows,
				data_out_host.begin());

		for(int i = 0; i < num_rows; i++){
			if(i == 1 || i == (num_rows - 1)){
				std::cout<<"Row "<<i<<" = "<<data_out_host[i]<<std::endl;
			}

		}
		delete_gdf_column(output_columns[0]);
		delete_gdf_column(output_columns[1]);
		cudaDeviceSynchronize();
		timer.reset();
		gdf_column output_temp = create_gdf_column(GDF_FLOAT64, num_rows, nullptr, 8);
		gdf_column output_final = create_gdf_column(GDF_FLOAT64, num_rows, nullptr, 8);
		cudaDeviceSynchronize();
		std::cout<<"temp allocations ==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;
		//	gdf_binary_operation_v_v_v(&output_temp, &columns[0], &columns[1],GDF_ADD);
		//	gdf_binary_operation_v_v_v(&output_final, &output_temp, &columns[1],GDF_MUL);

		thrust::transform(thrust::cuda::par,thrust::device_pointer_cast((int *) columns[0].data),
				thrust::device_pointer_cast((int *) columns[0].data) + num_rows,
				thrust::device_pointer_cast((double *) columns[1].data),
				thrust::device_pointer_cast((double *) output_temp.data),
				thrust::plus<double>());

		thrust::transform(thrust::cuda::par,thrust::device_pointer_cast((double *) output_temp.data),
				thrust::device_pointer_cast((double *) output_temp.data) + num_rows,
				thrust::device_pointer_cast((double *) columns[1].data),
				thrust::device_pointer_cast((double *) output_final.data),
				thrust::multiplies<double>());

		cudaDeviceSynchronize();
		std::cout<<"total time thrust ==>"<<(double)timer.elapsed() / 1000000.0<<"ms"<<std::endl;
		thrust::copy(thrust::device_pointer_cast((double *)output_final.data),
				thrust::device_pointer_cast((double *)output_final.data) + num_rows,
				data_out_host.begin());


		for(int i = 0; i < num_rows; i++){
			if(i == 1 || i == (num_rows - 1)){
				std::cout<<"thrust Row "<<i<<" = "<<data_out_host[i]<<std::endl;
			}

		}



		delete_gdf_column(columns[0]);
		delete_gdf_column(columns[1]);
		delete_gdf_column(output_temp);
		delete_gdf_column(output_final);

	}



	return 0;
}
 */
