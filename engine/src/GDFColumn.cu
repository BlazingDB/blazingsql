/*
 * GDFColumn.cu
 *
 *  Created on: Sep 12, 2018
 *      Author: rqc
 */

#include <arrow/util/bit_util.h>

//readme:  use bit-utils to compute valid.size in a standard way
// see https://github.com/apache/arrow/blob/e34057c4b4be8c7abf3537dd4998b5b38919ba73/cpp/src/arrow/ipc/writer.cc#L66

#include <cudf.h>
#include "GDFColumn.cuh"
#include "gdf_wrapper/gdf_wrapper.cuh"
#include "cuDF/Allocator.h"
#include "cuio/parquet/util/bit_util.cuh"
#include "Traits/RuntimeTraits.h"

#include <cudf/legacy/bitmask.hpp>

gdf_column_cpp::gdf_column_cpp()
{
}

gdf_column_cpp::gdf_column_cpp(const gdf_column_cpp& col)
{
    column = col.column;
    this->set_name(col.column_name);
    
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
	//GDFRefCounter::getInstance()->increment(const_cast<gdf_column*>(col.column));
}

void gdf_column_cpp::set_name(std::string name){
	this->column_name = name;
}

std::string gdf_column_cpp::name() const{
	return this->column_name;
}

gdf_column_cpp gdf_column_cpp::clone(std::string name, bool register_column)
{
	char * data_dev = nullptr;
	char * valid_dev = nullptr;

    if (this->column->type().id() == cudf::type_id::STRING){
        throw std::runtime_error("In gdf_column_cpp::clone unsupported data type GDF_STRING");
    }

	gdf_column_cpp col1;
    col1.column = this->column;
	col1.set_name(this->column_name);

	return col1;
}

void gdf_column_cpp::operator=(const gdf_column_cpp& col)
{
    if (column == col.column) {
        return;
    }

	column = col.column;
    this->set_name(col.column_name);
}

cudf::column* gdf_column_cpp::get_gdf_column() const
{
    return column;
}

void gdf_column_cpp::create_gdf_column(NVCategory* category, size_t num_values,std::string column_name){

	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//    decrement_counter(column);

//    //TODO crate column here
//    this->column = new gdf_column{};
//    cudf::type_id type = cudf::type_id::CATEGORY;
//    gdf_column_view(this->column, nullptr, nullptr, num_values, type);

//    this->column->dtype_info.category = (void*) category;
//    this->column->dtype_info.time_unit = TIME_UNIT_NONE; // TODO this should not be hardcoded

//    this->allocated_size_data = sizeof(nv_category_index_type) * this->column->size;
//    cuDF::Allocator::allocate((void**)&this->column->data, this->allocated_size_data);
//    CheckCudaErrors( cudaMemcpy(
//        this->column->data,
//        category->values_cptr(),
//        this->allocated_size_data,
//        cudaMemcpyDeviceToDevice) );

//    this->column->valid = nullptr;
//    this->allocated_size_valid = 0;
//    this->column->null_count = 0;

//    if (category->has_nulls()) {
//        this->column->valid = allocate_valid();
//        this->column->null_count = category->set_null_bitarray((cudf::valid_type*)this->column->valid);
//    }

//    this->is_ipc_column = false;
//    this->column_token = 0;
//    this->set_name(column_name);

}

void gdf_column_cpp::create_gdf_column(NVStrings* strings, size_t num_values, std::string column_name) {
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
	
//    //TODO crate column here
//    this->column = new gdf_column{};
//    gdf_column_view(this->column, static_cast<void*>(strings), nullptr, num_values, cudf::type_id::STRING);

//    this->allocated_size_data = 0; // TODO: do we care? what should be put there?
//    this->allocated_size_valid = 0;

//    this->is_ipc_column = false;
//    this->column_token = 0;
//    this->set_name(column_name);
}

void gdf_column_cpp::create_gdf_column(cudf::type_id type, size_t num_values, void * input_data, cudf::valid_type * host_valids, size_t width_per_value, const std::string &column_name)
{
	// TODO percy cudf0.12 port to cudf::column with auto ptrs

//	// TODO percy cudf0.12 was invalid here, should we consider empty?
//    assert(type != cudf::type_id::EMPTY);

//    this->column = new gdf_column{};

//    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
//    //needing to not require numvalues so it can be called rom outside
//    this->get_gdf_column()->size = num_values;
//    char * data = nullptr;
//    this->is_ipc_column = false;
//    this->column_token = 0;

//    this->allocated_size_data = (width_per_value * num_values);
//    this->allocated_size_valid = 0;

//    cudf::valid_type * valid_device = nullptr;

//    if (num_values > 0) {
//        cuDF::Allocator::allocate((void**)&data, allocated_size_data);

//        if(host_valids != nullptr){
//            valid_device = allocate_valid();
//            CheckCudaErrors(cudaMemcpy(valid_device, host_valids, this->allocated_size_valid, cudaMemcpyHostToDevice));
//        }
//    }

//    gdf_column_view(this->column, (void *) data, valid_device, num_values, type);

//    this->set_name(column_name);
//    if(input_data != nullptr){
//        CheckCudaErrors(cudaMemcpy(data, input_data, num_values * width_per_value, cudaMemcpyHostToDevice));
//    }

//    if(host_valids != nullptr){
//        this->update_null_count();
//    }
}

//Todo: Verificar que al llamar mas de una vez al create_gdf_column se desaloque cualquier memoria alocada anteriormente
void gdf_column_cpp::create_gdf_column(cudf::type_id type, size_t num_values, void * input_data, size_t width_per_value, const std::string &column_name, bool allocate_valid_buffer )
{
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//	// TODO percy cudf0.12 was invalid here, should we consider empty?
//    assert(type != cudf::type_id::EMPTY);
//    decrement_counter(column);

//    this->column = new gdf_column{};

//    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
//    //needing to not require numvalues so it can be called rom outside
//    this->get_gdf_column()->size = num_values;
//    char * data = nullptr;
//    this->is_ipc_column = false;
//    this->column_token = 0;

//    this->allocated_size_data = (width_per_value * num_values);
//    this->allocated_size_valid = 0;

//    cudf::valid_type * valid_device = nullptr;

//    if (num_values > 0) {
//        if (type != GDF_STRING){
//            valid_device = allocate_valid();
//        }

//        cuDF::Allocator::allocate((void**)&data, allocated_size_data);
//    }

//		if(allocate_valid_buffer){
//			this->allocate_set_valid();
//		}
//    gdf_column_view(this->column, (void *) data, valid_device, num_values, type);

//    this->set_name(column_name);
//    if(input_data != nullptr){
//        CheckCudaErrors(cudaMemcpy(data, input_data, num_values * width_per_value, cudaMemcpyHostToDevice));
//    }
}

void gdf_column_cpp::create_gdf_column(cudf::column * column, bool registerColumn){
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//    if (column != this->column) { // if this gdf_column_cpp already represented this gdf_column, we dont want to do anything. Especially do decrement_counter, since it might actually free the gdf_column we are trying to use
//        decrement_counter(this->column);

//        this->column = column;

//        if (column->dtype != GDF_STRING){
//            int width_per_value = ral::traits::get_dtype_size_in_bytes(column);

//            //TODO: we are assuming they are not padding,
//            this->allocated_size_data = width_per_value * column->size;
//        } else {
//            this->allocated_size_data = 0; // TODO: do we care? what should be put there?
//        }
//        if(column->valid != nullptr){
//            this->allocated_size_valid = gdf_valid_allocation_size(column->size); //so allocations are supposed to be 64byte aligned
//        } else {
//            this->allocated_size_valid = 0;
//        }
//        this->is_ipc_column = false;
//        this->column_token = 0;
//        if (column->col_name)
//            this->set_name(std::string(column->col_name));
//				if(registerColumn){
//        	GDFRefCounter::getInstance()->register_column(this->column);
//				}

//    }
}

void gdf_column_cpp::create_gdf_column(const std::unique_ptr<cudf::scalar> & scalar, const std::string &column_name){
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//	// TODO percy cudf0.12 was invalid here, should we consider empty?
//    assert(scalar->type().id() != cudf::type_id::EMPTY);

//    this->column = new gdf_column{};

//    cudf::type_id type = scalar->type().id();

//    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
//    //needing to not require numvalues so it can be called rom outside
//    this->get_gdf_column()->size = 1;
//    this->get_gdf_column()->dtype = to_gdf_type(type);
//    char * data;
//    this->is_ipc_column = false;
//    this->column_token = 0;
//    size_t width_per_value = ral::traits::get_dtype_size_in_bytes(type);

//    this->allocated_size_data = width_per_value;

//    cuDF::Allocator::allocate((void**)&data, allocated_size_data);

//    cudf::valid_type * valid_device = nullptr;
//    if(!scalar->is_valid()){
//        valid_device = allocate_valid();
//        CheckCudaErrors(cudaMemset(valid_device, 0, this->allocated_size_valid));
//         this->get_gdf_column()->null_count = 1;
//    } else {
//        this->allocated_size_valid = 0;
//        this->get_gdf_column()->null_count = 0;
//    }
//    this->get_gdf_column()->data = (void *) data;
//    this->get_gdf_column()->valid = valid_device;

//    this->set_name(column_name);
//    if(scalar->is_valid()){
//        if(type == cudf::type_id::INT8){
//			// TODO percy cudf0.12 implement proper scalar support			
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si08), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::INT16){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si16), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::INT32){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si32), width_per_value, cudaMemcpyHostToDevice));
//		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
//        }else if(type == cudf::type_id::TIMESTAMP_DAYS){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.dt32), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::INT64 ){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si64), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::TIMESTAMP_SECONDS){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.dt64), width_per_value, cudaMemcpyHostToDevice));
//        } else if(type == cudf::type_id::TIMESTAMP_MILLISECONDS){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.tmst), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::FLOAT32){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.fp32), width_per_value, cudaMemcpyHostToDevice));
//        }else if(type == cudf::type_id::FLOAT64){
//			// TODO percy cudf0.12 implement proper scalar support
//            //CheckCudaErrors(cudaMemcpy(data, &(scalar.data.fp64), width_per_value, cudaMemcpyHostToDevice));
//        }
//    }
}

void gdf_column_cpp::create_empty(const cudf::type_id     dtype,
                                  const std::string & column_name) {
    if (cudf::type_id::STRING == dtype || cudf::type_id::CATEGORY == dtype ) {  // cudf::size_of doesn't support GDF_STRING
        create_gdf_column(
            NVCategory::create_from_array(nullptr, 0), 0, column_name);
    } else {
        create_gdf_column(
            dtype, 0, nullptr, ral::traits::get_dtype_size_in_bytes(dtype), column_name);
    }
}

void gdf_column_cpp::create_empty(const cudf::type_id dtype) {
    create_empty(dtype, "");
}

void gdf_column_cpp::allocate_like(const gdf_column_cpp& other){
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//    this->column = new gdf_column{};

//    this->is_ipc_column = false;
//    this->column_token = 0;

//    this->allocated_size_data = other.allocated_size_data;
//    this->allocated_size_valid = other.allocated_size_valid;

//    char * data = nullptr;
//    if (this->allocated_size_data > 0) {
//        cuDF::Allocator::allocate((void**)&data, this->allocated_size_data);
//    }

//    cudf::valid_type * valid_device = nullptr;
//    if(this->allocated_size_valid > 0) {
//        cuDF::Allocator::allocate((void**)&valid_device,  this->allocated_size_valid);
//    }

//    gdf_column_view(this->column, (void *) data, valid_device, other.size(), other.dtype());
//    this->column->dtype_info.category = nullptr;
//    this->column->dtype_info.time_unit = TIME_UNIT_NONE; // TODO this should not be hardcoded

//    this->set_name("");
}

/*
void gdf_column_cpp::realloc_gdf_column(cudf::type_id type, size_t size, size_t width){
	const std::string col_name = this->column_name;
    GDFRefCounter::getInstance()->decrement(&this->column); //decremeting reference, deallocating space

	this->create_gdf_column(type, size, nullptr, width, col_name);
}*/

gdf_error gdf_column_cpp::gdf_column_view(cudf::column *column, void *data, cudf::valid_type *valid, cudf::size_type size, cudf::type_id dtype)
{
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
//    column->data = data;
//    column->valid = valid;
//    column->size = size;
//    column->dtype = to_gdf_type(dtype);
//    column->null_count = 0;

    return GDF_SUCCESS;
}

gdf_column_cpp::~gdf_column_cpp()
{
	// TODO percy cudf0.12 port to cudf::column with auto ptrs
	delete this->column;
}
