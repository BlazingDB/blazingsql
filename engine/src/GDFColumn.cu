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
	column = nullptr;
    this->allocated_size_data = 0;
    this->allocated_size_valid = 0;
    this->is_ipc_column = false;
    this->column_token = 0;
}


gdf_column_cpp::gdf_column_cpp(const gdf_column_cpp& col)
{
    column = col.column;
    this->allocated_size_data = col.allocated_size_data;
    this->allocated_size_valid = col.allocated_size_valid;
    this->set_name(col.column_name);
    this->is_ipc_column = col.is_ipc_column;
    this->column_token = col.column_token;
    GDFRefCounter::getInstance()->increment(const_cast<gdf_column*>(col.column));

}

void gdf_column_cpp::set_name(std::string name){
	this->column_name = name;
    if(this->column){
	    this->column->col_name = const_cast<char*>(this->column_name.c_str());
    }
}

void gdf_column_cpp::set_name_cpp_only(std::string name){
	this->column_name = name;
}

std::string gdf_column_cpp::name() const{
	return this->column_name;
}

gdf_column_cpp gdf_column_cpp::clone(std::string name, bool register_column)
{
	char * data_dev = nullptr;
	char * valid_dev = nullptr;

    if (this->column->dtype == GDF_STRING){
        throw std::runtime_error("In gdf_column_cpp::clone unsupported data type GDF_STRING");
    }

    cuDF::Allocator::allocate((void**)&data_dev, allocated_size_data);
    if (column->valid != nullptr) {
        cuDF::Allocator::allocate((void**)&valid_dev, allocated_size_valid);
    }

    CheckCudaErrors(cudaMemcpy(data_dev, this->column->data, this->allocated_size_data, cudaMemcpyDeviceToDevice));
	// print_gdf_column(this->get_gdf_column());
    if (this->column->valid != nullptr) {
	    CheckCudaErrors(cudaMemcpy(valid_dev, this->column->valid, this->allocated_size_valid, cudaMemcpyDeviceToDevice));
    }

	gdf_column_cpp col1;
    col1.column = new gdf_column{};
	*col1.column = *this->column;
	col1.column->data = (void *) data_dev;
	col1.column->valid =(cudf::valid_type *) valid_dev;
	col1.allocated_size_data = this->allocated_size_data;
	col1.allocated_size_valid = this->allocated_size_valid;

    if (this->column->dtype == GDF_STRING_CATEGORY){
        col1.column->dtype_info.time_unit = TIME_UNIT_NONE;
        if (this->column->dtype_info.category){
            NVCategory *input_cat = static_cast<NVCategory *>(this->column->dtype_info.category);
            NVCategory *cat = input_cat->copy();
            col1.column->dtype_info.category = static_cast<void*>(cat);
        }
    }

    col1.is_ipc_column = false;
    col1.column_token = 0;
	if(name == ""){
		col1.set_name(this->column_name);
	}else{
		col1.set_name(name);
	}

    if (register_column){
	    GDFRefCounter::getInstance()->register_column(col1.column);
    }

	return col1;
}

void gdf_column_cpp::operator=(const gdf_column_cpp& col)
{
    if (column == col.column) {
        return;
    }
    decrement_counter(column);

	column = col.column;
    this->allocated_size_data = col.allocated_size_data;
    this->allocated_size_valid = col.allocated_size_valid;
    this->set_name(col.column_name);
    this->is_ipc_column = col.is_ipc_column;
    this->column_token = col.column_token;
    GDFRefCounter::getInstance()->increment(const_cast<gdf_column*>(col.column));

}

gdf_column* gdf_column_cpp::get_gdf_column() const
{
    return column;
}

void gdf_column_cpp::resize(size_t new_size){
	this->column->size = new_size;
}
//TODO: needs to be implemented for efficiency though not strictly necessary
gdf_error gdf_column_cpp::compact(){
    if( this->allocated_size_valid != static_cast<std::size_t>(gdf_valid_allocation_size(this->size()) )){
    	//compact valid allcoation
    }

    int byte_width = ral::traits::get_dtype_size_in_bytes(this->get_gdf_column());
    if( this->allocated_size_data != (this->size() * static_cast<std::size_t>(byte_width)) ){
    	//compact data allocation
    }
    return GDF_SUCCESS;
}

void gdf_column_cpp::update_null_count()
{
    update_null_count(column);
}

void gdf_column_cpp::allocate_set_valid(){
	this->column->valid = allocate_valid();
}
cudf::valid_type * gdf_column_cpp::allocate_valid(){
	size_t num_values = this->size();
    cudf::valid_type * valid_device;
	this->allocated_size_valid = gdf_valid_allocation_size(num_values); //so allocations are supposed to be 64byte aligned

    cuDF::Allocator::allocate((void**)&valid_device, allocated_size_valid);

//TODO: this will fail gloriously whenever the valid type cahnges
    CheckCudaErrors(cudaMemset(valid_device, (cudf::valid_type)255, allocated_size_valid)); //assume all relevant bits are set to on
	return valid_device;
}

void gdf_column_cpp::create_gdf_column_for_ipc(cudf::type_id type, gdf_dtype_extra_info dtype_info, void * col_data,cudf::valid_type * valid_data, cudf::size_type num_values, cudf::size_type null_count, std::string column_name){
	// TODO percy cudf0.12 was invalid here, should we consider empty?
    assert(type != cudf::type_id::EMPTY);
    decrement_counter(column);

    //TODO crate column here
    this->column = new gdf_column{};
    gdf_column_view(this->column, col_data, valid_data, num_values, type);
    this->column->dtype_info = {dtype_info.time_unit, nullptr};
    this->column->null_count = null_count;
    int width = ral::traits::get_dtype_size_in_bytes(this->column);
    this->allocated_size_data = num_values * width;

    if(col_data == nullptr){
        // std::cout<<"WARNING: create_gdf_column_for_ipc received null col_data"<<std::endl;
        cuDF::Allocator::allocate((void**)&this->column->data, this->allocated_size_data);
    }
    if (valid_data == nullptr){
        this->allocate_set_valid();
    } else {
        this->allocated_size_valid = gdf_valid_allocation_size(num_values);
    }

    is_ipc_column = true;
    this->column_token = 0;
    this->set_name(column_name);

    //Todo: deprecated? or need to register the nvstrings pointers?
}

void gdf_column_cpp::create_gdf_column(NVCategory* category, size_t num_values,std::string column_name){

    decrement_counter(column);

    //TODO crate column here
    this->column = new gdf_column{};
    cudf::type_id type = cudf::type_id::CATEGORY;
    gdf_column_view(this->column, nullptr, nullptr, num_values, type);

    this->column->dtype_info.category = (void*) category;
    this->column->dtype_info.time_unit = TIME_UNIT_NONE; // TODO this should not be hardcoded

    this->allocated_size_data = sizeof(nv_category_index_type) * this->column->size;
    cuDF::Allocator::allocate((void**)&this->column->data, this->allocated_size_data);
    CheckCudaErrors( cudaMemcpy(
        this->column->data,
        category->values_cptr(),
        this->allocated_size_data,
        cudaMemcpyDeviceToDevice) );

    this->column->valid = nullptr;
    this->allocated_size_valid = 0;
    this->column->null_count = 0;

    if (category->has_nulls()) {
        this->column->valid = allocate_valid();
        this->column->null_count = category->set_null_bitarray((cudf::valid_type*)this->column->valid);
    }

    this->is_ipc_column = false;
    this->column_token = 0;
    this->set_name(column_name);

    GDFRefCounter::getInstance()->register_column(this->column);

}

void gdf_column_cpp::create_gdf_column(NVStrings* strings, size_t num_values, std::string column_name) {
    decrement_counter(column);

    //TODO crate column here
    this->column = new gdf_column{};
    gdf_column_view(this->column, static_cast<void*>(strings), nullptr, num_values, cudf::type_id::STRING);

    this->allocated_size_data = 0; // TODO: do we care? what should be put there?
    this->allocated_size_valid = 0;

    this->is_ipc_column = false;
    this->column_token = 0;
    this->set_name(column_name);

    GDFRefCounter::getInstance()->register_column(this->column);
}

void gdf_column_cpp::create_gdf_column(cudf::type_id type, gdf_dtype_extra_info dtype_info, size_t num_values, void * input_data, cudf::valid_type * host_valids, size_t width_per_value, const std::string &column_name)
{
	// TODO percy cudf0.12 was invalid here, should we consider empty?
    assert(type != cudf::type_id::EMPTY);
    decrement_counter(column);

    this->column = new gdf_column{};

    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
    //needing to not require numvalues so it can be called rom outside
    this->get_gdf_column()->size = num_values;
    char * data = nullptr;
    this->is_ipc_column = false;
    this->column_token = 0;

    this->allocated_size_data = (width_per_value * num_values);
    this->allocated_size_valid = 0;

    cudf::valid_type * valid_device = nullptr;

    if (num_values > 0) {
        cuDF::Allocator::allocate((void**)&data, allocated_size_data);

        if(host_valids != nullptr){
            valid_device = allocate_valid();
            CheckCudaErrors(cudaMemcpy(valid_device, host_valids, this->allocated_size_valid, cudaMemcpyHostToDevice));
        }
    }

    gdf_column_view(this->column, (void *) data, valid_device, num_values, type);
    this->column->dtype_info = {dtype_info.time_unit, nullptr};

    this->set_name(column_name);
    if(input_data != nullptr){
        CheckCudaErrors(cudaMemcpy(data, input_data, num_values * width_per_value, cudaMemcpyHostToDevice));
    }

    if(host_valids != nullptr){
        this->update_null_count();
    }

    GDFRefCounter::getInstance()->register_column(this->column);
}

//Todo: Verificar que al llamar mas de una vez al create_gdf_column se desaloque cualquier memoria alocada anteriormente
void gdf_column_cpp::create_gdf_column(cudf::type_id type, gdf_dtype_extra_info dtype_info, size_t num_values, void * input_data, size_t width_per_value, const std::string &column_name, bool allocate_valid_buffer )
{
	// TODO percy cudf0.12 was invalid here, should we consider empty?
    assert(type != cudf::type_id::EMPTY);
    decrement_counter(column);

    this->column = new gdf_column{};

    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
    //needing to not require numvalues so it can be called rom outside
    this->get_gdf_column()->size = num_values;
    char * data = nullptr;
    this->is_ipc_column = false;
    this->column_token = 0;

    this->allocated_size_data = (width_per_value * num_values);
    this->allocated_size_valid = 0;

    cudf::valid_type * valid_device = nullptr;

    if (num_values > 0) {
        if (type != GDF_STRING){
            valid_device = allocate_valid();
        }

        cuDF::Allocator::allocate((void**)&data, allocated_size_data);
    }

		if(allocate_valid_buffer){
			this->allocate_set_valid();
		}
    gdf_column_view(this->column, (void *) data, valid_device, num_values, type);
    this->column->dtype_info = {dtype_info.time_unit, nullptr};

    this->set_name(column_name);
    if(input_data != nullptr){
        CheckCudaErrors(cudaMemcpy(data, input_data, num_values * width_per_value, cudaMemcpyHostToDevice));
    }

    GDFRefCounter::getInstance()->register_column(this->column);
}

void gdf_column_cpp::create_gdf_column(gdf_column * column, bool registerColumn){

    if (column != this->column) { // if this gdf_column_cpp already represented this gdf_column, we dont want to do anything. Especially do decrement_counter, since it might actually free the gdf_column we are trying to use
        decrement_counter(this->column);

        this->column = column;

        if (column->dtype != GDF_STRING){
            int width_per_value = ral::traits::get_dtype_size_in_bytes(column);

            //TODO: we are assuming they are not padding,
            this->allocated_size_data = width_per_value * column->size;
        } else {
            this->allocated_size_data = 0; // TODO: do we care? what should be put there?
        }
        if(column->valid != nullptr){
            this->allocated_size_valid = gdf_valid_allocation_size(column->size); //so allocations are supposed to be 64byte aligned
        } else {
            this->allocated_size_valid = 0;
        }
        this->is_ipc_column = false;
        this->column_token = 0;
        if (column->col_name)
            this->set_name(std::string(column->col_name));
				if(registerColumn){
        	GDFRefCounter::getInstance()->register_column(this->column);
				}

    }
}

void gdf_column_cpp::create_gdf_column(const gdf_scalar & scalar, const std::string &column_name){
	// TODO percy cudf0.12 was invalid here, should we consider empty?
    assert(scalar.dtype != to_gdf_type(cudf::type_id::EMPTY));
    decrement_counter(column);

    this->column = new gdf_column{};

    cudf::type_id type = to_type_id(scalar.dtype);

    //TODO: this is kind of bad its a chicken and egg situation with column_view requiring a pointer to device and allocate_valid
    //needing to not require numvalues so it can be called rom outside
    this->get_gdf_column()->size = 1;
    this->get_gdf_column()->dtype = to_gdf_type(type);
    char * data;
    this->is_ipc_column = false;
    this->column_token = 0;
    size_t width_per_value = ral::traits::get_dtype_size_in_bytes(type);

    this->allocated_size_data = width_per_value;

    cuDF::Allocator::allocate((void**)&data, allocated_size_data);

    cudf::valid_type * valid_device = nullptr;
    if(!scalar.is_valid){
        valid_device = allocate_valid();
        CheckCudaErrors(cudaMemset(valid_device, 0, this->allocated_size_valid));
         this->get_gdf_column()->null_count = 1;
    } else {
        this->allocated_size_valid = 0;
        this->get_gdf_column()->null_count = 0;
    }
    this->get_gdf_column()->data = (void *) data;
    this->get_gdf_column()->valid = valid_device;

    this->set_name(column_name);
    if(scalar.is_valid){
        if(type == cudf::type_id::INT8){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si08), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::INT16){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si16), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::INT32){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si32), width_per_value, cudaMemcpyHostToDevice));
		// TODO percy cudf0.12 by default timestamp for bz is MS but we need to use proper time resolution
        }else if(type == cudf::type_id::TIMESTAMP_DAYS){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.dt32), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::INT64 ){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.si64), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::TIMESTAMP_SECONDS){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.dt64), width_per_value, cudaMemcpyHostToDevice));
        } else if(type == cudf::type_id::TIMESTAMP_MILLISECONDS){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.tmst), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::FLOAT32){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.fp32), width_per_value, cudaMemcpyHostToDevice));
        }else if(type == cudf::type_id::FLOAT64){
            CheckCudaErrors(cudaMemcpy(data, &(scalar.data.fp64), width_per_value, cudaMemcpyHostToDevice));
        }
    }

    GDFRefCounter::getInstance()->register_column(this->column);
}

void gdf_column_cpp::create_empty(const cudf::type_id     dtype,
                                  const std::string & column_name,
                                  const gdf_time_unit     time_unit) {
    if (cudf::type_id::STRING == dtype || cudf::type_id::CATEGORY == dtype ) {  // cudf::size_of doesn't support GDF_STRING
        create_gdf_column(
            NVCategory::create_from_array(nullptr, 0), 0, column_name);
    } else {
        create_gdf_column(
            dtype, gdf_dtype_extra_info{time_unit,nullptr}, 0, nullptr, ral::traits::get_dtype_size_in_bytes(dtype), column_name);
    }
}

void gdf_column_cpp::create_empty(const cudf::type_id dtype) {
    create_empty(dtype, "");
}

void gdf_column_cpp::allocate_like(const gdf_column_cpp& other){
    decrement_counter(column);

    this->column = new gdf_column{};

    this->is_ipc_column = false;
    this->column_token = 0;

    this->allocated_size_data = other.allocated_size_data;
    this->allocated_size_valid = other.allocated_size_valid;

    char * data = nullptr;
    if (this->allocated_size_data > 0) {
        cuDF::Allocator::allocate((void**)&data, this->allocated_size_data);
    }

    cudf::valid_type * valid_device = nullptr;
    if(this->allocated_size_valid > 0) {
        cuDF::Allocator::allocate((void**)&valid_device,  this->allocated_size_valid);
    }

    gdf_column_view(this->column, (void *) data, valid_device, other.size(), other.dtype());
    this->column->dtype_info.category = nullptr;
    this->column->dtype_info.time_unit = TIME_UNIT_NONE; // TODO this should not be hardcoded

    this->set_name("");

    GDFRefCounter::getInstance()->register_column(this->column);
}

/*
void gdf_column_cpp::realloc_gdf_column(cudf::type_id type, size_t size, size_t width){
	const std::string col_name = this->column_name;
    GDFRefCounter::getInstance()->decrement(&this->column); //decremeting reference, deallocating space

	this->create_gdf_column(type, size, nullptr, width, col_name);
}*/

gdf_error gdf_column_cpp::gdf_column_view(gdf_column *column, void *data, cudf::valid_type *valid, cudf::size_type size, cudf::type_id dtype)
{
    column->data = data;
    column->valid = valid;
    column->size = size;
    column->dtype = to_gdf_type(dtype);
    column->null_count = 0;
    return GDF_SUCCESS;
}

gdf_column_cpp::~gdf_column_cpp()
{
	if(this->is_ipc_column){
		//TODO: this is a big memory leak. we probably just need to have anothe reference
		//counter, the valid pointer was allocated on our side
		//we cant free it here because we dont know if this ipc column is used somewhere else
	}else{
	    GDFRefCounter::getInstance()->decrement(this->column);
	}

}
bool gdf_column_cpp::is_ipc() const {
	return this->is_ipc_column;
}

bool gdf_column_cpp::has_token(){
    return (this->column_token != 0);
}

void* gdf_column_cpp::data() const{
    return column->data;
}

cudf::valid_type* gdf_column_cpp::valid() const {
    return column->valid;
}
cudf::size_type gdf_column_cpp::size() const {
    return column->size;
}

cudf::type_id gdf_column_cpp::dtype() const {
    return to_type_id(column->dtype);
}

cudf::size_type gdf_column_cpp::null_count(){
    return column->null_count;
}

gdf_dtype_extra_info gdf_column_cpp::dtype_info() const {
    return column->dtype_info;
}

void gdf_column_cpp::set_dtype(cudf::type_id dtype){
    column->dtype=to_gdf_type(dtype);
}

std::size_t gdf_column_cpp::get_valid_size() const {
    return allocated_size_valid;
}

column_token_t gdf_column_cpp::get_column_token() const {
    return this->column_token;
}

void gdf_column_cpp::set_column_token(column_token_t column_token){
    this->column_token = column_token;
}

void gdf_column_cpp::update_null_count(gdf_column* column) const {
    if (column->size == 0 || column->valid == nullptr) {
        column->null_count = 0;
    }
    else {
        int count;
        gdf_error result = gdf_count_nonzero_mask(column->valid, column->size, &count);
        assert(result == GDF_SUCCESS);
        column->null_count = column->size - static_cast<cudf::size_type>(count);
    }
}
