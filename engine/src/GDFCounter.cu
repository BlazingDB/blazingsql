/*
 * GDFCounter.cu
 *
 *  Created on: Sep 12, 2018
 *      Author: rqc
 */

#include "GDFCounter.cuh"
#include <iostream>
#include "cuDF/Allocator.h"
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>


GDFRefCounter* GDFRefCounter::Instance=0;

void GDFRefCounter::register_column(gdf_column* col_ptr){

    if(col_ptr != nullptr){
        std::lock_guard<std::mutex> lock(gc_mutex);
        gdf_column * map_key = {col_ptr};

        if(map.find(map_key) == map.end()){
            map[map_key]=1;
        }
    }
}

void GDFRefCounter::deregister_column(gdf_column* col_ptr)
{
    if (col_ptr != nullptr) {  // TODO: use exceptions instead jump nulls
        std::lock_guard<std::mutex> lock(gc_mutex);
        gdf_column * map_key = {col_ptr};

        if(map.find(map_key) != map.end()){
            map[map_key]=0; //deregistering
        }
    }
}

void GDFRefCounter::increment(gdf_column* col_ptr)
{
    std::lock_guard<std::mutex> lock(gc_mutex);
    gdf_column * map_key = {col_ptr};

    if(map.find(map_key)!=map.end()){
        if(map[map_key]!=0){ //is already deregistered
            map[map_key]++;
        }
    }
}



void deallocate(gdf_column* col_ptr){

    if (col_ptr->data != nullptr){
        if (col_ptr->dtype == GDF_STRING){
            NVStrings::destroy(static_cast<NVStrings *>(col_ptr->data));
            col_ptr->data = nullptr;
        } else if (col_ptr->dtype == GDF_STRING_CATEGORY){
            NVCategory::destroy(static_cast<NVCategory *>(col_ptr->dtype_info.category));
            cuDF::Allocator::deallocate(col_ptr->data);
        } else {
            cuDF::Allocator::deallocate(col_ptr->data);
        }
    }
    if (col_ptr->valid != nullptr) {
        cuDF::Allocator::deallocate(col_ptr->valid);
    }
}


void GDFRefCounter::free(gdf_column* col_ptr)
{
    std::lock_guard<std::mutex> lock(gc_mutex);
    gdf_column * map_key = {col_ptr};

    if(map.find(map_key)!=map.end()){
        map.erase(map_key);

        try {
            deallocate(map_key);
        }
        catch (const std::exception& e) {
            delete map_key;
            throw;
        }

        delete map_key;
    }
}

void GDFRefCounter::decrement(gdf_column* col_ptr)
{
    std::lock_guard<std::mutex> lock(gc_mutex);
    gdf_column * map_key = {col_ptr};

    if(map.find(map_key)!=map.end()){
        if(map[map_key]>0){
            map[map_key]--;

            if(map[map_key]==0){
                map.erase(map_key);

                try {
                    deallocate(map_key);
                }
                catch (const std::exception& e) {
                    delete map_key;
                    throw;
                }

                delete map_key;
            }
        }
    }
}

bool GDFRefCounter::contains_column(gdf_column * ptrs){
	if(this->map.find(ptrs) == this->map.end()){
		return false;
	}
	return true;
}

void GDFRefCounter::show_summary()
{
    std::cout<<"--------------------- RefCounter Summary -------------------------\n";

    for (auto& iter : this->map)
        std::cout << "Ptr: " << iter.first << " count: " << iter.second <<"\n";
    
    std::cout<<"Size: "<<get_map_size()<<"\n";

    std::cout<<"------------------ End RefCounter Summary -------------------------\n";
}

GDFRefCounter::GDFRefCounter()
{

}

// Testing purposes
size_t GDFRefCounter::get_map_size()
{
    return map.size();
}

GDFRefCounter* GDFRefCounter::getInstance()
{
    if(!Instance)
        Instance=new GDFRefCounter();
    return Instance;
}

std::size_t GDFRefCounter::column_ref_value(gdf_column* column) {
    std::lock_guard<std::mutex> lock(gc_mutex);
    auto it = map.find(column);
    if (it == map.end()) {
        return 0;
    }
    return it->second;
}
