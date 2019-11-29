#include "RuntimeTraits.h"

namespace blazingdb {
namespace test {
    gdf_size_type get_dtype_size_in_bytes(gdf_dtype dtype) {

        gdf_size_type size = 0;
        switch (dtype){
            case GDF_INT8:
            case GDF_BOOL8:
                size = 1;
                break;
            case GDF_INT16:
                size = 2;
                break;
            case GDF_INT32:
            case GDF_DATE32:
            case GDF_FLOAT32:
            case GDF_CATEGORY:
            case GDF_STRING_CATEGORY:
                size = 4;
                break;
            case GDF_INT64:
            case GDF_FLOAT64:
            case GDF_DATE64:
            case GDF_TIMESTAMP:
                size = 8;
                break;
            default:
                size = 0;
                break;
        }
        return size;
    }

    gdf_size_type get_dtype_size_in_bytes(const gdf_column* column) {
        return get_dtype_size_in_bytes(column->dtype);        
    }

    gdf_size_type get_data_size_in_bytes(const gdf_column* column) {
        return (column->size * get_dtype_size_in_bytes(column->dtype));
    }

    gdf_size_type get_data_size_in_bytes(gdf_size_type quantity, gdf_dtype dtype) {
        return (quantity * get_dtype_size_in_bytes(dtype));
    }

    gdf_size_type get_bitmask_size_in_bytes(const gdf_column* column) {
        return gdf_valid_allocation_size(column->size);
    }

    gdf_size_type get_bitmask_size_in_bytes(gdf_size_type quantity) {
        return gdf_valid_allocation_size(quantity);
    }

    gdf_dtype convert_string_dtype(std::string str){
        if(str == "GDF_INT8"){
            return GDF_INT8;
        }else if(str == "GDF_INT16"){
            return GDF_INT16;
        }else if(str == "GDF_INT32"){
            return GDF_INT32;
        }else if(str == "GDF_INT64"){
            return GDF_INT64;
        }else if(str == "GDF_FLOAT32"){
            return GDF_FLOAT32;
        }else if(str == "GDF_FLOAT64"){
            return GDF_FLOAT64;
        }else if (str == "GDF_BOOL8"){
            return GDF_BOOL8;
        }else if(str == "GDF_DATE32"){
            return GDF_DATE32;
        }else if(str == "GDF_DATE64"){
            return GDF_DATE64;
        }else if(str == "GDF_TIMESTAMP"){
            return GDF_TIMESTAMP;
        }else if(str == "GDF_CATEGORY"){
            return GDF_CATEGORY;
        }else if(str == "GDF_STRING"){
            return GDF_STRING;
        }else if(str == "GDF_STRING_CATEGORY"){
            return GDF_STRING_CATEGORY;
        }else{
            return GDF_invalid;
        }
    }

} // namespace traits
} // namespace ral
