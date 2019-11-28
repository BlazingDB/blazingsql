#ifndef PRIMITIVES_UTIL_CUH
#define PRIMITIVES_UTIL_CUH

#include "GDFColumn.cuh"

namespace ral {
namespace distribution {

    void sort_indices(gdf_column_cpp & indexes);

}
}


#endif  //PRIMITIVES_UTIL_CUH