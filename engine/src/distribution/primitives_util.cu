#include "primitives_util.cuh"

#include <thrust/sort.h>
#include <rmm/rmm.h>
#include <rmm/thrust_rmm_allocator.h>

namespace ral {
namespace distribution {

    void sort_indices(gdf_column_cpp & indexes){
		// TODO percy cudf0.12 port to cudf::column
        //thrust::sort(rmm::exec_policy()->on(0), static_cast<gdf_size_type*>(indexes.data()), static_cast<gdf_size_type*>(indexes.data()) + indexes.size());
    }

}
}