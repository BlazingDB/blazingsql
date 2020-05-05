#include "kernel.h"
namespace ral {
namespace cache {
    
std::size_t kernel::kernel_count(0);

// this function gets the estimated num_rows for the output
// the default is that its the same as the input (i.e. project, sort, ...)
std::pair<bool, uint64_t> kernel::get_estimated_output_num_rows(){
    return this->query_graph->get_estimated_input_rows_to_kernel(this->kernel_id);
}


}  // end namespace cache
}  // end namespace ral