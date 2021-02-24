#pragma once

#include "BatchProcessing.h"
#include "operators/OrderBy.h"
#include "operators/GroupBy.h"

namespace ral {
namespace batch {

//using ral::cache::distributing_kernel;
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

/**
 * @brief This kernel computes the main Window Function (ROW_NUMBER, LAG, LEAD, MIN, ...) to each batch already pattitioned
 * A new column should be added to the batch
 */

class ComputeWindowKernel : public kernel {
public:
	ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
		std::shared_ptr<Context> context,
		std::shared_ptr<ral::cache::graph> query_graph);

	std::unique_ptr<CudfColumn> compute_column_from_window_function(cudf::table_view input_cudf_view, cudf::column_view input_col_view, std::size_t pos);

	std::string kernel_name() { return "ComputeWindow";}

	ral::execution::task_result do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
		std::shared_ptr<ral::cache::CacheMachine> output,
		cudaStream_t stream, const std::map<std::string, std::string>& args) override;

	kstatus run() override;

private:
	std::vector<int> column_indices_partitioned;   // column indices to be partitioned
	std::vector<int> column_indices_wind_func;     // column indices to be agg
	std::vector<AggregateKind> aggs_wind_func; 
};

} // namespace batch
} // namespace ral
