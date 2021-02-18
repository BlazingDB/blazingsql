#include "BatchWindowFunctionProcessing.h"
#include "execution_graph/logic_controllers/BlazingColumn.h"
#include "taskflow/executor.h"
#include "CodeTimer.h"

#include <src/utilities/CommonOperations.h>
#include "parser/expression_utils.hpp"
#include <blazingdb/io/Util/StringUtil.h>

#include <cudf/concatenate.hpp>
#include <cudf/stream_compaction.hpp>
#include "cudf/column/column_view.hpp"
#include <cudf/rolling.hpp>
#include <cudf/filling.hpp>
#include <cudf/partitioning.hpp>
#include <cudf/types.hpp>
#include <cudf/copying.hpp>
#include <cudf/aggregation.hpp>

namespace ral {
namespace batch {

// BEGIN ComputeWindowKernel

ComputeWindowKernel::ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::ComputeWindowKernel} {
    this->query_graph = query_graph;
}

// TODO: Support for RANK() and DENSE_RANK()
// TODO: Support for first_value() and last_value()
std::unique_ptr<CudfColumn> ComputeWindowKernel::compute_column_from_window_function(
    cudf::table_view input_cudf_view,
    cudf::column_view input_col_view,
    std::size_t pos, int & constant_count ) {

    std::unique_ptr<cudf::aggregation> window_aggregation;
    
    // it contains `LEAD` or `LAG` aggs
    if (this->constant_values.size() > constant_count && (this->type_aggs_as_str[pos] == "LAG" || this->type_aggs_as_str[pos] == "LEAD") ) {
        window_aggregation = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos], this->constant_values[constant_count]);
        constant_count++;
    } else {
        window_aggregation = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos]); 
    }

    std::unique_ptr<CudfColumn> windowed_col;
    std::vector<cudf::column_view> table_to_rolling;

    // want all columns to be partitioned
    for (std::size_t col_i = 0; col_i < this->column_indices_partitioned.size(); ++col_i) {
        table_to_rolling.push_back(input_cudf_view.column(this->column_indices_partitioned[col_i]));
    }

    cudf::table_view table_view_with_single_col(table_to_rolling);

    if (this->expression.find("ORDER BY") != std::string::npos) {
        // default ROWS/RANGE statement
        if (this->expression.find("UNBOUNDED PRECEDING and CURRENT ROW") != std::string::npos || this->expression.find("BETWEEN") != std::string::npos) {
            throw std::runtime_error("In Window Function: RANGE or ROWS bound is not currently supported");
        } else if (this->type_aggs_as_str[pos] == "LEAD") {
            windowed_col = cudf::grouped_rolling_window(table_view_with_single_col , input_col_view, 0, input_col_view.size(), 1, window_aggregation);
        } else {
            windowed_col = cudf::grouped_rolling_window(table_view_with_single_col , input_col_view, input_col_view.size(), 0, 1, window_aggregation);
        }
    } else {
        windowed_col = cudf::grouped_rolling_window(table_view_with_single_col , input_col_view, input_col_view.size(), input_col_view.size(), 1, window_aggregation);
    }

    return std::move(windowed_col);
}

ral::execution::task_result ComputeWindowKernel::do_process(std::vector< std::unique_ptr<ral::frame::BlazingTable> > inputs,
    std::shared_ptr<ral::cache::CacheMachine> output,
    cudaStream_t /*stream*/, const std::map<std::string, std::string>& /*args*/) {

    if (inputs.size() == 0) {
        return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    CodeTimer eventTimer(false);

    std::unique_ptr<ral::frame::BlazingTable> & input = inputs[0];

    try{
        cudf::table_view input_cudf_view = input->view();

        std::vector<std::string> input_names = input->names();
        std::tie(this->column_indices_to_agg, this->type_aggs_as_str, this->constant_values) = 
                                        get_cols_to_apply_window_and_cols_to_apply_agg(this->expression);
        std::tie(this->column_indices_partitioned, std::ignore) = ral::operators::get_vars_to_partition(this->expression);

        // fill all the Kind aggregations
        for (std::size_t col_i = 0; col_i < this->type_aggs_as_str.size(); ++col_i) {
            AggregateKind aggr_kind_i = ral::operators::get_aggregation_operation(this->type_aggs_as_str[col_i]);
            this->aggs_wind_func.push_back(aggr_kind_i);
        }

        std::vector< std::unique_ptr<CudfColumn> > new_wf_cols;
        int constant_count = 0;
        for (std::size_t col_i = 0; col_i < this->type_aggs_as_str.size(); ++col_i) {
            cudf::column_view input_col_view = input_cudf_view.column(column_indices_to_agg[col_i]);

            // calling main window function
            std::unique_ptr<CudfColumn> windowed_col = compute_column_from_window_function(input_cudf_view, input_col_view, col_i, constant_count);
            new_wf_cols.push_back(std::move(windowed_col));
        }

        std::unique_ptr<cudf::table> cudf_table_input = input->releaseCudfTable();
        std::vector< std::unique_ptr<CudfColumn> > input_cudf_columns = cudf_table_input->release();
        
        size_t total_output_columns = input_cudf_columns.size() + new_wf_cols.size();
        size_t num_input_cols = input_cudf_columns.size();
        std::vector<std::string> output_names;
        std::vector< std::unique_ptr<CudfColumn> > output_columns;
        
        for (size_t col_i = 0; col_i < total_output_columns; ++col_i) {
            // appending wf columns
            if (col_i >= num_input_cols) {
                output_columns.push_back(std::move(new_wf_cols[col_i - num_input_cols]));
                output_names.push_back("");
            } else {
                output_columns.push_back(std::move(input_cudf_columns[col_i]));
                output_names.push_back(input_names[col_i]);
            }
        }

        std::unique_ptr<cudf::table> cudf_table_window = std::make_unique<cudf::table>(std::move(output_columns));
        std::unique_ptr<ral::frame::BlazingTable> windowed_table = std::make_unique<ral::frame::BlazingTable>(std::move(cudf_table_window), output_names);

        if (windowed_table) {
            cudf::size_type num_rows = windowed_table->num_rows();
            std::size_t num_bytes = windowed_table->sizeInBytes();
        }

        output->addToCache(std::move(windowed_table));
    }catch(rmm::bad_alloc e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(std::exception e){
        return {ral::execution::task_status::FAIL, std::string(e.what()), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
    }

    return {ral::execution::task_status::SUCCESS, std::string(), std::vector< std::unique_ptr<ral::frame::BlazingTable> > ()};
}

kstatus ComputeWindowKernel::run() {
    CodeTimer timer;

    std::unique_ptr<ral::cache::CacheData> cache_data = this->input_cache()->pullCacheData();

    while (cache_data != nullptr ){
        std::vector<std::unique_ptr <ral::cache::CacheData> > inputs;
        inputs.push_back(std::move(cache_data));

        ral::execution::executor::get_instance()->add_task(
                std::move(inputs),
                this->output_cache(),
                this);

        cache_data = this->input_cache()->pullCacheData();
    }

    std::unique_lock<std::mutex> lock(kernel_mutex);
    kernel_cv.wait(lock,[this]{
        return this->tasks.empty();
    });

    if (logger != nullptr) {
        logger->debug("{query_id}|{step}|{substep}|{info}|{duration}|kernel_id|{kernel_id}||",
                    "query_id"_a=context->getContextToken(),
                    "step"_a=context->getQueryStep(),
                    "substep"_a=context->getQuerySubstep(),
                    "info"_a="ComputeWindow Kernel Completed",
                    "duration"_a=timer.elapsed_time(),
                    "kernel_id"_a=this->get_id());
    }
    return kstatus::proceed;
}

// END ComputeWindowKernel

} // namespace batch
} // namespace ral
