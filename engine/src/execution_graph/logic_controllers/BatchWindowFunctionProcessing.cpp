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
#include <cudf/search.hpp>

namespace ral {
namespace batch {

// BEGIN ComputeWindowKernel

ComputeWindowKernel::ComputeWindowKernel(std::size_t kernel_id, const std::string & queryString,
    std::shared_ptr<Context> context,
    std::shared_ptr<ral::cache::graph> query_graph)
    : kernel{kernel_id, queryString, context, kernel_type::ComputeWindowKernel} {
    this->query_graph = query_graph;
    std::tie(this->preceding_value, this->following_value) = get_bounds_from_window_expression(this->expression);
    this->frame_type = get_frame_type_from_over_clause(this->expression);
}

// TODO: Support for RANK() and DENSE_RANK()
std::unique_ptr<CudfColumn> ComputeWindowKernel::compute_column_from_window_function(
    cudf::table_view input_table_cudf_view,
    cudf::column_view col_view_to_agg,
    std::size_t pos, int & agg_param_count ) {

    std::unique_ptr<cudf::aggregation> window_aggregation;
    
    // we want firs get the type of aggregation
    if (this->agg_param_values.size() > agg_param_count && is_lag_or_lead_aggregation(this->type_aggs_as_str[pos])) {
        window_aggregation = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos], this->agg_param_values[agg_param_count]);
        agg_param_count++;
    } else if (is_last_value_window(this->type_aggs_as_str[pos])) {
        window_aggregation = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos], -1);
    } else {
        window_aggregation = ral::operators::makeCudfAggregation(this->aggs_wind_func[pos]);
    }

    // want all columns to be partitioned
    std::vector<cudf::column_view> columns_to_partition;
    std::vector<int> column_partitioned_indices;
    for (std::size_t col_i = 0; col_i < this->column_indices_partitioned.size(); ++col_i) {
        columns_to_partition.push_back(input_table_cudf_view.column(this->column_indices_partitioned[col_i]));
        column_partitioned_indices.push_back(col_i);
    }

    cudf::table_view table_view_to_partition(columns_to_partition);
    
    std::unique_ptr<CudfColumn> windowed_col;
    if (is_first_value_window(this->type_aggs_as_str[pos]) || is_last_value_window(this->type_aggs_as_str[pos])) {
        
        // When using cudf::grouped_rolling_window() it crashes
        // rolling_detail.cuh:738: Aggregation operator and/or input type combination is invalid
        // https://github.com/rapidsai/cudf/issues/7231 expecting unit test using make_nth_element_aggregation()

        // first: we want to get all the first (or last) values (due to the columns to partition)
        std::vector<cudf::groupby::aggregation_request> requests;
        requests.emplace_back(cudf::groupby::aggregation_request());
        requests[0].values = col_view_to_agg;
        requests[0].aggregations.push_back(std::move(window_aggregation));
        
        cudf::groupby::groupby gb_obj(cudf::table_view({table_view_to_partition}), cudf::null_policy::EXCLUDE, cudf::sorted::YES, {}, {});
        std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> result = gb_obj.aggregate(requests);

        windowed_col = std::move(result.second[0].results[0]);

        // if exists duplicated values (in table_view_to_partition) we want to fill the `windowed_col` by repeating values
        if (windowed_col->size() < col_view_to_agg.size()) { 
            std::unique_ptr<cudf::table> table_view_to_partition_wo_repetitions = cudf::drop_duplicates(table_view_to_partition,
                                                                                             column_partitioned_indices,
                                                                                             cudf::duplicate_keep_option::KEEP_FIRST);

            // TODO this is just a default setting. Will want to be able to properly set null_order
            std::vector<cudf::null_order> null_orders(column_partitioned_indices.size(), cudf::null_order::AFTER);
            std::vector<cudf::order> sortOrderTypes(column_partitioned_indices.size(), cudf::order::ASCENDING);
            std::unique_ptr<cudf::column> pivot_indexes = cudf::lower_bound(table_view_to_partition,
                                                                            table_view_to_partition_wo_repetitions->view(),
                                                                            sortOrderTypes,
                                                                            null_orders);
            // we want to get the differences between two contiguous pivots
            std::vector<cudf::size_type> pivot_indexes_host = ral::utilities::column_to_vector<cudf::size_type>(pivot_indexes->view());
            std::vector<cudf::size_type> offsets;
            for (size_t i = 0; i < pivot_indexes_host.size() - 1; ++i) {
                offsets.push_back(pivot_indexes_host[i + 1] - pivot_indexes_host[i]);
            }
            offsets.push_back(col_view_to_agg.size() - pivot_indexes_host[pivot_indexes_host.size() - 1]);
            std::unique_ptr<cudf::column> offsets_col = ral::utilities::vector_to_column(offsets, cudf::data_type(cudf::type_id::INT32));

            // we want to repeat these unique values from `windowed_col`
            cudf::table_view windowed_table_view{{std::move(windowed_col)->view()}};
            std::unique_ptr<cudf::table> last_table = cudf::repeat(windowed_table_view, offsets_col->view());

            windowed_col = std::move(last_table->release()[0]);
        }
    }
    else if (window_expression_contains_order_by(this->expression)) {
        if (window_expression_contains_bounds(this->expression)) {
            // TODO: for now just ROWS bounds works (not RANGE)
            windowed_col = cudf::grouped_rolling_window(table_view_to_partition, col_view_to_agg, this->preceding_value + 1, this->following_value, 1, window_aggregation);
        } else {
            if (this->type_aggs_as_str[pos] == "LEAD") {
                windowed_col = cudf::grouped_rolling_window(table_view_to_partition, col_view_to_agg, 0, col_view_to_agg.size(), 1, window_aggregation);
            } else {
                windowed_col = cudf::grouped_rolling_window(table_view_to_partition, col_view_to_agg, col_view_to_agg.size(), 0, 1, window_aggregation);
            }
        }
    } else {
        windowed_col = cudf::grouped_rolling_window(table_view_to_partition, col_view_to_agg, col_view_to_agg.size(), col_view_to_agg.size(), 1, window_aggregation);
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
        cudf::table_view input_table_cudf_view = input->view();

        std::vector<std::string> input_names = input->names();
        std::tie(this->column_indices_to_agg, this->type_aggs_as_str, this->agg_param_values) = 
                                        get_cols_to_apply_window_and_cols_to_apply_agg(this->expression);
        std::tie(this->column_indices_partitioned, std::ignore) = ral::operators::get_vars_to_partition(this->expression);

        // fill all the Kind aggregations
        for (std::size_t col_i = 0; col_i < this->type_aggs_as_str.size(); ++col_i) {
            AggregateKind aggr_kind_i = ral::operators::get_aggregation_operation(this->type_aggs_as_str[col_i]);
            this->aggs_wind_func.push_back(aggr_kind_i);
        }

        std::vector< std::unique_ptr<CudfColumn> > new_wf_cols;
        int agg_param_count = 0;
        for (std::size_t col_i = 0; col_i < this->type_aggs_as_str.size(); ++col_i) {
            cudf::column_view col_view_to_agg = input_table_cudf_view.column(column_indices_to_agg[col_i]);

            // calling main window function
            std::unique_ptr<CudfColumn> windowed_col = compute_column_from_window_function(input_table_cudf_view, col_view_to_agg, col_i, agg_param_count);
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
    }catch(const rmm::bad_alloc& e){
        return {ral::execution::task_status::RETRY, std::string(e.what()), std::move(inputs)};
    }catch(const std::exception& e){
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
