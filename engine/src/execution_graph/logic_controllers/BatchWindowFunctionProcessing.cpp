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
#include <cudf/join.hpp>
#include <cudf/sorting.hpp>

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
    for (std::size_t col_i = 0; col_i < this->column_indices_partitioned.size(); ++col_i) {
        columns_to_partition.push_back(input_table_cudf_view.column(this->column_indices_partitioned[col_i]));
    }

    cudf::table_view partitioned_table_view(columns_to_partition);

    std::unique_ptr<CudfColumn> windowed_col;
    if (is_first_value_window(this->type_aggs_as_str[pos]) || is_last_value_window(this->type_aggs_as_str[pos])) {

        if (is_last_value_window(this->type_aggs_as_str[pos])) {
            // We want also all the ordered columns
            for (std::size_t col_i = 0; col_i < this->column_indices_ordered.size(); ++col_i) {
                columns_to_partition.push_back(input_table_cudf_view.column(this->column_indices_ordered[col_i]));
            }

            partitioned_table_view = {{cudf::table_view(columns_to_partition)}};
        }

        // first: we want to get all the first (or last) values (due to the columns to partition)
        std::vector<cudf::groupby::aggregation_request> requests;
        requests.emplace_back(cudf::groupby::aggregation_request());
        requests[0].values = col_view_to_agg;
        requests[0].aggregations.push_back(std::move(window_aggregation));

        cudf::groupby::groupby gb_obj(cudf::table_view({partitioned_table_view}), cudf::null_policy::INCLUDE, cudf::sorted::YES, {}, {});
        std::pair<std::unique_ptr<cudf::table>, std::vector<cudf::groupby::aggregation_result>> result = gb_obj.aggregate(requests);

        windowed_col = std::move(result.second[0].results[0]);

        // if exists duplicated values (in partitioned_table_view) we want to fill `windowed_col` with repeated values
        // So let's do a join
        if (windowed_col->size() < col_view_to_agg.size()) {
            std::vector<std::unique_ptr<cudf::column>> keys_grouped = result.first->release();
            keys_grouped.push_back(std::move(windowed_col));

            std::unique_ptr<cudf::table> left_table = std::make_unique<cudf::table>(std::move(keys_grouped));

            // Let's get all the necessary params for the join

            // we just want the key columns, not the values column (which is the last column)
            std::vector<cudf::size_type> left_column_indices(left_table->num_columns() - 1);
            std::iota(left_column_indices.begin(), left_column_indices.end(), 0);

            std::vector<cudf::size_type> right_column_indices(partitioned_table_view.num_columns());
            std::iota(right_column_indices.begin(), right_column_indices.end(), 0);

            std::unique_ptr<cudf::table> join_table = cudf::inner_join(
                                                            left_table->view(),
                                                            partitioned_table_view,
                                                            left_column_indices,
                                                            right_column_indices
                                                            );

            // Because the values column is unordered, we want to sort it
	        std::vector<cudf::null_order> null_orders(join_table->num_columns(), cudf::null_order::AFTER);

            // partition by is always in ASCENDING order
            std::vector<cudf::order> sortOrderTypes(join_table->num_columns(), cudf::order::ASCENDING);
	        std::unique_ptr<cudf::table> sorted_table = cudf::sort(join_table->view(), sortOrderTypes, null_orders);

            size_t position_of_values_column = left_table->num_columns() - 1;
            windowed_col = std::move(sorted_table->release()[position_of_values_column]);
        }
    }
    else if (window_expression_contains_order_by(this->expression)) {
        if (window_expression_contains_bounds(this->expression)) {
            // TODO: for now just ROWS bounds works (not RANGE)
            windowed_col = cudf::grouped_rolling_window(partitioned_table_view, col_view_to_agg, this->preceding_value + 1, this->following_value, 1, window_aggregation);
        } else {
            if (this->type_aggs_as_str[pos] == "LEAD") {
                windowed_col = cudf::grouped_rolling_window(partitioned_table_view, col_view_to_agg, 0, col_view_to_agg.size(), 1, window_aggregation);
            } else {
                windowed_col = cudf::grouped_rolling_window(partitioned_table_view, col_view_to_agg, col_view_to_agg.size(), 0, 1, window_aggregation);
            }
        }
    } else {
        windowed_col = cudf::grouped_rolling_window(partitioned_table_view, col_view_to_agg, col_view_to_agg.size(), col_view_to_agg.size(), 1, window_aggregation);
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
        std::tie(this->column_indices_ordered, std::ignore) = ral::operators::get_vars_to_orders(this->expression);

        // fill all the Kind aggregations
        for (std::size_t col_i = 0; col_i < this->type_aggs_as_str.size(); ++col_i) {
            AggregateKind aggr_kind_i = ral::operators::get_aggregation_operation(this->type_aggs_as_str[col_i], true);
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
