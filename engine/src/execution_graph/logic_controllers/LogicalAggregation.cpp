

#include "LogicalAggregation.h"

namespace ral{

namespace processor{


  std::unique_ptr<ral::frame::BlazingTable> computeAggregation(
    const ral::frame::BlazingTableView & table,
    std::vector<int> key_indeces,  // if size > 0 that means we are doing a group by and key_indices and table can create keys table_view
    std::vector<string> agg_value_expressions,
    std::vector<AggregateKind> aggs,  // this we use to create aggregation requests or map them to aggregation::Kind for reduce
    std::vector<std::string> output_column_names)

   internally, this funciton needs to see if its:
   - group by with aggregation 
            - then have to create [table_view const& keys] 
            - then you have to create the aggregation values (which may involve evaluate expression)
            - then create [std::vector<aggregation_request> const& requests]
            - then create groupby object and call aggregate
   - aggregation without group by
            - then in a loop run reduce for each aggregation
            - convert scalars into columns and then create a BLazingTable from those columns
            - need to see what is the canonical way to convert a scalar into a column
    NOTE THE TWO ABOVE ARE CURRENTLY DONE by compute_aggregations

    - group by without aggregation  (not sure yet if we can just use group by and have empty aggregation requests)

// this is for group by with aggregations and maybe for groupby without aggregations (still TBD)
//ignore_null_keys, keys_are_sorted, column_order, null_precedence  these we dont care, leave the default

groupby(table_view const& keys, bool ignore_null_keys = true,
                   bool keys_are_sorted = false,
                   std::vector<order> const& column_order = {},
                   std::vector<null_order> const& null_precedence = {})

std::pair<std::unique_ptr<table>, std::vector<aggregation_result>> aggregate(
      std::vector<aggregation_request> const& requests,
      rmm::mr::device_memory_resource* mr = rmm::mr::get_default_resource());

// for aggregations without group by its still in a PR 
// https://github.com/karthikeyann/cudf/blob/port-reductions2/cpp/include/cudf/reduction.hpp

std::unique_ptr<scalar> reduce(
    const column_view& col, aggregation::Kind op, data_type output_dtype,
    cudf::size_type ddof = 1,
    rmm::mr::device_memory_resource* mr = rmm::mr::get_default_resource());
