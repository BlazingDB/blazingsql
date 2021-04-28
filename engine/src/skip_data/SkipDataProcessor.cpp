#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "SkipDataProcessor.h"

#include "blazing_table/BlazingColumnView.h"
#include <cudf/column/column_factories.hpp>
#include "parser/CalciteExpressionParsing.h"
#include "execution_kernels/LogicalFilter.h"
#include "execution_kernels/LogicalProject.h"
#include "error.hpp"

#include <numeric>

using namespace fmt::literals;

namespace ral {
namespace skip_data {

namespace {
using namespace ral::parser;

struct skip_data_drop_transformer : public node_transformer {
public:
    explicit skip_data_drop_transformer(const std::string & drop_value) : drop_value_{drop_value} {}

    node * transform(operad_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

    node * transform(operator_node& node) override {
        if (node.value == drop_value_) {
            return new operator_node("NONE");
        }

        return &node;
    }

private:
    std::string drop_value_;
};

struct skip_data_transformer : public node_transformer {
public:
    node * transform(operad_node& node) override { return &node; }

    node * transform(operator_node& node) override {
        const std::string & op = node.value;
        if (op == "=") {
            return transform_equal(node);
        } else if (op == "<" or op == "<=") {
            return transform_less_or_lesseq(node);
        } else if (op == ">" or op == ">=") {
            return transform_greater_or_greatereq(node);
        } else if (op == "+" or op == "-") {
            return transform_sum_or_sub(node);
        }

        // just skip like AND or OR
        return &node;
    }

private:
    node * transform_operand_inc(operad_node * node, int inc) {
        if (node->type == node_type::VARIABLE ) {
            auto id = ral::skip_data::get_id(node->value);
            std::string new_expr = "$" + std::to_string(2 * id + inc);

            return new variable_node(new_expr);
        }

        return node->clone();
    }

    node * transform_operator_inc(operator_node * node, int inc) {
        assert(node->children.size() == 2);

        if (node->value == "&&&") {
            if (inc == 0) {
                assert(node->children[0] != nullptr);
                return node->children[0].release();
            } else {
                assert(node->children[1] != nullptr);
                return node->children[1].release();
            }
        }

        return node->clone();
    }

    node * transform_inc(node * node, int inc) {
        if (node->type == node_type::OPERATOR) {
            return transform_operator_inc(static_cast<operator_node*>(node), inc);
        } else {
            return transform_operand_inc(static_cast<operad_node*>(node), inc);
        }
    }

    node * transform_equal(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        auto less_eq = std::unique_ptr<node>(new operator_node("<="));
        less_eq->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        less_eq->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        auto greater_eq = std::unique_ptr<node>(new operator_node(">="));
        greater_eq->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        greater_eq->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        node * and_node = new operator_node("AND");
        and_node->children.push_back(std::move(less_eq));
        and_node->children.push_back(std::move(greater_eq));

        return and_node;
    }

    node * transform_less_or_lesseq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        return ptr;
    }

    node * transform_greater_or_greatereq(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        node * ptr = new operator_node(operator_node_.value);
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        ptr->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        return ptr;
    }

    node * transform_sum_or_sub(operator_node& operator_node_) {
        assert(operator_node_.children.size() == 2);

        node * n = operator_node_.children[0].get();
        node * m = operator_node_.children[1].get();

        auto expr1 = std::unique_ptr<node>(new operator_node(operator_node_.value));
        expr1->children.push_back(std::unique_ptr<node>(transform_inc(n, 0)));
        expr1->children.push_back(std::unique_ptr<node>(transform_inc(m, 0)));

        auto expr2 = std::unique_ptr<node>(new operator_node(operator_node_.value));
        expr2->children.push_back(std::unique_ptr<node>(transform_inc(n, 1)));
        expr2->children.push_back(std::unique_ptr<node>(transform_inc(m, 1)));

        node * ptr = new operator_node("&&&"); // this is parent node (), for sum, sub after skip_data
        ptr->children.push_back(std::move(expr1));
        ptr->children.push_back(std::move(expr2));

        return ptr;
    }
};

struct skip_data_reducer : public node_transformer {
public:
    node * transform(operad_node& node) override { return &node; }

    node * transform(operator_node& node) override {
        if (ral::skip_data::is_unsupported_binary_op(node.value)) {
            return new operator_node("NONE");
        }

        assert(node.children.size() == 2);

        auto& n = node.children[0];
        auto& m = node.children[1];

        bool left_is_exclusion_unary_op = false;
        bool right_is_exclusion_unary_op = false;
        if (n->type == node_type::OPERATOR) {
            if (ral::skip_data::is_exclusion_unary_op(n->value)) {
                left_is_exclusion_unary_op = true;
            }
        }
        if (m->type == node_type::OPERATOR) {
            if (ral::skip_data::is_exclusion_unary_op(m->value)) {
                right_is_exclusion_unary_op = true;
            }
        }
        if (left_is_exclusion_unary_op and not right_is_exclusion_unary_op) {
            if (node.value == "AND") {
                return m.release();
            } else {
                return new operator_node("NONE");
            }
        } else if (right_is_exclusion_unary_op and not left_is_exclusion_unary_op) {
            if (node.value == "AND") {
                return n.release();
            } else {
                return new operator_node("NONE");
            }
        } else if (left_is_exclusion_unary_op and right_is_exclusion_unary_op) {
            return new operator_node("NONE");
        }

        return &node;
    }
};

} // namespace

void drop_value(ral::parser::parse_tree& tree, const std::string & value) {
    skip_data_drop_transformer t(value);
    tree.transform(t);
}

bool apply_skip_data_rules(ral::parser::parse_tree& tree) {
    skip_data_reducer r;
    tree.transform(r);

    if (tree.root().value == "NONE") {
        return false;
    }

    skip_data_transformer t;
    tree.transform(t);

    return true;
}

// "BindableTableScan(table=[[main, customer]], filters=[[OR(AND(<($0, 15000), =($1, 5)), =($0, *($1, $1)), >=($1, 10), <=($2, 500))]], projects=[[0, 3, 5]], aliases=[[c_custkey, c_nationkey, c_acctbal]])"
//      projects=[[0, 3, 5]]
// minmax_metadata_table => use these indices [[0, 3, 5]]
// minmax_metadata_table => minmax_metadata_table[[0, 1,  6, 7,  10, 11, size - 2, size - 1]]
std::pair<std::unique_ptr<ral::frame::BlazingTable>, bool> process_skipdata_for_table(
    const ral::frame::BlazingTableView & metadata_view, const std::vector<std::string> & names, std::string table_scan) {

    std::string filter_string;
    try {
        filter_string = get_named_expression(table_scan, "condition");
        if(filter_string.empty()) {
            filter_string = get_named_expression(table_scan, "filters");
        }
        if (filter_string.empty()) {
            return std::make_pair(nullptr, true);
        }
    } catch(const std::exception & e) {
        std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
        if(logger){
            logger->error("|||{info}|||||",
                                        "info"_a="In process_skipdata_for_table. What: {}"_format(e.what()));
        }

        return std::make_pair(nullptr, true);
    }
    filter_string = replace_calcite_regex(filter_string);
    filter_string = expand_if_logical_op(filter_string);

    std::string projects = get_named_expression(table_scan, "projects");
    std::vector<int> column_indeces;
    if (projects == ""){
        column_indeces.resize(names.size());
        std::iota(column_indeces.begin(), column_indeces.end(), 0);
    } else {
        std::vector<std::string> column_index_strings = get_expressions_from_expression_list(projects, true);
        for (auto & column_index_string : column_index_strings){
            int index = std::stoi(column_index_string);
            column_indeces.push_back(index);
        }
    }

    cudf::size_type rows = metadata_view.num_rows();
    std::unique_ptr<cudf::column> temp_no_data = cudf::make_fixed_width_column(
        cudf::data_type{cudf::type_id::INT8}, rows,
        cudf::mask_state::UNINITIALIZED);

    std::vector<std::string> metadata_names = metadata_view.names();
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> metadata_columns = metadata_view.toBlazingColumns();
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> projected_metadata_cols;
    std::vector<bool> valid_metadata_columns;
    for (int col_index : column_indeces){
        std::string metadata_min_name = "min_" + std::to_string(col_index) + '_' + names[col_index];
        std::string metadata_max_name = "max_" + std::to_string(col_index) + '_' + names[col_index];
        if (std::find(metadata_names.begin(), metadata_names.end(), metadata_min_name) != metadata_names.end() &&
                std::find(metadata_names.begin(), metadata_names.end(), metadata_max_name) != metadata_names.end()){
            valid_metadata_columns.push_back(true);

            auto it = std::find(metadata_names.begin(), metadata_names.end(), metadata_min_name);
            int min_col_index = std::distance(metadata_names.begin(), it);
            projected_metadata_cols.emplace_back(std::move(metadata_columns[min_col_index]));
            projected_metadata_cols.emplace_back(std::move(metadata_columns[min_col_index + 1]));
        } else {
            valid_metadata_columns.push_back(false);
            projected_metadata_cols.emplace_back(std::move(std::make_unique<ral::frame::BlazingColumnView>(temp_no_data->view()))); // these are dummy columns that we wont actually use
            projected_metadata_cols.emplace_back(std::move(std::make_unique<ral::frame::BlazingColumnView>(temp_no_data->view())));
        }
    }

    // process filter_string to convert to skip data version
    ral::parser::parse_tree tree;
    if (tree.build(filter_string)){
        // lets drop all columns that do not have skip data
        for (size_t i = 0; i < valid_metadata_columns.size(); i++){
            if (!valid_metadata_columns[i]) { // if this column has no metadata lets drop it from the expression tree
                drop_value(tree, "$" + std::to_string(i));
            }
        }
        if (apply_skip_data_rules(tree)) {
            // std::cout << " skiP-data: " << filter_string << " | " << tree.rebuildExpression() << std::endl;
            filter_string =  tree.rebuildExpression();
        } else{
            return std::make_pair(nullptr, true);
        }
    } else { // something happened and could not process
        return std::make_pair(nullptr, true);
    }

    if (filter_string.empty()) {
        return std::make_pair(nullptr, true);
    }

    // then we follow a similar pattern to process_filter
    std::vector<cudf::column_view> projected_metadata_col_views;
    projected_metadata_col_views.reserve(projected_metadata_cols.size());
    for (auto &&c : projected_metadata_cols) {
        projected_metadata_col_views.push_back(c->view());
    }
    std::vector<std::unique_ptr<ral::frame::BlazingColumn>> evaluated_table = ral::processor::evaluate_expressions(cudf::table_view{projected_metadata_col_views}, {filter_string});

    RAL_EXPECTS(evaluated_table.size() == 1 && evaluated_table[0]->view().type().id() == cudf::type_id::BOOL8, "Expression in skip_data processing did not evaluate to a boolean mask");

    CudfTableView metadata_ids = metadata_view.view().select({metadata_view.num_columns()-2,metadata_view.num_columns()-1});
    std::vector<std::string> metadata_id_names{metadata_view.names()[metadata_view.num_columns()-2], metadata_view.names()[metadata_view.num_columns()-1]};
    ral::frame::BlazingTableView metadata_ids_view(metadata_ids, metadata_id_names);

    std::unique_ptr<ral::frame::BlazingTable> filtered_metadata_ids = ral::processor::applyBooleanFilter(metadata_ids_view, evaluated_table[0]->view());

    return std::make_pair(std::move(filtered_metadata_ids), false);
}


} // namespace skip_data
} // namespace ral
