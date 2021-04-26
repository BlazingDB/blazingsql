/*
 * Copyright 2021 Percy Camilo Triveño Aucahuasi <percy.camilo.ta@gmail.com>
 */

#ifndef _BZ_SQL_TRANSPILER_H_
#define _BZ_SQL_TRANSPILER_H_

#include "parser/expression_tree.hpp"

namespace ral {
namespace io {
namespace sql_tools {

/*

Transpiler design:
The first stages should go from parsing the Blazing SQL source to a genereic
SQL AST:

┌──────────────────┐             ┌───────────────┐             ┌────────────────────┐
│Blazing SQL source│ ─ Step 1 ─> │Blazing SQL AST│ ─ Step 2 ─> │Intermediate SQL AST│
└──────────────────┘             └───────────────┘             └────────────────────┘

For the final stages, we should translate from the genereic SQL AST to the actual 
target SQL source.

┌────────────────────┐             ┌──────────────┐              ┌─────────────────┐
│Intermediate SQL AST│ ─ Step 3 ─> │Target SQL AST│ ─ Step 4  ─> │Target SQL source│
└────────────────────┘             └──────────────┘              └─────────────────┘

Here the targets can be: MySQL, PostgreSQL, SQLite, Snowflake, etc
The steps are:
- Step 1: Parse the Blazing SQL source from string to the actual Blazing SQL AST.
- Step 2: Transform the Blazing SQL AST to an Intermediate SQL AST.
- Step 3: Transform the Intermediate SQL AST to the Target SQL AST.
- Step 4: Code generation, here we convert the Target SQL AST to its string
          representation.

NOTE Current implementation and future improvements/ideas:
- for now the backends should provide their transformers
- the source ast and the target ast are based on the same struct: 
  ral::parser::parse_tree 
- for now we can use that one for this use case, but in the future it would be 
  a good idea to create a dedicated ast for each target sql backend/dialect
- maybe we can use calcite before hand here

*/

/**
 * @brief This is the main entry point for the sql predicate transpiler.
 * 
 * @param source The query part corresponding to TableScan/BindableTableScan
 * @param predicate_transformer The transformation rules to be used by the transpiler
 */
std::string transpile_predicate(const std::string &source,
                                ral::parser::node_transformer *predicate_transformer);

struct operator_info {
  std::string label;
  ral::parser::operator_node::placement_type placement = ral::parser::operator_node::AUTO;
  bool parentheses_wrap = true;
};

/**
 * @brief This Returns common operators for a generic SQL dialect.
 */
std::map<operator_type, operator_info> get_default_operators();

/**
 * @brief This is the default transformer for the sql predicate.
 * 
 * If the backend needs a custom/complex transformation logic, then we need to 
 * provide the transformer based on the ral::parser::node_transformer interface
 */
class default_predicate_transformer : public ral::parser::node_transformer {
public:
  default_predicate_transformer(
    const std::vector<int> &column_indices,
    const std::vector<std::string> &column_names,
    const std::map<operator_type, operator_info> &operators);

  virtual ~default_predicate_transformer();

  virtual ral::parser::node * transform(ral::parser::operad_node& node);
  virtual ral::parser::node * transform(ral::parser::operator_node& node);

private:
  std::map<ral::parser::node*, bool> visited;
  std::vector<int> column_indices;
  std::vector<std::string> column_names;
  std::map<operator_type, operator_info> operators;
};

} /* namespace sql_tools */
} /* namespace io */
} /* namespace ral */

#endif // _BZ_SQL_TRANSPILER_H_
