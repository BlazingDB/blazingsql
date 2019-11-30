#ifndef BLAZINGDB_RAL_ORDERBY_OPERATOR_H
#define BLAZINGDB_RAL_ORDERBY_OPERATOR_H

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
#include <vector>

namespace ral {
namespace operators {

namespace {
using blazingdb::manager::Context;
}  // namespace

bool is_sort(std::string query_part);

void process_sort(blazing_frame & input, std::string query_part, Context * queryContext);

}  // namespace operators
}  // namespace ral

#endif  // BLAZINGDB_RAL_ORDERBY_OPERATOR_H
