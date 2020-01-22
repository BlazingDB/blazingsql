#pragma once

#include "DataFrame.h"
#include <blazingdb/manager/Context.h>
#include <string>
// Forward declaration
namespace blazingdb {
namespace communication {
class Context;
}
}  // namespace blazingdb

namespace ral {
namespace operators {

// Alias
namespace {
using blazingdb::manager::experimental::Context;
}

bool is_join(const std::string & query_part);

blazing_frame process_join(Context * context, blazing_frame & input, const std::string & query);

}  // namespace operators
}  // namespace ral
