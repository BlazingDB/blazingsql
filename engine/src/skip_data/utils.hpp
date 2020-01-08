#pragma once
#include <string>
#include <vector>

namespace ral {
namespace skip_data {

bool is_unary_op(const std::string &test);

bool is_binary_op(const std::string &test);

bool is_unsupported_binary_op(const std::string &test);

bool is_exclusion_unary_op(const std::string &test);

int get_id(const std::string &s);

std::vector<std::string> split(const std::string &str,
                               const std::string &delim = " ");
} // namespace skip_data
} // namespace ral