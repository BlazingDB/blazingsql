#include "utils.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

#include "parser/expression_utils.hpp"

namespace ral {
namespace skip_data {

bool is_binary_op(const std::string &test) {
  return interops::is_binary_operator(map_to_operator_type(test));
}

bool is_unsupported_binary_op(const std::string &test) {
  const static std::vector<std::string> supported_operators = {
      "=", ">", ">=", "<", "<=", "+", "-", "AND", "OR"};
  return std::none_of(supported_operators.begin(), supported_operators.end(),
                      [&test](std::string op) { return op == test; });
}
bool is_unary_op(const std::string &test) {
  return interops::is_unary_operator(map_to_operator_type(test));
}

// Non skip data support exclusion rules:
bool is_exclusion_unary_op(const std::string &test) {
  // TODO: quitar not de esta list:
  //  NOT $1 ==> NOT $1, no valido por ahora!
  //   NOT esta siendo invalidado porque parte de la logica asume que true es
  //   seguro (porque no aplicaria un filtro)!
  //     => un not podria invalidar esta premisa

  if (test == "NONE") {
    // special type
    return true;
  }
  
  return interops::is_unary_operator(map_to_operator_type(test));
}

int get_id(const std::string &s) {
  auto text = s.substr(1);
  int number;
  std::istringstream iss(text);
  iss >> number;
  if (iss.fail()) {
    return -1;
  }
  return number;
}

std::vector<std::string> split(const std::string &str,
                               const std::string &delim) {
  std::vector<std::string> tokens;
  size_t prev = 0, pos = 0;
  do {
    pos = str.find(delim, prev);
    if (pos == std::string::npos)
      pos = str.length();
    std::string token = str.substr(prev, pos - prev);
    if (!token.empty())
      tokens.push_back(token);
    prev = pos + delim.length();
  } while (pos < str.length() && prev < str.length());
  return tokens;
}
} // namespace skip_data
} // namespace ral
