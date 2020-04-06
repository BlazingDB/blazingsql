#pragma once

#include <iostream>
#include <vector>

#define STRINGIFY_DETAIL(x) #x
#define RAL_STRINGIFY(x) STRINGIFY_DETAIL(x)

#define RAL_EXPECTS(cond, reason)                            \
  (!!(cond))                                                 \
      ? static_cast<void>(0)                                 \
      : throw cudf::logic_error("Ral failure at: " __FILE__ \
                                ":" RAL_STRINGIFY(__LINE__) ": " reason)

#define RAL_FAIL(reason)                              \
  throw cudf::logic_error("Ral failure at: " __FILE__ \
                          ":" CUDF_STRINGIFY(__LINE__) ": " reason)
