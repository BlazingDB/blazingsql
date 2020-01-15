#pragma once

#include <cudf/types.hpp>
#include <cudf/scalar/scalar.hpp>

namespace strings {

std::unique_ptr<cudf::scalar> str_to_timestamp_scalar( std::string const& str,
                                                      cudf::data_type timestamp_type,
                                                      std::string const& format );

} // namespace strings
