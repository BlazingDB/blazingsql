//
// Created by aocsa on 2/6/20.
//
#pragma once

#include <thrust/sequence.h>
#include <cudf/cudf.h>
#include <cudf/types.hpp>
#include <thrust/device_vector.h>
#include <column/column_factories.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>

#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <from_cudf/cpp_tests/utilities/column_wrapper.hpp>
#include <from_cudf/cpp_tests/utilities/table_utilities.hpp>

std::unique_ptr<ral::frame::BlazingTable> build_custom_table() ;

std::unique_ptr<ral::frame::BlazingTable> build_custom_one_column_table() ;
