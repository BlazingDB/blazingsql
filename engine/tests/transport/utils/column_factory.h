//
// Created by aocsa on 9/20/19.
//

#ifndef BLAZINGDB_COLUMN_FACTORY_H
#define BLAZINGDB_COLUMN_FACTORY_H

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <cudf/io/functions.hpp>
#include <cudf/column/column_factories.hpp> 
#include <from_cudf/cpp_tests/utilities/column_utilities.hpp>
#include <from_cudf/cpp_tests/utilities/type_lists.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/strings/detail/utilities.hpp>
#include <string>

#include <cudf/strings/string_view.cuh>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <execution_graph/logic_controllers/LogicPrimitives.h>

namespace blazingdb {
namespace test {

const std::string content =
	R"(0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
6|FRANCE|3|refully final requests. regular, ironi
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully
)";

//static std::vector<gdf_column_cpp> transfor_to_nv_category(cudf::table & input) {
//	std::vector<gdf_column_cpp> columns(input.num_columns());
//	for(size_t i = 0; i < input.num_columns(); i++) {
//		gdf_column * col = input.get_column(i);
//		if(col->dtype == GDF_STRING) {
//			NVStrings * strs = static_cast<NVStrings *>(col->data);
//			NVCategory * category = NVCategory::create_from_strings(*strs);
//			columns[i].create_gdf_column(category, col->size, col->col_name);
//		} else {
//			columns[i].create_gdf_column(col);
//		}
//	}
//	return columns;
//}
namespace cudf_io = cudf::experimental::io;

static ral::frame::BlazingTable build_table() {
	std::string filename = "/tmp/nation.psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	cudf_io::read_csv_args in_args{cudf_io::source_info{filename}};
	in_args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	in_args.dtype = { "int32", "str", "int32", "str"};
	in_args.delimiter = '|';
	in_args.header = -1;

	auto result = cudf_io::read_csv(in_args);
	const auto column_names = result.metadata.column_names;

	return ral::frame::BlazingTable(std::move(result.tbl), column_names);
}

rmm::device_vector<thrust::pair<const char*,cudf::size_type>> create_test_string ();

ral::frame::BlazingTable build_custom_table();


}  // namespace test
}  // namespace blazingdb

#endif  // BLAZINGDB_COLUMN_FACTORY_H
