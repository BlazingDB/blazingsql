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

#include "rmm/rmm.h"
#include <algorithm>
#include <cudf.h>
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>

#include <GDFColumn.cuh>
#include <GDFCounter.cuh>

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
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
6|FRANCE|3|refully final requests. regular, ironi
7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco
8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun
9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
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

static std::vector<gdf_column_cpp> transfor_to_nv_category(cudf::table & input) {
	std::vector<gdf_column_cpp> columns(input.num_columns());
	for(size_t i = 0; i < input.num_columns(); i++) {
		gdf_column * col = input.get_column(i);
		if(col->dtype == GDF_STRING) {
			NVStrings * strs = static_cast<NVStrings *>(col->data);
			NVCategory * category = NVCategory::create_from_strings(*strs);
			columns[i].create_gdf_column(category, col->size, col->col_name);
		} else {
			columns[i].create_gdf_column(col);
		}
	}
	return columns;
}

static std::vector<gdf_column_cpp> build_table() {
	std::cout << "csv_with_strings\n";
	std::string filename = "/tmp/nation.psv";
	std::ofstream outfile(filename, std::ofstream::out);
	outfile << content << std::endl;
	outfile.close();

	cudf::csv_read_arg args(cudf::source_info{filename});
	args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
	args.dtype = {"int32", "str", "int32", "str"};
	args.header = -1;
	args.delimiter = '|';
	args.use_cols_names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};

	cudf::table table = cudf::read_csv(args);
	std::cout << "table_size: " << table.num_columns() << "|" << table.num_rows() << std::endl;
	return transfor_to_nv_category(table);
}

}  // namespace test
}  // namespace blazingdb

#endif  // BLAZINGDB_COLUMN_FACTORY_H
