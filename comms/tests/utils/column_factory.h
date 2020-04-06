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

#include <cudf.h>
#include <nvstrings/NVCategory.h>
#include <nvstrings/NVStrings.h>
#include <algorithm>
#include "gpu_functions.h"
#include "rmm/rmm.h"

namespace blazingdb {
namespace test {

const std::string BASE_MESSAGE{"ERROR, cuDF::Allocator, "};

void allocate(void** pointer, std::size_t size, cudaStream_t stream = 0) {
  auto error = RMM_ALLOC(pointer, size, stream);

  if (error != RMM_SUCCESS) {
    throw std::runtime_error(BASE_MESSAGE);
  }
}

gdf_valid_type* allocate_valid(gdf_column* col) {
  size_t num_values = col->size;
  gdf_valid_type* valid_device;
  auto allocated_size_valid = gdf_valid_allocation_size(
      num_values);  // so allocations are supposed to be 64byte aligned

  allocate((void**)&valid_device, allocated_size_valid);
  CheckCudaErrors(cudaMemset(
      valid_device, (gdf_valid_type)255,
      allocated_size_valid));  // assume all relevant bits are set to on
  return valid_device;
}

static gdf_column* create_nv_category_gdf_column(NVCategory* category,
                                                 size_t num_values, char* str) {
  using nv_category_index_type = int;

  gdf_column* column = new gdf_column;
  // TODO crate column here
  column = new gdf_column;
  gdf_dtype type = GDF_STRING_CATEGORY;
  gdf_column_view(column, nullptr, nullptr, num_values, type);

  column->dtype_info.category = (void*)category;
  column->dtype_info.time_unit =
      TIME_UNIT_NONE;  // TODO this should not be hardcoded
  auto allocated_size_data = sizeof(nv_category_index_type) * column->size;
  allocate((void**)&column->data, allocated_size_data);
  CheckCudaErrors(cudaMemcpy(column->data, category->values_cptr(),
                             allocated_size_data, cudaMemcpyDeviceToDevice));

  column->valid = nullptr;
  auto allocated_size_valid = 0;
  column->null_count = 0;

  if (category->has_nulls()) {
    column->valid = allocate_valid(column);
    column->null_count =
        category->set_null_bitarray((gdf_valid_type*)column->valid);
  }
  if (str) column->col_name = str;
  return column;
}

const std::string content =
    R"(0|ALGERIA|0|A. haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|B. al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|C. y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|D. eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|E. y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d)";

static cudf::table transfor_to_nv_category(cudf::table& input) {
  std::vector<gdf_column*> columns;
  for (size_t i = 0; i < input.num_columns(); i++) {
    gdf_column* col = input.get_column(i);
    if (col->dtype == GDF_STRING) {
      NVStrings* strs = static_cast<NVStrings*>(col->data);
      NVCategory* category = NVCategory::create_from_strings(*strs);
      auto nv_col =
          create_nv_category_gdf_column(category, col->size, col->col_name);
      columns.push_back(nv_col);
    } else {
      columns.push_back(col);
    }
  }
  return cudf::table{columns};
}

static cudf::table build_table() {
  std::cout << "csv_with_strings\n";
  std::string filename = "/tmp/nation.psv";
  std::ofstream outfile(filename, std::ofstream::out);
  outfile << content << std::endl;
  outfile.close();

  cudf::csv_read_arg args(cudf::source_info{filename});
  args.names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
  args.dtype = {"int32", "int64", "int32", "int64"};
  args.header = -1;
  args.delimiter = '|';
  args.use_cols_names = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};

  cudf::table table = cudf::read_csv(args);
  std::cout << "table_size: " << table.num_columns() << "|" << table.num_rows()
            << std::endl;
  return transfor_to_nv_category(table);
}

}  // namespace test
}  // namespace blazingdb

#endif  // BLAZINGDB_COLUMN_FACTORY_H
