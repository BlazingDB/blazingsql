#include "GDFColumn.cuh"
#include "GDFCounter.cuh"
#include "from_cudf/cpp_src/utilities/legacy/bit_util.cuh"
#include "gtest/gtest.h"

namespace {

struct GdfColumnCppTest : public testing::Test {
  GdfColumnCppTest() { counter_instance = GDFRefCounter::getInstance(); }

  ~GdfColumnCppTest() {}

  void SetUp() { rmmInitialize(nullptr); }

  void TearDown() override {}

  GDFRefCounter *counter_instance;
};

TEST_F(GdfColumnCppTest, AssignOperatorInGdfCounter) {
  gdf_column *gdf_col_1{};
  gdf_column *gdf_col_2{};
  gdf_column *gdf_col_3{};

  ASSERT_EQ(counter_instance->get_map_size(), 0);

  {
    // initialization
    gdf_column_cpp cpp_col_1;
	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    cpp_col_1.create_gdf_column(GDF_INT32, extra_info, 16, nullptr, 4, "sample");
    gdf_col_1 = cpp_col_1.get_gdf_column();

    gdf_column_cpp cpp_col_2;
    cpp_col_2.create_gdf_column(GDF_INT64, extra_info, 32, nullptr, 8, "sample");
    gdf_col_2 = cpp_col_2.get_gdf_column();

    gdf_column_cpp cpp_col_3;
    cpp_col_3.create_gdf_column(GDF_INT64, extra_info, 32, nullptr, 8, "sample");
    gdf_col_3 = cpp_col_3.get_gdf_column();

    ASSERT_EQ(counter_instance->get_map_size(), 3);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 1);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_2), 1);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_3), 1);

    // test assign operator
    cpp_col_2 = cpp_col_1;

    ASSERT_EQ(counter_instance->get_map_size(), 2);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 2);
    ASSERT_EQ(counter_instance->contains_column(gdf_col_2), false);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_3), 1);

    // test assign operator on equal gdf_column_cpp
    cpp_col_1 = cpp_col_2;

    ASSERT_EQ(counter_instance->get_map_size(), 2);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 2);
    ASSERT_EQ(counter_instance->contains_column(gdf_col_2), false);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_3), 1);

    // test assign operator again
    cpp_col_1 = cpp_col_3;

    ASSERT_EQ(counter_instance->get_map_size(), 2);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 1);
    ASSERT_EQ(counter_instance->contains_column(gdf_col_2), false);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_3), 2);
  }

  ASSERT_EQ(counter_instance->get_map_size(), 0);
  ASSERT_TRUE(counter_instance->contains_column(gdf_col_1) == false);
  ASSERT_TRUE(counter_instance->contains_column(gdf_col_2) == false);
  ASSERT_TRUE(counter_instance->contains_column(gdf_col_3) == false);
}

// void gdf_column_cpp::create_gdf_column(gdf_dtype type,
//                                        size_t num_values,
//                                        void * input_data,
//                                        cudf::valid_type * host_valids,
//                                        size_t width_per_value,
//                                        const std::string &column_name)
TEST_F(GdfColumnCppTest, CreateGdfColumnCppTypeOne_DoubleCreation) {
  gdf_column *gdf_col_1{};
  gdf_column *gdf_col_2{};
  ASSERT_EQ(counter_instance->get_map_size(), 0);

  {
    // initialize
    gdf_column_cpp cpp_col;
	gdf_dtype_extra_info extra_info{TIME_UNIT_NONE};
    cpp_col.create_gdf_column(GDF_INT32, extra_info, 32, nullptr, nullptr, 4, "sample 1");
    gdf_col_1 = cpp_col.get_gdf_column();

    ASSERT_EQ(counter_instance->get_map_size(), 1);
    ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 1);
    ASSERT_EQ(counter_instance->contains_column(gdf_col_2), false);

    // create again - note: os could reuse the pointer
    cpp_col.create_gdf_column(GDF_INT64, extra_info, 64, nullptr, nullptr, 8, "sample 2");
    gdf_col_2 = cpp_col.get_gdf_column();

    ASSERT_EQ(counter_instance->get_map_size(), 1);
    if (gdf_col_1 == gdf_col_2) {
      ASSERT_EQ(counter_instance->column_ref_value(gdf_col_1), 1);
    } else {
      ASSERT_EQ(counter_instance->contains_column(gdf_col_1), false);
      ASSERT_EQ(counter_instance->column_ref_value(gdf_col_2), 1);
    }
  }

  ASSERT_EQ(counter_instance->get_map_size(), 0);
  ASSERT_TRUE(counter_instance->contains_column(gdf_col_1) == false);
  ASSERT_TRUE(counter_instance->contains_column(gdf_col_2) == false);
}

TEST_F(GdfColumnCppTest, CreateGdfColumnCppNVCategoryValids) {
  size_t totalStrings = 7;
  const char *cstrings[] = {"aaaaaaab", nullptr,    "aaaaaaak", nullptr,
                            nullptr,    "aaaaaaax", nullptr};

  NVCategory *category = NVCategory::create_from_array(cstrings, totalStrings);

  gdf_column_cpp cpp_col;
  cpp_col.create_gdf_column(category, totalStrings, "test");

  std::vector<cudf::valid_type> valids(gdf_valid_allocation_size(cpp_col.size()));
  CheckCudaErrors(cudaMemcpy(valids.data(), cpp_col.valid(), valids.size(),
                             cudaMemcpyDeviceToHost));

  for (size_t i = 0; i < totalStrings; i++) {
    ASSERT_EQ(gdf_is_valid(valids.data(), i), cstrings[i] != nullptr);
  }

  ASSERT_EQ(cpp_col.null_count(), 4);
}

} // namespace
