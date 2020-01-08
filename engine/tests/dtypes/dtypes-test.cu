#include <type_traits>

#include <gtest/gtest.h>

#include <CalciteInterpreter.h>
#include <GDFColumn.cuh>

#include "../utils/gdf/library/table_group.h"
#include "../query_test.h"

template <class T>
class DTypesTest : public query_test {
protected:
  void SetUp() {
	  rmmInitialize(nullptr);
  }
  void Check(gdf_column_cpp out_col, T *host_output) {
    T *device_output;
    device_output = new T[out_col.size()];
    cudaMemcpy(device_output,
               out_col.data(),
               out_col.size() * sizeof(T),
               cudaMemcpyDeviceToHost);

    for (cudf::size_type i = 0; i < out_col.size(); i++) {
      ASSERT_TRUE(host_output[i] == device_output[i]);
    }
  }
};

template <class U>
struct DTypeTraits {};

#define DTYPE_FACTORY(U, D)                                                    \
  template <>                                                                  \
  struct DTypeTraits<U> {                                                      \
    static constexpr gdf_dtype dtype = GDF_##D;                                \
  }

DTYPE_FACTORY(std::int8_t, INT8);
DTYPE_FACTORY(std::int16_t, INT16);
DTYPE_FACTORY(std::int32_t, INT32);
DTYPE_FACTORY(std::int64_t, INT64);

//TODO percy noboa gonzales see upgrade to uints
//DTYPE_FACTORY(std::uint8_t, UINT8);
//DTYPE_FACTORY(std::uint16_t, UINT16);
//DTYPE_FACTORY(std::uint32_t, UINT32);
//DTYPE_FACTORY(std::uint64_t, UINT64);

DTYPE_FACTORY(float, FLOAT32);
DTYPE_FACTORY(double, FLOAT64);
DTYPE_FACTORY(char, BOOL8);

#undef DTYPE_FACTORY

using DTypesTestTypes = ::testing::Types<std::int8_t,
                                         std::int16_t,
                                         std::int32_t,
                                         std::int64_t,

                                         //TODO percy noboa gonzales see upgrade to uints
//                                         std::uint8_t,
//                                         std::uint16_t,
//                                         std::uint32_t,
//                                         std::uint64_t,

                                         float,
                                         double>;
TYPED_TEST_CASE(DTypesTest, DTypesTestTypes);

template <class T = void>
class floating : public std::false_type {};
template <>
class floating<float> : public std::true_type {};
template <>
class floating<double> : public std::true_type {};

TYPED_TEST(DTypesTest, withGdfDType) {
  using gdf::library::DType;
  using gdf::library::Index;
  using gdf::library::TableGroupBuilder;

  using RType = DType<DTypeTraits<TypeParam>::dtype>;

  auto input_tables =
    TableGroupBuilder{
      {"hr.emps",
       {
         {"x",
          [](Index i) -> RType {
            return i % 2 ? (floating<TypeParam>::value
                              ? static_cast<TypeParam>(i) / 1000
                              : i)
                         : 1;
          }},
         {"y",
          [](Index i) -> RType {
            return floating<TypeParam>::value
                     ? static_cast<TypeParam>(i) / 100000
                     : i;
          }},
         {"z", [](Index) -> RType { return 1; }},
       }},
      {"hr.sales",
       {
         {"x",
          [](Index i) -> RType {
            return i % 2 ? (floating<TypeParam>::value
                              ? static_cast<TypeParam>(i) / 1000
                              : i)
                         : 1;
          }},
         {"y",
          [](Index i) -> RType {
            return floating<TypeParam>::value
                     ? static_cast<TypeParam>(i) / 100000
                     : i;
          }},
         {"z", [](Index) -> RType { return 1; }},
       }},
    }
      .Build(100);

  input_tables[0].print(std::cout);

  std::vector<std::string>               table_names  = {"hr.emps", "hr.sales"};
  std::vector<std::vector<std::string> > column_names = {{"x", "y", "z"},
                                                         {"a", "b", "x"}};
  std::vector<gdf_column_cpp>            outputs;
  {
    std::string query = "\
LogicalProject(S=[-($0, $1)])\n\
  LogicalTableScan(table=[[hr, emps]])";

    gdf_error err = interpreter_wrapper::evaluate_query(
      input_tables.ToBlazingFrame(), table_names, column_names, query, outputs);
    EXPECT_TRUE(err == GDF_SUCCESS);
    EXPECT_TRUE(outputs.size() == 1);

    TypeParam *host_output = new TypeParam[100];
    for (std::size_t i = 0; i < 100; i++) {
      host_output[i] = input_tables[0][0][i].get<RType::value>()
                       - input_tables[0][1][i].get<RType::value>();
    }

    this->Check(outputs[0], host_output);
  }
}
