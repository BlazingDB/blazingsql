#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <cudf_test/column_wrapper.hpp>
#include <cudf_test/base_fixture.hpp>
#include <cudf_test/type_lists.hpp>
#include <cudf_test/table_utilities.hpp>
#include "tests/utilities/BlazingUnitTest.h"
#include "io/data_parser/metadata/parquet_metadata.h"
#include <parquet/statistics.h>
#include <parquet/types.h>
#include <parquet/schema.h>

struct ParquetMetadataTest : public ::testing::Test {
	ParquetMetadataTest() {}
	~ParquetMetadataTest() {}
};

/* This function implements the same functionality from function get_minmax_metadata() without parquet calls.
   Reproduces the issue when the float type is processed.
*/
template<typename T, parquet::Type::type FType>
void process_minmax_metadata(){
    cudf::test::fixed_width_column_wrapper<T> col1{{std::numeric_limits<T>::min(), std::numeric_limits<T>::min()}};
    cudf::test::fixed_width_column_wrapper<T> col2{{std::numeric_limits<T>::max(), std::numeric_limits<T>::max()}};

    cudf::table_view in_table_view {{col1, col2}};
    cudf::data_type t{in_table_view.column(0).type()};

    std::shared_ptr<parquet::schema::Node> node = parquet::schema::PrimitiveNode::Make("foo", parquet::Repetition::OPTIONAL, FType);
    parquet::ColumnDescriptor descr(node, 1, 1);

    parquet::ColumnDescriptor* desc = &descr;
    T min = std::numeric_limits<T>::min();
    T max = std::numeric_limits<T>::max();
    std::string encoded_min = std::string((const char*)&min, sizeof(min));
    std::string encoded_max = std::string((const char*)&max, sizeof(max));

    size_t num_files = 2;
    size_t total_num_row_groups=2; /*1 rowgroup by each file*/
    size_t columns_with_metadata=1;

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::shared_ptr<parquet::Statistics> statistics = parquet::Statistics::Make(desc,
         encoded_min, encoded_max, 100 /*num_values*/, 0 /*null_count*/, 0 /*distinct_count*/, true /*has_min_max*/,
          false /*has_null_count*/,  false/*has_distinct_count*/, pool);

    size_t num_metadata_cols = 2; //min and max
    std::vector<std::vector<std::vector<int64_t>>> minmax_metadata_table_per_file(num_files);

    for(size_t file_index=0;file_index<num_files;file_index++){
        std::vector<std::vector<int64_t>> this_minmax_metadata_table(num_metadata_cols);
        int num_row_groups = 1;
        for (int row_group_index = 0; row_group_index < num_row_groups; row_group_index++) {
            parquet::Type::type physical = FType;
            parquet::ConvertedType::type logical = parquet::ConvertedType::type::INT_64;

            for (size_t col_count = 0; col_count < columns_with_metadata; col_count++) {
                set_min_max(this_minmax_metadata_table, col_count*2, physical, logical, statistics);
            }
        }
        minmax_metadata_table_per_file[file_index] = std::move(this_minmax_metadata_table);
    }

    size_t valid_parquet_reader = 0;
    std::vector<std::vector<int64_t>> minmax_metadata_table = minmax_metadata_table_per_file[valid_parquet_reader];

    for (size_t i = valid_parquet_reader + 1; i < minmax_metadata_table_per_file.size(); i++) {
        for (size_t j = 0; j < 	minmax_metadata_table_per_file[i].size(); j++) {
            std::copy(minmax_metadata_table_per_file[i][j].begin(), minmax_metadata_table_per_file[i][j].end(), std::back_inserter(minmax_metadata_table[j]));
        }
    }

    auto dtype = in_table_view.column(0).type();
    for (size_t index = 0; index < 	minmax_metadata_table.size(); index++) {
        auto vector = minmax_metadata_table[index];
        std::basic_string<char> content = get_typed_vector_content(dtype.id(), vector);
        auto column = make_cudf_column_from_vector(dtype, content, total_num_row_groups);
        cudf::test::expect_columns_equal(column->view(), in_table_view.column(index));
    }
}

TEST_F(ParquetMetadataTest, typed_test) {
    process_minmax_metadata<bool, parquet::Type::type::BOOLEAN>();
    process_minmax_metadata<float, parquet::Type::type::FLOAT>();
    process_minmax_metadata<int32_t, parquet::Type::type::INT32>();
    process_minmax_metadata<int64_t, parquet::Type::type::INT64>();
    process_minmax_metadata<double, parquet::Type::type::DOUBLE>();
}
