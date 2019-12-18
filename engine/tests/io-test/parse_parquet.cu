#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "io/data_parser/DataParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/data_provider/UriDataProvider.h"
#include <gdf_wrapper/gdf_wrapper.cuh>

#include <DataFrame.h>
#include <fstream>

#include <GDFColumn.cuh>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "blazingdb/io/Library/Logging/ServiceLogging.h"
#include <blazingdb/io/Library/Logging/CoutOutput.h>
#include <blazingdb/io/Library/Logging/Logger.h>

class ParquetReaderAPITest : public testing::Test {
protected:
  ParquetReaderAPITest() : filename("/tmp/simple_parquet") {}

  std::int32_t genInt32(int i) {
    if (i >= 100 && i < 150) {
      return 10000;
    } else if (i >= 200 && i < 300) {
      return 20000;
    } else if (i >= 310 && i < 350) {
      return 30000;
    } else if (i >= 450 && i < 550) {
      return 40000;
    } else if (i >= 800 && i < 950) {
      return 50000;
    } else {
      return i * 100;
    }
  }

  std::int64_t genInt64(int i) {
    if (i >= 100 && i < 150) {
      return 10000;
    } else if (i >= 200 && i < 300) {
      return 20000;
    } else if (i >= 310 && i < 350) {
      return 30000;
    } else if (i >= 450 && i < 550) {
      return 40000;
    } else if (i >= 800 && i < 950) {
      return 50000;
    } else {
      return i * 100000;
    }
  }

  void SetUp() final {
    rmmInitialize(nullptr); 
    auto output = new Library::Logging::CoutOutput();
    Library::Logging::ServiceLogging::getInstance().setLogOutput(output);

    static constexpr std::size_t kGroups = 3;
    static constexpr std::size_t kRowsPerGroup = 15;
    try {

      std::shared_ptr<::arrow::io::FileOutputStream> stream;
      PARQUET_THROW_NOT_OK(
          ::arrow::io::FileOutputStream::Open(filename, &stream));

      std::shared_ptr<::parquet::schema::GroupNode> schema = CreateSchema();

      ::parquet::WriterProperties::Builder builder;
      builder.compression(::parquet::Compression::SNAPPY);
      std::shared_ptr<::parquet::WriterProperties> properties = builder.build();

      std::shared_ptr<::parquet::ParquetFileWriter> file_writer =
          ::parquet::ParquetFileWriter::Open(stream, schema, properties);

      std::int16_t repetition_level = 0;

      for (std::size_t i = 0; i < kGroups; i++) {
        ::parquet::RowGroupWriter *row_group_writer =
            file_writer->AppendRowGroup(kRowsPerGroup);

        ::parquet::BoolWriter *bool_writer =
            static_cast<::parquet::BoolWriter *>(
                row_group_writer->NextColumn());
        for (std::size_t j = 0; j < kRowsPerGroup; j++) {
          int ind = i * kRowsPerGroup + j;
          std::int16_t definition_level = ind % 3 > 0 ? 1 : 0;
          bool bool_value = true;
          bool_writer->WriteBatch(1, &definition_level, &repetition_level,
                                  &bool_value);
        }

        ::parquet::Int32Writer *int32_writer =
            static_cast<::parquet::Int32Writer *>(
                row_group_writer->NextColumn());
        for (std::size_t j = 0; j < kRowsPerGroup; j++) {
          int ind = i * kRowsPerGroup + j;
          std::int16_t definition_level = ind % 3 > 0 ? 1 : 0;
          std::int32_t int32_value = genInt32(ind);
          int32_writer->WriteBatch(1, &definition_level, &repetition_level,
                                   &int32_value);
        }

        ::parquet::Int64Writer *int64_writer =
            static_cast<::parquet::Int64Writer *>(
                row_group_writer->NextColumn());
        for (std::size_t j = 0; j < kRowsPerGroup; j++) {
          int ind = i * kRowsPerGroup + j;
          std::int16_t definition_level = ind % 3 > 0 ? 1 : 0;
          std::int64_t int64_value = genInt64(ind);
          int64_writer->WriteBatch(1, &definition_level, &repetition_level,
                                   &int64_value);
        }

        ::parquet::DoubleWriter *double_writer =
            static_cast<::parquet::DoubleWriter *>(
                row_group_writer->NextColumn());
        for (std::size_t j = 0; j < kRowsPerGroup; j++) {
          int ind = i * kRowsPerGroup + j;
          std::int16_t definition_level = ind % 3 > 0 ? 1 : 0;
          double double_value = (double)ind;
          double_writer->WriteBatch(1, &definition_level, &repetition_level,
                                    &double_value);
        }
      }

      file_writer->Close();

      DCHECK(stream->Close().ok());
    } catch (const std::exception &e) {
      FAIL() << "Generate file" << e.what();
    }
  }

  std ::shared_ptr<::parquet::schema::GroupNode> CreateSchema() {
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
        ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            ::parquet::schema::NodeVector{
                ::parquet::schema::PrimitiveNode::Make(
                    "boolean_field", ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::BOOLEAN, ::parquet::ConvertedType::NONE),
                ::parquet::schema::PrimitiveNode::Make(
                    "int32_field", ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::INT32, ::parquet::ConvertedType::NONE),
                ::parquet::schema::PrimitiveNode::Make(
                    "int64_field", ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::INT64, ::parquet::ConvertedType::NONE),
                ::parquet::schema::PrimitiveNode::Make(
                    "double_field", ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::DOUBLE, ::parquet::ConvertedType::NONE),
            }));
  }

  void TearDown() final {
    if (std::remove(filename.c_str())) {
      FAIL() << "Remove file";
    }
  }

  void checkNulls(/*const */ gdf_column &column) {

    const std::size_t valid_size = arrow::BitUtil::BytesForBits(column.size);
    const std::size_t valid_last = valid_size - 1;

    int fails = 0;
    for (std::size_t i = 0; i < valid_last; i++) {

      if (i % 3 == 0) {
        std::uint8_t valid = column.valid[i];
        std::uint8_t expected = 0b10110110;
        EXPECT_EQ(expected, valid);
        if (expected != valid) {
          std::cout << "fail at checkNulls i: " << i << std::endl;
          fails++;
          if (fails > 5)
            break;
        }
      } else if (i % 3 == 1) {
        std::uint8_t valid = column.valid[i];
        std::uint8_t expected = 0b01101101;
        EXPECT_EQ(expected, valid);
        if (expected != valid) {
          std::cout << "fail at checkNulls i: " << i << std::endl;
          fails++;
          if (fails > 5)
            break;
        }
      } else {
        std::uint8_t valid = column.valid[i];
        std::uint8_t expected = 0b11011011;
        EXPECT_EQ(expected, valid);
        if (expected != valid) {
          std::cout << "fail at checkNulls i: " << i << std::endl;
          fails++;
          if (fails > 5)
            break;
        }
      }
    }
    //        EXPECT_EQ(0b00101101, 0b00101101 & column.valid[valid_last]);
  }

  const std::string filename;

  gdf_column *columns = nullptr;
  std::size_t columns_length = 0;
};

/**
 *
 TODO: fix this test
TEST_F(ParquetReaderAPITest, ByIdsInFromInterface) {

    std::shared_ptr<::arrow::io::ReadableFile> file;
    const ::parquet::ReaderProperties          properties =
      ::parquet::default_reader_properties();
    ::arrow::io::ReadableFile::Open(filename, properties.memory_pool(), &file);


     std::vector<Uri> uris = {Uri{this->filename}};
    std::vector<bool> include_column = {true, false, true, false};
    std::unique_ptr<ral::io::data_provider> provider =
std::make_unique<ral::io::uri_data_provider>(uris);
    std::unique_ptr<ral::io::data_parser> parser =
std::make_unique<ral::io::parquet_parser>();


    EXPECT_TRUE(provider->has_next());
    std::vector<gdf_column_cpp> gdf_columns_cpp;
    parser->parse(provider->get_next(), gdf_columns_cpp, include_column);

    for(size_t column_index = 0; column_index < gdf_columns_cpp.size();
column_index++){ std::cout << "col_name: " <<
gdf_columns_cpp[column_index].get_gdf_column()->col_name << std::endl;
        print_gdf_column(gdf_columns_cpp[column_index].get_gdf_column());
    }
}
*/
