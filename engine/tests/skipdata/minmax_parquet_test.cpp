#include <gtest/gtest.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "io/Metadata.h"
#include "io/DataLoader.h"
#include "io/data_parser/DataParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/data_provider/UriDataProvider.h"
#include <DataFrame.h>
#include <fstream>
#include <gdf_wrapper/gdf_wrapper.cuh>

#include <GDFColumn.cuh>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <boost/filesystem.hpp>

#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

struct MinMaxParquetTest : public testing::Test {
  MinMaxParquetTest() : filename("/tmp/simple_parquet") {}

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
	
    static constexpr std::size_t kGroups = 3;
    static constexpr std::size_t kRowsPerGroup = 5;
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

void print_minmax(std::string parquetFileLocation) {

  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::OpenFile(parquetFileLocation, false);

  std::shared_ptr<parquet::FileMetaData> file_metadata =
      parquet_reader->metadata();

  const parquet::SchemaDescriptor *schema = file_metadata->schema();
  int numRowGroups = file_metadata->num_row_groups();
  std::vector<unsigned long long> numRowsPerGroup(numRowGroups);

  for (int j = 0; j < numRowGroups; j++) {
    std::shared_ptr<parquet::RowGroupReader> groupReader =
        parquet_reader->RowGroup(j);
    const parquet::RowGroupMetaData *rowGroupMetadata = groupReader->metadata();
    numRowsPerGroup[j] = rowGroupMetadata->num_rows();
  }
  parquet_reader->Close();
  // std::vector<std::shared_ptr<Container>> columns;

  for (int rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++) {
    std::cout << "==> row_group: " << rowGroupIndex << std::endl;
    std::shared_ptr<parquet::RowGroupReader> groupReader =
        parquet_reader->RowGroup(rowGroupIndex);
    const parquet::RowGroupMetaData *rowGroupMetadata = groupReader->metadata();
    for (int blazingColumnIndex = 0;
         blazingColumnIndex < file_metadata->num_columns();
         blazingColumnIndex++) {
      const parquet::ColumnDescriptor *column =
          schema->Column(blazingColumnIndex);
      std::unique_ptr<parquet::ColumnChunkMetaData> columnMetaData =
          rowGroupMetadata->ColumnChunk(blazingColumnIndex);
      parquet::Type::type type = column->physical_type();

      if (columnMetaData->is_stats_set()) {
        std::shared_ptr<parquet::Statistics> statistics =
            columnMetaData->statistics();
        if (statistics->HasMinMax()) {
          if (type == parquet::Type::BOOLEAN) {
            std::shared_ptr<parquet::BoolStatistics> convertedStats =
                std::static_pointer_cast<parquet::BoolStatistics>(statistics);
            std::cout << "BOOLEAN"
                      << "|" << convertedStats->min() << "|"
                      << convertedStats->max() << std::endl;
          } else if (type == parquet::Type::INT32) {
            std::shared_ptr<parquet::Int32Statistics> convertedStats =
                std::static_pointer_cast<parquet::Int32Statistics>(statistics);
            if (column->converted_type() == parquet::ConvertedType::INT_32 ||
                column->converted_type() == parquet::ConvertedType::NONE) {
              std::cout << "INT_32"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            } else if (column->converted_type() ==
                       parquet::ConvertedType::INT_16) {
              std::cout << "INT_16"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            } else if (column->converted_type() ==
                       parquet::ConvertedType::INT_8) {
              std::cout << "INT_8"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            } else if (column->converted_type() ==
                       parquet::ConvertedType::DATE) {

              std::cout << "DATE"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;

            } else if (column->converted_type() ==
                       parquet::ConvertedType::TIME_MILLIS) {
              std::cout << "TIME_MILLIS"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            }
          } else if (type == parquet::Type::INT64) {
            std::shared_ptr<parquet::Int64Statistics> convertedStats =
                std::static_pointer_cast<parquet::Int64Statistics>(statistics);
            if (column->converted_type() == parquet::ConvertedType::INT_64 ||
                column->converted_type() == parquet::ConvertedType::NONE) {
              std::cout << "INT_64"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            } else if (column->converted_type() ==
                       parquet::ConvertedType::TIMESTAMP_MILLIS) {
              std::cout << "TIMESTAMP_MILLIS"
                        << "|" << convertedStats->min() << "|"
                        << convertedStats->max() << std::endl;
            }
          } else if (type == parquet::Type::FLOAT) {
            std::shared_ptr<parquet::FloatStatistics> convertedStats =
                std::static_pointer_cast<parquet::FloatStatistics>(statistics);
            std::cout << "FLOAT"
                      << "|" << convertedStats->min() << "|"
                      << convertedStats->max() << std::endl;
          } else if (type == parquet::Type::DOUBLE) {
            std::shared_ptr<parquet::DoubleStatistics> convertedStats =
                std::static_pointer_cast<parquet::DoubleStatistics>(statistics);
            std::cout << "DOUBLE"
                      << "|" << convertedStats->min() << "|"
                      << convertedStats->max() << std::endl;
          } else if (type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
            std::shared_ptr<parquet::FLBAStatistics> convertedStats =
                std::static_pointer_cast<parquet::FLBAStatistics>(statistics);

          } else if (type == parquet::Type::BYTE_ARRAY) {
          }
        }
      }
      const std::shared_ptr<parquet::ColumnReader> columnReader =
          groupReader->Column(blazingColumnIndex);
      int64_t numRecords = rowGroupMetadata->num_rows();

      // auto output = containerFrom(columnReader, numRecords);
      // columns.push_back(output);
    }
    std::cout << "<== row_group: " << std::endl;
  }
  // return columns;
}

TEST_F(MinMaxParquetTest, UsingRalIO) {
  std::vector<Uri> uris;
  uris.push_back(Uri{this->filename});

//  ral::io::Schema schema;
//  {
//    //TODO: error with the api, you can't get schema and load_data with the same provider
//    auto parser = std::make_shared<ral::io::parquet_parser>();
//    auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
//    ral::io::data_loader loader(parser, provider);
//    try {
//      loader.get_schema(schema, {});
//    } catch (std::exception &e) {
//       std::cout << "error: " << e.what() << std::endl;

//      return;
//    }
//  }
 auto parser = std::make_shared<ral::io::parquet_parser>();
 auto provider = std::make_shared<ral::io::uri_data_provider>(uris);

 ral::io::data_loader loader(parser, provider);
 
 auto nfiles = 1;
 ral::io::Metadata metadata({}, std::make_pair(0, nfiles));
 loader.get_metadata(metadata, {});
 std::vector<gdf_column_cpp> input_table = metadata.get_columns();

 std::cout << "metadata: " << input_table.size() << std::endl;
 
 for (size_t column_index = 0; column_index < input_table.size();
      column_index++) {
   std::cout << "col_name: "
             << input_table[column_index].name()
             << "|"
             << input_table[column_index].get_gdf_column()->size
             << std::endl;
   print_gdf_column(input_table[column_index].get_gdf_column());
 }
}

TEST_F(MinMaxParquetTest, GetStats) { print_minmax(this->filename); }