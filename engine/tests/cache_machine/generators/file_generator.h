#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <src/io/data_parser/CSVParser.h>
#include <src/io/data_parser/ParquetParser.h>
#include <src/io/data_provider/UriDataProvider.h>
#include <blazingdb/manager/Context.h>

namespace blazingdb {
namespace test {
using data_provider_pair = std::pair<std::shared_ptr<ral::io::csv_parser>, std::shared_ptr<ral::io::uri_data_provider>>;

using data_parquet_provider_pair = std::tuple<std::shared_ptr<ral::io::parquet_parser>, std::shared_ptr<ral::io::uri_data_provider>, ral::io::Schema>;

using Context = blazingdb::manager::Context;

data_provider_pair CreateCsvCustomerTableProvider(int index = 0);

data_provider_pair CreateCsvOrderTableProvider(int index = 0);

data_provider_pair CreateCsvNationTableProvider(int index = 0);

data_parquet_provider_pair CreateParquetCustomerTableProvider(Context * context, int n_batches);

data_parquet_provider_pair CreateParquetOrderTableProvider(Context * context, int n_batches);

data_parquet_provider_pair CreateParquetNationTableProvider(Context * context, int n_batches);

}  // namespace test
}  // namespace blazingdb
