#pragma once 

#include <src/io/data_parser/CSVParser.h>
#include <src/io/data_parser/ParquetParser.h>
#include <src/io/data_provider/UriDataProvider.h>

namespace blazingdb {
namespace test {
using data_provider_pair =
		std::pair<std::shared_ptr<ral::io::csv_parser>, std::shared_ptr<ral::io::uri_data_provider>>;

data_provider_pair CreateCsvCustomerTableProvider(int index = 0);

data_provider_pair CreateCsvOrderTableProvider(int index = 0);


using data_parquet_provider_pair =
		std::pair<std::shared_ptr<ral::io::parquet_parser>, std::shared_ptr<ral::io::uri_data_provider>>;


// data_parquet_provider_pair CreateParquetCustomerTableProvider(int index = 0);

// data_parquet_provider_pair CreateParquetOrderTableProvider(int index = 0);

}  // namespace test
}  // namespace blazingdb