#include "../src/gdf_wrapper/gdf_wrapper.cuh"
#include "../src/io/DataType.h"
#include "cudf/legacy/io_types.hpp"
#include <map>
#include <string>
#include <vector>
#include <arrow/table.h>
#include <memory>

#pragma once

typedef ral::io::DataType DataType;

struct ReaderArgs {
	cudf::orc_read_arg orcReaderArg = cudf::orc_read_arg(cudf::source_info(""));
	cudf::json_read_arg jsonReaderArg = cudf::json_read_arg(cudf::source_info(""));
	cudf::csv_read_arg csvReaderArg = cudf::csv_read_arg(cudf::source_info(""));
};

struct TableSchema {
	std::vector<gdf_column *> columns;
	std::vector<std::string> files;
	std::vector<std::string> datasource;
	std::vector<std::string> names;
	std::vector<size_t> calcite_to_file_indices;
	std::vector<size_t> num_row_groups;
	std::vector<bool> in_file;
	int data_type;
	ReaderArgs args;

	std::vector<gdf_column *> metadata;
	std::vector<std::vector<int>> row_groups_ids;
	std::shared_ptr<arrow::Table> arrow_table;
};

struct HDFS {
	std::string host;
	int port;
	std::string user;
	short DriverType;
	std::string kerberosTicket;
};


struct S3 {
	std::string bucketName;
	short encryptionType;
	std::string kmsKeyAmazonResourceName;
	std::string accessKeyId;
	std::string secretKey;
	std::string sessionToken;
};

struct GCS {
	std::string projectId;
	std::string bucketName;
	bool useDefaultAdcJsonFile;
	std::string adcJsonFile;
};


#define parquetFileType 0
#define orcFileType 1
#define csvFileType 2
#define jsonFileType 3
#define gdfFileType 4
#define daskFileType 5


TableSchema parseSchema(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, gdf_dtype>> extra_columns);

TableSchema parseMetadata(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, gdf_dtype>> extra_columns);

std::pair<bool, std::string> registerFileSystemHDFS(HDFS hdfs, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemGCS(GCS gcs, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemS3(S3 s3, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemLocal(std::string root, std::string authority);
