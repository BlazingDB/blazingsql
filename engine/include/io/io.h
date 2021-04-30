#pragma once

//#include "cudf/cudf.h"

#include "../src/io/DataType.h"
#include <map>
#include <string>
#include <vector>
#include <set>
#include <arrow/table.h>
#include <memory>
#include <execution_graph/logic_controllers/LogicPrimitives.h>
#include "../../src/error.hpp"

typedef ral::io::DataType DataType;

struct PartitionedResultSet {
	std::vector<std::unique_ptr<cudf::table>> cudfTables;
	std::vector<std::string> names;
	bool skipdata_analysis_fail;
};

struct ResultSet {
	std::unique_ptr<cudf::table> cudfTable;
	std::vector<std::string> names;
	bool skipdata_analysis_fail;
};

struct TableSchema {
	std::vector<ral::frame::BlazingTableView> blazingTableViews;
	std::vector<cudf::type_id> types;
	std::vector<std::string> files;
	std::vector<std::string> datasource;
	std::vector<std::string> names;
	std::vector<size_t> calcite_to_file_indices;
	std::vector<bool> in_file;
	int data_type;
	bool has_header_csv = false;

	ral::frame::BlazingTableView metadata;
	std::vector<std::vector<int>> row_groups_ids;
	std::shared_ptr<arrow::Table> arrow_table; //must be a vector?
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
	std::string endpointOverride;
	std::string region;
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

struct FolderPartitionMetadata {
	std::string name;
	std::set<std::string> values;
	cudf::type_id data_type;
};

TableSchema parseSchema(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, cudf::type_id>> extra_columns,
	bool ignore_missing_paths);

std::unique_ptr<ResultSet> parseMetadata(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values);

std::pair<bool, std::string> registerFileSystemHDFS(HDFS hdfs, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemGCS(GCS gcs, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemS3(S3 s3, std::string root, std::string authority);
std::pair<bool, std::string> registerFileSystemLocal(std::string root, std::string authority);

std::vector<FolderPartitionMetadata> inferFolderPartitionMetadata(std::string folder_path);

extern "C" {

std::pair<TableSchema, error_code_t> parseSchema_C(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, cudf::type_id>> extra_columns,
	bool ignore_missing_paths);

std::pair<std::unique_ptr<ResultSet>, error_code_t> parseMetadata_C(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values);

std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemHDFS_C(HDFS hdfs, std::string root, std::string authority);
std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemGCS_C(GCS gcs, std::string root, std::string authority);
std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemS3_C(S3 s3, std::string root, std::string authority);
std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemLocal_C(std::string root, std::string authority);

} // extern "C"
