#include "../../include/io/io.h"
#include "../io/DataLoader.h"
#include "../io/Schema.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_provider/UriDataProvider.h"

#include "utilities/CommonOperations.h"
#include "utilities/DebuggingUtils.h"

#include <blazingdb/io/Config/BlazingContext.h>
#include <blazingdb/io/FileSystem/FileSystemConnection.h>
#include <blazingdb/io/FileSystem/FileSystemManager.h>
#include <blazingdb/io/FileSystem/HadoopFileSystem.h>
#include <blazingdb/io/FileSystem/S3FileSystem.h>

// #include <blazingdb/io/Library/Logging/TcpOutput.h>
// #include "blazingdb/io/Library/Network/NormalSyncSocket.h"

#include <numeric>

TableSchema parseSchema(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, cudf::type_id>> extra_columns) {
	const DataType data_type_hint = ral::io::inferDataType(file_format_hint);
	const DataType fileType = inferFileType(files, data_type_hint);
	ReaderArgs args = getReaderArgs(fileType, ral::io::to_map(arg_keys, arg_values));
	TableSchema tableSchema;
	tableSchema.data_type = fileType;
	tableSchema.args.orcReaderArg = args.orcReaderArg;
	tableSchema.args.jsonReaderArg = args.jsonReaderArg;
	tableSchema.args.csvReaderArg = args.csvReaderArg;

	std::shared_ptr<ral::io::data_parser> parser;
	if(fileType == ral::io::DataType::PARQUET) {
		parser = std::make_shared<ral::io::parquet_parser>();
	} else if(fileType == ral::io::DataType::ORC) {
		parser = std::make_shared<ral::io::orc_parser>(args.orcReaderArg);
	} else if(fileType == ral::io::DataType::JSON) {
		parser = std::make_shared<ral::io::json_parser>(args.jsonReaderArg);
	} else if(fileType == ral::io::DataType::CSV) {
		parser = std::make_shared<ral::io::csv_parser>(args.csvReaderArg);
	}

	std::vector<Uri> uris;
	for(auto file_path : files) {
		uris.push_back(Uri{file_path});
	}
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	auto loader = std::make_shared<ral::io::data_loader>(parser, provider);

	ral::io::Schema schema;

	try {
		loader->get_schema(schema, extra_columns);
	} catch(std::exception & e) {
		throw;
	}

	std::vector<size_t> column_indices(schema.get_num_columns());
	std::iota(column_indices.begin(), column_indices.end(), 0);

	tableSchema.types = schema.get_dtypes();
	tableSchema.names = schema.get_names();
	tableSchema.files = schema.get_files();
	tableSchema.calcite_to_file_indices = schema.get_calcite_to_file_indices();
	tableSchema.in_file = schema.get_in_file();

	return tableSchema;
}

std::unique_ptr<ResultSet> parseMetadata(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values) {
	if (offset.second == 0) {
		// cover case for empty files to parse
		
		std::vector<size_t> column_indices(2 * schema.types.size() + 2);
		std::iota(column_indices.begin(), column_indices.end(), 0);

		std::vector<std::string> names(2 * schema.types.size() + 2);
		std::vector<cudf::type_id> dtypes(2 * schema.types.size() + 2);

		size_t index = 0;
		for(; index < schema.types.size(); index++) {
			cudf::type_id dtype = schema.types[index];
			if (dtype == cudf::type_id::STRING)
				dtype = cudf::type_id::INT32;

			dtypes[2*index] = dtype;
			dtypes[2*index + 1] = dtype;
			
			auto col_name_min = "min_" + std::to_string(index) + "_" + schema.names[index];
			auto col_name_max = "max_" + std::to_string(index)  + "_" + schema.names[index];

			names[2*index] = col_name_min;
			names[2*index + 1] = col_name_max;
		}
		dtypes[2*index] = cudf::type_id::INT32;
		names[2*index] = "file_handle_index";

		dtypes[2*index + 1] = cudf::type_id::INT32;
		names[2*index + 1] = "row_group_index";
		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();
		result->names = names;
		auto table = ral::utilities::experimental::create_empty_table(dtypes);
		result->cudfTable = std::move(table);
		result->skipdata_analysis_fail = false;
		return result;
	}
	const DataType data_type_hint = ral::io::inferDataType(file_format_hint);
	const DataType fileType = inferFileType(files, data_type_hint);
	ReaderArgs args = getReaderArgs(fileType, ral::io::to_map(arg_keys, arg_values));

	std::shared_ptr<ral::io::data_parser> parser;
	if(fileType == ral::io::DataType::PARQUET) {
		parser = std::make_shared<ral::io::parquet_parser>();
	} else if(fileType == ral::io::DataType::ORC) {
		parser = std::make_shared<ral::io::orc_parser>(args.orcReaderArg);
	} else if(fileType == ral::io::DataType::JSON) {
		parser = std::make_shared<ral::io::json_parser>(args.jsonReaderArg);
	} else if(fileType == ral::io::DataType::CSV) {
		parser = std::make_shared<ral::io::csv_parser>(args.csvReaderArg);
	}
	std::vector<Uri> uris;
	for(auto file_path : files) {
		uris.push_back(Uri{file_path});
	}
	auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
	auto loader = std::make_shared<ral::io::data_loader>(parser, provider);
	try{
		std::unique_ptr<ral::frame::BlazingTable> metadata = loader->get_metadata(offset.first);
		// ral::utilities::print_blazing_table_view(metadata->toBlazingTableView());
		std::unique_ptr<ResultSet> result = std::make_unique<ResultSet>();
		result->names = metadata->names();
		result->cudfTable = metadata->releaseCudfTable();
		result->skipdata_analysis_fail = false;
		return result;
	} catch(std::exception e) {
		std::cerr << e.what() << std::endl;
		throw e;
	}
}



std::pair<bool, std::string> registerFileSystem(
	FileSystemConnection fileSystemConnection, std::string root, std::string authority) {
	Path rootPath(root);
	if(rootPath.isValid() == false) {
		std::string msg =
			"Invalid root " + root + " for filesystem " + authority + " :" + fileSystemConnection.toString();
		return std::make_pair(false, msg);
	}
	FileSystemEntity fileSystemEntity(authority, fileSystemConnection, rootPath);
	try {
		bool ok = BlazingContext::getInstance()->getFileSystemManager()->deregisterFileSystem(authority);
		ok = BlazingContext::getInstance()->getFileSystemManager()->registerFileSystem(fileSystemEntity);
	} catch(const std::exception & e) {
		return std::make_pair(false, std::string(e.what()));
	} catch(...) {
		std::string msg =
			std::string("Unknown error for filesystem " + authority + " :" + fileSystemConnection.toString());
		return std::make_pair(false, msg);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> registerFileSystemHDFS(HDFS hdfs, std::string root, std::string authority) {
	FileSystemConnection fileSystemConnection = FileSystemConnection(
		hdfs.host, hdfs.port, hdfs.user, (HadoopFileSystemConnection::DriverType) hdfs.DriverType, hdfs.kerberosTicket);
	return registerFileSystem(fileSystemConnection, root, authority);
}

std::pair<bool, std::string> registerFileSystemGCS(GCS gcs, std::string root, std::string authority) {
	FileSystemConnection fileSystemConnection =
		FileSystemConnection(gcs.projectId, gcs.bucketName, gcs.useDefaultAdcJsonFile, gcs.adcJsonFile);
	return registerFileSystem(fileSystemConnection, root, authority);
}
std::pair<bool, std::string> registerFileSystemS3(S3 s3, std::string root, std::string authority) {
	FileSystemConnection fileSystemConnection = FileSystemConnection(s3.bucketName,
		(S3FileSystemConnection::EncryptionType) s3.encryptionType,
		s3.kmsKeyAmazonResourceName,
		s3.accessKeyId,
		s3.secretKey,
		s3.sessionToken);
	return registerFileSystem(fileSystemConnection, root, authority);
}
std::pair<bool, std::string> registerFileSystemLocal(std::string root, std::string authority) {
	FileSystemConnection fileSystemConnection = FileSystemConnection(FileSystemType::LOCAL);
	return registerFileSystem(fileSystemConnection, root, authority);
}
