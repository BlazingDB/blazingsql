#include "../../include/io/io.h"
#include "../io/DataLoader.h"
#include "../io/Schema.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_parser/ParserUtil.h"
#include "../io/data_provider/UriDataProvider.h"

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
	std::vector<std::pair<std::string, gdf_dtype>> extra_columns) {
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

	auto columns_cpp =
		ral::io::create_empty_columns(schema.get_names(), schema.get_dtypes(), column_indices);

	for(auto column_cpp : columns_cpp) {
		GDFRefCounter::getInstance()->deregister_column(column_cpp.get_gdf_column());
		tableSchema.columns.push_back(column_cpp.get_gdf_column());
		tableSchema.names.push_back(column_cpp.name());
	}
	tableSchema.files = schema.get_files();
	tableSchema.num_row_groups = schema.get_num_row_groups();
	tableSchema.calcite_to_file_indices = schema.get_calcite_to_file_indices();
	tableSchema.in_file = schema.get_in_file();

	return tableSchema;
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
