#include "../../include/io/io.h"
#include "../io/DataLoader.h"
#include "../io/Schema.h"
#include "../io/Metadata.h"
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

	tableSchema.types = schema.get_dtypes();
	tableSchema.names = schema.get_names();
	tableSchema.files = schema.get_files();
	tableSchema.num_row_groups = schema.get_num_row_groups();
	tableSchema.calcite_to_file_indices = schema.get_calcite_to_file_indices();
	tableSchema.in_file = schema.get_in_file();

	return tableSchema;
}


TableSchema parseMetadata(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, gdf_dtype>> extra_columns) {
	if (offset.second == 0) {
		// cover case for empty files to parse
		// std::cout << "empty offset: " << std::endl;
		
		// TODO cudf0.12 port. Needs to be updated due to new dtypes
		// std::vector<size_t> column_indices(2 * schema.columns.size() + 2);
		// std::iota(column_indices.begin(), column_indices.end(), 0);

		// std::vector<std::string> names(2 * schema.columns.size() + 2);
		// std::vector<gdf_dtype> dtypes(2 * schema.columns.size() + 2);
		// std::vector<gdf_time_unit> time_units(2 * schema.columns.size() + 2);

		// size_t index = 0;
		// for(; index < schema.columns.size(); index++) {
		// 	auto col = schema.columns[index];
		// 	auto dtype = col->dtype;
		// 	if (dtype == GDF_CATEGORY || dtype == GDF_STRING || dtype == GDF_STRING_CATEGORY)
		// 		dtype = GDF_INT32;

		// 	dtypes[2*index] = dtype;
		// 	dtypes[2*index + 1] = dtype;
			
		// 	time_units[2*index] = col->dtype_info.time_unit;
		// 	time_units[2*index + 1] = col->dtype_info.time_unit;

		// 	auto col_name_min = "min_" + std::to_string(index) + "_" + schema.names[index];
		// 	auto col_name_max = "max_" + std::to_string(index)  + "_" + schema.names[index];

		// 	names[2*index] = col_name_min;
		// 	names[2*index + 1] = col_name_max;
		// }
		// dtypes[2*index] = GDF_INT32;
		// time_units[2*index] = TIME_UNIT_NONE;
		// names[2*index] = "file_handle_index";

		// dtypes[2*index + 1] = GDF_INT32;
		// time_units[2*index + 1] = TIME_UNIT_NONE;
		// names[2*index + 1] = "row_group_index";
				
		// auto columns_cpp = ral::io::create_empty_columns(names, dtypes, time_units, column_indices);
		TableSchema tableSchema;

		// for(auto column_cpp : columns_cpp) {
		// 	GDFRefCounter::getInstance()->deregister_column(column_cpp.get_gdf_column());
		// 	tableSchema.columns.push_back(column_cpp.get_gdf_column());
		// 	tableSchema.names.push_back(column_cpp.name());
		// }
		//TODO, @alex init tableShema with valid None Values
		return tableSchema;
	}
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

	ral::io::Metadata metadata({}, offset);

	try {
		// loader->get_schema(schema, extra_columns);

		 loader->get_metadata(metadata, extra_columns);

	} catch(std::exception & e) {
		std::cerr << "**[parseMetadata]** error parsing metadata.\n";
		return tableSchema;
	}

	auto gdf_columns = metadata.get_columns();
 
	for(auto column_cpp : gdf_columns) {
		// TODO percy cudf0.12 port to cudf::column
		// GDFRefCounter::getInstance()->deregister_column(column_cpp.get_gdf_column());
		// tableSchema.columns.push_back(column_cpp.get_gdf_column());
		// tableSchema.names.push_back(column_cpp.name());
	}
	//TODO: Alexander
	// tableSchema.files = schema.get_files();
	// tableSchema.num_row_groups = schema.get_num_row_groups();
	// tableSchema.calcite_to_file_indices = schema.get_calcite_to_file_indices();
	// tableSchema.in_file = schema.get_in_file();
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
