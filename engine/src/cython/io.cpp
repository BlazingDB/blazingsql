#include "../../include/io/io.h"
#include "../io/DataLoader.h"
#include "../io/data_parser/ArgsUtil.h"
#include "../io/data_parser/CSVParser.h"
#include "../io/data_parser/JSONParser.h"
#include "../io/data_parser/OrcParser.h"
#include "../io/data_parser/ParquetParser.h"
#include "../io/data_provider/UriDataProvider.h"

#include "utilities/CommonOperations.h"
#include "parser/expression_tree.hpp"

#include <blazingdb/io/Config/BlazingContext.h>

#ifdef MYSQL_SUPPORT
#include "../io/data_parser/sql/MySQLParser.h"
#include "../io/data_provider/sql/MySQLDataProvider.h"
#endif

#ifdef POSTGRESQL_SUPPORT
#include "../io/data_parser/sql/PostgreSQLParser.h"
#include "../io/data_provider/sql/PostgreSQLDataProvider.h"
#endif

#ifdef SQLITE_SUPPORT
#include "../io/data_parser/sql/SQLiteParser.h"
#include "../io/data_provider/sql/SQLiteDataProvider.h"
#endif

using namespace fmt::literals;

// #include <blazingdb/io/Library/Logging/TcpOutput.h>
// #include "blazingdb/io/Library/Network/NormalSyncSocket.h"

#include <numeric>


TableSchema parseSchema(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, cudf::type_id>> extra_columns,
	bool ignore_missing_paths) {

	// sanitize and normalize paths
	for (size_t i = 0; i < files.size(); ++i) {
		files[i] = Uri(files[i]).toString(true);
	}

	const DataType data_type_hint = ral::io::inferDataType(file_format_hint);
	const DataType fileType = inferFileType(files, data_type_hint, ignore_missing_paths);
	auto args_map = ral::io::to_map(arg_keys, arg_values);
	TableSchema tableSchema;
	tableSchema.data_type = fileType;

	std::vector<Uri> uris;
	for(auto file_path : files) {
		uris.push_back(Uri{file_path});
	}

	std::shared_ptr<ral::io::data_parser> parser;
  std::shared_ptr<ral::io::data_provider> provider = nullptr;

  bool isSqlProvider = false;

	if(fileType == ral::io::DataType::PARQUET) {
		parser = std::make_shared<ral::io::parquet_parser>();
	} else if(fileType == ral::io::DataType::ORC) {
		parser = std::make_shared<ral::io::orc_parser>(args_map);
	} else if(fileType == ral::io::DataType::JSON) {
		parser = std::make_shared<ral::io::json_parser>(args_map);
	} else if(fileType == ral::io::DataType::CSV) {
		parser = std::make_shared<ral::io::csv_parser>(args_map);
	} else if(fileType == ral::io::DataType::MYSQL) {
#ifdef MYSQL_SUPPORT
		parser = std::make_shared<ral::io::mysql_parser>();
    auto sql = ral::io::getSqlInfo(args_map);
    provider = std::make_shared<ral::io::mysql_data_provider>(sql, 0, 0);
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support MySQL integration");
#endif
    isSqlProvider = true;
  } else if(fileType == ral::io::DataType::POSTGRESQL) {
#ifdef POSTGRESQL_SUPPORT
		parser = std::make_shared<ral::io::postgresql_parser>();
    auto sql = ral::io::getSqlInfo(args_map);
    provider = std::make_shared<ral::io::postgresql_data_provider>(sql, 0, 0);
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support PostgreSQL integration");
#endif
    isSqlProvider = true;
  } else if(fileType == ral::io::DataType::SQLITE) {
#ifdef SQLITE_SUPPORT
    parser = std::make_shared<ral::io::sqlite_parser>();
    auto sql = ral::io::getSqlInfo(args_map);
    provider = std::make_shared<ral::io::sqlite_data_provider>(sql, 0, 0);
    isSqlProvider = true;
#else
      throw std::runtime_error("ERROR: This BlazingSQL version doesn't support SQLite integration");
#endif
  }

  if (!isSqlProvider) {
      provider = std::make_shared<ral::io::uri_data_provider>(uris, ignore_missing_paths);
  }

	auto loader = std::make_shared<ral::io::data_loader>(parser, provider);

	ral::io::Schema schema;

	try {
		bool got_schema = false;
    if (isSqlProvider) {
        ral::io::data_handle handle = provider->get_next(false);
        parser->parse_schema(handle, schema);
        if (schema.get_num_columns() > 0){
          got_schema = true;
        }
    } else {
      while (!got_schema && provider->has_next()){
        ral::io::data_handle handle = provider->get_next();
        if (handle.file_handle != nullptr){
          parser->parse_schema(handle, schema);
          if (schema.get_num_columns() > 0){
            got_schema = true;
            schema.add_file(handle.uri.toString(true));
          }
        }
      }
		}
		if (!got_schema){
			std::cout<<"ERROR: Could not get schema"<<std::endl;
		}

		bool open_file = false;
    if (!isSqlProvider) {
      while (provider->has_next()){
        std::vector<ral::io::data_handle> handles = provider->get_some(64, open_file);
        for(auto handle : handles) {
          schema.add_file(handle.uri.toString(true));
        }
      }
    }

		for(auto extra_column : extra_columns) {
			schema.add_column(extra_column.first, extra_column.second, 0, false);
		}
		provider->reset();

	} catch(std::exception & e) {
		std::shared_ptr<spdlog::logger> logger = spdlog::get("batch_logger");
		if(logger){
            logger->error("|||{info}|||||",
                                        "info"_a="In parseSchema. What: {}"_format(e.what()));
            logger->flush();
		}
        std::cerr << "**[performPartition]** error partitioning table.\n";

		throw;
	}

	tableSchema.types = schema.get_dtypes();
	tableSchema.names = schema.get_names();
	tableSchema.files = schema.get_files();
	tableSchema.calcite_to_file_indices = schema.get_calcite_to_file_indices();
	tableSchema.in_file = schema.get_in_file();
	tableSchema.has_header_csv = schema.get_has_header_csv();

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
		auto table = ral::utilities::create_empty_table(dtypes);
		result->cudfTable = std::move(table);
		result->skipdata_analysis_fail = false;
		return result;
	}
	const DataType data_type_hint = ral::io::inferDataType(file_format_hint);
	const DataType fileType = inferFileType(files, data_type_hint);
	std::map<std::string, std::string> args_map = ral::io::to_map(arg_keys, arg_values);

	std::shared_ptr<ral::io::data_parser> parser;
	if(fileType == ral::io::DataType::PARQUET) {
		parser = std::make_shared<ral::io::parquet_parser>();
	} else if(fileType == ral::io::DataType::ORC) {
		parser = std::make_shared<ral::io::orc_parser>(args_map);
	} else if(fileType == ral::io::DataType::JSON) {
		parser = std::make_shared<ral::io::json_parser>(args_map);
	} else if(fileType == ral::io::DataType::CSV) {
		parser = std::make_shared<ral::io::csv_parser>(args_map);
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
	} catch(std::exception & e) {
		std::cerr << e.what() << std::endl;
		throw;
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
		if (ok == false) {
			return std::make_pair(false, "Filesystem failed to register");
		}
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
		s3.sessionToken,
		s3.endpointOverride,
		s3.region);
	return registerFileSystem(fileSystemConnection, root, authority);
}

std::pair<bool, std::string> registerFileSystemLocal(std::string root, std::string authority) {
	FileSystemConnection fileSystemConnection = FileSystemConnection(FileSystemType::LOCAL);
	return registerFileSystem(fileSystemConnection, root, authority);
}

void visitPartitionFolder(Uri folder_uri, std::vector<FolderPartitionMetadata>& metadata, int depth) {
	auto fs = BlazingContext::getInstance()->getFileSystemManager();

	auto matches = fs->list(folder_uri, "*=*");
	for (auto &&uri : matches) {
		auto status = fs->getFileStatus(uri);
		if (!status.isDirectory()) {
			continue;
		}

		std::string name = uri.getPath().getResourceName();
		auto parts = StringUtil::split(name, '=');

		if (metadata.size() < static_cast<size_t>(depth) + 1) {
			metadata.resize(depth + 1);
		}

		metadata[depth].name = parts[0];
		metadata[depth].values.insert(parts[1]);

		visitPartitionFolder(uri, metadata, depth + 1);
	}
}

std::vector<FolderPartitionMetadata> inferFolderPartitionMetadata(std::string folder_path) {
	Uri folder_uri{folder_path};

	auto fs = BlazingContext::getInstance()->getFileSystemManager();
	if (!fs->exists(folder_uri)) {
		return {};
	}

	auto status = fs->getFileStatus(folder_uri);
	if (!status.isDirectory()) {
		return {};
	}

	std::vector<FolderPartitionMetadata> metadata;
	visitPartitionFolder(folder_uri, metadata, 0);

	static std::regex boolean_regex{std::string(ral::parser::detail::lexer::BOOLEAN_REGEX_STR)};
  	static std::regex number_regex{std::string(ral::parser::detail::lexer::NUMBER_REGEX_STR)};
  	static std::regex timestamp_regex{std::string(ral::parser::detail::lexer::TIMESTAMP_D_REGEX_STR)};

	for (auto &&m : metadata) {
		m.data_type = cudf::type_id::EMPTY;
		for (auto &&value : m.values) {
			ral::parser::detail::lexer::token token;
		 	if (std::regex_match(value, boolean_regex)) {
				token = {ral::parser::detail::lexer::token_type::Boolean, value};
			} else if (std::regex_match(value, timestamp_regex)) {
				token = {ral::parser::detail::lexer::token_type::Timestamp_d, value};
			} else if (std::regex_match(value, number_regex)) {
				token = {ral::parser::detail::lexer::token_type::Number, value};
			} else {
				token = {ral::parser::detail::lexer::token_type::String, value};
			}

			cudf::data_type inferred_type = ral::parser::detail::infer_type_from_literal_token(token);
			if (m.data_type == cudf::type_id::EMPTY)		{
				m.data_type = inferred_type.id();
			} else {
				m.data_type = ral::utilities::get_common_type(cudf::data_type{m.data_type}, inferred_type, false).id();
			}
		}
	}

	return metadata;
}

std::pair<TableSchema, error_code_t> parseSchema_C(std::vector<std::string> files,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values,
	std::vector<std::pair<std::string, cudf::type_id>> extra_columns,
	bool ignore_missing_paths) {

	TableSchema result;

	try {
		result = parseSchema(files,
					file_format_hint,
					arg_keys,
					arg_values,
					extra_columns,
					ignore_missing_paths);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}

std::pair<std::unique_ptr<ResultSet>, error_code_t> parseMetadata_C(std::vector<std::string> files,
	std::pair<int, int> offset,
	TableSchema schema,
	std::string file_format_hint,
	std::vector<std::string> arg_keys,
	std::vector<std::string> arg_values) {

	std::unique_ptr<ResultSet> result = nullptr;

	try {
		result = std::move(parseMetadata(files,
					offset,
					schema,
					file_format_hint,
					arg_keys,
					arg_values));
		return std::make_pair(std::move(result), E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(std::move(result), E_EXCEPTION);
	}
}

std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemHDFS_C(HDFS hdfs, std::string root, std::string authority) {
	std::pair<bool, std::string> result;

	try {
		result = registerFileSystemHDFS(hdfs, root, authority);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}

std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemGCS_C(GCS gcs, std::string root, std::string authority) {
	std::pair<bool, std::string> result;

	try {
		result = registerFileSystemGCS(gcs, root, authority);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}

std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemS3_C(S3 s3, std::string root, std::string authority) {
	std::pair<bool, std::string> result;

	try {
		result = registerFileSystemS3(s3, root, authority);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}

std::pair<std::pair<bool, std::string>, error_code_t> registerFileSystemLocal_C(std::string root, std::string authority) {
	std::pair<bool, std::string> result;

	try {
		result = registerFileSystemLocal(root, authority);
		return std::make_pair(result, E_SUCCESS);
	} catch (std::exception& e) {
		return std::make_pair(result, E_EXCEPTION);
	}
}
