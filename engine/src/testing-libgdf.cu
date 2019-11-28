/*
 ============================================================================
 Name        : testing-libgdf.cu
 Author      : felipe
 Version     :
 Copyright   : Your copyright notice
 Description : MVP
 ============================================================================
 */

#include <stdio.h>
#include <unistd.h>
#include <string.h> /* for strncpy */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>

#include <cuda_runtime.h>
#include <memory>
#include <algorithm>
#include <thread>
#include <chrono>
#include "CalciteInterpreter.h"
#include "ResultSetRepository.h"
#include "DataFrame.h"
#include "Utils.cuh"
#include "Types.h"
#include <cuda_runtime.h>


#include "gdf_wrapper/gdf_wrapper.cuh"

#include <tuple>

#include <blazingdb/protocol/api.h>
#include <blazingdb/protocol/message/messages.h>
#include <blazingdb/protocol/message/orchestrator/messages.h>
#include <blazingdb/protocol/message/interpreter/messages.h>
#include <blazingdb/protocol/message/io/file_system.h>
#include "ral-message.cuh"


using namespace blazingdb::protocol;

#include <blazingdb/io/Util/StringUtil.h>

#include <blazingdb/io/FileSystem/HadoopFileSystem.h>
#include <blazingdb/io/FileSystem/S3FileSystem.h>
#include <blazingdb/io/FileSystem/FileSystemRepository.h>
#include <blazingdb/io/FileSystem/FileSystemCommandParser.h>
#include <blazingdb/io/FileSystem/FileSystemManager.h>
#include <blazingdb/io/Config/BlazingContext.h>
#include <blazingdb/io/Library/Logging/Logger.h>
#include <blazingdb/io/Library/Logging/FileOutput.h>
// #include <blazingdb/io/Library/Logging/TcpOutput.h>
#include "blazingdb/io/Library/Logging/ServiceLogging.h"
// #include "blazingdb/io/Library/Network/NormalSyncSocket.h"

#include "CalciteExpressionParsing.h"
#include "io/data_parser/CSVParser.h"
#include "io/data_parser/GDFParser.h"
#include "io/data_parser/ParquetParser.h"
#include "io/data_parser/OrcParser.h"
#include "io/data_parser/JSONParser.h"
#include "io/data_provider/DummyProvider.h"
#include "io/data_provider/UriDataProvider.h"

#include "io/data_parser/DataParser.h"
#include "io/data_provider/DataProvider.h"
#include "io/DataLoader.h"
#include "Traits/RuntimeTraits.h"


#include "CodeTimer.h"
#include "config/BlazingConfig.h"
#include "config/GPUManager.cuh"

#include "communication/CommunicationData.h"
#include "communication/factory/MessageFactory.h"
#include "communication/network/Client.h"
#include "communication/network/Server.h"
#include <blazingdb/manager/Context.h>
#include <blazingdb/manager/NodeDataMessage.h>
#include <blazingdb/transport/io/reader_writer.h>

const Path FS_NAMESPACES_FILE("/tmp/file_system.bin");
using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;
using FunctionType = result_pair (*)(uint64_t, Buffer&& buffer);

ConnectionAddress connectionAddress;

static result_pair  registerFileSystem(uint64_t accessToken, Buffer&& buffer) {
  std::cout << "registerFileSystem: " << accessToken << std::endl;
  blazingdb::message::io::FileSystemRegisterRequestMessage message(buffer.data());

  FileSystemConnection fileSystemConnection;
  Path root("/");
  const std::string authority =  message.getAuthority();
  if (message.isLocal()) {
    fileSystemConnection = FileSystemConnection(FileSystemType::LOCAL);
  } else if (message.isHdfs()) {
    auto hdfs = message.getHdfs();
    fileSystemConnection = FileSystemConnection(hdfs.host, hdfs.port, hdfs.user, (HadoopFileSystemConnection::DriverType)hdfs.driverType, hdfs.kerberosTicket);
  } else if (message.isS3()) {
    auto s3 = message.getS3();
    fileSystemConnection = FileSystemConnection(s3.bucketName, ( S3FileSystemConnection::EncryptionType )s3.encryptionType, s3.kmsKeyAmazonResourceName, s3.accessKeyId, s3.secretKey, s3.sessionToken);
  } else if (message.isGcs()) {
      auto gcs = message.getGcs();
      fileSystemConnection = FileSystemConnection(gcs.projectId, gcs.bucketName, gcs.useDefaultAdcJsonFile, gcs.adcJsonFile);
    }
  root = message.getRoot();
  if (root.isValid() == false) {
    std::cout << "something went wrong when registering filesystem ..." << std::endl;
    ResponseErrorMessage errorMessage{ std::string{ "ERROR: Invalid root provided when registering file system"} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  try {
    FileSystemEntity fileSystemEntity(authority, fileSystemConnection, root);
    bool ok = BlazingContext::getInstance()->getFileSystemManager()->deregisterFileSystem(authority);
    ok = BlazingContext::getInstance()->getFileSystemManager()->registerFileSystem(fileSystemEntity);
    if (ok) { // then save the fs
      const FileSystemRepository fileSystemRepository(FS_NAMESPACES_FILE, true);
      const bool saved = fileSystemRepository.add(fileSystemEntity);
      if (saved == false) {
        std::cerr << "WARNING: could not save the registered file system into ... the data file uri ..."; //TODO percy error message
      }
    } else {
        std::cerr << "something went wrong when registering filesystem ..." << std::endl;
        ResponseErrorMessage errorMessage{ std::string{"ERROR: Something went wrong when registering file system"} };
        return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
  } catch(const std::exception& e) {
    ResponseErrorMessage errorMessage{ std::string{e.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }  catch (...) {
    ResponseErrorMessage errorMessage{ std::string{"Unknown error about filesystem. Probably wrong port."} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair  deregisterFileSystem(uint64_t accessToken, Buffer&& buffer)  {
  std::cout << "deregisterFileSystem: " << accessToken << std::endl;
  blazingdb::message::io::FileSystemDeregisterRequestMessage message(buffer.data());
  auto authority =  message.getAuthority();
  if (authority.empty() == true) {
     ResponseErrorMessage errorMessage{ std::string{"derigistering an empty authority"} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  const bool ok = BlazingContext::getInstance()->getFileSystemManager()->deregisterFileSystem(authority);
  if (ok) { // then save the fs
    const FileSystemRepository fileSystemRepository(FS_NAMESPACES_FILE, true);
    const bool deleted = fileSystemRepository.deleteByAuthority(authority);
    if (deleted == false) {
      std::cout << "WARNING: could not delete the registered file system into ... the data file uri ..."; //TODO percy error message
    }
  }
  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair scanDataSource(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
    using namespace blazingdb::protocol::orchestrator;

    const DataSourceRequestMessage data_source_request(requestPayloadBuffer.data());
    const Uri directory(data_source_request.getDirectory());

    if (false == BlazingContext::getInstance()->getFileSystemManager()->exists(directory)) {
        const std::string error = "Create table error: Data source directory " + directory.toString() + " does not exists";
        std::cout << error << std::endl;
        ResponseErrorMessage errorMessage(error);
        return std::make_pair(Status_Error, errorMessage.getBufferData());
    }

    const std::string wildcard(data_source_request.getWildcard());
    const bool hasWildcard = !wildcard.empty();

    std::vector<Uri> fsFiles;

    if (hasWildcard) {
        fsFiles = BlazingContext::getInstance()->getFileSystemManager()->list(directory, wildcard);
    } else {
        fsFiles = BlazingContext::getInstance()->getFileSystemManager()->list(directory);
    }

    if (fsFiles.empty()) {
        const std::string error = "Create table error: Data source directory " + directory.toString() + " is empty";
        std::cout << error << std::endl;
        ResponseErrorMessage errorMessage(error);
        return std::make_pair(Status_Error, errorMessage.getBufferData());
    }

    std::vector<std::string> files;

    for (int i = 0; i < fsFiles.size(); ++i) {
        const Uri file = fsFiles.at(i);
        files.push_back(file.toString());
    }

    DataSourceResponseMessage payload(files);
    return std::make_pair(Status_Success, payload.getBufferData());
}

static result_pair systemCommand(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
    using namespace blazingdb::protocol::orchestrator;

    const SystemCommandRequestMessage system_command_request(requestPayloadBuffer.data());

    if (system_command_request.getCommand() == "ping") {
      SystemCommandResponseMessage payload("ping");
      return std::make_pair(Status_Success, payload.getBufferData());
    } else if (system_command_request.getCommand().find("shutdown") != std::string::npos) {
      std::cout<<"Shutting down"<<std::endl;

      std::thread([]() {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          ral::communication::network::Client::closeConnections();
          ral::communication::network::Server::getInstance().close();      
          cudaDeviceReset();
          exit(0);
      }).detach();
      
      SystemCommandResponseMessage payload("success");
      return std::make_pair(Status_Shutdown, payload.getBufferData());
    } else {
      ResponseErrorMessage errorMessage{ "Unknown Command" };
      return std::make_pair(Status_Error, errorMessage.getBufferData());
    }
}

using result_pair = std::pair<Status, std::shared_ptr<flatbuffers::DetachedBuffer>>;
using FunctionType = result_pair (*)(uint64_t, Buffer&& buffer);

static result_pair closeConnectionService(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
  std::cout << "accessToken: " << accessToken << std::endl;

  try {
    result_set_repository::get_instance().remove_all_connection_tokens(accessToken);
    // NOTE: use next 3 lines to check with "/usr/local/cuda/bin/cuda-memcheck  --leak-check full  ./testing-libgdf"
    // GDFRefCounter::getInstance()->show_summary();
    // cudaDeviceReset();
    // exit(0);
  } catch (const std::exception& e) {
     std::cerr << e.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{e.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }

  ZeroMessage response{};
  return std::make_pair(Status_Success, response.getBufferData());
}

static result_pair getResultService(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
  std::cout << "accessToken: " << accessToken << std::endl;

  interpreter::GetResultRequestMessage request(requestPayloadBuffer.data());
  std::cout << "resultToken: " << request.getResultToken() << std::endl;

  try {
    // get result from repository using accessToken and resultToken
    result_set_t result = result_set_repository::get_instance().get_result(accessToken, request.getResultToken());

    std::string status = "Error";
    std::string errorMsg = result.errorMsg;
    std::vector<std::string> fieldNames;
    std::vector<uint64_t> columnTokens;
    std::vector<::gdf_dto::gdf_column> values;
    int rows = 0;

    if (errorMsg.empty()) {
      status = "OK";
      //TODO ojo el result siempre es una sola tabla por eso indice 0
      rows =  result.result_frame.get_num_rows_in_table(0);

      for(std::size_t i = 0; i < result.result_frame.get_columns()[0].size(); ++i) {
        fieldNames.push_back(result.result_frame.get_columns()[0][i].name());
        columnTokens.push_back(result.result_frame.get_columns()[0][i].get_column_token());

        std::cout << "col_name: " << result.result_frame.get_columns()[0][i].name() << std::endl;
        nvstrings_ipc_transfer ipc;
        gdf_dto::gdf_dtype_extra_info dtype_info;
        ::gdf_dto::gdf_column col;

        std::basic_string<int8_t> data;
        std::basic_string<int8_t> valid;

        if(result.result_frame.get_columns()[0][i].dtype() == GDF_STRING){
          NVStrings* strings = static_cast<NVStrings *> (result.result_frame.get_columns()[0][i].get_gdf_column()->data);
          if(result.result_frame.get_columns()[0][i].size() > 0)
            strings->create_ipc_transfer(ipc);
          dtype_info = gdf_dto::gdf_dtype_extra_info {
                .time_unit = (gdf_dto::gdf_time_unit)0,
            };

          col.data = data;
          col.valid = valid;
          col.size = result.result_frame.get_columns()[0][i].size();
          col.dtype =  (gdf_dto::gdf_dtype)result.result_frame.get_columns()[0][i].dtype();
          col.dtype_info = dtype_info;
          col.null_count = static_cast<gdf_size_type>(result.result_frame.get_columns()[0][i].null_count()),
          // custrings data
          col.custrings_data = libgdf::ConvertIpcByteArray(ipc);

        }else{
          dtype_info = gdf_dto::gdf_dtype_extra_info {
                .time_unit = (gdf_dto::gdf_time_unit)result.result_frame.get_columns()[0][i].dtype_info().time_unit
          };

          if(result.result_frame.get_columns()[0][i].size() > 0) {
            data = libgdf::BuildCudaIpcMemHandler(result.result_frame.get_columns()[0][i].get_gdf_column()->data);
            valid = libgdf::BuildCudaIpcMemHandler(result.result_frame.get_columns()[0][i].get_gdf_column()->valid);
          }

          col.data = data;
          col.valid = valid;
          col.size = result.result_frame.get_columns()[0][i].size();
          col.dtype =  (gdf_dto::gdf_dtype)result.result_frame.get_columns()[0][i].dtype();
          col.null_count = result.result_frame.get_columns()[0][i].null_count();
          col.dtype_info = dtype_info;
        }

        values.push_back(col);
      }
    }

    interpreter::BlazingMetadataDTO  metadata = {
      .status = status,
      .message = errorMsg,
      .time = result.duration,
      .rows = rows
    };

    interpreter::GetResultResponseMessage responsePayload(metadata, fieldNames, columnTokens, values);
    return std::make_pair(Status_Success, responsePayload.getBufferData());

  } catch (const std::exception& e) {
     std::cerr << e.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{e.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  } catch (...) {
    ResponseErrorMessage errorMessage{ std::string{"Unknown error"} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
}

static result_pair freeResultService(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
   std::cout << "freeResultService: " << accessToken << std::endl;

  interpreter::GetResultRequestMessage request(requestPayloadBuffer.data());
  std::cout << "resultToken: " << request.getResultToken() << std::endl;
  bool success = false;
  try {
    success = result_set_repository::get_instance().try_free_result(accessToken, request.getResultToken());
  } catch (const std::runtime_error& e) {
    std::cerr << e.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{e.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }
  if(success){
	  ZeroMessage response{};
	  return std::make_pair(Status_Success, response.getBufferData());
  }else{
	  ResponseErrorMessage errorMessage{ std::string{"Could not free result set!"} };
	  return std::make_pair(Status_Error, errorMessage.getBufferData());
  }

}

static result_pair parseSchemaService(uint64_t accessToken, Buffer&& requestPayloadBuffer) {
	blazingdb::protocol::orchestrator::DDLCreateTableRequestMessage requestPayload(requestPayloadBuffer.data());

	std::shared_ptr<ral::io::data_parser> parser;
    if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_PARQUET){
		parser = std::make_shared<ral::io::parquet_parser>();
    }else if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_CSV){
		std::vector<gdf_dtype> types;
		for(auto val : requestPayload.columnTypes){
			types.push_back(ral::traits::convert_string_dtype(val));
    }

		parser =  std::make_shared<ral::io::csv_parser>(
				requestPayload.csvDelimiter,
  				requestPayload.csvLineTerminator,
          (int) requestPayload.csvSkipRows,
          requestPayload.csvHeader,
			    requestPayload.csvNrows,
			    requestPayload.csvSkipinitialspace,
			    requestPayload.csvDelimWhitespace,
          requestPayload.csvSkipBlankLines,
          requestPayload.csvQuotechar,
          requestPayload.csvQuoting,
          requestPayload.csvDoublequote,
          requestPayload.csvDecimal,
          requestPayload.csvSkipfooter,
          requestPayload.csvNaFilter,
          requestPayload.csvKeepDefaultNa,
          requestPayload.csvDayfirst,
          requestPayload.csvThousands,
          requestPayload.csvComment,
          requestPayload.csvTrueValues,
          requestPayload.csvFalseValues,
          requestPayload.csvNaValues,
  				requestPayload.columnNames, types);
  }else if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_ORC){
		parser = std::make_shared<ral::io::orc_parser>(
        requestPayload.orcStripe,
        requestPayload.orcSkipRows,
        requestPayload.orcNumRows,
        requestPayload.orcUseIndex);
  }else if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_JSON){
    parser = std::make_shared<ral::io::json_parser>(requestPayload.jsonLines);
  }else{
		//indicate error here
		//this shoudl be done in the orchestrator
	}

  std::vector<Uri> uris;
  for (auto file_path : requestPayload.files) {
      uris.push_back(Uri{file_path});
  }
  auto provider = std::make_shared<ral::io::uri_data_provider>(uris);
  auto loader = std::make_shared<ral::io::data_loader>( parser,provider);
  ral::io::Schema schema;
  try {
    loader->get_schema(schema);
  } catch(std::exception & e) {
    ResponseErrorMessage errorMessage{ std::string{e.what()} };
    return std::make_pair(Status_Error, errorMessage.getBufferData());
  }

	blazingdb::protocol::TableSchemaSTL transport_schema = schema.getTransport();

    if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_CSV){
		transport_schema.csvDelimiter = requestPayload.csvDelimiter;
		transport_schema.csvSkipRows = requestPayload.csvSkipRows;
    transport_schema.csvLineTerminator = requestPayload.csvLineTerminator;
    transport_schema.csvHeader = requestPayload.csvHeader;
    transport_schema.csvNrows = requestPayload.csvNrows;
    transport_schema.csvSkipinitialspace = requestPayload.csvSkipinitialspace;
    transport_schema.csvDelimWhitespace = requestPayload.csvDelimWhitespace;
    transport_schema.csvSkipBlankLines = requestPayload.csvSkipBlankLines;
    transport_schema.csvQuotechar = requestPayload.csvQuotechar;
    transport_schema.csvQuoting = requestPayload.csvQuoting;
    transport_schema.csvDoublequote = requestPayload.csvDoublequote;
    transport_schema.csvDecimal = requestPayload.csvDecimal;
    transport_schema.csvSkipfooter = requestPayload.csvSkipfooter;
    transport_schema.csvNaFilter = requestPayload.csvNaFilter;
    transport_schema.csvKeepDefaultNa = requestPayload.csvKeepDefaultNa;
    transport_schema.csvDayfirst = requestPayload.csvDayfirst;
    transport_schema.csvThousands = requestPayload.csvThousands;
    transport_schema.csvComment = requestPayload.csvComment;
    transport_schema.csvTrueValues = requestPayload.csvTrueValues;
    transport_schema.csvFalseValues = requestPayload.csvFalseValues;
    transport_schema.csvNaValues = requestPayload.csvNaValues;
    }else if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_JSON){
		transport_schema.jsonLines = requestPayload.jsonLines;
  }else if(requestPayload.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_ORC){
		transport_schema.orcStripe = requestPayload.orcStripe;
		transport_schema.orcSkipRows = requestPayload.orcSkipRows;
		transport_schema.orcNumRows = requestPayload.orcNumRows;
		transport_schema.orcUseIndex = requestPayload.orcUseIndex;
	}
	transport_schema.files = requestPayload.files;

	blazingdb::protocol::interpreter::CreateTableResponseMessage responsePayload(transport_schema);
	return std::make_pair(Status_Success, responsePayload.getBufferData());
}

static result_pair executeFileSystemPlanService (uint64_t accessToken, Buffer&& requestPayloadBuffer) {

  CodeTimer timer;
	timer.reset();
  blazingdb::message::io::FileSystemDMLRequestMessage requestPayload(requestPayloadBuffer.data());

  //make dataloaders
	std::vector<ral::io::data_loader > input_loaders;
	std::vector<ral::io::Schema> schemas;
	std::vector<std::string> table_names;
  for(auto table : requestPayload.tableGroup().tables){
	  ral::io::Schema schema(table.tableSchema);
	  std::shared_ptr<ral::io::data_parser> parser;
      if(table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_PARQUET){
	  		parser = std::make_shared<ral::io::parquet_parser>();

    }else if(table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_CSV){
      std::vector<gdf_dtype> types;
			for(auto val : table.tableSchema.types){
				types.push_back((gdf_dtype) val);
			}

      parser =  std::make_shared<ral::io::csv_parser>(
	  				table.tableSchema.csvDelimiter,
	  				table.tableSchema.csvLineTerminator,
            table.tableSchema.csvSkipRows,
            table.tableSchema.csvHeader,
			      table.tableSchema.csvNrows,
			      table.tableSchema.csvSkipinitialspace,
			      table.tableSchema.csvDelimWhitespace,
            table.tableSchema.csvSkipBlankLines,
            table.tableSchema.csvQuotechar,
            table.tableSchema.csvQuoting,
            table.tableSchema.csvDoublequote,
            table.tableSchema.csvDecimal,
            table.tableSchema.csvSkipfooter,
            table.tableSchema.csvNaFilter,
            table.tableSchema.csvKeepDefaultNa,
            table.tableSchema.csvDayfirst,
            table.tableSchema.csvThousands,
            table.tableSchema.csvComment,
            table.tableSchema.csvTrueValues,
            table.tableSchema.csvFalseValues,
            table.tableSchema.csvNaValues,
	  				table.tableSchema.names, types);
      }else if(table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_JSON){
        parser = std::make_shared<ral::io::json_parser>(table.tableSchema.jsonLines);
      }else if(table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_ORC){
        parser = std::make_shared<ral::io::orc_parser>(
          table.tableSchema.orcStripe,
          table.tableSchema.orcSkipRows,
          table.tableSchema.orcNumRows,
          table.tableSchema.orcUseIndex);
      }else{
        parser = std::make_shared<ral::io::gdf_parser>(table,accessToken);
      }

    std::shared_ptr<ral::io::data_provider> provider;
    std::vector<Uri> uris;
    for (auto file_path : table.tableSchema.files) {
        uris.push_back(Uri{file_path});
    }

    if(table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_CSV ||
        table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_PARQUET ||
        table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_JSON ||
        table.schemaType == blazingdb::protocol::io::FileSchemaType::FileSchemaType_ORC){
        provider = std::make_shared<ral::io::uri_data_provider>(uris);
    }else{
      provider = std::make_shared<ral::io::dummy_data_provider>();
    }
    ral::io::data_loader loader( parser,provider);
    input_loaders.push_back(loader);
    schemas.push_back(schema);
    table_names.push_back(table.name);

  }


  std::cout << "accessToken: " << accessToken << std::endl;
  std::cout << "query: " << requestPayload.statement() << std::endl;
  std::cout << "tableGroup: " << requestPayload.tableGroup().name << std::endl;
 	std::cout << "num tables: " << requestPayload.tableGroup().tables.size() << std::endl;
  std::cout << "contextToken: " << requestPayload.communicationContext().token << std::endl;
  std::cout << "contextTotalNodes: " << requestPayload.communicationContext().nodes.size() << std::endl;

  uint64_t resultToken = 0L;
  try {
    using blazingdb::manager::Context;
    using blazingdb::transport::Node;
    auto& rawCommContext = requestPayload.communicationContext();
    std::vector<std::shared_ptr<Node>> contextNodes;
    for(auto& rawNode: rawCommContext.nodes){
      auto& rawBuffer = rawNode.buffer;
      using MetaData = blazingdb::transport::Address::MetaData;
      MetaData metadata;
      memcpy(&metadata, rawBuffer.data(), sizeof(metadata));
      std::cout << "communication_context: " << metadata.ip << "|" << metadata.comunication_port << std::endl;
      auto address = blazingdb::transport::Address::TCP(metadata.ip, metadata.comunication_port, metadata.protocol_port);
      contextNodes.push_back(Node::Make(address));
    }
    uint32_t ctxToken = static_cast<uint32_t>(rawCommContext.token);
    Context queryContext{ctxToken, contextNodes, contextNodes[rawCommContext.masterIndex], ""};
    Library::Logging::ServiceLogging::getInstance().setNodeIdentifier(queryContext.getNodeIndex(ral::communication::CommunicationData::getInstance().getSelfNode())); // TODO we only need to do this once. Lets change this once we ral synching system
    Library::Logging::Logger().logInfo(timer.logDuration(queryContext, "\"Query Start\n" + requestPayload.statement() + "\""));
    
    ral::communication::network::Server::getInstance().registerContext(ctxToken);
    resultToken = requestPayload.resultToken();
    result_set_repository::get_instance().register_query(accessToken,resultToken);

    std::cout<<"Query Start: queryContext Nodes: "<<std::endl;
    std::cout<<"queryContext master node: "<<std::endl;
    queryContext.getMasterNode().print();
    for (auto node : queryContext.getAllNodes()) {
      node.get()->print();
    }

    // Execute query

    evaluate_query(input_loaders, schemas, table_names, requestPayload.statement(), accessToken, queryContext, resultToken );

  } catch (const std::exception& e) {
     std::cerr << e.what() << std::endl;
     ResponseErrorMessage errorMessage{ std::string{e.what()} };
     return std::make_pair(Status_Error, errorMessage.getBufferData());
  }

  #ifdef USE_UNIX_SOCKETS

  interpreter::NodeConnectionDTO nodeInfo {
      .port = -1,
      .path = ral::config::BlazingConfig::getInstance().getSocketPath(),
      .type = NodeConnectionType {NodeConnectionType_TCP}
  };

  #else

  interpreter::NodeConnectionDTO nodeInfo {
      .port = connectionAddress.tcp_port,
      //.path = ral::config::BlazingConfig::getInstance().getSocketPath(),
      //TODO percy felipe experimento con dask worker: si funciona mejora esto
      .path = "127.0.0.1",
      .type = NodeConnectionType {NodeConnectionType_TCP}
  };

  #endif

  interpreter::ExecutePlanResponseMessage responsePayload{resultToken, nodeInfo};
  return std::make_pair(Status_Success, responsePayload.getBufferData());
}


static result_pair addToResultRepo(std::uint64_t accessToken,
                                   Buffer &&     requestPayloadBuffer) {
    blazingdb::protocol::interpreter::RegisterDaskSliceRequestMessage
        requestPayload(requestPayloadBuffer.data());

    blazingdb::protocol::BlazingTableSchema blazingTableSchema =
        requestPayload.blazingTableSchema();
    blazingdb::message::io::FileSystemBlazingTableSchema
                                        fileSystemBlazingTableSchema;
    blazingdb::protocol::TableSchemaSTL tableSchemaSTL;

    fileSystemBlazingTableSchema.gdf         = blazingTableSchema;
    fileSystemBlazingTableSchema.tableSchema = tableSchemaSTL;

    auto parser = std::make_shared<ral::io::gdf_parser>(
        fileSystemBlazingTableSchema, accessToken);

    auto provider = std::make_shared<ral::io::dummy_data_provider>();

    ral::io::data_loader loader(parser, provider);

    ral::io::Schema schema;
    loader.get_schema(schema);

    using blazingdb::manager::Context;
    using blazingdb::transport::Node;
    Context queryContext{accessToken, std::vector<std::shared_ptr<Node>>(), std::shared_ptr<Node>(), ""};
    std::vector<gdf_column_cpp> columns;
    loader.load_data(queryContext, columns, {}, schema);

    blazing_frame frame;
    frame.add_table(columns);

    result_set_repository::get_instance().register_query(
        accessToken, requestPayload.resultToken());
    result_set_repository::get_instance().update_token(
        requestPayload.resultToken(), frame.clone(), 0, "");

    ZeroMessage response{};
    return std::make_pair(Status_Success, response.getBufferData());
}


static  std::map<int8_t, FunctionType> services;


//@todo execuplan with filesystem
auto  interpreterServices(const blazingdb::protocol::Buffer &requestPayloadBuffer) -> std::pair<blazingdb::protocol::Buffer,bool> {
  RequestMessage request{requestPayloadBuffer.data()};
  std::cout << "header: " << (int)request.messageType() << std::endl;

  auto result = services[request.messageType()] ( request.accessToken(),  request.getPayloadBuffer() );
  if (result.first == Status_Shutdown){
    ResponseMessage responseObject{Status_Success, result.second};
    return std::make_pair(Buffer{responseObject.getBufferData()}, false);
  } else {
    ResponseMessage responseObject{result.first, result.second};
    return std::make_pair(Buffer{responseObject.getBufferData()}, true);
  }  
}


std::string get_ip(const std::string &iface_name = "eth0") {
    int fd;
    struct ifreq ifr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);

    /* I want to get an IPv4 IP address */
    ifr.ifr_addr.sa_family = AF_INET;

    /* I want IP address attached to "eth0" */
    strncpy(ifr.ifr_name, iface_name.c_str(), IFNAMSIZ-1);

    ioctl(fd, SIOCGIFADDR, &ifr);

    close(fd);

    /* display result */
    //printf("%s\n", inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));
    const std::string the_ip(inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr));

    return the_ip;
}

int main(int argc, const char *argv[])
{
    std::cout << "Usage: " << argv[0]
              << " <RAL_ID> <GPU_ID>"
                 " <ORCHESTRATOR_HTTP_COMMUNICATION_[IP|HOSTNAME]> <ORCHESTRATOR_HTTP_COMMUNICATION_PORT>"
                 " <RAL_HTTP_COMMUNICATION_[IP|HOSTNAME]> <RAL_HTTP_COMMUNICATION_PORT> <RAL_TCP_PROTOCOL_PORT>"
                 " <OPTIONAL_NETWORK_INTERFACE_NAME_ETH0_DEFAULT> <OPTIONAL_LOGGING_[IP|HOSTNAME]> <OPTIONAL_LOGGING_PORT>" << std::endl;

    if (argc < 7) {
        std::cout << "FATAL: Invalid number of arguments" << std::endl;
        return EXIT_FAILURE;
    }
    rmmInitialize(nullptr);
  // #ifndef VERBOSE
  // std::cout.rdbuf(nullptr); // substitute internal std::cout buffer with
  // #endif // VERBOSE


    std::cout << "RAL Engine starting" << std::endl;

    const std::string ralId = std::string(argv[1]);
    const std::string gpuId = std::string(argv[2]);
    const std::string orchestratorHost = std::string(argv[3]);

    const int orchestratorCommunicationPort = ConnectionUtils::parsePort(argv[4]);

    if (orchestratorCommunicationPort == -1) {
        std::cout << "FATAL: Invalid Orchestrator HTTP communication port " + std::string(argv[4]) << std::endl;
        return EXIT_FAILURE;
    }

    std::string ralHost = std::string(argv[5]);

    const int ralCommunicationPort = ConnectionUtils::parsePort(argv[6]);

    if (ralCommunicationPort == -1) {
        std::cout << "FATAL: Invalid RAL HTTP communication port " + std::string(argv[6]) << std::endl;
        return EXIT_FAILURE;
    }

    const int ralProtocolPort = ConnectionUtils::parsePort(argv[7]);

    if (ralProtocolPort == -1) {
        std::cout << "FATAL: Invalid RAL TCP protocol port " + std::string(argv[7]) << std::endl;
        return EXIT_FAILURE;
    }

    //TODO percy make const and review this hacks
    std::string network_iface_name = "eth0";
    if (argc == 9) {
        network_iface_name = argv[8];
    }

    std::cout << "Using the network interface: " + network_iface_name << std::endl;
    ralHost = get_ip(network_iface_name);

    std::cout << "RAL ID: " << ralId << std::endl;
    std::cout << "GPU ID: " << gpuId << std::endl;
    std::cout << "Orchestrator HTTP communication host: " << orchestratorHost << std::endl;
    std::cout << "Orchestrator HTTP communication port: " << orchestratorCommunicationPort << std::endl;
    std::cout << "RAL HTTP communication host: " << ralHost << std::endl;
    std::cout << "RAL HTTP communication port: " << ralCommunicationPort << std::endl;
    std::cout << "RAL protocol port: " << ralProtocolPort << std::endl;

    // std::string loggingHost = "";
    // std::string loggingPort = 0;
    std::string loggingName = "";
    // if (argc == 11) {
    //   loggingHost = std::string(argv[9]);
    //   loggingPort = std::string(argv[10]);
    //   std::cout << "Logging host: " << ralHost << std::endl;
    //   std::cout << "Logging port: " << ralCommunicationPort << std::endl;
    // } else {
       loggingName = "RAL." + ralId + ".log";
      std::cout << "Logging to "<<loggingName << std::endl;
    // }

    ral::config::GPUManager::getInstance().initialize(std::stoi(gpuId));
    size_t total_gpu_mem_size = ral::config::GPUManager::getInstance().gpuMemorySize();
    assert(total_gpu_mem_size > 0);
    auto nthread = 4;
    blazingdb::transport::io::setPinnedBufferProvider(0.1 * total_gpu_mem_size, nthread);

    auto& communicationData = ral::communication::CommunicationData::getInstance();
    communicationData.initialize(
        std::atoi(ralId.c_str()),
        orchestratorHost,
        orchestratorCommunicationPort,
        ralHost,
        ralCommunicationPort,
        ralProtocolPort);

    int num_tries=0;
    int max_tries=60;
    bool connection_success = false;
    while (num_tries < max_tries && !connection_success) {
      try {
          std::cout<<"Attempting to connect to Orchestrator"<<std::endl;
          std::shared_ptr<blazingdb::transport::Node> node = communicationData.getSharedSelfNode();
          auto nodeDataMesssage = blazingdb::manager::NodeDataMessage::Make(node);
          ral::communication::network::Client::sendNodeData(communicationData.getOrchestratorIp(),
                                                            communicationData.getOrchestratorPort(),
                                                            *nodeDataMesssage);

          ral::communication::network::Server::start(ralCommunicationPort);
          connection_success = true;
          std::cout<<"Successfully Connected to Orchestrator"<<std::endl;
      } catch (std::exception &e) {
          if (num_tries < max_tries)
            std::cerr << e.what() << "\n";
          num_tries++;
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
    if (!connection_success)
      return EXIT_FAILURE;

    auto& config = ral::config::BlazingConfig::getInstance();


#ifdef USE_UNIX_SOCKETS

    config.setLogName("RAL." + ralId + ".log")
          .setSocketPath("/tmp/ral." + ralId + ".socket");

    std::cout << "Socket Name: " << config.getSocketPath() << std::endl;

#else

    // NOTE IMPORTANT PERCY aqui es que pyblazing se entera que este es el ip del RAL en el _send de pyblazing
    config.setLogName(loggingName)
          .setSocketPath(ralHost);

    std::cout << "Socket Name: " << config.getSocketPath() << std::endl;

#endif

    // if (loggingName != ""){
      auto output = new Library::Logging::FileOutput(config.getLogName(), false);
      Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
    // } else {
    //   auto output = new Library::Logging::TcpOutput();
    //   std::shared_ptr<Library::Network::NormalSyncSocket> loggingSocket = std::make_shared<Library::Network::NormalSyncSocket>();
    //   loggingSocket->connect(loggingHost.c_str(), loggingPort.c_str());
    //   output.setSocket(loggingSocket);
    //   Library::Logging::ServiceLogging::getInstance().setLogOutput(output);
    // }

    // Init AWS S3 ... TODO see if we need to call shutdown and avoid leaks from s3 percy
    BlazingContext::getInstance()->initExternalSystems();

#ifdef USE_UNIX_SOCKETS

  connectionAddress.unix_socket_path = config.getSocketPath();
  blazingdb::protocol::UnixSocketConnection connection(connectionAddress);

#else

  connectionAddress.tcp_host = "127.0.0.1"; // NOTE always use localhost for protocol server
  connectionAddress.tcp_port = ralProtocolPort;

  std::cout << "RAL TCP protocol port: " << connectionAddress.tcp_port << std::endl;

#endif

  blazingdb::protocol::Server server(connectionAddress.tcp_port);

  services.insert(std::make_pair(interpreter::MessageType_ExecutePlanFileSystem, &executeFileSystemPlanService));
  services.insert(std::make_pair(interpreter::MessageType_LoadCsvSchema, &parseSchemaService));
  services.insert(std::make_pair(interpreter::MessageType_CloseConnection, &closeConnectionService));
  services.insert(std::make_pair(interpreter::MessageType_GetResult, &getResultService));
  services.insert(std::make_pair(interpreter::MessageType_FreeResult, &freeResultService));
  services.insert(std::make_pair(interpreter::MessageType_RegisterFileSystem, &registerFileSystem));
  services.insert(std::make_pair(interpreter::MessageType_DeregisterFileSystem, &deregisterFileSystem));
  services.insert(std::make_pair(interpreter::MessageType_ScanDataSource, &scanDataSource));
  services.insert(std::make_pair(interpreter::MessageType_SystemCommand, &systemCommand));
  services.insert(std::make_pair(interpreter::MessageType_AddToResultRepo, &addToResultRepo));

  server.handle(&interpreterServices);

	return 0;
}
