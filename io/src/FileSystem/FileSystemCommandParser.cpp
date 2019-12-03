/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemCommandParser.h"

#include "FileSystem/HadoopFileSystem.h"
#include "FileSystem/S3FileSystem.h"
#include "Util/StringUtil.h"

// TODO percy improve error messages
namespace FileSystemCommandParser {

FileSystemEntity parseRegisterFileSystem(const std::string & command, std::string & error) {
	const bool isLocal = StringUtil::beginsWith(command, "register local");
	const bool isHdfs = StringUtil::beginsWith(command, "register hdfs");
	const bool isS3 = StringUtil::beginsWith(command, "register s3");

	if((isLocal == false) && (isHdfs == false) && (isS3 == false)) {
		error = "ERROR: Bad syntax in registering file system, try with register [local|hdfs|s3] file system";
		return FileSystemEntity();
	}

	const FileSystemType fileSystemType =
		(isLocal) ? FileSystemType::LOCAL : (isHdfs) ? FileSystemType::HDFS : FileSystemType::S3;
	const std::vector<std::string> commandTokens = StringUtil::split(command, " ");

	if(commandTokens.size() < 7) {
		error = "ERROR: Malformed register file system statement";
		return FileSystemEntity();
	}

	const std::string authority = StringUtil::removeEncapsulation(commandTokens[6], "'");
	const std::string fileSystem =
		(fileSystemType == FileSystemType::LOCAL) ? "local" : Uri::fileSystemTypeToScheme(fileSystemType);
	const std::string authSplit = "register " + fileSystem + " file system stored as '" + authority + "'";
	const std::vector<std::string> authSplitTokens = StringUtil::split(command, authSplit);

	FileSystemConnection fileSystemConnection;
	Path root("/");

	switch(authSplitTokens.size()) {
	case 1: {
		if(fileSystemType == FileSystemType::LOCAL) {
			fileSystemConnection = FileSystemConnection(FileSystemType::LOCAL);
		} else {
			error = "ERROR: Register file system without connection string is only supported by local file system";
			return FileSystemEntity();
		}
	} break;

	case 2: {
		const std::string connRoot = authSplitTokens[1];
		const size_t withToken = connRoot.find("with ('");
		const bool hasWith = (withToken != std::string::npos);
		const bool isHdfsOrS3 = (isHdfs || isS3);

		if((hasWith == false) && isHdfsOrS3) {
			error = "ERROR: hdfs and s3 must specify connection string with";
			return FileSystemEntity();
		} else if(hasWith && isLocal) {
			error = "ERROR: local doesn't need to specify any connection string";
			return FileSystemEntity();
		}

		const size_t openParenthesesToken = connRoot.find('(');
		const size_t closeParenthesesToken = connRoot.find(')');
		const std::string connectionArgs =
			connRoot.substr(openParenthesesToken + 1, (closeParenthesesToken - openParenthesesToken) - 1);
		std::string normalizedConnectionArgs = StringUtil::replace(std::string(connectionArgs), "'", "");
		normalizedConnectionArgs = StringUtil::replace(std::string(normalizedConnectionArgs), " ", "");
		const std::vector<std::string> connectionTokens = StringUtil::split(normalizedConnectionArgs, ",");

		if(isHdfs && (connectionTokens.size() != 5)) {
			error = "ERROR: When registering hdfs file system you must specify 5 arguments";
			return FileSystemEntity();
		} else if(isS3 && (connectionTokens.size() != 6)) {
			error = "ERROR: When registering s3 file system you must specify 6 arguments";
			return FileSystemEntity();
		}

		if(isLocal) {
			fileSystemConnection = FileSystemConnection(FileSystemType::LOCAL);
		} else if(isHdfs) {
			const std::string host = connectionTokens[0];
			const int port = atoi(connectionTokens[1].c_str());
			const std::string user = connectionTokens[2];
			const HadoopFileSystemConnection::DriverType driverType =
				HadoopFileSystemConnection::driverTypeFromName(connectionTokens[3]);
			const std::string kerberosTicket = connectionTokens[4];

			fileSystemConnection = FileSystemConnection(host, port, user, driverType, kerberosTicket);
		} else if(isS3) {
			const std::string bucketName = connectionTokens[0];
			const S3FileSystemConnection::EncryptionType encryptionType =
				S3FileSystemConnection::encryptionTypeFromName(connectionTokens[1]);
			const std::string kmsKeyAmazonResourceName = connectionTokens[2];
			const std::string accessKeyId = connectionTokens[3];
			const std::string secretKey = connectionTokens[4];
			const std::string sessionToken = connectionTokens[5];

			fileSystemConnection = FileSystemConnection(
				bucketName, encryptionType, kmsKeyAmazonResourceName, accessKeyId, secretKey, sessionToken);
		}

		const size_t rootToken = connRoot.find("root");
		const bool hasRoot = (rootToken != std::string::npos);

		if(hasRoot) {
			root = StringUtil::removeEncapsulation(connRoot.substr(rootToken + 5), "'");

			if(root.isValid() == false) {
				error = "ERROR: Invalid root provided when registering file system";
				return FileSystemEntity();
			}
		}
	} break;

	default: {
		error = "ERROR: Malformed register file system statement";
		return FileSystemEntity();
	} break;
	}

	return FileSystemEntity(std::move(authority), std::move(fileSystemConnection), std::move(root));
}

std::string parseDeregisterFileSystem(const std::string & command, std::string & error) {
	const bool valid = StringUtil::beginsWith(std::string(command), "deregister file system ");

	if(valid == false) {
		error = "invalid deregister file system statement";
		return std::string();
	}

	const std::vector<std::string> commandTokens = StringUtil::split(command, " ");

	if(commandTokens.size() != 4) {
		error = "malformed deregister fs stmt";
		return std::string();
	}

	const std::string authority = StringUtil::removeEncapsulation(commandTokens[3], "'");

	return authority;
}

}  // namespace FileSystemCommandParser
