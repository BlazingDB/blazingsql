/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_COMMAND_PARSER_FUNCTIONS_H_
#define _FILESYSTEM_COMMAND_PARSER_FUNCTIONS_H_

#include "FileSystem/FileSystemEntity.h"

namespace FileSystemCommandParser {
	/**
	 * @brief Parse the register file system command.
	 *
	 * Returns a FileSystemEntity if the command is correct, otherwise fill the error argument and return invalid FileSystemEntity.
	 *
	 * @note
	 * The command has the next syntax:
	 * register [local/hdfs/s3] file system stored as 'namespaceId' with ('connectionPropertyValue1', 'connectionPropertyValue2', ... , 'connectionPropertyValueN') [root 'optionalRootPath']
	 * Specific syntax for each File System:
	 * register local file system stored as 'namespaceId' root 'optionalRootPath'
	 * register hdfs file system stored as 'namespaceId' with (host, port, user, driver, kerberos_ticket) root 'optionalRootPath'
	 * register s3 file system stored as 'namespaceId' with (bucket, encryption_type, kms_key_amazon_resource_name, access_key_id, secret_key, session_token) root 'optionalRootPath'
	 *
	 * @param command must represent a register file system statement
	 * @param error is filled in case command cannot parse the command
	 * @return FileSystemEntity with valid data if parsing was successful, otherwise will have invalid data
	*/
	FileSystemEntity parseRegisterFileSystem(const std::string &command, std::string &error);

	/**
	 * @brief Parse the deregister file system command.
	 *
	 * Returns the authority if the command is correct, otherwise fill the error argument and return empty string.
	 *
	 * @note
	 * The command has the next syntax:
	 * deregister file system 'namespaceId'
	 *
	 * @param command must represent a deregister file system statement
	 * @param error is filled in case command cannot parse the command
	 * @return authority string if parsing was successful, otherwise will have empty string
	*/
	std::string parseDeregisterFileSystem(const std::string &command, std::string &error);
}

#endif /* _FILESYSTEM_COMMAND_PARSER_FUNCTIONS_H_ */
