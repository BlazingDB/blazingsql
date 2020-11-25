/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemException.h"

#include <sstream>

const std::string FileSystemException::RUNTIME_ERROR = "file system error";

static const std::string whatMessage(FileSystemType /*fileSystemType*/,
	const std::string & error,
	const std::string & uri1 = "",
	const std::string & uri2 = "") {
	// TODO stringfy FilesystemType enum
	std::stringstream messageBuilder;
	messageBuilder << FileSystemException::RUNTIME_ERROR;
	messageBuilder << ": " << error;
	messageBuilder << " ("
				   << "fileSystemType"
				   << " file system) ";

	if(uri1.empty() != false) {
		messageBuilder << " using file " << uri1;
	}

	if(uri2.empty() != false) {
		messageBuilder << " and " << uri2;
	}

	const std::string message = messageBuilder.str();

	return message;
}

FileSystemException::FileSystemException(FileSystemType fileSystemType, const std::string & error) noexcept
	: std::runtime_error(whatMessage(fileSystemType, error)), fileSystemType(fileSystemType), error(error) {}

FileSystemException::FileSystemException(
	FileSystemType fileSystemType, const std::string & error, const std::string & uri1) noexcept
	: std::runtime_error(whatMessage(fileSystemType, error, uri1)), fileSystemType(fileSystemType), error(error),
	  uri1(uri1) {}

FileSystemException::FileSystemException(FileSystemType fileSystemType,
	const std::string & error,
	const std::string & uri1,
	const std::string & uri2) noexcept
	: std::runtime_error(whatMessage(fileSystemType, error, uri1, uri2)), fileSystemType(fileSystemType), error(error),
	  uri1(uri1), uri2(uri2) {}

const std::string FileSystemException::getError() const noexcept { return this->error; }

const std::string FileSystemException::getUri1() const noexcept { return this->uri1; }

const std::string FileSystemException::getUri2() const noexcept { return this->uri2; }

const FileSystemType FileSystemException::getFileSystemType() const noexcept { return this->fileSystemType; }
