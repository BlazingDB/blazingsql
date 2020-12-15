/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEMEXCEPTION_H_
#define _FILESYSTEMEXCEPTION_H_

#include <stdexcept>
#include <string>

#include "FileSystem/FileSystemType.h"

class FileSystemException : public std::runtime_error {
public:
	static const std::string RUNTIME_ERROR;

	explicit FileSystemException(FileSystemType fileSystemType, const std::string & error) noexcept;

	explicit FileSystemException(
		FileSystemType fileSystemType, const std::string & error, const std::string & uri1) noexcept;

	explicit FileSystemException(FileSystemType fileSystemType,
		const std::string & error,
		const std::string & uri1,
		const std::string & uri2) noexcept;

	FileSystemType getFileSystemType() const noexcept;
	const std::string getError() const noexcept;
	const std::string getUri1() const noexcept;
	const std::string getUri2() const noexcept;

private:
	const FileSystemType fileSystemType;
	const std::string error;
	const std::string uri1;
	const std::string uri2;
};

#endif /* _FILESYSTEMEXCEPTION_H_ */
