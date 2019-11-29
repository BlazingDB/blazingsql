/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _S3_FILE_SYSTEM_H_
#define _S3_FILE_SYSTEM_H_

#include <memory>

#include "FileSystem/FileSystemInterface.h"

/**
 *  @class S3FileSystem
 *
 *  @implements FileSystemInterface
 *
 *  @brief Implements FileSystemInterface for S3.
 *
 *  @tparam InputBlazingType is the input C++ Blazing type
 *  @tparam OutputBlazingType is the output C++ Blazing type
 */
class S3FileSystem : public FileSystemInterface {
public:
	S3FileSystem(const FileSystemConnection & fileSystemConnection, const Path & root = Path("/"));
	virtual ~S3FileSystem();

	FileSystemType getFileSystemType() const noexcept { return FileSystemType::S3; }

	// Connection
	FileSystemConnection getFileSystemConnection() const noexcept;

	// State
	Path getRoot() const noexcept;

	// Query
	bool exists(const Uri & uri) const;
	FileStatus getFileStatus(const Uri & uri) const;

	// List
	std::vector<FileStatus> list(const Uri & uri, const FileFilter & filter) const;
	std::vector<FileStatus> list(const Uri & uri, FileType fileType, const std::string & wildcard = "*") const;
	std::vector<Uri> list(const Uri & uri, const std::string & wildcard = "*") const;
	std::vector<std::string> listResourceNames(
		const Uri & uri, FileType fileType, const std::string & wildcard = "*") const;
	std::vector<std::string> listResourceNames(const Uri & uri, const std::string & wildcard = "*") const;

	// Operations
	bool makeDirectory(const Uri & uri) const;
	bool remove(const Uri & uri) const;
	bool move(const Uri & src, const Uri & dst) const;
	bool truncateFile(const Uri & uri, long long length) const;

	// I/O
	std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri & uri) const;
	std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri & uri) const;

private:
	S3FileSystem(FileSystemType fileSystemType);

private:
	class Private;
	const std::unique_ptr<Private> pimpl;  // private implementation
};

#endif /* _S3_FILE_SYSTEM_H_ */
