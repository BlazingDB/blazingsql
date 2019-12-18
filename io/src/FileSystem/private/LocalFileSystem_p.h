/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _LOCAL_FILE_SYSTEM_PRIVATE_H_
#define _LOCAL_FILE_SYSTEM_PRIVATE_H_

#include "FileSystem/LocalFileSystem.h"

class LocalFileSystem::Private {
public:
	Private(const Path & root);

	// Query
	bool exists(const Uri & uri) const;
	FileStatus getFileStatus(const Uri & uri) const;

	// List
	std::vector<FileStatus> list(const Uri & uri, const FileFilter & filter) const;
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

public:
	// State
	Path root;
};

#endif /* _LOCAL_FILE_SYSTEM_PRIVATE_H_ */
