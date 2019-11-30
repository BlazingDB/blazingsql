/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_MANAGER_H_
#define _FILESYSTEM_MANAGER_H_

#include <memory>

#include "arrow/io/interfaces.h"

#include "FileSystem/FileFilter.h"
#include "FileSystem/FileSystemEntity.h"

class FileSystemManager {
public:
	FileSystemManager();
	virtual ~FileSystemManager();

	bool registerFileSystem(const FileSystemEntity & fileSystemEntity);					 // will reuse connections
	bool registerFileSystems(const std::vector<FileSystemEntity> & fileSystemEntities);  // convenience method
	bool deregisterFileSystem(const std::string &
			authority);  // will try to disconnect only when there are no more authorities using the same fs

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
	bool move(const Uri & src, const Uri & dst) const;  // powerfull: can move between diferent fs
	bool truncateFile(const Uri & path, const long long length) const;

	// I/O
	std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri & uri) const;
	std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri & uri) const;

private:
	class Private;
	const std::unique_ptr<Private> pimpl;  // private implementation
};

#endif /* _FILESYSTEM_MANAGER_H_ */
