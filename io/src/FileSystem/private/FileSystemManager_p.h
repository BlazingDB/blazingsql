/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_MANAGER_PRIVATE_H_
#define _FILESYSTEM_MANAGER_PRIVATE_H_

#include <map>
#include <vector>
#include <string>

#include "FileSystem/FileSystemManager.h"
#include "FileSystem/FileSystemInterface.h"

//Composite pattern but we don't need to use FileSystemInterface as base class
class FileSystemManager::Private {
	public:
		Private();
		virtual ~Private();

		bool registerFileSystem(const FileSystemEntity &fileSystemEntity);
		bool deregisterFileSystem(const std::string &authority);

		// Query
		bool exists(const Uri &uri) const;
		FileStatus getFileStatus(const Uri &uri) const;

		// List
		std::vector<FileStatus> list(const Uri &uri, const FileFilter &filter) const;
		std::vector<FileStatus> list(const Uri &uri, FileType fileType, const std::string &wildcard = "*") const;
		std::vector<Uri> list(const Uri &uri, const std::string &wildcard = "*") const;
		std::vector<std::string> listResourceNames(const Uri &uri, FileType fileType, const std::string &wildcard = "*") const;
		std::vector<std::string> listResourceNames(const Uri &uri, const std::string &wildcard = "*") const;

		// Operations
		bool makeDirectory(const Uri &uri) const;
		bool remove(const Uri &uri) const;
		bool move(const Uri &src, const Uri &dst) const; // powerfull: can move between diferent fs
		bool truncateFile(const Uri &uri, long long length) const;

		// I/O
		std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri &uri) const;
		std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri &uri) const;

	private:
		int verifyFileSystemUri(const Uri &uri) const; // returns FileSystem id if ok, -1 otherwise

	private:
		std::map<std::string, Path> roots; // <authority, root>
		std::map<std::string, int> fileSystemIds; // <authority, fs id>
		std::vector<std::unique_ptr<FileSystemInterface>> fileSystems; // [fs id] = fs
};

#endif /* _FILESYSTEM_MANAGER_PRIVATE_H_ */
