/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _GOOGLECLOUDSTORAGE_FILE_SYSTEM_H_
#define _GOOGLECLOUDSTORAGE_FILE_SYSTEM_H_

#include <memory>

#include "FileSystem/FileSystemInterface.h"

/**
 *  @class GoogleCloudStorage
 *
 *  @implements FileSystemInterface
 *
 *  @brief Implements FileSystemInterface for GoogleCloudStorage.
 *
 *  @tparam InputBlazingType is the input C++ Blazing type
 *  @tparam OutputBlazingType is the output C++ Blazing type
*/
class GoogleCloudStorage: public FileSystemInterface {
	public:
		GoogleCloudStorage(const FileSystemConnection &fileSystemConnection, const Path &root = Path("/"));
		virtual ~GoogleCloudStorage();

		FileSystemType getFileSystemType() const noexcept {
                        return FileSystemType::GOOGLE_CLOUD_STORAGE;
		}

		// Connection
		FileSystemConnection getFileSystemConnection() const noexcept;

		// State
		Path getRoot() const noexcept;

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
		bool move(const Uri &src, const Uri &dst) const;
		bool truncateFile(const Uri &uri, long long length) const;

		// I/O
		std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri &uri) const;
		std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri &uri) const;

	private:
		GoogleCloudStorage(FileSystemType fileSystemType);

	private:
		class Private;
		const std::unique_ptr<Private> pimpl; // private implementation
};

#endif /* _GOOGLECLOUDSTORAGE_FILE_SYSTEM_H_ */
