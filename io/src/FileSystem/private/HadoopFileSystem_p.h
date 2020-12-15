/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _HADOOP_FILE_SYSTEM_PRIVATE_H_
#define _HADOOP_FILE_SYSTEM_PRIVATE_H_

#include "FileSystem/HadoopFileSystem.h"

#include "arrow/io/hdfs.h"

class HadoopFileSystem::Private {
public:
	Private(const FileSystemConnection & fileSystemConnection, const Path & root);
	~Private();

	// Connection
	FileSystemConnection getFileSystemConnection() const noexcept;

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
	bool truncateFile(const Uri & uri, unsigned long long length) const;

	// I/O
	std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri & uri) const;
	std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri & uri) const;

public:
	// State
	bool connected;
	Path root;

private:
	// Setup connection
	/**
	 *  @brief Establish connection with HDSF server.
	 *
	 *  @details The \p connectionProperties need to be correct, \see HadoopFileSystem::buildConnectionProperties
	 *
	 *  @param connectionProperties a map
	 *
	 *  @return true if could establish connection, false otherwise.
	 *
	 *  @throw FileSystemException
	 */
	bool connect(const FileSystemConnection & fileSystemConnection);
	bool disconnect();

private:
	FileSystemConnection fileSystemConnection;
	std::shared_ptr<arrow::io::HadoopFileSystem> hdfs;  // should be std::unique_ptr but we are contrained by Arrow API
};

#endif /* _HADOOP_FILE_SYSTEM_PRIVATE_H_ */
