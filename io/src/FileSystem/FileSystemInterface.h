/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEMINTERFACE_H_
#define _FILESYSTEMINTERFACE_H_

#include "arrow/io/interfaces.h"

#include "FileSystem/FileFilter.h"
#include "FileSystem/FileSystemConnection.h"

// NOTE Instances of FileSystemInterface must implement a constructor that takes a:
// - files system connection
// - root path
// and the constructor must connect to the underlying file system
class FileSystemInterface {
public:
	// NOTE each file system instance will close its connection to the underlying fs in the destructor
	// We plan to manage the lifetime of derived classes using a pointer
	// (std::unique_ptr) to this interface. Only derived classes can be deleted.
	virtual ~FileSystemInterface() = default;

	virtual FileSystemType getFileSystemType() const noexcept = 0;

	// Connection
	virtual FileSystemConnection getFileSystemConnection() const noexcept = 0;

	// State
	virtual Path getRoot() const noexcept = 0;  // if setRoot wan't invoked, then returns Path::DEFAULT_ROOT by default

	// Query
	virtual bool exists(const Uri & uri) const = 0;
	virtual FileStatus getFileStatus(const Uri & uri) const = 0;

	// List

	/**
	 * @brief Returns a list of the file status of all the files and directories in the directory.
	 *
	 * Returns an empty list if the URI is invalid or the directory does not exist or if nothing matches the wildcard
	 * pattern.
	 *
	 * @note
	 * This call will get the file status info for each file in order to apply the filter.
	 *
	 * @param uri must represent a directory
	 * @param wildcard is the string pattern used to filter the list
	 * @return list of URIs
	 */
	virtual std::vector<FileStatus> list(const Uri & uri, const FileFilter & filter) const = 0;

	/**
	 * Convenience method, will use list(const Uri &uri, const FileFilter &filter) under the hood in order to do the
	 * job.
	 */
	virtual std::vector<FileStatus> list(
		const Uri & uri, FileType fileType, const std::string & wildcard = "*") const = 0;

	/**
	 * @brief Returns a list of the file status of all the files and directories in the directory.
	 *
	 * Returns an empty list if the URI is invalid or the directory does not exist or if nothing matches the wildcard
	 * pattern.
	 *
	 * @note
	 * This call is faster than other list methods since doesn't need to get the file status info for each file in order
	 * to apply the filter.
	 *
	 * @param uri must represent a directory
	 * @param wildcard is the string pattern used to filter the list
	 * @return list of URIs
	 */
	virtual std::vector<Uri> list(const Uri & uri, const std::string & wildcard = "*") const = 0;

	/**
	 * @brief Returns a list of the resource names of all the files and directories in the directory.
	 *
	 * Returns an empty list if the URI is invalid or the directory does not exist or if nothing matches the wildcard
	 * pattern.
	 *
	 * @note
	 * This call will get the file status info for each file in order to apply the filter for FileType.
	 *
	 * @param uri must represent a directory
	 * @param fileType will be used to filter the list
	 * @param wildcard is the string pattern used to filter the list
	 * @return list of URI resource names
	 *
	 * @see Uri::getPath()
	 * @see Path::getResourceName()
	 */
	virtual std::vector<std::string> listResourceNames(
		const Uri & uri, FileType fileType, const std::string & wildcard = "*") const = 0;

	/**
	 * @brief Returns a list of the resource names of all the files and directories in the directory.
	 *
	 * Returns an empty list if the URI is invalid or the directory does not exist or if nothing matches the wildcard
	 * pattern.
	 *
	 * @note
	 * This call is faster than other listResourceNames methods since doesn't need to get the file status info for each
	 * file in order to apply the filter.
	 *
	 * @param uri must represent a directory
	 * @param wildcard is the string pattern used to filter the list
	 * @return list of URI resource names
	 *
	 * @see Uri::getPath()
	 * @see Path::getResourceName()
	 */
	virtual std::vector<std::string> listResourceNames(const Uri & uri, const std::string & wildcard = "*") const = 0;

	// Operations
	virtual bool makeDirectory(const Uri & uri) const = 0;
	virtual bool remove(const Uri & uri) const = 0;
	virtual bool move(const Uri & src, const Uri & dst) const = 0;
	virtual bool truncateFile(const Uri & uri, long long length) const = 0;

	// I/O
	virtual std::shared_ptr<arrow::io::RandomAccessFile> openReadable(const Uri & uri) const = 0;
	virtual std::shared_ptr<arrow::io::OutputStream> openWriteable(const Uri & uri) const = 0;

protected:
	FileSystemInterface() = default;

private:
	FileSystemInterface(const FileSystemInterface &) = delete;
	FileSystemInterface(const FileSystemInterface &&) = delete;
	FileSystemInterface & operator=(const FileSystemInterface &) = delete;
};

#endif /* _FILESYSTEMINTERFACE_H_ */
