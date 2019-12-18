/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _GOOGLECLOUDSTORAGE_FILE_SYSTEM_PRIVATE_H_
#define _GOOGLECLOUDSTORAGE_FILE_SYSTEM_PRIVATE_H_

#include "GoogleCloudStorageOutputStream.h"
#include "GoogleCloudStorageReadableFile.h"

#include "FileSystem/GoogleCloudStorage.h"

class GoogleCloudStorage::Private {
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
	bool truncateFile(const Uri & uri, long long length) const;

	// I/O
	bool openReadable(const Uri & uri, std::shared_ptr<GoogleCloudStorageReadableFile> * file) const;
	bool openWriteable(const Uri & uri, std::shared_ptr<GoogleCloudStorageOutputStream> * file) const;

public:
	// State
	Path root;

private:
	// Setup connection
	bool connect(const FileSystemConnection & fileSystemConnection);
	bool disconnect();

	const std::string getProjectId() const;   // get the project id from the current Google Cloud Storage connection
	const std::string getBucketName() const;  // get the bucket name from the current Google Cloud Storage connection
	bool useDefaultAdcJsonFile() const;		  // returns true if the user chosse to use the default ADC configuraiton
	const std::string
	getAdcJsonFile() const;  // if useDefaultAdcJsonFile is false then use the new location in order to setup the auth

private:
	FileSystemConnection fileSystemConnection;
	std::shared_ptr<gcs::Client> gcsClient;
	std::string regionName;
};

#endif /* _GOOGLECLOUDSTORAGE_FILE_SYSTEM_PRIVATE_H_ */
