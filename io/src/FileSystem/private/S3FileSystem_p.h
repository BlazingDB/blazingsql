/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _S3_FILE_SYSTEM_PRIVATE_H_
#define _S3_FILE_SYSTEM_PRIVATE_H_

#include "aws/s3/S3Client.h"

#include "S3OutputStream.h"
#include "S3ReadableFile.h"

#include "FileSystem/S3FileSystem.h"

class S3FileSystem::Private {
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
	bool openReadable(const Uri & uri, std::shared_ptr<S3ReadableFile> * file) const;
	bool openWriteable(const Uri & uri, std::shared_ptr<S3OutputStream> * file) const;

public:
	// State
	Path root;

private:
	// Setup connection
	bool connect(const FileSystemConnection & fileSystemConnection);
	bool disconnect();

	const std::string getBucketName() const;  // get the bucket name from the current s3 file system connection
	bool checkBucket() const;
	S3FileSystemConnection::EncryptionType
	encryptionType() const;	// get the encryption type from the current s3 file system connection
	bool isEncrypted() const;  // returns true if the connection is encrypted (AES-256 or AWS-KMS), returns false when
							   // EncryptionType is None
	Aws::S3::Model::ServerSideEncryption
	serverSideEncryption() const;	// get the encryption type from the current s3 file system connection
	bool isAWSKMSEncrypted() const;  // returns true if the connection is encrypted with AWS-KMS
	const std::string
	getSSEKMSKeyId() const;  // if isAWSKMSEncrypted is true then returns the KMS_KEY_AMAZON_RESOURCE_NAME

private:
	FileSystemConnection fileSystemConnection;
	std::shared_ptr<Aws::S3::S3Client> s3Client;
	std::string regionName;
};

#endif /* _S3_FILE_SYSTEM_PRIVATE_H_ */
