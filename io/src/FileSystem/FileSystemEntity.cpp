/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemEntity.h"

#include "Util/EncryptionUtil.h"

FileSystemEntity::FileSystemEntity() {}

FileSystemEntity::FileSystemEntity(
	const std::string & authority, const FileSystemConnection & fileSystemConnection, const Path & root)
	: authority(authority), fileSystemConnection(fileSystemConnection), root(root) {}

FileSystemEntity::FileSystemEntity(
	const std::string & authority, const std::string & fileSystemConnection, const std::string & root, bool encrypted) {
	if(encrypted) {
		this->authority = EncryptionUtil::decrypt(authority);
		this->fileSystemConnection = FileSystemConnection(EncryptionUtil::decrypt(fileSystemConnection));
		this->root = Path(EncryptionUtil::decrypt(root), true);
	} else {
		this->authority = authority;
		this->fileSystemConnection = FileSystemConnection(fileSystemConnection);
		this->root = Path(root, true);
	}
}

FileSystemEntity::FileSystemEntity(const FileSystemEntity & other)
	: authority(other.authority), fileSystemConnection(other.fileSystemConnection), root(other.root) {}

FileSystemEntity::FileSystemEntity(FileSystemEntity && other)
	: authority(std::move(other.authority)), fileSystemConnection(std::move(other.fileSystemConnection)),
	  root(std::move(other.root)) {}

FileSystemEntity::~FileSystemEntity() {}

bool FileSystemEntity::isValid() const noexcept {
	if(this->authority.empty()) {
		return false;
	}

	if(this->fileSystemConnection.isValid() == false) {
		return false;
	}

	if(this->root.isValid() == false) {
		return false;
	}

	return true;
}

const std::string FileSystemEntity::getAuthority() const { return this->authority; }

const FileSystemConnection FileSystemEntity::getFileSystemConnection() const { return this->fileSystemConnection; }

const Path FileSystemEntity::getRoot() const { return this->root; }

const std::string FileSystemEntity::getEncryptedAuthority() const { return EncryptionUtil::encrypt(this->authority); }

const std::string FileSystemEntity::getEncryptedFileSystemConnection() const {
	return EncryptionUtil::encrypt(this->fileSystemConnection.toString());
}

const std::string FileSystemEntity::getEncryptedRoot() const {
	return EncryptionUtil::encrypt(this->root.toString(false));
}

std::string FileSystemEntity::toString() const {
	// TODO
	//	std::string fileSystemType;
	//
	//	//TODO percy avoid hardstring (should we put this in FileType.h? as map or function?)
	//	switch (this->fileSystemType) {
	//		case FileSystemType::LOCAL: fileSystemType = "LOCAL"; break;
	//		case FileSystemType::HDFS: fileSystemType = "HDFS"; break;
	//		case FileSystemType::S3: fileSystemType = "S3"; break;
	//		default: fileSystemType = "UNSUPPORTED"; break; //TODO see TODO above
	//	}
	//
	//	std::string connectionProperties = "[";
	//
	//	for (const auto &connectionProperty : this->connectionProperties) {
	//		connectionProperties += connectionProperty.first + " :" + connectionProperty.second;
	//	}
	//
	//	connectionProperties += "]";
	//
	//	return fileSystemType + " => " + connectionProperties;

	return std::string();
}

FileSystemEntity & FileSystemEntity::operator=(const FileSystemEntity & other) {
	this->authority = other.authority;
	this->fileSystemConnection = other.fileSystemConnection;
	this->root = other.root;

	return *this;
}

FileSystemEntity & FileSystemEntity::operator=(FileSystemEntity && other) {
	this->authority = std::move(other.authority);
	this->fileSystemConnection = std::move(other.fileSystemConnection);
	this->root = std::move(other.root);

	return *this;
}

bool FileSystemEntity::operator==(const FileSystemEntity & other) const {
	const bool authorityEquals = (this->authority == other.authority);
	const bool fileSystemConnectionEquals = (this->fileSystemConnection == other.fileSystemConnection);
	const bool rootEquals = (this->root == other.root);

	const bool equals = (authorityEquals && fileSystemConnectionEquals && rootEquals);

	return equals;
}

bool FileSystemEntity::operator!=(const FileSystemEntity & other) const { return !(*this == other); }
