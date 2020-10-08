/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemManager_p.h"

#include <iostream>

#include "ExceptionHandling/BlazingException.h"
#include "FileSystemFactory.h"
#include "Library/Logging/Logger.h"
#include "Util/FileUtil.h"

namespace Logging = Library::Logging;

FileSystemManager::Private::Private() {}

FileSystemManager::Private::~Private() {}

bool FileSystemManager::Private::registerFileSystem(const FileSystemEntity & fileSystemEntity) {
	const std::string & authority = fileSystemEntity.getAuthority();
	const FileSystemConnection & fileSystemConnection = fileSystemEntity.getFileSystemConnection();
	const Path & root = fileSystemEntity.getRoot();

	for(const auto & entry : this->fileSystemIds) {
		if(authority == entry.first) {
			// TODO percy error handling
			// namespace (authority) must be unique
			Logging::Logger().logError("Was unable to register filesystem. Filesystem must be unique");
			return false;
		}
	}

	int foundIndex = -1;

	for(int i = 0; i < this->fileSystems.size(); ++i) {
		const bool found = (this->fileSystems[i]->getFileSystemConnection() == fileSystemConnection) &&
						   this->fileSystems[i]->getRoot() == root;

		if(found) {
			foundIndex = i;
			break;
		}
	}

	if(foundIndex == -1) {  // if fs was not found
		FileSystemFactory fileSystemFactory;
		auto fileSystem = fileSystemFactory.createFileSystem(fileSystemConnection, root);

		if(fileSystem == nullptr) {
			Logging::Logger().logError("Was unable to create and connect to filesystem");
			// TODO percy error handling
			return false;
		}

		this->fileSystems.push_back(std::move(fileSystem));
		this->fileSystemIds[authority] = this->fileSystems.size() - 1;
	} else {  // only reuse fs that aren't null and were connected
		Logging::Logger().logTrace("filesystem previously created. It was found and will be reused");
		this->fileSystemIds[authority] = foundIndex;
	}

	const int fileSystemId = this->fileSystemIds[authority];

	this->roots[authority] = root;

	return true;
}

bool FileSystemManager::Private::deregisterFileSystem(const std::string & authority) {
	bool found = false;

	for(const auto & entry : this->fileSystemIds) {
		if(authority == entry.first) {
			found = true;
			break;
		}
	}

	if(found == false) {
		return false;
	}

	const int fileSystemId = this->fileSystemIds[authority];

	this->roots.erase(authority);
	this->fileSystemIds.erase(authority);

	// if nobody else is using the associated file system then delete the file system too
	bool deleteFileSystem = true;

	for(const auto & entry : this->fileSystemIds) {
		const int usedFileSystemId = this->fileSystemIds[entry.first];

		if(usedFileSystemId == fileSystemId) {
			deleteFileSystem = false;
			break;
		}
	}

	if(deleteFileSystem) {
		auto fileSystem =
			std::move(this->fileSystems[fileSystemId]);  // transfer ownership so it can be deleted within this scope
		this->fileSystems.erase(this->fileSystems.begin() + fileSystemId);
	}
}

bool FileSystemManager::Private::exists(const Uri & uri) const {
	if(uri.isValid() == false) {
		return false;
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		const auto ret = this->fileSystems.at(fileSystemId)->exists(uri);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in exists with Uri: " + uriStr);
		return false;
	}
}

FileStatus FileSystemManager::Private::getFileStatus(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}

	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->getFileStatus(uri);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in getFileStatus with Uri: " + uriStr);
		throw;
	}
}

std::vector<FileStatus> FileSystemManager::Private::list(const Uri & uri, const FileFilter & filter) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}

	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->list(uri, filter);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in list with Uri: " + uriStr);
		throw;
	}
}

std::vector<FileStatus> FileSystemManager::Private::list(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->list(uri, fileType, wildcard);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in list with Uri: " + uriStr);
		throw;
	}
}

std::vector<Uri> FileSystemManager::Private::list(const Uri & uri, const std::string & wildcard) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->list(uri, wildcard);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in list with Uri: " + uriStr);
		throw;
	}
}

std::vector<std::string> FileSystemManager::Private::listResourceNames(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->listResourceNames(uri, fileType, wildcard);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in listResourceNames with Uri: " + uriStr);
		throw;
	}
}

std::vector<std::string> FileSystemManager::Private::listResourceNames(
	const Uri & uri, const std::string & wildcard) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->listResourceNames(uri, wildcard);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in listResourceNames with Uri: " + uriStr);
		throw;
	}
}

bool FileSystemManager::Private::makeDirectory(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->makeDirectory(uri);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in makeDirectory with Uri: " + uriStr);
		throw;
	}
}

bool FileSystemManager::Private::remove(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->remove(uri);

		return ret;
	} catch(BlazingFileNotFoundException & e) {
		return true;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in remove with Uri: " + uriStr);
		throw;
	}
}

bool FileSystemManager::Private::move(const Uri & src, const Uri & dst) const {
	// TODO percy there are duplicated validations since each file system do these kind of checking
	if(src.isValid() == false) {
		// TODO percy thrown exception
	}

	if(dst.isValid() == false) {
		// TODO percy thrown exception
	}

	try {
		const int fileSystemIdSrc = this->verifyFileSystemUri(src);
		const int fileSystemIdDst = this->verifyFileSystemUri(dst);

		if(fileSystemIdSrc != fileSystemIdDst) {
			// we need to copy and then delete the original
			// TODO when we implement the copy operation in the FileSystemManager, we can replace the manual copy step
			// and replace with a general copy
			FileUtilv2::copyFile(src, dst);
			return remove(src);

		} else {
			const auto ret = this->fileSystems.at(fileSystemIdSrc)->move(src, dst);
			return ret;
		}

	} catch(const std::exception & e) {
		std::string uriStr1 = src.toString();
		std::string uriStr2 = dst.toString();
		Logging::Logger().logError("Caught error in move with Uri: " + uriStr1 + " and Uri " + uriStr2);
		throw;
	}
}

bool FileSystemManager::Private::truncateFile(const Uri & uri, long long length) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}

	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		const auto ret = this->fileSystems.at(fileSystemId)->truncateFile(uri, length);

		return ret;
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in truncateFile with Uri: " + uriStr);
		throw;
	}
}

std::shared_ptr<arrow::io::RandomAccessFile> FileSystemManager::Private::openReadable(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}
	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		return this->fileSystems.at(fileSystemId)->openReadable(uri);
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in openReadable with Uri: " + uriStr);
		throw;
	}
}

std::shared_ptr<arrow::io::OutputStream> FileSystemManager::Private::openWriteable(const Uri & uri) const {
	if(uri.isValid() == false) {
		// TODO percy thrown exception
	}

	try {
		const int fileSystemId = this->verifyFileSystemUri(uri);

		// TODO check fileSystemId ... manage error cases

		return this->fileSystems.at(fileSystemId)->openWriteable(uri);
	} catch(const std::exception & e) {
		std::string uriStr = uri.toString();
		Logging::Logger().logError("Caught error in openWriteable with Uri: " + uriStr);
		throw;
	}
}

// Private stuff

int FileSystemManager::Private::verifyFileSystemUri(const Uri & uri) const {
	try {
		const int fileSystemId = this->fileSystemIds.at(uri.getAuthority());
		const FileSystemType fileSystemType = this->fileSystems.at(fileSystemId)->getFileSystemType();

		if(fileSystemType != uri.getFileSystemType()) {
			// TODO percy thrown exception
			std::string authority = uri.getAuthority();
			std::string uriStr = uri.toString();
			Logging::Logger().logError(
				"Caught error (-1) verifyFileSystemUri with Uri: " + uriStr + " with name space name " + authority);

			return -1;
		}

		return fileSystemId;
	} catch(const std::exception & e) {
		std::string authority = uri.getAuthority();
		std::string uriStr = uri.toString();
		Logging::Logger().logError(
			"Caught error verifyFileSystemUri with Uri: " + uriStr + " with name space name " + authority);
		throw;
	}
}
