/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemManager.h"

#include "private/FileSystemManager_p.h"

#include <iostream>

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

FileSystemManager::FileSystemManager() : pimpl(new FileSystemManager::Private()) {
	//NOTE setup default local connection
	const FileSystemConnection connection(FileSystemType::LOCAL);
	const bool ok = this->registerFileSystem(FileSystemEntity("local", std::move(connection)));

	if (ok == false) { // could not register local file system
		//TODO percy error handilng
		//assert here, this is a fatal error
	}
}

FileSystemManager::~FileSystemManager(){
}

bool FileSystemManager::registerFileSystem(const FileSystemEntity &fileSystemEntity) {
	return this->pimpl->registerFileSystem(fileSystemEntity);
}

//return false in case of empty input
bool FileSystemManager::registerFileSystems(const std::vector<FileSystemEntity> &fileSystemEntities) {
	if (fileSystemEntities.empty()) {
		return false;
	}

	for (const FileSystemEntity &fileSystemEntity : fileSystemEntities) {
		const bool ok = this->registerFileSystem(fileSystemEntity);
		if (ok == false) {
			Logging::Logger().logError("Was unable to register filesystem");
			//TODO percy error handling
			return false;
		}
	}

	return true;
}

bool FileSystemManager::deregisterFileSystem(const std::string& authority) {
	return this->pimpl->deregisterFileSystem(authority);
}

bool FileSystemManager::exists(const Uri& uri) const {
	return this->pimpl->exists(uri);
}

FileStatus FileSystemManager::getFileStatus(const Uri& uri) const {
	return this->pimpl->getFileStatus(uri);
}

std::vector<FileStatus> FileSystemManager::list(const Uri &uri, const FileFilter &filter) const {
	return this->pimpl->list(uri, filter);
}

std::vector<FileStatus> FileSystemManager::list(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	return this->pimpl->list(uri, fileType, wildcard);
}

std::vector<Uri> FileSystemManager::list(const Uri &uri, const std::string &wildcard) const {
	return this->pimpl->list(uri, wildcard);
}

std::vector<std::string> FileSystemManager::listResourceNames(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	return this->pimpl->listResourceNames(uri, fileType, wildcard);
}

std::vector<std::string> FileSystemManager::listResourceNames(const Uri &uri, const std::string &wildcard) const {
	return this->pimpl->listResourceNames(uri, wildcard);
}

bool FileSystemManager::makeDirectory(const Uri& uri) const {
	return this->pimpl->makeDirectory(uri);
}

bool FileSystemManager::remove(const Uri& uri) const {
	return this->pimpl->remove(uri);
}

bool FileSystemManager::move(const Uri& src, const Uri& dst) const {
	return this->pimpl->move(src, dst);
}

bool FileSystemManager::truncateFile(const Uri& uri, const long long length) const {
	return this->pimpl->truncateFile(uri, length);
}

std::shared_ptr<arrow::io::RandomAccessFile> FileSystemManager::openReadable(const Uri& uri) const {
	return this->pimpl->openReadable(uri);
}

std::shared_ptr<arrow::io::OutputStream> FileSystemManager::openWriteable(const Uri& uri) const {
	return this->pimpl->openWriteable(uri);
}
