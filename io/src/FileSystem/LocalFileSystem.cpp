/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "HadoopFileSystem.h"

#include "private/LocalFileSystem_p.h"

LocalFileSystem::LocalFileSystem(const Path &root)
	: pimpl(new LocalFileSystem::Private(root)) {
}

LocalFileSystem::~LocalFileSystem() {
}

FileSystemConnection LocalFileSystem::getFileSystemConnection() const noexcept {
	// alwayes returns a fiexed connection string
	return FileSystemConnection(FileSystemType::LOCAL);
}

Path LocalFileSystem::getRoot() const noexcept {
	return this->pimpl->root;
}

bool LocalFileSystem::exists(const Uri &uri) const {
	const bool result = this->pimpl->exists(uri);
	return result;
}

FileStatus LocalFileSystem::getFileStatus(const Uri &uri) const {
	const FileStatus result = this->pimpl->getFileStatus(uri);
	return result;
}

std::vector<FileStatus> LocalFileSystem::list(const Uri &uri, const FileFilter &filter) const {
	const std::vector<FileStatus> result = this->pimpl->list(uri, filter);
	return result;
}

std::vector<FileStatus> LocalFileSystem::list(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	FileTypeWildcardFilter filter(fileType, wildcard);
	return this->list(uri, filter);
}

std::vector<Uri> LocalFileSystem::list(const Uri &uri, const std::string &wildcard) const {
	const std::vector<Uri> result = this->pimpl->list(uri, wildcard);
	return result;
}

std::vector<std::string> LocalFileSystem::listResourceNames(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, fileType, wildcard);
	return result;
}

std::vector<std::string> LocalFileSystem::listResourceNames(const Uri &uri, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, wildcard);
	return result;
}

bool LocalFileSystem::makeDirectory(const Uri &uri) const {
	const bool result = this->pimpl->makeDirectory(uri);
	return result;
}

bool LocalFileSystem::remove(const Uri &uri) const {
	const bool result = this->pimpl->remove(uri);
	return result;
}

bool LocalFileSystem::move(const Uri &src, const Uri &dst) const {
	const bool result = this->pimpl->move(src, dst);
	return result;
}

bool LocalFileSystem::truncateFile(const Uri &uri, const long long length) const {
	const bool result = this->pimpl->truncateFile(uri, length);
	return result;
}

std::shared_ptr<arrow::io::RandomAccessFile> LocalFileSystem::openReadable(const Uri &uri) const {
	return this->pimpl->openReadable(uri);
}

std::shared_ptr<arrow::io::OutputStream> LocalFileSystem::openWriteable(const Uri &uri) const {
	return this->pimpl->openWriteable(uri);
}
