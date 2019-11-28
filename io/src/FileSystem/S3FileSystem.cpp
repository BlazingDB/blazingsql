/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "S3FileSystem.h"

#include "private/S3FileSystem_p.h"

S3FileSystem::S3FileSystem(const FileSystemConnection &fileSystemConnection, const Path &root)
	: pimpl(new S3FileSystem::Private(fileSystemConnection, root)) {
}

S3FileSystem::~S3FileSystem() {
}

FileSystemConnection S3FileSystem::getFileSystemConnection() const noexcept {
	const FileSystemConnection result = this->pimpl->getFileSystemConnection();
	return result;
}

Path S3FileSystem::getRoot() const noexcept {
	return this->pimpl->root;
}

bool S3FileSystem::exists(const Uri &uri) const {
	const bool result = this->pimpl->exists(uri);
	return result;
}

FileStatus S3FileSystem::getFileStatus(const Uri &uri) const {
	const FileStatus result = this->pimpl->getFileStatus(uri);
	return result;
}

std::vector<FileStatus> S3FileSystem::list(const Uri &uri, const FileFilter &filter) const {
	const std::vector<FileStatus> result = this->pimpl->list(uri, filter);
	return result;
}

std::vector<FileStatus> S3FileSystem::list(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	FileTypeWildcardFilter filter(fileType, wildcard);
	return this->list(uri, filter);
}

std::vector<Uri> S3FileSystem::list(const Uri &uri, const std::string &wildcard) const {
	const std::vector<Uri> result = this->pimpl->list(uri, wildcard);
	return result;
}

std::vector<std::string> S3FileSystem::listResourceNames(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, fileType, wildcard);
	return result;
}

std::vector<std::string> S3FileSystem::listResourceNames(const Uri &uri, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, wildcard);
	return result;
}

bool S3FileSystem::makeDirectory(const Uri &uri) const {
	const bool result = this->pimpl->makeDirectory(uri);
	return result;
}

bool S3FileSystem::remove(const Uri &uri) const {
	const bool result = this->pimpl->remove(uri);
	return result;
}

bool S3FileSystem::move(const Uri &src, const Uri &dst) const {
	const bool result = this->pimpl->move(src, dst);
	return result;
}

bool S3FileSystem::truncateFile(const Uri &uri, const long long length) const {
	const bool result = this->pimpl->truncateFile(uri, length);
	return result;
}

std::shared_ptr<arrow::io::RandomAccessFile> S3FileSystem::openReadable(const Uri &uri) const {
	std::shared_ptr<S3ReadableFile> file;
	this->pimpl->openReadable(uri,&file);
	return std::static_pointer_cast<arrow::io::RandomAccessFile>(file);
}

std::shared_ptr<arrow::io::OutputStream> S3FileSystem::openWriteable(const Uri &uri) const {
	std::shared_ptr<S3OutputStream> file;
	this->pimpl->openWriteable(uri,&file);
	return std::static_pointer_cast<arrow::io::OutputStream>(file);
}
