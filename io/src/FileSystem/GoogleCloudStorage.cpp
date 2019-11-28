/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "GoogleCloudStorage.h"

#include "private/GoogleCloudStorage_p.h"

GoogleCloudStorage::GoogleCloudStorage(const FileSystemConnection &fileSystemConnection, const Path &root)
	: pimpl(new GoogleCloudStorage::Private(fileSystemConnection, root)) {
}

GoogleCloudStorage::~GoogleCloudStorage() {
}

FileSystemConnection GoogleCloudStorage::getFileSystemConnection() const noexcept {
	const FileSystemConnection result = this->pimpl->getFileSystemConnection();
	return result;
}

Path GoogleCloudStorage::getRoot() const noexcept {
	return this->pimpl->root;
}

bool GoogleCloudStorage::exists(const Uri &uri) const {
	const bool result = this->pimpl->exists(uri);
	return result;
}

FileStatus GoogleCloudStorage::getFileStatus(const Uri &uri) const {
	const FileStatus result = this->pimpl->getFileStatus(uri);
	return result;
}

std::vector<FileStatus> GoogleCloudStorage::list(const Uri &uri, const FileFilter &filter) const {
	const std::vector<FileStatus> result = this->pimpl->list(uri, filter);
	return result;
}

std::vector<FileStatus> GoogleCloudStorage::list(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	FileTypeWildcardFilter filter(fileType, wildcard);
	return this->list(uri, filter);
}

std::vector<Uri> GoogleCloudStorage::list(const Uri &uri, const std::string &wildcard) const {
	const std::vector<Uri> result = this->pimpl->list(uri, wildcard);
	return result;
}

std::vector<std::string> GoogleCloudStorage::listResourceNames(const Uri &uri, FileType fileType, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, fileType, wildcard);
	return result;
}

std::vector<std::string> GoogleCloudStorage::listResourceNames(const Uri &uri, const std::string &wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, wildcard);
	return result;
}

bool GoogleCloudStorage::makeDirectory(const Uri &uri) const {
	const bool result = this->pimpl->makeDirectory(uri);
	return result;
}

bool GoogleCloudStorage::remove(const Uri &uri) const {
	const bool result = this->pimpl->remove(uri);
	return result;
}

bool GoogleCloudStorage::move(const Uri &src, const Uri &dst) const {
	const bool result = this->pimpl->move(src, dst);
	return result;
}

bool GoogleCloudStorage::truncateFile(const Uri &uri, const long long length) const {
	const bool result = this->pimpl->truncateFile(uri, length);
	return result;
}

std::shared_ptr<arrow::io::RandomAccessFile> GoogleCloudStorage::openReadable(const Uri &uri) const {
    std::shared_ptr<GoogleCloudStorageReadableFile> file;
	this->pimpl->openReadable(uri,&file);
	return std::static_pointer_cast<arrow::io::RandomAccessFile>(file);
}

std::shared_ptr<arrow::io::OutputStream> GoogleCloudStorage::openWriteable(const Uri &uri) const {
    std::shared_ptr<GoogleCloudStorageOutputStream> file;
	this->pimpl->openWriteable(uri,&file);
	return std::static_pointer_cast<arrow::io::OutputStream>(file);
}
