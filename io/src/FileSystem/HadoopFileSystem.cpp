/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "HadoopFileSystem.h"

#include "private/HadoopFileSystem_p.h"

HadoopFileSystem::HadoopFileSystem(const FileSystemConnection & fileSystemConnection, const Path & root)
	: pimpl(new HadoopFileSystem::Private(fileSystemConnection, root)) {}

HadoopFileSystem::~HadoopFileSystem() {}

FileSystemConnection HadoopFileSystem::getFileSystemConnection() const noexcept {
	const FileSystemConnection result = this->pimpl->getFileSystemConnection();
	return result;
}

Path HadoopFileSystem::getRoot() const noexcept { return this->pimpl->root; }

bool HadoopFileSystem::exists(const Uri & uri) const {
	const bool result = this->pimpl->exists(uri);
	return result;
}

FileStatus HadoopFileSystem::getFileStatus(const Uri & uri) const {
	const FileStatus result = this->pimpl->getFileStatus(uri);
	return result;
}

std::vector<FileStatus> HadoopFileSystem::list(const Uri & uri, const FileFilter & filter) const {
	const std::vector<FileStatus> result = this->pimpl->list(uri, filter);
	return result;
}

std::vector<FileStatus> HadoopFileSystem::list(const Uri & uri, FileType fileType, const std::string & wildcard) const {
	FileTypeWildcardFilter filter(fileType, wildcard);
	return this->list(uri, filter);
}

std::vector<Uri> HadoopFileSystem::list(const Uri & uri, const std::string & wildcard) const {
	const std::vector<Uri> result = this->pimpl->list(uri, wildcard);
	return result;
}

std::vector<std::string> HadoopFileSystem::listResourceNames(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, fileType, wildcard);
	return result;
}

std::vector<std::string> HadoopFileSystem::listResourceNames(const Uri & uri, const std::string & wildcard) const {
	const std::vector<std::string> result = this->pimpl->listResourceNames(uri, wildcard);
	return result;
}

bool HadoopFileSystem::makeDirectory(const Uri & uri) const {
	const bool result = this->pimpl->makeDirectory(uri);
	return result;
}

bool HadoopFileSystem::remove(const Uri & uri) const {
	const bool result = this->pimpl->remove(uri);
	return result;
}

bool HadoopFileSystem::move(const Uri & src, const Uri & dst) const {
	const bool result = this->pimpl->move(src, dst);
	return result;
}

bool HadoopFileSystem::truncateFile(const Uri & uri, const long long length) const {
	const bool result = this->pimpl->truncateFile(uri, length);
	return result;
}

std::shared_ptr<arrow::io::RandomAccessFile> HadoopFileSystem::openReadable(const Uri & uri) const {
	return this->pimpl->openReadable(uri);
}

std::shared_ptr<arrow::io::OutputStream> HadoopFileSystem::openWriteable(const Uri & uri) const {
	return this->pimpl->openWriteable(uri);
}
