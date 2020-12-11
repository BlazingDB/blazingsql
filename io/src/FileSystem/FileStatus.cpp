/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileStatus.h"

FileStatus::FileStatus() : uri(Uri()), fileType(FileType::UNDEFINED), fileSize(0) {}

FileStatus::FileStatus(const Uri & uri, FileType fileType, unsigned long long fileSize)
	: uri(uri), fileType(fileType), fileSize(fileSize) {}

FileStatus::FileStatus(const FileStatus & other) : uri(other.uri), fileType(other.fileType), fileSize(other.fileSize) {}

FileStatus::FileStatus(FileStatus && other)
	: uri(std::move(other.uri)), fileType(std::move(other.fileType)), fileSize(std::move(other.fileSize)) {}

FileStatus::~FileStatus() {}

Uri FileStatus::getUri() const noexcept { return this->uri; }

FileType FileStatus::getFileType() const noexcept { return this->fileType; }

unsigned long long FileStatus::getFileSize() const noexcept { return this->fileSize; }

bool FileStatus::isFile() const noexcept { return (this->fileType == FileType::FILE); }

bool FileStatus::isDirectory() const noexcept { return (this->fileType == FileType::DIRECTORY); }

std::string FileStatus::toString(bool /*normalize*/) const {
	// TODO percy
	return "";
}

FileStatus & FileStatus::operator=(const FileStatus & other) {
	this->uri = other.uri;
	this->fileType = other.fileType;
	this->fileSize = other.fileSize;

	return *this;
}

FileStatus & FileStatus::operator=(FileStatus && other) {
	this->uri = std::move(other.uri);
	this->fileType = std::move(other.fileType);
	this->fileSize = std::move(other.fileSize);

	return *this;
}

bool FileStatus::operator==(const FileStatus & other) const {
	const bool pathEquals = (this->uri == other.uri);
	const bool fileTypeEquals = (this->fileType == other.fileType);
	const bool fileSizeEquals = (this->fileSize == other.fileSize);

	const bool equals = (pathEquals && fileTypeEquals && fileSizeEquals);

	return equals;
}

bool FileStatus::operator!=(const FileStatus & other) const { return !(*this == other); }
