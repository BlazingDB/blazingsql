/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemRepository.h"

#include "private/FileSystemRepository_p.h"

FileSystemRepository::FileSystemRepository(const Path & dataFile, bool encrypted)
	: pimpl(new FileSystemRepository::Private(dataFile, encrypted)) {}

FileSystemRepository::~FileSystemRepository() {}

const Path FileSystemRepository::getDataFile() const noexcept { return this->pimpl->dataFile; }

bool FileSystemRepository::isEncrypted() const noexcept { return this->pimpl->encrypted; }

std::vector<FileSystemEntity> FileSystemRepository::findAll() const { return this->pimpl->findAll(); }

bool FileSystemRepository::add(const FileSystemEntity & fileSystemEntity) const {
	return this->pimpl->add(fileSystemEntity);
}

bool FileSystemRepository::deleteByAuthority(const std::string & authority) const {
	return this->pimpl->deleteByAuthority(authority);
}
