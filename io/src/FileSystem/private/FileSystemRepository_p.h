/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_REPOSITORY_PRIVATE_H_
#define _FILESYSTEM_REPOSITORY_PRIVATE_H_

#include "FileSystem/FileSystemRepository.h"

class FileSystemRepository::Private {
	public:
		Private(const Path &dataFile, bool encrypted = true);
		virtual ~Private();

		std::vector<FileSystemEntity> findAll() const;
		bool add(const FileSystemEntity &fileSystemEntity) const;
		bool deleteByAuthority(const std::string &authority) const;

	public:
		const Path dataFile;
		const bool encrypted;
};

#endif /* _FILESYSTEM_REPOSITORY_PRIVATE_H_ */
