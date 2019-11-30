/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_REPOSITORY_H_
#define _FILESYSTEM_REPOSITORY_H_

#include <memory>
#include <vector>

#include "FileSystem/FileSystemEntity.h"

// NOTE Stateless class (so no caching for now)
// Repository pattern (persistence abstraction), domain object: the FileSystem entity concept
class FileSystemRepository {
public:
	// NOTE will NOT try to load the data if file is not empty (use findAll to load all the file data)
	// For dataFile must be localted in the local file system
	FileSystemRepository(const Path & dataFile, bool encrypted = true);
	virtual ~FileSystemRepository();

	const Path getDataFile() const noexcept;
	bool isEncrypted() const noexcept;

	std::vector<FileSystemEntity> findAll() const;
	bool add(const FileSystemEntity & fileSystemEntity)
		const;  // returns false when fileSystemEntity is invalid or fileSystemEntity.authority already exists
	bool deleteByAuthority(const std::string & authority) const;

	// deleteAll ... when in doubt leave it out ... maybe in the future ...

private:
	class Private;
	const std::unique_ptr<Private> pimpl;  // private implementation
};

#endif /* _FILESYSTEM_REPOSITORY_H_ */
