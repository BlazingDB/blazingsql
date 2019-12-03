/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _FILESYSTEM_METHOD_FACTORY_PRIVATE_H_
#define _FILESYSTEM_METHOD_FACTORY_PRIVATE_H_

#include <memory>

#include "FileSystem/FileSystemInterface.h"

class FileSystemFactory {
public:
	std::unique_ptr<FileSystemInterface> createFileSystem(
		const FileSystemConnection & fileSystemConnection, const Path & root = Path("/"));
};

#endif /* _FILESYSTEM_METHOD_FACTORY_PRIVATE_H_ */
