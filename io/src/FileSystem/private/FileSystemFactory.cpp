/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemFactory.h"

#include "FileSystem/HadoopFileSystem.h"
#include "FileSystem/LocalFileSystem.h"

#ifdef GCS_SUPPORT
#include "FileSystem/GoogleCloudStorage.h"
#endif

#ifdef S3_SUPPORT
#include "FileSystem/S3FileSystem.h"
#endif

std::unique_ptr<FileSystemInterface> FileSystemFactory::createFileSystem(
	const FileSystemConnection & fileSystemConnection, const Path & root) {
	const FileSystemType fileSystemType = fileSystemConnection.getFileSystemType();

	std::unique_ptr<FileSystemInterface> fileSystem = nullptr;

	switch(fileSystemType) {
	case FileSystemType::LOCAL: {
		fileSystem = std::unique_ptr<LocalFileSystem>(new LocalFileSystem(root));
	} break;

	case FileSystemType::HDFS: {
		fileSystem = std::unique_ptr<HadoopFileSystem>(new HadoopFileSystem(fileSystemConnection, root));
	} break;

	case FileSystemType::S3: {
#ifdef S3_SUPPORT
		fileSystem = std::unique_ptr<S3FileSystem>(new S3FileSystem(fileSystemConnection, root));
#endif
	} break;

	case FileSystemType::NFS4: {
		// TODO percy make NFS4 and NFS3 implementations
	} break;

	case FileSystemType::GOOGLE_CLOUD_STORAGE: {
#ifdef GCS_SUPPORT
		fileSystem = std::unique_ptr<GoogleCloudStorage>(new GoogleCloudStorage(fileSystemConnection, root));
#endif
	} break;

	default: {
		// TODO percy error handling ... unsupported operation
	} break;
	}

	return fileSystem;
}
