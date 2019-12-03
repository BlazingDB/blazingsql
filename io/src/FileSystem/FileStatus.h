/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FILE_STATUS_H_
#define _BLAZING_FILE_STATUS_H_

#include <string>

#include "FileSystem/Uri.h"

// NOTE Immutable class
class FileStatus {
public:
	FileStatus();
	FileStatus(const Uri & uri, FileType fileType, unsigned long long fileSize);
	FileStatus(const FileStatus & other);
	FileStatus(FileStatus && other);
	~FileStatus();

	Uri getUri() const noexcept;
	FileType getFileType() const noexcept;
	unsigned long long getFileSize() const noexcept;

	// Helpers
	bool isFile() const noexcept;
	bool isDirectory() const noexcept;

	std::string toString(
		bool normalize = false) const;  // if normalize = true then remove redundant directory separators (//)

	FileStatus & operator=(const FileStatus & other);
	FileStatus & operator=(FileStatus && other);

	bool operator==(const FileStatus & other) const;
	bool operator!=(const FileStatus & other) const;

	/*BEGIN TODO when in doubt ... leave it out
	 Permission getPermission()

	 unsigned long long getBlockSize() const noexcept;

	 unsigned long long getModificationTime() const noexcept;
	 unsigned long long getAccessTime() const noexcept;

	 std::string getOwner() const noexcept;
	 std::string getGroup() const noexcept;
	 END TODO */

private:
	Uri uri;
	FileType fileType;
	unsigned long long fileSize;
};

#endif /* _BLAZING_FILE_STATUS_H_ */
