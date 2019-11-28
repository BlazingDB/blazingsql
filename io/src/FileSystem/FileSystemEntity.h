/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FILE_SYSTEM_ENTITY_H_
#define _BLAZING_FILE_SYSTEM_ENTITY_H_

#include "FileSystem/Path.h"
#include "FileSystem/FileSystemConnection.h"

//NOTE Immutable class
class FileSystemEntity {
	public:
		FileSystemEntity(); // creates an invalid FileSystemEntity
		FileSystemEntity(const std::string &authority, const FileSystemConnection &fileSystemConnection, const Path &root = Path("/", false));
		FileSystemEntity(const std::string &authority, const std::string &fileSystemConnection, const std::string &root, bool encrypted = true);
		FileSystemEntity(const FileSystemEntity &other);
		FileSystemEntity(FileSystemEntity &&other);
		virtual ~FileSystemEntity();

		bool isValid() const noexcept;

		const std::string getAuthority() const;
		const FileSystemConnection getFileSystemConnection() const;
		const Path getRoot() const;

		const std::string getEncryptedAuthority() const;
		const std::string getEncryptedFileSystemConnection() const;
		const std::string getEncryptedRoot() const;

		std::string toString() const; // json format

		FileSystemEntity & operator=(const FileSystemEntity &other);
		FileSystemEntity & operator=(FileSystemEntity &&other);

		bool operator==(const FileSystemEntity &other) const;
		bool operator!=(const FileSystemEntity &other) const;

	private:
		std::string authority; // primary key
		FileSystemConnection fileSystemConnection; // text format: fileSystemType|connectionProperties where connectionProperties = "Val1,Val2,...,Valn"
		Path root; // text Path::toString(false)
};

#endif /* _BLAZING_FILE_SYSTEM_ENTITY_H_ */
