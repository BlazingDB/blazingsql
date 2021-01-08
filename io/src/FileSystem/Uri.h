/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FS_URI_H_
#define _BLAZING_FS_URI_H_

#include <map>

#include "FileSystem/FileSystemType.h"
#include "FileSystem/Path.h"

// NOTE Immutable class
class Uri {
public:
	static const std::string fileSystemTypeToScheme(FileSystemType fileSystemType);
	static FileSystemType schemeToFileSystemType(const std::string & scheme);

	Uri();
	Uri(const std::string & uri, bool strict = true);  // TODO: remove all constructors except this one and default
	Uri(const std::string & scheme, const std::string & authority, const Path & path, bool strict = false);
	Uri(FileSystemType fileSystemType,
		const std::string & authority,
		const Path & path,
		bool strict = false);  // convenience ctor, will use Uri(scheme, auth, Path, strict)
	Uri(const Uri & other);
	Uri(Uri && other);
	~Uri();

	FileSystemType getFileSystemType() const noexcept;

	std::string getScheme() const noexcept;
	std::string getAuthority() const noexcept;
	Path getPath() const noexcept;
	bool isEmpty() const noexcept;
	bool isValid() const noexcept;
	bool isParentOf(const Uri & child) const;  // this function assumes that child is a folder and child.scheme/auth ==
											   // this.scheme/auth (false in case invalid or empty)

	// this function assumes that currentParentPath and newParentPath are folders
	Uri replaceParentUri(const Uri & currentParent, const Uri & newParent) const;

	std::string toString(bool normalize = false) const;  // normalize will be use with Path::toString(normalize)

	Uri & operator=(const std::string & uri);  // parse the uri
	Uri & operator=(const Uri & other);
	Uri & operator=(Uri && other);

	bool operator==(const Uri & other) const;
	bool operator!=(const Uri & other) const;

	Uri operator+(const std::string & path) const;  // path will be append to Uri::path

private:
	// TODO percy pimpl data mem shared
	FileSystemType fileSystemType;
	std::string scheme;
	std::string authority;
	Path path;
	bool valid;
};

#endif /* _BLAZING_FS_URI_H_ */
