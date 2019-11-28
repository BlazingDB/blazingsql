/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FILE_FILTER_H_
#define _BLAZING_FILE_FILTER_H_

#include <functional>

#include "FileSystem/FileStatus.h"

using FileFilter = std::function< bool (const FileStatus &fileStatus) >;

using FileFilterFunctor = std::unary_function<FileStatus, bool>;

struct FilesFilter : FileFilterFunctor {
	bool operator()(const FileStatus &fileStatus) const;
};

struct DirsFilter : FileFilterFunctor {
	bool operator()(const FileStatus &fileStatus) const;
};

struct WildcardFilter : FileFilterFunctor {
	static bool match(const std::string &input, const std::string &wildcard);

	WildcardFilter(const std::string &wildcard);

	bool operator()(const FileStatus &fileStatus) const;

	std::string wildcard;
};

struct FileTypeWildcardFilter : FileFilterFunctor {
	FileTypeWildcardFilter(FileType fileType, const std::string &wildcard);

	bool operator()(const FileStatus &fileStatus) const;

	FileType fileType;
	std::string wildcard;
};

struct FileOrFolderFilter : FileFilterFunctor {
	bool operator()(const FileStatus &fileStatus) const;
};

#endif /* _BLAZING_FILE_FILTER_H_ */
