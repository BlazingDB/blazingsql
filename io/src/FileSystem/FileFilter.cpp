/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileFilter.h"

#include <fnmatch.h>

// BEGIN FilesFilter

bool FilesFilter::operator()(const FileStatus & fileStatus) const { return fileStatus.isFile(); }

// END FilesFilter

// BEGIN DirsFilter

bool DirsFilter::operator()(const FileStatus & fileStatus) const { return fileStatus.isDirectory(); }

// END DirsFilter

// BEGIN WildcardFilter

bool WildcardFilter::match(const std::string & input, const std::string & wildcard) {
	const int flags = FNM_EXTMATCH;
	const int match = fnmatch(wildcard.c_str(), input.c_str(), flags);
	return (match == 0);
}

WildcardFilter::WildcardFilter(const std::string & wildcard) : wildcard(wildcard) {}

bool WildcardFilter::operator()(const FileStatus & fileStatus) const {
	return WildcardFilter::match(fileStatus.getUri().getPath().toString(), this->wildcard);
}

// END WildcardFilter

// BEGIN FileTypeWildcardFilter

FileTypeWildcardFilter::FileTypeWildcardFilter(FileType fileType, const std::string & wildcard)
	: fileType(fileType), wildcard(wildcard) {}

bool FileTypeWildcardFilter::operator()(const FileStatus & fileStatus) const {
	FileFilter fileFilter;

	switch(fileType) {
	case FileType::FILE: {
		fileFilter = FilesFilter();
	} break;

	case FileType::DIRECTORY: {
		fileFilter = DirsFilter();
	} break;

	default: break;
	}

	WildcardFilter wildcardFilter(wildcard);

	const bool matchFileFilter = fileFilter(fileStatus);
	const bool matchWildcardFilter = wildcardFilter(fileStatus);
	const bool match = (matchFileFilter && matchWildcardFilter);

	return match;
}

// END FileTypeWildcardFilter

// BEGIN FileOrFolderFilter

bool FileOrFolderFilter::operator()(const FileStatus & fileStatus) const {
	return fileStatus.isFile() || fileStatus.isDirectory();
}

// END FileOrFolderFilter
