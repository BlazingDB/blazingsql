/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#ifndef _BLAZING_FS_PATH_H_
#define _BLAZING_FS_PATH_H_

#include <string>

// NOTE Immutable class
class Path {
public:
	Path();
	Path(const std::string & path, bool strict = true);
	Path(const Path & other);
	Path(Path && other);
	~Path();

	bool isEmpty() const noexcept;
	bool isValid() const noexcept;
	bool isRoot() const noexcept;  // test if path is "/"
	bool isParentOf(
		const Path & child) const;  // this function assumes that child is a folder (false in case invalid or empty)

	std::string getResourceName() const noexcept;  // /dir1/file1.txt => file1.txt | /dir1/dir2/ => dir2
	std::string getFileExtension() const
		noexcept;  // /dir1/file1.txt => "txt" | /dir1/dir2/ => ""  | /dir1/dir2/a*.parquet => "parquet"

	Path getSubRootPath() const noexcept;  // /dir1/dir2/file1.txt => /dir1
	Path getParentPath() const noexcept;   // /dir1/dir2/file1.txt => /dir1/dir2/

	// this function assumes that currentParentPath and newParentPath are folders
	Path replaceParentPath(const Path & currentParent, const Path & newParent) const;

	void addExtention(const std::string & file_format_hint);

	// normalized folder convention is that if its a folder, it should have a slash at the end (for S3 and GS
	// compatibility). This function also assumes that if its a file, then it should have a dot something termination.
	Path getPathWithNormalizedFolderConvention() const;

	bool hasTrailingSlash() const;  // Check if it has '/' at the end (for S3 and GS compatibility).
	bool hasWildcard() const;		// /dir1/file*.txt => true | /dir1/dir2/ => false

	std::string toString(
		bool normalize = false) const;  // if normalize = true then remove redundant directory separators (//)

	Path & operator=(const std::string & path);  // assigns the path without any validation
	Path & operator=(const Path & other);
	Path & operator=(Path && other);

	bool operator==(const Path & other) const;
	bool operator!=(const Path & other) const;

	Path operator+(const std::string & path) const;  // path will be append

private:
	// TODO percy pimpl data mem shared
	std::string path;
	bool valid;
	bool root;  // flag that checks if path is "/"
};

#endif /* _BLAZING_FS_PATH_H_ */
