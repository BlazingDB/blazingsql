/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "LocalFileSystem_p.h"

#include "ExceptionHandling/BlazingThread.h"
#include <chrono>
#include <cstring>
#include <iostream>

#include <stddef.h>

#include <cerrno>
#include <dirent.h>
#include <fcntl.h>  // O_RDONLY
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>  // read
#include <unistd.h>

#include "arrow/io/file.h"
#include "arrow/status.h"

const int FILE_RETRY_DELAY = 10000;

#include "ExceptionHandling/BlazingException.h"
#include <errno.h>

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

#define FILE_PERMISSION_BITS_MODE 0700

LocalFileSystem::Private::Private(const Path & root) : root(root) {}

inline void openDirExceptions(Uri uri) {
	switch(errno) {
	case EACCES: throw BlazingInvalidPermissionsFileException(uri);
	case EBADF: throw BlazingFileSystemException("The file descriptor to " + uri.toString() + " is no longer valid.");
	case ENOENT: throw BlazingFileNotFoundException("File at " + uri.toString() + " does not exist.");
	case EMFILE:
		throw BlazingFileSystemException(
			"The per process limit of files has been reached. Consider increasing the per process limit using ulimit");
	case ENFILE:
		throw BlazingFileSystemException(
			"The per process limit of files has been reached. Consider increasing the per process limit using ulimit");
	case ENOMEM: throw BlazingOutOfMemoryException("System ran out of memory");
	case ENOTDIR: throw BlazingFileSystemException(uri.toString() + " is not a directory ");
	}
}

bool LocalFileSystem::Private::exists(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	struct stat buffer;

	int success = lstat(path.toString().c_str(), &buffer);

	if(success == 0) {
		return true;
	} else {
		if(errno != 0 && errno != ENOENT) {  // #define ENOENT       2  /* No such file or directory */

			switch(errno) {
			case EACCES: throw BlazingInvalidPermissionsFileException(uriWithRoot);
			case EBADF:
				throw BlazingFileSystemException(
					"There was an error checking the existence of " + uriWithRoot.toString());
			case EFAULT: throw BlazingInvalidPathException(uriWithRoot);
			case ELOOP:
				throw BlazingFileSystemException(
					"There were too many symbolic links to follow when checking the existence of " +
					uriWithRoot.toString());
			case ENOMEM: throw BlazingOutOfMemoryException("System ran out of memory");
			case ENOTDIR: throw BlazingInvalidPathException(uriWithRoot);
			case EOVERFLOW:
				throw BlazingFileSystemException(
					"The file found at " + uriWithRoot.toString() +
					" is to big to be represented on your platform, are you using a 32 bit operating system?");
			}
			// TODO percy overload stream ops for path and uri std::cout<<"Error checking existence of "<<path<<"  .
			// Error was: "<<errno<<std::endl;
		}
		return false;
	}
}

FileStatus LocalFileSystem::Private::getFileStatus(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	struct stat stat_buf;

	const int success = stat(path.toString().c_str(), &stat_buf);

	if(success == 0) {
		FileType fileType;

		switch(stat_buf.st_mode & S_IFMT) {
		// case S_IFBLK: block device break;
		// case S_IFCHR: character device break;
		case S_IFDIR:
			fileType = FileType::DIRECTORY;
			break;
			// case S_IFIFO: FIFO/pipe break;
			// case S_IFLNK: fileType = FileType::SYMLINK; break; //DEPRECATED we don't symlink in simplicity anymore
		case S_IFREG:
			fileType = FileType::FILE;
			break;
			// case S_IFSOCK: socket  break;
		default: fileType = FileType::UNDEFINED; break;
		}

		return FileStatus(uri, fileType, stat_buf.st_size);
	} else {
		switch(errno) {
		case EACCES: throw BlazingInvalidPermissionsFileException(uri);
		case EBADF:
			throw BlazingFileSystemException("There was an error getting file status of " + uriWithRoot.toString());
		case ENOENT: throw BlazingFileNotFoundException(uriWithRoot);
		case EFAULT: throw BlazingInvalidPathException(uriWithRoot);
		case ELOOP:
			throw BlazingFileSystemException(
				"There were too many symbolic links to follow when getting file status of " + uriWithRoot.toString());
		case ENOMEM: throw BlazingOutOfMemoryException("System ran out of memory");
		case ENOTDIR: throw BlazingInvalidPathException(uriWithRoot);
		case EOVERFLOW:
			throw BlazingFileSystemException(
				"The file found at " + uriWithRoot.toString() +
				" is to big to be represented on your platform, are you using a 32 bit operating system?");
		}
	}

	return FileStatus();
}

// TODO percy list: reuse duplicated code in all list methods

std::vector<FileStatus> LocalFileSystem::Private::list(const Uri & uri, const FileFilter & filter) const {
	std::vector<FileStatus> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	DIR * dirp = opendir(path.toString().c_str());

	if(dirp) {
		struct dirent * entry;

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path listedPath = path + name;
				const FileStatus fileStatus = this->getFileStatus(Uri(uri.getScheme(), uri.getAuthority(), listedPath));
				const bool pass = filter(fileStatus);

				if(pass) {
					response.push_back(fileStatus);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path fullPath = path + name;
				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);

				const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
				const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

				const FileStatus relativeFileStatus = this->getFileStatus(relativeUri);
				const FileStatus fullFileStatus(
					fullUri, relativeFileStatus.getFileType(), relativeFileStatus.getFileSize());

				const bool pass = filter(fullFileStatus);  // filter must use the full path

				if(pass) {
					response.push_back(relativeFileStatus);
				}
			}
		}

		closedir(dirp);
	} else {
		openDirExceptions(uri);
	}

	return response;
}

std::vector<Uri> LocalFileSystem::Private::list(const Uri & uri, const std::string & wildcard) const {
	std::vector<Uri> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	DIR * dirp = opendir(path.toString().c_str());

	if(dirp) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		struct dirent * entry;

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path fullPath = path + name;
				const bool pass = WildcardFilter::match(fullPath.toString(true), finalWildcard);

				if(pass) {
					response.push_back(Uri(uri.getScheme(), uri.getAuthority(), fullPath));
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path fullPath = path + name;
				const bool pass =
					WildcardFilter::match(fullPath.toString(true), finalWildcard);  // filter must use the full path

				if(pass) {
					const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
					const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

					response.push_back(relativeUri);
				}
			}
		}

		closedir(dirp);
	} else {
		openDirExceptions(uri);
	}

	return response;
}

std::vector<std::string> LocalFileSystem::Private::listResourceNames(
	const Uri & uri, FileType fileType, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uri.getPath();

	DIR * dirp = opendir(path.toString().c_str());

	if(dirp) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		struct dirent * entry;

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path listedPath = path + name;
				const FileStatus fileStatus = this->getFileStatus(Uri(uri.getScheme(), uri.getAuthority(), listedPath));
				const FileTypeWildcardFilter filter(fileType, finalWildcard);
				const bool pass = filter(fileStatus);

				if(pass) {
					response.push_back(name);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path fullPath = uri.getPath() + name;
				const Uri fullUri(uri.getScheme(), uri.getAuthority(), fullPath);

				const Path relativePath = fullPath.replaceParentPath(uriWithRoot.getPath(), uri.getPath());
				const Uri relativeUri(uri.getScheme(), uri.getAuthority(), relativePath);

				const FileStatus relativeFileStatus = this->getFileStatus(relativeUri);
				const FileStatus fullFileStatus(
					fullUri, relativeFileStatus.getFileType(), relativeFileStatus.getFileSize());

				const FileTypeWildcardFilter filter(fileType, finalWildcard);
				const bool pass = filter(relativeFileStatus);  // filter must use the full path

				if(pass) {
					response.push_back(name);
				}
			}
		}

		closedir(dirp);
	} else {
		openDirExceptions(uri);
	}

	return response;
}

std::vector<std::string> LocalFileSystem::Private::listResourceNames(
	const Uri & uri, const std::string & wildcard) const {
	std::vector<std::string> response;

	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	DIR * dirp = opendir(path.toString().c_str());

	if(dirp) {
		const Path wildcardPath = uriWithRoot.getPath() + wildcard;
		const std::string finalWildcard = wildcardPath.toString(true);

		struct dirent * entry;

		if(this->root.isRoot()) {  // if root is '/' then we don't need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const bool pass = WildcardFilter::match(name, finalWildcard);

				if(pass) {
					response.push_back(name);
				}
			}
		} else {  // if root is not '/' then we need to replace the uris to relative paths
			while((entry = readdir(dirp)) != NULL) {
				const std::string name(entry->d_name);
				const bool skip = (name == ".") || ((name == ".."));

				if(skip) {
					continue;
				}

				const Path fullPath = uri.getPath() + name;

				const bool pass =
					WildcardFilter::match(fullPath.toString(true), finalWildcard);  // filter must use the full path

				if(pass) {
					response.push_back(name);
				}
			}
		}

		closedir(dirp);
	} else {
		openDirExceptions(uri);
	}

	return response;
}

bool LocalFileSystem::Private::makeDirectory(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	bool dirExists = false;
	int success = -1;
	struct stat sb;

	if(stat(path.toString().c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) {
		dirExists = true;
		success = 0;
	}


	if(!dirExists) {
		success = mkdir(path.toString().c_str(), FILE_PERMISSION_BITS_MODE);


		int countInvalids = 0;

		while(success != 0) {
			if(success == -1 && stat(path.toString().c_str(), &sb) == 0 &&
				S_ISDIR(sb.st_mode)) {  // if failed, check again if it exists or not
				success = 0;
				break;
			}

			int fileRetryDelay = FILE_RETRY_DELAY;
			const int sleep_milliseconds = (countInvalids + 1) * fileRetryDelay;
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));

			if(countInvalids % 5 == 0) {
				const std::string sysError(std::strerror(errno));

				Logging::Logger().logError("LocalFileSystem::Private::makeDirectory failed to make directory: " +
										   path.toString() + " try number " + std::to_string(countInvalids) +
										   "   Error was: " + sysError + " (" + std::to_string(errno) + ")");
			}
			countInvalids++;

			success = mkdir(path.toString().c_str(), FILE_PERMISSION_BITS_MODE);

			if(countInvalids > 10) {
				break;
			}
		}
		if(success == 0 && countInvalids > 0) {
			Logging::Logger().logError(
				"LocalFileSystem::Private::makeDirectory finally succeeded making directory: " + path.toString() +
				"   Succeeded after " + std::to_string(countInvalids) + " tries");
		}
	}

	if(success == -1 && (stat(path.toString().c_str(), &sb) != 0 ||
							!S_ISDIR(sb.st_mode))) {  // if failed, check again if it exists or not

		success = 0;

		switch(errno) {
		case EACCES: throw BlazingInvalidPermissionsFileException(uriWithRoot);
		case EDQUOT:
			throw BlazingFileSystemException(
				"Quota of diskblocks or indoes has been exhausted could not make directory " + uriWithRoot.toString());
		case EEXIST: throw BlazingFileSystemException("Pathname " + uriWithRoot.toString() + " already exists.");
		case ENOENT: throw BlazingFileNotFoundException("File at " + uriWithRoot.toString() + " does not exist.");
		case EFAULT: throw BlazingInvalidPathException(uri);
		case ELOOP:
			throw BlazingFileSystemException(
				"There were too many symbolic links to follow when making directory " + uriWithRoot.toString());
		case EMLINK:
			throw BlazingFileSystemException(
				"There were too many symbolic links to follow when making directory " + uriWithRoot.toString());
		case ENOMEM: throw BlazingOutOfMemoryException("System ran out of memory");
		case ENOSPC: throw BlazingFileSystemException("FileSystem out of space when making " + uriWithRoot.toString());
		case ENOTDIR: throw BlazingInvalidPathException(uri);
		case EPERM:
			throw BlazingFileSystemException(
				"The path " + uriWithRoot.toString() + " cannot be created. That path is immutable.");
		case EROFS:
			throw BlazingFileSystemException(
				"The path " + uriWithRoot.toString() + " cannot be created. This is a read-only file system.");
		case ENAMETOOLONG: throw BlazingFileSystemException("path " + uriWithRoot.toString() + " is too long.");
		}
	}

	const bool result = (success == -1) ? false : true;

	return result;
}

bool LocalFileSystem::Private::remove(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	if(path.isRoot()) {  // lets make sure we cant delete the whole computer
		return false;
	}

	int success = -1;

	// TODO: debate whether this is appropriate
	if(!exists(uri)) {
		return true;
	}

	FileStatus status = this->getFileStatus(uri);

	if(status.isDirectory()) {
		std::vector<Uri> files = this->list(uri);

		bool successToReturn = true;
		for(Uri sub : files) {
			successToReturn = successToReturn && remove(sub);
		}
		success = rmdir(path.toString().c_str());

		int count = 0;
		while(success == -1) {
			if(!exists(uri)) {
				return true;
			}
			int fileRetryDelay = FILE_RETRY_DELAY;
			const int sleep_milliseconds = (count + 1) * fileRetryDelay;
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));

			const std::string sysError(std::strerror(errno));

			Logging::Logger().logError("Was unable to remove folder for " + path.toString() +
									   " | Error was: " + sysError + " (" + std::to_string(errno) + ")");
			success = rmdir(path.toString().c_str());

			count++;
			if(count > 6) {
				break;
			}
		}

		successToReturn = successToReturn && (success == 0);
		if(!successToReturn) {
			// the only error it can return is enotempty
			if(errno == ENOTEMPTY) {
				throw BlazingFileSystemException("The directory " + uriWithRoot.toString() + " is not empty ");
			}
		}
		return successToReturn;

	} else {
		success = success && std::remove(path.toString().c_str());

		int count = 0;
		while(success == -1) {
			if(!exists(uri)) {
				return true;
			}
			int fileRetryDelay = FILE_RETRY_DELAY;
			const int sleep_milliseconds = (count + 1) * fileRetryDelay;
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));

			const std::string sysError(std::strerror(errno));

			Logging::Logger().logError("Was unable to remove file for " + path.toString() +
									   " | Error was: " + sysError + " (" + std::to_string(errno) + ")");
			success = success && std::remove(path.toString().c_str());

			count++;
			if(count > 6) {
				break;
			}
		}
		if(success != 0) {
		}
		return success == 0;
	}
}

bool LocalFileSystem::Private::move(const Uri & src, const Uri & dst) const {
	if(!src.isValid()) {
		throw BlazingInvalidPathException(src);
	}

	if(!dst.isValid()) {
		throw BlazingInvalidPathException(dst);
	}
	if(!exists(src)) {
		Logging::Logger().logError(
			"Can't rename " + src.toString() + " to " + dst.toString() + ". Could not find the file.");
		return false;
	}

	const Uri srcWithRoot(src.getScheme(), src.getAuthority(), this->root + src.getPath().toString());
	const Uri dstWithRoot(dst.getScheme(), dst.getAuthority(), this->root + dst.getPath().toString());

	const std::string srcFullPath = srcWithRoot.getPath().toString();
	const std::string dstFullPath = dstWithRoot.getPath().toString();

	int dataFile = rename(srcFullPath.c_str(), dstFullPath.c_str());

	int count = 0;
	while(dataFile == -1) {
		int fileRetryDelay = FILE_RETRY_DELAY;
		const int sleep_milliseconds = (count + 1) * fileRetryDelay;
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));

		const std::string sysError(std::strerror(errno));
		Logging::Logger().logError("Was unable to rename " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
								   " | Error was: " + sysError + " (" + std::to_string(errno) + ")");
		dataFile = rename(srcFullPath.c_str(), dstFullPath.c_str());

		count++;
		if(count > 6) {
			break;
		}
	}

	if(dataFile == -1) {
		Logging::Logger().logError("Can't rename " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
								   ". Error was: " + std::to_string(errno));

		switch(errno) {
		case EACCES:
			throw BlazingFileSystemException("You do not have permissions to move the file " + srcWithRoot.toString() +
											 " to " + dstWithRoot.toString());
		case ENOTEMPTY: throw BlazingFileSystemException("The directory " + dstWithRoot.toString() + " is not empty ");
		case ENOENT: throw BlazingFileNotFoundException(srcWithRoot);
		case EINVAL:
			throw BlazingFileSystemException(
				"The path " + srcWithRoot.toString() + " contains " + dstWithRoot.toString());
		case EISDIR:
			throw BlazingFileSystemException("The destination path " + dstWithRoot.toString() +
											 " is a directory but the source " + src.toString() + " is not.");
		case EMLINK:
			throw BlazingFileSystemException("Moving " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
											 " would result in too many link entries.");
		case ENOSPC:
			throw BlazingFileSystemException("Moving " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
											 " would require too long a name for the destination path.");
		case EROFS:
			throw BlazingFileSystemException("Moving " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
											 " would invovle writing to a read only filesystem.");
		case EXDEV:
			throw BlazingFileSystemException("Moving " + srcWithRoot.toString() + " to " + dstWithRoot.toString() +
											 " is not possible because the files are on different filesystems.");
		}
	}
	return (dataFile != -1);
}

bool LocalFileSystem::Private::truncateFile(const Uri & uri, long long length) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	int success = -1;

	if(!exists(uri)) {
		return false;
	}

	success = truncate(path.toString().c_str(), length);

	int count = 0;
	while(success == -1) {
		int fileRetryDelay = FILE_RETRY_DELAY;
		const int sleep_milliseconds = (count + 1) * fileRetryDelay;
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_milliseconds));

		success = truncate(path.toString().c_str(), length);

		count++;
		if(count > 6) {
			break;
		}
	}

	if(success == -1) {
		const std::string sysError(std::strerror(errno));
		Logging::Logger().logError("Can't remove folder" + path.toString() + ". Error was: " + std::to_string(errno));

		switch(errno) {
		case EACCES:
			throw BlazingFileSystemException("The path " + uriWithRoot.toString() +
											 " is either a directory or not a writeable location. Check permissions");
		case EINTR:
			throw BlazingFileSystemException("Truncating " + uriWithRoot.toString() + " was interrupted by a signal");
		case EINVAL:
			throw BlazingFileSystemException("The file " + uriWithRoot.toString() +
											 " was attempting to be truncated with a length of " +
											 std::to_string(length));
		case EIO:
			throw BlazingFileSystemException(
				"A hardware I/O error occurred while truncating " + uriWithRoot.toString());
		case EPERM:
			throw BlazingFileSystemException(
				"The path " + uriWithRoot.toString() + " cannot be truncated. It is append only or immutable.");
		case EFBIG:
			throw BlazingFileSystemException(
				"The file found at " + uriWithRoot.toString() +
				" is to big to be represented on your platform, are you using a 32 bit operating system?");
		}
	}

	const bool result = (success == -1) ? false : true;

	return result;
}

std::shared_ptr<arrow::io::RandomAccessFile> LocalFileSystem::Private::openReadable(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	auto readableFile = arrow::io::ReadableFile::Open(path.toString());
            
	if(!readableFile.status().ok()) {
		throw BlazingFileSystemException("Unable to open " + uriWithRoot.toString() + " for reading");
	}
	return readableFile.ValueOrDie();
}

std::shared_ptr<arrow::io::OutputStream> LocalFileSystem::Private::openWriteable(const Uri & uri) const {
	if(uri.isValid() == false) {
		throw BlazingInvalidPathException(uri);
	}

	const Uri uriWithRoot(uri.getScheme(), uri.getAuthority(), this->root + uri.getPath().toString());
	const Path path = uriWithRoot.getPath();

	auto outputStream = arrow::io::FileOutputStream::Open(path.toString());

	if(!outputStream.status().ok()) {
		throw BlazingFileSystemException("Unable to open " + uriWithRoot.toString() + " for writing");
	}

	return outputStream.ValueOrDie();
}
