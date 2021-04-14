/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018-2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "Uri.h"

#include "Util/StringUtil.h"
#include <iostream>

static const std::string SCHEME_AUTH_SPLITTER = "://";

const std::string Uri::fileSystemTypeToScheme(FileSystemType fileSystemType) {
	switch(fileSystemType) {
	case FileSystemType::LOCAL: return "file"; break;
	case FileSystemType::HDFS: return "hdfs"; break;
	case FileSystemType::S3: return "s3"; break;
	case FileSystemType::GOOGLE_CLOUD_STORAGE: return "gcs"; break;
	default: break;
	}

	return "unknown";
}

FileSystemType Uri::schemeToFileSystemType(const std::string & scheme) {
	if(scheme == "file") {
		return FileSystemType::LOCAL;
	}

	if(scheme == "hdfs") {
		return FileSystemType::HDFS;
	}

	if(scheme == "s3") {
		return FileSystemType::S3;
	}

	if(scheme == "gcs" || scheme == "gs") {
		return FileSystemType::GOOGLE_CLOUD_STORAGE;
	}

	return FileSystemType::UNDEFINED;  // return invalid
}

// BEGIN TODO move this to pimpl class

struct MatchResult {
	bool valid;
	FileSystemType fileSystemType;
	std::string scheme;
	std::string authority;
	Path path;
};

// check if uri has absolute linux path and is valid: return the 3 components
static MatchResult checkMatch(const std::string uri) {
	MatchResult result;
	result.valid = false;

	std::string mutableUri = uri;  // TODO ugly
	std::vector<std::string> tokens = StringUtil::split(mutableUri, SCHEME_AUTH_SPLITTER);

	if(tokens.size() != 2) {
		return result;
	}

	std::string scheme = tokens[0];

	if(StringUtil::contains(scheme, " ") || scheme.empty()) {
		return result;
	}

	std::vector<std::string> authPath = StringUtil::split(tokens[1], "/");

	if(authPath.size() == 1) {
		return result;
	}

	std::string authority = authPath[0];

	if(StringUtil::contains(authority, " ")) {
		return result;
	}

	const unsigned int schemaAuthPos = scheme.size() + SCHEME_AUTH_SPLITTER.size() + authority.size();
	const std::string pathPart = uri.substr(schemaAuthPos, uri.size() - schemaAuthPos);
	const Path path(pathPart, true);

	if(path.isValid() == false) {
		return result;
	}

	std::string normalizedScheme = scheme;
	std::transform(normalizedScheme.begin(),
		normalizedScheme.end(),
		normalizedScheme.begin(),
		::tolower);  // TODO percy do we need to do this?

	result.valid = true;
	result.scheme = scheme;
	result.authority = authority;
	result.path = path;
	result.fileSystemType = Uri::schemeToFileSystemType(scheme);

	return result;
}

// END

Uri::Uri() : valid(false) {}

// TODO too many branches, duplicated code
Uri::Uri(const std::string & uri, bool strict) {
	if(uri.empty()) {
		this->valid = false;
		return;
	}

	// first check if Uri string is a default path (like /some/path/a.txt)
	const Path localPath(uri, true);


	if(localPath.isValid()) {
		*this = Uri(Uri::fileSystemTypeToScheme(FileSystemType::LOCAL), "local", localPath);
		return;
	}

	const MatchResult matchResult = checkMatch(uri);

	if(matchResult.valid) {
		this->fileSystemType = matchResult.fileSystemType;
		this->scheme = matchResult.scheme;
		this->authority = matchResult.authority;
		this->path = matchResult.path;
		this->valid = true;
	} else {
		this->path = localPath;
		if(strict) {
			// TODO percy error handling
			this->valid = false;

			return;
		}
	}
}

Uri::Uri(const std::string & scheme, const std::string & authority, const Path & path, bool strict) {
	std::string normalizedScheme = scheme;
	std::transform(normalizedScheme.begin(),
		normalizedScheme.end(),
		normalizedScheme.begin(),
		::tolower);  // TODO percy do we need to do this?

	if(strict) {  // TODO check scheme and pass strict to path object
	}

	this->fileSystemType = Uri::schemeToFileSystemType(normalizedScheme);
	this->scheme = scheme;
	this->authority = authority;
	this->path = path;
	this->valid = true;
}

Uri::Uri(FileSystemType fileSystemType, const std::string & authority, const Path & path, bool strict)
	: Uri(Uri::fileSystemTypeToScheme(fileSystemType), authority, path, strict) {}

Uri::Uri(const std::string &scheme,
         const std::string &authority,
         const std::string &path,
         const std::string &query,
         const std::string &fragment)
  : fileSystemType(FileSystemType::UNDEFINED)
  , scheme(scheme)
  , authority(authority)
  , path(Path(path, false))
  , query(query)
  , fragment(fragment)
  , valid(true) {}

Uri::Uri(const Uri & other)
	: fileSystemType(other.fileSystemType), scheme(other.scheme), authority(other.authority), path(other.path),
	  valid(other.valid) {}

Uri::Uri(Uri && other)
	: fileSystemType(std::move(other.fileSystemType)), scheme(std::move(other.scheme)),
	  authority(std::move(other.authority)), path(std::move(other.path)), valid(std::move(other.valid)) {}

Uri::~Uri() {}

FileSystemType Uri::getFileSystemType() const noexcept { return this->fileSystemType; }

std::string Uri::getScheme() const noexcept { return this->scheme; }

std::string Uri::getAuthority() const noexcept { return this->authority; }

Path Uri::getPath() const noexcept { return this->path; }

bool Uri::isEmpty() const noexcept {
	const bool result = (this->scheme.empty() && this->authority.empty() && this->path.isEmpty());
	return result;
}

bool Uri::isValid() const noexcept { return this->valid; }

bool Uri::isParentOf(const Uri & child) const {
	// TODO validations

	if(this->scheme != child.scheme) {
		return false;
	}

	if(this->authority != child.authority) {
		return false;
	}

	return this->path.isParentOf(child.path);
}

Uri Uri::replaceParentUri(const Uri & currentParent, const Uri & newParent) const {
	Uri newUri = *this;
	newUri.fileSystemType = newParent.fileSystemType;
	newUri.scheme = newParent.scheme;
	newUri.authority = newParent.authority;
	newUri.path = newUri.path.replaceParentPath(currentParent.path, newParent.path);

	return newUri;
}

std::string Uri::toString(bool normalize) const {
	if(this->valid) {
		if(this->fileSystemType == FileSystemType::LOCAL &&
			this->scheme == Uri::fileSystemTypeToScheme(this->fileSystemType) &&
			this->authority == "local") {  // WSM TODO may want to get rid of this once its all working
			return this->path.toString(normalize);
		} else {
			// TODO percy avoid hard coded strings
			return this->scheme + SCHEME_AUTH_SPLITTER + this->authority + this->path.toString(normalize);
		}
	} else {
		return this->path.toString(normalize);
	}
}

// TODO percy duplicated code with the ctor version
Uri & Uri::operator=(const std::string & uri) {
	const MatchResult matchResult = checkMatch(uri);

	if(matchResult.valid) {
		this->fileSystemType = matchResult.fileSystemType;
		this->scheme = matchResult.scheme;
		this->authority = matchResult.authority;
		this->path = matchResult.path;
		this->valid = true;
	} else {
		this->path = Path(uri, true);

		if(this->path.isValid()) {
			*this = Uri(uri, true);
		} else {
			// TODO percy error handling
			// TODO check if is path
			this->valid = false;
		}
	}

	return *this;
}

Uri & Uri::operator=(const Uri & other) {
	this->fileSystemType = other.fileSystemType;
	this->scheme = other.scheme;
	this->authority = other.authority;
	this->path = other.path;
	this->valid = other.valid;

	return *this;
}

Uri & Uri::operator=(Uri && other) {
	this->fileSystemType = std::move(other.fileSystemType);
	this->scheme = std::move(other.scheme);
	this->authority = std::move(other.authority);
	this->path = std::move(other.path);
	this->valid = std::move(other.valid);

	return *this;
}

bool Uri::operator==(const Uri & other) const {
	// empty is a class invariant so we can optimize a little here
	const bool selfEmpty = (this->scheme.empty() && this->authority.empty() && this->path.isEmpty());
	const bool otherEmpty = (other.scheme.empty() && other.authority.empty() && other.path.isEmpty());
	if(selfEmpty && otherEmpty) {
		return true;
	}

	const bool fileSystemTypeEquals = (this->fileSystemType == other.fileSystemType);
	const bool schemeEquals = (this->scheme == other.scheme);
	const bool authorityEquals = (this->authority == other.authority);
	const bool pathEquals = (this->path == other.path);
	const bool validEquals = (this->valid == other.valid);

	const bool equals = (fileSystemTypeEquals && schemeEquals && authorityEquals && pathEquals && validEquals);

	return equals;
}

bool Uri::operator!=(const Uri & other) const { return !(*this == other); }

Uri Uri::operator+(const std::string & path) const {
	Uri newUri = Uri(*this);
	newUri.path = this->path + path;

	return newUri;
}
