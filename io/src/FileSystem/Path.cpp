/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "Path.h"

#include "Util/StringUtil.h"

const char SLASH = '/';

// check if path is equals to "/"
static bool hasBeginSlashChar(const std::string & path) { return (path.front() == SLASH); }

static bool hasEndSlashChar(const std::string & path) { return (path.back() == SLASH); }

// check if path is valid absolute path
static bool isValidPath(const std::string & path) {
	if(path.empty()) {  // TODO percy duplicated check
		return false;
	} else {
		if(hasBeginSlashChar(path) == false) {
			return false;
		}
	}

	std::string mutablePath = path;  // TODO ugly

	if(StringUtil::contains(mutablePath, "..")) {
		return false;
	}

	return true;
}

/*static bool isRootOrInvalidPath(const Path & path) {
	const bool isRoot = path.isRoot();
	const bool isInvalid = (path.isValid() == false);

	return (isRoot || isInvalid);
}*/

static int countEndSlashChars(const std::string & path) {
	int endSlashCount = 0;

	// if this->path has the pattern /some/dir// (note the slash / chars at the end)
	// then we need to know how many slash / chars we have at the end of the path
	for(size_t i = 0; i < path.size(); ++i) {
		const int index = path.size() - 1 - i;

		if(path.at(index) == SLASH) {
			++endSlashCount;
		} else {
			break;
		}
	}

	return endSlashCount;
}

static int countBeginSlashChars(const std::string & path) {
	int beginSlashCount = 0;

	// if this->path has the pattern //some///dir// (note the slash / chars in the sub root)
	// then we need to know how many slash / chars we have at the begining of the path
	for(size_t i = 0; i < path.size(); ++i) {
		if(path.at(i) == SLASH) {
			++beginSlashCount;
		} else {
			break;
		}
	}

	return beginSlashCount;
}

Path::Path() : valid(false), root(false) {}

Path::Path(const std::string & path, bool strict) {
	if(path.empty()) {
		this->valid = false;
		return;
	}

	this->path = path;
	if(strict) {
		this->valid = isValidPath(path);

		if(this->valid == false) {
			// TODO percy error handling
			return;
		}
	}

	this->valid = true;
	this->root = (this->toString(true) == std::string(1, SLASH));
}

Path::Path(const Path & other) : path(other.path), valid(other.valid), root(other.root) {}

Path::Path(Path && other) : path(std::move(other.path)), valid(std::move(other.valid)), root(std::move(other.root)) {}

Path::~Path() {}

bool Path::isEmpty() const noexcept { return this->path.empty(); }

bool Path::isValid() const noexcept { return this->valid; }

bool Path::isRoot() const noexcept { return this->root; }

bool Path::isParentOf(const Path & child) const {
	const std::string normalizedParent = this->toString(true);
	const std::string normalizedChild = child.toString(true);
	const bool isParent = StringUtil::beginsWith(normalizedChild, normalizedParent);

	return isParent;
}

std::string Path::getResourceName() const noexcept {
	const int endSlashCount = countEndSlashChars(this->path);
	const std::vector<std::string> tokens = StringUtil::split(this->path, std::string(1, SLASH));
	const int index = tokens.size() - 1 - endSlashCount;
	std::string fileName = tokens.at(index);  // TODO should be const

	return fileName;
}

std::string Path::getFileExtension() const noexcept {
	std::string rname = this->getResourceName();

	if(StringUtil::contains(rname, ".") == false) {
		return "";
	}

	const std::vector<std::string> parts = StringUtil::split(rname, ".");
	return parts.back();
}

Path Path::getSubRootPath() const noexcept {
	const std::string normalized = this->toString(true);
	const Path testRoot(normalized);
	if(testRoot.isRoot()) {
		return testRoot;
	}

	const int beginSlashCount = countBeginSlashChars(this->path);
	const std::vector<std::string> tokens = StringUtil::split(this->path, std::string(1, SLASH));
	const std::string subRoot = tokens.at(beginSlashCount);

	const Path rootPath(SLASH + subRoot + SLASH, true);

	return rootPath;
}

Path Path::getParentPath() const noexcept {
	const int endSlashCount = countEndSlashChars(this->path);
	const std::string fileName = this->getResourceName();
	const int len = this->path.size() - fileName.size() - endSlashCount;
	const std::string parent = this->path.substr(0, len);

	return Path(parent, true);
}

Path Path::replaceParentPath(const Path & currentParent, const Path & newParent) const {
	if(currentParent.isParentOf(*this) == false) {
		return Path();
	}

	const bool fixCurrentParent = (false == hasEndSlashChar(currentParent.toString(false)));
	const bool fixNewParent = (false == hasEndSlashChar(newParent.toString(false)));
	const Path fixedCurrentParent = fixCurrentParent ? currentParent.toString(true) + SLASH : currentParent;
	const Path fixedNewParent = fixNewParent ? newParent.toString(true) + SLASH : newParent;
	const Path result = fixedNewParent.path + this->path.substr(fixedCurrentParent.path.size());

	return result;
}

bool Path::hasTrailingSlash() const {
	const bool result = (this->path.back() == SLASH);
	return result;
}

bool Path::hasWildcard() const {
	std::string wildcard = this->getResourceName();
	const bool result = StringUtil::contains(wildcard, "*");
	return result;
}

std::string Path::toString(bool normalize) const {
	if(normalize == false) {
		return this->path;
	}

	std::string mutablePath = path;

	while(StringUtil::contains(mutablePath, "//")) {
		mutablePath = StringUtil::replace(mutablePath, "//", "/");
	}

	return mutablePath;
}

// NOTE this operation is always using strict mode
Path & Path::operator=(const std::string & path) {
	if(path.empty()) {
		this->path = std::string();
		this->valid = false;
		this->root = false;

		return *this;
	}

	this->valid = isValidPath(path);
	this->path = path;

	const Path testRootPath(path, true);
	this->root = (testRootPath.toString(true) == std::string(1, SLASH));

	return *this;
}

Path & Path::operator=(const Path & other) {
	this->path = other.path;
	this->valid = other.valid;
	this->root = other.root;

	return *this;
}

Path & Path::operator=(Path && other) {
	this->path = std::move(other.path);
	this->valid = std::move(other.valid);
	this->root = std::move(other.root);

	return *this;
}

bool Path::operator==(const Path & other) const {
	// empty is a class invariant so we can optimize a little here
	if(this->isEmpty() && other.isEmpty()) {
		return true;
	}

	const bool pathEquals = (this->path == other.path);
	const bool validEquals = (this->valid == other.valid);

	const bool equals = (pathEquals && validEquals);

	return equals;
}

bool Path::operator!=(const Path & other) const { return !(*this == other); }

Path Path::operator+(const std::string & path) const {
	if(path.size() == 0) {
		Path newPath = *this;
		return newPath;
	}

	if(this->path.size() == 0) {
		Path newPath(path);
		return newPath;
	}

	if(this->path.at(this->path.size() - 1) == SLASH) {
		if(path.at(0) == '/') {
			Path newPath = Path(this->path + path.substr(1));
			return newPath;
		} else {
			Path newPath = Path(this->path + path);
			return newPath;
		}
	} else {
		if(path.at(0) == '/') {
			Path newPath = Path(this->path + path);
			return newPath;
		} else {
			Path newPath = Path(this->path + "/" + path);
			return newPath;
		}
	}
}
