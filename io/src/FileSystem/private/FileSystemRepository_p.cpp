/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "FileSystemRepository_p.h"

#include <fstream>
#include <iostream>

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

#include <sys/stat.h>

static std::ostream & writeString(std::ofstream & stream, const std::string & value) {
	size_t size = value.size();
	std::ostream & write = stream.write(reinterpret_cast<const char *>(&size), sizeof(size));

	if(!write) {
		return write;
	}

	return stream.write(value.c_str(), size);
}

static std::istream & readString(std::ifstream & stream, std::string & value) {
	size_t size;
	std::istream & read = stream.read(reinterpret_cast<char *>(&size), sizeof(size));

	if(!read) {
		return read;
	}

	value.resize(size);

	return stream.read(&value[0], size);
}

FileSystemRepository::Private::Private(const Path & dataFile, bool encrypted)
	: dataFile(dataFile), encrypted(encrypted) {
	if(dataFile.isValid() == false) {
		// TODO percy error handling
	}
}

FileSystemRepository::Private::~Private() {}

std::vector<FileSystemEntity> FileSystemRepository::Private::findAll() const {
	std::vector<FileSystemEntity> result;

	struct stat buffer;
	std::string filePathStr = this->dataFile.toString(true);
	int success = lstat(filePathStr.c_str(), &buffer);


	if(success == 0) {  // file exists
		std::ifstream stream;
		stream.open(this->dataFile.toString(true), std::ios::binary | std::ios::in);


		if((stream.good() == false) || (stream.is_open() == false)) {
			// TODO percy error handling
			return result;
		}

		Logging::Logger().logTrace("Loading registed namespaces: ");
		std::string value;
		while(readString(stream, value)) {
			const std::string authority = value;
			readString(stream, value);
			const std::string fileSystemConnection = value;
			readString(stream, value);
			const std::string root = value;
			const FileSystemEntity fileSystemEntity(
				std::move(authority), std::move(fileSystemConnection), std::move(root), this->encrypted);
			Logging::Logger().logTrace(
				fileSystemEntity.getAuthority() + " -> " + fileSystemEntity.getRoot().toString());
			result.push_back(std::move(fileSystemEntity));
		}

		stream.close();
	} else {
		Logging::Logger().logError("Could not find filesystem namespaces file located at: " + filePathStr);
	}

	return result;
}

bool FileSystemRepository::Private::add(const FileSystemEntity & fileSystemEntity) const {
	if(fileSystemEntity.isValid() == false) {
		return false;
	}

	const std::vector<FileSystemEntity> all = this->findAll();

	for(const FileSystemEntity & loaded : all) {
		if(loaded.getAuthority() == fileSystemEntity.getAuthority()) {
			// authority already exists
			return false;
		}
	}

	std::ofstream stream;
	stream.open(this->dataFile.toString(true), std::ios::binary | std::ios::out | std::ios::app);

	if((stream.good() == false) || (stream.is_open() == false)) {
		// TODO percy error handling
		// std::cout << "WARNING: could not load the namespaces data file ... " << std::endl;
		return false;
	}

	// Write
	const std::string authority =
		this->encrypted ? fileSystemEntity.getEncryptedAuthority() : fileSystemEntity.getAuthority();
	const std::string fileSystemConnection = this->encrypted ? fileSystemEntity.getEncryptedFileSystemConnection()
															 : fileSystemEntity.getFileSystemConnection().toString();
	const std::string root =
		this->encrypted ? fileSystemEntity.getEncryptedRoot() : fileSystemEntity.getRoot().toString(false);

	writeString(stream, authority);
	writeString(stream, fileSystemConnection);
	writeString(stream, root);

	stream.close();

	return true;
}

// in case has issues re sync the namespaces then will try to rollback
bool FileSystemRepository::Private::deleteByAuthority(const std::string & authority) const {
	std::vector<FileSystemEntity> fileSystemEntities = this->findAll();

	int foundIndex = -1;

	for(size_t i = 0; i < fileSystemEntities.size(); ++i) {
		const FileSystemEntity & fileSystemEntity = fileSystemEntities.at(i);

		if(fileSystemEntity.getAuthority() == authority) {
			foundIndex = i;
			break;
		}
	}

	if(foundIndex != -1) {  // if the authority was found then
		fileSystemEntities.erase(fileSystemEntities.begin() + foundIndex);
	} else {  // else if not found return false
		// TODO percy error handling
		Logging::Logger().logWarn(
			"can't delete record from namespace file, could not found the authority " + authority);
		return false;
	}

	const std::string currentDataFile = this->dataFile.toString(true);

	// if there was only 1 single entry that was deleted then
	if(fileSystemEntities.empty()) {
		std::remove(currentDataFile.c_str());

		std::ifstream stream;
		stream.open(this->dataFile.toString(true), std::ios::binary | std::ios::out | std::ios::app);

		if((stream.good() == false) || (stream.is_open() == false)) {
			// TODO percy error handling
			// std::cout << "WARNING: could not load the namespaces data file ... " << std::endl;
			return false;
		}

		return true;
	}

	std::vector<int> errorIndexes;

	const std::string newDataFile = currentDataFile + ".new";
	const FileSystemRepository fileSystemRepository(newDataFile, this->encrypted);

	for(size_t i = 0; i < fileSystemEntities.size(); ++i) {
		const FileSystemEntity & fileSystemEntity = fileSystemEntities.at(i);
		const bool ok = fileSystemRepository.add(fileSystemEntity);

		if(ok == false) {
			errorIndexes.push_back(i);
		}
	}

	if(errorIndexes.empty() == false) {  // if the could not save some file system then
		// TODO percy error handling
		Logging::Logger().logError(
			"could not sync the namespace file, error in records " + std::to_string(errorIndexes.size()));

		std::remove(newDataFile.c_str());
		const bool deleted = !std::ifstream(newDataFile);

		if(deleted == false) {
			// TODO percy error handling
			// ERROR could not delete the temporary new namespace file
		}

		return false;
	}

	std::remove(currentDataFile.c_str());
	const bool deletedCurrent = !std::ifstream(currentDataFile);

	if(deletedCurrent == false) {
		// TODO percy error handling
		// ERROR could not delete the current namespacefile
		return false;
	}

	const bool moved = (std::rename(newDataFile.c_str(), currentDataFile.c_str()) == 0);

	if(moved == false) {
		// TODO percy error handling
		// warning could not save the new namespace file
		return false;
	}

	return true;
}
