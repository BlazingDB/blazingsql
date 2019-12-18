#include "Library/Logging/FileOutput.h"
#include "Library/Logging/Logger.h"
#include <cstdio>
#include <iostream>
#include <stdio.h>
#include <sys/stat.h>

namespace Library {
namespace Logging {
FileOutput::FileOutput(const std::string & filename, bool truncate) {
	struct stat buffer;
	bool file_exists = (stat(filename.c_str(), &buffer) == 0);
	if(file_exists && !truncate) {  // if we are not truncating, lets make sure it does not get too big. If the file
									// there is alread big, lets archive it. Will only keep one old one in archive
		if(buffer.st_size > 10000000) {
			std::string filename_old = filename + ".old";
			file_exists = (stat(filename_old.c_str(), &buffer) == 0);
			if(file_exists) {
				remove(filename_old.c_str());
			}
			std::rename(filename.c_str(), filename_old.c_str());
		}
	}
	auto mode = std::ios::out | (truncate ? std::ios::trunc : std::ios::app);
	file.open(filename, mode);
}

FileOutput::~FileOutput() {}

void FileOutput::flush(std::string && log) {
	std::unique_lock<std::mutex> lock(mutex);
	file << log << std::endl;
}

void FileOutput::flush(const std::string & log) {
	std::unique_lock<std::mutex> lock(mutex);
	file << log << std::endl;
}


void FileOutput::flush(
	const int nodeInd, const std::string & datetime, const std::string & level, const std::string & log) {
	std::unique_lock<std::mutex> lock(mutex);
	file << datetime << "|" << nodeInd << "|" << level << "|" << log << std::endl;
}

// void FileOutput::setNodeIdentifier(const unsigned int nodeInd){
// 	Logging::Logger().logInfo("Node index is " + std::to_string(nodeInd));
// }
}  // namespace Logging
}  // namespace Library
