/*
 * BlazingException.cpp
 *
 *  Created on: Feb 9, 2018
 *      Author: felipe
 */

#include "BlazingException.h"

BlazingException::BlazingException(const std::string & details) {
	this->details = details;
}

BlazingException::~BlazingException() {
	// TODO Auto-generated destructor stub
}

void BlazingException::append(const std::string & newDetails){
	this->details = newDetails + "\n" + this->details;
}
BlazingIOException::BlazingIOException(const std::string & details) : BlazingException(details){

}

BlazingOutOfMemoryException::BlazingOutOfMemoryException(const std::string & details) : BlazingException(details){

}


BlazingOutOfRangeException::BlazingOutOfRangeException(const std::string & details) : BlazingException(details){

}

BlazingSocketException::BlazingSocketException(const std::string & details) : BlazingIOException(details){

}

BlazingFileSystemException::BlazingFileSystemException(const std::string & details) : BlazingIOException(details) {

}

BlazingInvalidPathException::BlazingInvalidPathException(const Uri & uri) : BlazingFileSystemException(details){
	this->details = "The path " + uri.toString() + " is not valid";
}

BlazingInvalidPermissionsFileException::BlazingInvalidPermissionsFileException(const Uri & uri) : BlazingFileSystemException(details){
	this->details = "You don't have permissions to access the file found at " + uri.toString();
}
BlazingFileNotFoundException::BlazingFileNotFoundException(const Uri & uri) : BlazingFileSystemException(details){
	this->details = "Could not find file at path " + uri.toString();
}

BlazingS3Exception::BlazingS3Exception(const std::string & details) : BlazingFileSystemException(details){

}
