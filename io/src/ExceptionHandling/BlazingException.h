/*
 * BlazingException.h
 *
 *  Created on: Feb 9, 2018
 *      Author: felipe
 */

#ifndef BLAZINGEXCEPTION_H_
#define BLAZINGEXCEPTION_H_

#include <string>
#include <exception>
#include "FileSystem/Uri.h"

class BlazingException : public std::exception {
public:

	BlazingException(const std::string & details);
	virtual ~BlazingException();
	virtual const char* what() const throw (){
	       return this->details.c_str();
	    }
	virtual void append(const std::string & newDetails);
protected:
	std::string details;
};

class BlazingOutOfMemoryException : public BlazingException {
public:
	BlazingOutOfMemoryException(const std::string & details);
};

class BlazingIOException : public BlazingException {
public:
	BlazingIOException(const std::string & details);
};

class BlazingOutOfRangeException : public BlazingException {
public:
	BlazingOutOfRangeException(const std::string & details);
};

class BlazingSocketException : public BlazingIOException{
public:
	BlazingSocketException(const std::string & details);
};

class BlazingFileSystemException : public BlazingIOException {
public:
	BlazingFileSystemException(const std::string & details);
};

class BlazingFileNotFoundException : public BlazingFileSystemException{
public:
	BlazingFileNotFoundException(const Uri & uri);
};

class BlazingInvalidPathException : public BlazingFileSystemException{
public:
	BlazingInvalidPathException(const Uri & uri);
};

class BlazingInvalidPermissionsFileException : public BlazingFileSystemException{
public:
	BlazingInvalidPermissionsFileException(const Uri & uri);
};



class BlazingS3Exception : public BlazingFileSystemException {
public:
	BlazingS3Exception(const std::string & details);
};









#endif /* BLAZINGEXCEPTION_H_ */
