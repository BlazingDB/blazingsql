/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "S3ReadableFile.h"

#include <streambuf>
#include <istream>
#include <aws/core/Aws.h>
#include "aws/s3/model/HeadObjectRequest.h"
#include <aws/s3/model/GetObjectRequest.h>

#include "arrow/buffer.h"
#include <arrow/memory_pool.h>

#include "Util/StringUtil.h"

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

//TODO: handle the situation when not all data is read

S3ReadableFile::~S3ReadableFile() {
}


S3ReadableFile::S3ReadableFile(std::shared_ptr<Aws::S3::S3Client> s3Client, std::string bucketName, std::string key) {
	this->key = key;
	this->bucketName = bucketName;
	this->s3Client = s3Client;
	position = 0;
	valid = true;
}

arrow::Status S3ReadableFile::Seek(int64_t position) {
	this->position = position;
	return arrow::Status::OK();
}
arrow::Status S3ReadableFile::Tell(int64_t* position) const {
	*position = this->position;
	return arrow::Status::OK();
}

arrow::Status S3ReadableFile::Close() {
	//because each read is its own request we really dont have to do this
	return arrow::Status::OK();

}

arrow::Status S3ReadableFile::GetSize(int64_t* size) {
	Aws::S3::Model::HeadObjectRequest request;

	request.SetBucket(bucketName);
	request.SetKey(key);

	Aws::S3::Model::HeadObjectOutcome results = this->s3Client->HeadObject(request);

	if (results.IsSuccess()) {
		*size = results.GetResult().GetContentLength();

	} else {
		*size = -1;
		Logging::Logger().logWarn("S3ReadableFile::GetSize, HeadObject failed");
		bool shouldRetry = results.GetError().ShouldRetry();
		if (shouldRetry){
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}

		return arrow::Status::IOError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage());

	}

	return arrow::Status::OK();
}

arrow::Status S3ReadableFile::Read(int64_t nbytes, int64_t* bytesRead, void* buffer) {

//	std::cout<<"S3ReadableFile::Read " + std::to_string(nbytes)<<std::endl;
	Aws::S3::Model::GetObjectRequest object_request;

	object_request.SetBucket(bucketName);
	object_request.SetKey(key);
	object_request.SetRange("bytes=" + std::to_string(position) + "-" + std::to_string(position + nbytes));

	auto results = this->s3Client->GetObject(object_request);

	if (!results.IsSuccess()) {
		//TODO: Make bad arrow status here
		Logging::Logger().logWarn("S3ReadableFile::Read, GetObject failed for bucketName: " + bucketName + " key " + key);
		bool shouldRetry = results.GetError().ShouldRetry();

		if (shouldRetry){
			Logging::Logger().logTrace("retrying");
			return this->Read(nbytes, bytesRead, buffer);
		} else {
			*bytesRead = 0;
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}

		return arrow::Status::IOError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage());
	} else {
		*bytesRead = results.GetResult().GetContentLength();
		*bytesRead = nbytes < *bytesRead ? nbytes : *bytesRead;
		results.GetResult().GetBody().read((char*)buffer, *bytesRead);
		position += *bytesRead;
		return arrow::Status::OK();
	}
}

arrow::Status S3ReadableFile::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {

//	std::cout<<"S3ReadableFile::Read " + std::to_string(nbytes)<<std::endl;
	Aws::S3::Model::GetObjectRequest object_request;

	object_request.SetBucket(bucketName);
	object_request.SetKey(key);
	object_request.SetRange("bytes=" + std::to_string(position) + "-" + std::to_string(position + nbytes));

	auto results = this->s3Client->GetObject(object_request);

	if (!results.IsSuccess()) {
		//TODO: Make bad arrow status here

		Logging::Logger().logWarn("S3ReadableFile::Read, GetObject failed for bucketName: " + bucketName + " key " + key);
		bool shouldRetry = results.GetError().ShouldRetry();
		if (shouldRetry){
			Logging::Logger().logTrace("retrying");
			return this->Read(nbytes, out);
		} else {
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}

		return arrow::Status::IOError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage());
	} else {

		// old implementation
				/*
		std::shared_ptr<arrow::ResizableBuffer> buffer;
		AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

		int64_t bytesRead = results.GetResult().GetContentLength();

		memcpy(buffer->mutable_data(), results.GetResult().GetBody().rdbuf(), bytesRead);

		position += bytesRead;
		*out = buffer;
		return arrow::Status::OK();
		*/

		int64_t bytesRead  = results.GetResult().GetContentLength();
		bytesRead = nbytes < bytesRead ? nbytes : bytesRead;
		position += bytesRead;
		if (bytesRead < nbytes){
			Logging::Logger().logError("Did not read all the bytes at S3ReadableFile::ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out)");
		}
//		results.GetResult().GetBody().read((char*)(out->get()->mutable_data()), bytesRead);

		std::shared_ptr<arrow::ResizableBuffer> buffer;
		AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

		results.GetResult().GetBody().read((char*)(buffer->mutable_data()), bytesRead);
		*out = buffer;

		return arrow::Status::OK();
	}
}

arrow::Status S3ReadableFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytesRead, void* buffer) {

//	std::cout<<"S3ReadableFile::ReadAt " + std::to_string(nbytes)<<std::endl;

	Aws::S3::Model::GetObjectRequest object_request;

	object_request.SetBucket(bucketName);
	object_request.SetKey(key);
	object_request.SetRange("bytes=" + std::to_string(position) + "-" + std::to_string(position + nbytes));

	auto results = this->s3Client->GetObject(object_request);

	if (!results.IsSuccess()) {
		//TODO: Make bad arrow status here

		Logging::Logger().logWarn("S3ReadableFile::ReadAt, GetObject failed for bucketName: " + bucketName + " key " + key);
		bool shouldRetry = results.GetError().ShouldRetry();
		if (shouldRetry){
			Logging::Logger().logTrace("retrying");
			return this->ReadAt(position, nbytes, bytesRead, buffer);
		} else {
			*bytesRead = 0;
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}

		return arrow::Status::IOError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage());
	} else {

		*bytesRead = results.GetResult().GetContentLength();
		*bytesRead = nbytes < *bytesRead ? nbytes : *bytesRead;
		this->position = position + *bytesRead;
		results.GetResult().GetBody().read((char*)buffer, *bytesRead);

		return arrow::Status::OK();
	}

}

arrow::Status S3ReadableFile::ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {

//	std::cout<<"S3ReadableFile::ReadAt " + std::to_string(nbytes)<<std::endl;

	Aws::S3::Model::GetObjectRequest object_request;

	object_request.SetBucket(bucketName);
	object_request.SetKey(key);
	object_request.SetRange("bytes=" + std::to_string(position) + "-" + std::to_string(position + nbytes));

	auto results = this->s3Client->GetObject(object_request);

	if (!results.IsSuccess()) {
		//TODO: Make bad arrow status here

		Logging::Logger().logWarn("S3ReadableFile::ReadAt, GetObject failed for bucketName: " + bucketName + " key " + key);
		bool shouldRetry = results.GetError().ShouldRetry();
		if (shouldRetry){
			Logging::Logger().logTrace("retrying");
			return this->ReadAt(position, nbytes, out);
		} else {
			Logging::Logger().logError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage() + "  SHOULD NOT RETRY");
		}

		return arrow::Status::IOError(results.GetError().GetExceptionName() + " : " + results.GetError().GetMessage());
	} else {
		// old implementation
		/*
		//byutes read should always be full amount
		//should almost always be nBytes
		std::shared_ptr<arrow::ResizableBuffer> buffer;
		AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

		int64_t bytesRead = nbytes;

		memcpy(buffer->mutable_data(), results.GetResult().GetBody().rdbuf(), bytesRead);

		*out = buffer;
		return arrow::Status::OK();
		*/

		int64_t bytesRead  = results.GetResult().GetContentLength();
		bytesRead = nbytes < bytesRead ? nbytes : bytesRead;
		this->position = position + nbytes;
		if (bytesRead < nbytes){
			Logging::Logger().logError("Did not read all the bytes at S3ReadableFile::ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out)");
		}
//		results.GetResult().GetBody().read((char*)(out->get()->mutable_data()), bytesRead);

		std::shared_ptr<arrow::ResizableBuffer> buffer;
		AllocateResizableBuffer(arrow::default_memory_pool(), nbytes, &buffer);

		results.GetResult().GetBody().read((char*)(buffer->mutable_data()), bytesRead);
		*out = buffer;

		return arrow::Status::OK();
	}
}

bool S3ReadableFile::supports_zero_copy() const {
	return false;
}

bool S3ReadableFile::closed() const {
	// Since every file interaction is a request, then the file is never really open. This function is necesary due to the Apache Arrow interface starting with v12. 
	// Depending on who or what uses this function, we may want this to always return false??
	return true;
}
