/*
 * Copyright 2019 BlazingDB, Inc.
 *     Copyright 2019 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "GoogleCloudStorageOutputStream.h"

#include <memory.h>

#include <arrow/io/api.h>
#include <arrow/memory_pool.h>

#include <FileSystem/private/GoogleCloudStorageReadableFile.h>

#include "arrow/buffer.h"
#include <istream>
#include <streambuf>

#include "ExceptionHandling/BlazingException.h"

#include "Library/Logging/Logger.h"

namespace Logging = Library::Logging;

// TODO: handle the situation when not all data is read
// const Aws::String FAILED_UPLOAD = "failed-upload";
class GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl {
public:
	//~GoogleCloudStorageOutputStreamImpl();
	GoogleCloudStorageOutputStreamImpl(
		const std::string & bucketName, const std::string & objectKey, std::shared_ptr<gcs::Client> gcsClient);

	arrow::Status close();
	arrow::Status write(const void * buffer, int64_t nbytes);
	arrow::Status write(const void * buffer, int64_t nbytes, int64_t * bytes_written);
	arrow::Status flush();
	arrow::Result<int64_t> tell() const;

private:
	std::shared_ptr<gcs::Client> gcsClient;
	std::string bucket;
	std::string key;

	//		Aws::String uploadId;

	//		std::vector<Aws::GoogleCloudStorage::Model::CompletedPart> completedParts; //just an etag (for response) and a
	//part number
	size_t currentPart;
	int64_t written;
};

struct membuf : std::streambuf {
	membuf(char const * base, size_t size) {
		char * p(const_cast<char *>(base));
		this->setg(p, p, p + size);
	}
};

struct imemstream : virtual membuf, std::iostream {
	imemstream(char const * base, size_t size)
		: membuf(base, size), std::iostream(static_cast<std::streambuf *>(this)) {}
};

GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl::GoogleCloudStorageOutputStreamImpl(
	const std::string & bucketName, const std::string & objectKey, std::shared_ptr<gcs::Client> gcsClient) {
	this->bucket = bucketName;
	this->key = objectKey;
	this->gcsClient = gcsClient;

	currentPart = 1;
	written = 0;

	//    Aws::GoogleCloudStorage::Model::CreateMultipartUploadRequest request;
	//	request.SetBucket(bucket);
	//	request.SetKey(key);
	//    Aws::GoogleCloudStorage::Model::CreateMultipartUploadOutcome createMultipartUploadOutcome =
	//    GoogleCloudStorageClient->CreateMultipartUpload(request);
	//	if (createMultipartUploadOutcome.IsSuccess()) {
	//		this->uploadId = createMultipartUploadOutcome.GetResult().GetUploadId();
	//	} else {
	//        Logging::Logger().logError("Failed to create Aws::GoogleCloudStorage::Model::CreateMultipartUploadOutcome
	//        for bucket: " + bucketName + " and key: " + objectKey);
	//		bool shouldRetry = createMultipartUploadOutcome.GetError().ShouldRetry();
	//		if (shouldRetry){
	//			Logging::Logger().logError(createMultipartUploadOutcome.GetError().GetExceptionName() + " : " +
	//createMultipartUploadOutcome.GetError().GetMessage() + "  SHOULD RETRY"); 		} else {
	//			Logging::Logger().logError(createMultipartUploadOutcome.GetError().GetExceptionName() + " : " +
	//createMultipartUploadOutcome.GetError().GetMessage() + "  SHOULD NOT RETRY");
	//		}
	//        throw BlazingGoogleCloudStorageException("Failed to create
	//        Aws::GoogleCloudStorage::Model::CreateMultipartUploadOutcome. Problem was " +
	//        createMultipartUploadOutcome.GetError().GetExceptionName() + " : " +
	//        createMultipartUploadOutcome.GetError().GetMessage());
	//		this->uploadId = FAILED_UPLOAD;
	//	}

	// start upload here
}

arrow::Status GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl::write(
	const void * /*buffer*/, int64_t /*nbytes*/) {
	//    Aws::GoogleCloudStorage::Model::UploadPartRequest uploadPartRequest;
	//	uploadPartRequest.SetBucket(bucket);
	//	uploadPartRequest.SetKey(key);
	//	uploadPartRequest.SetPartNumber(currentPart);
	//	uploadPartRequest.SetUploadId(uploadId);
	//	//char * tempBuffer = (char *) buffer;

	//	//std::shared_ptr<imemstream> membuffer =  std::make_shared<imemstream>(new );
	//	//   imemstream membuffer((char *) buffer,nbytes);

	////    std::shared_ptr<imemstream> memBufferPtr
	//	uploadPartRequest.SetBody(std::make_shared < imemstream > ((char *) buffer, nbytes));

	//	// uploadPart1Request.SetContentMD5(HashingUtils::Base64Encode(md5OfStream));

	//	uploadPartRequest.SetContentLength(nbytes);

	//	written += nbytes;
	//    Aws::GoogleCloudStorage::Model::UploadPartOutcome uploadOutcome =
	//    GoogleCloudStorageClient->UploadPart(uploadPartRequest);
	//	if (uploadOutcome.IsSuccess()) {
	//        Aws::GoogleCloudStorage::Model::CompletedPart completedPart;
	//		completedPart.SetETag(uploadOutcome.GetResult().GetETag());
	//		completedPart.SetPartNumber(currentPart);
	//		this->completedParts.push_back(completedPart);
	//		currentPart++;
	//		return arrow::Status::OK();
	//	} else {
	//		Logging::Logger().logError("In Write: Uploading part " + std::to_string(currentPart) + " on file " +
	//this->bucket + "/" + key + ". Problem was " + uploadOutcome.GetError().GetExceptionName() + " : " +
	//uploadOutcome.GetError().GetMessage()); 		return arrow::Status::IOError("Had a trouble uploading part " +
	//std::to_string(currentPart) + " on file " + this->bucket + "/" + key + ". Problem was " +
	//uploadOutcome.GetError().GetExceptionName() + " : " + uploadOutcome.GetError().GetMessage());
	//	}
}

arrow::Status GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl::flush() {
	// flush is a pass through in all reality
	// we are making each write send the contents for now for simplicity of design
	return arrow::Status::OK();
}

arrow::Status GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl::close() {
	//	//flush the buffer
	//	flush();
	//    Aws::GoogleCloudStorage::Model::CompleteMultipartUploadRequest completeMultipartUploadRequest;

	//	completeMultipartUploadRequest.SetBucket(bucket);
	//	completeMultipartUploadRequest.SetKey(key);
	//	completeMultipartUploadRequest.SetUploadId(uploadId);

	//    Aws::GoogleCloudStorage::Model::CompletedMultipartUpload completedMultipartUpload;
	//	completedMultipartUpload.SetParts(completedParts);
	//	completeMultipartUploadRequest.WithMultipartUpload(completedMultipartUpload);

	//    Aws::GoogleCloudStorage::Model::CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
	//    GoogleCloudStorageClient->CompleteMultipartUpload(completeMultipartUploadRequest);
	//	if (completeMultipartUploadOutcome.IsSuccess()) {
	//		return arrow::Status::OK();
	//	} else {
	//		Logging::Logger().logError("In closing outputstream. Problem was " +
	//completeMultipartUploadOutcome.GetError().GetExceptionName() + " : " +
	//completeMultipartUploadOutcome.GetError().GetMessage()); 		return arrow::Status::IOError("Error closing
	//outputstream. Problem was " + completeMultipartUploadOutcome.GetError().GetExceptionName() + " : " +
	//completeMultipartUploadOutcome.GetError().GetMessage());

	//	}
	////	GoogleCloudStorageClient->CompleteMultipartUpload(uploadCompleteRequest);
}

arrow::Result<int64_t> GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl::tell() const {
	return written;
}

// BEGIN GoogleCloudStorageOutputStream

GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStream(
	const std::string & bucketName, const std::string & objectKey, std::shared_ptr<gcs::Client> gcsClient)
	: impl_(new GoogleCloudStorageOutputStream::GoogleCloudStorageOutputStreamImpl(bucketName, objectKey, gcsClient)) {}

GoogleCloudStorageOutputStream::~GoogleCloudStorageOutputStream() {}

arrow::Status GoogleCloudStorageOutputStream::Close() { return this->impl_->close(); }

arrow::Status GoogleCloudStorageOutputStream::Write(const void * buffer, int64_t nbytes) {
	return this->impl_->write(buffer, nbytes);
}

arrow::Status GoogleCloudStorageOutputStream::Flush() { return this->impl_->flush(); }

arrow::Result<int64_t> GoogleCloudStorageOutputStream::Tell() const { return this->impl_->tell(); }

bool GoogleCloudStorageOutputStream::closed() const {
	// Since every file interaction is a request, then the file is never really open. This function is necesary due to
	// the Apache Arrow interface starting with v12. Depending on who or what uses this function, we may want this to
	// always return false??
	return true;
}

// END GoogleCloudStorageOutputStream
