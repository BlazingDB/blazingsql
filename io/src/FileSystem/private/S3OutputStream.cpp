/*
 * Copyright 2017 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

#include "S3OutputStream.h"

#include <arrow/memory_pool.h>
#include <arrow/io/api.h>

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <memory.h>

#include <Util/StringUtil.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/GetBucketLocationRequest.h>

#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/Object.h>

#include <FileSystem/private/S3ReadableFile.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "arrow/buffer.h"
#include <istream>
#include <streambuf>

#include "ExceptionHandling/BlazingException.h"

#include "Library/Logging/Logger.h"
namespace Logging = Library::Logging;

// TODO: handle the situation when not all data is read
const Aws::String FAILED_UPLOAD = "failed-upload";
class S3OutputStream::S3OutputStreamImpl {
public:
	//~S3OutputStreamImpl();
	S3OutputStreamImpl(
		const std::string & bucketName, const std::string & objectKey, std::shared_ptr<Aws::S3::S3Client> s3Client);

	arrow::Status close();
	arrow::Status write(const void * buffer, int64_t nbytes);
	arrow::Status write(const void * buffer, int64_t nbytes, int64_t * bytes_written);
	arrow::Status flush();
    arrow::Result<int64_t> tell() const;

private:
	std::shared_ptr<Aws::S3::S3Client> s3Client;
	std::string bucket;
	std::string key;

	Aws::String uploadId;

	Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;  // just an etag (for response) and a part number
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

S3OutputStream::S3OutputStreamImpl::S3OutputStreamImpl(
	const std::string & bucketName, const std::string & objectKey, std::shared_ptr<Aws::S3::S3Client> s3Client) {
	this->bucket = bucketName;
	this->key = objectKey;
	this->s3Client = s3Client;

	currentPart = 1;
	written = 0;

	Aws::S3::Model::CreateMultipartUploadRequest request;
	request.SetBucket(bucket.data());
	request.SetKey(key.data());
	Aws::S3::Model::CreateMultipartUploadOutcome createMultipartUploadOutcome =
		s3Client->CreateMultipartUpload(request);
	if(createMultipartUploadOutcome.IsSuccess()) {
		this->uploadId = createMultipartUploadOutcome.GetResult().GetUploadId();
	} else {
		Logging::Logger().logError("Failed to create Aws::S3::Model::CreateMultipartUploadOutcome for bucket: " +
								   bucketName + " and key: " + objectKey);
		bool shouldRetry = createMultipartUploadOutcome.GetError().ShouldRetry();
		if(shouldRetry) {
			Logging::Logger().logError(std::string(createMultipartUploadOutcome.GetError().GetExceptionName().data()) + " : " +
									   createMultipartUploadOutcome.GetError().GetMessage().data() + "  SHOULD RETRY");
		} else {
			Logging::Logger().logError(std::string(createMultipartUploadOutcome.GetError().GetExceptionName().data()) + " : " +
									   createMultipartUploadOutcome.GetError().GetMessage().data() + "  SHOULD NOT RETRY");
		}
		throw BlazingS3Exception("Failed to create Aws::S3::Model::CreateMultipartUploadOutcome. Problem was " +
								 std::string(createMultipartUploadOutcome.GetError().GetExceptionName().data()) + " : " +
								 createMultipartUploadOutcome.GetError().GetMessage().data());
		this->uploadId = FAILED_UPLOAD;
	}

	// start upload here
}

arrow::Status S3OutputStream::S3OutputStreamImpl::write(const void * buffer, int64_t nbytes) {
	Aws::S3::Model::UploadPartRequest uploadPartRequest;
	uploadPartRequest.SetBucket(bucket.data());
	uploadPartRequest.SetKey(key.data());
	uploadPartRequest.SetPartNumber(currentPart);
	uploadPartRequest.SetUploadId(uploadId);
	// char * tempBuffer = (char *) buffer;

	// std::shared_ptr<imemstream> membuffer =  std::make_shared<imemstream>(new );
	//   imemstream membuffer((char *) buffer,nbytes);

	//    std::shared_ptr<imemstream> memBufferPtr
	uploadPartRequest.SetBody(std::make_shared<imemstream>((char *) buffer, nbytes));

	// uploadPart1Request.SetContentMD5(HashingUtils::Base64Encode(md5OfStream));

	uploadPartRequest.SetContentLength(nbytes);

	written += nbytes;
	Aws::S3::Model::UploadPartOutcome uploadOutcome = s3Client->UploadPart(uploadPartRequest);
	if(uploadOutcome.IsSuccess()) {
		Aws::S3::Model::CompletedPart completedPart;
		completedPart.SetETag(uploadOutcome.GetResult().GetETag());
		completedPart.SetPartNumber(currentPart);
		this->completedParts.push_back(completedPart);
		currentPart++;
		return arrow::Status::OK();
	} else {
		Logging::Logger().logError("In Write: Uploading part " + std::to_string(currentPart) + " on file " +
								   this->bucket + "/" + key + ". Problem was " +
								   std::string(uploadOutcome.GetError().GetExceptionName().data()) + " : " +
								   uploadOutcome.GetError().GetMessage().data());
		return arrow::Status::IOError("Had a trouble uploading part " + std::to_string(currentPart) + " on file " +
									  this->bucket + "/" + key + ". Problem was " +
									  std::string(uploadOutcome.GetError().GetExceptionName().data()) + " : " +
									  uploadOutcome.GetError().GetMessage().data());
	}
}

arrow::Status S3OutputStream::S3OutputStreamImpl::flush() {
	// flush is a pass through in all reality
	// we are making each write send the contents for now for simplicity of design
	return arrow::Status::OK();
}

arrow::Status S3OutputStream::S3OutputStreamImpl::close() {
	// flush the buffer
	flush();
	Aws::S3::Model::CompleteMultipartUploadRequest completeMultipartUploadRequest;

	completeMultipartUploadRequest.SetBucket(bucket.data());
	completeMultipartUploadRequest.SetKey(key.data());
	completeMultipartUploadRequest.SetUploadId(uploadId);

	Aws::S3::Model::CompletedMultipartUpload completedMultipartUpload;
	completedMultipartUpload.SetParts(completedParts);
	completeMultipartUploadRequest.WithMultipartUpload(completedMultipartUpload);

	Aws::S3::Model::CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
		s3Client->CompleteMultipartUpload(completeMultipartUploadRequest);
	if(completeMultipartUploadOutcome.IsSuccess()) {
		return arrow::Status::OK();
	} else {
		Logging::Logger().logError("In closing outputstream. Problem was " +
								   std::string(completeMultipartUploadOutcome.GetError().GetExceptionName().data()) + " : " +
								   std::string(completeMultipartUploadOutcome.GetError().GetMessage().data()));
		return arrow::Status::IOError("Error closing outputstream. Problem was " +
									  std::string(completeMultipartUploadOutcome.GetError().GetExceptionName().data()) + " : " +
									  completeMultipartUploadOutcome.GetError().GetMessage().data());
	}
	//	s3Client->CompleteMultipartUpload(uploadCompleteRequest);
}

arrow::Result<int64_t> S3OutputStream::S3OutputStreamImpl::tell() const { 
    return written;
}

// BEGIN S3OutputStream

S3OutputStream::S3OutputStream(
	const std::string & bucketName, const std::string & objectKey, std::shared_ptr<Aws::S3::S3Client> s3Client)
	: impl_(new S3OutputStream::S3OutputStreamImpl(bucketName, objectKey, s3Client)) {}

S3OutputStream::~S3OutputStream() {}

arrow::Status S3OutputStream::Close() { return this->impl_->close(); }

arrow::Status S3OutputStream::Write(const void * buffer, int64_t nbytes) { return this->impl_->write(buffer, nbytes); }

arrow::Status S3OutputStream::Flush() { return this->impl_->flush(); }

arrow::Result<int64_t> S3OutputStream::Tell() const { 
    return this->impl_->tell();
}

bool S3OutputStream::closed() const {
	// Since every file interaction is a request, then the file is never really open. This function is necesary due to
	// the Apache Arrow interface starting with v12. Depending on who or what uses this function, we may want this to
	// always return false??
	return true;
}

// END S3OutputStream
